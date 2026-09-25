"""Select two daily mosaics, compute dNBR, and render a burn-scar overlay."""

from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime
from io import BytesIO
from pathlib import Path
from uuid import uuid4

import cloudpickle
import numpy as np
import xarray as xr
from odc.geo.cog import write_cog
from odc.geo.geobox import GeoBox
from PIL import Image
from shapely import Geometry, box, union_all
from tilebox.datasets import Client, iter_datapoints
from tilebox.datasets.assets import AssetCollection
from tilebox.workflows import ExecutionContext, Task

from burn_scar_mapping.imagery import (
    burn_overlay,
    normalized_burn_ratio,
    output_grid,
    read_mosaic,
    read_raster,
    render_rgba,
)

DATASET = "open_data.aws_earth.sentinel2"


def group_days(scenes: xr.Dataset) -> dict[str, list[xr.Dataset]]:
    """Group scenes by UTC date (e.g. '2025-04-23'), in acquisition-time order.

    Args:
        scenes: Queried scene metadata with a time coordinate.
    """
    groups: defaultdict[str, list[xr.Dataset]] = defaultdict(list)
    for scene in iter_datapoints(scenes.sortby("time")):
        day = str(scene.time.values.astype("datetime64[D]"))
        groups[day].append(scene)
    return dict(groups)


def select_maximum_coverage_scene(
    groups: Iterable[tuple[str, list[xr.Dataset]]], area: Geometry
) -> tuple[str, list[xr.Dataset]]:
    """Select the group of scenes with the most coverage for the given area.

    The first tie wins.

    Args:
        groups: Date/group pairs in preferred tie-breaking order.
        area: Query polygon in the same CRS as the footprints.
    """

    def coverage(group: tuple[str, list[xr.Dataset]]) -> float:
        """Measure the day's footprint union inside the query area.

        Args:
            group: Date and its scene metadata.
        """
        footprints = union_all([scene.geometry.item() for scene in group[1]])
        return footprints.intersection(area).area

    return max(groups, key=coverage)


class MapBurnScars(Task):
    """Select two daily mosaics and submit the burn-change workflow.

    Args:
        bounds: West, south, east, north in WGS84 degrees.
        time_range: Timezone-aware start and exclusive end of the search.
        dnbr_threshold: Minimum dNBR highlighted in the overlay.
        resolution: Output pixel size in metres, defaulting to 20.
    """

    bounds: tuple[float, float, float, float]
    time_range: tuple[datetime, datetime]
    dnbr_threshold: float = 0.27
    resolution: float = 20.0

    @staticmethod
    def identifier() -> tuple[str, str]:
        """Return the workflow's task name and version."""
        return "burn-scar-mapping/MapBurnScars", "v1.0"

    def execute(self, context: ExecutionContext) -> None:
        """Select dates and submit mosaic, delta, and overlay tasks.

        Args:
            context: Tilebox task submission, progress, and logging context.
        """
        context.current_task.display = "Select before/after daily mosaics"
        area = box(*self.bounds)
        scenes = Client().dataset(DATASET).collection("L2A").query(temporal_extent=self.time_range, spatial_extent=area)
        groups = list(group_days(scenes).items())
        if len(groups) < 2:
            raise ValueError("Need at least two days of imagery. Widen the time range.")
        middle = len(groups) // 2
        before, before_scenes = select_maximum_coverage_scene(groups[:middle], area)
        after, after_scenes = select_maximum_coverage_scene(reversed(groups[middle:]), area)
        for day, selected in [(before, before_scenes), (after, after_scenes)]:
            assets = [AssetCollection.from_datapoint(scene) for scene in selected]
            context.job_cache[f"{day}/assets"] = cloudpickle.dumps(assets)

        grid = output_grid(self.bounds, self.resolution)
        context.progress("RGB mosaics").add(2)
        context.progress("NBR mosaics").add(2)
        rgb_mosaics = context.submit_subtasks([MosaicRGB(before, grid), MosaicRGB(after, grid)])
        nbr_mosaics = context.submit_subtasks([ComputeNBR(before, grid), ComputeNBR(after, grid)])
        delta = context.submit_subtask(ComputeDelta(before, after), depends_on=nbr_mosaics)
        context.submit_subtask(
            RenderOverlay(f"{after}/rgb.tif", "dnbr.tif", self.dnbr_threshold),
            depends_on=[rgb_mosaics[1], delta],
        )
        context.logger.info("Selected daily mosaics", before=before, after=after)


class MosaicRGB(Task):
    """Create an RGB mosaic for one selected date.

    Args:
        day: UTC date key in the job cache.
        grid: Shared UTM output grid.
    """

    day: str
    grid: GeoBox

    @staticmethod
    def identifier() -> tuple[str, str]:
        """Return the task name and version."""
        return "MosaicRGB", "v1.0"

    async def execute(self, context: ExecutionContext) -> None:
        """Read the selected RGB bands and save their daily mosaic.

        Args:
            context: Tilebox job identity, progress, and logging context.
        """
        context.current_task.display = f"RGB mosaic {self.day}"
        assets = cloudpickle.loads(context.job_cache[f"{self.day}/assets"])
        logger = context.logger.bind(day=self.day, scenes=len(assets))
        logger.info("Reading RGB bands into mosaic", width=self.grid.width, height=self.grid.height)
        rgb = await read_mosaic(assets, self.grid, ["red", "green", "blue"], tracer=context.tracer, mask="scl")
        rgba = render_rgba(rgb)
        context.job_cache[f"{self.day}/rgb.tif"] = write_cog(
            rgba,
            ":mem:",
            nodata=None,
            photometric="RGB",
            alpha="YES",
        )
        logger.info("Wrote RGB mosaic", key=f"{self.day}/rgb.tif")
        context.progress("RGB mosaics").done(1)


class ComputeNBR(Task):
    """Compute NBR from a daily NIR/SWIR mosaic.

    Args:
        day: UTC date key in the job cache.
        grid: Shared UTM output grid.
    """

    day: str
    grid: GeoBox

    @staticmethod
    def identifier() -> tuple[str, str]:
        """Return the task name and version."""
        return "ComputeNBR", "v1.0"

    async def execute(self, context: ExecutionContext) -> None:
        """Read the selected NIR/SWIR bands and save their daily NBR.

        Args:
            context: Tilebox job cache, progress, and logging context.
        """
        context.current_task.display = f"NBR mosaic {self.day}"
        assets = cloudpickle.loads(context.job_cache[f"{self.day}/assets"])
        logger = context.logger.bind(day=self.day, scenes=len(assets))
        logger.info("Reading NIR/SWIR bands into mosaic")
        reflectance = await read_mosaic(assets, self.grid, ["nir", "swir22"], tracer=context.tracer, mask="scl")
        nbr = normalized_burn_ratio(reflectance)
        context.job_cache[f"{self.day}/nbr.tif"] = write_cog(nbr, ":mem:", nodata=np.nan)
        logger.info("Wrote NBR mosaic", key=f"{self.day}/nbr.tif", valid_fraction=float(np.isfinite(nbr).mean()))
        context.progress("NBR mosaics").done(1)


class ComputeDelta(Task):
    """Compute delta NBR: ΔNBR = NBR_before - NBR_after.

    Args:
        before: Earlier mosaic's UTC date.
        after: Later mosaic's UTC date.
    """

    before: str
    after: str

    @staticmethod
    def identifier() -> tuple[str, str]:
        """Return the task name and version."""
        return "ComputeDelta", "v1.0"

    def execute(self, context: ExecutionContext) -> None:
        """Save delta NBR: ΔNBR = NBR_before - NBR_after on the common grid.

        Args:
            context: Tilebox context providing the job cache.
        """
        context.current_task.display = f"Render delta NBR ({self.before} vs {self.after})"
        context.logger.info("Reading NBR mosaics for delta", before=self.before, after=self.after)
        before = read_raster(context.job_cache[f"{self.before}/nbr.tif"])
        after = read_raster(context.job_cache[f"{self.after}/nbr.tif"])
        dnbr = before - after
        valid = dnbr.values[np.isfinite(dnbr.values)]
        context.logger.info(
            "Computed delta NBR",
            valid_fraction=valid.size / dnbr.size,
            quantile95=float(np.quantile(valid, 0.95)) if valid.size else None,
            max_value=float(valid.max()) if valid.size else None,
        )
        context.job_cache["dnbr.tif"] = write_cog(dnbr, ":mem:", nodata=np.nan)
        context.logger.info("Wrote delta NBR", key="dnbr.tif")


class RenderOverlay(Task):
    """Highlight burn change on the after-day RGB.

    Args:
        rgb_scene: RGB TIFF job-cache key, e.g. '2025-04-23/rgb.tif'.
        dnbr_file: Delta NBR TIFF job-cache key.
        dnbr_threshold: Minimum dNBR highlighted in the PNG.
    """

    rgb_scene: str
    dnbr_file: str
    dnbr_threshold: float = 0.27

    @staticmethod
    def identifier() -> tuple[str, str]:
        """Return the task name and version."""
        return "RenderOverlay", "v1.0"

    def execute(self, context: ExecutionContext) -> None:
        """Save the full-resolution PNG overlay.

        Args:
            context: Tilebox job identity and logging context.
        """
        context.current_task.display = "Render burn-scar overlay"
        context.logger.info(
            "Rendering burn-scar overlay",
            rgb_scene=self.rgb_scene,
            dnbr_file=self.dnbr_file,
            threshold=self.dnbr_threshold,
        )
        dnbr = read_raster(context.job_cache[self.dnbr_file])
        with Image.open(BytesIO(context.job_cache[self.rgb_scene])) as rgb:
            overlay = burn_overlay(dnbr, rgb.convert("RGBA"), self.dnbr_threshold)
        with BytesIO() as buffer:
            overlay.save(buffer, format="PNG")
            png = buffer.getvalue()
        context.job_cache["burn_overlay.png"] = png
        path = Path.home() / f"burn_overlay_{uuid4().hex}.png"
        path.write_bytes(png)
        context.logger.info("Saved burn-scar overlay", key="burn_overlay.png", path=str(path))
