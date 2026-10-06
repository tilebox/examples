from datetime import datetime
from io import BytesIO
from pathlib import Path
from uuid import uuid4

import cloudpickle
import numpy as np
from odc.geo.geobox import GeoBox
from PIL import Image
from shapely import Polygon
from tilebox.datasets.assets import AssetCollection
from tilebox.workflows import ExecutionContext, Task

from imagery import (
    burn_overlay,
    normalized_burn_ratio,
    output_grid,
    read_cog_from_bytes,
    read_mosaic,
    render_rgba,
    write_cog_to_bytes,
)
from observations import select_observations


class MapBurnScars(Task):
    """Select two daily mosaics and submit the burn-change workflow.

    Args:
        area: Query polygon in WGS84 longitude/latitude degrees.
        before_time_range: Timezone-aware start and exclusive end before the fire.
        after_time_range: Timezone-aware start and exclusive end after the fire.
        dnbr_threshold: Minimum dNBR highlighted in the overlay.
        resolution: Output pixel size in metres, defaulting to 20.
    """

    area: Polygon
    before_time_range: tuple[datetime, datetime]
    after_time_range: tuple[datetime, datetime]
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
        before, before_scenes = select_observations(self.area, self.before_time_range)
        after, after_scenes = select_observations(self.area, self.after_time_range)
        for day, selected in [(before, before_scenes), (after, after_scenes)]:
            assets = [AssetCollection.from_datapoint(scene) for scene in selected]
            context.job_cache[f"{day}/assets"] = cloudpickle.dumps(assets)

        grid = output_grid(self.area, self.resolution)
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
        with context.tracer.span("Read RGB mosaic"):
            rgb = await read_mosaic(assets, self.grid, ["red", "green", "blue"], mask="scl")
        rgba = render_rgba(rgb)
        context.job_cache[f"{self.day}/rgb.tif"] = write_cog_to_bytes(rgba, rgba=True)
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
        with context.tracer.span("Read NIR/SWIR mosaic"):
            reflectance = await read_mosaic(assets, self.grid, ["nir", "swir22"], mask="scl")
        nbr = normalized_burn_ratio(reflectance)
        context.job_cache[f"{self.day}/nbr.tif"] = write_cog_to_bytes(nbr)
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
        before = read_cog_from_bytes(context.job_cache[f"{self.before}/nbr.tif"])
        after = read_cog_from_bytes(context.job_cache[f"{self.after}/nbr.tif"])
        dnbr = before - after
        valid = dnbr.values[np.isfinite(dnbr.values)]
        context.logger.info(
            "Computed delta NBR",
            valid_fraction=valid.size / dnbr.size,
            quantile95=float(np.quantile(valid, 0.95)) if valid.size else None,
            max_value=float(valid.max()) if valid.size else None,
        )
        context.job_cache["dnbr.tif"] = write_cog_to_bytes(dnbr)
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
        dnbr = read_cog_from_bytes(context.job_cache[self.dnbr_file])
        rgba = read_cog_from_bytes(context.job_cache[self.rgb_scene])
        rgb = Image.fromarray(rgba.values.transpose(1, 2, 0))
        overlay = burn_overlay(dnbr, rgb, self.dnbr_threshold)
        with BytesIO() as buffer:
            overlay.save(buffer, format="PNG")
            png = buffer.getvalue()
        context.job_cache["burn_overlay.png"] = png
        path = Path.home() / f"burn_overlay_{uuid4().hex}.png"
        path.write_bytes(png)
        context.logger.info("Saved burn-scar overlay", key="burn_overlay.png", path=str(path))
