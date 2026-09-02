from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import xarray as xr
from pyproj import CRS, Transformer
from shapely import Geometry, Point, box, transform
from tilebox.datasets import Client, field
from tilebox.datasets.data import TimeInterval
from tilebox.workflows import ExecutionContext, Task

from seasonal_rgb_timelapse.imagery import encode_animated, read_visual, render_frame

DATASET = "open_data.aws_earth.sentinel2"
COLLECTION = "L2A"
_SEASON_MONTHS = {
    "DJF": ("December", "February"),
    "MAM": ("March", "May"),
    "JJA": ("June", "August"),
    "SON": ("September", "November"),
}


class BuildSeasonalTimelapse(Task):
    center_lat_lon: tuple[float, float]
    square_width_km: float
    time_range: tuple[datetime, datetime] | None = None
    max_cloud_percent: float = 20.0

    @staticmethod
    def identifier() -> tuple[str, str]:
        """Return the stable root-task identifier."""
        return "tilebox.com/seasonal-rgb-timelapse/BuildSeasonalTimelapse", "v1.0"

    def execute(self, context: ExecutionContext) -> None:
        """Select one scene per season and submit frame and animation tasks."""
        # Determine the time range for the timelapse, defaulting to the last three years if not specified.
        if self.time_range is None:
            end = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
            start = end - timedelta(days=365 * 3)  # last three years
        else:
            start, end = self.time_range
        context.current_task.display = f"SeasonalTimelapse({start:%Y-%m-%d} - {end:%Y-%m-%d})"

        # Construct a square AOI around the center coordinates
        aoi = _square_aoi(self.center_lat_lon, self.square_width_km)
        # Construct a descriptive output name based on the time range, center coordinates, and square width
        lat, lon = self.center_lat_lon
        latitude = f"{abs(lat):.2f}{'N' if lat >= 0 else 'S'}"
        longitude = f"{abs(lon):.2f}{'E' if lon >= 0 else 'W'}"
        output_name = (
            f"{start.astimezone(UTC):%Y%m%d}_{end.astimezone(UTC):%Y%m%d}_"
            f"{latitude}_{longitude}_{self.square_width_km:g}km"
        )

        # Query the Sentinel-2 dataset for low-cloud scenes within the AOI and time range
        with context.tracer.span("query"):
            context.logger.info("Searching for low-cloud scenes", start=start.isoformat(), end=end.isoformat())
            collection = Client().dataset(DATASET).collection(COLLECTION)
            scenes = collection.query(
                temporal_extent=TimeInterval(start=start, end=end),
                spatial_extent=aoi,
                filter=field("cloud_cover") <= self.max_cloud_percent,
            )

        if scenes.sizes.get("time", 0) == 0:
            raise RuntimeError("no low-cloud scenes found in the requested time range")
        context.logger.info("Found low-cloud scenes", count=scenes.sizes.get("time", 0))

        scenes = scenes.isel(time=[geometry.item().covers(aoi) for geometry in scenes.geometry])
        if scenes.sizes.get("time", 0) == 0:
            raise RuntimeError("no low-cloud scenes cover the requested area")

        best_scenes_per_season = (
            scenes.resample(time="QS-DEC")
            .map(lambda group: group.isel(time=group.cloud_cover.argmin("time")))
            .dropna("time", subset=["id"])
        )

        frame_tasks: list[RenderSeasonalFrame] = []
        for index in range(best_scenes_per_season.sizes["time"]):
            chosen = best_scenes_per_season.isel(time=index)
            season, season_start = _season_name(chosen.time)
            frame_tasks.append(
                RenderSeasonalFrame(
                    aoi=aoi,
                    output_name=output_name,
                    season=season,
                    season_start=season_start,
                    datapoint_id=chosen.id.item(),
                )
            )

        context.logger.info("Mapped scenes to seasons", seasons=len(frame_tasks))
        context.progress().add(len(frame_tasks))
        frames = context.submit_subtasks(frame_tasks, max_retries=1)
        context.submit_subtask(AssembleTimelapse(output_name=output_name), depends_on=frames, max_retries=1)


class RenderSeasonalFrame(Task):
    aoi: Geometry
    output_name: str
    season: str
    season_start: datetime
    datapoint_id: UUID

    @staticmethod
    def identifier() -> tuple[str, str]:
        """Return the stable frame-task identifier."""
        return "tilebox.com/seasonal-rgb-timelapse/RenderSeasonalFrame", "v1.0"

    async def execute(self, context: ExecutionContext) -> None:
        """Read and render one unlabelled seasonal RGB frame."""
        context.current_task.display = f"Render {self.season}"
        datapoint = Client().dataset(DATASET).collection(COLLECTION).find(self.datapoint_id)
        stac_id = str(datapoint.stac_id.item())

        with context.tracer.span("read-sentinel-2-window"):
            visual = await read_visual(datapoint, self.aoi.bounds)

        end_month = (self.season_start.month + 1) % 12 + 1
        end_year = self.season_start.year + (self.season_start.month == 12)
        frame_name = f"{self.season_start:%y_%m}-{end_year % 100:02d}_{end_month:02d}.png"
        relative_frame_path = Path("frames") / frame_name
        frame_path = _output_root() / self.output_name / relative_frame_path
        render_frame(visual, destination=frame_path)
        context.progress().done(1)
        context.logger.info(
            "Rendered seasonal frame", frame=str(relative_frame_path), season=self.season, stac_id=stac_id
        )


class AssembleTimelapse(Task):
    output_name: str

    @staticmethod
    def identifier() -> tuple[str, str]:
        """Return the stable assembly-task identifier."""
        return "tilebox.com/seasonal-rgb-timelapse/AssembleTimelapse", "v1.0"

    def execute(self, context: ExecutionContext) -> None:
        """Encode the rendered frames into the final animated WebP."""
        output_dir = _output_root() / self.output_name
        frame_paths = sorted((output_dir / "frames").glob("*.png"))
        if not frame_paths:
            raise RuntimeError("no rendered frames found")
        context.current_task.display = f"Assemble WebP ({len(frame_paths)} frames)"

        relative_destination = Path(self.output_name) / "timelapse.webp"
        destination = _output_root() / relative_destination
        context.logger.info("Encoding animated WebP", frames=len(frame_paths))
        with context.tracer.span("encode-webp"):
            encode_animated(frame_paths, destination)
        context.logger.info(
            f"Timelapse saved to {destination}",
            frames=len(frame_paths),
            size_bytes=destination.stat().st_size,
        )


def _output_root() -> Path:
    """Return the output directory relative to the current working directory."""
    return Path("outputs")


def _season_name(time: xr.DataArray) -> tuple[str, datetime]:
    """Return an explicit season name and its UTC start time."""
    shorthand = str(time.dt.season.item())
    start_month, end_month = _SEASON_MONTHS[shorthand]
    season_start = time.values.astype("datetime64[us]").item().replace(tzinfo=UTC)
    if shorthand == "DJF":
        name = f"{start_month} {season_start.year} - {end_month} {season_start.year + 1}"
    else:
        name = f"{start_month} - {end_month} {season_start.year}"
    return name, season_start


def _square_aoi(center_lat_lon: tuple[float, float], square_width_km: float) -> Geometry:
    """Create a WGS84 square with the requested metric width."""
    lat, lon = center_lat_lon
    if not -90 <= lat <= 90 or not -180 <= lon <= 180:
        raise ValueError("center_lat_lon must contain valid WGS84 coordinates")
    if square_width_km <= 0:
        raise ValueError("square_width_km must be greater than zero")

    local_crs = CRS.from_proj4(f"+proj=aeqd +lat_0={lat} +lon_0={lon} +datum=WGS84 +units=m +no_defs")
    to_local = Transformer.from_crs("EPSG:4326", local_crs, always_xy=True)
    to_wgs84 = Transformer.from_crs(local_crs, "EPSG:4326", always_xy=True)
    center = transform(Point(lon, lat), to_local.transform, interleaved=False)
    half_width = square_width_km * 500
    local_square = box(
        center.x - half_width,
        center.y - half_width,
        center.x + half_width,
        center.y + half_width,
    )
    return transform(local_square, to_wgs84.transform, interleaved=False)
