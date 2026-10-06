"""Select a low-cloud daily observation covering the full query area."""

from collections import defaultdict
from datetime import UTC, datetime

import numpy as np
import xarray as xr
from shapely import Polygon, box, union_all
from tilebox.datasets import Client, field, iter_datapoints


def query_scenes(area: Polygon, time_range: tuple[str, str]) -> xr.Dataset:
    """Find low-cloud granules for the first imagery preview."""
    collection = Client().dataset("open_data.aws_earth.sentinel2").collection("L2A")
    return collection.query(
        temporal_extent=time_range,
        spatial_extent=area,
        filter=field("cloud_cover") < 5,
    )


def group_days(scenes: xr.Dataset) -> dict[str, list[xr.Dataset]]:
    """Group by UTC date, preserving acquisition-time order within each day."""
    groups = defaultdict(list)
    for scene in iter_datapoints(scenes.sortby("time")):
        day = str(scene.time.values.astype("datetime64[D]"))
        groups[day].append(scene)
    return dict(groups)


def select_observations(area: Polygon, time_range: tuple[datetime, datetime]) -> tuple[str, list[xr.Dataset]]:
    """Select the lowest-cloud day with full footprint coverage in one window."""
    scenes = (
        Client()
        .dataset("open_data.aws_earth.sentinel2")
        .collection("L2A")
        .query(temporal_extent=time_range, spatial_extent=area)
    )
    candidates = [
        (day, granules)
        for day, granules in group_days(scenes).items()
        if union_all([scene.geometry.item() for scene in granules]).covers(area)
    ]
    if not candidates:
        raise ValueError("No day covers the full area. Widen this time window or reduce the area.")

    def cloud_cover(group: tuple[str, list[xr.Dataset]]) -> float:
        clouds = np.array([scene.cloud_cover.item() for scene in group[1]], dtype=float)
        return float(np.mean(np.where(np.isfinite(clouds), clouds, 100.0)))

    return min(candidates, key=cloud_cover)


if __name__ == "__main__":
    # All of Rhodes, Greece. The end date is exclusive.
    area = box(27.65, 35.85, 28.30, 36.50)
    before = select_observations(
        area,
        (datetime(2023, 7, 15, tzinfo=UTC), datetime(2023, 7, 20, tzinfo=UTC)),
    )
    after = select_observations(
        area,
        (datetime(2023, 7, 25, tzinfo=UTC), datetime(2023, 7, 30, tzinfo=UTC)),
    )
    for label, (day, granules) in [("before", before), ("after", after)]:
        print(f"{label}: {day}, {len(granules)} granules")  # noqa: T201 - script output
