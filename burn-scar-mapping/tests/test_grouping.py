from datetime import UTC, datetime

import numpy as np
import pytest
import xarray as xr
from shapely import Geometry, box

import observations
from observations import group_days, select_observations


def test_group_days_sorts_times_and_combines_same_utc_day_passes() -> None:
    scenes = xr.Dataset(
        {"id": ("time", ["late", "next-day", "early"])},
        coords={
            "time": np.array(
                ["2025-08-21T23:59", "2025-08-22T00:00", "2025-08-21T10:00"],
                dtype="datetime64[ns]",
            )
        },
    )
    groups = group_days(scenes)
    assert list(groups) == ["2025-08-21", "2025-08-22"]
    assert [scene.id.item() for scene in groups["2025-08-21"]] == ["early", "late"]


def scenes(footprints: list[list[Geometry]], clouds: list[list[float]]) -> xr.Dataset:
    times = []
    geometries = []
    cloud_cover = []
    for day, (shapes, values) in enumerate(zip(footprints, clouds, strict=True), start=1):
        for hour, (shape, cloud) in enumerate(zip(shapes, values, strict=True)):
            times.append(f"2025-08-{day:02}T{hour:02}:00")
            geometries.append(shape)
            cloud_cover.append(cloud)
    return xr.Dataset(
        {"geometry": ("time", geometries), "cloud_cover": ("time", cloud_cover)},
        coords={"time": np.array(times, dtype="datetime64[ns]")},
    )


def mock_client(monkeypatch: pytest.MonkeyPatch, result: xr.Dataset) -> None:
    class Collection:
        def query(self, **_kwargs: object) -> xr.Dataset:
            return result

    class Dataset:
        def collection(self, _name: str) -> Collection:
            return Collection()

    class Client:
        def dataset(self, _name: str) -> Dataset:
            return Dataset()

    monkeypatch.setattr(observations, "Client", Client)


def test_selection_requires_union_to_cover_polygon_despite_gaps_and_overlaps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    area = box(0, 0, 4, 3)
    result = scenes(
        [
            [box(-20, 0, 1, 3), box(2, 0, 4, 3)],  # Large envelope, but a gap in the AOI.
            [box(0, 0, 2, 3), box(2, 0, 4, 3)],  # Full union.
            [box(0, 0, 3, 3), box(0, 0, 3, 3)],  # Overlap does not fill the missing strip.
            [area.difference(box(1, 1, 2, 2))],  # A hole is not full coverage.
        ],
        [[0, 0], [30, 30], [0, 0], [0]],
    )
    mock_client(monkeypatch, result)

    day, selected = select_observations(
        area,
        (datetime(2025, 8, 1, tzinfo=UTC), datetime(2025, 8, 5, tzinfo=UTC)),
    )
    assert day == "2025-08-02"
    assert len(selected) == 2


def test_selection_uses_lowest_unweighted_cloud_mean_unknown_as_100_and_earliest_tie(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    area = box(0, 0, 10, 1)
    result = scenes(
        [
            [box(0, 0, 9, 1), box(9, 0, 10, 1)],
            [area],
            [area],
            [area],
        ],
        [[0, 100], [49], [np.nan], [49]],
    )
    mock_client(monkeypatch, result)
    time_range = (
        datetime(2025, 8, 1, tzinfo=UTC),
        datetime(2025, 8, 5, tzinfo=UTC),
    )

    day, _ = select_observations(area, time_range)
    assert day == "2025-08-02"  # Unweighted 50 loses to 49; the equal-cloud later day loses its tie.


def test_selection_queries_expected_dataset_and_raises_without_full_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    area = box(0, 0, 2, 2)
    time_range = (
        datetime(2025, 8, 1, tzinfo=UTC),
        datetime(2025, 8, 2, tzinfo=UTC),
    )
    calls: list[tuple[str, object]] = []
    result = scenes([[box(0, 0, 1, 2)]], [[0]])

    class Collection:
        def query(self, **kwargs: object) -> xr.Dataset:
            calls.extend(kwargs.items())
            return result

    class Dataset:
        def collection(self, name: str) -> Collection:
            assert name == "L2A"
            return Collection()

    class Client:
        def dataset(self, name: str) -> Dataset:
            assert name == "open_data.aws_earth.sentinel2"
            return Dataset()

    monkeypatch.setattr(observations, "Client", Client)
    with pytest.raises(ValueError, match="No day covers the full area"):
        select_observations(area, time_range)
    assert dict(calls) == {"temporal_extent": time_range, "spatial_extent": area}
