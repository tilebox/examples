from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pytest
import xarray as xr
from pyproj import CRS, Transformer
from shapely import transform

from seasonal_rgb_timelapse.tasks import (
    BuildSeasonalTimelapse,
    RenderSeasonalFrame,
    _season_name,
    _square_aoi,
    _upload_to_workflow_storage,
)


def test_square_aoi_has_requested_metric_dimensions() -> None:
    """The generated AOI is centered and square in a local metric CRS."""
    center = (48.2082, 16.3738)
    aoi = _square_aoi(center, 12.5)
    local_crs = CRS.from_proj4(f"+proj=aeqd +lat_0={center[0]} +lon_0={center[1]} +datum=WGS84 +units=m")
    transformer = Transformer.from_crs("EPSG:4326", local_crs, always_xy=True)
    local_aoi = transform(aoi, transformer.transform, interleaved=False)
    west, south, east, north = local_aoi.bounds

    assert east - west == pytest.approx(12_500)
    assert north - south == pytest.approx(12_500)


def test_task_inputs_round_trip_native_geospatial_types() -> None:
    """Tilebox serializes native UUID and Shapely task fields."""
    datapoint_id = UUID("019c8e2d-4aaa-7000-8000-000000000001")
    task = RenderSeasonalFrame(
        aoi=_square_aoi((48.2082, 16.3738), 5),
        output_name="output",
        season="March-May 2026",
        season_start=datetime(2026, 3, 1, tzinfo=timezone.utc),
        datapoint_id=datapoint_id,
    )

    restored = RenderSeasonalFrame._deserialize(task._serialize())  # noqa: SLF001

    assert restored.aoi.equals_exact(task.aoi, tolerance=0)
    assert restored.datapoint_id == datapoint_id
    assert restored.season_start == task.season_start


def test_root_task_defaults_to_an_optional_time_range() -> None:
    """The root task exposes the requested defaults and typed inputs."""
    task = BuildSeasonalTimelapse(
        center_lat_lon=(48.2082, 16.3738),
        square_width_km=5,
    )

    assert task.time_range is None
    assert task.max_cloud_percent == 20.0


def test_upload_uses_runner_api_connection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Workflow storage reuses the API connection inherited by the runner."""
    source = tmp_path / "timelapse.webp"
    source.write_bytes(b"webp")
    response = Mock()
    response.json.return_value = {
        "path": "/019d1c17-8636-7259-a5fd-dda1f9f26c1e/a57bb082e728a0cdce930ecfcccf4510a3a247be5f322b09b3a971a3f5ed34f8/seasonal output/timelapse.webp"
    }
    put = Mock(return_value=response)
    monkeypatch.setattr("seasonal_rgb_timelapse.tasks.niquests.put", put)
    client = SimpleNamespace(_auth={"url": "https://api.tilebox.com/", "token": "secret"})
    context = SimpleNamespace(
        runner_context=SimpleNamespace(storage_locations=SimpleNamespace(_client=client)),
    )

    storage_path = _upload_to_workflow_storage(source, "seasonal output/timelapse.webp", context)

    put.assert_called_once_with(
        "https://api.tilebox.com/v1/storage/a57bb082e728a0cdce930ecfcccf4510a3a247be5f322b09b3a971a3f5ed34f8/seasonal%20output/timelapse.webp",
        data=b"webp",
        headers={"Authorization": "Bearer secret", "Content-Type": "image/webp"},
        timeout=60,
    )
    response.raise_for_status.assert_called_once_with()
    assert storage_path == response.json.return_value["path"]


def test_season_name_for_cross_year_season() -> None:
    """December seasons include both years."""
    name, start = _season_name(xr.DataArray(datetime(2025, 12, 1)))

    assert name == "December 2025 - February 2026"
    assert start == datetime(2025, 12, 1, tzinfo=timezone.utc)


def test_season_name_within_one_year() -> None:
    """Seasons within one year show that year once."""
    name, _ = _season_name(xr.DataArray(datetime(2026, 6, 1)))

    assert name == "June - August 2026"
