import shlex
from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr
from cyclopts import App

from atmospheric_correction.tasks import select_scenes
from scripts import download_scenes, query_scenes
from scripts.submit import main


def test_cloud_filter_precedes_limit() -> None:
    """An early cloudy scene must not crowd out a later eligible scene."""
    scenes = xr.Dataset(
        {"id": ("time", ["late", "cloudy", "early", "missing"]), "cloud_cover": ("time", [20.0, 20.1, 3.0, np.nan])},
        coords={"time": np.array(["2025-08-12", "2025-08-01", "2025-08-05", "2025-08-03"], dtype="datetime64[ns]")},
    )
    assert select_scenes(scenes, 20, 2).id.values.tolist() == ["early", "late"]
    assert select_scenes(scenes, 20, 1).id.values.tolist() == ["early"]
    assert select_scenes(scenes.isel(time=[1, 3]), 20, 2).sizes["time"] == 0
    assert select_scenes(xr.Dataset(), 20, 2).sizes == {}


def test_cli_parses_dataset_collection_pairs_without_submitting() -> None:
    """Check real CLI token parsing without running the command or calling Tilebox."""
    app = App()
    app.default(main)
    _, arguments, _ = app.parse_args(
        "--start 2025-08-01 --end 2025-09-01 --source source.dataset S2B_S2MSI1C "
        "--destination destination.dataset S2B_L2A --bounds 12 45 13 46 --max-scenes 2",
    )
    assert arguments.kwargs["source"] == ("source.dataset", "S2B_S2MSI1C")
    assert arguments.kwargs["destination"] == ("destination.dataset", "S2B_L2A")
    assert arguments.kwargs["bounds"] == (12, 45, 13, 46)


def test_query_scenes_prints_selected_ids_and_download_command(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The printed command downloads exactly the eligible scenes in acquisition order."""
    client = Mock()
    monkeypatch.setattr(query_scenes, "Client", lambda: client)
    query = client.dataset.return_value.collection.return_value.query
    query.return_value = xr.Dataset(
        {"id": ("time", ["late", "cloudy", "early", "missing"]), "cloud_cover": ("time", [10, 10.1, 2, np.nan])},
        coords={"time": np.array(["2025-07-12", "2025-07-01", "2025-07-05", "2025-07-02"], dtype="datetime64[ns]")},
    )
    query_scenes.main(
        start="2025-07-01",
        end="2025-08-01",
        bounds=(12, 45, 13, 46),
        dataset="org.source",
        collection="custom collection",
        max_scenes=2,
        max_cloud_cover=10,
    )
    client.dataset.assert_called_once_with("org.source")
    client.dataset.return_value.collection.assert_called_once_with("custom collection")
    assert query.call_args.kwargs["temporal_extent"] == ("2025-07-01", "2025-08-01")
    assert query.call_args.kwargs["spatial_extent"].bounds == (12, 45, 13, 46)
    output = capsys.readouterr().out
    assert output.startswith("Selected scene IDs:\nearly\nlate\n")
    command = shlex.split(output.strip().splitlines()[-1])
    assert command[:3] == ["uv", "run", "scripts/download_scenes.py"]
    app = App()
    app.default(download_scenes.main)
    _, arguments, _ = app.parse_args(command[3:])
    assert arguments.arguments == {
        "source_ids": ["early", "late"],
        "dataset": "org.source",
        "collection": "custom collection",
    }
    query.return_value = xr.Dataset()
    query_scenes.main()
    assert capsys.readouterr().out == "No scenes match the selection.\n"
