from datetime import UTC, datetime
from unittest.mock import Mock, call

import pytest
import xarray as xr
from cyclopts import App
from tilebox.datasets.query import TimeInterval

from scripts import catalog, download_scenes, submit


def test_catalog_commands_and_empty_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deletion includes the last acquisition and stays scoped to the requested collection."""
    command, arguments, _ = catalog.app.parse_args("create --name custom --collection S2B_L2A")
    assert command is catalog.create
    assert arguments.kwargs == {"name": "custom", "collection": "S2B_L2A"}
    command, arguments, _ = catalog.app.parse_args("empty --dataset org.results --collection S2B_L2A")
    client = Mock()
    monkeypatch.setattr(catalog, "Client", lambda: client)
    target = client.dataset.return_value.collection.return_value
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = datetime(2025, 2, 1, tzinfo=UTC)
    target.info.return_value.availability = TimeInterval(start, end)
    points = xr.Dataset({"id": ("time", ["first", "last"])})
    target.query.return_value = points
    command(**arguments.kwargs)
    client.dataset.assert_called_once_with("org.results")
    client.dataset.return_value.collection.assert_called_once_with("S2B_L2A")
    target.query.assert_called_once_with(temporal_extent=TimeInterval(start, end, end_inclusive=True), skip_data=True)
    target.delete.assert_called_once_with(points, show_progress=True)
    target.reset_mock()
    target.info.return_value.availability = None
    catalog.empty(dataset="org.results", collection="S2B_L2A")
    target.query.assert_not_called()
    target.delete.assert_not_called()


def test_download_multiple_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    """Parse multiple IDs and download each from the chosen dataset and collection."""
    app = App()
    app.default(download_scenes.main)
    command, arguments, _ = app.parse_args("first second --dataset org.source --collection S2B")
    client = Mock()
    monkeypatch.setattr(download_scenes, "Client", lambda: client)
    source = client.dataset.return_value.collection.return_value
    scenes = [xr.Dataset({"granule_name": "granule-one"}), xr.Dataset({"granule_name": "granule-two"})]
    source.find.side_effect = scenes
    download = Mock(side_effect=["cache/one", "cache/two"])
    monkeypatch.setattr(download_scenes, "download_scene", download)
    command(**arguments.arguments)
    client.dataset.assert_called_once_with("org.source")
    client.dataset.return_value.collection.assert_called_once_with("S2B")
    assert source.find.call_args_list == [call("first"), call("second")]
    assert download.call_args_list == [call(scenes[0]), call(scenes[1])]


@pytest.mark.parametrize("cluster", [None, "", "org.custom"])
def test_submit_default_cluster(monkeypatch: pytest.MonkeyPatch, cluster: str | None) -> None:
    """Unset and empty environment values choose the SDK default; explicit slugs are preserved."""
    if cluster is None:
        monkeypatch.delenv("TILEBOX_CLUSTER", raising=False)
    else:
        monkeypatch.setenv("TILEBOX_CLUSTER", cluster)
    client = Mock()
    monkeypatch.setattr(submit, "Client", lambda: client)
    submit.main(start="2025-01-01", end="2025-02-01")
    assert client.jobs.return_value.submit.call_args.kwargs["cluster"] == (cluster or None)
