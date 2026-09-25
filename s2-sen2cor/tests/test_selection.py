from unittest.mock import MagicMock

import numpy as np
import pytest
import xarray as xr

from sen2cor_workflow import tasks


def test_cloud_filter_precedes_scene_limit_and_does_not_use_server_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Check local cloud filtering before sorting and limiting the selected scenes."""
    scenes = xr.Dataset(
        {"id": ("time", ["late", "cloudy", "early", "missing"]), "cloud_cover": ("time", [20.0, 20.1, 3.0, np.nan])},
        coords={"time": np.array(["2025-08-12", "2025-08-01", "2025-08-05", "2025-08-03"], dtype="datetime64[ns]")},
    )
    client = MagicMock()
    query = client.dataset.return_value.collection.return_value.query
    query.return_value = scenes
    monkeypatch.setattr(tasks, "Client", lambda: client)
    context = MagicMock()
    task = tasks.ProcessArea(start="2025-08-01", end="2025-09-01", bounds=(54, 24, 55, 25), max_scenes=2)
    task.execute(context)
    assert "filter" not in query.call_args.kwargs
    submitted = context.submit_subtasks.call_args.args[0]
    assert [item.source_id for item in submitted] == ["early", "late"]
    assert context.submit_subtasks.call_args.kwargs == {"max_retries": 2}
    context.reset_mock()
    query.return_value = scenes.isel(time=[1, 3])
    task.execute(context)
    context.submit_subtasks.assert_not_called()
