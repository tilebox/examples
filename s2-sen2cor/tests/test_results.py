from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import numpy as np
import obstore as obs
import pytest
import xarray as xr
from obstore.store import AzureStore, LocalStore, MemoryStore
from shapely.geometry import box
from tilebox.datasets.assets import AssetCollection

from sen2cor_workflow import catalog, results, tasks
from sen2cor_workflow.catalog import metadata_row
from sen2cor_workflow.results import completion_key, download_asset, open_store, publish, read_completion
from sen2cor_workflow.tasks import ProcessScene


def test_catalog_uses_only_two_queryable_strings(monkeypatch):
    """Reserve the backend's two string indexes for source and pipeline lookups."""
    client = MagicMock()
    monkeypatch.setattr(catalog, "Client", lambda: client)
    catalog.create_catalog("test_results")
    fields = client.create_or_update_dataset.call_args.kwargs["fields"]
    indexed = {item["name"] for item in fields if item["type"] is str and item.get("queryable")}
    assert indexed == {"source_datapoint_id", "pipeline_version"}
    assert any(item["name"] == "sen2cor_version" for item in fields)
    assert not any(item["name"].endswith("_url") for item in fields)
    client.create_or_update_dataset.return_value.get_or_create_collection.assert_called_once_with("L2A")


@pytest.fixture(params=["local", "memory"])
def storage(request, tmp_path):
    """Provide a local or in-memory object store and its result URL."""
    if request.param == "local":
        root = tmp_path / "results with spaces"
        return LocalStore(root, mkdir=True), root.as_uri()
    return MemoryStore(), "https://example.blob.core.windows.net/results"


@pytest.fixture
def files(tmp_path):
    """Create sample product, NDVI, and thumbnail files for upload tests."""
    product = tmp_path / "output space.SAFE"
    product.mkdir()
    (product / "MTD_MSIL2A.xml").write_text("metadata")
    ndvi = tmp_path / "ndvi.tif"
    ndvi.write_bytes(b"raster")
    thumbnail = tmp_path / "thumbnail.png"
    thumbnail.write_bytes(b"image")
    return product, ndvi, thumbnail


def test_partial_upload_never_publishes_completion(storage, files, monkeypatch):
    """Check that an interrupted upload leaves no completion record."""
    store, url = storage
    put = obs.put

    def interrupted(store, key, content, **kwargs):
        """Fail the thumbnail upload while allowing earlier writes."""
        if key.endswith("thumbnail.png"):
            raise OSError("interrupted upload")
        return put(store, key, content, **kwargs)

    monkeypatch.setattr(obs, "put", interrupted)
    with pytest.raises(OSError):
        publish(store, url, "source", *files)
    assert read_completion(store, "source") is None


@pytest.mark.parametrize("version", ["v1", "v2"])
def test_old_completion_is_not_reused(storage, version):
    """Ignore completion records from older processing and metadata schemas."""
    store, _ = storage
    obs.put(store, f"sen2cor-02.12.04-ndvi-{version}/source/complete.json", b'{"sen2cor_version":"02.12.04"}')
    assert read_completion(store, "source") is None


def test_concurrent_publication_registers_winner_without_overwriting(storage, files):
    """Check that concurrent attempts and later retries return the same completed result."""
    store, url = storage
    with ThreadPoolExecutor(max_workers=2) as executor:
        attempts = [executor.submit(publish, store, url, "source", *files) for _ in range(2)]
        first, second = [attempt.result() for attempt in attempts]
    assert first == second == read_completion(store, "source")
    original = bytes(obs.get(store, completion_key("source")).bytes())
    assert publish(store, url, "source", *files) == first
    assert bytes(obs.get(store, completion_key("source")).bytes()) == original


def test_metadata_preserves_acquisition_geometry_and_assets(storage, files):
    """Check that repeatable metadata retains the source time, footprint, and output assets."""
    store, url = storage
    record = publish(store, url, "source", *files)
    source = xr.Dataset({"geometry": box(54, 24, 55, 25)}, coords={"time": np.datetime64("2026-08-17T07:03:00")})
    row = metadata_row(source, record)
    assert row.sizes == {"time": 1}
    assert row.sen2cor_version.item() == "02.12.04"
    assert row.pipeline_version.item() == "sen2cor-02.12.04-ndvi-v3"
    assert row.time.values[0] == source.time.values
    assert row.geometry.item().equals(source.geometry.item())
    assets = AssetCollection.from_datapoint(row.isel(time=0))
    assert set(assets) == {"product", "ndvi", "metadata", "rgb"}
    assert not any(name.endswith("_url") for name in row.data_vars)
    for key, asset in assets.items():
        assert asset.primary.href == record["assets"][key]["href"]
        assert "THUMBNAIL" not in {role.name for role in asset.roles}
    assert assets["product"].primary.href.endswith("output%20space.SAFE/")
    xr.testing.assert_identical(row, metadata_row(source, record))


def test_local_publication_and_notebook_reads_need_no_azure(files, tmp_path, monkeypatch):
    """Check local writes and asset reads without constructing Azure credentials."""
    credential = MagicMock(side_effect=AssertionError("Local storage must not authenticate to Azure"))
    monkeypatch.setattr(results, "DefaultAzureCredential", credential)
    url = (tmp_path / "new results").as_uri()
    with open_store(url) as store:
        record = publish(store, url + "/", "source", *files)
    source = xr.Dataset({"geometry": box(54, 24, 55, 25)}, coords={"time": np.datetime64("2026-08-17")})
    assets = AssetCollection.from_datapoint(metadata_row(source, record).isel(time=0))
    for key, expected in [("ndvi", b"raster"), ("rgb", b"image"), ("metadata", b"metadata")]:
        destination = tmp_path / key
        download_asset(assets[key].primary.href, destination)
        assert destination.read_bytes() == expected
    credential.assert_not_called()


def test_azure_factory_uses_identity_and_preserves_container_prefix(monkeypatch):
    """Check Azure store configuration and credential cleanup without network calls."""
    credential = MagicMock()
    factory = MagicMock(return_value=credential)
    monkeypatch.setattr(results, "DefaultAzureCredential", factory)
    url = "https://account.blob.core.windows.net/results/prefix"
    with open_store(url) as store:
        assert isinstance(store, AzureStore)
        assert store.config["account_name"] == "account"
        assert store.config["container_name"] == "results"
        assert store.prefix == "prefix"
    factory.assert_called_once_with()
    credential.__exit__.assert_called_once()


def test_laptop_task_publishes_then_retries_catalog_without_reprocessing(files, tmp_path, monkeypatch):
    """Check that a catalog retry reuses local outputs without rerunning correction."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("RESULTS_STORAGE_URL", raising=False)
    monkeypatch.setenv("RESULTS_DATASET", "test.outputs")
    monkeypatch.setenv("CDSE_ACCESS_KEY", "test-cdse-access")
    monkeypatch.setenv("CDSE_SECRET_KEY", "test-cdse-secret")
    credential = MagicMock(side_effect=AssertionError("must not use Azure"))
    monkeypatch.setattr(results, "DefaultAzureCredential", credential)
    monkeypatch.setattr(tasks, "Client", MagicMock())
    cdse = MagicMock()
    cdse.return_value.download.return_value = tmp_path / "input.SAFE"
    monkeypatch.setattr(tasks, "CopernicusStorageClient", cdse)
    correction = MagicMock(return_value=files[0])
    monkeypatch.setattr(tasks, "correct", correction)
    monkeypatch.setattr(tasks, "derive", lambda *args: files[1:])
    register = MagicMock(side_effect=[RuntimeError("catalog unavailable"), None])
    monkeypatch.setattr(tasks, "register", register)
    task = ProcessScene(source_id="source", source_collection="S2A_S2MSI1C")
    context = MagicMock()
    with pytest.raises(RuntimeError, match="catalog unavailable"):
        task.execute(context)
    task.execute(context)
    messages = [call.args[0] for call in context.logger.info.call_args_list]
    assert messages.count("Running Sen2Cor") == 1
    assert messages.count("Reusing completed result") == 1
    assert messages.count("Registering L2A metadata") == 2
    assert messages.count("L2A result registered") == 1
    cdse.assert_called_once_with(
        access_key="test-cdse-access", secret_access_key="test-cdse-secret", cache_directory=None
    )
    correction.assert_called_once()
    assert register.call_count == 2
    assert register.call_args_list[0].args[2] == register.call_args_list[1].args[2]
    destination = tmp_path / "notebook.tif"
    download_asset(register.call_args.args[2]["assets"]["ndvi"]["href"], destination)
    assert destination.read_bytes() == b"raster"
    credential.assert_not_called()
