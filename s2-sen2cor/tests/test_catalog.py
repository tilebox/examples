from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr
from shapely.geometry import box
from tilebox.datasets.assets import AssetCollection
from tilebox.datasets.datasets.stac.v1.asset_pb import KnownAssetRole

from atmospheric_correction import catalog
from atmospheric_correction.catalog import metadata_row, upload_rgb


def test_assets_and_schema(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Preserve provenance and distinguish the real HTTP preview from mock SAFE files."""
    product = tmp_path / "test product.SAFE"
    band = product / "GRANULE/tile/IMG_DATA/R20m/B04_20m.jp2"
    band.parent.mkdir(parents=True)
    band.touch()
    (product / "MTD_MSIL2A.xml").touch()
    source = xr.Dataset(
        {"id": "source-id", "geometry": box(12, 45, 13, 46)}, coords={"time": np.datetime64("2025-08-03T10:11:12")}
    )
    url = "https://workflow-storage.tilebox.com/org/digest/rgb.png"
    row = metadata_row(source, product, url)
    assert row.time.values[0] == source.time.values
    assert row.geometry.item().equals(source.geometry.item())
    assert row.source_datapoint_id.item() == "source-id"
    assets = AssetCollection.from_datapoint(row.isel(time=0))
    assert set(assets) == {"rgb", "MTD_MSIL2A.xml", "GRANULE/tile/IMG_DATA/R20m/B04_20m.jp2"}
    assert assets["rgb"].primary.href == url
    assert KnownAssetRole.THUMBNAIL in assets["rgb"].roles
    assert assets["MTD_MSIL2A.xml"].media_type == "application/xml"
    assert assets["GRANULE/tile/IMG_DATA/R20m/B04_20m.jp2"].media_type == "image/jp2"
    assert assets["MTD_MSIL2A.xml"].primary.href == "s3://output-bucket/test%20product.SAFE/MTD_MSIL2A.xml"
    assert not assets["rgb"].primary.storage_schemes
    storage = assets["MTD_MSIL2A.xml"].primary.storage_schemes["output-bucket"]
    assert storage.bucket == "output-bucket"
    assert "Not uploaded" in storage.description
    xr.testing.assert_identical(row, metadata_row(source, product, url))
    client = Mock()
    monkeypatch.setattr(catalog, "Client", lambda: client)
    catalog.create_dataset("demo", collection="S2B_L2A")
    fields = client.create_or_update_dataset.call_args.kwargs["fields"]
    assert {field["name"] for field in fields} == set(row.data_vars) - {"geometry"}
    assert {field["name"] for field in fields if field.get("queryable")} == {"source_datapoint_id"}
    client.create_or_update_dataset.return_value.get_or_create_collection.assert_called_once_with("S2B_L2A")


@pytest.mark.parametrize("domain", ["tilebox.com", "tilebox.dev"])
def test_hosted_upload_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, domain: str) -> None:
    """Use the upload response's organization path, not the upload endpoint as the asset URL."""
    image = tmp_path / "rgb.png"
    image.write_bytes(b"abc")
    monkeypatch.setenv("TILEBOX_API_KEY", "test-key")
    monkeypatch.setenv("TILEBOX_API_URL", f"https://api.{domain}/")
    response = Mock()
    response.json.return_value = {"path": "/org/digest/scene name/rgb.png"}
    put = Mock(return_value=response)
    monkeypatch.setattr("atmospheric_correction.catalog.niquests.put", put)
    assert (
        upload_rgb(image, "scene name/rgb.png") == f"https://workflow-storage.{domain}/org/digest/scene%20name/rgb.png"
    )
    put.assert_called_once_with(
        f"https://api.{domain}/v1/storage/ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad/scene%20name/rgb.png",
        data=b"abc",
        headers={"Authorization": "Bearer test-key", "Content-Type": "image/png"},
        timeout=60,
    )
    response.raise_for_status.assert_called_once()
    response.raise_for_status.side_effect = RuntimeError("upload failed")
    with pytest.raises(RuntimeError, match="upload failed"):
        upload_rgb(image, "scene name/rgb.png")
