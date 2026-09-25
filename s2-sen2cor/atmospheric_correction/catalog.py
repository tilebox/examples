import hashlib
import os
from pathlib import Path
from urllib.parse import quote

import niquests
import numpy as np
import xarray as xr
from tilebox.datasets import Client, DatasetClient
from tilebox.datasets.assets import Asset, AssetCollection, AssetLocation
from tilebox.datasets.data.datasets import DatasetKind
from tilebox.datasets.datasets.stac.v1.storage_pb import KnownStorageType, StorageScheme
from tilebox.datasets.schema import Assets, Storage

from atmospheric_correction.processing import PIPELINE_VERSION, SEN2COR_VERSION


def create_dataset(name: str, collection: str) -> DatasetClient:
    """Create or update the L2A schema and ensure the destination collection exists."""
    dataset = Client().create_or_update_dataset(
        DatasetKind.SPATIOTEMPORAL,
        name,
        fields=[
            {"name": "title", "type": str, "roles": ["primary_title"]},
            {"name": "source_datapoint_id", "type": str, "queryable": True},
            {"name": "pipeline_version", "type": str},
            {"name": "sen2cor_version", "type": str},
            {"name": "assets", "type": Assets},
            {"name": "storage", "type": Storage},
        ],
        name="Sentinel-2 atmospheric correction",
    )
    dataset.get_or_create_collection(collection)
    return dataset


def upload_rgb(path: Path, object_path: str) -> str:
    """
    Upload the RGB preview to Tilebox storage.

    Disclaimer: Tilebox workflow-storage upload is currently in private preview. The API may change in the future, and this function may break.
    """
    content = path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    api_url = (os.environ.get("TILEBOX_API_URL") or "https://api.tilebox.com").rstrip("/")
    response = niquests.put(
        f"{api_url}/v1/storage/{digest}/{quote(object_path, safe='/')}",
        data=content,
        headers={"Authorization": f"Bearer {os.environ['TILEBOX_API_KEY']}", "Content-Type": "image/png"},
        timeout=60,
    )
    response.raise_for_status()
    domain = api_url.removeprefix("https://api.").removeprefix("api.")
    storage_path = quote(str(response.json()["path"]).lstrip("/"), safe="/%")
    return f"https://workflow-storage.{domain}/{storage_path}"


def metadata_row(source: xr.Dataset, product: Path, rgb_url: str) -> xr.Dataset:
    """Describe the hosted preview and explicitly mock locations for every SAFE file."""
    output_storage = StorageScheme(
        known_type=KnownStorageType.AWS_S3,
        platform="https://{bucket}.s3.{region}.amazonaws.com",
        bucket="output-bucket",
        region="eu-central-1",
        title="Demo output bucket (placeholder)",
        description="Not uploaded. These SAFE files exist only on the worker's local disk.",
    )
    assets = [
        Asset(
            key="rgb",
            primary=AssetLocation(href=rgb_url),
            media_type="image/png",
            roles=frozenset({"visual", "thumbnail"}),
        )
    ]
    for path in sorted(product.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(product).as_posix()
        media_type = {".jp2": "image/jp2", ".xml": "application/xml"}.get(path.suffix, "application/octet-stream")
        assets.append(
            Asset(
                key=relative,
                primary=AssetLocation(
                    href=f"s3://output-bucket/{quote(product.name + '/' + relative, safe='/')}",
                    storage_schemes={"output-bucket": output_storage},
                ),
                media_type=media_type,
                roles=frozenset({"metadata" if path.suffix == ".xml" else "data"}),
            )
        )
    fields = {
        "geometry": source.geometry.item(),
        "title": product.name,
        "source_datapoint_id": str(source.id.item()),
        "pipeline_version": PIPELINE_VERSION,
        "sen2cor_version": SEN2COR_VERSION,
        **AssetCollection.from_assets(assets).to_fields(),
    }
    return xr.Dataset(
        {key: ("time", np.array([value], dtype=object)) for key, value in fields.items()},
        coords={"time": [source.time.values]},
    )
