import numpy as np
import xarray as xr
from tilebox.datasets import Client
from tilebox.datasets.assets import Asset, AssetCollection, AssetLocation
from tilebox.datasets.data.datasets import DatasetKind
from tilebox.datasets.schema import Assets, Authentication, Storage

COLLECTION = "L2A"


def create_catalog(code_name: str):
    """Create or update the results schema and ensure the L2A collection exists."""
    dataset = Client().create_or_update_dataset(
        DatasetKind.SPATIOTEMPORAL,
        code_name,
        fields=[
            {"name": "title", "type": str, "queryable": True, "roles": ["primary_title"]},
            *[
                {"name": name, "type": str, "queryable": True}
                for name in ("source_datapoint_id", "pipeline_version", "sen2cor_version")
            ],
            *[{"name": name, "type": str} for name in ("product_url", "metadata_url", "ndvi_url", "thumbnail_url")],
            {"name": "assets", "type": Assets},
            {"name": "storage", "type": Storage},
            # Asset schema metadata does not configure storage-client credentials.
            {"name": "authentication", "type": Authentication},
        ],
        name="Sentinel-2 Sen2Cor results",
    )
    dataset.get_or_create_collection(COLLECTION)
    return dataset


def metadata_row(source: xr.Dataset, record: dict) -> xr.Dataset:
    """Combine output assets with the source acquisition time and footprint."""
    assets = AssetCollection.from_assets(
        [
            Asset(
                key="ndvi",
                primary=AssetLocation(href=record["ndvi_url"]),
                media_type="image/tiff; application=geotiff; profile=cloud-optimized",
                roles=frozenset({"data"}),
            ),
            Asset(
                key="metadata",
                primary=AssetLocation(href=record["metadata_url"]),
                media_type="application/xml",
                roles=frozenset({"metadata"}),
            ),
        ]
    ).to_fields()
    fields = {"geometry": ("time", np.array([source.geometry.item()], dtype=object))}
    for name, value in record.items():
        fields[name] = ("time", [value])
    for name, value in assets.items():
        # Keep each nested asset field as one object in the single-row dataset.
        fields[name] = ("time", np.array([value], dtype=object))
    return xr.Dataset(fields, coords={"time": [source.time.values]})


def register(dataset_slug: str, source: xr.Dataset, record: dict):
    """Ingest result metadata, allowing an identical record on retry."""
    # The SDK assigns IDs; retries must reuse the same metadata.
    return (
        Client()
        .dataset(dataset_slug)
        .collection(COLLECTION)
        .ingest(
            metadata_row(source, record),
            allow_existing=True,
        )
    )
