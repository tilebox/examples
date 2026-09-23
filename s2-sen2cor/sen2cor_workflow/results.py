import json
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote, unquote
from uuid import uuid4

import obstore as obs
from azure.identity import DefaultAzureCredential
from obstore.auth.azure import AzureCredentialProvider
from obstore.exceptions import AlreadyExistsError
from obstore.store import AzureStore, LocalStore, ObjectStore

from sen2cor_workflow.processing import PIPELINE_VERSION, SEN2COR_VERSION


@contextmanager
def open_store(url: str):
    """Open local storage without credentials or Azure storage with identity authentication."""
    if url.startswith("file://"):
        yield LocalStore.from_url(url, mkdir=True)
    else:
        with DefaultAzureCredential() as credential:
            yield AzureStore.from_url(url, credential_provider=AzureCredentialProvider(credential))


def download_asset(url: str, destination: Path) -> None:
    """Stream a local or Azure asset to a file."""
    base_url, key = url.rsplit("/", 1)
    with open_store(base_url) as store, destination.open("wb") as target:
        for chunk in obs.get(store, unquote(key)).stream():
            target.write(chunk)


def completion_key(source_id: str) -> str:
    """Return the completion record key for this source and pipeline version."""
    return f"{PIPELINE_VERSION}/{source_id}/complete.json"


def read_completion(store: ObjectStore, source_id: str) -> dict | None:
    """Read a completed result record, returning None when it does not exist."""
    try:
        return json.loads(bytes(obs.get(store, completion_key(source_id)).bytes()))
    except FileNotFoundError:
        return None


def publish(store: ObjectStore, base_url: str, source_id: str, product: Path, ndvi: Path, thumbnail: Path) -> dict:
    """Upload this attempt's files and return the first completed attempt's record."""
    prefix = f"{PIPELINE_VERSION}/{source_id}/attempts/{uuid4()}"
    base_url = base_url.rstrip("/")

    def upload(path: Path, relative: str) -> str:
        """Stream a file to this attempt's prefix and return its URL."""
        key = f"{prefix}/{relative}"
        # Unique attempt paths permit streaming multipart writes. Conditional creates
        # would buffer entire SAFE files in memory; only the small completion record needs one.
        obs.put(store, key, path, use_multipart=True, max_concurrency=4)
        return f"{base_url}/{quote(key, safe='/')}"

    for path in sorted(product.rglob("*")):
        if path.is_file():
            upload(path, f"{product.name}/{path.relative_to(product).as_posix()}")
    record = {
        "title": product.name,
        "source_datapoint_id": source_id,
        "pipeline_version": PIPELINE_VERSION,
        "sen2cor_version": SEN2COR_VERSION,
        "product_url": f"{base_url}/{quote(prefix + '/' + product.name, safe='/')}/",
        "metadata_url": f"{base_url}/{quote(prefix + '/' + product.name, safe='/')}/MTD_MSIL2A.xml",
        "ndvi_url": upload(ndvi, "ndvi.tif"),
        "thumbnail_url": upload(thumbnail, "thumbnail.png"),
    }
    try:
        obs.put(
            store,
            completion_key(source_id),
            json.dumps(record, sort_keys=True).encode(),
            mode="create",
        )
    except AlreadyExistsError:
        # Another attempt won. Register its immutable metadata, not this attempt's URLs.
        return read_completion(store, source_id)
    return record
