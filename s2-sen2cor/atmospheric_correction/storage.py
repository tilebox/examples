"""Publish immutable objects so retries cannot replace a downstream task's input."""

from contextlib import suppress
from pathlib import Path, PurePosixPath
from uuid import UUID

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import ContainerClient, ContentSettings

PREFIX = "v1"


def scene_key(location: str) -> str:
    """Validate a completion marker's path and return its scene UUID."""
    path = PurePosixPath(location)
    if path.parent.as_posix() != f"{PREFIX}/l2a" or path.suffix != ".ready":
        raise ValueError(f"Unexpected L2A event: {location}")
    return str(UUID(path.stem))


def publish(
    container: ContainerClient, key: str, path: Path, *, content_settings: ContentSettings | None = None
) -> None:
    """Upload a file as a blob, leaving any existing blob unchanged."""
    # A concurrent attempt may already have published the complete object.
    with path.open("rb") as data, suppress(ResourceExistsError):
        container.upload_blob(key, data, overwrite=False, blob_type="BlockBlob", content_settings=content_settings)
