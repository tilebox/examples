"""Turn a native local-storage event into a thumbnail and JSON metadata."""

import hashlib
import io
import json
from pathlib import Path, PurePosixPath

from PIL import Image, ImageOps
from tilebox.workflows import ExecutionContext, Runner
from tilebox.workflows.automations import StorageEventTask


def make_thumbnail(data: bytes, source_key: str, output_dir: str) -> dict:
    """Decode a complete JPEG and write repeatable outputs outside the watched folder."""
    if not Path(output_dir).is_absolute():
        raise ValueError("output_dir must be absolute; release workers have their own directory")
    with Image.open(io.BytesIO(data)) as image:
        # Sony sample JPGs are MPO containers. Use their primary JPEG frame.
        if image.format not in ("JPEG", "MPO"):
            raise ValueError("Expected a JPEG image")
        image.load()  # Reject incomplete copies before writing any output.
        thumbnail = ImageOps.exif_transpose(image).convert("RGB")
        width, height = thumbnail.size
        thumbnail.thumbnail((480, 480))
        checksum = hashlib.sha256(data).hexdigest()
        identity = hashlib.sha256(f"{source_key}:{checksum}".encode()).hexdigest()[:12]
        destination = Path(output_dir) / f"{PurePosixPath(source_key).stem}-{identity}"
        report = {
            "source_key": source_key,
            "sha256": checksum,
            "width": width,
            "height": height,
            "thumbnail_width": thumbnail.width,
            "thumbnail_height": thumbnail.height,
        }
        destination.mkdir(parents=True, exist_ok=True)
        # Do not copy camera EXIF (including GPS) to the thumbnail.
        thumbnail.save(destination / "thumbnail.jpg", format="JPEG", quality=85, exif=b"")
        (destination / "metadata.json").write_text(json.dumps(report, indent=2) + "\n")
        return report


class MakeThumbnail(StorageEventTask):
    output_dir: str

    @staticmethod
    def identifier() -> tuple[str, str]:
        return "local-storage/MakeThumbnail", "v1.0"

    def execute(self, context: ExecutionContext) -> None:
        key = self.trigger.location
        context.current_task.display = f"Thumbnail {PurePosixPath(key).name}"
        make_thumbnail(self.trigger.storage.read(key), key, self.output_dir)
        context.logger.info("Thumbnail saved", source_key=key)


runner = Runner(tasks=[MakeThumbnail])
