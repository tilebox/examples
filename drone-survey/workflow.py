"""Prepare arriving drone photos and reconstruct completed surveys on local runners."""

import hashlib
import io
import json
import os
import shutil
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from urllib.parse import urlparse
from uuid import uuid4

import cv2
import numpy as np
from PIL import ExifTags, Image, ImageOps
from pyodm import Node
from tilebox.workflows import ExecutionContext, Runner
from tilebox.workflows.automations import StorageEventTask


def validate_output_dir(directory: str) -> None:
    if not Path(directory).is_absolute():
        raise ValueError(
            "output_dir must be absolute; release runners use their own working directory"
        )


def prepare_photo(data: bytes, source_key: str, output_dir: str) -> dict:
    """Decode a complete JPEG, measure it, and write an EXIF-oriented preview and report."""
    validate_output_dir(output_dir)
    with Image.open(io.BytesIO(data)) as image:
        # Sony survey JPGs contain MPF metadata; Pillow opens them as MPO.
        # Process the primary JPEG frame, not the embedded secondary images.
        if image.format not in ("JPEG", "MPO"):
            raise ValueError("The photo demo accepts JPEG images only")
        image.load()  # Reject truncated images before emitting any outputs.
        exif = image.getexif()
        details = exif.get_ifd(ExifTags.IFD.Exif)
        gps = exif.get_ifd(ExifTags.IFD.GPSInfo)
        position = None
        if all(key in gps for key in (1, 2, 3, 4)):
            lat = sum(float(value) / divisor for value, divisor in zip(gps[2], (1, 60, 3600)))
            lon = sum(float(value) / divisor for value, divisor in zip(gps[4], (1, 60, 3600)))
            lat *= -1 if gps[1] in ("S", b"S") else 1
            lon *= -1 if gps[3] in ("W", b"W") else 1
            position = {"type": "Point", "coordinates": [lon, lat]}

        oriented = ImageOps.exif_transpose(image).convert("RGB")
        analysis = oriented.copy()
        analysis.thumbnail((1600, 1600))
        gray = cv2.cvtColor(np.asarray(analysis), cv2.COLOR_RGB2GRAY)
        checksum = hashlib.sha256(data).hexdigest()
        object_id = hashlib.sha256(f"{source_key}:{checksum}".encode()).hexdigest()
        destination = Path(output_dir) / "photos" / object_id
        report = {
            "source_key": source_key,
            "sha256": checksum,
            "width": oriented.width,
            "height": oriented.height,
            "camera": str(exif.get(272, "unknown")),
            # Preserve camera time and its optional offset; do not assume UTC.
            "capture_time": details.get(36867),
            "capture_time_offset": details.get(36881),
            "geometry": position,
            "analysis_size": list(analysis.size),
            "laplacian_variance": float(cv2.Laplacian(gray, cv2.CV_64F).var()),
            "dark_pixel_fraction": float(np.mean(gray <= 5)),
            "bright_pixel_fraction": float(np.mean(gray >= 250)),
            "preview_path": str(destination / "preview.jpg"),
        }
        oriented.thumbnail((960, 960))
        preview = io.BytesIO()
        oriented.save(preview, format="JPEG", quality=85)
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "preview.jpg").write_bytes(preview.getvalue())
        (destination / "metadata.json").write_text(json.dumps(report, indent=2))
        return report


def nodeodm() -> Node:
    url = urlparse(os.environ.get("NODEODM_URL", "http://127.0.0.1:3000"))
    if (
        url.scheme not in ("http", "https")
        or not url.hostname
        or url.path not in ("", "/")
        or url.query
        or url.fragment
        or url.username
        or url.password
    ):
        raise ValueError("NODEODM_URL must be an http(s) origin without a path or credentials")
    port = url.port or (443 if url.scheme == "https" else 80)
    # PyODM 1.5.13 selects HTTPS only on port 443, regardless of the host string.
    if (url.scheme == "https") != (port == 443):
        raise ValueError("PyODM requires HTTPS on port 443; use HTTP for local NodeODM")
    host = f"[{url.hostname}]" if ":" in url.hostname else url.hostname
    return Node(host, port, token=os.environ.get("NODEODM_TOKEN", ""), timeout=120)


class PreparePhoto(StorageEventTask):
    output_dir: str

    @staticmethod
    def identifier() -> tuple[str, str]:
        return "drone-survey/PreparePhoto", "v1.0"

    def execute(self, context: ExecutionContext) -> None:
        key = self.trigger.location
        context.current_task.display = f"Prepare {PurePosixPath(key).name}"
        report = prepare_photo(self.trigger.storage.read(key), key, self.output_dir)
        context.logger.info("Photo prepared", **report)


class ReconstructSurvey(StorageEventTask):
    """Submit manually for a manifest after the survey copy is complete, with zero retries."""

    output_dir: str

    @staticmethod
    def identifier() -> tuple[str, str]:
        return "drone-survey/ReconstructSurvey", "v1.0"

    def execute(self, context: ExecutionContext) -> None:
        validate_output_dir(self.output_dir)
        manifest_bytes = self.trigger.storage.read(self.trigger.location)
        images = json.loads(manifest_bytes)["images"]
        if not isinstance(images, list) or len(images) < 3:
            raise ValueError("Survey manifest needs at least three overlapping image keys")
        if any(
            not isinstance(key, str)
            or not key
            or PurePosixPath(key).is_absolute()
            or ".." in PurePosixPath(key).parts
            or "\\" in key
            or PurePosixPath(key).suffix.lower() != ".jpg"
            for key in images
        ):
            raise ValueError("Survey image keys must be relative JPG paths without traversal")
        if len({str(PurePosixPath(key)) for key in images}) != len(images):
            raise ValueError("Survey manifest must contain unique image keys")

        node = nodeodm()
        survey_id = hashlib.sha256(manifest_bytes).hexdigest()
        destination = Path(self.output_dir) / "surveys" / survey_id
        destination.mkdir(parents=True, exist_ok=True)
        receipt = destination / "nodeodm-task.json"
        # Reserve the ID before upload so failures during create_task remain recoverable.
        # Exclusive creation prevents a repeated submission from starting another task.
        task_id = str(uuid4())
        with receipt.open("x") as file:
            json.dump({"task_id": task_id, "manifest": self.trigger.location}, file, indent=2)
        context.logger.info(
            "OpenDroneMap task reserved", nodeodm_task_id=task_id, receipt=str(receipt)
        )
        context.current_task.display = f"Reconstruct {len(images)} survey photos"
        with TemporaryDirectory(prefix="drone-survey-") as temporary:
            root = Path(temporary)
            paths = []
            for index, key in enumerate(images):
                path = root / f"{index:04d}-{PurePosixPath(key).name}"
                path.write_bytes(self.trigger.storage.read(key))
                paths.append(str(path))
            # This is a real OpenDroneMap call, not simulated processing.
            # Do not retry this Tilebox task automatically: create_task is not idempotent.
            task = node.create_task(
                paths,
                {"fast-orthophoto": True, "orthophoto-resolution": 5, "max-concurrency": 2},
                name="Tilebox drone survey",
                parallel_uploads=2,
                max_retries=0,
                task_uuid=task_id,
            )
            context.logger.info("OpenDroneMap task created", nodeodm_task_id=task.uuid)
            task.wait_for_completion(
                interval=5,
                status_callback=lambda info: context.logger.info(
                    "OpenDroneMap progress", status=info.status.name, progress=info.progress
                ),
            )
            assets = Path(task.download_assets(str(root / "assets")))
            orthophoto = assets / "odm_orthophoto" / "odm_orthophoto.tif"
            if not orthophoto.is_file():
                raise RuntimeError("OpenDroneMap completed without an orthophoto")
            for path in (orthophoto, assets / "odm_report" / "report.pdf"):
                if path.is_file():
                    shutil.copyfile(path, destination / path.name)
            context.logger.info("Survey reconstructed", output_dir=str(destination))


runner = Runner(tasks=[PreparePhoto, ReconstructSurvey])
