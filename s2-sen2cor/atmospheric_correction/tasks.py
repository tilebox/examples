import os
import shutil
from pathlib import Path
from time import monotonic
from typing import cast

import xarray as xr
from shapely.geometry import box
from tilebox.datasets import Client
from tilebox.datasets.datapoints import iter_datapoints
from tilebox.storage import CopernicusStorageClient
from tilebox.workflows import ExecutionContext, Task

from atmospheric_correction.catalog import metadata_row, upload_rgb
from atmospheric_correction.processing import correct, rgb_preview


def select_scenes(scenes: xr.Dataset, max_cloud_cover: float, max_scenes: int) -> xr.Dataset:
    """Filter clouds before selecting the earliest scenes; NaN cloud values are excluded."""
    if scenes.sizes.get("time", 0) == 0:
        return scenes
    return scenes.isel(time=scenes.cloud_cover <= max_cloud_cover).sortby("time").isel(time=slice(0, max_scenes))


def download_scene(scene: xr.Dataset) -> Path:
    """Download a full SAFE, reusing the SDK's persistent per-file cache."""
    storage = CopernicusStorageClient(
        access_key=os.environ["CDSE_ACCESS_KEY"],
        secret_access_key=os.environ["CDSE_SECRET_KEY"],
        cache_directory=Path("outputs/cache").resolve(),
    )
    # The synchronous SDK wrapper retains the asynchronous method's type annotation.
    return cast("Path", storage.download(scene, show_progress=False))


class ProcessArea(Task):
    """Select L1C scenes and fan out independent atmospheric-correction tasks."""

    start: str
    end: str
    bounds: tuple[float, float, float, float]
    source: tuple[str, str] = ("open_data.copernicus.sentinel2_msi", "S2A_S2MSI1C")
    destination: tuple[str, str] = ("tilebox.sentinel2_l2a", "S2A_L2A")
    max_cloud_cover: float = 20.0
    max_scenes: int = 3

    def execute(self, context: ExecutionContext) -> None:
        """Query scenes, then submit one retryable task per selected scene."""
        if self.max_scenes < 1 or not 0 <= self.max_cloud_cover <= 100:
            raise ValueError("Expected positive max_scenes and cloud cover in [0, 100]")
        west, south, east, north = self.bounds
        if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
            raise ValueError("Expected WGS84 west, south, east, north bounds")
        context.current_task.display = "Select L1C scenes"
        with context.tracer.span("query-scenes"):
            scenes = (
                Client()
                .dataset(self.source[0])
                .collection(self.source[1])
                .query(
                    temporal_extent=(self.start, self.end),
                    spatial_extent=box(*self.bounds),
                )
            )
        selected = select_scenes(scenes, self.max_cloud_cover, self.max_scenes)
        count = selected.sizes.get("time", 0)
        context.logger.info(
            "Selected scenes",
            matched=scenes.sizes.get("time", 0),
            selected=count,
            scene_ids=[str(value) for value in selected.id.values] if count else [],
            granule_names=selected.granule_name.values.tolist() if count else [],
        )
        if not count:
            return
        context.progress("scenes").add(count)
        context.submit_subtasks(
            [
                ProcessScene(source_id=str(scene.id.item()), source=self.source, destination=self.destination)
                for scene in iter_datapoints(selected)
            ],
            max_retries=2,
        )


class ProcessScene(Task):
    """Download, correct, upload a preview, and ingest one scene's asset metadata."""

    source_id: str
    source: tuple[str, str]
    destination: tuple[str, str]

    def execute(self, context: ExecutionContext) -> None:
        """Keep large intermediate files local to the worker that consumes them."""
        started = monotonic()
        log = context.logger.bind(source_id=self.source_id)
        context.current_task.display = f"Correct {self.source_id}"
        client = Client()
        scene = client.dataset(self.source[0]).collection(self.source[1]).find(self.source_id)
        with context.tracer.span("download-l1c"):
            log.info("Downloading L1C SAFE (cached files are reused)")
            input_safe = download_scene(scene)
        # A retry reruns correction. No completion records or cross-job result reuse.
        output = Path("outputs/results").resolve() / str(context.current_task.job.id) / self.source_id
        if output.exists():
            shutil.rmtree(output)
        with context.tracer.span("sen2cor-20m"):
            log.info("Running Sen2Cor", input_safe=str(input_safe))
            product = correct(input_safe, output)
        with context.tracer.span("upload-rgb"):
            preview = rgb_preview(product, output / "rgb.png")
            rgb_url = upload_rgb(preview, f"atmospheric-correction/{product.name}/rgb.png")
            log.info("RGB preview uploaded", url=rgb_url)
        with context.tracer.span("ingest-l2a"):
            client.dataset(self.destination[0]).collection(self.destination[1]).ingest(
                metadata_row(scene, product, rgb_url),
                allow_existing=True,
            )
        context.progress("scenes").done(1)
        log.info("L2A registered", product=str(product), rgb_url=rgb_url, seconds=round(monotonic() - started, 2))
