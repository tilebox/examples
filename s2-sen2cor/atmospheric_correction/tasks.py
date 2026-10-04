import os
import shutil
from pathlib import Path
from time import monotonic

import xarray as xr
from shapely.geometry import box
from tilebox.datasets import Client
from tilebox.datasets.datapoints import iter_datapoints
from tilebox.storage import CopernicusStorageClient
from tilebox.workflows import ExecutionContext, Task

from atmospheric_correction.catalog import metadata_row, upload_rgb
from atmospheric_correction.ndvi import derive_ndvi
from atmospheric_correction.processing import correct, rgb_preview


def select_scenes(scenes: xr.Dataset, max_cloud_cover: float, max_scenes: int) -> xr.Dataset:
    """Filter clouds before selecting the earliest scenes; NaN cloud values are excluded."""
    if scenes.sizes.get("time", 0) == 0:
        return scenes
    return scenes.isel(time=scenes.cloud_cover <= max_cloud_cover).sortby("time").isel(time=slice(0, max_scenes))


def download_scene(
    scene: xr.Dataset, cache_directory: Path | None = None, *, context: ExecutionContext | None = None
) -> Path:
    """Download SAFE files sequentially, reusing cached files and logging progress when a task context is provided."""
    if cache_directory is None:
        cache_directory = Path(os.environ.get("S2_OUTPUT_DIRECTORY", "outputs")) / "cache"
    storage = CopernicusStorageClient(
        access_key=os.environ["CDSE_ACCESS_KEY"],
        secret_access_key=os.environ["CDSE_SECRET_KEY"],
        cache_directory=cache_directory.resolve(),
    )
    objects = storage.list_objects(scene)
    product = None
    for index, name in enumerate(objects, start=1):
        started = monotonic()
        if context is not None:
            context.logger.info(
                "Fetching L1C file (cached files are reused)", file=name, file_number=index, total=len(objects)
            )
        product = storage.download_objects(scene, [name], show_progress=False, max_concurrent_downloads=1)
        if context is not None:
            context.logger.info(
                "L1C file ready",
                file=name,
                completed=index,
                total=len(objects),
                bytes=(product / name).stat().st_size,
                seconds=round(monotonic() - started, 2),
            )
    if product is None:
        raise ValueError("No files found for the L1C scene")
    return product


class ProcessArea(Task):
    """Select L1C scenes and fan out independent atmospheric-correction tasks."""

    start: str
    end: str
    bounds: tuple[float, float, float, float]
    source: tuple[str, str] = ("open_data.copernicus.sentinel2_msi", "S2A_S2MSI1C")
    destination: tuple[str, str] = ("tilebox.sentinel2_l2a", "S2A_L2A")
    max_cloud_cover: float = 20.0
    max_scenes: int = 3
    event_driven: bool = False
    ndvi_collection: str = "S2A_NDVI"

    def execute(self, context: ExecutionContext) -> None:
        """Query scenes, then submit one retryable task per selected scene."""
        if self.max_scenes < 1 or not 0 <= self.max_cloud_cover <= 100:
            raise ValueError("Expected positive max_scenes and cloud cover in [0, 100]")
        west, south, east, north = self.bounds
        if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
            raise ValueError("Expected WGS84 west, south, east, north bounds")
        context.current_task.display = "Select L1C scenes"
        with context.tracer.span("query-scenes"):
            context.logger.info("Querying L1C scenes", start=self.start, end=self.end)
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
        if self.event_driven:
            # Import here because the automation wrappers reuse download_scene.
            from atmospheric_correction.automations import CorrectAndUpload  # noqa: PLC0415

            context.submit_subtasks(
                [
                    CorrectAndUpload(
                        source_id=str(scene.id.item()),
                        source=self.source,
                        destination=self.destination,
                        ndvi_collection=self.ndvi_collection,
                    )
                    for scene in iter_datapoints(selected)
                ],
                max_retries=2,
            )
            return
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
            input_safe = download_scene(scene, context=context)
            log.info("L1C download complete")
        # A retry reruns correction. No completion records or cross-job result reuse.
        output_root = Path(os.environ.get("S2_OUTPUT_DIRECTORY", "outputs")).resolve()
        output = output_root / "results" / str(context.current_task.job.id) / self.source_id
        if output.exists():
            shutil.rmtree(output)
        with context.tracer.span("sen2cor-20m"):
            log.info("Running Sen2Cor", input_safe=str(input_safe))
            product = correct(input_safe, output)
            log.info("Sen2Cor complete")
        with context.tracer.span("ndvi-20m"):
            log.info("Calculating masked NDVI at 20 m")
            ndvi = derive_ndvi(product, output / "ndvi.tif")
            log.info("NDVI calculated", path=str(ndvi))
        with context.tracer.span("upload-rgb"):
            log.info("Creating and uploading RGB preview")
            preview = rgb_preview(product, output / "rgb.png")
            rgb_url = upload_rgb(preview, f"atmospheric-correction/{product.name}/rgb.png")
            log.info("RGB preview uploaded", url=rgb_url)
        with context.tracer.span("ingest-l2a"):
            log.info("Registering L2A catalog record", collection=self.destination[1])
            client.dataset(self.destination[0]).collection(self.destination[1]).ingest(
                metadata_row(scene, product, rgb_url),
                allow_existing=True,
            )
        context.progress("scenes").done(1)
        log.info("L2A registered", product=str(product), rgb_url=rgb_url, seconds=round(monotonic() - started, 2))
