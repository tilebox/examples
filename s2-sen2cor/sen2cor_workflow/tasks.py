import os
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
from typing import cast

from shapely.geometry import box
from tilebox.datasets import Client
from tilebox.datasets.datapoints import iter_datapoints
from tilebox.storage import CopernicusStorageClient
from tilebox.workflows import ExecutionContext, Task

from sen2cor_workflow.catalog import register
from sen2cor_workflow.processing import correct, derive
from sen2cor_workflow.results import open_store, publish, read_completion

SOURCE_DATASET = "open_data.copernicus.sentinel2_msi"


class ProcessArea(Task):
    """Select L1C scenes by area, acquisition time, and cloud cover."""

    start: str
    end: str
    bounds: tuple[float, float, float, float]
    source_collection: str = "S2A_S2MSI1C"
    max_cloud_cover: float = 20.0
    max_scenes: int = 1

    def execute(self, context: ExecutionContext) -> None:
        """Query matching scenes and submit up to max_scenes correction tasks."""
        if self.max_scenes < 1 or not 0 <= self.max_cloud_cover <= 100:
            raise ValueError("max_scenes must be positive and max_cloud_cover must be in [0, 100]")
        west, south, east, north = self.bounds
        if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
            raise ValueError("Expected WGS84 west, south, east, north bounds without antimeridian crossing")
        context.logger.info(
            "Querying L1C products",
            collection=self.source_collection,
            start=self.start,
            end=self.end,
            bounds=self.bounds,
            max_cloud_cover=self.max_cloud_cover,
            max_scenes=self.max_scenes,
        )
        scenes = (
            Client()
            .dataset(SOURCE_DATASET)
            .collection(self.source_collection)
            .query(
                temporal_extent=(self.start, self.end),
                spatial_extent=box(*self.bounds),
            )
        )
        matched = scenes.sizes.get("time", 0)
        if matched == 0:
            context.logger.info("No matching L1C products")
            return
        # Copernicus exposes cloud_cover, but does not mark it queryable server-side.
        scenes = scenes.isel(time=scenes.cloud_cover <= self.max_cloud_cover)
        cloud_filtered = scenes.sizes["time"]
        scenes = scenes.sortby("time").isel(time=slice(0, self.max_scenes))
        context.logger.info(
            "Selected L1C products",
            matched=matched,
            passing_cloud_filter=cloud_filtered,
            selected=scenes.sizes["time"],
        )
        if scenes.sizes["time"] == 0:
            context.logger.info("No L1C products pass the cloud filter")
            return
        context.submit_subtasks(
            [
                ProcessScene(source_id=str(scene.id.item()), source_collection=self.source_collection)
                for scene in iter_datapoints(scenes)
            ],
            max_retries=2,
        )
        context.logger.info("Submitted scene tasks", count=scenes.sizes["time"], max_retries=2)


class ProcessScene(Task):
    """Correct one L1C scene and register its stored outputs."""

    source_id: str
    source_collection: str

    def execute(self, context: ExecutionContext) -> None:
        """Process and publish the scene, reusing completed outputs on retry."""
        started = monotonic()
        context.logger.info("Starting scene", source_id=self.source_id, collection=self.source_collection)
        source = Client().dataset(SOURCE_DATASET).collection(self.source_collection).find(self.source_id)
        base_url = os.environ.get("RESULTS_STORAGE_URL", Path("outputs/results").resolve().as_uri())
        with open_store(base_url) as store:
            record = read_completion(store, self.source_id)
            if record is None:
                with TemporaryDirectory(prefix="sen2cor-", dir=os.environ.get("WORK_DIR")) as scratch:
                    root = Path(scratch)
                    storage = CopernicusStorageClient(
                        access_key=os.environ["CDSE_ACCESS_KEY"],
                        secret_access_key=os.environ["CDSE_SECRET_KEY"],
                        cache_directory=None,
                    )
                    context.logger.info("Downloading L1C SAFE", source_id=self.source_id)
                    phase = monotonic()
                    # The sync client wraps download at runtime but retains its async type annotation.
                    input_safe = cast(Path, storage.download(source, output_dir=root / "input", show_progress=False))
                    context.logger.info(
                        "L1C download complete", source_id=self.source_id, seconds=round(monotonic() - phase, 2)
                    )
                    phase = monotonic()
                    context.logger.info("Running Sen2Cor", source_id=self.source_id)
                    product = correct(input_safe, root / "output")
                    context.logger.info(
                        "Sen2Cor complete",
                        source_id=self.source_id,
                        product=product.name,
                        seconds=round(monotonic() - phase, 2),
                    )
                    phase = monotonic()
                    context.logger.info("Deriving NDVI and RGB", source_id=self.source_id)
                    ndvi_path, thumbnail = derive(product, root / "derived")
                    context.logger.info(
                        "Derived products complete", source_id=self.source_id, seconds=round(monotonic() - phase, 2)
                    )
                    phase = monotonic()
                    context.logger.info("Publishing result assets", source_id=self.source_id)
                    record = publish(store, base_url, self.source_id, product, ndvi_path, thumbnail)
                    context.logger.info(
                        "Publication complete", source_id=self.source_id, seconds=round(monotonic() - phase, 2)
                    )
            else:
                context.logger.info("Reusing completed result", source_id=self.source_id, product=record["title"])
            context.logger.info(
                "Registering L2A metadata", source_id=self.source_id, dataset=os.environ["RESULTS_DATASET"]
            )
            register(os.environ["RESULTS_DATASET"], source, record)
            context.logger.info(
                f"L2A result registered; NDVI: {record['assets']['ndvi']['href']}",
                source_id=self.source_id,
                product=record["title"],
                seconds=round(monotonic() - started, 2),
            )
