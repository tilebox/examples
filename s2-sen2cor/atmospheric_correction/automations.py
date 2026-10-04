import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
from uuid import uuid4

from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient, ContentSettings
from rasterio.shutil import copy as copy_raster  # ty: ignore[unresolved-import]
from tilebox.datasets import Client as DatasetsClient
from tilebox.workflows import ExecutionContext, Task
from tilebox.workflows.automations import StorageEventTask
from tilebox.workflows.data import AzureStorageLocation

from atmospheric_correction.catalog import l2a_metadata_row, ndvi_metadata_row
from atmospheric_correction.ndvi import derive_ndvi, ndvi_bands, ndvi_preview
from atmospheric_correction.processing import correct, rgb_preview
from atmospheric_correction.storage import PREFIX, publish, scene_key
from atmospheric_correction.tasks import download_scene


@contextmanager
def container(name: str | None = None) -> Iterator[ContainerClient]:
    """Open an Azure container with the worker's credentials and close it after use."""
    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    with (
        DefaultAzureCredential() as credential,
        ContainerClient(
            f"https://{account}.blob.core.windows.net",
            name or os.environ["AZURE_STORAGE_CONTAINER"],
            credential=credential,
        ) as client,
    ):
        yield client


class CorrectAndUpload(Task):
    """Correct locally, upload and catalog L2A inputs, then publish the completion marker."""

    source_id: str
    source: tuple[str, str]
    destination: tuple[str, str] = ("tilebox.sentinel2_l2a", "S2A_L2A")
    ndvi_collection: str = "S2A_NDVI"

    def execute(self, context: ExecutionContext) -> None:  # noqa: PLR0915 - keep the processing stages in execution order
        """Upload and catalog corrected bands before triggering NDVI with a .ready marker."""
        started = monotonic()
        log = context.logger.bind(source_id=self.source_id)
        context.current_task.display = f"Correct and upload {self.source_id}"
        log.info("Starting L2A correction task")
        with container() as target, container(os.environ["AZURE_PREVIEW_CONTAINER"]) as previews:
            key = f"{PREFIX}/l2a/{self.source_id}"
            client = DatasetsClient()
            if target.get_blob_client(f"{key}.ready").exists():
                log.info("L2A inputs already uploaded; registering catalog record without rerunning correction")
                marker = json.loads(target.download_blob(f"{key}.ready").readall())
                scene = client.dataset(marker["source"][0]).collection(marker["source"][1]).find(self.source_id)
                rgb_url = f"{previews.url}/{marker['l2a_preview']}" if "l2a_preview" in marker else None
                with context.tracer.span("ingest-l2a"):
                    client.dataset(marker["destination"][0]).collection(marker["destination"][1]).ingest(
                        l2a_metadata_row(scene, f"{target.url}/{key}", rgb_url), allow_existing=True
                    )
                log.info("L2A cataloged; existing marker left unchanged")
                context.progress("scenes").done(1)
                return
            scene = client.dataset(self.source[0]).collection(self.source[1]).find(self.source_id)
            with TemporaryDirectory() as directory:
                root = Path(directory)
                with context.tracer.span("download-l1c"):
                    log.info("Downloading L1C SAFE from Copernicus")
                    product = download_scene(scene, cache_directory=root / "cache", context=context)
                    log.info("L1C download complete")
                with context.tracer.span("sen2cor-20m"):
                    log.info("Running Sen2Cor at 20 m; this can take several minutes")
                    corrected = correct(product, root / "corrected")
                    log.info("Sen2Cor complete")
                for name, band in zip(("B04", "B8A", "SCL"), ndvi_bands(corrected), strict=True):
                    cog = root / f"{name}.tif"
                    with context.tracer.span(f"cog-{name}"):
                        log.info("Converting band to COG", band=name)
                        copy_raster(band, cog, driver="COG", compress="DEFLATE", overview_resampling="NEAREST")
                    with context.tracer.span(f"upload-{name}"):
                        log.info("Uploading band", band=name, bytes=cog.stat().st_size)
                        publish(
                            target,
                            f"{key}/{name}.tif",
                            cog,
                            content_settings=ContentSettings(content_type="image/tiff"),
                        )
                        log.info("Band available in Azure", band=name)
                with context.tracer.span("upload-l2a-metadata"):
                    log.info("Uploading L2A metadata")
                    publish(
                        target,
                        f"{key}/MTD_MSIL2A.xml",
                        corrected / "MTD_MSIL2A.xml",
                        content_settings=ContentSettings(content_type="application/xml"),
                    )
                preview_id = str(uuid4())
                preview_key = f"l2a/{preview_id}/rgb.png"
                with context.tracer.span("publish-l2a-preview"):
                    log.info("Creating and uploading L2A RGB preview")
                    publish(
                        previews,
                        preview_key,
                        rgb_preview(corrected, root / "rgb.png"),
                        content_settings=ContentSettings(content_type="image/png"),
                    )
                with context.tracer.span("ingest-l2a"):
                    log.info("Registering L2A catalog record", collection=self.destination[1])
                    client.dataset(self.destination[0]).collection(self.destination[1]).ingest(
                        l2a_metadata_row(scene, f"{target.url}/{key}", f"{previews.url}/{preview_key}"),
                        allow_existing=True,
                    )
                marker = root / "complete.ready"
                marker.write_text(
                    json.dumps(
                        {
                            "source": self.source,
                            "destination": self.destination,
                            "ndvi_collection": self.ndvi_collection,
                            "preview_id": preview_id,
                            "l2a_preview": preview_key,
                        }
                    )
                )
                with context.tracer.span("publish-ready"):
                    log.info("Publishing completion marker to trigger NDVI")
                    publish(target, f"{key}.ready", marker)
            context.progress("scenes").done(1)
            log.info("NDVI inputs uploaded", blob=f"{key}.ready", seconds=round(monotonic() - started, 2))


class CalculateNDVI(StorageEventTask):
    """Turn an L2A completion event into NDVI assets and a catalog record."""

    @staticmethod
    def identifier() -> tuple[str, str]:
        """Return the task name and version used to register the NDVI automation."""
        return "tilebox.com/examples/s2-events/ndvi", "v1.0"

    def execute(self, context: ExecutionContext) -> None:
        """Read the completed scene's inputs, publish NDVI and its preview, and ingest their metadata."""
        location = self.trigger.storage
        if (
            not isinstance(location, AzureStorageLocation)
            or location.location != os.environ["AZURE_STORAGE_CONTAINER"]
            or location.storage_account_resource_id.rsplit("/", 1)[-1].lower()
            != os.environ["AZURE_STORAGE_ACCOUNT"].lower()
        ):
            raise ValueError("Event storage location does not match the configured Azure account and container")
        source_id = scene_key(self.trigger.location)
        started = monotonic()
        log = context.logger.bind(source_id=source_id)
        context.current_task.display = f"Calculate NDVI {source_id}"
        log.info("Starting NDVI task", marker=self.trigger.location)
        key = f"{PREFIX}/ndvi/{source_id}.tif"
        with container() as target, container(os.environ["AZURE_PREVIEW_CONTAINER"]) as previews:
            marker = json.loads(target.download_blob(self.trigger.location).readall())
            preview_key = f"ndvi/{marker['preview_id']}/preview.png"
            with TemporaryDirectory() as directory:
                root = Path(directory)
                ndvi = root / "ndvi.tif"
                if not target.get_blob_client(key).exists():
                    for name in ("B04.tif", "B8A.tif", "SCL.tif", "MTD_MSIL2A.xml"):
                        with context.tracer.span(f"download-{name}"), (root / name).open("wb") as output:
                            log.info("Downloading NDVI input", file=name)
                            target.download_blob(f"{PREFIX}/l2a/{source_id}/{name}").readinto(output)
                            log.info("NDVI input downloaded", file=name, bytes=output.tell())
                    with context.tracer.span("ndvi-20m"):
                        log.info("Calculating masked NDVI at 20 m")
                        bands = [root / f"{name}.tif" for name in ("B04", "B8A", "SCL")]
                        derive_ndvi(root, ndvi, bands=bands)
                        log.info("NDVI calculation complete")
                    with context.tracer.span("upload-ndvi"):
                        log.info("Uploading NDVI GeoTIFF", bytes=ndvi.stat().st_size)
                        publish(target, key, ndvi)
                else:
                    log.info("Reusing existing NDVI GeoTIFF")
                if not previews.get_blob_client(preview_key).exists():
                    # Read the published result, including after a concurrent attempt won.
                    with context.tracer.span("download-published-ndvi"), ndvi.open("wb") as result:
                        log.info("Downloading published NDVI for the preview")
                        target.download_blob(key).readinto(result)
                    with context.tracer.span("publish-ndvi-preview"):
                        log.info("Creating and uploading NDVI preview")
                        publish(
                            previews,
                            preview_key,
                            ndvi_preview(ndvi, root / "preview.png"),
                            content_settings=ContentSettings(content_type="image/png"),
                        )
                else:
                    log.info("Reusing existing NDVI preview")
            client = DatasetsClient()
            scene = client.dataset(marker["source"][0]).collection(marker["source"][1]).find(source_id)
            ndvi_url, preview_url = f"{target.url}/{key}", f"{previews.url}/{preview_key}"
            with context.tracer.span("ingest-ndvi"):
                log.info("Registering NDVI catalog record", collection=marker.get("ndvi_collection", "S2A_NDVI"))
                client.dataset(marker["destination"][0]).collection(marker.get("ndvi_collection", "S2A_NDVI")).ingest(
                    ndvi_metadata_row(scene, ndvi_url, preview_url),
                    allow_existing=True,
                )
            log.info("NDVI cataloged", url=ndvi_url, preview=preview_url, seconds=round(monotonic() - started, 2))
