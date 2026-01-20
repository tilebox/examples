import math
import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import boto3
import numpy as np
import torch
import xarray as xr
import yaml
import zarr
from botocore.config import Config
from box import Box
from claymodel.module import ClayMAEModule
from cyclopts import App
from dotenv import load_dotenv
from odc.geo.geobox import GeoBox
from sklearn.metrics.pairwise import cosine_distances
from tilebox.workflows import Client as WorkflowsClient
from tilebox.workflows import ExecutionContext, Task
from tilebox.workflows.observability.logging import configure_console_logging, configure_otel_logging_axiom, get_logger
from tilebox.workflows.observability.tracing import configure_otel_tracing_axiom
from torchvision.transforms.v2 import Normalize, Transform

from sentinel2zarr import (
    COMPRESSOR,
    OUTPUT_BUCKET,
    Chunk2D,
    OTCBucketCache,
    RegionOfInterest,
    open_zarr_store,
)

logger = get_logger()

CLAY_INFERENCE_TILE_SIZE = 256  # input tile size for the model is 256x256 pixels
CLAY_PATCH_SIZE = 8  # the model computes embeddings for 8x8 patches within each tile
CLAY_EMBEDDING_DIM = 1024  # embedding dimensionality of the model

# wget -q https://huggingface.co/made-with-clay/Clay/resolve/main/v1.5/clay-v1.5.ckpt
_CLAY_CHECKPOINT = Path(__file__).parent / "clay-v1.5.ckpt"
_CLAY_METADATA = Path(__file__).parent / "configs/metadata.yaml"
_CLAY_PLATFORM = "sentinel-2-l2a"


@lru_cache
def device() -> torch.device:
    if torch.cuda.is_available():
        logger.info("CUDA is available, using GPU")
        return torch.device("cuda:0")  # use the GPU if available
    if torch.backends.mps.is_available():
        logger.info("MPS is available, using Mac GPU")
        return torch.device("mps:0")  # use the GPU if available
    logger.info("CUDA is not available, falling back to CPU")
    return torch.device("cpu")  # otherwise fall back to CPU


@lru_cache
def clay_model() -> ClayMAEModule:
    """Load the Clay model weights into memory"""
    logger.info("Loading Clay model weights into memory")
    model = ClayMAEModule.load_from_checkpoint(
        _CLAY_CHECKPOINT,
        model_size="large",
        metadata_path=_CLAY_METADATA.as_posix(),
        dolls=[16, 32, 64, 128, 256, 768, 1024],
        doll_weights=[1, 1, 1, 1, 1, 1, 1],
        mask_ratio=0.0,
        shuffle=False,
    )
    return model.to(device()).eval()


class ClayInferenceOnMosaic(Task):
    mosaic_zarr_group: str
    """Path to the zarr group containing the mosaic to run inference on. The group is expected to have a "mosaic" array
    with the shape (band, y, x) and a "band" array with the shape (band,) containing the band names as strings.
    """

    roi: RegionOfInterest
    """The region of interest that the mosaic was computed for"""

    crs: str
    """The CRS of the mosaic"""

    resolution: float
    """The resolution of the mosaic in units of the CRS"""

    output_zarr: tuple[str, str]
    """The path to the output zarr group and the name of the output array"""

    def execute(self, context: ExecutionContext) -> None:
        geobox = self.roi.area.as_geobox(self.crs, self.resolution)

        output_group, output_array = self.output_zarr
        store = open_zarr_store(output_group)
        zarr.create_array(
            store=store,
            name=output_array,
            shape=(geobox.shape.y // CLAY_PATCH_SIZE, geobox.shape.x // CLAY_PATCH_SIZE, CLAY_EMBEDDING_DIM),
            chunks=(
                CLAY_INFERENCE_TILE_SIZE // CLAY_PATCH_SIZE,  # 32
                CLAY_INFERENCE_TILE_SIZE // CLAY_PATCH_SIZE,  # 32
                CLAY_EMBEDDING_DIM,  # 1024
            ),
            dimension_names=("y", "x", "embedding"),
            compressors=COMPRESSOR,
            dtype=np.float32,
            overwrite=True,
        )

        chunks = self.roi.area.chunks(self.crs, self.resolution, (CLAY_INFERENCE_TILE_SIZE, CLAY_INFERENCE_TILE_SIZE))
        for chunk in chunks:
            context.submit_subtask(
                ClayInferenceTile(
                    chunk,
                    self.mosaic_zarr_group,
                    self.roi,
                    self.crs,
                    self.resolution,
                    self.output_zarr,
                ),
            )
        context.progress("inference").add(len(chunks))


@lru_cache
def open_dataset(group: str) -> xr.Dataset:
    zarr_store = open_zarr_store(group)
    return xr.open_zarr(zarr_store, zarr_format=3, consolidated=False)


def get_tile_center_coordiante(geobox: GeoBox, chunk: Chunk2D) -> tuple[float, float]:
    tile = geobox[chunk.y_start : chunk.y_end, chunk.x_start : chunk.x_end]
    center_coord = tile.to_crs("EPSG:4326").center_pixel.coordinates
    lat = center_coord["latitude"].values[0].item()
    lon = center_coord["longitude"].values[0].item()
    return lat, lon


def normalize_latlon(lat: float, lon: float) -> tuple[tuple[float, float], tuple[float, float]]:
    lat = lat * np.pi / 180
    lon = lon * np.pi / 180

    return (math.sin(lat), math.cos(lat)), (math.sin(lon), math.cos(lon))


def normalize_timestamp(date: datetime) -> tuple[tuple[float, float], tuple[float, float]]:
    week = date.isocalendar().week * 2 * np.pi / 52
    hour = date.hour * 2 * np.pi / 24

    return (math.sin(week), math.cos(week)), (math.sin(hour), math.cos(hour))


def load_transform(bands: list[str], platform: str) -> tuple[Transform, list[float]]:
    with _CLAY_METADATA.open("r") as f:
        metadata = Box(yaml.safe_load(f))[platform]

    mean = [metadata.bands.mean[band] for band in bands]
    std = [metadata.bands.std[band] for band in bands]
    wavelength = [metadata.bands.wavelength[band] for band in bands]

    return Normalize(mean, std), wavelength


class ClayInferenceTile(Task):
    chunk: Chunk2D
    mosaic_zarr_group: str
    roi: RegionOfInterest
    crs: str
    resolution: float
    output_zarr: tuple[str, str]

    def execute(self, context: ExecutionContext) -> None:
        tracer = context._runner.tracer._tracer  # type: ignore[arg-defined], # noqa: SLF001
        context.current_task.display = f"ClayInferenceTile({self.chunk})"  # type: ignore[attr-defined]

        with tracer.start_span("load_data"):
            start, end = self.roi.time
            start = datetime.fromisoformat(start)
            end = datetime.fromisoformat(end)
            mean_time = start + (end - start) / 2
            # the model takes the time of day into account, since we have a mosaic of lots of images we set it to noon
            # as an approximation of the middle of the day
            mean_time = mean_time.replace(hour=12, minute=0)
            lat, lon = get_tile_center_coordiante(self.roi.area.as_geobox(self.crs, self.resolution), self.chunk)

            week_norm, hour_norm = normalize_timestamp(mean_time)
            lat_norm, lon_norm = normalize_latlon(lat, lon)

            logger.info(f"Inference for tile {self.chunk} at lat={lat:.4f}, lon={lon:.4f} on {mean_time.isoformat()}")

            cube = open_dataset(self.mosaic_zarr_group)
            bands = [s.item().decode("utf-8") for s in cube.band]
            transform, wavelengths = load_transform(bands, _CLAY_PLATFORM)

            mosaic = cube.mosaic.isel(
                y=slice(self.chunk.y_start, self.chunk.y_end), x=slice(self.chunk.x_start, self.chunk.x_end)
            )

            data = mosaic.load().to_numpy()
            # add a batch size
            data = np.expand_dims(data, axis=0)
            # convert to a contiguous array in float32
            data = np.ascontiguousarray(data.astype(np.float32))
            pixels = transform(torch.from_numpy(data))
            logger.info("Successfully loaded pixels")

            model_input = {
                "platform": _CLAY_PLATFORM,
                "time": torch.tensor(
                    np.hstack((week_norm, hour_norm)).reshape(1, 4),
                    dtype=torch.float32,
                    device=device(),
                ),
                "latlon": torch.tensor(
                    np.hstack((lat_norm, lon_norm)).reshape(1, 4), dtype=torch.float32, device=device()
                ),
                "pixels": pixels.to(device()),
                "gsd": torch.tensor([self.resolution], device=device()),
                "waves": torch.tensor(wavelengths, device=device()),
            }

        with tracer.start_span("load_model"):
            model = clay_model()

        with tracer.start_span("inference"), torch.no_grad():
            unmsk_patch, _, _, _ = model.model.encoder(model_input)
            patches = unmsk_patch.detach().cpu().numpy()[0, 1:, :]
            patches = patches.reshape(  # 32, 32, 1024
                CLAY_INFERENCE_TILE_SIZE // CLAY_PATCH_SIZE,
                CLAY_INFERENCE_TILE_SIZE // CLAY_PATCH_SIZE,
                CLAY_EMBEDDING_DIM,
            )

        with tracer.start_span("write_output"):
            zarr_group_name, zarr_array_name = self.output_zarr
            zarr_group = zarr.open_group(open_zarr_store(zarr_group_name), mode="a")
            zarr_array: zarr.Array = zarr_group[zarr_array_name]  # type: ignore[arg-type]
            zarr_array[
                self.chunk.y_start // CLAY_PATCH_SIZE : self.chunk.y_end // CLAY_PATCH_SIZE,
                self.chunk.x_start // CLAY_PATCH_SIZE : self.chunk.x_end // CLAY_PATCH_SIZE,
                :,
            ] = patches

        logger.info(f"Successfully wrote patches to Zarr array for tile {self.chunk}")

        context.progress("inference").done(1)


class ComputeEmbeddingDelta(Task):
    input_zarr_1: tuple[str, str]
    input_zarr_2: tuple[str, str]
    output_zarr: tuple[str, str]

    def execute(self, context: ExecutionContext) -> None:
        array1 = open_dataset(self.input_zarr_1[0])[self.input_zarr_1[1]]
        array2 = open_dataset(self.input_zarr_2[0])[self.input_zarr_2[1]]

        if array1.shape != array2.shape:
            raise ValueError(f"Array shapes do not match: {array1.shape} != {array2.shape}")

        ny, nx, _ = array1.shape
        chunk_size = CLAY_INFERENCE_TILE_SIZE // CLAY_PATCH_SIZE  # 32

        output_group, output_array = self.output_zarr
        store = open_zarr_store(output_group)
        zarr.create_array(
            store=store,
            name=output_array,
            shape=(ny, nx),
            chunks=(chunk_size, chunk_size),
            dimension_names=("y", "x"),
            compressors=COMPRESSOR,
            dtype=np.float32,
            overwrite=True,
        )

        chunks = Chunk2D(0, ny, 0, nx).sub_chunks(
            CLAY_INFERENCE_TILE_SIZE // CLAY_PATCH_SIZE, CLAY_INFERENCE_TILE_SIZE // CLAY_PATCH_SIZE
        )
        for chunk in chunks:
            context.submit_subtask(
                ComputeEmbeddingDeltaTile(
                    chunk,
                    self.input_zarr_1,
                    self.input_zarr_2,
                    self.output_zarr,
                )
            )
        context.progress("compute-delta").add(len(chunks))


class ComputeEmbeddingDeltaTile(Task):
    chunk: Chunk2D
    input_zarr_1: tuple[str, str]
    input_zarr_2: tuple[str, str]
    output_zarr: tuple[str, str]

    def execute(self, context: ExecutionContext) -> None:
        array1 = open_dataset(self.input_zarr_1[0])[self.input_zarr_1[1]]
        array2 = open_dataset(self.input_zarr_2[0])[self.input_zarr_2[1]]

        chunk = self.chunk
        patches1 = array1[chunk.y_start : chunk.y_end, chunk.x_start : chunk.x_end, :].to_numpy()
        patches2 = array2[chunk.y_start : chunk.y_end, chunk.x_start : chunk.x_end, :].to_numpy()

        if patches1.shape != patches2.shape:
            raise ValueError(f"Array shapes do not match: {patches1.shape} != {patches2.shape}")

        ny, nx, n_embedding = patches1.shape
        cosine_distance_matrix = cosine_distances(
            patches1.reshape(ny * nx, n_embedding), patches2.reshape(ny * nx, n_embedding)
        )
        # the diagonal of our matrix contains the values we are interested in
        delta = cosine_distance_matrix[np.diag_indices(ny * nx)].reshape(ny, nx)

        output_group, output_array = self.output_zarr
        zarr_group = zarr.open_group(open_zarr_store(output_group), mode="a")
        zarr_array: zarr.Array = zarr_group[output_array]  # type: ignore[arg-type]
        zarr_array[chunk.y_start : chunk.y_end, chunk.x_start : chunk.x_end] = delta

        context.progress("compute-delta").done(1)


app = App()


@app.default
def main(cluster: str | None = None, preload_model: bool = False) -> None:
    if Path(".env").exists():
        assert load_dotenv()

    service_name = f"{os.environ['RUNNER_NAME']}-{os.getpid()}"
    configure_console_logging()
    if os.environ.get("AXIOM_API_KEY"):
        configure_otel_logging_axiom(service_name)
        configure_otel_tracing_axiom(service_name)

    client = WorkflowsClient()  # a workflow client for https://api.tilebox.com

    cache_client = boto3.client(
        "s3",
        endpoint_url="https://obs.eu-nl.otc.t-systems.com",
        aws_access_key_id=os.environ["OTC_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["OTC_SECRET_ACCESS_KEY"],
        region_name="eu-nl",
        # without this boto will append x-amz-checksum-crc32:... to the contents of uploaded blobs
        config=Config(request_checksum_calculation="when_required", response_checksum_validation="when_required"),
    )
    cache = OTCBucketCache(OUTPUT_BUCKET, cache_client, prefix="cache/jobs")

    logger.info(f"Starting runner on {cluster or 'default'} cluster")

    if preload_model:
        # preload the model weights into memory
        logger.info("Preloading model weights into memory")
        clay_model()
        logger.info("Model weights preloaded")

    runner = client.runner(
        cluster,
        tasks=[ClayInferenceOnMosaic, ClayInferenceTile],
        cache=cache,
    )
    runner.run_forever()


if __name__ == "__main__":
    app()
