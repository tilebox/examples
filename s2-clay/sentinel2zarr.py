import os
import pickle
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import boto3
import numpy as np
import rasterio
import xarray as xr
import zarr
from botocore.config import Config
from cyclopts import App
from dotenv import load_dotenv
from numpy.typing import DTypeLike
from obstore.store import LocalStore, ObjectStore, S3Store
from odc.geo.geobox import GeoBox
from odc.geo.xr import wrap_xr
from pyproj import Transformer
from rasterio.enums import Resampling
from shapely import Polygon, box, transform
from tilebox.datasets import Client as DatasetClient
from tilebox.workflows import Client as WorkflowsClient
from tilebox.workflows import ExecutionContext, Task
from tilebox.workflows.cache import AmazonS3Cache
from tilebox.workflows.observability.logging import configure_console_logging, configure_otel_logging_axiom, get_logger
from tilebox.workflows.observability.tracing import configure_otel_tracing_axiom
from zarr.codecs import BloscCodec
from zarr.storage import ObjectStore as ZarrObjectStore

logger = get_logger()


@dataclass(frozen=True)
class S2Product:
    name: str
    native_resolution: int
    dtype: DTypeLike


_SCENE_CLASSIFICATION_PRODUCT = S2Product("scene_classification", 20, np.uint8)
_S2_BANDS = {
    "B02": S2Product("blue", 10, np.uint16),
    "B03": S2Product("green", 10, np.uint16),
    "B04": S2Product("red", 10, np.uint16),
    "B05": S2Product("rededge1", 20, np.uint16),
    "B06": S2Product("rededge2", 20, np.uint16),
    "B07": S2Product("rededge3", 20, np.uint16),
    "B08": S2Product("nir", 10, np.uint16),
    "B8A": S2Product("nir08", 20, np.uint16),
    "B11": S2Product("swir16", 20, np.uint16),
    "B12": S2Product("swir22", 20, np.uint16),
}
_S2_PRODUCTS = {
    **_S2_BANDS,
    "SCL": _SCENE_CLASSIFICATION_PRODUCT,
}
"""The S2 bands we are reading for each granule"""


OUTPUT_BUCKET = "s2-clay"
OUTPUT_ZARR_PREFIX = "sentinel2-zarr"
SPATIAL_CHUNK_SIZE = 256
COMPRESSOR = BloscCodec(cname="lz4hc", clevel=5, shuffle="shuffle")


@lru_cache
def sentinel2_data_store() -> ObjectStore:
    """An object store for reading the input Sentinel-2 data from

    Running on a CloudFerro VM, the full Copernicus archive is mounted as /eodata. Otherwise, we access it via S3,
    using credentials generated via https://eodata-s3keysmanager.dataspace.copernicus.eu/
    """
    eodata_mounted = Path("/eodata")  # on CloudFerro, the copernicus bucket is mounted as /eodata
    if eodata_mounted.exists():
        logger.info("Configured local mounted filesystem access to Sentinel-2 archive")
        return LocalStore(eodata_mounted)

    logger.info("Configured remote S3 API access to Sentinel-2 archive")
    # to access the Copernicus S3 bucket directly, generate credentials via
    # https://eodata-s3keysmanager.dataspace.copernicus.eu/ and then add them as a `copernicus-dataspace` profile in
    # ~/.aws/credentials

    return S3Store(
        bucket="eodata",
        endpoint="https://eodata.dataspace.copernicus.eu",
        access_key_id=os.environ["COPERNICUS_ACCESS_KEY_ID"],
        secret_access_key=os.environ["COPERNICUS_SECRET_ACCESS_KEY"],
    )


@lru_cache
def output_bucket(prefix: str) -> ObjectStore:
    """An object store for writing the output Zarr datacube to"""
    return S3Store(
        bucket=OUTPUT_BUCKET,
        endpoint="https://obs.eu-nl.otc.t-systems.com",
        prefix=prefix,
        access_key_id=os.environ["OTC_ACCESS_KEY_ID"],
        secret_access_key=os.environ["OTC_SECRET_ACCESS_KEY"],
    )


@lru_cache
def open_zarr_store(path: str) -> ZarrObjectStore:
    return ZarrObjectStore(output_bucket(path))


@dataclass(frozen=True, order=True)
class Chunk2D:
    y_start: int
    y_end: int
    x_start: int
    x_end: int

    def __str__(self) -> str:
        """String representation of the chunk in slice notation."""
        return f"{self.y_start}:{self.y_end}, {self.x_start}:{self.x_end}"

    def __repr__(self) -> str:
        return f"Chunk2D({self.y_start}, {self.y_end}, {self.x_start}, {self.x_end})"

    def sub_chunks(self, y_size: int, x_size: int) -> list["Chunk2D"]:
        """Subdivide a given chunk into sub-chunks, for dividing it for parallel processing."""

        chunks = []
        for y_start in range(self.y_start, self.y_end, y_size):
            for x_start in range(self.x_start, self.x_end, x_size):
                y_end = min(y_start + y_size, self.y_end)
                x_end = min(x_start + x_size, self.x_end)
                chunks.append(Chunk2D(y_start, y_end, x_start, x_end))

        return chunks


@dataclass
class AreaOfInterest:
    degrees_west: float
    degrees_south: float
    degrees_east: float
    degrees_north: float

    @property
    def shape(self) -> Polygon:
        """The area of interest as a shapely Polygon"""
        return box(self.degrees_west, self.degrees_south, self.degrees_east, self.degrees_north)

    def as_geobox(self, crs: str, resolution: float) -> GeoBox:
        """Convert the area of interest into a GeoBox in the given target coordinate reference system and resolution

        Args:
            crs: The target CRS to use for the output grid, e.g. "EPSG:2157"
            resolution: The target resolution to use for the output grid, in the unit system of the target CRS

        Returns:
            A GeoBox representing the area of interest in the target CRS and resolution
        """
        to_target_crs = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
        target_shape = transform(self.shape, to_target_crs.transform, interleaved=False)  # type: ignore[arg-type]
        return GeoBox.from_bbox(target_shape.bounds, crs=crs, resolution=resolution)

    def chunks(self, crs: str, resolution: float, chunk_size_yx: tuple[int, int]) -> list[Chunk2D]:
        """Divide the area of interest into chunks of the given size in the target CRS and resolution"""
        geobox = self.as_geobox(crs, resolution)
        root_chunk = Chunk2D(0, geobox.shape.y, 0, geobox.shape.x)
        return root_chunk.sub_chunks(*chunk_size_yx)


@dataclass
class RegionOfInterest:
    area: AreaOfInterest
    time: tuple[str, str]


class Sentinel2ToZarr(Task):
    collections: list[str]
    """The name of the S2 collections to query and convert"""

    roi: RegionOfInterest
    """The region of interest to query"""

    crs: str
    """The target CRS to use for the output grid"""

    resolution: float
    """The target resolution for our output grid, in units of the target CRS"""

    def execute(self, context: ExecutionContext) -> None:
        dataset = DatasetClient().dataset("open_data.copernicus.sentinel2_msi")
        collection_granules = [
            dataset.collection(collection).query(temporal_extent=self.roi.time, spatial_extent=self.roi.area.shape)
            for collection in self.collections
        ]

        collection_granules = [cg for cg in collection_granules if cg]  # remove empty datasets
        if len(collection_granules) == 0:
            logger.info("No granules found, skipping remaining workflow")
            return

        granules = xr.concat(collection_granules, dim="time").sortby("time")
        locations = [str(location).removeprefix("/eodata/") for location in granules.location.values]

        if len(locations) == 0:
            logger.info("No granules found, skipping remaining workflow")
            return

        logger.info(f"Found {len(locations)} matching S2 granules")

        geobox = self.roi.area.as_geobox(self.crs, self.resolution)
        context.job_cache["target_grid"] = pickle.dumps(geobox)  # type: ignore[attr-defined]

        initialize_datacube = context.submit_subtask(
            InitializeZarrDatacube(len(locations), geobox.shape.y, geobox.shape.x)
        )

        context.progress("read-granules").add(len(locations))
        read_granules = [
            context.submit_subtask(
                GranuleToZarr(granule, i),
                depends_on=[initialize_datacube],
            )
            for i, granule in enumerate(locations)
        ]

        compute_chunks = self.roi.area.chunks(self.crs, self.resolution, (SPATIAL_CHUNK_SIZE, SPATIAL_CHUNK_SIZE))
        context.progress("compute-mosaic").add(len(compute_chunks))
        for chunk in compute_chunks:
            context.submit_subtask(ComputeMosaic(chunk), depends_on=read_granules)


class InitializeZarrDatacube(Task):
    n_time: int
    n_y: int
    n_x: int

    def execute(self, context: ExecutionContext) -> None:
        zarr_store = open_zarr_store(f"{OUTPUT_ZARR_PREFIX}/{context.current_task.job.id}/cube")  # type: ignore[attr-defined]

        n_band = len(_S2_BANDS)

        for variable_name, product in _S2_PRODUCTS.items():
            zarr.create_array(
                store=zarr_store,
                name=variable_name,
                shape=(self.n_time, self.n_y, self.n_x),
                chunks=(1, SPATIAL_CHUNK_SIZE, SPATIAL_CHUNK_SIZE),
                dimension_names=("time", "y", "x"),
                dtype=product.dtype,
                compressors=COMPRESSOR,
                fill_value=0,
                attributes={
                    "band_name": product.name,
                },
            )

        zarr.create_array(
            store=zarr_store,
            name="mosaic",
            shape=(n_band, self.n_y, self.n_x),
            chunks=(1, SPATIAL_CHUNK_SIZE, SPATIAL_CHUNK_SIZE),
            dimension_names=("band", "y", "x"),
            dtype=np.uint16,
            compressors=COMPRESSOR,
            fill_value=0,
        )

        bands = zarr.create_array(
            store=zarr_store,
            name="band",
            shape=(n_band,),
            chunks=(n_band,),
            dimension_names=("band",),
            dtype="S20",
            compressors=COMPRESSOR,
        )
        bands[:] = [product.name for product in _S2_PRODUCTS.values()]

        dims = f"time={self.n_time}, y={self.n_y}, x={self.n_x}, band={n_band}"
        logger.info(f"Successfully initialized a Zarr datacube with shape {dims}")
        context.current_task.display = f"InitZarrCube({dims})"  # type: ignore[attr-defined]


class GranuleToZarr(Task):
    granule_location: str
    """The location of the granule to process"""

    time_index: int
    """The time index of the granule in the output Zarr datacube"""

    def execute(self, context: ExecutionContext) -> None:
        granule_name = Path(self.granule_location).stem
        context.current_task.display = f"GranuleToZarr({granule_name})"  # type: ignore[attr-defined]

        suffixes = {f"{var_name}_{product.native_resolution}m.jp2" for var_name, product in _S2_PRODUCTS.items()}

        for page in sentinel2_data_store().list(self.granule_location):
            for obj in page:
                product = obj["path"]
                if any(product.endswith(suffix) for suffix in suffixes):
                    context.progress("read-product").add(1)
                    context.submit_subtask(
                        GranuleProductToZarr(product, self.time_index),
                    )

        # mark one granule as done
        context.progress("read-granules").done(1)


class GranuleProductToZarr(Task):
    product_location: str
    """A concrete Sentinel 2 product to convert to Zarr"""

    time_index: int
    """The time index of the granule in the output Zarr datacube"""

    def execute(self, context: ExecutionContext) -> None:
        variable_name = Path(self.product_location).stem.split("_")[-2]  # B02, B03, B04, ..., B11, B12, SCL
        context.current_task.display = f"ProductToZarr({variable_name})"  # type: ignore[attr-defined]

        tracer = context._runner.tracer._tracer  # type: ignore[arg-defined], # noqa: SLF001
        with tracer.start_span(f"read_product/{variable_name}"):
            logger.info(f"Reading product {self.product_location}")
            buffer = bytes(sentinel2_data_store().get(self.product_location).bytes())
            logger.info(f"Product read, size={len(buffer)} bytes")

            with rasterio.MemoryFile(buffer).open(driver="JP2OpenJPEG") as product:
                arr = product.read(1)
                src_grid = GeoBox(shape=arr.shape, affine=product.transform, crs=product.crs)

        with tracer.start_span(f"reproject/{variable_name}"):
            dataset = xr.Dataset({variable_name: (["y", "x"], arr)})
            dataset[variable_name] = wrap_xr(dataset[variable_name], gbox=src_grid)  # add source spatial_ref metadata

            target_grid: GeoBox = pickle.loads(context.job_cache["target_grid"])  # type: ignore[attr-defined]  # noqa: S301
            target_dataset = dataset.odc.reproject(how=target_grid, resampling=Resampling.nearest, dst_nodata=0)
            reprojected_product = target_dataset[variable_name].to_numpy()
            logger.info(f"Projected variable {variable_name} of product {self.product_location} to target grid")

        with tracer.start_span(f"write_to_zarr/{variable_name}"):
            zarr_group = zarr.open_group(
                open_zarr_store(f"{OUTPUT_ZARR_PREFIX}/{context.current_task.job.id}/cube"), mode="a"
            )

            product_array: zarr.Array = zarr_group[variable_name]  # type: ignore[arg-type]
            product_array[self.time_index, :, :] = reprojected_product
            logger.info(f"Successfully wrote variable {variable_name} to Zarr datacube")

        context.progress("read-product").done(1)


class ComputeMosaic(Task):
    chunk: Chunk2D

    def execute(self, context: ExecutionContext) -> None:
        y_start, y_end, x_start, x_end = self.chunk.y_start, self.chunk.y_end, self.chunk.x_start, self.chunk.x_end
        context.current_task.display = f"ComputeMosaic(y={y_start}:{y_end}, x={x_start}:{x_end})"  # type: ignore[attr-defined]

        tracer = context._runner.tracer._tracer  # type: ignore[arg-defined], # noqa: SLF001

        zarr_prefix = f"{OUTPUT_ZARR_PREFIX}/{context.current_task.job.id}/cube"  # type: ignore[attr-defined]
        zarr_store = open_zarr_store(zarr_prefix)
        cube = xr.open_zarr(zarr_store, zarr_format=3, consolidated=False)

        mosaic: zarr.Array = zarr.open_group(zarr_store, mode="a")["mosaic"]  # type: ignore[arg-type]

        valid = cube.SCL[:, y_start:y_end, x_start:x_end].isin([2, 4, 5, 6, 11]).compute()

        for i, band in enumerate(_S2_BANDS):
            with tracer.start_span(f"band_{band}"):
                has_data = cube[band][:, y_start:y_end, x_start:x_end] != 0
                mosaic_band = (
                    (
                        cube[band][:, y_start:y_end, x_start:x_end]
                        .where(valid & has_data)
                        .quantile(0.25, dim="time")
                        .fillna(0)
                        .clip(0, np.iinfo(np.uint16).max)
                        .astype(np.uint16)
                    )
                    .compute()
                    .to_numpy()
                )
                mosaic[i, y_start:y_end, x_start:x_end] = mosaic_band

        context.progress("compute-mosaic").done(1)


app = App()


class OTCBucketCache(AmazonS3Cache):
    def __init__(self, bucket: str, s3_client: Any, prefix: str = "jobs") -> None:
        self.bucket = bucket
        self.prefix = Path(prefix)
        self._s3 = s3_client

    def group(self, key: str) -> "OTCBucketCache":
        return OTCBucketCache(self.bucket, self._s3, prefix=(self.prefix / key).as_posix())


@app.default
def main(tasks: Literal["all", "compute-only", "data-only"] = "all", cluster: str | None = None) -> None:
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

    selected_tasks = [
        Sentinel2ToZarr,
        InitializeZarrDatacube,
        GranuleToZarr,
        GranuleProductToZarr,
        ComputeMosaic,
    ]
    if tasks == "compute-only":
        selected_tasks = [
            Sentinel2ToZarr,
            InitializeZarrDatacube,
            ComputeMosaic,
        ]
    elif tasks == "data-only":
        selected_tasks = [
            GranuleToZarr,
            GranuleProductToZarr,
        ]
    elif tasks != "all":
        raise ValueError(f"Unknown tasks selection: {tasks}")

    logger.info(f"Starting runner with {tasks} tasks on {cluster or 'default'} cluster")

    runner = client.runner(
        cluster,
        tasks=selected_tasks,
        cache=cache,
    )
    runner.run_forever()


if __name__ == "__main__":
    app()
