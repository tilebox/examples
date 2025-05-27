import math
import os
import pickle
from collections.abc import Iterator
from dataclasses import dataclass
from functools import lru_cache
from hashlib import md5
from io import BytesIO
from pathlib import Path

import dask.array
import numpy as np
import rasterio
import xarray as xr
from boto3 import Session
from dotenv import load_dotenv
from google.cloud.storage import Client as StorageClient
from numpy.typing import DTypeLike
from obstore.auth.boto3 import Boto3CredentialProvider
from obstore.store import LocalStore, ObjectStore, S3Store
from odc.geo.geobox import GeoBox
from odc.geo.xr import wrap_xr
from pyproj import Transformer
from rasterio.enums import Resampling
from shapely import Polygon, box, transform
from tilebox.datasets import Client as DatasetClient
from tilebox.datasets.data.time_interval import TimeInterval
from tilebox.workflows import Client as WorkflowsClient
from tilebox.workflows import ExecutionContext, Task
from tilebox.workflows.cache import GoogleStorageCache
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


_S2_PRODUCTS = {
    "B02": S2Product("blue band", 10, np.uint16),
    "B03": S2Product("green band", 10, np.uint16),
    "B04": S2Product("red band", 10, np.uint16),
    "SCL": S2Product("scene classification", 20, np.uint8),
}
"""The S2 products we are reading for each granule"""

CACHE_GCS_BUCKET = "workflow-cache-15c9850"
ZARR_S3_BUCKET = "workflow-cache-35ee674"
CACHE_PREFIX = "s2-zarr"
SPATIAL_CHUNK_SIZE = 2048


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
        credential_provider=Boto3CredentialProvider(Session(profile_name="copernicus-dataspace")),
    )


@lru_cache
def zarr_storage(prefix: str) -> ObjectStore:
    """An object store for writing the output Zarr datacube to"""
    return S3Store(
        bucket=ZARR_S3_BUCKET,
        region="eu-central-1",
        prefix=prefix,
        credential_provider=Boto3CredentialProvider(Session(profile_name="default")),
    )


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

    def as_geobox(self, crs: str, resolution: int) -> GeoBox:
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


@dataclass
class RegionOfInterest:
    area: AreaOfInterest
    time: TimeInterval


class Sentinel2ToZarr(Task):
    collection: str
    """The name of the S2 collection to query and convert"""

    roi: RegionOfInterest
    """The region of interest to query"""

    crs: str
    """The target CRS to use for the output grid"""

    resolution: int
    """The target resolution for our output grid, in units of the target CRS"""

    def execute(self, context: ExecutionContext) -> None:
        collection = DatasetClient().dataset("open_data.copernicus.sentinel2_msi").collection(self.collection)
        granules = collection.query(temporal_extent=self.roi.time, spatial_extent=self.roi.area.shape)
        locations = [str(location).removeprefix("/eodata/") for location in granules.location.values]

        if len(locations) == 0:
            logger.info("No granules found, skipping remaining workflow")
            return

        logger.info(f"Found {len(locations)} matching S2 granules")

        context.job_cache["granules"] = "\n".join(locations).encode()  # type: ignore[attr-defined]

        geobox = self.roi.area.as_geobox(self.crs, self.resolution)
        context.job_cache["target_grid"] = pickle.dumps(geobox)  # type: ignore[attr-defined]

        initialize_datacube = context.submit_subtask(
            InitializeZarrDatacube(len(locations), geobox.shape.y, geobox.shape.x)
        )
        read_granules = context.submit_subtask(
            GranulesToZarr((0, len(locations))),
            depends_on=[initialize_datacube],
        )
        context.submit_subtask(
            ComputeMosaic((0, geobox.shape.y), (0, geobox.shape.x)),
            depends_on=[read_granules],
        )


class InitializeZarrDatacube(Task):
    n_time: int
    n_y: int
    n_x: int

    def execute(self, context: ExecutionContext) -> None:
        dataset = xr.Dataset()
        encodings = {}
        compressor = BloscCodec(cname="lz4hc", clevel=5, shuffle="shuffle")
        for variable_name, product in _S2_PRODUCTS.items():
            dataset[variable_name] = (
                ["time", "y", "x"],
                dask.array.zeros(
                    (self.n_time, self.n_y, self.n_x),
                    chunks=(1, SPATIAL_CHUNK_SIZE, SPATIAL_CHUNK_SIZE),
                    dtype=product.dtype,
                ),
            )
            dataset.attrs["long_name"] = product.name
            encodings[variable_name] = {"compressors": (compressor,)}

        dataset["mosaic"] = (
            ["band", "y", "x"],
            dask.array.zeros(
                (3, self.n_y, self.n_x),
                chunks=(1, SPATIAL_CHUNK_SIZE, SPATIAL_CHUNK_SIZE),
                dtype=np.float32,
            ),
        )
        encodings["mosaic"] = {"_FillValue": 0, "scale_factor": 1 / 10000, "compressors": (compressor,)}

        zarr_prefix = f"{CACHE_PREFIX}/{context.current_task.job.id}/cube"  # type: ignore[attr-defined]
        zarr_store = ZarrObjectStore(zarr_storage(zarr_prefix))
        dataset.to_zarr(
            zarr_store,  # type: ignore[arg-type]
            encoding=encodings,
            compute=False,
            mode="w",
            consolidated=False,
            zarr_format=3,
        )
        dims = f"time={self.n_time}, y={self.n_y}, x={self.n_x}"
        logger.info(f"Successfully initialized a Zarr datacube with shape {dims}")
        context.current_task.display = f"InitZarrCube({dims})"  # type: ignore[attr-defined]


class GranulesToZarr(Task):
    granule_range: tuple[int, int]
    """Integer range of the queried granules to process in this task"""

    def execute(self, context: ExecutionContext) -> None:
        # for ideal parallelization, span up a nice processing tree, by subdividing too large ranges into smaller chunks
        start, end = self.granule_range
        context.current_task.display = f"GranulesToZarr[{start}:{end}]"  # type: ignore[attr-defined]
        if end - start > 8:
            mid = (start + end) // 2
            context.submit_subtask(GranulesToZarr((start, mid)))
            context.submit_subtask(GranulesToZarr((mid, end)))
            return

        granules = context.job_cache["granules"].decode().split("\n")[start:end]  # type: ignore[attr-defined]
        for i, granule in enumerate(granules):
            context.submit_subtask(
                GranuleToZarr(granule, start + i),
            )


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
                    context.submit_subtask(
                        GranuleProductToZarr(product, self.time_index),
                    )


class GranuleProductToZarr(Task):
    product_location: str
    """A concrete Sentinel 2 product to convert to Zarr"""

    time_index: int
    """The time index of the granule in the output Zarr datacube"""

    def execute(self, context: ExecutionContext) -> None:
        variable_name = Path(self.product_location).stem.split("_")[-2]  # B02, B03, B04 or SCL
        context.current_task.display = f"ProductToZarr({variable_name})"  # type: ignore[attr-defined]

        tracer = context._runner.tracer._tracer  # type: ignore[arg-defined], # noqa: SLF001
        with tracer.start_span("read_product"):
            logger.info(f"Reading product {self.product_location}")

            with BytesIO() as buffer:
                object_hash = md5()  # noqa: S324
                for chunk in sentinel2_data_store().get(self.product_location):
                    buffer.write(chunk)
                    object_hash.update(chunk)

                data = buffer.getvalue()
                logger.info(f"Product read, size={len(data)} bytes, md5={object_hash.hexdigest()}")

                with rasterio.MemoryFile(data).open(driver="JP2OpenJPEG") as product:
                    arr = product.read(1)
                    src_grid = GeoBox(shape=arr.shape, affine=product.transform, crs=product.crs)

        with tracer.start_span("reproject"):
            dataset = xr.Dataset({variable_name: (["y", "x"], arr)})
            dataset[variable_name] = wrap_xr(dataset[variable_name], gbox=src_grid)  # add source spatial_ref metadata

            target_grid: GeoBox = pickle.loads(context.job_cache["target_grid"])  # type: ignore[attr-defined]  # noqa: S301
            target_dataset = dataset.odc.reproject(how=target_grid, resampling=Resampling.nearest, dst_nodata=0)
            target_dataset = target_dataset.expand_dims(time=1)
            target_dataset = target_dataset.drop_vars("spatial_ref")  # don't write this to zarr, not needed

            logger.info(f"Projected variable {variable_name} of product {self.product_location} to target grid")

        with tracer.start_span("write_zarr"):
            zarr_prefix = f"{CACHE_PREFIX}/{context.current_task.job.id}/cube"  # type: ignore[attr-defined]
            zarr_store = ZarrObjectStore(zarr_storage(zarr_prefix))
            target_dataset.to_zarr(
                zarr_store,  # type: ignore[arg-type]
                region={
                    "time": slice(self.time_index, self.time_index + 1),
                    "y": slice(0, target_grid.shape.y),
                    "x": slice(0, target_grid.shape.x),
                },
                write_empty_chunks=False,
                safe_chunks=False,  # our grid size is not an exact multiple of chunk size
                consolidated=False,
                zarr_format=3,
            )

            logger.info(f"Successfully wrote variable {variable_name} to Zarr datacube")


class ComputeMosaic(Task):
    ys: tuple[int, int]
    xs: tuple[int, int]

    def execute(self, context: ExecutionContext) -> None:
        chunks = []
        for y_start, y_end in _split_interval(*self.ys, SPATIAL_CHUNK_SIZE):
            for x_start, x_end in _split_interval(*self.xs, SPATIAL_CHUNK_SIZE):
                chunks.append(((y_start, y_end), (x_start, x_end)))

        if len(chunks) > 1:
            for chunk in chunks:
                context.submit_subtask(ComputeMosaic(*chunk))

        (y_start, y_end), (x_start, x_end) = chunks[0]
        context.current_task.display = f"ComputeMosaic(y={y_start}:{y_end}, x={x_start}:{x_end})"  # type: ignore[attr-defined]

        tracer = context._runner.tracer._tracer  # type: ignore[arg-defined], # noqa: SLF001

        zarr_prefix = f"{CACHE_PREFIX}/{context.current_task.job.id}/cube"  # type: ignore[attr-defined]
        zarr_store = ZarrObjectStore(zarr_storage(zarr_prefix))
        cube = xr.open_zarr(zarr_store, zarr_format=3, consolidated=False)

        valid = cube.SCL[:, y_start:y_end, x_start:x_end].isin([2, 4, 5, 6, 11]).compute()

        for i, band in enumerate(["B04", "B03", "B02"]):  # red, green, blue
            with tracer.start_span(f"band_{band}"):
                has_data = cube[band][:, y_start:y_end, x_start:x_end] != 0
                mosaic_arr = (
                    cube[band][:, y_start:y_end, x_start:x_end].where(valid & has_data).quantile(0.25, dim="time")
                    / 10000
                ).compute()
                mosaic = xr.Dataset({"mosaic": mosaic_arr}).expand_dims({"band": 1})
                mosaic.to_zarr(
                    zarr_store,  # type: ignore[arg-type]
                    region={
                        "band": slice(i, i + 1),
                        "y": slice(y_start, y_end),
                        "x": slice(x_start, x_end),
                    },
                    write_empty_chunks=False,
                    safe_chunks=False,  # our grid size is not an exact multiple of chunk size
                    consolidated=False,
                    zarr_format=3,
                )


def _split_interval(start: int, end: int, max_size: int) -> Iterator[tuple[int, int]]:
    """
    Split an interval into two sub-intervals in case it is larger than the given maximum size.
    The split is done at the closest power of two.

    Example:
        _split_interval(0, 1000, 512) -> (0, 512), (512, 1000)
        _split_interval(512, 1000, 512) -> (512, 1000)

    Args:
        start (_type_): _description_
        end (_type_): _description_
        max_size (_type_): _description_

    Yields:
        _type_: _description_
    """
    n = end - start
    if n > max_size:
        split_at = 2 ** math.floor(math.log2(n - 1)) + start
        yield start, split_at
        yield split_at, end
    else:
        yield start, end


def main() -> None:
    assert load_dotenv()
    service_name = f"{os.environ['RUNNER_NAME']}-{os.getpid()}"
    configure_console_logging()
    configure_otel_logging_axiom(service_name)
    configure_otel_tracing_axiom(service_name)

    client = WorkflowsClient()  # a workflow client for https://api.tilebox.com

    cache = GoogleStorageCache(
        StorageClient(project="tilebox-production").bucket(CACHE_GCS_BUCKET), prefix=CACHE_PREFIX
    )
    runner = client.runner(
        "workflows-demo-7GzWwLrcvfJ8xZ",
        tasks=[
            Sentinel2ToZarr,
            InitializeZarrDatacube,
            GranulesToZarr,
            GranuleToZarr,
            GranuleProductToZarr,
            ComputeMosaic,
        ],
        cache=cache,
    )
    runner.run_forever()


if __name__ == "__main__":
    main()
