from functools import lru_cache
from typing import cast

import numpy as np
import rasterio.warp
import zarr
from affine import Affine
from google.cloud.storage import Client as GoogleStorageClient
from obstore.auth.google import GoogleCredentialProvider
from obstore.store import GCSStore, ObjectStore
from odc.geo import CRS, Geometry, Resolution
from odc.geo.geobox import GeoBox, GeoboxTiles
from rasterio.enums import Resampling
from tilebox.datasets.aio import Client as DatasetClient
from tilebox.datasets.assets import Asset, AssetCollection
from tilebox.datasets.query import TimeInterval, field
from tilebox.storage.aio import open_geotiff
from tilebox.storage.geotiff import window_from_bounds
from tilebox.workflows import ExecutionContext, Task
from tilebox.workflows.cache import GoogleStorageCache
from zarr.codecs import BloscCodec
from zarr.storage import ObjectStore as ZarrObjectStore

DATASET_SLUG = "open_data.aws_earth.sentinel2"
COLLECTION = "L2A"
ZARR_GCS_BUCKET = "vci-datacube-bucket-1513742"
ZARR_GCS_PROJECT = "careful-striker-387117"
CACHE_PREFIX = "s2-mosaic"
TILE_SHAPE = (1024, 1024)
RGB_ASSETS = ("red", "green", "blue")
VALID_SCL_CLASSES = (2, 4, 5, 6, 11)
COMPRESSOR = BloscCodec(cname="lz4hc", clevel=5, shuffle="shuffle")


@lru_cache
def zarr_storage(prefix: str) -> ObjectStore:
    return GCSStore(bucket=ZARR_GCS_BUCKET, prefix=prefix, credential_provider=GoogleCredentialProvider())


def workflow_cache() -> GoogleStorageCache:
    bucket = GoogleStorageClient(project=ZARR_GCS_PROJECT).bucket(ZARR_GCS_BUCKET)
    return GoogleStorageCache(bucket, prefix=CACHE_PREFIX)


def _output_prefix(context: ExecutionContext) -> str:
    return f"{CACHE_PREFIX}/{context.current_task.job.id}/mosaic"  # type: ignore[attr-defined]


def _output_store(context: ExecutionContext) -> ZarrObjectStore:
    return ZarrObjectStore(zarr_storage(_output_prefix(context)))


def _initialize_output(context: ExecutionContext, target: GeoBox) -> None:
    root = zarr.open_group(_output_store(context), mode="w")
    root.create_array(
        "mosaic",
        shape=(len(RGB_ASSETS), target.shape.y, target.shape.x),
        chunks=(1, *TILE_SHAPE),
        dimension_names=("band", "y", "x"),
        dtype=np.uint16,
        fill_value=0,
        compressors=COMPRESSOR,
    )
    bands = root.create_array(
        "band",
        shape=(len(RGB_ASSETS),),
        chunks=(len(RGB_ASSETS),),
        dimension_names=("band",),
        dtype="S5",
    )
    bands[:] = RGB_ASSETS
    root.attrs.update(
        {
            "crs": str(target.crs),
            "transform": tuple(target.transform)[:6],
            "scale_factor": 0.0001,
        }
    )


async def _read_asset(asset: Asset, tile: GeoBox) -> np.ndarray:
    geotiff = await open_geotiff(asset)
    if tile.crs is None:
        raise ValueError("Mosaic tiles must have a CRS")
    try:
        window = window_from_bounds(geotiff, tile.boundingbox.bbox, crs=tile.crs.proj)
    except ValueError:
        return np.zeros(tile.shape.yx, dtype=np.dtype(geotiff.dtype))

    source = np.asarray(await geotiff.read(window=window)).squeeze()
    destination = np.zeros(tile.shape.yx, dtype=source.dtype)
    source_transform = geotiff.transform * Affine.translation(window.col_off, window.row_off)
    rasterio.warp.reproject(
        source,
        destination,
        src_transform=source_transform,
        src_crs=geotiff.crs,
        src_nodata=geotiff.nodata,
        dst_transform=tile.transform,
        dst_crs=tile.crs,
        dst_nodata=0,
        resampling=Resampling.nearest,
    )
    return destination


class BuildMosaic(Task):
    area: Geometry
    time_interval: TimeInterval
    output_crs: CRS
    resolution: Resolution
    max_cloud_cover: float = 20

    async def execute(self, context: ExecutionContext) -> None:
        dataset = await DatasetClient().dataset(DATASET_SLUG)
        scenes = await dataset.query(
            collections=[COLLECTION],
            temporal_extent=self.time_interval,
            spatial_extent=self.area.geom,
            filter=field("cloud_cover") < self.max_cloud_cover,
        )
        if scenes.sizes.get("time", 0) == 0:
            context.logger.info("No matching Sentinel-2 scenes")
            return

        scenes = scenes.sortby("time")
        scene_ids = [str(value) for value in scenes["id"].values]
        context.job_cache["scene_ids"] = "\n".join(scene_ids).encode()  # type: ignore[attr-defined]

        target = GeoBox.from_geopolygon(
            self.area,
            crs=self.output_crs,
            resolution=self.resolution,
            anchor="edge",
        )
        tiles = GeoboxTiles(target, TILE_SHAPE)
        _initialize_output(context, target)

        rows, columns = tiles.shape.yx
        context.current_task.display = f"BuildMosaic({len(scene_ids)} scenes, {rows * columns} tiles)"  # type: ignore[attr-defined]
        context.progress("mosaic-tiles").add(rows * columns)
        context.submit_subtasks(
            [ComputeMosaicTile(tiles, (row, column)) for row in range(rows) for column in range(columns)],
            max_retries=2,
        )


class ComputeMosaicTile(Task):
    tiles: GeoboxTiles
    tile_index: tuple[int, int]

    async def execute(self, context: ExecutionContext) -> None:
        tile = cast(GeoBox, self.tiles[self.tile_index])
        y_slice, x_slice = self.tiles.roi[self.tile_index]
        context.current_task.display = f"MosaicTile({self.tile_index[0]}, {self.tile_index[1]})"  # type: ignore[attr-defined]
        logger = context.logger.bind(tile_index=self.tile_index)

        scene_ids = context.job_cache["scene_ids"].decode().splitlines()  # type: ignore[attr-defined]
        dataset = await DatasetClient().dataset(DATASET_SLUG)
        observations: list[np.ndarray] = []
        for scene_id in scene_ids:
            datapoint = await dataset.find(scene_id, collections=[COLLECTION])
            assets = AssetCollection.from_datapoint(datapoint)
            scl = await _read_asset(assets["scl"], tile)
            valid = np.isin(scl, VALID_SCL_CLASSES)

            rgb = np.stack([await _read_asset(assets[key], tile) for key in RGB_ASSETS])
            observations.append(np.where(valid & (rgb != 0), rgb, np.nan))

        with context.tracer.span("composite"):
            with np.errstate(all="ignore"):
                mosaic = np.nanquantile(np.stack(observations), 0.25, axis=0)
            mosaic = np.nan_to_num(mosaic).clip(0, np.iinfo(np.uint16).max).astype(np.uint16)

        output: zarr.Array = zarr.open_group(_output_store(context), mode="a")["mosaic"]  # type: ignore[assignment]
        output[:, y_slice, x_slice] = mosaic
        logger.info("Wrote mosaic tile")
        context.progress("mosaic-tiles").done(1)


TASKS = [BuildMosaic, ComputeMosaicTile]
