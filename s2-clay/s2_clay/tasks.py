import asyncio
import math
import os
import warnings
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import rasterio.warp
import torch
import zarr
from affine import Affine
from huggingface_hub import hf_hub_download
from obstore.store import ObjectStore, S3Store
from odc.geo import CRS, Geometry, Resolution
from odc.geo.geobox import GeoBox, GeoboxTiles
from rasterio.enums import Resampling
from tilebox.datasets.aio import Client as DatasetClient
from tilebox.datasets.assets import Asset, AssetCollection
from tilebox.datasets.query import TimeInterval, field
from tilebox.storage.aio import open_geotiff
from tilebox.storage.geotiff import window_from_bounds
from tilebox.workflows import ExecutionContext, Task
from zarr.codecs import BloscCodec
from zarr.storage import ObjectStore as ZarrObjectStore

DATASET_SLUG = "open_data.aws_earth.sentinel2"
COLLECTION = "L2A"
OUTPUT_BUCKET = "s2-clay"
OUTPUT_PREFIX = "change-detection"
TILE_SHAPE = (256, 256)
PATCH_SIZE = 8
EMBEDDING_DIM = 1024
PLATFORM = "sentinel-2-l2a"
VALID_SCL_CLASSES = (2, 4, 5, 6, 11)
BANDS = ("blue", "green", "red", "rededge1", "rededge2", "rededge3", "nir", "nir08", "swir16", "swir22")
BAND_MEANS = np.array((1105, 1355, 1552, 1887, 2422, 2630, 2743, 2785, 2388, 1835), dtype=np.float32)
BAND_STDS = np.array((1809, 1757, 1888, 1870, 1732, 1697, 1742, 1648, 1470, 1379), dtype=np.float32)
BAND_WAVELENGTHS = np.array((0.493, 0.56, 0.665, 0.704, 0.74, 0.783, 0.842, 0.865, 1.61, 2.19), dtype=np.float32)
COMPRESSOR = BloscCodec(cname="lz4hc", clevel=5, shuffle="shuffle")


@lru_cache
def output_storage(prefix: str) -> ObjectStore:
    return S3Store(
        bucket=OUTPUT_BUCKET,
        endpoint="https://obs.eu-nl.otc.t-systems.com",
        prefix=prefix,
        access_key_id=os.environ["OTC_ACCESS_KEY_ID"],
        secret_access_key=os.environ["OTC_SECRET_ACCESS_KEY"],
    )


def _output_prefix(context: ExecutionContext) -> str:
    return f"{OUTPUT_PREFIX}/{context.current_task.job.id}"  # type: ignore[attr-defined]


def _output_store(context: ExecutionContext) -> ZarrObjectStore:
    return ZarrObjectStore(output_storage(_output_prefix(context)))


def _initialize_output(context: ExecutionContext, target: GeoBox) -> None:
    patch_shape = (math.ceil(target.shape.y / PATCH_SIZE), math.ceil(target.shape.x / PATCH_SIZE))
    patch_chunks = tuple(math.ceil(size / PATCH_SIZE) for size in TILE_SHAPE)
    root = zarr.open_group(_output_store(context), mode="w")
    for period in ("before", "after"):
        root.create_array(
            period,
            shape=(*patch_shape, EMBEDDING_DIM),
            chunks=(*patch_chunks, EMBEDDING_DIM),
            dimension_names=("y", "x", "embedding"),
            dtype=np.float32,
            compressors=COMPRESSOR,
        )
    root.create_array(
        "change",
        shape=patch_shape,
        chunks=patch_chunks,
        dimension_names=("y", "x"),
        dtype=np.float32,
        compressors=COMPRESSOR,
    )
    root.attrs.update(
        {
            "crs": str(target.crs),
            "transform": tuple(target.transform * Affine.scale(PATCH_SIZE))[:6],
            "source_pixel_size": PATCH_SIZE,
            "model": "made-with-clay/Clay v1.5",
        }
    )


async def _read_asset(asset: Asset, tile: GeoBox) -> np.ndarray:
    geotiff = await open_geotiff(asset)
    if tile.crs is None:
        raise ValueError("Clay tiles must have a CRS")
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


async def _read_observation(assets: AssetCollection, tile: GeoBox) -> np.ndarray:
    scl, *bands = await asyncio.gather(
        _read_asset(assets["scl"], tile), *(_read_asset(assets[key], tile) for key in BANDS)
    )
    pixels = np.stack(bands)
    valid = np.isin(scl, VALID_SCL_CLASSES) & (pixels != 0)
    return np.where(valid, pixels, np.nan)


def _normalized_time(interval: TimeInterval) -> tuple[float, float, float, float]:
    mean_time = interval.start + (interval.end - interval.start) / 2
    week = mean_time.isocalendar().week * 2 * np.pi / 52
    hour = 12 * 2 * np.pi / 24
    return math.sin(week), math.cos(week), math.sin(hour), math.cos(hour)


def _normalized_location(tile: GeoBox) -> tuple[float, float, float, float]:
    center = tile.extent.to_crs("EPSG:4326").geom.centroid
    latitude = math.radians(center.y)
    longitude = math.radians(center.x)
    return math.sin(latitude), math.cos(latitude), math.sin(longitude), math.cos(longitude)


@lru_cache
def _device() -> Any:
    if torch.cuda.is_available():
        return torch.device("cuda:0")
    if torch.backends.mps.is_available():
        return torch.device("mps:0")
    return torch.device("cpu")


@lru_cache
def _model() -> Any:
    from claymodel.module import ClayMAEModule  # noqa: PLC0415

    checkpoint = Path(hf_hub_download(repo_id="made-with-clay/Clay", filename="v1.5/clay-v1.5.ckpt"))
    model = ClayMAEModule.load_from_checkpoint(
        checkpoint,
        model_size="large",
        metadata_path=(Path(__file__).parents[1] / "configs/metadata.yaml").as_posix(),
        dolls=[16, 32, 64, 128, 256, 768, 1024],
        doll_weights=[1, 1, 1, 1, 1, 1, 1],
        mask_ratio=0,
        shuffle=False,
    )
    return model.to(_device()).eval()


class DetectChanges(Task):
    area: Geometry
    before: TimeInterval
    after: TimeInterval
    output_crs: CRS
    resolution: Resolution
    max_cloud_cover: float = 20

    async def execute(self, context: ExecutionContext) -> None:
        dataset = await DatasetClient().dataset(DATASET_SLUG)
        scene_ids: dict[str, list[str]] = {}
        for period, interval in (("before", self.before), ("after", self.after)):
            scenes = await dataset.query(
                collections=[COLLECTION],
                temporal_extent=interval,
                spatial_extent=self.area.geom,
                filter=field("cloud_cover") < self.max_cloud_cover,
            )
            ids = [str(value) for value in scenes.sortby("time")["id"].values]
            if not ids:
                context.logger.info(f"No matching Sentinel-2 scenes for the {period} period")
                return
            scene_ids[period] = ids
            context.job_cache[f"{period}_scene_ids"] = "\n".join(ids).encode()  # type: ignore[attr-defined]

        target = GeoBox.from_geopolygon(self.area, crs=self.output_crs, resolution=self.resolution, anchor="edge")
        tiles = GeoboxTiles(target, TILE_SHAPE)
        _initialize_output(context, target)

        rows, columns = tiles.shape.yx
        context.current_task.display = f"DetectChanges({rows * columns} tiles)"  # type: ignore[attr-defined]
        context.progress("clay-inference").add(rows * columns * 2)
        context.progress("change-detection").add(rows * columns)
        for row in range(rows):
            for column in range(columns):
                index = (row, column)
                before = context.submit_subtask(ComputeClayTile(tiles, index, "before", self.before), max_retries=2)
                after = context.submit_subtask(ComputeClayTile(tiles, index, "after", self.after), max_retries=2)
                context.submit_subtask(ComputeChangeTile(tiles, index), depends_on=[before, after], max_retries=2)

        context.logger.info(
            f"Submitted {rows * columns} tiles using {len(scene_ids['before'])} before and {len(scene_ids['after'])} after scenes"
        )


class ComputeClayTile(Task):
    tiles: GeoboxTiles
    tile_index: tuple[int, int]
    period: Literal["before", "after"]
    time_interval: TimeInterval

    async def execute(self, context: ExecutionContext) -> None:
        tile = cast(GeoBox, self.tiles[self.tile_index])
        y_slice, x_slice = self.tiles.roi[self.tile_index]
        context.current_task.display = f"ClayTile({self.period}, {self.tile_index[0]}, {self.tile_index[1]})"  # type: ignore[attr-defined]

        scene_ids = context.job_cache[f"{self.period}_scene_ids"].decode().splitlines()  # type: ignore[attr-defined]
        dataset = await DatasetClient().dataset(DATASET_SLUG)
        observations = []
        for scene_id in scene_ids:
            datapoint = await dataset.find(scene_id, collections=[COLLECTION])
            observations.append(await _read_observation(AssetCollection.from_datapoint(datapoint), tile))

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            mosaic = np.nanquantile(np.stack(observations), 0.25, axis=0)
        mosaic = np.nan_to_num(mosaic).astype(np.float32)

        padded = np.zeros((len(BANDS), *TILE_SHAPE), dtype=np.float32)
        padded[:, : tile.shape.y, : tile.shape.x] = mosaic
        pixels = (padded - BAND_MEANS[:, None, None]) / BAND_STDS[:, None, None]
        model_input = {
            "platform": PLATFORM,
            "time": torch.tensor([_normalized_time(self.time_interval)], dtype=torch.float32, device=_device()),
            "latlon": torch.tensor([_normalized_location(tile)], dtype=torch.float32, device=_device()),
            "pixels": torch.from_numpy(pixels[None]).to(_device()),
            "gsd": torch.tensor([abs(tile.resolution.x)], device=_device()),
            "waves": torch.from_numpy(BAND_WAVELENGTHS).to(_device()),
        }
        with context.tracer.span("clay-inference"), torch.no_grad():
            encoded, _, _, _ = _model().model.encoder(model_input)
            patches = encoded[0, 1:, :].detach().cpu().numpy().reshape(32, 32, EMBEDDING_DIM)

        patch_y = slice(y_slice.start // PATCH_SIZE, math.ceil(y_slice.stop / PATCH_SIZE))
        patch_x = slice(x_slice.start // PATCH_SIZE, math.ceil(x_slice.stop / PATCH_SIZE))
        output: zarr.Array = zarr.open_group(_output_store(context), mode="a")[self.period]  # type: ignore[assignment]
        output[patch_y, patch_x, :] = patches[: patch_y.stop - patch_y.start, : patch_x.stop - patch_x.start]
        context.progress("clay-inference").done(1)


class ComputeChangeTile(Task):
    tiles: GeoboxTiles
    tile_index: tuple[int, int]

    async def execute(self, context: ExecutionContext) -> None:
        y_slice, x_slice = self.tiles.roi[self.tile_index]
        patch_y = slice(y_slice.start // PATCH_SIZE, math.ceil(y_slice.stop / PATCH_SIZE))
        patch_x = slice(x_slice.start // PATCH_SIZE, math.ceil(x_slice.stop / PATCH_SIZE))
        root = zarr.open_group(_output_store(context), mode="a")
        before_array = cast(zarr.Array, root["before"])
        after_array = cast(zarr.Array, root["after"])
        change_array = cast(zarr.Array, root["change"])
        before = np.asarray(before_array[patch_y, patch_x, :])
        after = np.asarray(after_array[patch_y, patch_x, :])

        denominator = np.linalg.norm(before, axis=-1) * np.linalg.norm(after, axis=-1)
        similarity = np.divide(
            np.sum(before * after, axis=-1),
            denominator,
            out=np.zeros_like(denominator),
            where=denominator > 0,
        )
        change_array[patch_y, patch_x] = 1 - np.clip(similarity, -1, 1)
        context.progress("change-detection").done(1)


TASKS = [DetectChanges, ComputeClayTile, ComputeChangeTile]
