import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from io import BytesIO
from pathlib import Path

import numpy as np
import rasterio
from odc.geo import GeoBox
from odc.geo.geobox import GeoboxTiles
from rasterio.windows import Window
from tilebox.workflows import ExecutionContext, Task
from vsifile.rasterio import VSIOpener

from distributed_pca import combine_local_statistics, compute_eigenvectors, compute_squared_deviations_matrix

CHUNK_SHAPE = (2048, 2048)
REDUCTION_FAN_IN = 16


@contextmanager
def open_product(product_path: str) -> Iterator[rasterio.DatasetReader]:
    with rasterio.Env(
        GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
        GDAL_INGESTED_BYTES_AT_OPEN=128 * 1024,
        GDAL_HTTP_MERGE_CONSECUTIVE_RANGES="YES",
        GDAL_HTTP_MULTIRANGE="YES",
        GDAL_HTTP_VERSION="2",
    ):
        if product_path.startswith("s3://"):
            with rasterio.open(
                product_path,
                mode="r",
                opener=VSIOpener(config={"skip_signature": True, "region": "ca-central-1"}),
            ) as product:
                yield product
        else:
            with rasterio.open(product_path, mode="r") as product:
                yield product


def _product_tiles(product_path: str) -> GeoboxTiles:
    with open_product(product_path) as product:
        grid = GeoBox(product.shape, product.transform, product.crs)
    return GeoboxTiles(grid, CHUNK_SHAPE)


def _read_stats(
    product_path: str, tiles: GeoboxTiles, tile_index: tuple[int, int]
) -> tuple[int, np.ndarray, np.ndarray]:
    y_slice, x_slice = tiles.roi[tile_index]
    window = Window.from_slices(y_slice, x_slice)
    with open_product(product_path) as product:
        measurements = product.read(window=window).transpose((1, 2, 0)).reshape(-1, product.count)
        valid = np.isfinite(measurements).all(axis=1)
        if product.nodata is not None:
            valid &= (measurements != product.nodata).all(axis=1)

    samples = measurements[valid]
    if len(samples) == 0:
        return 0, np.zeros((measurements.shape[1], measurements.shape[1])), np.zeros(measurements.shape[1])
    deviations, mean = compute_squared_deviations_matrix(samples)
    return len(samples), deviations, mean


def _encode_stats(stats: tuple[int, np.ndarray, np.ndarray]) -> bytes:
    output = BytesIO()
    np.savez_compressed(output, samples=np.int64(stats[0]), deviations=stats[1], mean=stats[2])
    return output.getvalue()


def _decode_stats(value: bytes) -> tuple[int, np.ndarray, np.ndarray]:
    with np.load(BytesIO(value), allow_pickle=False) as data:
        return int(data["samples"].item()), data["deviations"], data["mean"]


class WyvernPCA(Task):
    product_paths: list[str]

    async def execute(self, context: ExecutionContext) -> None:
        products = sorted(set(self.product_paths))
        if not products:
            raise ValueError("At least one product is required")

        product_tiles = await asyncio.gather(*(asyncio.to_thread(_product_tiles, product) for product in products))
        pending = []
        for product_index, (product, tiles) in enumerate(zip(products, product_tiles, strict=True)):
            rows, columns = tiles.shape.yx
            for row in range(rows):
                for column in range(columns):
                    key = f"stats/0/{product_index}-{row}-{column}"
                    future = context.submit_subtask(
                        ComputeChunkStats(product, tiles, (row, column), key),
                        max_retries=2,
                    )
                    pending.append((key, future))

        context.current_task.display = f"WyvernPCA({len(products)} products, {len(pending)} chunks)"  # type: ignore[attr-defined]
        context.progress("chunk-statistics").add(len(pending))

        level = 1
        while len(pending) > 1:
            reduced = []
            for group_index, offset in enumerate(range(0, len(pending), REDUCTION_FAN_IN)):
                group = pending[offset : offset + REDUCTION_FAN_IN]
                if len(group) == 1:
                    reduced.append(group[0])
                    continue
                output_key = f"stats/{level}/{group_index}"
                future = context.submit_subtask(
                    CombineStats([key for key, _ in group], output_key),
                    depends_on=[future for _, future in group],
                )
                reduced.append((output_key, future))
            pending = reduced
            level += 1

        stats_key, stats_task = pending[0]
        context.submit_subtask(ComputePrincipalComponents(stats_key), depends_on=stats_task)


class ComputeChunkStats(Task):
    product_path: str
    tiles: GeoboxTiles
    tile_index: tuple[int, int]
    output_key: str

    async def execute(self, context: ExecutionContext) -> None:
        context.current_task.display = f"ChunkStats({Path(self.product_path).stem}, {self.tile_index})"  # type: ignore[attr-defined]
        stats = await asyncio.to_thread(_read_stats, self.product_path, self.tiles, self.tile_index)
        context.job_cache[self.output_key] = _encode_stats(stats)  # type: ignore[attr-defined]
        context.progress("chunk-statistics").done(1)


class CombineStats(Task):
    input_keys: list[str]
    output_key: str

    async def execute(self, context: ExecutionContext) -> None:
        stats = _decode_stats(context.job_cache[self.input_keys[0]])  # type: ignore[attr-defined]
        for key in self.input_keys[1:]:
            stats = combine_local_statistics(*stats, *_decode_stats(context.job_cache[key]))  # type: ignore[attr-defined]
        context.job_cache[self.output_key] = _encode_stats(stats)  # type: ignore[attr-defined]


class ComputePrincipalComponents(Task):
    stats_key: str

    async def execute(self, context: ExecutionContext) -> None:
        samples, deviations, _ = _decode_stats(context.job_cache[self.stats_key])  # type: ignore[attr-defined]
        if samples < 2:
            raise ValueError("PCA requires at least two valid pixels")

        eigenvalues, eigenvectors = compute_eigenvectors(deviations / (samples - 1))
        output = BytesIO()
        np.savez_compressed(output, eigenvalues=eigenvalues, eigenvectors=eigenvectors)
        context.job_cache["principal_components"] = output.getvalue()  # type: ignore[attr-defined]
        context.current_task.display = f"PrincipalComponents({samples} pixels)"  # type: ignore[attr-defined]


TASKS: list[type[Task]] = [WyvernPCA, ComputeChunkStats, CombineStats, ComputePrincipalComponents]
