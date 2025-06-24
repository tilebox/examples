import math
import os
import pickle
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import fsspec
import numpy as np
import rasterio
from cyclopts import App
from dotenv import load_dotenv
from google.cloud.storage import Client as StorageClient
from rasterio.windows import Window
from tilebox.workflows import Client as WorkflowsClient
from tilebox.workflows import ExecutionContext, Task
from tilebox.workflows.cache import GoogleStorageCache, LocalFileSystemCache
from tilebox.workflows.observability.logging import configure_console_logging, configure_otel_logging_axiom, get_logger
from tilebox.workflows.observability.tracing import configure_otel_tracing_axiom

from distributed_pca import (
    combine_local_statistics,
    compute_eigenvectors,
    compute_squared_deviations_matrix,
)

logger = get_logger()

_DEFAULT_SPATIAL_CHUNK_SIZE = 2048


@dataclass(frozen=True, order=True)
class SpatialChunk:
    """
    SpatialChunk represents a 2D chunk within a potentially larger 2D space.

    It is useful for sub-dividing larger 2D spaces into smaller chunks for parallel processing.
    """

    y_start: int
    y_end: int
    x_start: int
    x_end: int

    def __str__(self) -> str:
        """String representation of the chunk in slice notation."""
        return f"{self.y_start}:{self.y_end}, {self.x_start}:{self.x_end}"

    def __repr__(self) -> str:
        return f"SpatialChunk({self.y_start}, {self.y_end}, {self.x_start}, {self.x_end})"

    @property
    def key(self) -> str:
        """A unique key for the chunk, which is used as a key in the job cache."""
        return f"chunk_{self.y_start}-{self.y_end}_{self.x_start}-{self.x_end}"

    def to_rasterio_window(self) -> Window:
        """Convert the chunk to a rasterio window for reading only the chunk from a GeoTiff file."""
        return Window(
            col_off=self.x_start,
            row_off=self.y_start,
            width=self.x_end - self.x_start,
            height=self.y_end - self.y_start,
        )

    def immediate_sub_chunks(
        self, y_size: int = _DEFAULT_SPATIAL_CHUNK_SIZE, x_size: int = _DEFAULT_SPATIAL_CHUNK_SIZE
    ) -> list["SpatialChunk"]:
        """
        Subdivide a given chunk into at most four sub-chunks, for dividing it for parallel processing.

        If a chunk is already smaller than the given size in both dimensions, it will no longer be subdivided and
        instead returned as it is as single element list.

        By calling this function recursively, a chunk tree is created, where each node is a chunk and the leaves are
        all chunks that are at most (y_size, x_size)

        Returns:
            list[SpatialChunk]: A list of immediate sub-chunks of this chunk by splitting it into at most
                four sub-chunks.
        """
        sub_chunks = []
        for y_start, y_end in _split_interval(self.y_start, self.y_end, y_size):
            for x_start, x_end in _split_interval(self.x_start, self.x_end, x_size):
                sub_chunks.append(SpatialChunk(y_start, y_end, x_start, x_end))
        return sub_chunks


@contextmanager
def open_wyvern_product(product_path: str, mode: str = "r") -> Iterator[rasterio.DatasetReader]:
    """Open a wyvern product either from the public S3 bucket or from a local file if it is already downloaded.

    Intended to be used as a context manager with the `with` statement.

    Args:
        product_path: The path to the product to open

    Yields:
        Iterator[rasterio.DatasetReader]: A rasterio dataset reader for the product.
    """
    if product_path.startswith("s3://"):  # open directly from S3
        fs = fsspec.filesystem("s3", anon=True)
        with rasterio.open(product_path.removeprefix("s3://"), mode=mode, opener=fs) as file:
            yield file
    else:  # open local file
        with rasterio.open(product_path, mode=mode) as file:
            yield file


class ComputePrincipalComponentsForProduct(Task):
    """
    ComputePrincipalComponentsForProduct is the entrypoint for performing PCA on a wyvern product.

    It subdivides the product into chunks, computes local statistics for each chunk, and then combines the
    local statistics recursively until eventually global statistics for the entire product are computed.

    From those global statistics, the principal components are computed.
    """

    product_path: str

    def execute(self, context: ExecutionContext) -> None:
        context.current_task.display = f"PCA({Path(self.product_path).stem})"
        with open_wyvern_product(self.product_path) as file:
            height, width = file.shape

        full_product_chunk = SpatialChunk(0, height, 0, width)

        stats_task = context.submit_subtask(ComputeLocalStatsForChunk(self.product_path, full_product_chunk))
        context.submit_subtask(
            ComputePrincipalComponents(self.product_path, full_product_chunk, output_key="eigenvectors"),
            depends_on=[stats_task],
        )


class ComputeLocalStatsForChunk(Task):
    """
    ComputeLocalStatsForChunk computes the local statistics for a given chunk of the product.

    The local statistics are the number of samples, the sum of squared deviations matrix, and the mean vector.

    If the chunk is larger than _DEFAULT_SPATIAL_CHUNK_SIZE in either dimension, it is subdivided into smaller chunks
    and the local statistics are computed for each of those smaller chunks in parallel. The statistics from the smaller
    chunks are then combined into a single set of local statistics for the original chunk.
    """

    product_path: str
    chunk: SpatialChunk

    def execute(self, context: ExecutionContext) -> None:
        context.current_task.display = f"LocalStats[{self.chunk}]"
        sub_chunks = self.chunk.immediate_sub_chunks()

        if len(sub_chunks) > 1:
            # more than a single chunk left, so let's sub-divide the work to allow for parallelism
            compute_local_stats = context.submit_subtasks(
                [ComputeLocalStatsForChunk(self.product_path, chunk) for chunk in sub_chunks]
            )
            # after all sub-chunks are done, we need to aggregate the results
            context.submit_subtask(
                CombineChunkStats(self.product_path, output_chunk=self.chunk, input_chunks=sub_chunks),
                depends_on=compute_local_stats,
            )
            return

        assert len(sub_chunks) == 1, "We should have a single chunk left by now"
        assert sub_chunks[0] == self.chunk, "The single chunk should be the same as the original chunk"

        # we only have a single chunk to process, so let's load it and then do PCA
        window = self.chunk.to_rasterio_window()
        with open_wyvern_product(self.product_path) as file:
            arr = file.read(window=window).transpose((1, 2, 0))
            nodata = file.nodata

        # convert the 3D array of shape (y, x, bands) to a feature array of shape (N, bands)
        all_measurements = arr.reshape(arr.shape[0] * arr.shape[1], arr.shape[2])
        # filter out only those pixels where all bands are valid
        all_bands_valid = (all_measurements != nodata).all(axis=1)
        valid_indices = np.where(all_bands_valid)
        valid_measurements = all_measurements[valid_indices]

        n_samples, n_bands = valid_measurements.shape
        if n_samples > 0:
            deviations_matrix, mean_vector = compute_squared_deviations_matrix(valid_measurements)
        else:
            # no valid pixels in this chunk, so we set deviations and mean vectors to zeros
            # combining other stats with these zeros results in a no-op
            deviations_matrix = np.zeros(shape=(n_bands, n_bands), dtype=float)
            mean_vector = np.zeros(shape=(n_bands,), dtype=float)

        product_cache = context.job_cache.group(Path(self.product_path).stem)
        logger.info(f"Product {self.product_path}: Writing stats for {self.chunk}")
        product_cache[self.chunk.key] = pickle.dumps((n_samples, deviations_matrix, mean_vector))


class CombineChunkStats(Task):
    """
    CombineChunkStats combines the local statistics from multiple chunks into a single set of statistics.
    """

    product_path: str
    output_chunk: SpatialChunk
    input_chunks: list[SpatialChunk]

    def execute(self, context: ExecutionContext) -> None:
        input_chunks_repr = ", ".join([f"[{chunk}]" for chunk in self.input_chunks])
        context.current_task.display = f"CombineStats[{self.output_chunk}]\n{input_chunks_repr}"
        product_cache = context.job_cache.group(Path(self.product_path).stem)
        if len(self.input_chunks) == 0:
            return  # no input chunks, so nothing to do

        # three-tuple of (n_samples, deviations_matrix, mean_vector)
        stats = pickle.loads(product_cache[self.input_chunks[0].key])

        # aggregate the local statistics for each chunk
        for chunk in self.input_chunks[1:]:
            chunk_stats = pickle.loads(product_cache[chunk.key])
            stats = combine_local_statistics(*stats, *chunk_stats)

        logger.info(f"Product {self.product_path}: Writing stats for {self.output_chunk}")
        product_cache[self.output_chunk.key] = pickle.dumps(stats)


class ComputePrincipalComponents(Task):
    """
    ComputePrincipalComponents computes the principal components for a given chunk from the chunk statistics that
    are already required to be computed and cached beforehand.
    """

    product_path: str
    chunk: SpatialChunk
    output_key: str

    def execute(self, context: ExecutionContext) -> None:
        context.current_task.display = f"ComputePC[{self.chunk}]"
        product_cache = context.job_cache.group(Path(self.product_path).stem)
        n, deviations_matrix, _ = pickle.loads(product_cache[self.chunk.key])

        # compute the covariance matrix
        covariance_matrix = deviations_matrix / (n - 1)
        eigenvalues, eigenvectors = compute_eigenvectors(covariance_matrix)
        product_cache[self.output_key] = pickle.dumps((eigenvalues, eigenvectors))


def _split_interval(start: int, end: int, max_size: int) -> Iterator[tuple[int, int]]:
    """
    Split an interval into two sub-intervals in case it is larger than the given maximum size.
    The split is done at the closest power of two.

    Example:
        _split_interval(0, 1000, 512) -> (0, 512), (512, 1000)
        _split_interval(512, 1000, 512) -> (512, 1000)
    """
    n = end - start
    if n > max_size:
        split_at = 2 ** math.floor(math.log2(n - 1)) + start
        yield start, split_at
        yield split_at, end
    else:
        yield start, end


app = App()


@app.default
def main(
    cache_bucket: str | None = None,
    cluster: str | None = None,
) -> None:
    if Path(".env").exists():
        assert load_dotenv()

    service_name = f"{os.environ['RUNNER_NAME']}-{os.getpid()}"
    configure_console_logging()
    if os.environ.get("AXIOM_API_KEY"):
        configure_otel_logging_axiom(service_name)
        configure_otel_tracing_axiom(service_name)

    client = WorkflowsClient()  # a workflow client for https://api.tilebox.com

    tasks = [
        ComputePrincipalComponentsForProduct,
        ComputeLocalStatsForChunk,
        CombineChunkStats,
        ComputePrincipalComponents,
    ]

    logger.info(f"Starting runner with {tasks} tasks on {cluster or 'default'} cluster")

    if cache_bucket is not None:
        if not cache_bucket.startswith("gs://"):
            raise ValueError("Expected a google storage bucket URL, but got {cache_bucket}")

        logger.info(f"Using a google storage cache bucket: {cache_bucket}")

        parts = cache_bucket.removeprefix("gs://").split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        cache = GoogleStorageCache(StorageClient().bucket(bucket), prefix=prefix)
    else:
        cache = LocalFileSystemCache()

    runner = client.runner(
        cluster,
        tasks=tasks,
        cache=cache,
    )
    runner.run_forever()


if __name__ == "__main__":
    app()
