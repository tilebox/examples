import os
import pickle
from pathlib import Path

import dotenv
import numpy as np
import pandas as pd
import xarray as xr
from dateutil.parser import parse
from tilebox.datasets import Client as DSClient, TimeseriesDataset
from tilebox.datasets.data import TimeInterval
from tilebox.storage import CopernicusStorageClient
from tilebox.workflows import Client as WFClient, Task, ExecutionContext
from tilebox.workflows.cache import LocalFileSystemCache
from tilebox.workflows.observability.logging import get_logger

from helpers import (
    polygon_from_poi,
    select_least_cloudy_granules,
)

logger = get_logger()


def s2_dataset() -> TimeseriesDataset:
    return DSClient().dataset("open_data.copernicus.sentinel2_msi")


S2_L1C_COLLECTIONS = ["S2A_S2MSI1C", "S2B_S2MSI1C", "S2C_S2MSI1C"]


class DownloadWorkflow(Task):
    start: str
    end: str
    coords_path: str
    download: bool

    def execute(self, context: ExecutionContext):
        # Load the metadata
        load_metadata = context.submit_subtask(
            LoadMetadata(start=self.start, end=self.end, coords_path=self.coords_path),
            max_retries=3,
        )

        # Filter the metadata
        filter_metadata = context.submit_subtask(
            FilterMetadata(coords_path=self.coords_path), depends_on=[load_metadata]
        )

        select_least_cloudy_granules = context.submit_subtask(
            SelectLeastCloudyGranules(), depends_on=[filter_metadata]
        )

        # Download the data
        if self.download:
            context.submit_subtask(
                DownloadData(), depends_on=[select_least_cloudy_granules], max_retries=5
            )
        else:
            context.submit_subtask(
                ListDownloads(), depends_on=[select_least_cloudy_granules]
            )


class LoadMetadata(Task):
    start: str
    end: str
    coords_path: str

    def execute(self, context: ExecutionContext):
        logger.info(f"Loading metadata for {self.start} to {self.end}")
        start = parse(self.start)
        end = parse(self.end)
        time_interval = TimeInterval(start=start, end=end)

        coords = pd.read_csv(self.coords_path)
        coords = coords.to_xarray()

        metadata = None
        for i in range(len(coords.index)):
            logger.info(f"Loading metadata for coordinate {i + 1} of {len(coords.index)}")

            # Load metadata for the coordinate from all L1C collections
            data = [
                s2_dataset()
                .collection(c)
                .query(
                    temporal_extent=time_interval,
                    spatial_extent=polygon_from_poi(
                        coords.lon.isel(index=i), coords.lat.isel(index=i)
                    ),
                )
                for c in S2_L1C_COLLECTIONS
            ]

            # Merge the data into a single dataset
            non_empty = [ds for ds in data if ds]  # filter out empty datasets
            data = xr.concat(non_empty, dim="time") if non_empty else None

            # Add the coordinate index to the data
            data = data.assign(coordinate_idx=("time", np.full(len(data.time), i)))

            logger.info(f"Loaded {len(data.time)} granules")

            # Merge the data into a single dataset
            metadata = xr.concat([metadata, data], dim="time") if metadata else data

        logger.info(f"Loaded {len(metadata.time)} granules in total")

        # Only use a single processing baseline, the most recent one
        processing_baselines = np.unique(
            metadata.processing_baseline
        )  # This returns a sorted list of unique values
        # Use the highest available processing baseline
        metadata = metadata.where(
            metadata.processing_baseline == processing_baselines[-1], drop=True
        )
        logger.info(
            f"Using {len(metadata.time)} granules with processing baseline {processing_baselines[-1]}"
        )

        context.job_cache["metadata"] = pickle.dumps(metadata)


class FilterMetadata(Task):
    coords_path: str

    def execute(self, context: ExecutionContext):
        logger.info(f"Filtering metadata")
        metadata = pickle.loads(context.job_cache["metadata"])

        # Filter the metadata for cloud cover < 5
        filtered = metadata.where(metadata.cloud_cover < 5, drop=True)

        logger.info(f"Filtered to {len(filtered.time)} granules, cloud cover < 5")

        context.job_cache["metadata_filtered"] = pickle.dumps(filtered)


class SelectLeastCloudyGranules(Task):
    def execute(self, context: ExecutionContext):
        logger.info(f"Selecting least cloudy granules")
        metadata = pickle.loads(context.job_cache["metadata_filtered"])

        filtered = select_least_cloudy_granules(metadata)
        logger.info(f"Filtered to {len(filtered.time)} granules")

        context.job_cache["best_candidates"] = pickle.dumps(filtered)


class DownloadData(Task):
    def execute(self, context: ExecutionContext):
        logger.info(f"Downloading data")
        filtered = pickle.loads(context.job_cache["best_candidates"])
        storage_client = CopernicusStorageClient(
            os.getenv("COPERNICUS_ACCESS_KEY"),
            os.getenv("COPERNICUS_SECRET_ACCESS_KEY"),
            Path("s2-data"),
        )
        for i in range(len(filtered.time)):
            logger.info(f"Downloading granule {i + 1} of {len(filtered.time)}")
            storage_client.download(filtered.isel(time=i), show_progress=True)


class ListDownloads(Task):
    def execute(self, context: ExecutionContext):
        logger.info(f"Listing downloads")
        metadata = pickle.loads(context.job_cache["best_candidates"])
        for i in range(len(metadata.time)):
            logger.info(f"Would-be downloading granule {i + 1} of {len(metadata.time)}")
            logger.info(f"  Center: {metadata.isel(time=i).centroid.values}")
            logger.info(f"  Cloud Cover: {metadata.isel(time=i).cloud_cover.values}")
            logger.info(f"  Thumbnail: {metadata.isel(time=i).thumbnail.values}")


def setup_environment() -> str:
    # setup environment
    dotenv.load_dotenv()

    if os.getenv("COPERNICUS_ACCESS_KEY") is None:
        raise ValueError("COPERNICUS_ACCESS_KEY environment variable is not set")
    if os.getenv("COPERNICUS_SECRET_ACCESS_KEY") is None:
        raise ValueError("COPERNICUS_SECRET_ACCESS_KEY environment variable is not set")
    if os.getenv("TILEBOX_API_KEY") is None:
        raise ValueError("TILEBOX_API_KEY environment variable is not set")
    cluster = os.getenv("TILEBOX_CLUSTER")
    if cluster is None:
        raise ValueError("TILEBOX_CLUSTER environment variable is not set")
    return cluster


if __name__ == "__main__":
    cluster = setup_environment()
    wfClient = WFClient()

    # Start a workflow runner right here
    runner = wfClient.runner(
        cluster,
        tasks=[
            DownloadWorkflow,
            LoadMetadata,
            FilterMetadata,
            SelectLeastCloudyGranules,
            DownloadData,
            ListDownloads,
        ],
        cache=LocalFileSystemCache(),
    )

    # Execute the job and exit when finished
    runner.run_forever()
