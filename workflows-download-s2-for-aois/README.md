# Workflows Download S2 Data for Points of Interest, Python

This example demonstrates how to use the [Tilebox](https://tilebox.com) SDKs to create a Workflows to find Sentinel-2 data for a set of points of interest (POIs), filter the data to be as cloud-free as possible and finally download the data.

The example uses the [Tilebox Sentinel 2 Open Dataset](https://console.tilebox.com/datasets/explorer/0190bbe6-1215-a90d-e8ce-0086add856c2) and a the [Copernicus Dataspace Storage Client](https://docs.tilebox.com/datasets/storage-clients#copernicus-data-space) to load Sentinel-2 metadata, progressively filter it, and finally, download the imagery.

Specifically, it loads Sentinel-2 metadata,
- filters it for a maximum cloud coverage
- filters it for the latest processing baseline
- applies a preliminary (vectorized) filter to remove granules that are too far from POIs based on haversine distance
- applies a precise filter that tests if POIs are within the footprint of the granule
- selects the granule with the lowest cloud coverage
- deduplicates granules across POIs
- downloads the granules

## Prerequisites

- Python 3.10+
- Environment variables – provide your API key and cluster slug as environment variables via .env file, as well as Copernicus Dataspace credentials
    - Tilebox API Key – [create here](https://console.tilebox.com/account/api-keys)
    - Tilebox Cluster slug – [create here](https://console.tilebox.com/workflows/clusters)
    - Copernicus Dataspace credentials – [see links here](https://docs.tilebox.com/datasets/storage-clients#copernicus-data-space)
- Install the `uv` Python package manager – [installation instructions](https://docs.astral.sh/uv/)

## Getting Started

```bash
# Install dependencies
uv sync

# Configure the job in submit_job.py
# Then submit the job
uv run python submit_job.py

# Start the runner
uv run python download_workflow.py
```

This job does not spatio-temporal capabilities yet, so for longer time-periods it will load large amounts of metadata, and might take a while to complete. You can watch teh logs, or the Console [Jobs tab](https://console.tilebox.com/workflows/jobs) to follow the progress.

The workflow is not parallelized but could be in case a larger number of nodes is available with independent internet bandwidth.
