# Workflows Cron Automation, Python

This example demonstrates how to use the [Tilebox](https://tilebox.com) SDKs to create a Workflows Cron Automation, a workflow that runs on a schedule, and showcases how to filter timeseries Datasets based on spatial and temporal criteria.

The example uses the [Tilebox Sentinel 2 Open Dataset](https://console.tilebox.com/datasets/explorer/0190bbe6-1215-a90d-e8ce-0086add856c2) and a [Cron Automation](https://docs.tilebox.com/workflows/near-real-time/cron) to load Sentinel-2 data and print statistics every minute.

Specifically, it loads Sentinel-2 metadata, filters it for specific areas of interest, and then calculates and prints statistics.

## Prerequisites

- Python 3.10+
- Environment variables – provide your API key and cluster slug as environment variables via .env file
    - Tilebox API Key – [create here](https://console.tilebox.com/account/api-keys)
    - Tilebox Cluster slug – [create here](https://console.tilebox.com/workflows/clusters)
- Install the `uv` Python package manager – [installation instructions](https://docs.astral.sh/uv/)

## Getting Started

```bash
# Install dependencies
uv sync

# Create the automation and start the workflow runner
uv run python cron_automation.py
```

Executing the Python script initiates the automation. Because the automation is configured with a Cron trigger, allowing the script to run for more than one minute will result in multiple task executions. Each execution is automatically triggered by the Cron schedule and processes a new set of input data.

Check the Automations tab in the [Tilebox Console](https://console.tilebox.com/workflows/automations) to see the automation in action, including all created jobs.

## Cleanup

To delete the automation, go to [https://console.tilebox.com/workflows/automations](https://console.tilebox.com/workflows/automations) and delete the respective entry.
