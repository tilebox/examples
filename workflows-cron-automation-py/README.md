# Workflows Cron Automation, Python

This example demonstrates a Tilebox Cron Automation that queries Sentinel-2 statistics on a schedule.

`S2Stats` is an async `CronTask`. Its inputs use native Shapely geometries and a standard-library `timedelta`. On every
trigger it builds a `TimeInterval`, queries the AWS Earth Search Sentinel-2 L2A dataset concurrently for all areas, and
logs scene counts and average cloud cover.

Specifically, it loads Sentinel-2 metadata, filters it for specific areas of interest, and then calculates and prints statistics.

## Prerequisites

- Python 3.12+
- Environment variables – provide your API key as environment variables via .env file
    - Tilebox API Key – [create here](https://console.tilebox.com/account/api-keys)
- Install the `uv` Python package manager – [installation instructions](https://docs.astral.sh/uv/)

## Getting Started

```bash
# Install dependencies
uv sync

# Validate, publish, and deploy the workflow release
tilebox workflow build-release --debug --json
tilebox workflow publish-release --json
tilebox workflow deploy-release --latest --cluster <cluster-slug> --json
tilebox runner start --cluster <cluster-slug>

# In another terminal, create the automation once
uv run python cron_automation.py
```

`cron_automation.py` idempotently creates the automation; it no longer embeds or starts a runner. The deployed release
owns task execution. The sample schedule runs every minute and each trigger examines the preceding 24 hours.

Check the Automations tab in the [Tilebox Console](https://console.tilebox.com/workflows/automations) to see the automation in action, including all created jobs.

## Cleanup

To delete the automation, go to [https://console.tilebox.com/workflows/automations](https://console.tilebox.com/workflows/automations) and delete the respective entry.
