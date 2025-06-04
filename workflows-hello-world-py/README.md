# Workflows Hello World, Python

This example demonstrates how to use the [Tilebox](https://tilebox.com) SDKs to submit a job and run a worker.

## Prerequisites

- Python 3.10+
- Environment variables – provide your API key as environment variables via .env file
    - Tilebox API Key – [create here](https://console.tilebox.com/account/api-keys)
- Install the `uv` Python package manager – [installation instructions](https://docs.astral.sh/uv/)
- To demonstrate automatic parallelization, install `call-in-parallel` – [installation instructions](https://github.com/tilebox/call-in-parallel)

## Getting Started

To start the notebook, run the following commands:

```bash
# Install dependencies
uv sync

# Run the notebook
uv run --with jupyter jupyter lab .
```
