# Multi-Language Workflows, Python and Go

This example demonstrates how to use the [Tilebox](https://tilebox.com) SDKs to create a Workflows that uses tasks implemented in different languages, Python and Go. Go is used to submit the job, while Python is used to execute it.

## Prerequisites

- Python 3.10+
- Go 1.24+
- Install the `uv` Python package manager – [installation instructions](https://docs.astral.sh/uv/)
- Environment variables – provide your API key as environment variables
    - Tilebox API Key (`TILEBOX_API_KEY`) – [create here](https://console.tilebox.com/account/api-keys)

## Getting Started

To start the runner, run the following commands:

```bash
cd python-runner

# Install dependencies
uv sync

# Start the runner
uv run runner.py
```

In a separate terminal, start the Go server:

```bash
cd go-server

go run .
```

Submit a job by calling the server's `/submit` endpoint:

```bash
curl http://localhost:8080/submit?lat=40.75&lon=-73.98&resolution=30&bands[]=489.0,560.6,666.5
```

The runner should execute the task and print the parameters to stdout.
