# Multi-Language Workflows, Python and Go

This example demonstrates a workflow submitted from Go and executed by an async Python task. Both implementations
share the task identifier `tilebox.com/schedule_image_capture` at `v1.0` and the same JSON wire contract.

The task uses native ODC Geo `Geometry` and `Resolution` inputs in Python. The Go submitter encodes their documented
JSON representation, including GeoJSON point coordinates and an EPSG CRS, so no custom Python geometry wrapper is
needed.

## Prerequisites

- Python 3.12+
- Go 1.26+
- Install the `uv` Python package manager – [installation instructions](https://docs.astral.sh/uv/)
- Environment variables – provide your API key as environment variables
    - Tilebox API Key (`TILEBOX_API_KEY`) – [create here](https://console.tilebox.com/account/api-keys)

## Getting Started

To start the runner, run the following commands:

```bash
cd python-runner

# Install dependencies
uv sync

# Validate, publish, and deploy the Python release
tilebox workflow build-release --debug --json
tilebox workflow publish-release --json
tilebox workflow deploy-release --latest --cluster <cluster-slug> --json
tilebox runner start --cluster <cluster-slug>
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

The Python task receives an ODC `Geometry` point in EPSG:4326, a north-up `Resolution`, and the spectral wavelengths,
then logs the request. `go test ./...` verifies the cross-language payload contract.
