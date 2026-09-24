# Sentinel-2 atmospheric correction on a laptop or Azure

Query Sentinel-2 L1C imagery with Tilebox, download it from Copernicus Data Space
(CDSE), and run Sen2Cor 02.12.04 in Docker. Each scene produces an L2A SAFE product,
a masked 10 m NDVI Cloud-Optimized GeoTIFF (COG), and an RGB image. Files stay on
your disk; their metadata and asset locations go into a custom Tilebox dataset.

The workflow processes full tiles, not crops of the selected area. It needs an
internet connection for CDSE, the Tilebox catalog, and workflow orchestration.
No Azure account is needed for the steps below.

For Azure deployment, follow the [infrastructure guide](infrastructure/README.md)
after creating the results dataset. It creates publicly readable result blobs
and a private image registry; use it only for data you can share.

## 1. Set up credentials

Install [Docker](https://docs.docker.com/get-started/get-docker/) and
[uv](https://docs.astral.sh/uv/getting-started/installation/). Run all commands
from this `s2-sen2cor` directory in Bash (use WSL on Windows).
The worker processes one scene at a time. Our three-tile test peaked at about
12 GiB RAM; allow headroom for other scenes and disk space for the image, scratch
files, and results. Apple Silicon uses amd64 emulation and may be slower.

You need:

- A Tilebox API key with access to the Copernicus dataset and Default workflow
  cluster, plus permission to create a dataset and ingest results.
- The Default cluster's full slug from the [Tilebox Console](https://console.tilebox.com).
  Copy the slug, not the display name `Default` or the literal string `default`.
- CDSE S3 keys from the [credentials manager](https://eodata-s3keysmanager.dataspace.copernicus.eu/).
  Sign in, choose **Add Credentials**, set an expiry, and save the secret when
  shown. These are S3 keys, not your CDSE password. See
  [CDSE registration](https://documentation.dataspace.copernicus.eu/Registration.html)
  if you need an account.

Create a private environment file:

```bash
touch .env
chmod 600 .env
```

Edit `.env` with these values, one per line. Leave `RESULTS_DATASET` empty for now:

```dotenv
TILEBOX_API_KEY=your-api-key
TILEBOX_CLUSTER=your-default-cluster-slug
CDSE_ACCESS_KEY=your-s3-access-key
CDSE_SECRET_KEY=your-s3-secret-key
RESULTS_DATASET=
```

Git ignores `.env`. Keep it private; Docker administrators can read container
environment variables.

## 2. Create the results dataset

```bash
uv sync --locked
uv run --env-file .env create_catalog.py sen2cor_test
```

Use an unused code name for a test run. The script creates the dataset and its
`L2A` collection; if the name exists, it updates that dataset's schema.
Copy the dataset's full slug, including your organization prefix, from the Console
into `RESULTS_DATASET` in `.env` (for example, `your-org.sen2cor_test`, not
`tilebox.sen2cor_test`). The script does not print the slug. If you have
the Tilebox CLI installed, `uv run --env-file .env tilebox dataset list` also
lists dataset slugs. Do this once, before starting the worker. Use separate
datasets for laptop and Azure results.

## 3. Start the worker

```bash
docker build --platform linux/amd64 -t s2-sen2cor:local .
mkdir -p outputs/results
```

On Linux, make the result directory writable by the container's UID 10001:

```bash
sudo chown 10001:10001 outputs/results
```

On Docker Desktop, allow sharing of this directory. Then start the worker:

```bash
export RESULTS_STORAGE_URL="$(uv run python -c 'from pathlib import Path; print(Path("outputs/results").resolve().as_uri())')"
docker run --rm --platform linux/amd64 --env-file .env \
  -e RESULTS_STORAGE_URL \
  -v "$PWD/outputs:$PWD/outputs" \
  s2-sen2cor:local
```

Leave this terminal running. The mount uses the same absolute path inside and
outside Docker so the notebook can read the cataloged `file://` assets. Those
paths work only on machines that can access that directory.

## 4. Submit a job

Open a second terminal in `s2-sen2cor`. Check that no other worker on the Default
cluster runs these task classes with different settings or storage paths.

```bash
uv run --env-file .env submit.py --start 2025-08-01 --end 2025-09-01 \
  --bounds 54.2 24.2 54.6 24.6 --max-scenes 1
```

This selects the earliest matching Sentinel-2A scene near Abu Dhabi with cloud
cover ≤20%. Bounds are west, south, east, north in WGS84; the end date is exclusive.
Use `--max-scenes 3` to process up to three scenes. Keep the area and date range
small: this limit bounds processing, not the initial catalog query.

Follow the job in the Console. Context logs record selection counts, source IDs,
processing stages, elapsed seconds, and reuse on retry. Docker's console formatter
shows message text; structured fields are retained in Tilebox's API logs.
The final `L2A result registered; NDVI: file:///.../ndvi.tif` message gives the
stored file's location directly in the worker terminal.
Wait for the job to complete before querying results or stopping the worker with
Ctrl-C. Scene tasks retry twice on failure.

## 5. View the results

Run the notebook on the same machine, with access to `outputs`:

```bash
uv sync --group notebook --locked
uv run --group notebook jupytext --to notebook query_results.py
uv run --group notebook --env-file .env jupyter lab query_results.ipynb
```

Run the notebook cells. Its default dates and bounds match the example job;
change them if you submitted a different query. It queries the catalog, downloads
one result's NDVI and RGB assets, and displays them. NDVI is computed by the
workflow, not the notebook. The plot stretches its colors between the 2nd and
98th percentiles of valid displayed pixels; the stored NDVI values are unchanged.

The notebook lists every matching NDVI asset URI, then prints
`Local NDVI copy: /absolute/path/outputs/<result-id>/ndvi.tif` for the displayed
result. That is a local GeoTIFF you can open in QGIS or read with Rasterio.
The original files remain under `outputs/results`.

To query the catalog from the terminal, install the
[Tilebox CLI](https://docs.tilebox.com/agents-and-ai-tools/tilebox-cli) and
[jq](https://jqlang.org/download/), then run:

```bash
uv run --env-file .env sh -c 'tilebox dataset query "$RESULTS_DATASET" \
  --collections L2A --after 2025-08-01 --before 2025-09-01 --limit 100 --json' \
  | jq -r '.datapoints[].assets | . as $a | .assets[] | select(.key == "ndvi") |
    $a.access_profiles[.primary.access_profile_index].base_href + .primary.href'
```

This lists NDVI asset URIs for up to 100 results in the example month, whether
stored locally or in Azure. The CLI returns each URI as a base and relative path;
`jq` joins them. The inner shell reads `RESULTS_DATASET` from `.env`.
Omit the pipe to inspect complete catalog records. To list files on disk instead:

```bash
find "$PWD/outputs/results" -name ndvi.tif -type f
```

Each catalog row retains the source time, footprint, ID, and processor/pipeline
versions. All output locations are in `assets`: `product` (SAFE directory),
`metadata` (XML), `ndvi` (COG), and `rgb` (image). The SAFE asset is a directory
prefix, not a ZIP. RGB is for notebook display; no Console thumbnail is registered
because the Console cannot read your filesystem.

## Processing assumptions and retries

Sen2Cor converts L1C top-of-atmosphere reflectance to L2A surface reflectance.
The image verifies the official installer checksum and uses Sen2Cor's default
configuration without an external DEM or ESA CCI land-cover data. Results may
differ from official Copernicus L2A. See
[ESA's configuration guidance](https://step.esa.int/main/snap-supported-plugins/sen2cor/sen2cor-v2-12/)
before production use.

NDVI uses 10 m B08 (NIR) and B04 (red), with offsets and scale from `MTD_MSIL2A.xml`:

```text
reflectance = (DN + BOA_ADD_OFFSET) / BOA_QUANTIFICATION_VALUE
NDVI = (NIR − red) / (NIR + red)
```

Missing offsets default to zero. DN=0, negative reflectance, zero denominators,
and scene-classification (SCL) classes other than 4, 5, or 6 (vegetation, bare soil,
water) become nodata (-9999). The 20 m SCL layer is resampled by nearest-neighbor
onto the 10 m grid. The full SAFE remains available
for other masks or calculations; the RGB image is not a cloud mask.

After uploading all files, a task writes a completion record, then registers the
metadata. Retries reuse that record without downloading or correcting again.
Concurrent attempts use separate paths; the first completed attempt wins.
Failed or losing attempts can leave unreferenced files. Clean them by comparing
attempt paths with completion records after jobs finish; never expire the entire
`attempts/` directory, which also contains successful results. Scratch files are
removed on task exit, but may survive a hard crash. Partial Sen2Cor runs restart
from the beginning.

Increment `PIPELINE_VERSION` and rebuild when changing processing code,
configuration, or auxiliary data. The current `sen2cor-02.12.04-ndvi-v3` schema
uses assets only and does not reuse v1/v2 records. Use a fresh dataset when
upgrading from those schemas. The notebook filters for the current version.

## Other execution options

**Native Linux x86_64:** Install
[Sen2Cor 02.12.04](https://step.esa.int/main/snap-supported-plugins/sen2cor/sen2cor-v2-12/)
with its unmodified default GIPP configuration and put `L2A_Process` on `PATH`.
Run `uv run --env-file .env runner.py`. The worker checks the executable's version
before processing and rejects anything other than 02.12.04. Native runs default
to `outputs/results`; set `WORK_DIR` to choose a scratch directory.

**Azure Blob Storage:** The same code uses obstore for local and Azure result
writes, completion records, and notebook reads. Set `RESULTS_STORAGE_URL` to
`https://<account>.blob.core.windows.net/<container>` and provide an identity
supported by `DefaultAzureCredential`. Workers need Storage Blob Data Contributor;
notebook users need Storage Blob Data Reader. A host notebook can use `az login`.
Containers do not inherit that login: supply service-principal or workload-identity
credentials, or use host networking on an Azure VM to reach managed identity.
Set `AZURE_CLIENT_ID` when selecting a user-assigned identity. Keep secrets out of
the image and URLs. Tilebox asset metadata does not configure authentication;
obstore handles it separately. Azure Console previews are outside this example.
The infrastructure setup injects `AZURE_CLIENT_ID` for the VM's identity.

## Local checks

```bash
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
```

Tests cover NDVI masks and offsets, raster output, asset reads, concurrent writes,
and retries. Before scaling, run a real scene and check its SAFE output, NDVI,
catalog row, and notebook reads with your credentials.
