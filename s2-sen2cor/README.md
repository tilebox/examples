# Sentinel-2 atmospheric correction on a laptop or Azure

Find Sentinel-2 L1C imagery with Tilebox, run ESA Sen2Cor to produce L2A surface
reflectance, store the result on local disk or Azure Blob Storage, and register
its metadata in a queryable custom Tilebox dataset. Each scene also produces a masked 10m NDVI
Cloud-Optimized GeoTIFF (COG) and an RGB thumbnail.

```text
Tilebox L1C query → one task per SAFE product → Sen2Cor → NDVI + thumbnail
                                                    ↓
                                  local disk or private Azure container
                                                    ↓
                                  Tilebox custom L2A metadata dataset
                                                    ↓
                                  notebook: query, download, display
```

The same workflow runs on a laptop or Azure. Set `RESULTS_STORAGE_URL` to a
`file:///...` directory or an Azure container URL; obstore handles result writes,
completion records, and notebook reads through the same API. Local results require
no Azure account or credentials. CDSE input downloads use Tilebox's
Copernicus storage client in both cases. Tilebox still provides the online catalog
and workflow orchestration; laptop execution is not an offline mode.
The workflow processes full single-tile SAFE products. The area of interest (AOI)
selects intersecting products; it does not crop them. By default, each job processes
at most one Sentinel-2A scene to limit cost;
inspect the source dataset's collections before selecting another satellite.

## Set up the catalog and submit a job

Install [uv](https://docs.astral.sh/uv/) and provide a Tilebox API key with access
to the Copernicus catalog and default workflow cluster, plus permission to create
a dataset and ingest results into it. Run these commands from `s2-sen2cor`:

Open the Default cluster in the [Tilebox Console](https://console.tilebox.com) and
copy its slug. Use that full value for `TILEBOX_CLUSTER`; the display name
"Default" and the literal value `default` are not cluster slugs.

```bash
uv sync --locked
export TILEBOX_API_KEY=...       # supply through your secret manager or shell
export TILEBOX_CLUSTER=...       # full slug of your Default cluster
uv run create_catalog.py sen2cor_test
```

`create_catalog.py` creates the custom results dataset and its `L2A` collection,
including the fields for processor versions, NDVI, and thumbnails. Use an unused
code name such as `sen2cor_test` for a test run; the command updates the schema if
that code name already exists. You do not need to create the dataset manually.

Open the dataset in the [Tilebox Console](https://console.tilebox.com) and copy its
full slug, including the workspace prefix. Set it in both the worker and the
terminal used to query results:

```bash
export RESULTS_DATASET=...      # paste the full slug, not just sen2cor_test
```

Create the dataset once before starting workers. The worker does not change its
schema. Use a fresh dataset for incompatible schema changes.

Create CDSE S3 credentials in the
[Copernicus S3 credentials manager](https://eodata-s3keysmanager.dataspace.copernicus.eu/).
Sign in with your CDSE account, choose **Add Credentials**, and set an expiration
date. Copy the secret key when shown; it is only displayed once. See the
[registration instructions](https://documentation.dataspace.copernicus.eu/Registration.html)
if you need an account, and the [S3 access guide](https://documentation.dataspace.copernicus.eu/APIs/S3.html)
for details. Supply these keys to the worker as `CDSE_ACCESS_KEY` and `CDSE_SECRET_KEY`.

Start a laptop worker below, or deploy one using the
[Azure instructions](infrastructure/README.md). This example uses the default Tilebox cluster.
The Azure setup makes result blobs publicly readable; use it only for data you
can share. Its image registry remains private.
Before submitting a job, check that no other worker on that cluster can execute
these task classes with different settings or pick up work intended for your disk.
Then submit a small job from a second terminal with the same Tilebox environment:

```bash
uv run submit.py --start 2025-08-01 --end 2025-09-01 \
  --bounds 54.2 24.2 54.6 24.6 --max-scenes 1
```

Bounds are west, south, east, north in WGS84. The end date is exclusive. Selection
uses acquisition time, intersection with the AOI, and cloud cover ≤20%, then takes
the earliest matching scenes up to `max-scenes`. Subtasks retry twice on failure.
Cloud filtering runs locally because the source field is not server-queryable.
Keep the date range and AOI small: `max-scenes` bounds processing, not query size.

## Run on a laptop

```bash
docker build --platform linux/amd64 -t s2-sen2cor:local .
docker run --rm s2-sen2cor:local L2A_Process --help
```

Put `TILEBOX_API_KEY`, `TILEBOX_CLUSTER`, `RESULTS_DATASET`, `CDSE_ACCESS_KEY`, and
`CDSE_SECRET_KEY` in a `.env` file (`NAME=value`, one per line, no `export`), then
restrict it with `chmod 600 .env`. Run the worker with persistent local results:

```bash
mkdir -p outputs/results
export RESULTS_STORAGE_URL="$(uv run python -c 'from pathlib import Path; print(Path("outputs/results").resolve().as_uri())')"
docker run --rm --platform linux/amd64 --env-file .env \
  -e RESULTS_STORAGE_URL \
  -v "$PWD/outputs:$PWD/outputs" \
  s2-sen2cor:local
```

The bind mount uses the **same absolute path** inside and outside
the container, so cataloged `file://` URLs also work in the host notebook. On Linux,
make `outputs/results` writable by the image's UID 10001, for example with
`sudo chown 10001:10001 outputs/results`. On Docker Desktop, enable sharing of this
directory. Local URLs only work on machines that can access that path; they are
not publicly accessible URLs. Keep laptop and Azure results in separate custom
datasets if you process the same source scenes in both environments.

On Linux x86_64, you can also install
[ESA Sen2Cor 02.12.04](https://step.esa.int/main/snap-supported-plugins/sen2cor/sen2cor-v2-12/),
put `L2A_Process` on `PATH`, export the five variables above, and run
`uv run runner.py` directly. Use a fresh installation and its unmodified default
GIPP configuration; do not reuse settings from an earlier installation.
Before correction, the worker checks the version reported by `L2A_Process --help`
and rejects any version other than 02.12.04, including unrecognized output.
The catalog's `sen2cor_version` field records this checked version. Server-side
string filters use `source_datapoint_id` and `pipeline_version`; the schema uses
Tilebox's two available queryable string fields for those lookups.
Pipeline version `sen2cor-02.12.04-ndvi-v3` stores all output locations as assets.
It does not reuse v1 or v2 completion records. Create a fresh dataset when upgrading
from those schemas; existing rows remain unchanged. The notebook filters for the
current pipeline version.
Without `RESULTS_STORAGE_URL`, native runs write to
`./outputs/results`. For macOS/Windows use the container; Apple Silicon requires
Linux/amd64 emulation and may be substantially slower. This is not a native ARM
Sen2Cor build. Give Docker enough RAM and disk for a full scene.

## Worker configuration

The Dockerfile installs the official Linux x86_64 Sen2Cor **02.12.04** standalone
distribution, verifies its SHA-256, and runs the worker as an unprivileged user.
Python dependencies are locked in `uv.lock`. The worker executes one task at a time.
Start with 8 vCPU, 32 GiB RAM, and 256 GiB disk; measure a representative scene
before changing capacity. Scratch data is deleted when the task finishes.

The execution context logger records selection counts and each scene's source ID,
download, correction, derivation, publication, and catalog registration. Stage
completion messages include elapsed seconds. Retries log when they reuse completed
outputs instead of downloading and processing again. Use `--max-scenes 3` when
submitting a job to process up to three matching scenes.

| Variable | Purpose |
| --- | --- |
| `TILEBOX_API_KEY` | Tilebox authentication |
| `TILEBOX_CLUSTER` | Existing workflow cluster slug |
| `RESULTS_DATASET` | Full custom dataset slug |
| `CDSE_ACCESS_KEY`, `CDSE_SECRET_KEY` | [Create CDSE S3 credentials here](https://eodata-s3keysmanager.dataspace.copernicus.eu/); not an Azure or AWS account key |
| `RESULTS_STORAGE_URL` | `file:///absolute/path` or `https://<account>.blob.core.windows.net/results`; defaults to local `./outputs/results` |
| `AZURE_CLIENT_ID` | User-assigned managed identity; Azure IaC injects this |
| `WORK_DIR` | Scratch directory, `/work` in the image |

To write to Azure from a laptop or on-prem compute, export `RESULTS_STORAGE_URL`
with the Azure container URL. Use an Azure service principal supported by
`DefaultAzureCredential` or workload identity, and grant it Storage Blob Data
Contributor on the results container. Keep credentials out of the image. For an
Azure VM, host networking lets the container reach the managed-identity endpoint.
On-prem workers can use a protected env file:

```bash
docker run --rm --platform linux/amd64 --env-file .env -e RESULTS_STORAGE_URL s2-sen2cor:local
```

The env file is ignored by Git; keep it mode 0600. Docker administrators can still
read container environment variables. A disk-backed volume mounted at `/work`
must be writable by UID 10001. Local `az login` credentials are not automatically
available inside a container.

## Correction and NDVI assumptions

Sen2Cor converts top-of-atmosphere L1C to bottom-of-atmosphere L2A reflectance.
This example requests 10m processing, which also generates the 20m scene
classification layer. NDVI uses B08 (broad NIR) and B04 (red):

```text
reflectance = (DN + BOA_ADD_OFFSET) / BOA_QUANTIFICATION_VALUE
NDVI = (NIR − red) / (NIR + red)
```

Offsets and scale come from `MTD_MSIL2A.xml`; older products without offsets use
zero. DN=0, negative reflectance, zero denominators, clouds, cloud shadows, snow,
and uncertain classifications become nodata (-9999). Only SCL classes 4, 5 and 6
are retained. SCL is resampled with nearest-neighbor onto the 10m band grid.
The original SAFE bands, classification and metadata remain available for other
scientific choices. The RGB thumbnail is a visualization, not a cloud mask.

The image uses Sen2Cor's packaged default configuration without ESA CCI land-cover
data or a digital elevation model (DEM). Its outputs may differ from official
Copernicus L2A products. For production processing, follow
[ESA's configuration guidance](https://step.esa.int/main/snap-supported-plugins/sen2cor/sen2cor-v2-12/)
to supply these auxiliary datasets. Increment `PIPELINE_VERSION` whenever code,
Sen2Cor configuration, or auxiliary inputs change, and rebuild the image. Do not
silently change processing settings under an existing version.

## Query the results

`query_results.py` is a Jupytext notebook stored as readable Python:

```bash
uv sync --group notebook --locked
uv run --group notebook jupytext --to notebook query_results.py
uv run --group notebook jupyter lab query_results.ipynb
```

Set `TILEBOX_API_KEY` and `RESULTS_DATASET`. For local results, run the notebook on
the same laptop with access to the stored paths; no Azure login is needed. For
Azure results, authenticate with `az login`; your user needs Storage Blob Data
Reader on the container. Match the notebook's date range and AOI to the submitted
job. It queries the metadata catalog, reads the selected NDVI and thumbnail from
local disk or Azure, then displays both. NDVI runs in the workflow so
every registered product has a reusable result; the notebook only reads it.

The catalog records acquisition time, footprint, source datapoint ID, processor
and pipeline versions. Output locations appear only in `assets`: `product` for the
SAFE directory, `metadata` for its XML, `ndvi` for the COG, and `rgb` for the image.
The RGB image is for notebook display; it is not registered as a thumbnail asset
because the Console cannot read the worker's local filesystem. Azure Console
previews are also outside this example's scope. The asset schema's `authentication`
field does not configure credentials: the worker and notebook access Azure through
obstore, independently of Tilebox's storage client. URLs contain no SAS tokens.
The notebook authenticates explicitly for Azure. The `product` asset points to a
directory/blob prefix, not an HTTP directory listing or downloadable ZIP.

## Retry behavior and storage

Each attempt uploads to a unique prefix. After all files arrive, it conditionally
creates `<pipeline>/<source-id>/complete.json`. If another attempt already won,
the loser uses the winner's record. Only then is metadata ingested with
`allow_existing=True`. A retry after a catalog failure reads the completion record
and skips download and correction. The SDK assigns datapoint IDs; the example
replays exactly the same metadata rather than inventing IDs.

Incomplete and losing attempts can leave unreferenced blobs. Clean these by
comparing attempt prefixes against completion records after jobs finish; do not
set a blanket lifecycle expiry on `attempts/`, because successful products also
live there. A hard process/VM failure may leave scratch files until the container
is replaced. The example does not resume a partially processed SAFE or checkpoint
inside Sen2Cor. Preserve both catalog and blob data for as long as results matter.

## Local checks

```bash
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
```

Tests cover reflectance offsets, masks, real raster I/O and COG georeferencing,
real local/memory object stores, concurrent publications, notebook reads, and
replay after catalog failure without Azure credentials. They do not replace a
real-scene smoke test with your CDSE and Tilebox credentials, plus Azure credentials
if using Blob Storage. Before scaling, process one scene and check its full SAFE output,
NDVI mask, catalog query, and authenticated notebook download.
