# Drone survey processing with local storage automation

Watch a local folder and run one Tilebox job for each arriving JPG. `PreparePhoto` reads the
photo through `trigger.storage.read`, extracts camera metadata and GPS coordinates with Pillow,
measures blur and exposure with OpenCV, and saves an oriented preview and JSON report locally.
Three dynamic-runner slots can process photos concurrently.

This arrival demo does **not** create an orthomosaic. Optional `ReconstructSurvey` submits a
completed survey to a local NodeODM server through PyODM. It calls `Node.create_task`, waits for
OpenDroneMap, and downloads the resulting GeoTIFF. Reconstruction runs only when you explicitly
submit a survey manifest, never once per image.

Photos, previews, and reconstruction outputs stay on your workstation. Tilebox hosts workflow
releases, event notifications, job orchestration, and logs, including photo metadata and GPS.
This is local processing with a hosted control plane; the recording needs internet access and a
Tilebox account. There is no AWS setup or cloud output bucket.

## Install and check locally

Use macOS or Linux, Python 3.12+, [uv](https://docs.astral.sh/uv/getting-started/installation/),
and the [Tilebox CLI](https://docs.tilebox.com/cli). These instructions were checked against CLI
0.12.0 and SDK 0.65.0. Docker with Compose is needed only for reconstruction.

Run commands from this example directory, whether cloned or extracted from the demo zip:

```bash
cd drone-survey
uv sync --locked --python 3.12
tilebox --version

# Public download; no Tilebox credentials or Docker needed.
# You can skip this if the zip already contains data/samples/*.JPG.
uv run demo.py download

uv run demo.py check
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
tilebox workflow build-release --api-key local-build-only --json
```

`check` directly processes every cached JPG without Tilebox API calls. It prints each report and
writes `results/check/photos/<hash>/preview.jpg` and `metadata.json`. Open a preview with your
file manager. The 12 default samples are 4896×3672 pixels and produce 960×720 previews. Reports
contain the source key, SHA-256, camera model, camera timestamp, optional timezone offset, GPS
point in longitude/latitude order, and quality measurements. Missing timezone offsets remain
unknown; camera timestamps are not treated as UTC.

Laplacian variance is a relative sharpness measure on an image resized to at most 1600 pixels
per side. Dark/bright fractions count grayscale pixels at ≤5 and ≥250. These measurements are
not calibrated pass/fail criteria for photogrammetry.

The release build is local. CLI 0.12.0 requires a nonempty API-key argument even for this command;
`local-build-only` is a dummy value, not a credential. Use a real key for the remote setup below.
The build allowlist contains only `workflow.py`, `pyproject.toml`, and `uv.lock`; it excludes
photos, results, tests, environment files, and the virtual environment.

## Public photos and license

The downloader uses [OpenDroneMap's Aukerman dataset](https://github.com/OpenDroneMap/odm_data_aukerman/tree/4e8031630f4193494c79b1c1d3524108826d1ba9),
pinned to that revision. It downloads the first 12 JPGs in filename order, about 89 MiB, and checks
their Git blob hashes against the pinned GitHub listing. These are real geotagged aerial photos
from a Sony DSC-WX220. Pillow identifies their MPF metadata as MPO; the example processes the
primary JPEG frame.

The photos are dedicated to the public domain under **CC0-1.0**. `data/CC0-1.0.txt` contains the
dataset's license and `data/SOURCE.txt` records its source. Credit “OpenDroneMap Aukerman sample
dataset (CC0)” in the recording or article so readers can find the same inputs. The example code
uses this repository's MIT license. Downloaded photos are gitignored.

## Set up the Tilebox demo on your workstation

The following commands **create remote resources** in your Tilebox organization. Run them when
you are ready to record. Do not run registration from an orb and expect it to watch your laptop:
the registered path and the runner must refer to the same machine and filesystem.

1. Export `TILEBOX_API_KEY` securely in each terminal, or use your existing shell secret manager.
   Get a key from [Console](https://console.tilebox.com/account/api-keys). Do not put it in source
   code or a recording. The example does not load `.env` automatically.
2. Create a dedicated cluster with no runners on other machines, and create the workflow:

   ```bash
   tilebox cluster create "Drone survey local demo" --json
   export TILEBOX_CLUSTER='<cluster slug returned above>'
   tilebox workflow create "Drone survey demo" --json
   ```

   Set `workflow.slug` in `tilebox.workflow.toml` to the exact workflow slug returned above.
   Reuse existing demo resources rather than creating new ones for each take.

3. Publish and deploy the release, then register the input folder:

   ```bash
   tilebox workflow build-release --json
   tilebox workflow publish-release --json
   tilebox workflow deploy-release --latest --cluster "$TILEBOX_CLUSTER" --json

   mkdir -p data/incoming
   tilebox storage-location create local \
     --name "Drone survey incoming" --path "$(pwd)/data/incoming" --json
   export LOCATION_ID='<storage location UUID returned above>'

   uv run demo.py register --location "$LOCATION_ID" --cluster "$TILEBOX_CLUSTER"
   ```

   Save the automation ID printed by `register` for cleanup. It refuses duplicate registration
   for the same location and cluster. The automation matches `**.[jJ][pP][gG]`, so `.part` files
   and `survey.json` cannot launch photo jobs. Keep registration single-threaded.

   `register` stores the **absolute** path to this copy's `results/` in the task input. Release
   workers change working directory. If you move the example, register the new input path and
   replace the old automation; otherwise its output path still points to the old copy.

4. Start the dynamic runner in terminal A:

   ```bash
   tilebox runner start --cluster "$TILEBOX_CLUSTER" -n 3
   ```

   Use this release runner, not a static `python workflow.py` process. The first run may need
   time to download the release and dependencies. Keep this runner on the workstation with
   the photos. Do not attach remote runners to this local-only cluster.

5. In terminal B, export the same API key and `LOCATION_ID`, then start the watcher:

   ```bash
   tilebox storage-location watch "$LOCATION_ID" --no-gitignore
   ```

   **`--no-gitignore` is required:** `data/` is deliberately gitignored. Wait for the watcher to
   start before importing. It watches new changes; do not rely on it to replay pre-existing
   files or files copied while it was stopped.

## Record the arrival demo

Open `data/incoming` in a file manager, the runner terminal, and
[Tilebox Console](https://console.tilebox.com/workflows/automations). Select your automation.
Keep API keys and unrelated jobs out of the capture. In terminal C, from this example directory:

```bash
uv run demo.py import
```

The importer copies cached photos into a new `survey-<UTC timestamp>/` directory. Each file is
written with a `.part` suffix and atomically renamed to `.JPG` on the same filesystem. After all
copies finish, it writes `survey.json` and prints the manifest key. Source photos remain intact.

For easier-to-follow arrivals, use `uv run demo.py import --interval 0.3`. This delays file copies
only; it does not pad processing times. Use the default burst import to give the three runner
slots work together. Per-photo processing can be brief, so do not promise sustained concurrency
or a particular duration. The Console and runner show the actual job timing.

Wait for the photo jobs to complete, then show a `results/photos/<hash>/preview.jpg` and its
`metadata.json`. Results use the source key and content hash, so retries overwrite the same
outputs rather than creating new result directories. Event delivery can create duplicate jobs;
count unique `source_key` values when checking that all 12 photos were handled.

Repeat `import` for another take; each run gets new keys. After `download --full`, import copies
all 77 cached photos, so finish the 12-photo recording before downloading the full survey.
Do not describe the preview or quality report as an orthomosaic.

If no jobs appear, check that the watcher was running before import, `--no-gitignore` is present,
the automation location matches `LOCATION_ID`, the release is deployed to the same cluster,
and the runner can read the registered absolute directory. Use
`tilebox storage-location events "$LOCATION_ID" --json` to inspect notified events.

## Optional real reconstruction

Do this separately from the short arrival recording. Use a machine with at least 16 GiB RAM
available to Docker and tens of GiB of free disk for the full dataset; required memory and time
depend on the imagery and options. A 12-photo subset can exercise the API but does not cover the
whole survey. No fixed processing time is promised.

Stop the watcher while preparing the full survey if you do not want another 77 preparation jobs.
Let any photo jobs finish, stop the `-n 3` runner, and use one runner slot for reconstruction.

```bash
uv run demo.py download --full
uv run demo.py import
# Save the printed survey-.../survey.json key; use this completed 77-photo manifest.

docker compose up -d nodeodm
export NODEODM_URL='http://127.0.0.1:3000'
uv run python -c 'from workflow import nodeodm; print(nodeodm().info())'

# Run in terminal A, with the API key and cluster set.
tilebox runner start --cluster "$TILEBOX_CLUSTER" -n 1
```

In terminal C, submit the manifest once:

```bash
uv run demo.py reconstruct \
  --location "$LOCATION_ID" --cluster "$TILEBOX_CLUSTER" \
  --manifest 'survey-<timestamp>/survey.json'
```

NodeODM is bound to loopback, uses a persistent Docker volume, and processes one survey at a time.
Both the server and task cap OpenDroneMap's internal concurrency at **2**. The task uses
`fast-orthophoto` and requests 5 cm/pixel output, without claiming that resolution is positional
accuracy. This path creates an orthophoto, not a dense point-cloud product. Keep Docker running
until assets have downloaded. Results are written to `results/surveys/<manifest-sha256>/`:

- `nodeodm-task.json`: reserved task UUID and manifest key, written before upload.
- `odm_orthophoto.tif`: the real orthophoto; open it in QGIS.
- `report.pdf`: copied when NodeODM supplies it.

The default NodeODM URL needs no additional environment setup. For an override, set
`NODEODM_URL` in the **runner's** environment before starting it, not only in the submit terminal.
PyODM 1.5.13 chooses HTTPS only on port 443; this example rejects HTTPS on other ports, HTTP on
443, URL paths, and embedded credentials. `NODEODM_TOKEN` is optional for your own secured server.

### Recover without creating another reconstruction

Tilebox task retries and PyODM upload retries are disabled for this operation. An exclusive
receipt file also blocks another submission of the exact same manifest into the same output
directory. Changing the manifest, output directory, or deleting the receipt bypasses that guard;
it is not global exactly-once execution.

If the runner disconnects, NodeODM may still be working. **Do not retry the Tilebox job or submit
another one blindly.** Read the saved UUID and inspect or download that task directly:

```bash
export RECEIPT='/absolute/path/to/results/surveys/<hash>/nodeodm-task.json'
uv run python - <<'PY'
import json
import os
from pathlib import Path
from workflow import nodeodm

receipt = Path(os.environ['RECEIPT'])
task = nodeodm().get_task(json.loads(receipt.read_text())['task_id'])
print(task.info())
# Wait for the existing task; this does not create one.
task.wait_for_completion(interval=10)
print(task.download_assets(str(receipt.parent / 'recovered-assets')))
PY
```

If creation/upload failed, the reserved UUID may refer to an incomplete upload or no server task.
Inspect NodeODM and its logs before removing the receipt or starting again. Cancel/remove the old
task in NodeODM before deliberately resubmitting. Canceling a Tilebox job does not cancel the
external NodeODM task. NodeODM's default cleanup removes finished tasks after two days; download
needed assets promptly. Stop the old runner before attempting recovery from another process.

## Cleanup

1. Stop the watcher and runner with Ctrl+C. Cancel any remaining demo jobs in Console.
2. Delete the demo automation in [Console](https://console.tilebox.com/workflows/automations),
   or use its saved ID:

   ```bash
   export AUTOMATION_ID='<automation UUID>'
   uv run python - <<'PY'
   import os
   from tilebox.workflows import Client
   Client().automations().delete(os.environ['AUTOMATION_ID'], cancel_jobs=True)
   PY
   tilebox workflow undeploy-release --cluster "$TILEBOX_CLUSTER"
   ```

   Delete the workflow/cluster only if you created them exclusively for this demo. CLI 0.12.0
   has no storage-location delete command; stop the watcher and remove its automation to stop
   new work. The registered location itself does not run a background watcher.
3. If you ran NodeODM, use `docker compose down`. This keeps its task data. Only after saving
   anything you need, use `docker compose down --volumes` to delete its task data permanently.
4. Delete `data/incoming/` and `results/` when no job is reading them. Keep `data/samples/`,
   `data/SOURCE.txt`, and `data/CC0-1.0.txt` for the next recording, or delete all of `data/`
   to reclaim disk. Do not delete the original photos from your own surveys.

## Verification scope

Offline tests cover SDK local-storage reads, Sony MPO/JPEG handling, orientation, GPS and time
metadata, truncated inputs, repeated processing, complete imports, SDK automation registration,
one-task reconstruction submission with zero retries, NodeODM URL handling, and receipt-based
duplicate protection. Reconstruction unit tests substitute the external ODM server.

On 2026-10-11, this release was also tested against the live Tilebox API in a Linux orb with a
dedicated workflow, cluster, local storage location, and automation. A real watcher with
`--no-gitignore` observed the import, and a real dynamic runner with `-n 3` completed all 12 photo
jobs. Each job had one successful task span, one local read span, and a `Photo prepared` log.
Trace timestamps showed a peak of **two overlapping task executions**; three configured slots
did not imply three simultaneously executing tasks in this short run. The `.part`, `.txt`, and
manifest controls created no jobs. All output hashes, metadata, and 960×720 previews were
checked. The runner and watcher were stopped afterward.

The same workflow code also completed a real NodeODM 3.6.2 reconstruction in a separate Linux
orb on the 12-photo subset: a 28,849,881-byte, 4265×4443, four-band GeoTIFF in EPSG:32617 with
approximately 0.05 m pixels. That check called `ReconstructSurvey.execute` through real SDK local
storage with a minimal logging context; it did not use Tilebox orchestration. Visual inspection
showed a recognizable stitched park scene, incomplete boundaries, and a central hole. This is
an API/reconstruction smoke test, not survey-quality output. Use the complete 77-photo survey
for reconstruction, and assess coverage and accuracy before using any output.

These checks do not establish workstation-specific timing or macOS behavior. Rehearse the live
path on your workstation before recording. `demo.py check` alone only exercises local processing.
