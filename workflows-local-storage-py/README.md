# Local filesystem automation, Python

Drop a JPG into a local folder, and a native Tilebox automation runs a Python task that writes a
thumbnail and JSON metadata. Images stay local; Tilebox hosts the automation and jobs.

## Setup (once)

You need Python 3.12+, [uv](https://docs.astral.sh/uv/), the [Tilebox CLI](https://tilebox.com/docs/cli),
and a `TILEBOX_API_KEY` from [Console](https://console.tilebox.com/settings/api-keys).
Run everything on the same machine, from this directory.

Download three [CC0 sample photos](https://github.com/OpenDroneMap/odm_data_aukerman).
`uv run` installs the Python dependencies automatically:

```bash
uv run demo.py download
```

Set `TILEBOX_API_KEY` in your shell, then create a dedicated cluster and workflow:

```bash
tilebox cluster create "Local thumbnail demo" --json
export TILEBOX_CLUSTER='<returned cluster slug>'
tilebox workflow create "Local storage thumbnails" --json
```

Set `workflow.slug` in `tilebox.workflow.toml` to the returned workflow slug, then publish and deploy:

```bash
tilebox workflow publish-release
tilebox workflow deploy-release --latest --cluster "$TILEBOX_CLUSTER"
```

Register the watched folder and create the automation:

```bash
mkdir -p data/incoming
tilebox storage-location create local \
  --name "Local input folder" --path "$(pwd)/data/incoming" --json
export LOCATION_ID='<returned storage location UUID>'
uv run demo.py register --location "$LOCATION_ID" --cluster "$TILEBOX_CLUSTER"
```

## Run

Keep the API key and saved values available in each terminal. Start the runner in one:

```bash
tilebox runner start --cluster "$TILEBOX_CLUSTER" -n 2
```

Start the native watcher in the other:

```bash
tilebox storage-location watch "$LOCATION_ID"
```

Once both processes are ready, import the photos from a third terminal:

```bash
uv run demo.py import
```

Watch the jobs in [Console](https://console.tilebox.com/workflows/automations), then open
the generated `thumbnail.jpg` and `metadata.json` files under `results/`.

To run again, use `uv run demo.py import --batch take-02`. When jobs finish, stop the watcher
and runner with Ctrl+C. You can remove the demo automation and deployment in Console.
