# Seasonal RGB Timelapse

This introductory Tilebox workflow builds an animated WebP from Sentinel-2 imagery. It
demonstrates the core workflow pattern: a root task plans the work, independent
tasks run in parallel, and a final task waits for their outputs.

```text
BuildSeasonalTimelapse
  ├── RenderSeasonalFrame × N
  └── AssembleTimelapse (waits for every frame)
```

The workflow queries Sentinel-2 once for the complete time range, keeps scenes
that fully cover the requested square, and uses xarray to select the scene with
the lowest reported cloud percentage in each three-month period. Periods
without a matching scene are skipped. The workflow fails only when no scenes
are available.

The periods are written as explicit month ranges—`December–February`,
`March–May`, `June–August`, and `September–November`—rather than unexplained
DJF/MAM/JJA/SON codes or hemisphere-specific season names.

## What this example teaches

- A `Task` declares typed, serializable job inputs.
- A root task can query a Tilebox dataset, process its xarray result, and submit
  dynamic subtasks.
- Independent frame tasks can run concurrently.
- `depends_on` creates a barrier before animation assembly.
- Task displays, progress, logs, retries, and tracing make work observable.
- Native values such as `datetime`, `UUID`, and Shapely geometry can cross task
  boundaries directly.

## Job inputs

`BuildSeasonalTimelapse` accepts:

- `center_lat_lon: tuple[float, float]` — latitude and longitude of the center.
- `square_width_km: float` — width and height of the square area.
- `time_range: tuple[datetime, datetime] | None = None` — UTC range to process;
  defaults to the three years ending at midnight after the current UTC day.
- `max_cloud_percent: float = 20.0` — maximum scene cloud percentage.

JSON represents tuples as arrays and datetimes as RFC 3339 strings.

## Run the workflow

You need `uv`, the Tilebox CLI, and a Tilebox API key. Export the key and verify
that the CLI can reach your account:

```bash
export TILEBOX_API_KEY=...
tilebox account get --json
```

Install the dependencies and verify the release locally:

```bash
uv sync
tilebox workflow build-release
```

Publish and deploy the workflow to your default cluster:

```bash
tilebox workflow publish-release --json
tilebox workflow deploy-release --latest --json
```

If the default cluster uses a local dynamic runner, start one in another
terminal. Concurrency allows independent frame tasks to overlap:

```bash
tilebox runner start --concurrency 4
```

Submit a three-year job around Vienna and wait for it to finish:

```bash
tilebox job submit \
  --name vienna-seasonal-timelapse \
  --task tilebox.com/seasonal-rgb-timelapse/BuildSeasonalTimelapse \
  --version v1.0 \
  --input '{
    "center_lat_lon": [48.2082, 16.3738],
    "square_width_km": 10,
    "time_range": ["2023-09-01T00:00:00Z", "2026-09-01T00:00:00Z"],
    "max_cloud_percent": 20
  }' \
  --wait
```

Use `"time_range": null` to select the default three-year range.

## Outputs

Frames and the animated WebP are written below `outputs/` in the runner's working
directory. Log paths are relative to `outputs/`. Directory names contain the
dates, coordinates, and square width so jobs for different inputs do not mix
their frames, for example `20230902_20260902_48.33N_14.62E_10km/`. The final
animation in each directory is named `timelapse.webp`.

Frame names begin with the period's starting year and month, so ordinary lexical
sorting is chronological, for example:

```text
25_12-26_02.png
26_03-26_05.png
26_06-26_08.png
26_09-26_11.png
```

The final WebP is a flat, unlabelled square animation. Every frame uses the same
fixed Sentinel-2 display curve—black point 5, white point 250, and gamma 1.05—so
the animation does not flicker from per-frame autocontrast.

Outputs are runner-local. The frame and assembly tasks must therefore execute on
runners that share the same working directory and filesystem.

## Project structure

```text
runner.py                              Registers the workflow tasks
seasonal_rgb_timelapse/tasks.py        Defines the Tilebox task graph
seasonal_rgb_timelapse/imagery.py      Reads, renders, and encodes imagery
```
