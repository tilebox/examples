# Seasonal RGB Timelapse

This is the workflow behind the [Tilebox Console quickstart](https://console.tilebox.com/home/quickstart). It turns seasonal Sentinel-2 scenes around a location into an animated WebP while showing how Tilebox plans a job, runs independent tasks in parallel, and tracks the execution.

Start with the quickstart if you want to see the workflow run. It submits the job for a location you choose and guides you through connecting your machine to run tasks. Come back here when you want to understand the code or adapt it for your own workflow.

## How the workflow runs

The workflow uses three task types:

```text
BuildSeasonalTimelapse
  ├── RenderSeasonalFrame × N
  └── AssembleTimelapse (waits for every frame)
```

`BuildSeasonalTimelapse` is the root task. It creates a square area around the requested coordinates, queries `open_data.aws_earth.sentinel2`, and keeps scenes that cover the full area and meet the cloud-cover limit. It then selects the scene with the lowest reported cloud percentage in each three-month period.

Each selected scene becomes an independent `RenderSeasonalFrame` task. Tilebox can distribute these tasks across the available runners, so multiple frames render at the same time. Once every frame finishes, `AssembleTimelapse` combines the ordered PNG files into the final animation.

## Code walkthrough

### Define the job inputs

The root task in [`seasonal_rgb_timelapse/tasks.py`](seasonal_rgb_timelapse/tasks.py) declares typed inputs and a stable identifier:

```python
class BuildSeasonalTimelapse(Task):
    center_lat_lon: tuple[float, float]
    square_width_km: float
    time_range: tuple[datetime, datetime] | None = None
    max_cloud_percent: float = 20.0

    @staticmethod
    def identifier() -> tuple[str, str]:
        return "tilebox.com/seasonal-rgb-timelapse/BuildSeasonalTimelapse", "v1.0"
```

The quickstart provides `center_lat_lon` and sets `square_width_km` to 10. When no time range is supplied, the workflow uses the three years ending at the next UTC midnight.

### Query Tilebox Open Data

The root task uses the Tilebox dataset client to find low-cloud Sentinel-2 L2A scenes for the requested area and time range:

```python
collection = Client().dataset(DATASET).collection(COLLECTION)
scenes = collection.query(
    temporal_extent=TimeInterval(start=start, end=end),
    spatial_extent=aoi,
    filter=field("cloud_cover") <= self.max_cloud_percent,
)
```

The result is an xarray dataset. The task filters out scenes that do not cover the complete square, groups the remaining scenes into three-month periods, and chooses the lowest-cloud scene from each group.

### Create parallel work

The root task creates one frame task per selected scene and submits them together:

```python
frames = context.submit_subtasks(frame_tasks, max_retries=1)
context.submit_subtask(
    AssembleTimelapse(output_name=output_name),
    depends_on=frames,
    max_retries=1,
)
```

`submit_subtasks` makes the frame tasks available for parallel execution. `depends_on` prevents the assembly task from starting until every frame is ready. The tasks update job progress and emit structured logs and tracing spans. Each submitted task allows one retry.

### Read and render imagery

[`seasonal_rgb_timelapse/imagery.py`](seasonal_rgb_timelapse/imagery.py) opens each scene's visual GeoTIFF through Tilebox Storage and reads only the window needed for the area. It crops the result to a square PNG and applies one fixed display curve to every frame, which avoids visible brightness changes caused by per-frame autocontrast.

`AssembleTimelapse` sorts the frame filenames chronologically and encodes them as a looping animated WebP. [`runner.py`](runner.py) registers all three task types so a Tilebox runner can execute them.

## Inputs

`BuildSeasonalTimelapse` accepts:

| Input | Description |
| --- | --- |
| `center_lat_lon` | Latitude and longitude at the center of the timelapse. |
| `square_width_km` | Width and height of the square area in kilometers. |
| `time_range` | Optional UTC start and end times. Defaults to the latest three-year period. |
| `max_cloud_percent` | Maximum reported scene cloud cover. Defaults to 20%. |

JSON represents tuples as arrays and datetimes as RFC 3339 strings.

## Outputs

The workflow writes its result to `outputs/<job-specific-directory>/timelapse.webp` in the runner's working directory. Intermediate PNG frames are stored in the `frames/` directory beside it.

Output directories include the dates, coordinates, and square width, so separate runs do not overwrite each other. Outputs are local to the runners. The frame and assembly tasks therefore need runners that share the same working directory and filesystem.

## Explore locally

Install the dependencies and run the tests with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
uv run pytest
```

The most useful places to start are:

```text
seasonal_rgb_timelapse/tasks.py    Task graph, dataset query, and job progress
seasonal_rgb_timelapse/imagery.py  GeoTIFF reads, frame rendering, and WebP encoding
runner.py                          Task registration
tests/                             AOI, task input, rendering, and encoding behavior
```
