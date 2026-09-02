# Sentinel-2 Clay Change Detection

This workflow compares two Sentinel-2 time periods with the
[Clay](https://github.com/Clay-foundation/model) foundation model and writes a patch-level change map to Zarr.

## Design

`DetectChanges` accepts native ODC `Geometry`, `CRS`, and `Resolution` values plus two Tilebox `TimeInterval` values.
It queries credentials-free Sentinel-2 L2A COGs from AWS Earth Search and divides the exact output `GeoBox` into
256 × 256 pixel model tiles.

For every tile and period, `ComputeClayTile` reads only the required COG windows, masks clouds with the scene
classification layer, creates a 25th-percentile composite in memory, and runs Clay. `ComputeChangeTile` then computes
the cosine distance between corresponding embeddings. No full products, time-series cubes, pickled grids, or
intermediate mosaics are written.

The Zarr group is written to `change-detection/<job-id>` in the configured `s2-clay` bucket and contains `before` and
`after` Clay embeddings and the resulting `change` distance array.

## Run the workflow

1. Copy `.env.example` to `.env` and configure the Tilebox and Open Telekom Cloud credentials.
2. Validate and publish the release:

```bash
tilebox workflow build-release --debug --json
tilebox workflow publish-release --json
tilebox workflow deploy-release --latest --cluster <cluster-slug> --json
tilebox runner start --cluster <cluster-slug>
```

The Clay v1.5 checkpoint is downloaded and cached by `huggingface_hub` on each runner when first needed.

## Submit a job

```python
from datetime import UTC, datetime

from odc.geo import CRS, Geometry, Resolution
from shapely import box
from tilebox.datasets.query import TimeInterval
from tilebox.workflows import Client

from s2_clay import DetectChanges

client = Client()
client.jobs().submit(
    "vienna-change-detection",
    DetectChanges(
        area=Geometry(box(16.2, 48.1, 16.5, 48.3), "EPSG:4326"),
        before=TimeInterval(datetime(2024, 6, 1, tzinfo=UTC), datetime(2024, 7, 1, tzinfo=UTC)),
        after=TimeInterval(datetime(2025, 6, 1, tzinfo=UTC), datetime(2025, 7, 1, tzinfo=UTC)),
        output_crs=CRS("EPSG:32633"),
        resolution=Resolution(x=10, y=-10),
        max_cloud_cover=20,
    ),
)
```
