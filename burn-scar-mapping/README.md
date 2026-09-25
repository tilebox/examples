# Burn-scar mapping

Build two Sentinel-2 daily mosaics and highlight vegetation loss between them. This example uses Tilebox to query imagery and run dependent tasks, with all output files saved locally.

## Outputs and task graph

Scenes are grouped by UTC day. The sorted days are split in half by count, with the middle day in the second half when the count is odd. Each half contributes the day with the largest footprint coverage inside the query area. Ties select the earliest day in the first half and the latest day in the second half.

Coverage uses the union of scene footprints clipped to the area, so overlapping tiles are not counted twice. Partial coverage is allowed; at least two days are required. Footprint coverage does not guarantee cloud-free pixels, and the date split does not detect the fire date. Choose a time range that brackets the fire.

Every job has seven tasks. The root submits four independent workers:

```text
MosaicRGB(before)
MosaicRGB(after) ──────────────────────────┐
ComputeNBR(before) ─┬─ ComputeDelta ────────┴─ RenderOverlay
ComputeNBR(after) ──┘
```

RGB and NBR are computed by separate tasks for each day. Delta NBR depends only on the two NBR tasks, and the overlay depends only on delta NBR and the later RGB task. The earlier RGB is saved independently. Each scene read has a child trace span named for its asset URL's scene directory. The overlay highlights pixels with `dNBR >= dnbr_threshold` on the after-day RGB.

All intermediate data and outputs use `context.job_cache`. The runner's `LocalFileSystemCache("cache")` stores these keys under `cache/<job-id>/` automatically; tasks never construct job-ID paths:

- `<date>/assets`: pickled asset collections in Tilebox's local job cache.
- `<date>/rgb.tif` and `<date>/nbr.tif`: daily RGBA and NBR mosaics.
- `dnbr.tif`: vegetation-change values; NaN where either date is invalid.
- `burn_overlay.png`: after-day RGB with orange burn highlights and transparency.

The overlay task also writes an identical copy to `Path.home() / "burn_overlay_<uuid>.png"` on the runner executing it, outside the cache, and logs the full path.

The TIFFs are Cloud Optimized GeoTIFFs encoded from georeferenced xarray arrays with `write_cog(array, ":mem:")`. The returned bytes go directly into the job cache; `read_raster` decodes them with Rasterio's `MemoryFile`. The library handles compression and overviews without staging files. The overlay is encoded into a `BytesIO` buffer and cached as PNG; it preserves transparency but is not georeferenced. Encoded files as well as decoded arrays must fit in memory.

The output grid is a simple odc-geo `GeoBox` in a local UTM CRS, with the requested pixel size in metres (20 m by default). It is the rectangular grid enclosing the WGS84 query bounds. There is no additional AOI polygon mask, so a small border outside those bounds may be included. Both daily COGs, dNBR, and the PNG retain the full output-grid dimensions. No separate low-resolution preview is produced; COG overviews are internal to the full-resolution files.

## Masking and RGB visualization

The source is `open_data.aws_earth.sentinel2`, collection `L2A`. Tilebox provides scene metadata and asset access; imagery comes from public Element 84 AWS assets without AWS credentials.

RGB reads the individual `red`, `green`, and `blue` reflectance assets, not the provider-rendered `visual` asset or a thumbnail. NBR is `(B08 - B12) / (B08 + B12)` using `nir` and `swir22`. In this collection, RGB and `nir` (B08) have native 10 m pixels; `swir22` (B12), SCL, and the separate `nir08` (B8A) asset have 20 m pixels. We use B08 for NBR. Native-resolution source windows are read and aligned to the shared 20 m output grid, rather than reading source overviews.

Both products use SCL to retain vegetation (4) and non-vegetated land (5), excluding water, clouds, shadows, and other invalid pixels. NBR also excludes negative reflectance and zero denominators.

Within each day, the first valid observation in acquisition-time order wins at each pixel. NBR bands are kept together rather than mixed between scenes. `read_mosaic` accepts the requested band keys and a scene-classification mask asset. Tasks compose reading, pure calculations, and writing: `render_rgba` or `normalized_burn_ratio`, followed by `write_cog`. The delta task reads each NBR, subtracts after from before, then writes the result. Mosaics, dNBR, and the PNG are processed in memory; use regional areas that fit runner memory.

`read_band` reads one two-dimensional band. Tilebox's `window_from_bounds` transforms the output bounds and clips the read window to the source image. After calibration, odc-geo's `xr_reproject` aligns the xarray array to the common grid using nearest-neighbor resampling. This is needed even with UTM imagery because source scenes can have different pixel sizes, grid origins, or UTM zones.

Band reads return xarray `DataArray`s carrying spatial coordinates, CRS, and NaN nodata. Masks use `.where()` and missing values propagate through NBR and dNBR arithmetic; no separate NumPy masked-array representation is needed. Raw nodata is still removed before calibration, and SCL filtering remains explicit. The RGB COG has an alpha band rather than zero-valued nodata, so valid black pixels remain opaque. COG encoding requires additional temporary memory inside odc-geo.

RGB rendering uses 0.3 as its display white point: 30% reflectance and higher maps to white. This is a visualization choice, not a physical maximum or burn threshold. The rendering function uses its default gamma of 2.2 to brighten midtones; gamma is not a workflow input. These settings affect RGB only, not NBR calculations or image dimensions. Pillow blends an orange image over the burn mask and retains the after-day alpha channel; pixels with unknown dNBR remain ordinary RGB, not confirmed unburned pixels. Consult `dnbr.tif` for change-analysis coverage.

## Threshold and interpretation

Delta NBR is signed: ΔNBR = NBR_before - NBR_after. Positive values indicate decreasing NBR, consistent with vegetation loss; negative values indicate increasing NBR, often associated with vegetation growth or recovery. Neither sign proves the cause. Do not take the absolute value or square the difference for burn thresholding, since that would also highlight increases in NBR.

The default `dnbr_threshold` is 0.27, a starting cutoff rather than a locally calibrated burn-severity classification. Vegetation seasonality, harvesting, smoke, and illumination changes can also affect dNBR. The baseline must precede the damage of interest; damage already present in that image is not measured.

## Run

Use Python 3.11 or newer. Set `TILEBOX_API_KEY`, then install and start a local runner:

```bash
uv sync
uv run python runner.py
```

All five task classes use version `v1.0`. Submit `burn-scar-mapping/MapBurnScars` with:

```json
{
  "bounds": [27.68, 35.87, 28.25, 36.46],
  "time_range": ["2023-07-18T00:00:00Z", "2023-07-29T00:00:00Z"],
  "dnbr_threshold": 0.27,
  "resolution": 20.0
}
```

Bounds are `[west, south, east, north]` in WGS84. Supply timezone-aware times; the end is exclusive. Use a regional box within UTM coverage, without crossing the antimeridian, and a positive output resolution in metres.

Run all tasks on the same machine and from the same working directory: this tutorial deliberately uses local files rather than shared storage. No output bucket or `output_uri` is needed.

The runner configures `LocalFileSystemCache`. The root caches the selected asset collections once under date keys such as `2025-04-23/assets`, and passes the shared `GeoBox` directly to each mosaic task. No manifest or repeat scene lookup is needed. Pickles are internal to this trusted local cache; do not load cache files supplied by others.

## Verification

```bash
uv run python -m pytest
uv run ruff check burn_scar_mapping runner.py tests
tilebox workflow build-release --debug --json
```

Tests cover daily grouping, coverage unions, partial coverage, tie-breaking, UTM grids, calibration, xarray masking, clipped single-band reads, scene trace spans, spectral RGB rendering, COG layout, full output dimensions, georeferencing, dNBR direction, overlay thresholds, and PNG transparency. Functions and task methods have type annotations and Google-style parameter documentation. The runner registers five task classes; `MosaicRGB` and `ComputeNBR` each run twice per job. The local `cache` directory is excluded from Git and release artifacts.
