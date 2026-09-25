import asyncio
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
import rasterio
import xarray as xr
from odc.geo.cog import write_cog
from odc.geo.geobox import GeoBox
from odc.geo.geom import box as geo_box
from odc.geo.xr import wrap_xr
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from PIL import Image
from rasterio.transform import from_origin
from tilebox.datasets.datasets.stac.v1.asset_metadata_pb import RasterProperties
from tilebox.workflows.cache import LocalFileSystemCache
from tilebox.workflows.observability.tracing import NoopWorkflowTracer

from burn_scar_mapping.imagery import (
    burn_overlay,
    calibrated,
    merge_observation,
    normalized_burn_ratio,
    output_grid,
    read_band,
    read_mosaic,
    read_raster,
    render_rgb,
    render_rgba,
)


def test_calibration_masks_raw_nodata_before_offset() -> None:
    actual = calibrated(np.array([[0, 1000, 2500, 6000]], dtype=np.uint16), 0, 0.0001, -0.1)
    np.testing.assert_allclose(actual, [[np.nan, 0, 0.15, 0.5]], atol=1e-7)


def test_clouds_and_incomplete_pixels_do_not_win_overlaps() -> None:
    scl = xr.DataArray(np.arange(12)[None, :], dims=("y", "x"))
    first = xr.DataArray(np.full((5, 1, 12), 0.2, dtype=np.float32), dims=("band", "y", "x"))
    first[2, 0, 5] = np.nan
    mosaic = xr.full_like(first, np.nan)
    mosaic = merge_observation(mosaic, first, scl)
    assert np.isfinite(mosaic).all("band").values.tolist() == [[i == 4 for i in range(12)]]
    second = xr.full_like(first, 0.6)
    mosaic = merge_observation(mosaic, second, xr.full_like(scl, 5))
    np.testing.assert_allclose(mosaic[:, 0, 4], 0.2)
    np.testing.assert_allclose(mosaic[:, 0, 5], 0.6)
    assert np.isfinite(mosaic).all()


def test_nbr_and_dnbr_direction_boundary_invalid_and_rgb() -> None:
    reflectance = np.array(
        [[[0.1, 0.3, 0.5, 0, -0.1, np.nan]], [[0.3, 0.3, 0.1, 0, 0.3, 0.3]]],
        dtype=np.float32,
    )
    nbr = normalized_burn_ratio(xr.DataArray(reflectance, dims=("band", "y", "x")))
    np.testing.assert_allclose(nbr, [[-0.5, 0, 2 / 3, np.nan, np.nan, np.nan]], rtol=1e-6)
    rgb, valid = render_rgb(np.full((3, 1, 6), 0.3))
    assert valid.all()
    rgba = np.concatenate([rgb, np.full((1, 1, 6), 255, dtype=np.uint8)])
    image = Image.fromarray(np.moveaxis(rgba, 0, -1), "RGBA")
    dnbr = xr.DataArray([[0.27, 0.26, -0.5, np.nan, np.nan, 0.8]])
    overlay = np.asarray(burn_overlay(dnbr, image, 0.27))
    np.testing.assert_array_equal(overlay[0, 0], [255, 92, 38, 255])
    np.testing.assert_array_equal(overlay[0, 1:5], np.moveaxis(rgba, 0, -1)[0, 1:5])
    assert (overlay[0, 5, :3] != 255).any()


def test_water_invalid_for_rgb_and_nbr() -> None:
    scl = xr.DataArray([[4, 5, 6, 8, 3, 0]], dims=("y", "x"))
    values = xr.DataArray(np.full((3, 1, 6), 0.2), dims=("band", "y", "x"))
    rgb = merge_observation(xr.full_like(values, np.nan), values, scl)
    nbr = merge_observation(xr.full_like(values[:2], np.nan), values[:2], scl)
    np.testing.assert_array_equal(np.isfinite(rgb[0]), [[True, True, False, False, False, False]])
    np.testing.assert_array_equal(np.isfinite(nbr[0]), [[True, True, False, False, False, False]])


def test_reflectance_display_levels_gamma_and_nodata() -> None:
    reflectance = np.tile([-0.1, 0, 0.075, 0.15, 0.3, 0.6, np.nan], (3, 1, 1))
    rgb, valid = render_rgb(reflectance)
    np.testing.assert_array_equal(rgb[0], [[0, 0, 136, 186, 255, 255, 0]])
    np.testing.assert_array_equal(valid, [[True] * 6 + [False]])
    linear, linear_valid = render_rgb(reflectance, gamma=1)
    default = render_rgba(
        wrap_xr(
            reflectance,
            GeoBox((1, 7), from_origin(0, 1, 1, 1), "EPSG:4326"),
            axis=1,
            dims=("band", "y", "x"),
        ),
    )
    assert linear[0, 0, 2].item() == 64
    assert default[0, 0, 2].item() == 136
    np.testing.assert_array_equal(linear_valid, valid)
    np.testing.assert_array_equal(default[3], valid.astype(np.uint8) * 255)


def test_read_aligns_different_resolution_and_partial_coverage() -> None:
    class Source:
        crs = "EPSG:32629"
        transform = from_origin(600000, 4400040, 20, 20)
        width = height = 2

        async def read(self, *, window: rasterio.windows.Window) -> SimpleNamespace:
            # The helper clips the larger destination bounds to source pixels.
            assert (window.col_off, window.row_off, window.width, window.height) == (
                0,
                0,
                2,
                2,
            )
            return SimpleNamespace(
                data=np.array([[[0, 2000], [4000, 6000]]], dtype=np.uint16),
                transform=self.transform,
                nodata=0,
            )

    asset = SimpleNamespace(key="nir", nodata=0, raster=RasterProperties(scale=0.0001, offset=-0.1))

    class Storage:
        async def open_geotiff(self, requested: SimpleNamespace) -> Source:
            assert requested is asset
            return Source()

    actual = asyncio.run(
        read_band(
            Storage(),
            asset,
            GeoBox((4, 6), from_origin(599980, 4400040, 10, 10), "EPSG:32629"),
        )
    )
    assert isinstance(actual, xr.DataArray)
    assert actual.dims == ("y", "x")
    assert actual.odc.geobox == GeoBox((4, 6), from_origin(599980, 4400040, 10, 10), "EPSG:32629")
    assert np.isnan(actual.odc.nodata)
    np.testing.assert_allclose(
        actual,
        [
            [np.nan, np.nan, np.nan, np.nan, 0.1, 0.1],
            [np.nan, np.nan, np.nan, np.nan, 0.1, 0.1],
            [np.nan, np.nan, 0.3, 0.3, 0.5, 0.5],
            [np.nan, np.nan, 0.3, 0.3, 0.5, 0.5],
        ],
        atol=1e-7,
    )
    # SCL has a spatial resolution but no scale: absent protobuf scale must mean 1, not 0.
    asset.raster = RasterProperties(spatial_resolution=20)
    scl = asyncio.run(read_band(Storage(), asset, GeoBox((2, 2), Source.transform, Source.crs)))
    np.testing.assert_allclose(scl, [[np.nan, 2000], [4000, 6000]])


@pytest.mark.parametrize(
    "bounds,resolution,epsg",
    [
        ((-7.82, 40.08, -7.76, 40.14), 20, 32629),
        ((150.1, -34.2, 150.3, -34.0), 30, 32756),
    ],
)
def test_grid_uses_utm_and_requested_resolution(
    bounds: tuple[float, float, float, float], resolution: float, epsg: int
) -> None:
    grid = output_grid(bounds, resolution)
    assert grid.crs.epsg == epsg
    assert grid.transform.a == resolution
    assert grid.transform.e == -resolution
    assert grid.transform.c % resolution == grid.transform.f % resolution == 0
    area = geo_box(*bounds, crs="EPSG:4326").to_crs(grid.crs, resolution=0.001)
    assert grid.extent.contains(area)


@pytest.mark.parametrize(
    "product,keys",
    [("rgb", ["red", "green", "blue", "scl"]), ("nbr", ["nir", "swir22", "scl"])],
)
def test_mosaic_reads_each_scene_asset_once_across_output_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, product: str, keys: list[str]
) -> None:
    from burn_scar_mapping import imagery

    bounds = (-7.9, 40.1, -7.7, 40.3)
    grid = output_grid(bounds)
    height, width = grid.shape
    assert min(height, width) > 512  # Catch accidentally restoring a per-output-block read loop.
    calls = []
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = NoopWorkflowTracer()
    monkeypatch.setattr(tracer, "_tracer", provider.get_tracer("test-mosaics"))

    class Source:
        crs = str(grid.crs)
        transform = grid.transform

        def __init__(self, asset: SimpleNamespace) -> None:
            self.asset = asset
            self.height, self.width = height, width

        async def read(self, *, window: rasterio.windows.Window) -> SimpleNamespace:
            calls.append((self.asset.scene, self.asset.key))
            assert trace.get_current_span().name == f"S2B_T59VME_2026082{self.asset.scene}T235827_L2A"
            assert window.width == width and window.height == height
            key, scene = self.asset.key, self.asset.scene
            value = {
                "red": 1750 if scene == 0 else 1000,
                "green": 2500 if scene == 0 else 1000,
                "blue": 4000 if scene == 0 else 1000,
                "nir": 4000 if scene == 0 else 2000,
                "swir22": 2000,
                "scl": 4,
            }[key]
            data = np.full((1, height, width), value)
            if key == "scl":
                data[:, :, width // 2 :] = 8 if scene == 0 else 4
                data[:, height // 2 :, : width // 4] = 6
            return SimpleNamespace(data=data, transform=self.transform, nodata=0)

    class Storage:
        async def open_geotiff(self, asset: SimpleNamespace) -> Source:
            return Source(asset)

    def assets(scene: int) -> dict[str, SimpleNamespace]:
        return {
            key: SimpleNamespace(
                key=key,
                scene=scene,
                primary=SimpleNamespace(
                    href=f"https://example.com/tiles/S2B_T59VME_2026082{scene}T235827_L2A/{key}.tif"
                ),
                nodata=0,
                raster=RasterProperties(scale=0.0001, offset=-0.1),
            )
            for key in keys
            if key != "scl"
        } | {"scl": SimpleNamespace(key="scl", scene=scene, nodata=0, raster=RasterProperties())}

    monkeypatch.setattr(imagery, "StorageClient", Storage)
    scenes = [assets(i) for i in range(2)]
    with tracer.span("mosaic task", attributes={"task_id": "test-task"}) as parent:
        mosaic = asyncio.run(read_mosaic(scenes, grid, keys[:-1], tracer=tracer))
    if product == "rgb":
        data = render_rgba(mosaic)
        write_cog(data, tmp_path / "rgb.tif", nodata=None, photometric="RGB", alpha="YES")
    else:
        data = normalized_burn_ratio(mosaic)
        write_cog(data, tmp_path / "nbr.tif", nodata=np.nan)
    path = tmp_path / f"{product}.tif"
    assert calls == [(i, key) for i in range(2) for key in keys]
    spans = exporter.get_finished_spans()[:-1]
    assert [span.name for span in spans] == [f"S2B_T59VME_2026082{i}T235827_L2A" for i in range(2)]
    assert all(span.parent.span_id == parent.get_span_context().span_id for span in spans)
    assert all(span.attributes["task_id"] == "test-task" for span in spans)
    provider.shutdown()
    with rasterio.open(path) as ds:
        assert ds.dataset_mask()[height * 3 // 4, width // 8] == 0  # Water.
        data = ds.read([1, 2, 3]) if product == "rgb" else ds.read()
        left = data[:, height // 4, width // 3]
        right = data[:, height // 4, width * 3 // 4]
        assert ds.tags(ns="IMAGE_STRUCTURE")["LAYOUT"] == "COG"
        assert ds.shape == grid.shape.yx
        if product == "rgb":
            assert ds.colorinterp[-1] == rasterio.enums.ColorInterp.alpha
            np.testing.assert_array_equal(left, [136, 186, 255])
            # There is no AOI polygon mask: clear corners of the grid remain valid.
            assert ds.dataset_mask()[0, 0] == 255
            np.testing.assert_array_equal(right, [0, 0, 0])
            assert ds.dataset_mask()[height // 4, width * 3 // 4] == 255  # Valid black is opaque.
        else:
            np.testing.assert_allclose(left, [0.5], atol=1e-7)
            np.testing.assert_allclose(right, [0], atol=1e-7)


def test_empty_mosaics_preserve_georeferencing_and_nodata(tmp_path: Path) -> None:
    bounds = (-7.9, 40.1, -7.7, 40.3)
    grid = output_grid(bounds)
    tracer = NoopWorkflowTracer()
    nbr = normalized_burn_ratio(asyncio.run(read_mosaic([], grid, ["nir", "swir22"], tracer=tracer)))
    rgba = render_rgba(asyncio.run(read_mosaic([], grid, ["red", "green", "blue"], tracer=tracer)))
    write_cog(nbr, tmp_path / "nbr.tif", nodata=np.nan)
    write_cog(rgba, tmp_path / "rgb.tif", nodata=None, photometric="RGB", alpha="YES")
    paths = [tmp_path / "nbr.tif", tmp_path / "rgb.tif"]
    for path in paths:
        with rasterio.open(path) as dataset:
            assert dataset.crs.to_epsg() == 32629
            assert dataset.transform == grid.transform
            assert dataset.shape == grid.shape.yx
            assert dataset.overviews(1)
            assert dataset.tags(ns="IMAGE_STRUCTURE")["LAYOUT"] == "COG"
            assert not dataset.dataset_mask().any()
            if dataset.count == 1:
                assert np.isnan(dataset.read()).all()
            else:
                assert dataset.nodata is None
                assert not dataset.read().any()


def test_delta_and_png_keep_transparency_and_unknown_change(tmp_path: Path) -> None:
    cache = LocalFileSystemCache(tmp_path).group("job")
    grid = GeoBox((1, 6), from_origin(500000, 4000000, 20, 20), "EPSG:32635")
    cache["before/nbr.tif"] = write_cog(
        wrap_xr(np.array([[0.6, 0.3, np.nan, 0.7, 0.8, 0.1]], dtype=np.float32), grid),
        ":mem:",
        nodata=np.nan,
    )
    cache["after/nbr.tif"] = write_cog(
        wrap_xr(np.array([[0.1, 0.4, 0.2, np.nan, 0.2, 0.2]], dtype=np.float32), grid),
        ":mem:",
        nodata=np.nan,
    )
    rgb = np.full((3, 1, 6), 120, dtype=np.uint8)
    rgb[:, :, 5] = 0  # Valid black must remain opaque.
    alpha = np.array([[[255, 255, 255, 255, 0, 255]]], dtype=np.uint8)
    cache["after/rgb.tif"] = write_cog(
        wrap_xr(np.concatenate([rgb, alpha]), grid, axis=1, dims=("band", "y", "x")),
        ":mem:",
        nodata=None,
        photometric="RGB",
        alpha="YES",
    )
    before = read_raster(cache["before/nbr.tif"])
    after = read_raster(cache["after/nbr.tif"])
    assert np.isnan(before.odc.nodata)
    dnbr = before - after
    cache["dnbr.tif"] = write_cog(dnbr, ":mem:", nodata=np.nan)
    with Image.open(BytesIO(cache["after/rgb.tif"])) as image:
        overlay = burn_overlay(dnbr, image.convert("RGBA"), 0.27)
    with BytesIO() as buffer:
        overlay.save(buffer, format="PNG")
        cache["burn_overlay.png"] = buffer.getvalue()
    result = read_raster(cache["dnbr.tif"])
    np.testing.assert_allclose(result, [[0.5, -0.1, np.nan, np.nan, 0.6, -0.1]], atol=1e-7)
    assert result.odc.geobox == grid
    with Image.open(BytesIO(cache["burn_overlay.png"])) as image:
        assert image.mode == "RGBA"
        rgba = np.asarray(image)
        expected = np.array(
            [
                [234, 72, 17, 255],
                [120, 120, 120, 255],
                [120, 120, 120, 255],
                [120, 120, 120, 255],
                [0, 0, 0, 0],
                [0, 0, 0, 255],
            ]
        )
        # Pillow truncates blend channels; RGB below fully transparent alpha is irrelevant.
        np.testing.assert_array_equal(rgba[0, :, 3], expected[:, 3])
        visible = rgba[0, :, 3] != 0
        np.testing.assert_array_equal(rgba[0, visible, :3], expected[visible, :3])
