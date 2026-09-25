"""Sentinel-2 daily mosaics, local GeoTIFFs, and a PNG burn overlay."""

from pathlib import PurePosixPath
from urllib.parse import urlsplit

import numpy as np
import xarray as xr
from numpy.typing import NDArray
from odc.geo.geobox import GeoBox
from odc.geo.geom import box
from odc.geo.xr import wrap_xr, xr_reproject
from PIL import Image
from rasterio.io import MemoryFile
from tilebox.datasets.assets import Asset, AssetCollection
from tilebox.storage.aio import Client as StorageClient
from tilebox.storage.geotiff import window_from_bounds
from tilebox.workflows.observability.tracing import WorkflowTracer

Bounds = tuple[float, float, float, float]


def output_grid(bounds: Bounds, resolution: float = 20.0) -> GeoBox:
    """Create a rectangular UTM grid enclosing the query bounds.

    Args:
        bounds: West, south, east, north in WGS84 degrees.
        resolution: Output pixel size in metres.
    """
    return GeoBox.from_geopolygon(box(*bounds, crs="EPSG:4326"), crs="utm", resolution=resolution)


def calibrated(raw: NDArray, nodata: float | None, scale: float, offset: float) -> NDArray[np.float32]:
    """Mask raw nodata before applying per-asset reflectance calibration.

    Args:
        raw: Unscaled source pixels.
        nodata: Source nodata value, or None if absent.
        scale: Multiplier converting source values to reflectance.
        offset: Reflectance offset added after scaling.
    """
    result = raw.astype(np.float32) * scale + offset
    invalid = ~np.isfinite(raw)
    if nodata is not None:
        invalid |= raw == nodata
    result[invalid] = np.nan
    return result


def merge_observation(mosaic: xr.DataArray, values: xr.DataArray, scl: xr.DataArray) -> xr.DataArray:
    """Keep the first valid complete observation at each pixel.

    Args:
        mosaic: Existing band-first mosaic, with NaN for missing pixels.
        values: Aligned bands from one scene, in the mosaic's band order.
        scl: Aligned scene classifications; retain vegetation (4) and land (5).
    """
    valid = scl.isin([4, 5]) & np.isfinite(values).all("band")
    take = valid & ~np.isfinite(mosaic).all("band")
    return mosaic.where(~take, values)


def normalized_burn_ratio(reflectance: xr.DataArray) -> xr.DataArray:
    """Compute NBR from calibrated NIR and SWIR reflectance.

    Args:
        reflectance: Band-first array ordered as NIR, SWIR, with NaN nodata.
    """
    nir = reflectance.isel(band=0, drop=True)
    swir = reflectance.isel(band=1, drop=True)
    valid = np.isfinite(reflectance).all("band") & (nir >= 0) & (swir >= 0) & ((nir + swir) > 0)
    return (nir - swir) / (nir + swir).where(valid)


def burn_overlay(dnbr: xr.DataArray, rgb: Image.Image, threshold: float) -> Image.Image:
    """Highlight positive vegetation loss; leave unknown change as ordinary RGB.

    Args:
        dnbr: Before-minus-after NBR values, with NaN for unknown change.
        rgb: After-day RGBA image.
        threshold: Minimum dNBR to highlight as a possible burn scar.
    """
    mask = Image.fromarray((np.isfinite(dnbr) & (dnbr >= threshold)).values)
    orange = Image.new("RGB", rgb.size, (255, 64, 0))
    tinted = Image.blend(rgb.convert("RGB"), orange, 0.85)
    overlay = Image.composite(tinted, rgb.convert("RGB"), mask)
    overlay.putalpha(rgb.getchannel("A"))
    return overlay


def render_rgb(
    reflectance: NDArray[np.float32],
    gamma: float = 2.2,
) -> tuple[NDArray[np.uint8], NDArray[np.bool_]]:
    """Map reflectance 0–0.3 to display RGB with gamma 2.2; return RGB and validity.

    Args:
        reflectance: Band-first red, green, blue reflectance with NaN nodata.
        gamma: Display gamma; 1 is linear and 2.2 brightens midtones.
    """
    valid = np.isfinite(reflectance).all(axis=0)
    # A display white point: 30% reflectance maps to white, not a scientific cutoff.
    levels = np.clip(np.nan_to_num(reflectance) / 0.3, 0, 1)
    rgb = np.round(255 * levels ** (1 / gamma)).astype(np.uint8)
    rgb[:, ~valid] = 0
    return rgb, valid


async def read_band(storage: StorageClient, asset: Asset, grid: GeoBox) -> xr.DataArray:
    """Read one native-resolution band window, calibrate it, and align it.

    Args:
        storage: Client used to open the source asset.
        asset: Single-band reflectance or SCL asset and its calibration metadata.
        grid: Shared destination grid for every band and date.
    """
    geotiff = await storage.open_geotiff(asset)
    window = window_from_bounds(geotiff, tuple(grid.boundingbox), crs=str(grid.crs))
    chunk = await geotiff.read(window=window)
    nodata = asset.nodata if asset.nodata is not None else chunk.nodata
    raster = asset.raster
    values = calibrated(
        chunk.data[0],
        nodata,
        # Absent protobuf fields read as zero; missing scale must mean one.
        raster.scale if raster is not None and raster.has_field("scale") else 1.0,
        raster.offset if raster is not None and raster.has_field("offset") else 0.0,
    )
    source_grid = GeoBox(values.shape, chunk.transform, geotiff.crs)
    source = wrap_xr(values, source_grid, nodata=np.nan)
    return xr_reproject(source, grid, resampling="nearest", dst_nodata=np.nan).assign_attrs(nodata=np.nan)


async def read_mosaic(
    scenes: list[AssetCollection], grid: GeoBox, bands: list[str], *, tracer: WorkflowTracer, mask: str = "scl"
) -> xr.DataArray:
    """Read and mosaic bands, keeping the first clear complete observation.

    Args:
        scenes: Asset collections for one day, in overlap priority order.
        grid: Common output grid at the requested resolution.
        bands: Asset keys in the desired output band order.
        tracer: Task tracer used for per-scene child spans.
        mask: Scene-classification asset key; retain classes 4 and 5.
    """
    mosaic = wrap_xr(
        np.full((len(bands), *grid.shape), np.nan, dtype=np.float32),
        grid,
        axis=1,
        dims=("band", "y", "x"),
    )
    storage = StorageClient()
    for assets in scenes:
        scene = PurePosixPath(urlsplit(assets[bands[0]].primary.href).path).parent.name
        with tracer.span(scene):
            values = [await read_band(storage, assets[key], grid) for key in bands]
            scl = await read_band(storage, assets[mask], grid)
            mosaic = merge_observation(mosaic, xr.concat(values, dim="band"), scl)
    return mosaic


def render_rgba(mosaic: xr.DataArray) -> xr.DataArray:
    """Render reflectance as georeferenced RGBA pixels without writing them.

    Args:
        mosaic: Georeferenced red, green, blue reflectance mosaic.
    """
    rgb, valid = render_rgb(mosaic.values)
    return wrap_xr(
        np.concatenate([rgb, (valid.astype(np.uint8) * 255)[None]]),
        mosaic.odc.geobox,
        axis=1,
        dims=("band", "y", "x"),
    )


def read_raster(data: bytes) -> xr.DataArray:
    """Read a single-band analysis raster with its grid and nodata.

    Args:
        data: Encoded NBR or dNBR TIFF bytes from the job cache.
    """
    with MemoryFile(data) as file, file.open() as source:
        grid = GeoBox(source.shape, source.transform, source.crs)
        return wrap_xr(source.read(1), grid, nodata=source.nodata)
