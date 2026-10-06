import asyncio
from datetime import UTC, datetime

import numpy as np
import xarray as xr
from numpy.typing import NDArray
from odc.geo.cog import to_cog, write_cog
from odc.geo.geobox import GeoBox
from odc.geo.geom import Geometry
from odc.geo.xr import wrap_xr, xr_reproject
from PIL import Image
from rasterio.io import MemoryFile
from shapely import Polygon, box
from tilebox.datasets.assets import Asset, AssetCollection
from tilebox.storage.aio import Client as StorageClient
from tilebox.storage.geotiff import window_from_bounds
from xarray.ufuncs import isfinite

from observations import select_observations


async def read_band(storage: StorageClient, asset: Asset, area: Polygon, crs: str = "EPSG:4326") -> xr.DataArray:
    """Read and calibrate an intersecting window at its native resolution.

    Args:
        storage: Client used to open the source asset.
        asset: Single-band asset and its calibration metadata.
        area: Query polygon in the supplied coordinate system.
        crs: Coordinate system of area; defaults to longitude/latitude.
    """
    geotiff = await storage.open_geotiff(asset)
    window = window_from_bounds(geotiff, area.bounds, crs=crs)
    chunk = await geotiff.read(window=window)
    nodata = asset.nodata if asset.nodata is not None else chunk.nodata
    raster = asset.raster
    values = _calibrate(
        chunk.data[0],
        nodata,
        # Absent protobuf fields read as zero; missing scale must mean one.
        raster.scale if raster is not None and raster.has_field("scale") else 1.0,
        raster.offset if raster is not None and raster.has_field("offset") else 0.0,
    )
    source_grid = GeoBox(values.shape, chunk.transform, geotiff.crs)
    return wrap_xr(values, source_grid, nodata=np.nan)


def _calibrate(raw: NDArray, nodata: float | None, scale: float, offset: float) -> NDArray[np.float32]:
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


def output_grid(area: Polygon, resolution: float = 20.0) -> GeoBox:
    """Create a rectangular UTM grid enclosing the query polygon.

    Args:
        area: Query polygon in WGS84 longitude/latitude degrees.
        resolution: Output pixel size in metres.
    """
    return GeoBox.from_geopolygon(Geometry(area, crs="EPSG:4326"), crs="utm", resolution=resolution)


def merge_observation(mosaic: xr.DataArray, values: xr.DataArray, scl: xr.DataArray) -> xr.DataArray:
    """Keep the first valid complete observation at each pixel.

    Args:
        mosaic: Existing band-first mosaic, with NaN for missing pixels.
        values: Aligned bands from one scene, in the mosaic's band order.
        scl: Aligned scene classifications; retain vegetation (4) and land (5).
    """
    valid = scl.isin([4, 5]) & isfinite(values).all("band")
    take = valid & ~isfinite(mosaic).all("band")
    return mosaic.where(~take, values)


def normalized_burn_ratio(reflectance: xr.DataArray) -> xr.DataArray:
    """Compute NBR from calibrated NIR and SWIR reflectance.

    Args:
        reflectance: Band-first array ordered as NIR, SWIR, with NaN nodata.
    """
    nir = reflectance.isel(band=0, drop=True)
    swir = reflectance.isel(band=1, drop=True)
    valid = isfinite(reflectance).all("band") & (nir >= 0) & (swir >= 0) & ((nir + swir) > 0)
    return (nir - swir) / (nir + swir).where(valid)


def burn_overlay(dnbr: xr.DataArray, rgb: Image.Image, threshold: float) -> Image.Image:
    """Highlight positive vegetation loss; leave unknown change as ordinary RGB.

    Args:
        dnbr: Before-minus-after NBR values, with NaN for unknown change.
        rgb: After-day RGBA image.
        threshold: Minimum dNBR to highlight as a possible burn scar.
    """
    mask = Image.fromarray((isfinite(dnbr) & (dnbr >= threshold)).values)
    orange = Image.new("RGB", rgb.size, (255, 64, 0))
    tinted = Image.blend(rgb.convert("RGB"), orange, 0.85)
    overlay = Image.composite(tinted, rgb.convert("RGB"), mask)
    overlay.putalpha(rgb.getchannel("A"))
    return overlay


def render_rgb(
    reflectance: NDArray[np.float32],
    gamma: float = 2.2,
) -> tuple[NDArray[np.uint8], NDArray[np.bool_]]:
    """Map reflectance 0-0.3 to display RGB with gamma 2.2; return RGB and validity.

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


async def read_mosaic(
    scenes: list[AssetCollection], grid: GeoBox, bands: list[str], *, mask: str = "scl"
) -> xr.DataArray:
    """Read and mosaic bands, keeping the first clear complete observation.

    Args:
        scenes: Asset collections for one day, in overlap priority order.
        grid: Common output grid at the requested resolution.
        bands: Asset keys in the desired output band order.
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
        aligned = []
        for key in [*bands, mask]:
            native = await read_band(storage, assets[key], grid.extent.geom, str(grid.crs))
            aligned.append(
                xr_reproject(native, grid, resampling="nearest", dst_nodata=np.nan).assign_attrs(nodata=np.nan)
            )
        mosaic = merge_observation(mosaic, xr.concat(aligned[:-1], dim="band"), aligned[-1])
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


def write_cog_to_bytes(raster: xr.DataArray, *, rgba: bool = False) -> bytes:
    """Encode an analysis raster with NaN nodata, or RGBA with an alpha band."""
    if rgba:
        return to_cog(raster, nodata=None, photometric="RGB", alpha="YES")
    return to_cog(raster, nodata=np.nan)


def read_cog_from_bytes(data: bytes) -> xr.DataArray:
    """Read an analysis or RGBA raster, preserving its grid and nodata.

    Args:
        data: Encoded GeoTIFF bytes from the job cache.
    """
    with MemoryFile(data) as file, file.open() as source:
        grid = GeoBox(source.shape, source.transform, source.crs)
        if source.count > 1:
            return wrap_xr(source.read(), grid, axis=1, dims=("band", "y", "x"), nodata=source.nodata)
        return wrap_xr(source.read(1), grid, nodata=source.nodata)


async def preview_change() -> None:
    # All of Rhodes, Greece. Both dates use this same grid.
    area = box(27.65, 35.85, 28.30, 36.50)
    grid = output_grid(area, resolution=20)
    after_day, after_scenes = select_observations(
        area,
        (datetime(2023, 7, 25, tzinfo=UTC), datetime(2023, 7, 30, tzinfo=UTC)),
    )
    after_assets = [AssetCollection.from_datapoint(scene) for scene in after_scenes]
    after_reflectance = await read_mosaic(after_assets, grid, ["nir", "swir22"])
    after_nbr = normalized_burn_ratio(after_reflectance)
    write_cog(after_nbr, "rhodes-nbr-after.tif", nodata=np.nan, overwrite=True)
    print(f"NBR {after_day}: {float(after_nbr.min()):.2f} to {float(after_nbr.max()):.2f}")  # noqa: T201 - script output

    # Before mosaic and delta NBR:
    before_day, before_scenes = select_observations(
        area,
        (datetime(2023, 7, 15, tzinfo=UTC), datetime(2023, 7, 20, tzinfo=UTC)),
    )
    before_assets = [AssetCollection.from_datapoint(scene) for scene in before_scenes]
    before_reflectance = await read_mosaic(before_assets, grid, ["nir", "swir22"])
    before_nbr = normalized_burn_ratio(before_reflectance)
    dnbr = before_nbr - after_nbr
    write_cog(before_nbr, "rhodes-nbr-before.tif", nodata=np.nan, overwrite=True)
    write_cog(dnbr, "rhodes-dnbr.tif", nodata=np.nan, overwrite=True)
    print(f"dNBR: {before_day} minus {after_day} → rhodes-dnbr.tif")  # noqa: T201 - script output

    # Overlay on the after-date RGB mosaic:
    reflectance = await read_mosaic(after_assets, grid, ["red", "green", "blue"])
    rgb, valid = render_rgb(reflectance.values)
    image = Image.fromarray(rgb.transpose(1, 2, 0))
    image.putalpha(Image.fromarray(valid.astype("uint8") * 255))
    burn_overlay(dnbr, image, threshold=0.27).save("rhodes-overlay.png")
    print("Saved rhodes-overlay.png")  # noqa: T201 - script output


if __name__ == "__main__":
    asyncio.run(preview_change())
