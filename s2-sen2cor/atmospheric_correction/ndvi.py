"""Derive 20 m NDVI from B04 and B8A, masking with Sen2Cor's scene classification."""

from pathlib import Path

import numpy as np
import rasterio
from defusedxml import ElementTree
from PIL import Image
from rasterio.enums import Resampling

# Rasterio ships this extension without typing metadata.
from rasterio.shutil import copy as copy_raster  # ty: ignore[unresolved-import]

NODATA = -9999.0


def radiometry(product: Path) -> tuple[float, float, float]:
    """Read the reflectance quantification value and B04/B8A offsets from L2A metadata."""
    root = ElementTree.parse(product / "MTD_MSIL2A.xml").getroot()
    for element in root.iter():
        element.tag = element.tag.rsplit("}", 1)[-1]
    scale = float(root.findtext(".//BOA_QUANTIFICATION_VALUE", "nan"))
    offsets = {int(e.attrib["band_id"]): float(e.text or "nan") for e in root.iter("BOA_ADD_OFFSET")}
    if offsets and not {3, 8} <= offsets.keys():
        raise ValueError("Missing B04 or B8A BOA offset")
    red, nir = offsets.get(3, 0.0), offsets.get(8, 0.0)
    if not np.isfinite([scale, red, nir]).all() or scale <= 0:
        raise ValueError("Invalid BOA radiometry")
    return scale, red, nir


def ndvi_bands(product: Path) -> list[Path]:
    """Find the 20 m B04, B8A, and SCL files in an L2A SAFE product, in that order."""
    bands = []
    for band in ("B04", "B8A", "SCL"):
        paths = list(product.glob(f"GRANULE/*/IMG_DATA/R20m/*_{band}_20m.jp2"))
        if len(paths) != 1:
            raise ValueError(f"Expected exactly one 20 m {band} band")
        bands.append(paths[0])
    return bands


def derive_ndvi(product: Path, destination: Path, *, bands: list[Path] | None = None) -> Path:
    """Write a compressed NDVI COG, applying reflectance offsets and masking invalid or excluded pixels."""
    scale, red_offset, nir_offset = radiometry(product)
    bands = ndvi_bands(product) if bands is None else bands
    working = destination.with_suffix(".working.tif")
    with rasterio.open(bands[0]) as red, rasterio.open(bands[1]) as nir, rasterio.open(bands[2]) as scl:
        if any((s.crs, s.transform, s.shape) != (red.crs, red.transform, red.shape) for s in (nir, scl)):
            raise ValueError("B04, B8A and SCL must share a grid")
        profile = red.profile | {"driver": "GTiff", "count": 1, "dtype": "float32", "nodata": NODATA}
        with rasterio.open(working, "w", **profile) as output:
            for _, window in output.block_windows(1):
                red_dn, nir_dn = red.read(1, window=window), nir.read(1, window=window)
                red_reflectance = (red_dn.astype("float32") + red_offset) / scale
                nir_reflectance = (nir_dn.astype("float32") + nir_offset) / scale
                denominator = nir_reflectance + red_reflectance
                valid = (
                    (red_dn != 0)
                    & (nir_dn != 0)
                    & (red_reflectance >= 0)
                    & (nir_reflectance >= 0)
                    & (denominator > 0)
                    & np.isin(scl.read(1, window=window), [4, 5, 6])
                )
                values = np.full(red_dn.shape, NODATA, dtype="float32")
                np.divide(nir_reflectance - red_reflectance, denominator, out=values, where=valid)
                output.write(values, 1, window=window)
    copy_raster(working, destination, driver="COG", compress="DEFLATE", overview_resampling="NEAREST")
    working.unlink()
    return destination


def ndvi_preview(ndvi: Path, destination: Path) -> Path:
    """Write a PNG preview with a fixed NDVI color scale and transparent nodata pixels."""
    with rasterio.open(ndvi) as source:
        width = min(512, source.width)
        height = max(1, round(source.height * width / source.width))
        values = source.read(1, out_shape=(height, width), masked=True, resampling=Resampling.nearest)
    # Fixed scale: negative NDVI is blue, zero is tan, positive NDVI is green.
    colors = np.array([[49, 54, 149], [224, 210, 170], [0, 104, 55]])
    data = values.filled(0)
    rgb = np.stack([np.interp(data, [-1, 0, 1], colors[:, channel]) for channel in range(3)], axis=-1)
    alpha = np.where(np.ma.getmaskarray(values), 0, 255)
    Image.fromarray(np.dstack([rgb, alpha]).astype("uint8")).save(destination)
    return destination
