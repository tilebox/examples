import re
import subprocess
from pathlib import Path

import numpy as np
import rasterio
from defusedxml import ElementTree
from numpy.typing import NDArray
from PIL import Image
from rasterio.enums import Resampling
from rasterio.shutil import copy as copy_raster  # ty: ignore[unresolved-import] - compiled Rasterio module
from rasterio.vrt import WarpedVRT

SEN2COR_VERSION = "02.12.04"
# Increment for any change to correction settings, masks, or derived products.
PIPELINE_VERSION = "sen2cor-02.12.04-ndvi-v3"
NODATA = -9999.0
VALID_SCL = (4, 5, 6)  # vegetation, bare soil, water; exclude clouds, shadows, snow and uncertain pixels


def check_sen2cor_version() -> None:
    """Reject a processor whose reported version differs from the cataloged version."""
    result = subprocess.run(
        ["L2A_Process", "--help"],  # noqa: S607 - Sen2Cor is installed on the runner's PATH
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    match = re.search(r"Sen2Cor\. Version:\s*(\d+\.\d+\.\d+),", result.stdout)
    version = match.group(1) if match else "unknown"
    if version != SEN2COR_VERSION:
        raise RuntimeError(f"Expected Sen2Cor {SEN2COR_VERSION}, found {version}; check L2A_Process on PATH")


def correct(input_safe: Path, output_dir: Path) -> Path:
    """Run Sen2Cor at 10m resolution and return the generated L2A SAFE directory."""
    if not (input_safe / "MTD_MSIL1C.xml").is_file():
        raise ValueError("Sen2Cor requires a complete L1C SAFE product")
    check_sen2cor_version()
    output_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(  # noqa: S603 - fixed executable and local paths, without a shell
        ["L2A_Process", str(input_safe), "--output_dir", str(output_dir), "--resolution", "10"],  # noqa: S607
        check=True,
        timeout=4 * 60 * 60,
    )
    products = list(output_dir.glob("*.SAFE"))
    if len(products) != 1 or not (products[0] / "MTD_MSIL2A.xml").is_file():
        raise RuntimeError("Sen2Cor did not produce exactly one L2A SAFE product")
    return products[0]


def reflectance_parameters(metadata: Path) -> tuple[float, float, float]:
    """Read the reflectance scale and B04/B08 offsets from L2A metadata."""
    root = ElementTree.parse(metadata).getroot()
    # SAFE XML namespaces differ between PSD versions.
    for element in root.iter():
        element.tag = element.tag.rsplit("}", 1)[-1]
    quantification = root.find(".//BOA_QUANTIFICATION_VALUE")
    if quantification is None or quantification.text is None:
        raise ValueError("L2A metadata is missing BOA_QUANTIFICATION_VALUE")
    scale = float(quantification.text)
    if not np.isfinite(scale) or scale <= 0:
        raise ValueError("Invalid BOA quantification value")
    offsets = {element.attrib["band_id"]: float(element.text) for element in root.iter("BOA_ADD_OFFSET")}
    # ESA band IDs are zero-based: B04 = 3, B08 = 7. Older products have no offsets.
    if offsets and not {"3", "7"} <= offsets.keys():
        raise ValueError("Incomplete BOA offsets for B04 and B08")
    return scale, offsets.get("3", 0.0), offsets.get("7", 0.0)


# The three input bands and their calibration parameters are independent inputs.
def ndvi(  # noqa: PLR0913, PLR0917
    red_dn: NDArray[np.uint16],
    nir_dn: NDArray[np.uint16],
    scl: NDArray[np.uint8],
    scale: float,
    red_offset: float,
    nir_offset: float,
) -> NDArray[np.float32]:
    """Compute NDVI from corrected reflectance, masking invalid pixels and excluded SCL classes."""
    red = (red_dn.astype(np.float32) + red_offset) / scale
    nir = (nir_dn.astype(np.float32) + nir_offset) / scale
    denominator = nir + red
    valid = (red_dn != 0) & (nir_dn != 0) & np.isin(scl, VALID_SCL) & (red >= 0) & (nir >= 0) & (denominator > 0)
    result = np.full(red.shape, NODATA, dtype=np.float32)
    np.divide(nir - red, denominator, out=result, where=valid)
    return result


def one_file(root: Path, pattern: str) -> Path:
    """Return the matching file or fail unless exactly one exists."""
    paths = list(root.glob(pattern))
    if len(paths) != 1:
        raise ValueError(f"Expected one {pattern} in {root.name}, got {len(paths)}")
    return paths[0]


def derive(product: Path, destination: Path) -> tuple[Path, Path]:
    """Create a 10m NDVI COG and RGB thumbnail without loading full bands into RAM."""
    scale, red_offset, nir_offset = reflectance_parameters(product / "MTD_MSIL2A.xml")
    red_path = one_file(product, "GRANULE/*/IMG_DATA/R10m/*_B04_10m.jp2")
    nir_path = one_file(product, "GRANULE/*/IMG_DATA/R10m/*_B08_10m.jp2")
    scl_path = one_file(product, "GRANULE/*/IMG_DATA/R20m/*_SCL_20m.jp2")
    tci_path = one_file(product, "GRANULE/*/IMG_DATA/R10m/*_TCI_10m.jp2")
    destination.mkdir(parents=True, exist_ok=True)
    temporary = destination / "ndvi-working.tif"
    output = destination / "ndvi.tif"
    with rasterio.open(red_path) as red, rasterio.open(nir_path) as nir, rasterio.open(scl_path) as scl:
        if (red.shape, red.transform, red.crs) != (nir.shape, nir.transform, nir.crs):
            raise ValueError("B04 and B08 grids differ")
        profile = {
            "driver": "GTiff",
            "width": red.width,
            "height": red.height,
            "count": 1,
            "dtype": "float32",
            "crs": red.crs,
            "transform": red.transform,
            "nodata": NODATA,
            "tiled": True,
            "blockxsize": 512,
            "blockysize": 512,
            "compress": "deflate",
        }
        with (
            WarpedVRT(
                scl,
                crs=red.crs,
                transform=red.transform,
                width=red.width,
                height=red.height,
                resampling=Resampling.nearest,
            ) as classification,
            rasterio.open(temporary, "w", **profile) as target,
        ):
            for _, window in target.block_windows(1):
                target.write(
                    ndvi(
                        red.read(1, window=window),
                        nir.read(1, window=window),
                        classification.read(1, window=window),
                        scale,
                        red_offset,
                        nir_offset,
                    ),
                    1,
                    window=window,
                )
    copy_raster(temporary, output, driver="COG", compress="DEFLATE", overview_resampling="NEAREST")
    temporary.unlink()
    thumbnail = destination / "thumbnail.png"
    with rasterio.open(tci_path) as tci:
        height = max(1, round(512 * tci.height / tci.width))
        rgb = tci.read((1, 2, 3), out_shape=(3, height, 512), resampling=Resampling.bilinear)
    Image.fromarray(np.moveaxis(rgb, 0, -1)).save(thumbnail)
    return output, thumbnail
