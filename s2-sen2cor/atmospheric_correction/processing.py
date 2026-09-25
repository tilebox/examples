import subprocess
from pathlib import Path

import numpy as np
import rasterio
from defusedxml import ElementTree
from PIL import Image
from rasterio.enums import Resampling

SEN2COR_VERSION = "02.12.04"
PIPELINE_VERSION = "sen2cor-02.12.04-20m-v1"


def correct(input_safe: Path, output_dir: Path) -> Path:
    """Run the pinned Sen2Cor at 20 m, without optional 60 m export."""
    if not (input_safe / "MTD_MSIL1C.xml").is_file():
        raise ValueError("Sen2Cor requires an L1C SAFE product")
    output_dir.mkdir(parents=True, exist_ok=True)
    config = ElementTree.parse(Path.home() / "sen2cor/2.12/cfg/L2A_GIPP.xml")
    downsample = config.find(".//Downsample_20_to_60")
    if downsample is None:
        raise ValueError("Sen2Cor configuration is missing Downsample_20_to_60")
    downsample.text = "FALSE"
    config_path = output_dir / "L2A_GIPP.xml"
    config.write(config_path)
    subprocess.run(  # noqa: S603
        [  # noqa: S607 - the pinned processor is installed on PATH
            "L2A_Process",
            str(input_safe),
            "--output_dir",
            str(output_dir),
            "--resolution",
            "20",
            "--GIP_L2A",
            str(config_path),
        ],
        check=True,
        timeout=4 * 60 * 60,
    )
    products = list(output_dir.glob("*.SAFE"))
    if len(products) != 1 or not (products[0] / "MTD_MSIL2A.xml").is_file():
        raise RuntimeError("Sen2Cor did not produce exactly one L2A SAFE product")
    return products[0]


def rgb_preview(product: Path, destination: Path) -> Path:
    """Resize Sen2Cor's 20 m true-colour image for the hosted catalog preview."""
    images = list(product.glob("GRANULE/*/IMG_DATA/R20m/*_TCI_20m.jp2"))
    if len(images) != 1:
        raise ValueError("Expected exactly one 20 m true-colour image")
    with rasterio.open(images[0]) as source:
        height = max(1, round(512 * source.height / source.width))
        rgb = source.read((1, 2, 3), out_shape=(3, height, 512), resampling=Resampling.bilinear)
    Image.fromarray(np.moveaxis(rgb, 0, -1)).save(destination)
    return destination
