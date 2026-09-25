import subprocess
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pytest
import rasterio
from defusedxml import ElementTree
from PIL import Image
from rasterio.transform import from_origin

from atmospheric_correction.processing import correct, rgb_preview


def test_correct_20m_configuration_and_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Use only 20 m, preserve installed settings, and reject missing output or failed processing."""
    monkeypatch.setenv("HOME", str(tmp_path))
    config = tmp_path / "sen2cor/2.12/cfg/L2A_GIPP.xml"
    config.parent.mkdir(parents=True)
    config.write_text("<config><Downsample_20_to_60>TRUE</Downsample_20_to_60><Other>keep</Other></config>")
    source = tmp_path / "input.SAFE"
    source.mkdir()
    (source / "MTD_MSIL1C.xml").touch()
    output = tmp_path / "output"
    product = output / "result.SAFE"
    product.mkdir(parents=True)
    metadata = product / "MTD_MSIL2A.xml"
    metadata.touch()
    run = Mock()
    monkeypatch.setattr(subprocess, "run", run)
    assert correct(source, output) == product
    run.assert_called_once_with(
        [
            "L2A_Process",
            str(source),
            "--output_dir",
            str(output),
            "--resolution",
            "20",
            "--GIP_L2A",
            str(output / "L2A_GIPP.xml"),
        ],
        check=True,
        timeout=14400,
    )
    configured = ElementTree.parse(output / "L2A_GIPP.xml")
    assert configured.findtext("Downsample_20_to_60") == "FALSE"
    assert configured.findtext("Other") == "keep"
    assert ElementTree.parse(config).findtext("Downsample_20_to_60") == "TRUE"
    metadata.unlink()
    with pytest.raises(RuntimeError, match="exactly one"):
        correct(source, output)
    run.side_effect = subprocess.CalledProcessError(1, "L2A_Process")
    with pytest.raises(subprocess.CalledProcessError):
        correct(source, output)


def test_rgb_preview_uses_20m_and_preserves_channels(tmp_path: Path) -> None:
    """Exercise real raster IO with unequal RGB channels and a non-square image."""
    product = tmp_path / "product.SAFE"
    tci = product / "GRANULE/tile/IMG_DATA/R20m/test_TCI_20m.jp2"
    tci.parent.mkdir(parents=True)
    with rasterio.open(
        tci,
        "w",
        driver="GTiff",
        width=6,
        height=4,
        count=3,
        dtype="uint8",
        crs="EPSG:32640",
        transform=from_origin(200000, 2700000, 20, 20),
    ) as raster:
        raster.write(np.stack([np.full((4, 6), value, dtype="uint8") for value in (40, 90, 170)]))
    preview = rgb_preview(product, tmp_path / "rgb.png")
    with Image.open(preview) as image:
        assert image.size == (512, 341)
        assert image.getpixel((250, 100)) == (40, 90, 170)
