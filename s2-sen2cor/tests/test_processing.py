import subprocess

import numpy as np
import pytest
import rasterio
from PIL import Image
from rasterio.transform import from_origin

from sen2cor_workflow import processing
from sen2cor_workflow.processing import NODATA, check_sen2cor_version, correct, derive, ndvi, reflectance_parameters


def metadata(path, offsets=True):
    """Write sample L2A metadata with optional band offsets."""
    path.write_text(
        '<n:Level2 xmlns:n="urn:test"><n:BOA_QUANTIFICATION_VALUE>10000</n:BOA_QUANTIFICATION_VALUE>'
        + (
            '<n:BOA_ADD_OFFSET band_id="3">-1000</n:BOA_ADD_OFFSET>'
            '<n:BOA_ADD_OFFSET band_id="7">-500</n:BOA_ADD_OFFSET>'
            if offsets
            else ""
        )
        + "</n:Level2>"
    )


def test_offsets_masks_and_unsigned_arithmetic():
    """Check reflectance offsets, signed subtraction, and invalid-pixel masking."""
    red = np.array([2000, 5000, 0, 2000, 1000, 900, 2000], dtype=np.uint16)
    nir = np.array([4500, 1500, 4500, 4500, 500, 4500, 4500], dtype=np.uint16)
    scl = np.array([4, 5, 4, 9, 6, 4, 3], dtype=np.uint8)
    actual = ndvi(red, nir, scl, 10000, -1000, -500)
    # Reflectances .1/.4 -> .6, .4/.1 -> -.6. Raw DN division would give different values.
    np.testing.assert_allclose(actual, [0.6, -0.6, NODATA, NODATA, NODATA, NODATA, NODATA])


def test_metadata_offsets_and_legacy(tmp_path):
    """Check present, absent, and incomplete band offsets in L2A metadata."""
    xml = tmp_path / "metadata.xml"
    metadata(xml)
    assert reflectance_parameters(xml) == (10000, -1000, -500)
    metadata(xml, offsets=False)
    assert reflectance_parameters(xml) == (10000, 0, 0)
    xml.write_text(
        "<root><BOA_QUANTIFICATION_VALUE>10000</BOA_QUANTIFICATION_VALUE>"
        '<BOA_ADD_OFFSET band_id="3">-1000</BOA_ADD_OFFSET></root>'
    )
    with pytest.raises(ValueError, match="Incomplete"):
        reflectance_parameters(xml)


def test_correct_propagates_failure_and_rejects_incomplete_output(tmp_path, monkeypatch):
    """Check the Sen2Cor command, process errors, and missing output rejection."""
    monkeypatch.setattr(processing, "check_sen2cor_version", lambda: None)
    source = tmp_path / "input.SAFE"
    source.mkdir()
    (source / "MTD_MSIL1C.xml").touch()
    destination = tmp_path / "output"

    def fail(command, **kwargs):
        """Validate the process arguments and simulate a Sen2Cor failure."""
        assert command == ["L2A_Process", str(source), "--output_dir", str(destination), "--resolution", "10"]
        assert kwargs == {"check": True, "timeout": 14400}
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        correct(source, destination)
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: None)
    with pytest.raises(RuntimeError, match="exactly one"):
        correct(source, destination)


@pytest.mark.parametrize("version", ["02.12.04", "02.12.03", "unknown"])
def test_processor_version_is_checked_before_correction(tmp_path, monkeypatch, version):
    """Accept the pinned release and reject wrong or missing versions before processing."""
    calls = []

    def help_output(command, **kwargs):
        """Return processor help and reject any attempt to process a scene."""
        calls.append(command)
        assert command == ["L2A_Process", "--help"]
        assert kwargs == {"check": True, "capture_output": True, "text": True, "timeout": 60}
        return subprocess.CompletedProcess(command, 0, f"Sen2Cor. Version: {version}, created: 2025.11.28")

    monkeypatch.setattr(subprocess, "run", help_output)
    if version == "02.12.04":
        check_sen2cor_version()
    else:
        source = tmp_path / "input.SAFE"
        source.mkdir()
        (source / "MTD_MSIL1C.xml").touch()
        with pytest.raises(RuntimeError, match=f"Expected Sen2Cor 02.12.04, found {version}"):
            correct(source, tmp_path / "output")
        assert not (tmp_path / "output").exists()
    assert len(calls) == 1


def test_derive_cog_georeferencing_and_scl_resampling(tmp_path):
    """Check the NDVI grid, resampled mask, COG layout, and thumbnail pixels."""
    product = tmp_path / "test.SAFE"
    r10 = product / "GRANULE" / "tile" / "IMG_DATA" / "R10m"
    r20 = r10.parent / "R20m"
    r10.mkdir(parents=True)
    r20.mkdir()
    metadata(product / "MTD_MSIL2A.xml")

    def write(path, data, resolution):
        """Write a small georeferenced raster at the requested resolution."""
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            width=data.shape[-1],
            height=data.shape[-2],
            count=data.shape[0],
            dtype=data.dtype,
            crs="EPSG:32640",
            transform=from_origin(200000, 2700000, resolution, resolution),
        ) as dst:
            dst.write(data)

    # Real raster I/O, small synthetic grids. GDAL detects the GTiff content despite SAFE filenames.
    write(r10 / "T_B04_10m.jp2", np.full((1, 4, 6), 2000, dtype="uint16"), 10)
    write(r10 / "T_B08_10m.jp2", np.full((1, 4, 6), 4500, dtype="uint16"), 10)
    write(r20 / "T_SCL_20m.jp2", np.array([[[4, 9, 5], [3, 6, 11]]], dtype="uint8"), 20)
    rgb = np.stack([np.full((4, 6), value, dtype="uint8") for value in (40, 90, 170)])
    write(r10 / "T_TCI_10m.jp2", rgb, 10)
    output, thumbnail = derive(product, tmp_path / "derived")
    with rasterio.open(output) as result:
        assert result.crs.to_epsg() == 32640
        assert result.transform == from_origin(200000, 2700000, 10, 10)
        assert result.nodata == NODATA
        assert result.tags(ns="IMAGE_STRUCTURE")["LAYOUT"] == "COG"
        expected = np.array([[0.6, NODATA, 0.6], [NODATA, 0.6, NODATA]], dtype="float32")
        np.testing.assert_allclose(result.read(1), expected.repeat(2, axis=0).repeat(2, axis=1))
    with Image.open(thumbnail) as image:
        assert image.size == (512, 341)
        assert image.getpixel((0, 0)) == (40, 90, 170)
    assert not (output.parent / "ndvi-working.tif").exists()
