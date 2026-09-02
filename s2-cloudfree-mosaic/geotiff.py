import numpy as np
import rasterio
from odc.geo.geobox import GeoBox


def save_numpy_to_geotiff(array: np.ndarray, geobox: GeoBox, output_filepath: str) -> None:
    """Save an RGBA array as a georeferenced GeoTIFF."""
    if array.ndim != 3 or array.shape[2] != 4:
        raise ValueError(f"Expected an array with shape (height, width, 4), got {array.shape}")

    height, width, count = array.shape
    if geobox.shape.yx != (height, width):
        raise ValueError(f"GeoBox shape {geobox.shape.yx} does not match array shape {(height, width)}")
    if geobox.crs is None:
        raise ValueError("GeoBox must have a CRS")

    rgba = array.astype(np.uint8, copy=False).transpose(2, 0, 1)
    with rasterio.open(
        output_filepath,
        "w",
        driver="GTiff",
        dtype=rgba.dtype,
        width=width,
        height=height,
        count=count,
        crs=geobox.crs.crs_str,
        transform=geobox.transform,
        interleave="pixel",
    ) as destination:
        destination.write(rgba)
        for band, description in enumerate(("Red", "Green", "Blue", "Alpha"), start=1):
            destination.set_band_description(band, description)
