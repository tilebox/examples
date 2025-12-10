import numpy as np
import rasterio
from odc.geo.geobox import GeoBox


def save_numpy_to_geotiff(numpy_array: np.ndarray, geobox: GeoBox, output_filepath: str):
    """
    Saves a NumPy array with RGB bands to a GeoTIFF file using rasterio,
    incorporating spatial information from an odc.geo.geobox.GeoBox object.

    Args:
        numpy_array (np.ndarray): The input NumPy array of shape (Y, X, bands).
                                  It's assumed that bands are in Red, Green, Blue order
                                  and the dtype is uint8.
        geobox (odc.geo.geobox.GeoBox): The GeoBox object containing spatial metadata
                                        (CRS, transform, dimensions).
        output_filepath (str): The path where the GeoTIFF file will be saved.
    """
    # Ensure the input array has 3 dimensions (Y, X, Bands)
    if numpy_array.ndim != 3:
        raise ValueError(f"Input array must be 3-dimensional (Y, X, Bands), but got {numpy_array.ndim} dimensions.")

    # Ensure the number of bands is 3 (R, G, B, A)
    if numpy_array.shape[2] != 4:
        raise ValueError(f"Input array must have 4 bands (R, G, B, A), but got {numpy_array.shape[2]} bands.")

    # Ensure the data type is uint8
    if numpy_array.dtype != np.uint8:
        print(
            f"Warning: Input array dtype is {numpy_array.dtype}, "
            f"but rasterio typically expects uint8 for RGB. Converting to uint8."
        )
        numpy_array = numpy_array.astype(np.uint8)

    # Extract dimensions from the array
    height, width, count = numpy_array.shape

    # Extract spatial metadata from the GeoBox
    # The geobox.transform is an affine.Affine object, which rasterio can use directly.
    # If it's not, you might need to convert it:
    # transform = Affine(*geobox.transform.to_gdal()) # If geobox.transform is GDAL-like
    # Or, if geobox.transform is already an affine.Affine object:
    transform = geobox.transform

    # Get the CRS from the geobox
    crs = geobox.crs.crs_str  # Or geobox.crs if it's already a rasterio CRS object

    # Verify that the geobox dimensions match the array dimensions
    if geobox.height != height or geobox.width != width:
        raise ValueError(
            f"GeoBox dimensions ({geobox.height}H x {geobox.width}W) "
            f"do not match NumPy array dimensions ({height}H x {width}W)."
        )

    # Prepare metadata for the GeoTIFF file
    # Rasterio expects bands in the first dimension (count, height, width)
    # Our numpy_array is (height, width, count), so we need to transpose it
    # when writing.
    meta = {
        "driver": "GTiff",  # Specify the GeoTIFF driver
        "dtype": str(numpy_array.dtype),  # Data type of the array
        "nodata": None,  # Specify NoData value if applicable, otherwise None
        "width": width,  # Width of the raster
        "height": height,  # Height of the raster
        "count": count,  # Number of bands (4 for RGBA)
        "crs": crs,  # Coordinate Reference System
        "transform": transform,  # Affine transformation matrix
        "interleave": "pixel",  # For RGB, pixel interleave is common
    }

    try:
        # Open the GeoTIFF file in write mode
        with rasterio.open(output_filepath, "w", **meta) as dst:
            # Rasterio expects data in (bands, rows, cols) order.
            # The input numpy_array is (rows, cols, bands).
            # We need to transpose it before writing.
            # (height, width, count) -> (count, height, width)
            array_to_write = numpy_array.transpose(2, 0, 1)

            # Write each band to the GeoTIFF file
            # For a 3-band RGB image, this can be done in one go if the array is shaped correctly.
            dst.write(array_to_write)

            # Optionally, set band descriptions (e.g., for RGB)
            dst.set_band_description(1, "Red")
            dst.set_band_description(2, "Green")
            dst.set_band_description(3, "Blue")
            dst.set_band_description(4, "Alpha")

        print(f"Successfully saved GeoTIFF: {output_filepath}")

    except Exception as e:
        raise
