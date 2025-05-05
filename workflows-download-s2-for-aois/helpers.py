import numpy as np
import xarray as xr
from shapely import Polygon
from tilebox.workflows.observability.logging import get_logger


def polygon_from_poi(lon: float = 0.0, lat: float = 0.0) -> Polygon:
    return Polygon(
        [
            (lon - 0.1, lat - 0.1),
            (lon - 0.1, lat + 0.1),
            (lon + 0.1, lat + 0.1),
            (lon + 0.1, lat - 0.1),
            (lon - 0.1, lat - 0.1),
        ]
    )


# select_least_cloudy_granules finds the granules that contain the point of interest with the lowest cloud cover
def select_least_cloudy_granules(data: xr.Dataset) -> xr.Dataset:
    logger = get_logger()

    coordinate_indices = np.unique(data.coordinate_idx.values)

    result = None

    # for each point of interest, find the granule with the lowest cloud cover
    for i in coordinate_indices:
        poi_data = data.where(data.coordinate_idx == i, drop=True)
        logger.debug(f"Coordinate {i:.0f} has {len(poi_data.time)} valid granules")

        poi_data = poi_data.sortby("cloud_cover")
        logger.debug(f"  Sorted by cloud cover")
        logger.debug(f"  Cloud covers (limited to 5 values):")
        for granule_index, cloud_cover in enumerate(poi_data.cloud_cover.values):
            granule = poi_data.isel(time=granule_index)
            logger.debug(f"    {granule_index}: {granule.id.values}: {cloud_cover}")
            if granule_index >= 5:
                break
        logger.debug(f"  Selected granule 0 with cloud cover {poi_data.cloud_cover.values[0]}")

        # Select the granule with the lowest cloud cover and add it to the result
        result = xr.concat([result, poi_data.isel(time=0)], dim="time") if result else poi_data.isel(time=0)

    return result
