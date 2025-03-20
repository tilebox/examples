from collections import defaultdict

import numpy as np
import shapely
import xarray as xr


# haversine_distance_km calculates the great-circle distance between two points on a sphere using the haversine formula
# This is used for performant, rough filtering of Sentinel 2 granules
def haversine_distance_km(
        coords_a_lat_lon: list | tuple, coords_b_lat_lon: list | tuple
) -> float:
    """
    Calculates the great-circle distance between two points on a sphere
    using the haversine formula

    More information: https://www.movable-type.co.uk/scripts/latlong.html

    Args:
        coords_a_lat_lon: coordinates of the first point (lat1, lon1)
        coords_b_lat_lon: coordinates of the second point (lat1, lon1)
    Returns:
        haversine distance between the two defined points in km
    """
    EARTH_RADIUS_METER = 6371000
    lat1, lon1 = coords_a_lat_lon[0], coords_a_lat_lon[1]
    lat2, lon2 = coords_b_lat_lon[0], coords_b_lat_lon[1]

    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)

    a = np.sin(delta_phi / 2) * np.sin(delta_phi / 2) + np.cos(phi1) * \
        np.cos(phi2) * np.sin(delta_lambda / 2) * np.sin(delta_lambda / 2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    distance_m = EARTH_RADIUS_METER * c

    return distance_m / 1000  # haversine distance in km


# preliminary_filter filters a dataset and removes all entries out of the AOI, using the center property of Sentinel 2
# metadata. We know Sentinel 2 granules are around 100km in width, so using 150km as a buffer is sufficient to cover
# the whole granule. Not all granules that are returned here will contain the coordinates of the AOI
def rough_aoi_filter(coords: xr.Dataset, data: xr.Dataset) -> xr.Dataset:
    data["match"] = xr.DataArray(np.zeros(len(data.time)), dims=("time",))
    for lon, lat in zip(coords.lon.values, coords.lat.values):
        distances_km = haversine_distance_km(data.center.T, (lat, lon))
        nearby = (distances_km < 150)  # find granules within 150km of our point of interest
        data["match"] = xr.where(nearby, 1, data["match"])

    filtered = data.where(data["match"] == 1, drop=True)
    filtered = filtered.drop_vars("match")
    return filtered


# filter_for_coords filters a dataset and removes all entries out of the AOI, using the geometry property of Sentinel 2
# metadata. This filter is precise, as it uses the actual geometry of the granule to determine if it contains the
# coordinates ofthe AOI
def precise_aoi_filter(coords: xr.Dataset, data: xr.Dataset) -> xr.Dataset:
    intersects = np.zeros(len(data.geometry.values), dtype=bool)
    for i, granule in enumerate(data.geometry.values):
        for lon, lat in zip(coords.lon.values, coords.lat.values):
            contained = shapely.contains_xy(granule, lon, lat)
            if contained:
                intersects[i] = True
                break
    return data.where(
        xr.DataArray(intersects, coords=data.geometry.coords, dims=data.geometry.dims),
        drop=True,
    )


# select_least_cloudy_granules finds the granules that contain the point of interest with the lowest cloud cover
def select_least_cloudy_granules(coords: xr.Dataset, data: xr.Dataset) -> xr.Dataset:
    # granule_indices_by_point contains granules that contain the point of interest
    granule_indices_by_point = defaultdict(list)
    # cloud_cover_by_point contains the cloud cover of each of the above granules that contains the point of interest
    cloud_cover_by_point = defaultdict(list)

    # For each point of interest, find all granules that contain it
    for point_index in range(len(coords.index)):
        for granule_index, granule_geometry in enumerate(data.geometry.values):
            contained = shapely.contains_xy(granule_geometry,
                                            coords.lon.isel(index=point_index).values,
                                            coords.lat.isel(index=point_index).values)
            if contained:
                granule_indices_by_point[point_index].append(granule_index)
                cloud_cover_by_point[point_index].append(data.cloud_cover.isel(time=granule_index).values)

    # Deduplication of granules.
    # If a granule is the best granule for multiple points, we want to keep it only once
    granules = set()

    # Find the granule with the lowest cloud cover for each point of interest
    for point_index, cloud_coverages in cloud_cover_by_point.items():
        # logger.info(f"Point {point_index} has {len(cloud_coverages)} granules")
        # logger.info(f"  Cloud covers (limited to 5 values):")
        # for granule_index, cloud_cover in enumerate(cloud_coverages)[:5]:
        #     granule = data.isel(time=granule_indices_by_point[point_index][granule_index])
        # logger.info(f"    {granule_index}: {granule.id.values}: {cloud_cover}")

        min_cloud_cover_index = np.argmin(cloud_coverages)
        selected_granule = granule_indices_by_point[point_index][min_cloud_cover_index]
        granules.add(selected_granule)
        # logger.info(f"  Selected granule with cloud cover {cloud_coverages[min_cloud_cover_index]}")
        # logger.info(f"  POI Coordinates: {coords.isel(index=point_index).lat.values}, {coords.isel(index=point_index).lon.values}")
        # logger.info(f"  Granule Center: {data.center.isel(time=selected_granule).values}")

    # Return the granules from our set
    return data.isel(time=list(granules))
