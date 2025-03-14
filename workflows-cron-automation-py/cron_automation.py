from datetime import datetime, timedelta
import os
from typing import List

from shapely import box
import dotenv
import xarray as xr
from tilebox.datasets import Client as DSClient
from tilebox.datasets.data import TimeInterval
from tilebox.workflows import Client as WFClient
from tilebox.workflows.observability.logging import get_logger
from tilebox.workflows.automations import CronTask
from tilebox.workflows.data import AutomationPrototype


import xarray as xr
from shapely import box

# setup environment
dotenv.load_dotenv()
dsClient = DSClient()
wfClient = WFClient()
logger = get_logger()


# aois defines the areas of interest, for which statistics are calculated
aois = {
    "Switzerland": box(6.0, 45.0, 10.5, 48),
    "USA": box(-180, 12.0, -40, 72.2),
}


# filter_for_aoi filters a dataset and removes all entries out of the AOI, using the geometry property of Sentinel 2 metadata
# The geometry property is a shapely.geometry.Polygon object
def filter_for_aoi(aoi: box, data: xr.Dataset) -> xr.Dataset:
    copy = data.copy()
    bools = [aoi.intersects(x) for x in copy.geometry.values]
    return copy.where(
        xr.DataArray(bools, coords=copy.geometry.coords, dims=copy.geometry.dims),
        drop=True,
    )


# S2Stats prints statistics of Sentinel 2 tasks of a configurable preceding time
class S2Stats(CronTask):
    duration_hours: int = 24

    def execute(self, context):
        # Specify the time interval to load data for based on the trigger time
        time_interval = TimeInterval(
            end=self.trigger.time,
            start=self.trigger.time - timedelta(hours=self.duration_hours),
        )

        # Load Sentinel 2 meta data for the specified time interval
        ds = dsClient.dataset("open_data.copernicus.sentinel2_msi")
        s2a = ds.collection("S2A_S2MSI2A").load(time_interval)
        s2b = ds.collection("S2B_S2MSI2A").load(time_interval)
        s2c = ds.collection("S2C_S2MSI2A").load(time_interval)

        # Assemble all collections into a single dataset
        non_empty = [ds for ds in (s2a, s2b, s2c) if ds]  # filter out empty datasets
        data = (
            xr.concat(non_empty, dim="time") if non_empty else None
        )

        # Print statistics for each AOI
        logger.info(f"Stats for {self.duration_hours}h preceding {self.trigger.time}:")
        for name, aoi in aois.items():
            filtered = filter_for_aoi(aoi, data)

            logger.info(f" {name}:")
            if data:
                logger.info(f"  Number of granules: {len(filtered.time)}")
                logger.info(
                    f" Average cloudiness: {filtered.cloud_cover.mean(dim='time').values:.2f}%"
                )
            else:
                logger.info(" No data found")

    @staticmethod
    def identifier() -> tuple[str, str]:
        return "tilebox.com/example/S2Stats", "v1.0"


# automation_exists checks if an automation with the given display name is already registered within Tilebox
def automation_exists(name: str, automations: List[AutomationPrototype]) -> bool:
    return any(automation.name == name for automation in automations)


def main():
    cluster = os.getenv("TILEBOX_CLUSTER")
    if cluster is None:
        raise ValueError("TILEBOX_CLUSTER environment variable is not set")

    # Create the automation if it doesn't exist, this can be done on
    automations = wfClient.automations()
    if not automation_exists("s2-stats-automation", automations.all()):
        cron_automation = automations.create_cron_automation(
            "s2-stats-automation",  # name of the cron automation
            S2Stats(
                duration_hours=24
            ),  # the task (and its input parameters) to run repeatedly
            ["* * * * *"],  # The cron schedule
            cluster,  # cluster slug to submit jobs to
            max_retries=3,
        )
        logger.info(f"Created cron automation {cron_automation.name}")
    else:
        logger.info("Cron automation already exists")

    # Start the workflow runner and wait for incoming tasks
    logger.info("Starting workflow runner")
    wfClient.runner(cluster, [S2Stats]).run_forever()


if __name__ == "__main__":
    main()
