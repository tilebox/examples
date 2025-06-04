from datetime import timedelta
import os
from typing import List

from shapely import MultiPolygon, box
import dotenv
import xarray as xr
from tilebox.datasets import Client as DSClient
from tilebox.datasets.data import TimeInterval
from tilebox.workflows import Client as WFClient
from tilebox.workflows.observability.logging import get_logger
from tilebox.workflows.automations import CronTask
from tilebox.workflows.data import AutomationPrototype


import xarray as xr

# setup environment
dotenv.load_dotenv()
dsClient = DSClient()
wfClient = WFClient()
logger = get_logger()


# aois defines the areas of interest, for which statistics are calculated
# Polygons created using Tilebox Console:
# - [Switzerland](https://console.tilebox.com/datasets/explorer/34d7b08b-8a27-4b40-819b-b11c6189695a?collectionId=5dea4d16-30f3-4f14-b713-8995fec173e8&view=explorer&polygons=AQETqQKXEjYCCxJMAv0RkQIaEsYC2BEpA%2BQRQwMWEoQD3BGuAxYSDAQCEg4EIxIiBCoSKARhEvQDXBLQA24S4AOMElQDuhI8A6cSqQKXEg==)
# - [USA](https://console.tilebox.com/datasets/explorer/34d7b08b-8a27-4b40-819b-b11c6189695a?collectionId=5dea4d16-30f3-4f14-b713-8995fec173e8&view=explorer&polygons=AwETwc4aE8/OjQ%2Bc0VoMOtb/C2HakAl82qMKyt5eC0zgbAkg4XgJtuC7C3riUA1c44kOu%2BYsEaTl9xK54w0SqeBlESveCBOk2pMTwc4aEwEHG8SrB9bClwh%2BwfQIwMBZCPbBBggFwyAHG8SrBwEJksnUFnbJmhuRwh8cv70QG0G%2BNBfGwDsWb7mEFCm6ZBOSydQW)
aois = {
    "Switzerland": MultiPolygon(
        [
            (
                (
                    (6.81, 47.59),
                    (5.66, 46.19),
                    (5.88, 46.05),
                    (6.57, 46.34),
                    (7.10, 45.68),
                    (8.09, 45.80),
                    (8.35, 46.30),
                    (9.00, 45.72),
                    (9.42, 46.30),
                    (10.36, 46.10),
                    (10.38, 46.43),
                    (10.58, 46.50),
                    (10.64, 47.05),
                    (10.12, 47.00),
                    (9.76, 47.18),
                    (9.92, 47.48),
                    (8.52, 47.94),
                    (8.28, 47.75),
                    (6.81, 47.59),
                ),
            ),
        ]
    ),
    "USA": MultiPolygon(
        [
            (
                (
                    (-126.07, 48.90),
                    (-125.93, 39.81),
                    (-118.76, 31.62),
                    (-106.94, 30.71),
                    (-96.31, 24.48),
                    (-96.04, 27.23),
                    (-85.02, 29.10),
                    (-81.16, 24.12),
                    (-79.04, 24.24),
                    (-80.10, 30.03),
                    (-75.58, 34.08),
                    (-73.32, 37.21),
                    (-64.69, 43.96),
                    (-67.48, 48.55),
                    (-72.39, 46.21),
                    (-80.23, 44.53),
                    (-86.61, 48.72),
                    (-95.64, 50.11),
                    (-126.07, 48.90),
                ),
            ),
            (
                (
                    (-153.33, 19.63),
                    (-156.58, 21.99),
                    (-160.02, 22.92),
                    (-161.92, 21.37),
                    (-158.82, 20.54),
                    (-156.11, 18.24),
                    (-153.33, 19.63),
                ),
            ),
            (
                (
                    (-139.34, 58.44),
                    (-139.62, 70.66),
                    (-157.27, 71.99),
                    (-169.61, 69.28),
                    (-168.31, 59.40),
                    (-161.86, 56.91),
                    (-180.65, 52.52),
                    (-178.79, 49.64),
                    (-139.34, 58.44),
                ),
            ),
        ]
    ),
}


def load_data_for_aoi(aoi: MultiPolygon, time_interval: TimeInterval) -> xr.Dataset:
    """Loads Sentinel 2 data for the specified AOI and time interval"""

    ds = dsClient.dataset("open_data.copernicus.sentinel2_msi")
    s2a = ds.collection("S2A_S2MSI2A").query(temporal_extent=time_interval, spatial_extent=aoi)
    s2b = ds.collection("S2B_S2MSI2A").query(temporal_extent=time_interval, spatial_extent=aoi)
    s2c = ds.collection("S2C_S2MSI2A").query(temporal_extent=time_interval, spatial_extent=aoi)

    non_empty = [ds for ds in (s2a, s2b, s2c) if ds]  # filter out empty datasets
    data = xr.concat(non_empty, dim="time") if non_empty else None

    return data


# S2Stats prints statistics of Sentinel 2 tasks of a configurable preceding time
class S2Stats(CronTask):
    duration_hours: int = 24

    def execute(self, context):
        # Specify the time interval to load data for based on the trigger time
        time_interval = TimeInterval(
            end=self.trigger.time,
            start=self.trigger.time - timedelta(hours=self.duration_hours),
        )

        # Print statistics for each AOI
        logger.info(f"Stats for {self.duration_hours}h preceding {self.trigger.time}:")
        for name, aoi in aois.items():
            # Query the data for the AOI
            data = load_data_for_aoi(aoi, time_interval)

            # filtered = filter_for_aoi(aoi, data)

            logger.info(f" {name}:")
            if data:
                logger.info(f"  Number of granules: {len(data.time)}")
                logger.info(
                    f" Average cloudiness: {data.cloud_cover.mean(dim='time').values:.2f}%"
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
    # Create the automation if it doesn't exist, this can be done on
    automations = wfClient.automations()
    if not automation_exists("s2-stats-automation", automations.all()):
        cron_automation = automations.create_cron_automation(
            "s2-stats-automation",  # name of the cron automation
            S2Stats(
                duration_hours=24
            ),  # the task (and its input parameters) to run repeatedly
            ["* * * * *"],  # The cron schedule
            max_retries=3,
        )
        logger.info(f"Created cron automation {cron_automation.name}")
    else:
        logger.info("Cron automation already exists")

    # Start the workflow runner and wait for incoming tasks
    logger.info("Starting workflow runner")
    wfClient.runner(tasks=[S2Stats]).run_forever()


if __name__ == "__main__":
    main()
