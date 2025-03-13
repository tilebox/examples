from datetime import timedelta
import os
from typing import List

import dotenv
import xarray as xr
from tilebox.datasets import Client as DSClient
from tilebox.datasets.data import TimeInterval
from tilebox.workflows import Client as WFClient
from tilebox.workflows.automations import CronTask
from tilebox.workflows.data import AutomationPrototype

dotenv.load_dotenv()


dsClient = DSClient()
wfClient = WFClient()


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
        data = None
        if s2a != 0:  # if dataset is nonempty
            data = s2a
        if s2b != 0:
            if data:
                data = xr.concat([data, s2b], dim="time")
            else:
                data = s2b
        if s2c != 0:
            if data:
                data = xr.concat([data, s2c], dim="time")
            else:
                data = s2c

        print(f"Stats for {self.duration_hours}h preceding {self.trigger.time}")
        if data:
            print(f" Number of granules: {len(data.time)}")
            print(f" Average cloudiness: {data.cloud_cover.mean(dim="time").values} %")
        else:
            print(" No data loaded")

    @staticmethod
    def identifier() -> tuple[str, str]:
        return "tilebox.com/example/S2Stats", "v1.0"


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
        print(f"Created cron automation {cron_automation.name}")
    else:
        print("Cron automation already exists")

    # Start the workflow runner and wait for incoming tasks
    print("Starting workflow runner")
    wfClient.runner(cluster, [S2Stats]).run_forever()


if __name__ == "__main__":
    main()
