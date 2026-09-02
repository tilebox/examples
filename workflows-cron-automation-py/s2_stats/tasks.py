import asyncio
from datetime import timedelta

import shapely
from tilebox.datasets.aio import Client as DatasetClient
from tilebox.datasets.query import TimeInterval
from tilebox.workflows import ExecutionContext, Task
from tilebox.workflows.automations import CronTask

DATASET_SLUG = "open_data.aws_earth.sentinel2"
COLLECTION = "L2A"


class S2Stats(CronTask):
    areas: dict[str, shapely.Geometry]
    duration: timedelta = timedelta(days=1)

    async def execute(self, context: ExecutionContext) -> None:
        interval = TimeInterval(start=self.trigger.time - self.duration, end=self.trigger.time)
        dataset = await DatasetClient().dataset(DATASET_SLUG)
        names = list(self.areas)
        results = await asyncio.gather(
            *(
                dataset.query(
                    collections=[COLLECTION],
                    temporal_extent=interval,
                    spatial_extent=area,
                )
                for area in self.areas.values()
            )
        )

        context.logger.info(f"Sentinel-2 statistics for {interval}")
        for name, data in zip(names, results, strict=True):
            count = data.sizes.get("time", 0)
            if count == 0:
                context.logger.info(f"{name}: no scenes")
                continue
            cloud_cover = float(data["cloud_cover"].mean().item())
            context.logger.info(f"{name}: {count} scenes, {cloud_cover:.2f}% average cloud cover")

    @staticmethod
    def identifier() -> tuple[str, str]:
        return "tilebox.com/example/S2Stats", "v1.0"


TASKS: list[type[Task]] = [S2Stats]
