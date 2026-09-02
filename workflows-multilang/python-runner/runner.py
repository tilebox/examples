from odc.geo import Geometry, Resolution
from tilebox.workflows import ExecutionContext, Runner, Task


class ScheduleImageCapture(Task):
    location: Geometry
    resolution: Resolution
    spectral_bands: list[float]

    async def execute(self, context: ExecutionContext) -> None:
        longitude, latitude = self.location.geom.coords[0]
        context.logger.info(
            f"Image captured at {latitude}, {longitude} with {abs(self.resolution.x)}m resolution "
            f"and bands {self.spectral_bands}"
        )

    @staticmethod
    def identifier() -> tuple[str, str]:
        return "tilebox.com/schedule_image_capture", "v1.0"


runner = Runner(tasks=[ScheduleImageCapture])
