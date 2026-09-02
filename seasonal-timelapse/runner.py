from tilebox.workflows import Runner

from seasonal_rgb_timelapse.tasks import (
    AssembleTimelapse,
    BuildSeasonalTimelapse,
    RenderSeasonalFrame,
)

runner = Runner(tasks=[BuildSeasonalTimelapse, RenderSeasonalFrame, AssembleTimelapse])
