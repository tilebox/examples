from tilebox.workflows import Client, Runner
from tilebox.workflows.cache import LocalFileSystemCache

from burn_scar_mapping.tasks import (
    ComputeDelta,
    ComputeNBR,
    MapBurnScars,
    MosaicRGB,
    RenderOverlay,
)

runner = Runner(
    tasks=[MapBurnScars, MosaicRGB, ComputeNBR, ComputeDelta, RenderOverlay],
    cache=LocalFileSystemCache("cache"),
)


if __name__ == "__main__":
    # direct runner mode, when started with `python runner.py`
    runner.connect_to(Client()).run_forever()
