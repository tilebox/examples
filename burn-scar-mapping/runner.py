from pathlib import Path

from tilebox.workflows import Client, Runner
from tilebox.workflows.cache import LocalFileSystemCache

from tasks import (
    ComputeDelta,
    ComputeNBR,
    MapBurnScars,
    MosaicRGB,
    RenderOverlay,
)

runner = Runner(
    tasks=[MapBurnScars, MosaicRGB, ComputeNBR, ComputeDelta, RenderOverlay],
    cache=LocalFileSystemCache(str(Path.home() / ".cache" / "tilebox" / "burn-scar-mapping")),
)


if __name__ == "__main__":
    # direct runner mode, when started with `python runner.py`
    runner.connect_to(Client()).run_forever()
