"""Preview scene selection and a cache-warming command: uv run scripts/query_scenes.py."""

import shlex

from cyclopts import run
from dotenv import load_dotenv
from shapely.geometry import box
from tilebox.datasets import Client

from atmospheric_correction.tasks import select_scenes


def main(  # noqa: PLR0913 - mirrors the root task's scene-selection inputs
    *,
    start: str = "2025-08-01",
    end: str = "2025-09-01",
    bounds: tuple[float, float, float, float] = (54.2, 24.2, 54.6, 24.6),
    dataset: str = "open_data.copernicus.sentinel2_msi",
    collection: str = "S2A_S2MSI1C",
    max_scenes: int = 3,
    max_cloud_cover: float = 20,
) -> None:
    """Print the scenes the root task would select, followed by their download command.

    Defaults match the README demo. Bounds are west/south/east/north; end is exclusive.
    This only queries metadata: it does not submit tasks or download imagery.

    Example: uv run scripts/query_scenes.py --start 2025-08-01 --end 2025-09-01 --max-scenes 3
    """
    if max_scenes < 1 or not 0 <= max_cloud_cover <= 100:
        raise ValueError("Expected positive max_scenes and cloud cover in [0, 100]")
    west, south, east, north = bounds
    if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
        raise ValueError("Expected WGS84 west, south, east, north bounds")
    scenes = (
        Client()
        .dataset(dataset)
        .collection(collection)
        .query(
            temporal_extent=(start, end),
            spatial_extent=box(*bounds),
        )
    )
    selected = select_scenes(scenes, max_cloud_cover, max_scenes)
    if not selected.sizes.get("time", 0):
        print("No scenes match the selection.")  # noqa: T201
        return
    scene_ids = [str(value) for value in selected.id.values]
    print("Selected scene IDs:", *scene_ids, sep="\n")  # noqa: T201
    command = ["uv", "run", "scripts/download_scenes.py", *scene_ids, "--dataset", dataset, "--collection", collection]
    print(f"\nWarm the cache:\n{shlex.join(command)}")  # noqa: T201


if __name__ == "__main__":
    load_dotenv()
    run(main)
