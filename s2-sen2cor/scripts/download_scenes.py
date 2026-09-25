"""Prewarm the cache: uv run scripts/download_scenes.py SCENE_ID_1 SCENE_ID_2."""

from cyclopts import run
from dotenv import load_dotenv
from tilebox.datasets import Client
from tqdm import tqdm

from atmospheric_correction.tasks import download_scene


def main(
    source_ids: list[str],
    *,
    dataset: str = "open_data.copernicus.sentinel2_msi",
    collection: str = "S2A_S2MSI1C",
) -> None:
    """Download scenes sequentially by ID, reusing cached files.

    Example: uv run scripts/download_scenes.py SCENE_ID_1 SCENE_ID_2 --collection S2A_S2MSI1C
    """
    source = Client().dataset(dataset).collection(collection)
    for source_id in tqdm(source_ids, desc="Downloading scenes", unit="scene"):
        scene = source.find(source_id)
        tqdm.write(f"{source_id} {scene.granule_name.item()}: {download_scene(scene)}")


if __name__ == "__main__":
    load_dotenv()
    run(main)
