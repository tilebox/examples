"""Manage the demo catalog: uv run scripts/catalog.py --help."""

from cyclopts import App
from dotenv import load_dotenv
from tilebox.datasets import Client
from tilebox.datasets.query import TimeInterval

from atmospheric_correction.catalog import create_dataset

app = App()


@app.command
def create(*, name: str = "sentinel2_l2a", collection: str = "S2A_L2A") -> None:
    """Create or update the results dataset and collection.

    Example: uv run scripts/catalog.py create --name "sentinel2_l2a"
    """
    print(create_dataset(name, collection))  # noqa: T201


@app.command
def empty(*, dataset: str, collection: str) -> None:
    """Delete ALL datapoints in the collection, keeping its schema and stored files.

    Example: uv run scripts/catalog.py empty --dataset tilebox.sentinel2_l2a --collection S2A_L2A
    """
    target = Client().dataset(dataset).collection(collection)
    availability = target.info().availability
    deleted = 0
    if availability is not None:
        points = target.query(
            temporal_extent=TimeInterval(availability.start, availability.end, end_inclusive=True),
            skip_data=True,
        )
        if points.sizes.get("time", 0):
            deleted = target.delete(points, show_progress=True)
    print(f"Deleted {deleted} datapoints from {dataset}/{collection}")  # noqa: T201


if __name__ == "__main__":
    load_dotenv()
    app()
