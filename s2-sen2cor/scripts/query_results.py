"""List previews: uv run scripts/query_results.py --start 2025-08-01 --end 2025-09-01."""

from cyclopts import run
from dotenv import load_dotenv
from tilebox.datasets import Client
from tilebox.datasets.assets import AssetCollection
from tilebox.datasets.datapoints import iter_datapoints


def main(
    *,
    start: str,
    end: str,
    destination: tuple[str, str] = ("tilebox.sentinel2_l2a", "S2A_L2A"),
) -> None:
    """Query an acquisition interval and print each result's public preview URL.

    Example: uv run scripts/query_results.py --start 2025-08-01 --end 2025-09-01
    """
    results = Client().dataset(destination[0]).collection(destination[1]).query(temporal_extent=(start, end))
    for result in iter_datapoints(results):
        assets = AssetCollection.from_datapoint(result)
        preview = assets.get("preview") or assets.get("rgb")
        if preview is not None:
            print(result.title.item(), preview.primary.href)  # noqa: T201


if __name__ == "__main__":
    load_dotenv()
    run(main)
