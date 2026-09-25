"""Submit scenes: uv run scripts/submit.py --start 2025-08-01 --end 2025-09-01."""

import os

from cyclopts import run
from dotenv import load_dotenv
from tilebox.workflows import Client

from atmospheric_correction.tasks import ProcessArea


def main(  # noqa: PLR0913 - these are the workflow's CLI inputs
    *,
    start: str = "2025-08-01",
    end: str = "2025-09-01",
    bounds: tuple[float, float, float, float] = (54.2, 24.2, 54.6, 24.6),
    source: tuple[str, str] = ("open_data.copernicus.sentinel2_msi", "S2A_S2MSI1C"),
    destination: tuple[str, str] = ("tilebox.sentinel2_l2a", "S2A_L2A"),
    max_scenes: int = 3,
    max_cloud_cover: float = 20,
) -> None:
    """Submit a job. Bounds are west/south/east/north; end is exclusive.

    Source and destination each take a dataset slug followed by a collection name.

    Example: uv run scripts/submit.py --start 2025-08-01 --end 2025-09-01 --max-scenes 3
    """
    job = (
        Client()
        .jobs()
        .submit(
            "sentinel-2-atmospheric-correction",
            ProcessArea(
                start=start,
                end=end,
                bounds=bounds,
                source=source,
                destination=destination,
                max_scenes=max_scenes,
                max_cloud_cover=max_cloud_cover,
            ),
            cluster=os.getenv("TILEBOX_CLUSTER") or None,
        )
    )
    print(f"Submitted job: https://console.tilebox.com/workflows/jobs/{job.id}")  # noqa: T201


if __name__ == "__main__":
    load_dotenv()
    run(main)
