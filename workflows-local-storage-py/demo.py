"""Download three CC0 photos, import them, or register a native storage automation."""

import hashlib
import json
import re
import shutil
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen

from cyclopts import App
from tilebox.workflows import Client
from tilebox.workflows.data import StorageType

from workflow import MakeThumbnail, make_thumbnail

app = App(help=__doc__, result_action="return_value")

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
REVISION = "4e8031630f4193494c79b1c1d3524108826d1ba9"
REPOSITORY = "OpenDroneMap/odm_data_aukerman"
SAMPLES = {
    "DSC00229.JPG": "307e06c034f70fc010505c0fac526b070675680c",
    "DSC00230.JPG": "1d565aea6857834a6b3cb3e40bdf5f6e9f00f24a",
    "DSC00232.JPG": "4c4cc41ff52fdda29ed2a2133079f2c1d138e812",
}


def fetch(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "tilebox-local-storage-example"})
    with urlopen(request, timeout=120) as response:
        return response.read()


def sample_paths() -> list[Path]:
    paths = [DATA / "samples" / name for name in SAMPLES]
    if not all(path.is_file() for path in paths):
        raise ValueError("Run `uv run demo.py download` first")
    return paths


@app.command
def download() -> None:
    """Download three pinned CC0 JPGs (~23 MiB)."""
    folder = DATA / "samples"
    folder.mkdir(parents=True, exist_ok=True)
    base = f"https://raw.githubusercontent.com/{REPOSITORY}/{REVISION}"
    for name, expected in SAMPLES.items():
        path = folder / name
        data = path.read_bytes() if path.exists() else fetch(f"{base}/images/{name}")
        digest = hashlib.sha1(
            f"blob {len(data)}\0".encode() + data, usedforsecurity=False
        ).hexdigest()
        if digest != expected:
            raise ValueError(f"Checksum mismatch: {path}; remove it and download again")
        if not path.exists():
            temporary = path.with_suffix(".JPG.part")
            temporary.write_bytes(data)
            temporary.replace(path)
        print(f"Verified {name} ({len(data):,} bytes)", flush=True)
    (DATA / "CC0-1.0.txt").write_bytes(fetch(f"{base}/license.txt"))
    (DATA / "SOURCE.txt").write_text(
        f"https://github.com/{REPOSITORY}/tree/{REVISION}\nLicense: CC0-1.0\n"
    )


@app.command(name="import")
def import_photos(*, batch: str = "take-01", interval: float = 0) -> None:
    """Copy three complete photos into the watched folder.

    Parameters
    ----------
    batch
        New folder name; existing batches are never overwritten.
    interval
        Seconds between file arrivals.
    """
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", batch):
        raise ValueError("--batch must contain only letters, digits, hyphens, or underscores")
    if interval < 0 or not interval < float("inf"):
        raise ValueError("--interval must be finite and nonnegative")
    photos = sample_paths()
    folder = DATA / "incoming" / batch
    folder.mkdir(parents=True)  # Refuse an existing batch instead of overwriting arrivals.
    for index, photo in enumerate(photos):
        temporary = folder / f"{photo.name}.part"
        shutil.copyfile(photo, temporary)  # Closes the file before the same-filesystem rename.
        temporary.replace(folder / photo.name)
        print(f"Imported {batch}/{photo.name}", flush=True)
        if index + 1 < len(photos):
            time.sleep(interval)


@app.command
def check() -> None:
    """Process samples directly; does not test Tilebox orchestration."""
    for photo in sample_paths():
        report = make_thumbnail(photo.read_bytes(), photo.name, str(ROOT / "results"))
        print(json.dumps(report))
    print("Processed 3 photos directly; no watcher, automation, or runner was used")


@app.command
def register(*, location: str, cluster: str) -> None:
    """Create a remote Tilebox storage automation."""
    if not cluster.strip():
        raise ValueError(
            "--cluster must not be blank; set TILEBOX_CLUSTER to your demo cluster slug"
        )
    client = Client()
    storage = client.storage_locations().find(location)
    if (
        storage.type != StorageType.LOCAL
        or Path(storage.location).resolve() != (DATA / "incoming").resolve()
    ):
        raise ValueError("Register this example's data/incoming directory as local storage first")
    name = f"local-thumbnails-{storage.id}-{cluster}"
    if any(automation.name == name for automation in client.automations().all()):
        raise ValueError(
            f"Automation {name} already exists; reuse it or delete it before replacing"
        )
    automation = client.automations().create_storage_event_automation(
        name,
        MakeThumbnail(output_dir=str(ROOT / "results")),
        (storage, "**.[jJ][pP][gG]"),
        cluster=cluster,
        max_retries=2,
    )
    print(f"Created automation: {automation.id}")
    print(f"Results: {ROOT / 'results'}")


def main() -> None:
    try:
        app(sys.argv[1:])
    except (ValueError, FileExistsError) as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(2) from error


if __name__ == "__main__":
    main()
