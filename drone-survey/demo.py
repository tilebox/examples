"""Download public sample photos, import them safely, and configure the local demo."""

import argparse
import hashlib
import json
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request, urlopen

from tilebox.workflows import Client

from workflow import PreparePhoto, ReconstructSurvey, prepare_photo

REVISION = "4e8031630f4193494c79b1c1d3524108826d1ba9"
REPOSITORY = "OpenDroneMap/odm_data_aukerman"
ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"


def fetch(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "tilebox-local-storage-example"})
    with urlopen(request, timeout=120) as response:
        return response.read()


def download(full: bool) -> None:
    folder = DATA / "samples"
    folder.mkdir(parents=True, exist_ok=True)
    files = json.loads(
        fetch(f"https://api.github.com/repos/{REPOSITORY}/contents/images?ref={REVISION}")
    )
    photos = sorted(
        (item for item in files if item["name"].endswith(".JPG")), key=lambda x: x["name"]
    )
    if not full:
        photos = photos[:12]
    base = f"https://raw.githubusercontent.com/{REPOSITORY}/{REVISION}"
    for item in photos:
        path = folder / item["name"]
        data = path.read_bytes() if path.exists() else fetch(f"{base}/images/{item['name']}")
        # Check the Git blob hash against the pinned dataset revision.
        digest = hashlib.sha1(
            f"blob {len(data)}\0".encode() + data, usedforsecurity=False
        ).hexdigest()
        if digest != item["sha"]:
            raise ValueError(f"Checksum mismatch for {path}; remove it and download again")
        if not path.exists():
            path.with_suffix(".part").write_bytes(data)
            path.with_suffix(".part").replace(path)
        print(f"Verified {path.name} ({len(data):,} bytes)", flush=True)
    (DATA / "CC0-1.0.txt").write_bytes(fetch(f"{base}/license.txt"))
    (DATA / "SOURCE.txt").write_text(
        f"https://github.com/{REPOSITORY}/tree/{REVISION}\nLicense: CC0-1.0\n"
    )
    print(f"Ready: {len(photos)} photos in {folder}")


def import_photos(interval: float) -> None:
    photos = sorted((DATA / "samples").glob("*.JPG"))
    if not photos:
        raise ValueError("Run `uv run demo.py download` first")
    run = datetime.now(UTC).strftime("survey-%Y%m%dT%H%M%S%fZ")
    folder = DATA / "incoming" / run
    folder.mkdir(parents=True)
    keys = []
    for photo in photos:
        temporary = folder / f"{photo.name}.part"
        shutil.copyfile(photo, temporary)
        temporary.replace(folder / photo.name)
        keys.append(f"{run}/{photo.name}")
        print(f"Imported {photo.name}", flush=True)
        time.sleep(interval)
    # This is a manifest for manual reconstruction, not a second automation trigger.
    (folder / "survey.json").write_text(json.dumps({"images": keys}, indent=2))
    print(f"Survey manifest key: {run}/survey.json")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    download_parser = commands.add_parser("download", help="Download 12 CC0 photos (~90 MiB)")
    download_parser.add_argument(
        "--full", action="store_true", help="Download all 77 for reconstruction"
    )
    import_parser = commands.add_parser(
        "import", help="Copy photos into a new watched survey folder"
    )
    import_parser.add_argument(
        "--interval", type=float, default=0, help="Seconds between file arrivals"
    )
    commands.add_parser("check", help="Process sample photos directly, without Tilebox API calls")
    register = commands.add_parser(
        "register", help="Create a photo automation in your Tilebox organization"
    )
    register.add_argument("--location", required=True)
    register.add_argument("--cluster", required=True)
    reconstruct = commands.add_parser(
        "reconstruct", help="Submit one completed survey to OpenDroneMap"
    )
    reconstruct.add_argument("--location", required=True)
    reconstruct.add_argument("--cluster", required=True)
    reconstruct.add_argument(
        "--manifest", required=True, help="Object key printed by the import command"
    )
    args = parser.parse_args()

    if args.command == "download":
        download(args.full)
    elif args.command == "import":
        if args.interval < 0:
            parser.error("--interval cannot be negative")
        import_photos(args.interval)
    elif args.command == "check":
        photos = sorted((DATA / "samples").glob("*.JPG"))
        if not photos:
            parser.error("Run `uv run demo.py download` first")
        for photo in photos:
            report = prepare_photo(photo.read_bytes(), photo.name, str(ROOT / "results" / "check"))
            print(json.dumps(report))
        print(f"Processed {len(photos)} photos without Tilebox API calls")
    else:
        client = Client()
        location = client.storage_locations().find(args.location)
        output = str(ROOT / "results")
        if args.command == "register":
            name = f"drone-photos-{location.id}-{args.cluster}"
            if any(automation.name == name for automation in client.automations().all()):
                parser.error(
                    f"Automation {name} already exists. Reuse it, or delete it in Console before replacing it."
                )
            automation = client.automations().create_storage_event_automation(
                name,
                PreparePhoto(output_dir=output),
                (location, "**.[jJ][pP][gG]"),
                cluster=args.cluster,
                max_retries=2,
            )
            print(f"Created automation: {automation.id}")
            print(f"Results: {output}")
        else:
            job = client.jobs().submit(
                "Reconstruct drone survey",
                ReconstructSurvey(output_dir=output).once(location, args.manifest),
                cluster=args.cluster,
                max_retries=0,
            )
            print(f"Submitted reconstruction job: {job.id}")


if __name__ == "__main__":
    main()
