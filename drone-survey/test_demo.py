"""Offline contracts; no Tilebox API, Docker, or downloaded dataset required."""

import hashlib
import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from PIL import ExifTags, Image
from tilebox.workflows.automations.client import AutomationClient
from tilebox.workflows.data import RunnerContext, StorageLocation, StorageType
from tilebox.workflows.jobs.client import JobClient
from tilebox.workflows.storage_locations.client import StorageLocationClient
from tilebox.workflows.task import deserialize_task

import demo
import workflow
from workflow import PreparePhoto, ReconstructSurvey, nodeodm, prepare_photo


@pytest.fixture
def context():
    return SimpleNamespace(
        current_task=SimpleNamespace(display=""),
        logger=SimpleNamespace(info=lambda *args, **kwargs: None),
    )


def local_storage(path):
    return StorageLocation(UUID(int=1), str(path), StorageType.LOCAL)


def photo_bytes(format="JPEG"):
    image = Image.new("RGB", (120, 80), "black")
    image.paste("white", (60, 0, 120, 80))
    exif = Image.Exif()
    exif[272] = "Test camera"
    exif[274] = 6  # Rotate clockwise: asymmetric dimensions catch missing orientation.
    exif[ExifTags.IFD.Exif] = {36867: "2024:03:12 09:30:00", 36881: "+01:00"}
    exif[ExifTags.IFD.GPSInfo] = {1: "S", 2: (12, 30, 0), 3: "E", 4: (45, 15, 0)}
    data = io.BytesIO()
    options = {"save_all": True, "append_images": [image.copy()]} if format == "MPO" else {}
    image.save(data, format=format, exif=exif, quality=100, **options)
    return data.getvalue()


@pytest.mark.parametrize("format", ["JPEG", "MPO"])
def test_storage_task_metadata_preview_and_repeat(tmp_path, monkeypatch, context, format):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    data = photo_bytes(format)
    (incoming / "photo.JPG").write_bytes(data)
    output = tmp_path / "output"
    task = PreparePhoto(output_dir=str(output)).once(local_storage(incoming), "photo.JPG")
    monkeypatch.chdir(tmp_path.parent)  # Release workers do not run in the example directory.
    task.execute(context)
    task.execute(context)
    reports = list(output.glob("photos/*/metadata.json"))
    assert len(reports) == 1
    report = json.loads(reports[0].read_text())
    assert report["sha256"] == hashlib.sha256(data).hexdigest()
    assert (report["width"], report["height"]) == (80, 120)
    assert report["camera"] == "Test camera"
    assert report["geometry"] == {"type": "Point", "coordinates": [45.25, -12.5]}
    assert report["capture_time"] == "2024:03:12 09:30:00"
    assert report["capture_time_offset"] == "+01:00"
    assert report["dark_pixel_fraction"] == pytest.approx(0.5, abs=0.02)
    assert report["bright_pixel_fraction"] == pytest.approx(0.5, abs=0.02)
    assert report["laplacian_variance"] > 0
    with Image.open(report["preview_path"]) as preview:
        assert preview.size == (80, 120)
        assert preview.getpixel((40, 15))[0] < 5
        assert preview.getpixel((40, 105))[0] > 250


def test_invalid_photos_and_relative_output_emit_nothing(tmp_path):
    with pytest.raises(ValueError, match="absolute"):
        prepare_photo(photo_bytes(), "a.JPG", "relative-output")
    png = io.BytesIO()
    Image.new("RGB", (10, 10)).save(png, format="PNG")
    for data in (png.getvalue(), photo_bytes()[:-100]):
        with pytest.raises((ValueError, OSError)):
            prepare_photo(data, "a.JPG", str(tmp_path))
    assert list(tmp_path.iterdir()) == []


def test_import_completes_manifest_with_unique_survey_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(demo, "DATA", tmp_path)
    samples = tmp_path / "samples"
    samples.mkdir()
    (samples / "a.JPG").write_bytes(photo_bytes())
    (samples / "unfinished.JPG.part").write_bytes(b"partial")
    demo.import_photos(0)
    demo.import_photos(0)
    manifests = list((tmp_path / "incoming").glob("*/survey.json"))
    assert len(manifests) == 2
    for manifest in manifests:
        keys = json.loads(manifest.read_text())["images"]
        assert keys == [f"{manifest.parent.name}/a.JPG"]
        assert (tmp_path / "incoming" / keys[0]).read_bytes() == photo_bytes()
    assert not list((tmp_path / "incoming").rglob("*.part"))


def test_register_uses_sdk_storage_api_and_only_photo_trigger(tmp_path, monkeypatch):
    registered = []
    locations = StorageLocationClient(SimpleNamespace(get_by_id=lambda id: local_storage(tmp_path)))
    automations = AutomationClient(
        SimpleNamespace(
            list=lambda: registered, create=lambda item: registered.append(item) or item
        )
    )
    monkeypatch.setattr(
        demo,
        "Client",
        lambda: SimpleNamespace(
            storage_locations=lambda: locations,
            automations=lambda: automations,
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "demo.py",
            "register",
            "--location",
            str(UUID(int=1)),
            "--cluster",
            "demo-cluster",
        ],
    )
    demo.main()
    assert len(registered) == 1
    automation = registered[0]
    assert automation.prototype.max_retries == 2
    assert automation.prototype.cluster_slug == "demo-cluster"
    assert automation.storage_event_triggers[0].glob_pattern == "**.[jJ][pP][gG]"
    assert Path(deserialize_task(PreparePhoto, automation.prototype.input).output_dir).is_absolute()
    with pytest.raises(SystemExit):
        demo.main()
    assert len(registered) == 1


def test_reconstruct_submits_one_task_without_retries(tmp_path, monkeypatch):
    submissions = []
    jobs = JobClient(
        SimpleNamespace(
            submit=lambda name, trace, tasks: (
                submissions.append(tasks) or SimpleNamespace(id=UUID(int=2))
            )
        ),
        None,
        RunnerContext().tracer,
    )
    locations = StorageLocationClient(SimpleNamespace(get_by_id=lambda id: local_storage(tmp_path)))
    monkeypatch.setattr(
        demo,
        "Client",
        lambda: SimpleNamespace(
            storage_locations=lambda: locations,
            jobs=lambda: jobs,
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "demo.py",
            "reconstruct",
            "--location",
            str(UUID(int=1)),
            "--cluster",
            "demo-cluster",
            "--manifest",
            "survey/survey.json",
        ],
    )
    demo.main()
    assert len(submissions) == 1
    assert len(submissions[0].task_groups) == 1
    group = submissions[0].task_groups[0]
    assert len(group.inputs) == 1
    assert group.max_retries_values == [0]
    assert submissions[0].identifier_lookup[0].name == "drone-survey/ReconstructSurvey"


@pytest.mark.parametrize(
    "url, expected",
    [
        ("http://127.0.0.1:3000", "http://127.0.0.1:3000/info"),
        ("https://localhost", "https://localhost/info"),
        ("http://[::1]:3000", "http://[::1]:3000/info"),
    ],
)
def test_nodeodm_protocol(monkeypatch, url, expected):
    monkeypatch.setenv("NODEODM_URL", url)
    monkeypatch.delenv("NODEODM_TOKEN", raising=False)
    assert nodeodm().url("/info", {}) == expected


@pytest.mark.parametrize(
    "url", ["https://localhost:3000", "http://localhost:443", "http://localhost/api"]
)
def test_nodeodm_rejects_silently_changed_urls(monkeypatch, url):
    monkeypatch.setenv("NODEODM_URL", url)
    with pytest.raises(ValueError):
        nodeodm()


@pytest.mark.parametrize(
    "images",
    [
        ["a.JPG", "b.JPG"],
        ["a.JPG", "b.JPG", "a.JPG"],
        ["a.JPG", "b.JPG", "./a.JPG"],
        ["a.JPG", "b.JPG", "../c.JPG"],
        ["a.JPG", "b.JPG", ["c.JPG"]],
    ],
)
def test_invalid_survey_rejected_before_reserving_task(tmp_path, context, images):
    (tmp_path / "survey.json").write_text(json.dumps({"images": images}))
    output = tmp_path / "output"
    task = ReconstructSurvey(output_dir=str(output)).once(local_storage(tmp_path), "survey.json")
    with pytest.raises(ValueError):
        task.execute(context)
    assert not output.exists()


def test_reconstruction_copies_assets_and_blocks_duplicate_submission(
    tmp_path, monkeypatch, context
):
    # Fake only the external ODM server. The SDK reads real files from local storage.
    class CompletedTask:
        uuid = "test-task"

        def wait_for_completion(self, **kwargs):
            pass

        def download_assets(self, destination):
            folder = Path(destination) / "odm_orthophoto"
            folder.mkdir(parents=True)
            (folder / "odm_orthophoto.tif").write_bytes(b"test raster bytes")
            return destination

    monkeypatch.setattr(
        workflow,
        "nodeodm",
        lambda: SimpleNamespace(
            create_task=lambda *args, **kwargs: CompletedTask(),
        ),
    )
    images = ["a.JPG", "b.JPG", "c.JPG"]
    for key in images:
        (tmp_path / key).write_bytes(photo_bytes())
    (tmp_path / "survey.json").write_text(json.dumps({"images": images}))
    output = tmp_path / "output"
    task = ReconstructSurvey(output_dir=str(output)).once(local_storage(tmp_path), "survey.json")
    task.execute(context)
    assert next(output.glob("surveys/*/odm_orthophoto.tif")).read_bytes() == b"test raster bytes"
    receipt = next(output.glob("surveys/*/nodeodm-task.json"))
    assert UUID(json.loads(receipt.read_text())["task_id"])
    with pytest.raises(FileExistsError):
        task.execute(context)
