import numpy as np
import pytest
import xarray as xr
from shapely import Geometry, box

from burn_scar_mapping.tasks import group_days, select_maximum_coverage_scene


def test_group_days_sorts_times_and_combines_same_day_passes() -> None:
    scenes = xr.Dataset(
        {"id": ("time", ["late", "next-day", "early"])},
        coords={
            "time": np.array(
                ["2025-08-21T23:59", "2025-08-22T00:00", "2025-08-21T10:00"],
                dtype="datetime64[ns]",
            )
        },
    )
    groups = group_days(scenes)
    assert list(groups) == ["2025-08-21", "2025-08-22"]
    assert [scene.id.item() for scene in groups["2025-08-21"]] == ["early", "late"]


def daily_groups(footprints: list[list[Geometry]]) -> dict[str, list[xr.Dataset]]:
    return {
        f"2025-08-{day:02}": [xr.Dataset({"geometry": ((), shape)}) for shape in shapes]
        for day, shapes in enumerate(footprints, start=1)
    }


def test_selector_uses_clipped_union_not_envelope_or_sum() -> None:
    area = box(0, 0, 4, 3)
    groups = daily_groups(
        [
            [box(-20, 0, 1, 3), box(2, 0, 4, 3)],  # Large footprint, but a gap in AOI.
            [box(0, 0, 2, 3), box(2, 0, 4, 3)],  # Full union.
            [box(0, 0, 3, 3), box(0, 0, 3, 3)],  # Overlaps do not count twice.
            [area],
            [area.difference(box(1, 1, 2, 2))],
            [box(0, 0, 3, 3)],
        ]
    )
    assert select_maximum_coverage_scene(groups.items(), area)[0] == "2025-08-02"


@pytest.mark.parametrize(
    "widths,reverse,expected",
    [
        ([4, 4, 4], False, 1),
        ([4, 4, 4], True, 3),
        ([1, 3, 3], False, 2),
        ([1, 3, 3], True, 3),
    ],
)
def test_selector_forward_and_reverse_ties(widths: list[int], reverse: bool, expected: int) -> None:
    groups = daily_groups([[box(0, 0, width, 3)] for width in widths])
    items = list(groups.items())
    if reverse:
        items.reverse()
    assert select_maximum_coverage_scene(items, box(0, 0, 4, 3))[0] == (f"2025-08-{expected:02}")
