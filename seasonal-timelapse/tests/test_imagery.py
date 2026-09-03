from pathlib import Path

import numpy as np
from affine import Affine
from PIL import Image

from seasonal_rgb_timelapse.imagery import align_visual, encode_animated, lut, render_frame


def test_lut_applies_fixed_levels_to_each_rgb_channel() -> None:
    """The vectorized display curve produces one identical LUT per channel."""
    table = lut(gamma=1, black_point=10, white_point=110)

    assert len(table) == 256 * 3
    assert table[:256] == table[256:512] == table[512:]
    assert table[10] == 0
    assert table[60] == 128
    assert table[110] == 255


def test_render_frame_writes_branded_square_image(tmp_path: Path) -> None:
    """A rendered frame contains a square crop and readable branding panel."""
    visual = np.full((300, 500, 3), (10, 20, 30), dtype=np.uint8)
    destination = tmp_path / "frame.png"

    render_frame(visual, destination=destination, caption="S2B_33UXP_20260306_0_L2A")

    with Image.open(destination) as image:
        assert image.size == (300, 300)
        assert max(image.getpixel((150, 150))) < 40
        panel = np.asarray(image)[266:292, 8:292]
        assert np.count_nonzero(panel[:, :, 0] > 200) > panel.shape[0] * panel.shape[1] // 2


def test_encode_animated_writes_looping_webp(tmp_path: Path) -> None:
    """Animated WebP encoding preserves dimensions, timing, and input frames."""
    frame_paths = [tmp_path / "0.png", tmp_path / "1.png"]
    for path, color in zip(frame_paths, ("red", "blue"), strict=True):
        Image.new("RGB", (32, 32), color).save(path)

    destination = tmp_path / "animation.webp"
    encode_animated(frame_paths, destination)

    with Image.open(destination) as image:
        assert image.format == "WEBP"
        assert image.size == (32, 32)
        assert image.n_frames == 2
        assert image.info["loop"] == 0


def test_render_frame_uses_fixed_sentinel2_display_levels(tmp_path: Path) -> None:
    """Sentinel-2 black and white points are fixed across frames."""
    dark_path = tmp_path / "dark.png"
    bright_path = tmp_path / "bright.png"

    render_frame(
        np.full((100, 100, 3), 5, dtype=np.uint8),
        destination=dark_path,
        caption="S2B_33UXP_20260306_0_L2A",
    )
    render_frame(
        np.full((100, 100, 3), 250, dtype=np.uint8),
        destination=bright_path,
        caption="S2B_33UXP_20260306_0_L2A",
    )

    with Image.open(dark_path) as dark, Image.open(bright_path) as bright:
        assert dark.getpixel((0, 0)) == (0, 0, 0)
        assert bright.getpixel((0, 0)) == (255, 255, 255)


def test_align_visual_keeps_world_coordinates_fixed_across_source_grids() -> None:
    """The same geographic feature lands on the same output row for every scene."""
    first = np.zeros((3, 10, 10), dtype=np.uint8)
    first[:, 4, :] = 255
    shifted = np.zeros((3, 12, 12), dtype=np.uint8)
    shifted[:, 5, 1:11] = 255

    first_aligned = align_visual(
        first,
        source_transform=Affine.translation(0, 10) * Affine.scale(1, -1),
        source_crs="EPSG:4326",
        source_nodata=None,
        bounds=(0, 0, 10, 10),
    )
    shifted_aligned = align_visual(
        shifted,
        source_transform=Affine.translation(-1, 11) * Affine.scale(1, -1),
        source_crs="EPSG:4326",
        source_nodata=None,
        bounds=(0, 0, 10, 10),
    )

    assert first_aligned.shape == (900, 900, 3)
    first_profile = first_aligned[:, :, 0].mean(axis=1)
    shifted_profile = shifted_aligned[:, :, 0].mean(axis=1)
    assert np.argmax(first_profile) == np.argmax(shifted_profile)
    np.testing.assert_array_equal(
        np.flatnonzero(first_profile > first_profile.max() / 2),
        np.flatnonzero(shifted_profile > shifted_profile.max() / 2),
    )
