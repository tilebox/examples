from pathlib import Path

import numpy as np
from PIL import Image

from seasonal_rgb_timelapse.imagery import encode_animated, lut, render_frame


def test_lut_applies_fixed_levels_to_each_rgb_channel() -> None:
    """The vectorized display curve produces one identical LUT per channel."""
    table = lut(gamma=1, black_point=10, white_point=110)

    assert len(table) == 256 * 3
    assert table[:256] == table[256:512] == table[512:]
    assert table[10] == 0
    assert table[60] == 128
    assert table[110] == 255


def test_render_frame_writes_unlabelled_square_image(tmp_path: Path) -> None:
    """A rendered frame contains only a square crop of the source image."""
    visual = np.full((60, 100, 3), (10, 20, 30), dtype=np.uint8)
    destination = tmp_path / "frame.png"

    render_frame(visual, destination=destination)

    with Image.open(destination) as image:
        assert image.size == (60, 60)


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

    render_frame(np.full((8, 8, 3), 5, dtype=np.uint8), destination=dark_path)
    render_frame(np.full((8, 8, 3), 250, dtype=np.uint8), destination=bright_path)

    with Image.open(dark_path) as dark, Image.open(bright_path) as bright:
        assert dark.getpixel((0, 0)) == (0, 0, 0)
        assert bright.getpixel((0, 0)) == (255, 255, 255)
