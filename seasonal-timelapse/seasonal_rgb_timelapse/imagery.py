from __future__ import annotations

from pathlib import Path

import numpy as np
import xarray as xr
from PIL import Image, ImageOps
from tilebox.datasets.assets import AssetCollection
from tilebox.storage.aio import Client as StorageClient
from tilebox.storage.geotiff import window_from_bounds


def lut(gamma: float, black_point: int, white_point: int) -> list[int]:
    """Build an RGB display LUT with fixed levels and gamma correction.

    Values at or below the black point become black, values at or above the
    white point become white, and values between them follow the gamma curve.
    """
    values = np.arange(256)
    normalized = np.clip(
        (values - black_point) / (white_point - black_point),
        0,
        1,
    )
    channel = np.round(255 * normalized ** (1 / gamma)).astype(np.uint8).tolist()
    return channel * 3


SENTINEL2_DISPLAY_LUT = lut(1.05, 5, 250)


async def read_visual(datapoint: xr.Dataset, bounds: tuple[float, float, float, float]) -> np.ndarray:
    """Read Sentinel-2 visual data for WGS84 bounds."""
    assets = AssetCollection.from_datapoint(datapoint)
    storage = StorageClient()
    geotiff = await storage.open_geotiff(assets["visual"])
    window = window_from_bounds(
        geotiff,
        bounds=bounds,
        crs="EPSG:4326",
        require_fully_contained=True,
    )
    chunk = await geotiff.read(window=window)
    return np.moveaxis(chunk.data[:3], 0, -1)


def render_frame(
    visual: np.ndarray,
    *,
    destination: Path,
) -> None:
    """Render an unlabelled square RGB frame."""
    image = Image.fromarray(visual)
    size = min(image.width, image.height, 900)
    frame = ImageOps.fit(image, (size, size), method=Image.Resampling.LANCZOS)
    frame = frame.point(SENTINEL2_DISPLAY_LUT)

    destination.parent.mkdir(parents=True, exist_ok=True)
    frame.save(destination, format="PNG", optimize=True)


def encode_animated(frame_paths: list[Path], destination: Path, duration_ms: int = 500) -> None:
    """Encode equally sized image frames as a looping animated WebP."""
    if not frame_paths:
        raise ValueError("at least one frame is required")
    frames = [Image.open(path).convert("RGB") for path in frame_paths]
    try:
        size = frames[0].size
        if any(frame.size != size for frame in frames):
            raise ValueError("all frames must have identical dimensions")

        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(".tmp.webp")
        frames[0].save(
            temporary,
            format="WEBP",
            save_all=True,
            append_images=frames[1:],
            duration=duration_ms,
            loop=0,
            quality=90,
            method=6,
        )
        temporary.replace(destination)
    finally:
        for frame in frames:
            frame.close()
