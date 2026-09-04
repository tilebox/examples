from __future__ import annotations

from pathlib import Path

import numpy as np
import xarray as xr
from affine import Affine
from PIL import Image, ImageDraw, ImageFont, ImageOps
from pyproj import CRS
from rasterio.enums import Resampling
from rasterio.transform import from_bounds
from rasterio.warp import reproject
from tilebox.datasets.assets import AssetCollection
from tilebox.storage.aio import Client as StorageClient
from tilebox.storage.geotiff import window_from_bounds

_ASSETS_DIR = Path(__file__).parent.parent / "assets"
_FONT_PATH = _ASSETS_DIR / "Poppins-Medium.ttf"
_LOGO_PATH = _ASSETS_DIR / "tilebox-logo.png"
_TEXT_COLOR = (21, 27, 43, 255)
_FRAME_SIZE = 900


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
    return align_visual(
        chunk.data,
        source_transform=chunk.transform,
        source_crs=chunk.crs,
        source_nodata=chunk.nodata,
        bounds=bounds,
    )


def align_visual(
    data: np.ndarray,
    *,
    source_transform: Affine,
    source_crs: CRS | str,
    source_nodata: float | None,
    bounds: tuple[float, float, float, float],
) -> np.ndarray:
    """Reproject RGB data onto one fixed WGS84 grid so frames stay aligned."""
    destination = np.zeros((3, _FRAME_SIZE, _FRAME_SIZE), dtype=data.dtype)
    reproject(
        data[:3],
        destination,
        src_transform=source_transform,
        src_crs=source_crs,
        src_nodata=source_nodata,
        dst_transform=from_bounds(*bounds, width=_FRAME_SIZE, height=_FRAME_SIZE),
        dst_crs="EPSG:4326",
        resampling=Resampling.bilinear,
    )
    return np.moveaxis(destination, 0, -1)


def render_frame(
    visual: np.ndarray,
    *,
    destination: Path,
    caption: str,
) -> None:
    """Render a branded square RGB frame with a readable caption."""
    image = Image.fromarray(visual)
    size = min(image.width, image.height, _FRAME_SIZE)
    frame = ImageOps.fit(image, (size, size), method=Image.Resampling.LANCZOS)
    frame = frame.point(SENTINEL2_DISPLAY_LUT)

    overlay = Image.new("RGBA", frame.size)
    draw = ImageDraw.Draw(overlay)
    margin = max(4, round(size * 0.027))
    panel_height = max(24, round(size * 0.09))
    panel_bounds = (margin, size - margin - panel_height, size - margin, size - margin)
    corner_radius = max(5, round(panel_height * 0.24))
    shadow_offset = max(1, round(size * 0.004))
    shadow_bounds = (
        panel_bounds[0] + shadow_offset,
        panel_bounds[1] + shadow_offset,
        panel_bounds[2] + shadow_offset,
        panel_bounds[3] + shadow_offset,
    )
    draw.rounded_rectangle(shadow_bounds, radius=corner_radius, fill=(0, 0, 0, 70))
    draw.rounded_rectangle(panel_bounds, radius=corner_radius, fill=(255, 255, 255, 235))

    content_padding = max(4, round(panel_height * 0.2))
    logo_height = panel_height - 2 * content_padding
    with Image.open(_LOGO_PATH) as source_logo:
        logo = source_logo.convert("RGBA")
        logo_width = round(logo.width * logo_height / logo.height)
        logo = logo.resize((logo_width, logo_height), Image.Resampling.LANCZOS)
    logo_x = panel_bounds[0] + content_padding
    logo_y = panel_bounds[1] + (panel_height - logo_height) // 2
    overlay.alpha_composite(logo, (logo_x, logo_y))

    font = ImageFont.truetype(_FONT_PATH, max(10, round(panel_height * 0.34)))
    caption_bounds = draw.textbbox((0, 0), caption, font=font)
    caption_width = caption_bounds[2] - caption_bounds[0]
    caption_height = caption_bounds[3] - caption_bounds[1]
    caption_x = panel_bounds[2] - content_padding - caption_width
    caption_y = panel_bounds[1] + (panel_height - caption_height) // 2 - caption_bounds[1]
    draw.text((caption_x, caption_y), caption, fill=_TEXT_COLOR, font=font)

    frame = Image.alpha_composite(frame.convert("RGBA"), overlay).convert("RGB")

    destination.parent.mkdir(parents=True, exist_ok=True)
    frame.save(destination, format="PNG", optimize=True)


def encode_animated(frame_paths: list[Path], destination: Path, duration_ms: int = 1000) -> None:
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
