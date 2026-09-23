# %% [markdown]
# # Query corrected Sentinel-2 imagery
#
# Run `uv run --group notebook jupytext --to notebook query_results.py`, then open
# `query_results.ipynb` in Jupyter. The workflow already computed and stored NDVI.
# This notebook queries the catalog, reads a result from local disk or private Azure storage,
# and displays NDVI and the RGB thumbnail. No signed URLs or credentials are cataloged.
#
# Set TILEBOX_API_KEY and RESULTS_DATASET. Local results need no Azure credentials.
# For Azure results run `az login`; your identity needs Storage Blob Data Reader.

# %%
import os
from pathlib import Path

import matplotlib.pyplot as plt
import rasterio
from IPython.display import Image, display
from shapely.geometry import box
from tilebox.datasets import Client, field

from sen2cor_workflow.processing import PIPELINE_VERSION
from sen2cor_workflow.results import download_asset

collection = Client().dataset(os.environ["RESULTS_DATASET"]).collection("L2A")
scenes = collection.query(
    temporal_extent=("2025-08-01", "2025-09-01"),
    spatial_extent=box(54.2, 24.2, 54.6, 24.6),
    filter=field("pipeline_version") == PIPELINE_VERSION,
)
display(scenes)

# %%
if scenes.sizes.get("time", 0) == 0:
    raise ValueError("No results. Match the dates and area to the job you submitted.")
scene = scenes.sortby("time").isel(time=0)
output_dir = Path("outputs") / str(scene.id.item())
output_dir.mkdir(parents=True, exist_ok=True)
for field_name, filename in [("ndvi_url", "ndvi.tif"), ("thumbnail_url", "thumbnail.png")]:
    download_asset(str(scene[field_name].item()), output_dir / filename)
display(Image(filename=str(output_dir / "thumbnail.png")))

# %%
with rasterio.open(output_dir / "ndvi.tif") as source:
    # Read an overview for display, rather than a full 10980 × 10980 tile.
    values = source.read(1, out_shape=(800, 800), masked=True)
fig, ax = plt.subplots(figsize=(8, 7))
image = ax.imshow(values, cmap="RdYlGn", vmin=-1, vmax=1)
ax.set_title(str(scene.title.item()))
ax.set_axis_off()
fig.colorbar(image, ax=ax, label="NDVI (B08 − B04) / (B08 + B04)")
plt.show()
