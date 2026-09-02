<h1 align="center">
  <img src="https://storage.googleapis.com/tbx-web-assets-2bad228/banners/tilebox-banner.svg" alt="Tilebox Logo">
  <br>
</h1>


<p align="center">
  <a href="https://docs.tilebox.com/introduction"><b>Documentation</b></a>
  |
  <a href="https://console.tilebox.com/"><b>Console</b></a>
  |
  <a href="https://tilebox.com/discord"><b>Discord</b></a>
</p>

# Tilebox Examples

This repository contains examples for using the [Tilebox](https://tilebox.com) SDKs.

List of examples:

**Workflows**

- [Workflows Hello World, Python](/workflows-hello-world-py/)  
A simple example that demonstrates how to use the Tilebox SDKs to submit a job and run a worker.
- [Workflows Cron Automation, Python](/workflows-cron-automation-py/)  
A release-based async Cron Automation that queries Sentinel-2 statistics for native Shapely areas.
- [Multi-Language Workflows, Python and Go](/workflows-multilang/)
An example that submits native ODC Geo task inputs from Go for execution by an async Python task.

**Mixed**

- [Download S2 Data for Points of Interest, Python](/workflows-download-s2-for-aois/)  
A more complex example that demonstrates how to use the Tilebox SDKs to create a Workflows to find Sentinel-2 data for a set of points of interest (POIs), filter the data to be as cloud-free as possible and finally download the data.
- [Sentinel-2 Cloud-free Mosaic](/s2-cloudfree-mosaic/)
This workflow window-reads Sentinel-2 COGs from AWS Earth Search and writes a cloud-free mosaic to Zarr.
- [Sentinel-2 Clay Change Detection](/s2-clay/)
This workflow composites two Sentinel-2 periods, runs tiled Clay inference, and writes embedding change distances.
- [Wyvern Hyperspectral PCA](/wyvern-hyperspectral-pca/)
This workflow computes distributed PCA with ODC geospatial tiling and a bounded statistics reduction tree.
