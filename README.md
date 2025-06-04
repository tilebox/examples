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
A simple example that demonstrates how to use the Tilebox SDKs to create a Workflows Cron Automation, a workflow that runs on a schedule, and showcases how to filter timeseries Datasets based on spatial and temporal criteria.
- [Multi-Language Workflows, Python and Go](/workflows-multilang/)
An example that demonstrates how to use the Tilebox SDKs to create a Workflows that uses tasks implemented in different languages, Python and Go. Go is used to submit the job, while Python is used to execute it.

**Mixed**

- [Download S2 Data for Points of Interest, Python](/workflows-download-s2-for-aois/)  
A more complex example that demonstrates how to use the Tilebox SDKs to create a Workflows to find Sentinel-2 data for a set of points of interest (POIs), filter the data to be as cloud-free as possible and finally download the data.
- [Sentinel-2 Cloud-free Mosaic](/s2-cloudfree-mosaic/)
This workflow reads Sentinel-2 data from the Copernicus archive, and writes a cloudfree mosaic to a Zarr datacube.
