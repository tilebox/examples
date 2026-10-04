"""Expose the scene and storage-event tasks to the workflow release runner."""

from tilebox.workflows import Runner

from atmospheric_correction.automations import CalculateNDVI, CorrectAndUpload
from atmospheric_correction.tasks import ProcessArea, ProcessScene

runner = Runner(tasks=[ProcessArea, ProcessScene, CorrectAndUpload, CalculateNDVI])
