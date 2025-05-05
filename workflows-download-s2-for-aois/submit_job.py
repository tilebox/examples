import os

import dotenv
from tilebox.workflows import Client
from tilebox.workflows.observability.logging import get_logger

from download_workflow import DownloadWorkflow

dotenv.load_dotenv()
logger = get_logger()

if os.getenv("TILEBOX_API_KEY") is None:
    raise ValueError("TILEBOX_API_KEY environment variable is not set")
cluster = os.getenv("TILEBOX_CLUSTER")
if cluster is None:
    raise ValueError("TILEBOX_CLUSTER environment variable is not set")

jc = Client().jobs()
job = jc.submit(
    "download-s2",
    DownloadWorkflow(
        start="2022-01-01",
        end="2022-12-31",
        coords_path="coords.csv",
        download=False,
    ),
    cluster=cluster,
    max_retries=3,
)
logger.info(f"Job submitted with ID {job.id}")
