import os

import dotenv
from tilebox.workflows import Client

from download_workflow import DownloadWorkflow

dotenv.load_dotenv()

if os.getenv("TILEBOX_API_KEY") is None:
    raise ValueError("TILEBOX_API_KEY environment variable is not set")

jc = Client(name="download-s2-for-aois-submit").jobs()
job = jc.submit(
    "download-s2",
    DownloadWorkflow(
        start="2022-01-01",
        end="2022-12-31",
        coords_path="coords.csv",
        download=False,
    ),
    max_retries=3,
)
print(f"Job submitted with ID {job.id}")
