import os
from pathlib import Path

from cyclopts import App
from dotenv import load_dotenv
from tilebox.workflows import Client as WorkflowsClient

from runner import runner as workflow_runner
from s2_cloudfree_mosaic import BuildMosaic, ComputeMosaicTile

__all__ = ["BuildMosaic", "ComputeMosaicTile"]


app = App()


@app.default
def main(cluster: str | None = None) -> None:
    if Path(".env").exists():
        assert load_dotenv()

    client = WorkflowsClient(name=os.environ.get("RUNNER_NAME", "s2-cloudfree-mosaic"))
    workflow_runner.connect_to(client, cluster=cluster).run_forever()


if __name__ == "__main__":
    app()
