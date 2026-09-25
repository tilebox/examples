"""Run a scene worker: uv run runner.py."""

import os

from cyclopts import run
from dotenv import load_dotenv
from tilebox.workflows import Client, Runner

from atmospheric_correction.tasks import ProcessArea, ProcessScene

runner = Runner(tasks=[ProcessArea, ProcessScene])


def main() -> None:
    """Listen for scene tasks on TILEBOX_CLUSTER; start more workers for concurrency.

    Example: uv run runner.py
    """
    runner.connect_to(Client(), cluster=os.environ.get("TILEBOX_CLUSTER")).run_forever()


if __name__ == "__main__":
    load_dotenv()
    run(main)
