import os

from tilebox.workflows import Client, Runner

from sen2cor_workflow.tasks import ProcessArea, ProcessScene

runner = Runner(tasks=[ProcessArea, ProcessScene])

if __name__ == "__main__":
    runner.connect_to(Client(), cluster=os.environ["TILEBOX_CLUSTER"]).run_forever()
