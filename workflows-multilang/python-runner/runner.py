from datetime import datetime
from tilebox.workflows import ExecutionContext, Task, Client


class TaskingWorkflow(Task):
    # The input parameters must match the ones defined in the Go task
    city: str
    time: datetime
    image_resolution: str

    def execute(self, context: ExecutionContext) -> None:
        # Here you can implement your task logic, submit subtasks, etc.
        print(f"Tasking workflow executed for {self.city} at {self.time} with resolution {self.image_resolution}")

    @staticmethod
    def identifier() -> tuple[str, str]:
        # The identifier must match the one defined in the Go task
        return "tilebox.com/tasking_workflow", "v1.0"


def main():
    client = Client()
    runner = client.runner(
        "test-cluster-tZD9Ca2qsqt4V",
        tasks=[
            TaskingWorkflow,
        ],
    )
    runner.run_forever()


if __name__ == "__main__":
    main()
