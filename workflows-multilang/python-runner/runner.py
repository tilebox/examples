from tilebox.workflows import ExecutionContext, Task, Client


class ScheduleImageCapture(Task):
    # The input parameters must match the ones defined in the Go task
    location: tuple[float, float]  # lat_lon
    resolution_m: int
    spectral_bands: list[float]  # spectral bands in nm

    def execute(self, context: ExecutionContext) -> None:
        # Here you can implement your task logic, submit subtasks, etc.
        print(f"Image captured for {self.location} with {self.resolution_m}m resolution and bands {self.spectral_bands}")

    @staticmethod
    def identifier() -> tuple[str, str]:
        # The identifier must match the one defined in the Go task
        return "tilebox.com/schedule_image_capture", "v1.0"


def main():
    client = Client()
    runner = client.runner(
        "test-cluster-tZD9Ca2qsqt4V",
        tasks=[
            ScheduleImageCapture,
        ],
    )
    runner.run_forever()


if __name__ == "__main__":
    main()
