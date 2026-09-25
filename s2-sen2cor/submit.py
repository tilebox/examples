import argparse
import os

from tilebox.workflows import Client

from sen2cor_workflow.tasks import ProcessArea

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit a bounded Sentinel-2 L1C processing job")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True, help="Exclusive interval end")
    parser.add_argument("--bounds", nargs=4, type=float, default=[54.2, 24.2, 54.6, 24.6])
    parser.add_argument("--collection", default="S2A_S2MSI1C")
    parser.add_argument("--max-scenes", type=int, default=1)
    args = parser.parse_args()
    job = (
        Client()
        .jobs()
        .submit(
            "sentinel-2-sen2cor",
            ProcessArea(
                start=args.start,
                end=args.end,
                bounds=tuple(args.bounds),
                source_collection=args.collection,
                max_scenes=args.max_scenes,
            ),
            cluster=os.environ["TILEBOX_CLUSTER"],
        )
    )
    print(job)  # noqa: T201 - CLI output
