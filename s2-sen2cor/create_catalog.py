import argparse

from sen2cor_workflow.catalog import create_catalog

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create the custom results dataset and L2A collection")
    parser.add_argument("code_name", nargs="?", default="sen2cor_outputs")
    args = parser.parse_args()
    print(create_catalog(args.code_name))
