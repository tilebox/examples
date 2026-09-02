from tilebox.workflows import Runner
from tilebox.workflows.cache import ObstoreCache

from s2_clay.tasks import TASKS, output_storage


def workflow_cache() -> ObstoreCache:
    return ObstoreCache(output_storage("cache"), prefix="jobs")


runner = Runner(tasks=TASKS, cache=workflow_cache())
