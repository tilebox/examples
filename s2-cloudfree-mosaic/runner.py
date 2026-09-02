from tilebox.workflows import Runner

from s2_cloudfree_mosaic.tasks import TASKS, workflow_cache

runner = Runner(tasks=TASKS, cache=workflow_cache())
