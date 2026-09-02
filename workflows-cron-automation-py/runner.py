from tilebox.workflows import Runner

from s2_stats.tasks import TASKS

runner = Runner(tasks=TASKS)
