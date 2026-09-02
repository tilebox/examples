import os

from google.cloud.storage import Client as StorageClient
from tilebox.workflows import Runner
from tilebox.workflows.cache import GoogleStorageCache, LocalFileSystemCache

from hyperspectral_pca.tasks import TASKS


def workflow_cache() -> GoogleStorageCache | LocalFileSystemCache:
    cache_bucket = os.environ.get("CACHE_BUCKET")
    if cache_bucket is None:
        return LocalFileSystemCache()
    if not cache_bucket.startswith("gs://"):
        raise ValueError("CACHE_BUCKET must be a gs:// URL")

    bucket, _, prefix = cache_bucket.removeprefix("gs://").partition("/")
    return GoogleStorageCache(StorageClient().bucket(bucket), prefix=prefix)


runner = Runner(tasks=TASKS, cache=workflow_cache())
