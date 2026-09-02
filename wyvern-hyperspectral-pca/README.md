# Parallel principal component analysis for hyperspectral data

This workflow demonstrates how to perform principal component analysis (PCA) on hyperspectral data in a distributed fashion.

As an example we use [Wyvern Open Data](https://wyvern.space/open-data), which is available publicly in a S3 bucket.

## The Algorithm

This parallel Principal Component Analysis (PCA) algorithm divides every product into native ODC `GeoboxTiles`,
computes local statistics in parallel, and aggregates them through a bounded 16-way reduction tree.

The core idea is based on the following steps:

1.  **Local Statistics Computation**: For each individual chunk of data, the number of samples ($n_i$), the local sum of squared deviations matrix (denoted as $D_i$) and the local mean vector ($\mu_i$) are computed. These statistics capture the variability and central tendency within each chunk.

2.  **Hierarchical Combination of Statistics**: The local statistics from the chunks are then combined iteratively up a tree structure. When combining two sets of statistics (from, say, chunk 1 and chunk 2), combined statistics can be computed as follows:

    Given:
    * $n_1$, $D_1$, $\mu_1$ for chunk 1 (number of samples, sum of squared deviations matrix, mean vector)
    * $n_2$, $D_2$, $\mu_2$ for chunk 2 (number of samples, sum of squared deviations matrix, mean vector)

    The combined statistics are:
    * **Combined Number of Samples**: $n = n_1 + n_2$
    * **Combined Sum of Squared Deviations Matrix**: $D = D_1 + D_2 + \frac{n_1 n_2}{n} (\mu_2 - \mu_1)(\mu_2 - \mu_1)^T$
    * **Combined Mean Vector**: $\mu = \mu_1 + (\mu_2 - \mu_1) \frac{n_2}{n}$

3.  **Eigen-decomposition**: Once all local statistics have been combined to yield a single, global sum of squared deviations matrix and mean vector for the entire dataset, the final step involves computing the eigenvalues and eigenvectors of the global covariance matrix (which can be derived directly from the global sum of squared deviations matrix and the total number of samples).

## Run the workflow

Install dependencies:

```bash
uv sync
```

```bash
tilebox workflow build-release --debug --json
tilebox workflow publish-release --json
tilebox workflow deploy-release --latest --cluster <cluster-slug> --json
tilebox runner start --cluster <cluster-slug>
```

Set `CACHE_BUCKET=gs://<bucket>/<prefix>` on distributed runners so chunk statistics are shared. If it is unset, the
runner uses a local filesystem cache for single-machine development.

The workflow accepts a local Wyvern product path or an S3 URI. Remote products are window-read through Rasterio and
`vsifile`; runners outside the bucket region will be slower.

### Running against local data using multiple cores

Download the wyvern product you want to perform `PCA` on using the [aws cli](https://aws.amazon.com/cli/)

```bash
aws s3 cp s3://wyvern-prod-public-open-data-program/wyvern_dragonette-001_20240703T171837_4c406dd3/wyvern_dragonette-001_20240703T171837_4c406dd3.tiff .
```

Now submit a job to the workflow to compute the `PCA` for the product.

```python
from tilebox.workflows import Client

from hyperspectral_pca import WyvernPCA

client = Client()
client.jobs().submit(
    "wyvern-pca",
    WyvernPCA(
        product_paths=[
            "s3://wyvern-prod-public-open-data-program/wyvern_dragonette-001_20240703T171837_4c406dd3/wyvern_dragonette-001_20240703T171837_4c406dd3.tiff"
        ]
    ),
)
```

The final task stores a compressed NumPy archive under `principal_components` in the job cache. It contains sorted
`eigenvalues` and corresponding `eigenvectors`. Intermediate statistics use the same non-pickle archive format.
