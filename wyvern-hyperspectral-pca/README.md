# Parallel principal component analysis for hyperspectral data

This workflow demonstrates how to perform principal component analysis (PCA) on hyperspectral data in a distributed fashion.

As an example we use [Wyvern Open Data](https://wyvern.space/open-data), which is available publicly in a S3 bucket.

## The Algorithm

This parallel Principal Component Analysis (PCA) algorithm leverages an incremental approach, where the dataset is divided into smaller chunks, and statistics from these chunks are aggregated in a tree-like structure. This allows for efficient parallel computation of the overall PCA.

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

## Starting task runners

Install dependencies:

```bash
uv sync -U
```

Start a runner:

> [!TIP]  
> We recommend using [call-in-parallel](https://github.com/tilebox/call-in-parallel) to easily start multiple runners in parallel.

```bash
call-in-parallel -n 4 -- uv run wyvern_pca.py
```


The workflow accepts a path to a wyvern product that is either already downloaded locally, or a URI to the full product
in the S3 bucket. In the latter case, the workflow will utilize [https://filesystem-spec.readthedocs.io/en/latest/](fsspec) to access the data directly from S3. However, unless the runners are co-located in the same region as the S3 bucket (e.g. the runners are `EC2` instances), this will be pretty slow.

### Running against local data using multiple cores

Download the wyvern product you want to perform `PCA` on using the [aws cli](https://aws.amazon.com/cli/)

```bash
aws s3 cp s3://wyvern-prod-public-open-data-program/wyvern_dragonette-001_20240703T171837_4c406dd3/wyvern_dragonette-001_20240703T171837_4c406dd3.tiff .
```

Now start as many runners as you want to utilize to perform `PCA` on the data in parallel.

> [!TIP]  
> We recommend using [call-in-parallel](https://github.com/tilebox/call-in-parallel) to easily start multiple runners in parallel.

```bash
call-in-parallel -n 4 -- uv run wyvern_pca.py
```

Now submit a job to the workflow to compute the `PCA` for the product.
