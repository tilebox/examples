from typing import TypeAlias

import numpy as np

NSamples: TypeAlias = int
NBands: TypeAlias = int

Samples: TypeAlias = np.ndarray[tuple[NSamples, NBands]]
BandsVector: TypeAlias = np.ndarray[tuple[NBands], np.dtype[np.floating]]
BandsMatrix: TypeAlias = np.ndarray[tuple[NBands, NBands], np.dtype[np.floating]]


def compute_covariance_matrix(data: Samples) -> BandsMatrix:
    """
    Computes the covariance matrix for the given data.

    Args:
        data: Samples array of shape (n_samples, n_bands)

    Returns:
        np.ndarray: Covariance matrix of shape (n_bands, n_bands)
    """
    data = data.astype(float)
    mean_vector = data.mean(axis=0)
    return np.cov(data - mean_vector, rowvar=False)


def compute_squared_deviations_matrix(data: Samples) -> tuple[BandsMatrix, BandsVector]:
    """
    Computes the sum of squared deviations matrix for the given data.

    Args:
        data (np.ndarray): Array of shape (n_samples, n_bands)

    Returns:
        tuple: A tuple containing the squared deviations matrix of shape (n_bands, n_bands) and
            the mean vector of shape (n_bands,)
    """
    data = data.astype(float)
    mean_vector = data.mean(axis=0)
    centered_data = data - mean_vector
    return centered_data.T @ centered_data, mean_vector


def combine_local_statistics(  # noqa: PLR0913
    n_1: int,
    deviations_matrix_1: BandsMatrix,
    mean_vector_1: BandsVector,
    n_2: int,
    deviations_matrix_2: BandsMatrix,
    mean_vector_2: BandsVector,
) -> tuple[int, BandsMatrix, BandsVector]:
    """
    Combines the local statistics from two chunks of data.

    Args:
        n_1: Number of samples in the first chunk
        deviations_matrix_1: Sum of squared deviations matrix for the first chunk
        mean_vector_1: Mean vector for the first chunk
        n_2: Number of samples in the second chunk
        deviations_matrix_2: Sum of squared deviations matrix for the second chunk
        mean_vector_2: Mean vector for the second chunk

    Returns:
        A tuple containing the combined number of samples (int), the combined sum of squared
        deviations matrix (np.ndarray of shape (n_bands, n_bands)), and the combined mean vector
        (np.ndarray of shape (n_bands,))
    """
    n = n_1 + n_2
    sigma = mean_vector_2 - mean_vector_1
    combined_mean_vector = mean_vector_1 + sigma * (n_2 / n)
    combined_deviations_matrix = deviations_matrix_1 + deviations_matrix_2 + (n_1 * n_2 / n) * np.outer(sigma, sigma)
    return n, combined_deviations_matrix, combined_mean_vector


def compute_eigenvectors(covariance_matrix: BandsMatrix) -> tuple[BandsVector, BandsMatrix]:
    """
    Computes the eigenvalues and eigenvectors for the given covariance matrix.

    Args:
        covariance_matrix: Covariance matrix of shape (n_bands, n_bands)

    Returns:
        tuple: A tuple containing eigenvalues (np.ndarray of shape (n_bands,)) and eigenvectors
            (np.ndarray of shape (n_bands, n_bands))
    """
    eigenvalues, eigenvectors = np.linalg.eig(covariance_matrix)
    # sort our eigenvalues and eigenvectors by decreasing eigenvalue
    sorted_by_desc_eigenvalues = np.argsort(eigenvalues)[::-1]
    eigenvalues[sorted_by_desc_eigenvalues]
    eigenvectors = eigenvectors[:, sorted_by_desc_eigenvalues]
    return eigenvalues, eigenvectors
