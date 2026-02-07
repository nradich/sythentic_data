"""
Configuration package for synthetic data pipeline
Handles environment variables and Azure Key Vault integration
"""

from .env import NVIDIA_API_KEY, AZURE_CONTAINER_NAME

__all__ = ["NVIDIA_API_KEY", "AZURE_CONTAINER_NAME"]