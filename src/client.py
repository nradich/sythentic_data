"""
NVIDIA Nemo Data Designer Client Configuration
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.env import key
from nemo_microservices.data_designer.essentials import NeMoDataDesignerClient

def get_data_designer_client():
    """Initialize and return NeMo Data Designer client with API key from env.py"""
    return NeMoDataDesignerClient(
        base_url="https://ai.api.nvidia.com/v1/nemo/dd",
        default_headers={"Authorization": f"Bearer {key}"}
    )

# Available models - using Nemotron 30B as specified
NEMOTRON_30B_MODEL = "nvidia/nemotron-3-nano-30b-a3b"