#!/bin/bash

# Databricks Init Script for NVIDIA Nemo Microservices Installation
# This script installs nemo-microservices[data-designer] for synthetic data generation

echo "Starting NVIDIA Nemo Microservices installation..."
echo "Timestamp: $(date)"

# Update pip to latest version
pip install --upgrade pip

# Install nemo-microservices with data-designer components
echo "Installing nemo-microservices[data-designer]..."
pip install nemo-microservices[data-designer]

# Verify installation
python -c "
try:
    import nemo_microservices
    from nemo_microservices.data_designer.essentials import NeMoDataDesignerClient
    print('✅ NVIDIA Nemo Microservices successfully installed')
    print(f'Version: {nemo_microservices.__version__}')
except ImportError as e:
    print('❌ Installation failed: {e}')
    exit(1)
"

echo "Init script completed at: $(date)"