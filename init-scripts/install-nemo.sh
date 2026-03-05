#!/bin/bash

set -euo pipefail

# Databricks Init Script for NVIDIA Nemo Microservices Installation
# This script installs:
# 1) Microsoft ODBC Driver 18 for SQL Server (required for pyodbc SQL publish)
# 2) nemo-microservices[data-designer] for synthetic data generation

echo "Starting cluster init script..."
echo "Timestamp: $(date)"

echo "Installing Microsoft ODBC Driver 18 for SQL Server..."

if [ -f /etc/debian_version ]; then
    export DEBIAN_FRONTEND=noninteractive

    apt-get update
    apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        apt-transport-https \
        unixodbc \
        unixodbc-dev

    curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /tmp/microsoft.gpg
    install -o root -g root -m 644 /tmp/microsoft.gpg /usr/share/keyrings/microsoft-prod.gpg

    DISTRO_CODENAME="$(. /etc/os-release && echo ${VERSION_CODENAME:-jammy})"
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/${DISTRO_CODENAME}/prod ${DISTRO_CODENAME} main" > /etc/apt/sources.list.d/microsoft-prod.list

    apt-get update
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18

    rm -f /tmp/microsoft.gpg
    rm -rf /var/lib/apt/lists/*
else
    echo "❌ Unsupported OS for automatic ODBC Driver 18 installation."
    exit 1
fi

echo "Verifying ODBC Driver 18 installation..."
if odbcinst -q -d | grep -q "ODBC Driver 18 for SQL Server"; then
    echo "✅ ODBC Driver 18 is installed"
else
    echo "❌ ODBC Driver 18 verification failed"
    exit 1
fi

# Update pip to latest version
pip install --upgrade pip

# Ensure pyodbc is available in cluster Python environment
pip install pyodbc

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