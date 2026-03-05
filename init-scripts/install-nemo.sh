#!/bin/bash

set -euo pipefail

echo "Starting cluster init script at: $(date)"

# Require Debian-based OS
if [ ! -f /etc/debian_version ]; then
    echo "❌ Unsupported OS — requires a Debian-based system."
    exit 1
fi

export DEBIAN_FRONTEND=noninteractive

# Install ODBC prerequisites
apt-get update -qq
apt-get install -y --no-install-recommends curl gnupg apt-transport-https unixodbc unixodbc-dev

# Detect OS codename — fail loudly if missing
DISTRO_CODENAME=$(. /etc/os-release && echo "${VERSION_CODENAME}")
if [ -z "$DISTRO_CODENAME" ]; then
    echo "❌ Could not determine OS codename from /etc/os-release"
    exit 1
fi
echo "Detected OS codename: ${DISTRO_CODENAME}"

# Add Microsoft repo and install ODBC Driver 18
curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg

echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
https://packages.microsoft.com/ubuntu/${DISTRO_CODENAME}/prod ${DISTRO_CODENAME} main" \
    > /etc/apt/sources.list.d/microsoft-prod.list

apt-get update -qq
ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18
rm -rf /var/lib/apt/lists/*

if ! odbcinst -q -d | grep -q "ODBC Driver 18 for SQL Server"; then
    echo "❌ ODBC Driver 18 verification failed"
    exit 1
fi
echo "✅ ODBC Driver 18 installed"

# Install Python packages
pip install --upgrade --quiet pip
pip install --quiet --no-cache-dir pyodbc "nemo-microservices[data-designer]"

# Verify Python installation
python - <<'EOF'
import sys
try:
    import nemo_microservices
    from nemo_microservices.data_designer.essentials import NeMoDataDesignerClient
    print(f"✅ nemo-microservices {nemo_microservices.__version__} installed successfully")
except ImportError as e:
    print(f"❌ Verification failed: {e}")
    sys.exit(1)
EOF

echo "Init script completed at: $(date)"