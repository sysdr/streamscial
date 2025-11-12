#!/bin/bash

# Start the connector

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Starting SocialStream Connector..."
echo "==================================="

# Activate virtual environment
source "${ROOT_DIR}/venv/bin/activate"

cd "${ROOT_DIR}"

# Start connector
python -m src.main
