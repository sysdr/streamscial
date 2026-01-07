#!/bin/bash
# Wrapper script to start the dashboard from project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/scripts/start.sh"

