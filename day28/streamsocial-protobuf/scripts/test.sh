#!/bin/bash
echo "🧪 Running tests..."
source venv/bin/activate
export PYTHONPATH=$PWD/src:$PYTHONPATH
python -m pytest tests/ -v
