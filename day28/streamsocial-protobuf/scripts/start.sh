#!/bin/bash
echo "🚀 Starting StreamSocial Protobuf Dashboard..."
source venv/bin/activate
export PYTHONPATH=$PWD/src:$PYTHONPATH
python src/dashboard.py
