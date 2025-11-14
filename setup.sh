#!/bin/bash

python3.13 --version >/dev/null 2>&1 && echo "Found Python version 3.13. Continuing" || { echo "Missing python version 3.13"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

if [ -d "venv" ]; then
    echo "found python venv. activating"
    source "venv/bin/activate"
    
else
    echo "Python venv folder missing, creating"
    python3.13 -m venv "venv" || { echo "Failed to create venv. Exiting. Check Permission"; exit 1; }
    echo "Activiating new venv"
    source "venv/bin/activate"
fi

echo "checking all python dependencies"
pip install -r requirements.txt

