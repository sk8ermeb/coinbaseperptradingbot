#!/bin/bash

if python3.13 --version >/dev/null 2>&1; then
    PY_CMD="python3.13"
    echo "Found Python version 3.13. Continuing"
elif python --version 2>&1 | grep -q "3\.13"; then
    PY_CMD="python"
    echo "Found Python version 3.13. Continuing"
else
    echo "Missing python version 3.13"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

if [ -d "venv" ]; then
    echo "found python venv. activating"
    source "venv/bin/activate"
    
else
    echo "Python venv folder missing, creating"
    $PY_CMD -m venv "venv" || { echo "Failed to create venv. Exiting. Check Permission"; exit 1; }
    echo "Activiating new venv"
    source "venv/bin/activate"
fi

echo "Checking all python dependencies."

pip install -r requirements.txt
echo "Running trading bot."

python server/run.py
