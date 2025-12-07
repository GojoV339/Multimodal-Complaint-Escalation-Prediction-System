#!/bin/bash

echo "--- Running Data Download CLI ---"

export PYTHONPATH=./src:$PYTHONPATH
# This command executes the Python CLI entry point
python project_cli/download_data.py

# Check the exit status of the python script
if [ $? -eq 0 ]; then
    echo "--- Data Download Completed Successfully ---"
else
    echo "--- Data Download Failed. Check logs. ---"
    exit 1
fi