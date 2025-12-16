#!/bin/bash

echo "--- Running Data Ingestion CLI (Bronze -> Silver) ---"

# Crucial: Ensure PYTHONPATH includes the src directory
export PYTHONPATH=./src:$PYTHONPATH

# Create the logs directory if it doesn't exist
mkdir -p logs

python project_cli/ingest_data.py

# Check the exit status of the python script
if [ $? -eq 0 ]; then
    echo "--- Data Ingestion Completed Successfully ---"
else
    echo "--- Data Ingestion Failed. Check logs/ingest_data_pipeline_*.log ---"
    exit 1
fi