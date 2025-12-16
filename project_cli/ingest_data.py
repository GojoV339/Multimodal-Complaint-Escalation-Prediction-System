from complaint_priority.data.data_ingestion import run_data_ingestion
from loguru import logger
import sys

LOG_FILE = "logs/ingest_data_pipelinne_{time}.log"
if __name__ == '__main__':
    logger.add(sys.stderr, level = "INFO")
    logger.add(
        LOG_FILE,
        rotation="10 MB",
        level = "INFO",
        catch = True,
        enqueue=True
    )
    
    logger.info("Starting Bronze to Silver Data Ingestion CLI Execution...")
    try:
        run_data_ingestion()
        logger.success("Data Ingestion Pipeline (Bronze -> Silver) executed successfully.")
    except Exception as e:
        logger.error(f"Data Ingestion CLI failed: {e}")
        sys.exit(1)
        