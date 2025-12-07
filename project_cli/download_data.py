# making the function executable from command line

from complaint_priority.data.download_dataset import download_data
from loguru import logger

if __name__ == "__main__":
    logger.add("file_{time}.log", rotation = "10 MB", level = "INFO")
    logger.info("Starting data download CLI execution...")
    try:
        download_data()
        logger.success("Data download script executed successfully.")
    except Exception as e:
        logger.error(f"Data download CLI failed: {e}")
        # Exit with a non zero status to signal failure in automation pipeline
        exit(1)