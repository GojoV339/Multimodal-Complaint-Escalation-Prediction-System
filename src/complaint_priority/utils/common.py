# The utility handles reading the configuration files 

import yaml
import os 
from loguru import logger
from pathlib import Path


def read_yaml(path_to_yaml: Path) -> dict:
    """
    Reads a yaml file and returns its content
    
    Args:
        path_to_yaml : Path to the yaml file 
    Return: 
        contnet of the yaml file as a dictionary
    
    """
    try:
        with open(path_to_yaml, 'r') as yaml_file:
            content = yaml.safe_load(yaml_file)
            logger.info(f"Successfully loaded YAML file from : {path_to_yaml}")
            return content
    except Exception as e:
        logger.error(f"Error reading YAML file: {path_to_yaml}: {e}")
        raise e
    
    
def create_directories(path_to_directories: list, verbose=True):
    """Creates list of directories.

    Args:
        path_to_directories: list of path to directories
        verbose: whether to log the creation (default: True)
    """
    for path in path_to_directories:
        os.makedirs(path, exist_ok=True)
        if verbose:
            logger.info(f"Created directory at: {path}")