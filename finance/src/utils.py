"""
Module for utility functions
"""
import logging


def custom_logger(logger_name: str, log_level: int = logging.WARNING):
    """Creates a custom logger.

    Args:
        logger_name (str): Name of the logger.
        log_level (int): Log level.

    Returns:
        logging.Logger: A custom logger.
    """
    logger = logging.getLogger(logger_name)

    return logger
