"""
Utility functions for OpenLineage integration.
"""

import os
import socket
import re
import logging
from typing import Optional, Union


def create_namespace(
    project_name: str,
    environment: str,
    prefix: Optional[str] = None,
    include_hostname: bool = True,
) -> str:
    """
    Create a consistent namespace for OpenLineage based on project and environment information.
    
    Args:
        project_name: Name of the project (required)
        environment: Environment name (dev, staging, prod, etc.) (required)
        prefix: Optional prefix for the namespace
        include_hostname: Whether to include the hostname in the namespace
        
    Returns:
        A formatted namespace string
    
    Examples:
        >>> create_namespace("my_project", "dev")
        'my_project.dev.hostname'
        
        >>> create_namespace("data_pipeline", "prod", prefix="company")
        'company.data_pipeline.prod.hostname'
        
        >>> create_namespace("analytics", "staging", include_hostname=False)
        'analytics.staging'
    """
    # Clean up project name (remove special chars, convert to lowercase)
    project_name = re.sub(r'[^a-zA-Z0-9]', '_', project_name).lower()
    
    # Create namespace components
    components = []
    
    # Add prefix if provided
    if prefix:
        components.append(prefix)
    
    # Add project name
    components.append(project_name)
    
    # Add environment
    components.append(environment)
    
    # Add hostname if requested
    if include_hostname:
        hostname = socket.gethostname().split('.')[0]  # Get short hostname
        components.append(hostname)
    
    # Join components with dots
    namespace = '.'.join(components)
    
    return namespace 


def setup_openlineage_logging(
    level: Union[int, str] = logging.DEBUG,
    format_string: Optional[str] = None,
    log_to_file: Optional[str] = None
):
    """
    Configure logging for the OpenLineage client.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Custom format for log messages
        log_to_file: Path to log file (if None, logs to console only)
    
    Example:
        >>> setup_openlineage_logging(level=logging.DEBUG)
        >>> setup_openlineage_logging(level="INFO", log_to_file="/tmp/openlineage.log")
    """
    # Set default format if not provided
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configure the formatter
    formatter = logging.Formatter(format_string)
    
    # Get the OpenLineage loggers
    ol_logger = logging.getLogger('openlineage')
    ol_client_logger = logging.getLogger('openlineage.client')
    
    # Clear any existing handlers to avoid duplicate logs
    for logger in [ol_logger, ol_client_logger]:
        logger.handlers = []
    
    # Set up console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Add file handler if requested
    if log_to_file:
        file_handler = logging.FileHandler(log_to_file)
        file_handler.setFormatter(formatter)
        ol_logger.addHandler(file_handler)
        ol_client_logger.addHandler(file_handler)
    
    # Add the console handler
    ol_logger.addHandler(console_handler)
    ol_client_logger.addHandler(console_handler)
    
    # Set the logging level for OpenLineage loggers
    ol_logger.setLevel(level)
    ol_client_logger.setLevel(level)
    
    # Ensure the loggers propagate to the root logger
    ol_logger.propagate = True
    ol_client_logger.propagate = True
    
    # Also check and use the environment variable if it's set
    env_level = os.environ.get('OPENLINEAGE_CLIENT_LOGGING')
    if env_level:
        env_level = env_level.upper()
        level_mapping = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        if env_level in level_mapping:
            ol_client_logger.setLevel(level_mapping[env_level])
            logging.info(f"Setting OpenLineage client logging level to {env_level} from environment variable")
    
    # Log a test message to confirm logging is working
    ol_client_logger.debug("OpenLineage client logging initialized")
    return ol_logger, ol_client_logger

