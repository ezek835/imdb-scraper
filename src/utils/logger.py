# src/utils/logger.py
"""
Advanced logging configuration with file rotation and formatting
"""

import logging
from pathlib import Path
from typing import Optional
import sys
from logging.handlers import RotatingFileHandler

# Custom log levels
logging.SUCCESS = 100
logging.addLevelName(logging.SUCCESS, 'SUCCESS')

def setup_logger(name: str, level: str = 'INFO', log_dir: Optional[Path] = None):
    """Configure a logger with console and file handlers"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (if log_dir is provided)
    if log_dir:
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / f'{name}.log'
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=5*1024*1024,  # 5MB
            backupCount=3
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Add success method to logger
    def success(self, message, *args, **kwargs):
        if self.isEnabledFor(logging.SUCCESS):
            self._log(logging.SUCCESS, message, args, **kwargs)
    
    logging.Logger.success = success

def get_logger(name: str) -> logging.Logger:
    """Get a configured logger instance"""
    return logging.getLogger(name)