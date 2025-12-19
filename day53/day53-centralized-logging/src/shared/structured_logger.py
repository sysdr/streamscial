"""Structured logging configuration for StreamSocial"""
import structlog
import logging
import json
import sys
from datetime import datetime
from pathlib import Path

def setup_structured_logger(component_name: str, log_file: str = None):
    """Setup structured JSON logging"""
    
    # Configure structlog
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Setup standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO,
    )
    
    # Add file handler if specified
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter('%(message)s'))
        logging.root.addHandler(file_handler)
    
    logger = structlog.get_logger(component_name)
    return logger

def generate_trace_id():
    """Generate unique trace ID for request correlation"""
    from uuid import uuid4
    return f"trace-{uuid4().hex[:12]}"

class LogContext:
    """Context manager for trace ID propagation"""
    def __init__(self, logger, trace_id=None, **kwargs):
        self.logger = logger
        self.trace_id = trace_id or generate_trace_id()
        self.context = kwargs
        self.context['trace_id'] = self.trace_id
    
    def __enter__(self):
        return self.logger.bind(**self.context)
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
