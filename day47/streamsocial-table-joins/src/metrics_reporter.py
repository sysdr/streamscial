"""
Metrics Reporter - Sends processor metrics to dashboard
"""

import requests
import time
import logging
from table_join_processor import TableJoinProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def report_metrics(processor: TableJoinProcessor, dashboard_url: str):
    """Periodically send metrics to dashboard"""
    while True:
        try:
            metrics = processor.get_metrics()
            response = requests.post(
                f"{dashboard_url}/api/update_metrics",
                json=metrics,
                timeout=5
            )
            if response.status_code == 200:
                logger.debug("Metrics reported successfully")
        except Exception as e:
            logger.error(f"Error reporting metrics: {e}")
        
        time.sleep(2)
