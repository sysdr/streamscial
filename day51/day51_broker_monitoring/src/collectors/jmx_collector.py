import requests
import time
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class BrokerMetrics:
    broker_id: int
    timestamp: datetime
    messages_in_per_sec: float = 0.0
    bytes_in_per_sec: float = 0.0
    bytes_out_per_sec: float = 0.0
    request_handler_idle_percent: float = 100.0
    network_processor_idle_percent: float = 100.0
    under_replicated_partitions: int = 0
    offline_partitions: int = 0
    active_controller_count: int = 0
    leader_count: int = 0
    partition_count: int = 0
    isr_shrinks_per_sec: float = 0.0
    isr_expands_per_sec: float = 0.0
    raw_metrics: Dict = field(default_factory=dict)

class JMXCollector:
    def __init__(self, broker_endpoints: List[str]):
        self.broker_endpoints = broker_endpoints
        self.previous_metrics: Dict[int, Dict] = {}
        
    def collect_from_broker(self, endpoint: str, broker_id: int) -> Optional[BrokerMetrics]:
        """Collect metrics from a single broker's JMX exporter endpoint"""
        try:
            response = requests.get(endpoint, timeout=5)
            if response.status_code != 200:
                logger.error(f"Failed to collect from broker {broker_id}: {response.status_code}")
                return None
            
            metrics_text = response.text
            parsed_metrics = self._parse_prometheus_metrics(metrics_text)
            
            # Calculate rates for counter metrics (fallback)
            current_time = time.time()
            rates = self._calculate_rates(broker_id, parsed_metrics, current_time)
            
            # Try to get OneMinuteRate metrics first (these are already rates)
            messages_in_rate = parsed_metrics.get(
                'kafka_server_brokertopicmetrics_messagesinpersec_oneminuterate',
                parsed_metrics.get('kafka_server_brokertopicmetrics_messagesinpersec', rates.get('messages_in', 0.0))
            )
            bytes_in_rate = parsed_metrics.get(
                'kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate',
                parsed_metrics.get('kafka_server_brokertopicmetrics_bytesinpersec', rates.get('bytes_in', 0.0))
            )
            bytes_out_rate = parsed_metrics.get(
                'kafka_server_brokertopicmetrics_bytesoutpersec_oneminuterate',
                parsed_metrics.get('kafka_server_brokertopicmetrics_bytesoutpersec', rates.get('bytes_out', 0.0))
            )
            
            # Network processor idle is a ratio (0-1), convert to percentage (0-100)
            network_idle_ratio = parsed_metrics.get(
                'kafka_network_socketserver_networkprocessoravgidlepercent', 1.0
            )
            # Convert ratio to percentage if it's less than 1.1 (assuming ratios are < 1.1)
            if network_idle_ratio <= 1.1:
                network_idle_percent = network_idle_ratio * 100.0
            else:
                network_idle_percent = network_idle_ratio
            
            broker_metrics = BrokerMetrics(
                broker_id=broker_id,
                timestamp=datetime.now(),
                messages_in_per_sec=messages_in_rate,
                bytes_in_per_sec=bytes_in_rate,
                bytes_out_per_sec=bytes_out_rate,
                request_handler_idle_percent=parsed_metrics.get(
                    'kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent', 100.0
                ),
                network_processor_idle_percent=network_idle_percent,
                under_replicated_partitions=int(parsed_metrics.get(
                    'kafka_server_replicamanager_underreplicatedpartitions', 0
                )),
                offline_partitions=int(parsed_metrics.get(
                    'kafka_controller_kafkacontroller_offlinepartitionscount', 0
                )),
                active_controller_count=int(parsed_metrics.get(
                    'kafka_controller_kafkacontroller_activecontrollercount', 0
                )),
                leader_count=int(parsed_metrics.get(
                    'kafka_server_replicamanager_leadercount', 0
                )),
                partition_count=int(parsed_metrics.get(
                    'kafka_server_replicamanager_partitioncount', 0
                )),
                isr_shrinks_per_sec=rates.get('isr_shrinks', 0.0),
                isr_expands_per_sec=rates.get('isr_expands', 0.0),
                raw_metrics=parsed_metrics
            )
            
            return broker_metrics
            
        except Exception as e:
            logger.error(f"Error collecting from broker {broker_id}: {str(e)}")
            return None
    
    def _parse_prometheus_metrics(self, text: str) -> Dict[str, float]:
        """Parse Prometheus format metrics"""
        metrics = {}
        for line in text.split('\n'):
            if line and not line.startswith('#'):
                try:
                    parts = line.split()
                    if len(parts) >= 2:
                        metric_name = parts[0]
                        value = float(parts[1])
                        metrics[metric_name] = value
                except Exception:
                    continue
        return metrics
    
    def _calculate_rates(self, broker_id: int, current_metrics: Dict, current_time: float) -> Dict[str, float]:
        """Calculate per-second rates for counter metrics"""
        rates = {}
        
        if broker_id not in self.previous_metrics:
            self.previous_metrics[broker_id] = {
                'metrics': current_metrics,
                'time': current_time
            }
            return rates
        
        prev = self.previous_metrics[broker_id]
        time_delta = current_time - prev['time']
        
        if time_delta <= 0:
            return rates
        
        # Calculate rates for counters
        counter_mappings = {
            'kafka_server_brokertopicmetrics_messagesinpersec': 'messages_in',
            'kafka_server_brokertopicmetrics_bytesinpersec': 'bytes_in',
            'kafka_server_brokertopicmetrics_bytesoutpersec': 'bytes_out',
            'kafka_server_replicamanager_isrshrinkspersec': 'isr_shrinks',
            'kafka_server_replicamanager_isrexpandspersec': 'isr_expands'
        }
        
        for metric_name, rate_name in counter_mappings.items():
            current_val = current_metrics.get(metric_name, 0)
            prev_val = prev['metrics'].get(metric_name, 0)
            rate = (current_val - prev_val) / time_delta
            rates[rate_name] = max(0, rate)  # Ensure non-negative
        
        # Update previous metrics
        self.previous_metrics[broker_id] = {
            'metrics': current_metrics,
            'time': current_time
        }
        
        return rates
    
    def collect_all(self) -> List[BrokerMetrics]:
        """Collect metrics from all brokers"""
        all_metrics = []
        for idx, endpoint in enumerate(self.broker_endpoints, start=1):
            metrics = self.collect_from_broker(endpoint, idx)
            if metrics:
                all_metrics.append(metrics)
        return all_metrics
