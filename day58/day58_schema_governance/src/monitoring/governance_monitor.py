"""
Schema Governance Monitor - Real-time tracking of schema evolution
Monitors adoption, compatibility, and health metrics
"""

import time
import psutil
from typing import Dict, List
from src.registry.schema_manager import registry_client


class GovernanceMonitor:
    """
    Monitors schema governance metrics
    Patterns from Netflix and LinkedIn monitoring
    """
    
    def __init__(self):
        self.start_time = time.time()
        self.metrics_history = []
    
    def collect_registry_metrics(self) -> Dict:
        """Collect schema registry metrics"""
        registry_metrics = registry_client.get_metrics()
        
        # Schema version distribution
        version_distribution = {}
        for subject, versions in registry_client.schemas.items():
            version_distribution[subject] = len(versions)
        
        # Schema states distribution
        state_counts = {}
        for subject, versions in registry_client.schemas.items():
            for version_data in versions:
                state = version_data.get('state', 'UNKNOWN')
                state_counts[state] = state_counts.get(state, 0) + 1
        
        return {
            'registry': registry_metrics,
            'version_distribution': version_distribution,
            'state_distribution': state_counts,
            'timestamp': time.time()
        }
    
    def collect_producer_metrics(self, producer) -> Dict:
        """Collect producer metrics"""
        metrics = producer.get_metrics()
        
        total = metrics['total_sent']
        if total == 0:
            return {'adoption': {}, 'total': 0}
        
        # Calculate adoption percentages
        adoption = {
            version: {
                'count': count,
                'percentage': (count / total) * 100
            }
            for version, count in metrics['sent_by_version'].items()
        }
        
        return {
            'adoption': adoption,
            'total_sent': total,
            'errors': metrics['errors']
        }
    
    def collect_consumer_metrics(self, consumer) -> Dict:
        """Collect consumer metrics"""
        metrics = consumer.get_metrics()
        
        total = metrics['total_consumed']
        if total == 0:
            return {'consumption': {}, 'total': 0}
        
        # Calculate consumption distribution
        consumption = {
            version: {
                'count': count,
                'percentage': (count / total) * 100
            }
            for version, count in metrics['consumed_by_version'].items()
        }
        
        return {
            'consumption': consumption,
            'total_consumed': total,
            'compatibility_errors': metrics['compatibility_errors'],
            'avg_processing_ms': sum(metrics['processing_times']) / max(len(metrics['processing_times']), 1) if metrics['processing_times'] else 0
        }
    
    def collect_system_metrics(self) -> Dict:
        """Collect system resource metrics"""
        return {
            'cpu_percent': psutil.cpu_percent(interval=0.1),
            'memory_percent': psutil.virtual_memory().percent,
            'uptime_seconds': time.time() - self.start_time
        }
    
    def generate_health_report(self, producer, consumer) -> Dict:
        """Generate comprehensive health report"""
        registry_metrics = self.collect_registry_metrics()
        producer_metrics = self.collect_producer_metrics(producer)
        consumer_metrics = self.collect_consumer_metrics(consumer)
        system_metrics = self.collect_system_metrics()
        
        # Calculate health score
        health_score = self._calculate_health_score(
            registry_metrics,
            producer_metrics,
            consumer_metrics
        )
        
        report = {
            'timestamp': time.time(),
            'health_score': health_score,
            'registry': registry_metrics,
            'producer': producer_metrics,
            'consumer': consumer_metrics,
            'system': system_metrics
        }
        
        self.metrics_history.append(report)
        
        return report
    
    def _calculate_health_score(self, registry, producer, consumer) -> int:
        """
        Calculate health score (0-100)
        Based on compatibility errors, adoption rate, and system health
        """
        score = 100
        
        # Penalize compatibility errors
        if registry['registry']['failures'] > 0:
            score -= min(20, registry['registry']['failures'] * 5)
        
        # Penalize consumer errors
        if consumer['compatibility_errors'] > 0:
            score -= min(15, consumer['compatibility_errors'] * 5)
        
        # Penalize slow migrations (if 90%+ still on old version)
        if producer.get('adoption', {}).get('v1', {}).get('percentage', 0) > 90:
            score -= 10
        
        return max(0, score)
    
    def print_report(self, report: Dict):
        """Print formatted health report"""
        print("\n" + "="*60)
        print("SCHEMA GOVERNANCE HEALTH REPORT")
        print("="*60)
        
        print(f"\n🏥 Health Score: {report['health_score']}/100")
        
        print(f"\n📊 Registry Status:")
        reg = report['registry']['registry']
        print(f"  - Total Schemas: {reg['total_schemas']}")
        print(f"  - Subjects: {reg['total_subjects']}")
        print(f"  - Registrations: {reg['registrations']}")
        print(f"  - Compatibility Checks: {reg['compatibility_checks']}")
        print(f"  - Failures: {reg['failures']}")
        
        print(f"\n📤 Producer Metrics:")
        prod = report['producer']
        if 'adoption' in prod:
            for version, data in prod['adoption'].items():
                print(f"  - {version}: {data['count']} events ({data['percentage']:.1f}%)")
        print(f"  - Errors: {prod.get('errors', 0)}")
        
        print(f"\n📥 Consumer Metrics:")
        cons = report['consumer']
        if 'consumption' in cons:
            for version, data in cons['consumption'].items():
                if data['count'] > 0:
                    print(f"  - {version}: {data['count']} events ({data['percentage']:.1f}%)")
        print(f"  - Compatibility Errors: {cons.get('compatibility_errors', 0)}")
        print(f"  - Avg Processing: {cons.get('avg_processing_ms', 0):.2f}ms")
        
        print(f"\n💻 System Resources:")
        sys = report['system']
        print(f"  - CPU: {sys['cpu_percent']:.1f}%")
        print(f"  - Memory: {sys['memory_percent']:.1f}%")
        print(f"  - Uptime: {sys['uptime_seconds']:.1f}s")
        
        print("\n" + "="*60)


# Create monitor instance
governance_monitor = GovernanceMonitor()
