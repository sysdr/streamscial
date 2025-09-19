"""
Chaos Engineering Tool for StreamSocial Kafka Cluster
Simulates various failure scenarios for testing disaster recovery
"""
import docker
import time
import random
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class FailureScenario:
    name: str
    description: str
    duration_seconds: int
    target_services: List[str]
    recovery_action: str

class ChaosEngineer:
    def __init__(self, compose_file: str = "docker/docker-compose.yml"):
        self.compose_file = compose_file
        self.docker_client = docker.from_env()
        self.active_failures = {}
        
        # Define failure scenarios
        self.scenarios = {
            "single_broker_failure": FailureScenario(
                name="Single Broker Failure",
                description="Simulate single Kafka broker going down",
                duration_seconds=60,
                target_services=["broker-us-1"],
                recovery_action="restart"
            ),
            "leader_broker_failure": FailureScenario(
                name="Leader Broker Failure", 
                description="Kill the broker that leads most partitions",
                duration_seconds=45,
                target_services=["broker-us-1"],  # Usually the first broker leads
                recovery_action="restart"
            ),
            "network_partition": FailureScenario(
                name="Network Partition",
                description="Isolate EU region from US region", 
                duration_seconds=90,
                target_services=["broker-eu-1", "broker-eu-2"],
                recovery_action="reconnect"
            ),
            "cascading_failure": FailureScenario(
                name="Cascading Failure",
                description="Multiple broker failures in sequence",
                duration_seconds=120,
                target_services=["broker-us-1", "broker-us-2", "broker-eu-1"],
                recovery_action="restart_all"
            ),
            "zk_failure": FailureScenario(
                name="Zookeeper Failure",
                description="Zookeeper becomes unavailable",
                duration_seconds=30,
                target_services=["zk1"],
                recovery_action="restart"
            )
        }

    def get_container_by_service(self, service_name: str):
        """Find container by docker-compose service name"""
        try:
            containers = self.docker_client.containers.list()
            for container in containers:
                # Check if container name contains the service name
                if service_name in container.name:
                    return container
            logger.warning(f"⚠️  Container for service {service_name} not found")
            return None
        except Exception as e:
            logger.error(f"❌ Error finding container {service_name}: {e}")
            return None

    def stop_container(self, service_name: str) -> bool:
        """Stop a specific container"""
        try:
            container = self.get_container_by_service(service_name)
            if container:
                container.stop()
                logger.warning(f"🔴 Stopped {service_name} ({container.name})")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Failed to stop {service_name}: {e}")
            return False

    def start_container(self, service_name: str) -> bool:
        """Start a specific container"""
        try:
            container = self.get_container_by_service(service_name)
            if container:
                container.start()
                logger.info(f"🟢 Started {service_name} ({container.name})")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Failed to start {service_name}: {e}")
            return False

    def pause_container(self, service_name: str) -> bool:
        """Pause a container (simulate network partition)"""
        try:
            container = self.get_container_by_service(service_name)
            if container:
                container.pause()
                logger.warning(f"⏸️  Paused {service_name} ({container.name})")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Failed to pause {service_name}: {e}")
            return False

    def unpause_container(self, service_name: str) -> bool:
        """Unpause a container"""
        try:
            container = self.get_container_by_service(service_name)
            if container:
                container.unpause()
                logger.info(f"▶️  Unpaused {service_name} ({container.name})")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Failed to unpause {service_name}: {e}")
            return False

    def inject_failure(self, scenario_name: str) -> str:
        """Inject a specific failure scenario"""
        if scenario_name not in self.scenarios:
            raise ValueError(f"Unknown scenario: {scenario_name}")
        
        scenario = self.scenarios[scenario_name]
        failure_id = f"{scenario_name}_{int(time.time())}"
        
        logger.warning(f"💣 Injecting failure: {scenario.name}")
        logger.info(f"📄 Description: {scenario.description}")
        logger.info(f"⏱️  Duration: {scenario.duration_seconds}s")
        
        # Execute failure based on scenario
        affected_services = []
        
        if scenario_name == "single_broker_failure":
            for service in scenario.target_services:
                if self.stop_container(service):
                    affected_services.append(service)
        
        elif scenario_name == "network_partition":
            for service in scenario.target_services:
                if self.pause_container(service):
                    affected_services.append(service)
        
        elif scenario_name == "cascading_failure":
            for i, service in enumerate(scenario.target_services):
                # Stagger the failures
                if i > 0:
                    time.sleep(10)
                if self.stop_container(service):
                    affected_services.append(service)
        
        else:  # Default: stop containers
            for service in scenario.target_services:
                if self.stop_container(service):
                    affected_services.append(service)
        
        # Schedule recovery
        recovery_thread = threading.Thread(
            target=self._schedule_recovery,
            args=(failure_id, scenario, affected_services),
            daemon=True
        )
        recovery_thread.start()
        
        self.active_failures[failure_id] = {
            'scenario': scenario,
            'affected_services': affected_services,
            'start_time': time.time(),
            'status': 'active'
        }
        
        return failure_id

    def _schedule_recovery(self, failure_id: str, scenario: FailureScenario, affected_services: List[str]):
        """Schedule automatic recovery after scenario duration"""
        time.sleep(scenario.duration_seconds)
        
        logger.info(f"🔄 Starting recovery for: {scenario.name}")
        
        # Recover based on scenario type
        if scenario.recovery_action == "restart":
            for service in affected_services:
                self.start_container(service)
        
        elif scenario.recovery_action == "reconnect":
            for service in affected_services:
                self.unpause_container(service)
        
        elif scenario.recovery_action == "restart_all":
            for service in affected_services:
                self.start_container(service)
        
        # Mark as recovered
        if failure_id in self.active_failures:
            self.active_failures[failure_id]['status'] = 'recovered'
            self.active_failures[failure_id]['recovery_time'] = time.time()
        
        logger.info(f"✅ Recovery completed for: {scenario.name}")

    def manual_recover(self, failure_id: str) -> bool:
        """Manually trigger recovery for a failure"""
        if failure_id not in self.active_failures:
            return False
        
        failure = self.active_failures[failure_id]
        if failure['status'] != 'active':
            return False
        
        scenario = failure['scenario']
        affected_services = failure['affected_services']
        
        logger.info(f"🔧 Manual recovery triggered for: {scenario.name}")
        
        # Same recovery logic as automatic
        if scenario.recovery_action == "restart":
            for service in affected_services:
                self.start_container(service)
        elif scenario.recovery_action == "reconnect":
            for service in affected_services:
                self.unpause_container(service)
        elif scenario.recovery_action == "restart_all":
            for service in affected_services:
                self.start_container(service)
        
        failure['status'] = 'recovered'
        failure['recovery_time'] = time.time()
        
        return True

    def get_failure_status(self) -> Dict:
        """Get status of all failures"""
        return {
            'active_failures': len([f for f in self.active_failures.values() if f['status'] == 'active']),
            'total_failures': len(self.active_failures),
            'failures': self.active_failures
        }

    def simulate_random_failures(self, duration_minutes: int = 10):
        """Simulate random failures for chaos testing"""
        logger.info(f"🎲 Starting random failure simulation for {duration_minutes} minutes")
        
        end_time = time.time() + (duration_minutes * 60)
        
        while time.time() < end_time:
            # Wait random interval between failures
            wait_time = random.randint(30, 120)
            time.sleep(wait_time)
            
            # Pick random scenario
            scenario_name = random.choice(list(self.scenarios.keys()))
            
            # Don't overlap cascading failures
            if scenario_name == "cascading_failure" and len([f for f in self.active_failures.values() if f['status'] == 'active']) > 0:
                continue
            
            try:
                failure_id = self.inject_failure(scenario_name)
                logger.info(f"🎯 Random failure injected: {failure_id}")
            except Exception as e:
                logger.error(f"❌ Failed to inject random failure: {e}")
        
        logger.info("🏁 Random failure simulation completed")

if __name__ == "__main__":
    chaos = ChaosEngineer()
    
    print("🔥 StreamSocial Chaos Engineering Tool")
    print("Available scenarios:")
    for name, scenario in chaos.scenarios.items():
        print(f"  {name}: {scenario.description}")
    
    # Demo: inject a single broker failure
    try:
        failure_id = chaos.inject_failure("single_broker_failure")
        print(f"Injected failure: {failure_id}")
        
        time.sleep(70)  # Wait for recovery
        
        status = chaos.get_failure_status()
        print(f"Final status: {status}")
        
    except KeyboardInterrupt:
        print("🛑 Chaos engineering stopped")
