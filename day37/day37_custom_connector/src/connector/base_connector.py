"""
Base Connector Class - Configuration and Task Coordination
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any
import yaml
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConnectorConfig:
    """Validates and manages connector configuration"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.errors = []
        
    def validate(self) -> List[str]:
        """Validate configuration parameters"""
        errors = []
        
        # Check required fields
        if 'connector' not in self.config:
            errors.append("Missing 'connector' section")
            
        if 'platforms' not in self.config:
            errors.append("Missing 'platforms' section")
            
        if 'kafka' not in self.config:
            errors.append("Missing 'kafka' section")
            
        # Validate platform configs
        if 'platforms' in self.config:
            for platform, config in self.config['platforms'].items():
                if not config.get('enabled'):
                    continue
                    
                if not config.get('api_base'):
                    errors.append(f"Platform {platform} missing 'api_base'")
                    
                if not config.get('rate_limit'):
                    errors.append(f"Platform {platform} missing 'rate_limit'")
                    
        # Validate accounts
        if 'accounts' not in self.config or not self.config['accounts']:
            errors.append("No accounts configured")
            
        for account in self.config.get('accounts', []):
            if 'platform' not in account:
                errors.append(f"Account missing 'platform': {account}")
            if 'account_id' not in account:
                errors.append(f"Account missing 'account_id': {account}")
            if 'credentials' not in account:
                errors.append(f"Account missing 'credentials': {account}")
                
        return errors
        
    def get(self, key: str, default=None):
        """Get configuration value"""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value


class BaseConnector(ABC):
    """Base connector class - manages lifecycle and task distribution"""
    
    def __init__(self):
        self.config = None
        self.state = "UNASSIGNED"
        
    def start(self, config: Dict[str, Any]):
        """Initialize connector with configuration"""
        logger.info("Starting connector...")
        
        self.config = ConnectorConfig(config)
        errors = self.config.validate()
        
        if errors:
            self.state = "FAILED"
            raise ValueError(f"Configuration errors: {', '.join(errors)}")
            
        self.state = "RUNNING"
        logger.info(f"Connector started: {self.config.get('connector.name')}")
        
    def stop(self):
        """Graceful shutdown"""
        logger.info("Stopping connector...")
        self.state = "STOPPED"
        
    def task_configs(self, max_tasks: int) -> List[Dict[str, Any]]:
        """
        Split work into task configurations
        One task per account for isolation
        """
        accounts = self.config.get('accounts', [])
        task_configs = []
        
        for account in accounts:
            platform = account['platform']
            
            # Only create tasks for enabled platforms
            if not self.config.get(f'platforms.{platform}.enabled'):
                continue
                
            task_config = {
                'platform': platform,
                'account_id': account['account_id'],
                'credentials': account['credentials'],
                'api_base': self.config.get(f'platforms.{platform}.api_base'),
                'rate_limit': self.config.get(f'platforms.{platform}.rate_limit'),
                'poll_interval_ms': self.config.get(f'platforms.{platform}.poll_interval_ms'),
                'kafka': self.config.get('kafka'),
            }
            task_configs.append(task_config)
            
        # Limit to max_tasks
        return task_configs[:max_tasks]
        
    @abstractmethod
    def version(self) -> str:
        """Return connector version"""
        pass
