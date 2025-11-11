import logging
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertManager:
    def __init__(self):
        self.active_alerts = {}
        
    def process_alert(self, alert):
        """Process and potentially escalate alert"""
        alert_key = f"{alert.get('connector', 'system')}:{alert['type']}"
        
        if alert_key in self.active_alerts:
            # Update existing alert
            self.active_alerts[alert_key]['count'] += 1
            self.active_alerts[alert_key]['last_seen'] = datetime.now()
        else:
            # New alert
            self.active_alerts[alert_key] = {
                'alert': alert,
                'first_seen': datetime.now(),
                'last_seen': datetime.now(),
                'count': 1
            }
            self.send_notification(alert)
            
    def send_notification(self, alert):
        """Send alert notification"""
        severity_emoji = {
            'P0': '🚨',
            'P1': '⚠️',
            'P2': 'ℹ️'
        }
        
        message = f"{severity_emoji.get(alert['severity'], '📢')} {alert['severity']} Alert: {alert['message']}"
        logger.warning(message)
        
        # In production, integrate with Slack, PagerDuty, etc.
        print(f"\n{'='*60}")
        print(f"ALERT TRIGGERED: {message}")
        print(f"Connector: {alert.get('connector', 'N/A')}")
        print(f"Type: {alert['type']}")
        print(f"{'='*60}\n")
        
    def get_active_alerts(self):
        """Get all active alerts"""
        return list(self.active_alerts.values())
        
    def resolve_alert(self, alert_key):
        """Mark alert as resolved"""
        if alert_key in self.active_alerts:
            del self.active_alerts[alert_key]
            logger.info(f"Alert resolved: {alert_key}")
