"""
Demo script for ACL enforcement
"""
import time
import logging
from acl_manager.acl_manager import ACLManager
from services.post_service import PostService
from services.analytics_service import AnalyticsService
from services.moderation_service import ModerationService
from monitoring.acl_dashboard import monitor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def demo_acl_enforcement():
    """Demonstrate ACL enforcement in action"""
    
    logger.info("=" * 70)
    logger.info("StreamSocial ACL Enforcement Demo")
    logger.info("=" * 70)
    
    # Initialize services
    logger.info("\n1️⃣  Initializing services...")
    post_svc = PostService('localhost:9093', 'post_service', 'post-secret')
    analytics_svc = AnalyticsService('localhost:9093', 'analytics_service', 'analytics-secret')
    mod_svc = ModerationService('localhost:9093', 'moderation_service', 'moderation-secret')
    
    # Demo 1: Post Service - Authorized Access
    logger.info("\n2️⃣  Post Service: Creating posts (AUTHORIZED)")
    for i in range(5):
        result = post_svc.create_post(f'user{i}', f'Post content #{i} - This is a test post')
        monitor.record_access('post-service', 'WRITE', 'posts.created', True)
        time.sleep(0.5)
    
    result = post_svc.update_post('post_123', 'Updated content')
    monitor.record_access('post-service', 'WRITE', 'posts.updated', True)
    
    logger.info("\n3️⃣  Post Service: Attempting unauthorized access to analytics (DENIED)")
    result = post_svc.test_unauthorized_access()
    monitor.record_access('post-service', 'WRITE', 'analytics.metrics', False)
    
    # Demo 2: Analytics Service - Authorized Access
    logger.info("\n4️⃣  Analytics Service: Reading posts (AUTHORIZED)")
    analytics_svc.subscribe_to_posts()
    time.sleep(2)
    posts = analytics_svc.process_posts(count=10)
    monitor.record_access('analytics-service', 'READ', 'posts.*', True)
    logger.info(f"Processed {len(posts)} posts")
    
    logger.info("\n5️⃣  Analytics Service: Attempting unauthorized write (DENIED)")
    result = analytics_svc.test_unauthorized_access()
    monitor.record_access('analytics-service', 'WRITE', 'posts.created', False)
    
    # Demo 3: Moderation Service - Elevated Permissions
    logger.info("\n6️⃣  Moderation Service: Scanning posts (AUTHORIZED)")
    mod_svc.subscribe_to_posts()
    time.sleep(2)
    
    # Create some flaggable content
    post_svc.create_post('user_bad', 'This is spam content')
    monitor.record_access('post-service', 'WRITE', 'posts.created', True)
    time.sleep(1)
    
    flagged = mod_svc.scan_posts(count=10)
    monitor.record_access('moderation-service', 'READ', 'posts.*', True)
    logger.info(f"Flagged {len(flagged)} posts")
    
    if flagged:
        logger.info("\n7️⃣  Moderation Service: Requesting deletion (AUTHORIZED)")
        result = mod_svc.request_deletion(flagged[0]['post_id'])
        monitor.record_access('moderation-service', 'DELETE', 'posts.deleted', True)
    
    # Show statistics
    logger.info("\n" + "=" * 70)
    logger.info("Service Statistics")
    logger.info("=" * 70)
    logger.info(f"Post Service: {post_svc.get_stats()}")
    logger.info(f"Analytics Service: {analytics_svc.get_stats()}")
    logger.info(f"Moderation Service: {mod_svc.get_stats()}")
    
    # ACL Summary
    logger.info("\n" + "=" * 70)
    logger.info("ACL Summary")
    logger.info("=" * 70)
    acl_manager = ACLManager('localhost:9093', 'admin', 'admin-secret')
    summary = acl_manager.get_acl_summary()
    logger.info(f"Total ACLs: {summary['total']}")
    logger.info(f"By Principal: {summary['by_principal']}")
    logger.info(f"By Operation: {summary['by_operation']}")
    
    logger.info("\n✅ Demo completed! Check the dashboard at http://localhost:5055")
    
    # Cleanup
    analytics_svc.close()
    mod_svc.close()


if __name__ == '__main__':
    try:
        demo_acl_enforcement()
    except KeyboardInterrupt:
        logger.info("\nDemo interrupted by user")
    except Exception as e:
        logger.error(f"Demo error: {e}", exc_info=True)
