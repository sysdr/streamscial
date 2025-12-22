"""
Provision ACLs for all StreamSocial services
"""
from acl_manager import ACLManager
from confluent_kafka.admin import ResourceType, AclOperation, ResourcePatternType, AdminClient, NewTopic
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_topics(bootstrap_servers: str):
    """Create required topics"""
    admin = AdminClient({
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'admin',
        'sasl.password': 'admin-secret'
    })
    
    topics = [
        NewTopic('posts.created', num_partitions=3, replication_factor=1),
        NewTopic('posts.updated', num_partitions=3, replication_factor=1),
        NewTopic('posts.deleted', num_partitions=3, replication_factor=1),
        NewTopic('analytics.metrics', num_partitions=3, replication_factor=1),
        NewTopic('analytics.trends', num_partitions=3, replication_factor=1),
        NewTopic('moderation.flags', num_partitions=3, replication_factor=1),
        NewTopic('moderation.actions', num_partitions=3, replication_factor=1),
    ]
    
    try:
        futures = admin.create_topics(topics)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"✓ Created topic: {topic}")
            except Exception as e:
                if 'already exists' not in str(e):
                    logger.error(f"✗ Failed to create {topic}: {e}")
    except Exception as e:
        logger.error(f"Topic creation error: {e}")


def provision_all_acls():
    """Provision ACLs for all services"""
    
    acl_manager = ACLManager(
        'localhost:9093',
        'admin',
        'admin-secret'
    )
    
    # Service configurations
    services = {
        'post-service': {
            'principal': 'post_service',
            'topics': [
                {
                    'name': 'posts.created',
                    'operations': [AclOperation.WRITE, AclOperation.DESCRIBE]
                },
                {
                    'name': 'posts.updated',
                    'operations': [AclOperation.WRITE, AclOperation.DESCRIBE]
                },
                {
                    'name': 'posts.deleted',
                    'operations': [AclOperation.WRITE, AclOperation.DESCRIBE]
                },
                {
                    'name': 'user.profile.updates',
                    'operations': [AclOperation.READ, AclOperation.DESCRIBE]
                }
            ],
            'consumer_groups': [],
            'cluster_operations': [AclOperation.DESCRIBE]
        },
        'analytics-service': {
            'principal': 'analytics_service',
            'topics': [
                {
                    'name': 'posts',
                    'operations': [AclOperation.READ, AclOperation.DESCRIBE],
                    'pattern': ResourcePatternType.PREFIXED
                },
                {
                    'name': 'analytics.metrics',
                    'operations': [AclOperation.WRITE, AclOperation.DESCRIBE]
                },
                {
                    'name': 'analytics.trends',
                    'operations': [AclOperation.WRITE, AclOperation.DESCRIBE]
                }
            ],
            'consumer_groups': [
                {
                    'name': 'analytics-aggregator',
                    'operations': [AclOperation.READ, AclOperation.DESCRIBE]
                }
            ],
            'cluster_operations': [AclOperation.DESCRIBE]
        },
        'moderation-service': {
            'principal': 'moderation_service',
            'topics': [
                {
                    'name': 'posts',
                    'operations': [AclOperation.READ, AclOperation.DELETE, AclOperation.DESCRIBE],
                    'pattern': ResourcePatternType.PREFIXED
                },
                {
                    'name': 'moderation.flags',
                    'operations': [AclOperation.WRITE, AclOperation.READ, AclOperation.DESCRIBE]
                },
                {
                    'name': 'moderation.actions',
                    'operations': [AclOperation.WRITE, AclOperation.DESCRIBE]
                }
            ],
            'consumer_groups': [
                {
                    'name': 'moderation-scanner',
                    'operations': [AclOperation.READ, AclOperation.DESCRIBE]
                }
            ],
            'cluster_operations': [AclOperation.DESCRIBE]
        }
    }
    
    logger.info("=" * 60)
    logger.info("Provisioning ACLs for StreamSocial Services")
    logger.info("=" * 60)
    
    for service_name, config in services.items():
        logger.info(f"\n📋 Provisioning: {service_name}")
        success = acl_manager.create_service_acls(config)
        if success:
            logger.info(f"✓ {service_name} ACLs created successfully")
        else:
            logger.error(f"✗ Failed to create all ACLs for {service_name}")
    
    # Show summary
    time.sleep(2)
    summary = acl_manager.get_acl_summary()
    logger.info("\n" + "=" * 60)
    logger.info("ACL Provisioning Summary")
    logger.info("=" * 60)
    logger.info(f"Total ACLs: {summary['total']}")
    logger.info(f"By Principal: {summary['by_principal']}")
    logger.info(f"By Resource Type: {summary['by_resource_type']}")
    logger.info(f"By Operation: {summary['by_operation']}")


if __name__ == '__main__':
    logger.info("Creating topics...")
    create_topics('localhost:9093')
    
    logger.info("\nWaiting for Kafka to be ready...")
    time.sleep(5)
    
    provision_all_acls()
