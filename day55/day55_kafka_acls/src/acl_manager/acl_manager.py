"""
Kafka ACL Manager - Manages access control lists for StreamSocial services
"""
from confluent_kafka.admin import AdminClient, AclBinding, AclBindingFilter, ResourceType, \
    ResourcePatternType, AclOperation, AclPermissionType
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ACLManager:
    """Manages Kafka ACLs for service-level access control"""
    
    def __init__(self, bootstrap_servers: str, sasl_username: str, sasl_password: str):
        self.admin_client = AdminClient({
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_PLAINTEXT',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': sasl_username,
            'sasl.password': sasl_password
        })
    
    def create_acl(self, principal: str, resource_type: ResourceType, 
                   resource_name: str, operation: AclOperation, 
                   permission: AclPermissionType = AclPermissionType.ALLOW,
                   pattern_type: ResourcePatternType = ResourcePatternType.LITERAL) -> bool:
        """Create a single ACL"""
        try:
            acl_binding = AclBinding(
                restype=resource_type,
                name=resource_name,
                resource_pattern_type=pattern_type,
                principal=f"User:{principal}",
                host="*",
                operation=operation,
                permission_type=permission
            )
            
            result = self.admin_client.create_acls([acl_binding])
            for acl, future in result.items():
                future.result()
                logger.info(f"✓ Created ACL: {principal} -> {operation.name} on {resource_name}")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to create ACL: {e}")
            return False
    
    def create_service_acls(self, service_config: Dict) -> bool:
        """Create all ACLs for a service"""
        principal = service_config['principal']
        success = True
        
        # Topic permissions
        for topic_perm in service_config.get('topics', []):
            topic = topic_perm['name']
            operations = topic_perm['operations']
            pattern = topic_perm.get('pattern', ResourcePatternType.LITERAL)
            
            for operation in operations:
                if not self.create_acl(principal, ResourceType.TOPIC, topic, 
                                      operation, pattern_type=pattern):
                    success = False
        
        # Consumer group permissions
        for group_perm in service_config.get('consumer_groups', []):
            group = group_perm['name']
            operations = group_perm['operations']
            pattern = group_perm.get('pattern', ResourcePatternType.LITERAL)
            
            for operation in operations:
                if not self.create_acl(principal, ResourceType.GROUP, group, 
                                      operation, pattern_type=pattern):
                    success = False
        
        # Cluster permissions
        # Note: confluent_kafka doesn't support ResourceType.CLUSTER
        # Cluster-level operations are typically handled via broker-level ACLs
        # or require super user privileges. Skipping for now.
        # If needed, these can be configured manually via kafka-acls.sh
        cluster_ops = service_config.get('cluster_operations', [])
        if cluster_ops:
            logger.warning(f"Cluster operations requested for {principal}, but confluent_kafka doesn't support ResourceType.CLUSTER. Skipping.")
        
        return success
    
    def list_acls(self, principal: Optional[str] = None) -> List[AclBinding]:
        """List all ACLs or ACLs for specific principal"""
        try:
            filter_principal = f"User:{principal}" if principal else None
            acl_filter = AclBindingFilter(
                restype=ResourceType.ANY,
                name=None,
                resource_pattern_type=ResourcePatternType.ANY,
                principal=filter_principal,
                host=None,
                operation=AclOperation.ANY,
                permission_type=AclPermissionType.ANY
            )
            
            result = self.admin_client.describe_acls(acl_filter)
            acls = result.result()
            return list(acls)
        except Exception as e:
            logger.error(f"Failed to list ACLs: {e}")
            return []
    
    def delete_acls(self, principal: str) -> bool:
        """Delete all ACLs for a principal"""
        try:
            acl_filter = AclBindingFilter(
                restype=ResourceType.ANY,
                name=None,
                resource_pattern_type=ResourcePatternType.ANY,
                principal=f"User:{principal}",
                host=None,
                operation=AclOperation.ANY,
                permission_type=AclPermissionType.ANY
            )
            
            result = self.admin_client.delete_acls([acl_filter])
            for filter_obj, future in result.items():
                deleted_acls = future.result()
                logger.info(f"✓ Deleted {len(deleted_acls)} ACLs for {principal}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete ACLs: {e}")
            return False
    
    def get_acl_summary(self) -> Dict:
        """Get summary of all ACLs"""
        acls = self.list_acls()
        summary = {
            'total': len(acls),
            'by_principal': {},
            'by_resource_type': {},
            'by_operation': {}
        }
        
        for acl in acls:
            # By principal
            principal = acl.principal.split(':')[1]
            summary['by_principal'][principal] = summary['by_principal'].get(principal, 0) + 1
            
            # By resource type
            res_type = acl.restype.name
            summary['by_resource_type'][res_type] = summary['by_resource_type'].get(res_type, 0) + 1
            
            # By operation
            operation = acl.operation.name
            summary['by_operation'][operation] = summary['by_operation'].get(operation, 0) + 1
        
        return summary
