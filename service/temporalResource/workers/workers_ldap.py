import os
import asyncio
from temporalio.worker import Worker
from service.temporalResource.activity import activities_ldap
from service.temporalResource.workflows import workflows_ldap
from utils.temporal_client import TemporalClientManager

async def start_all_ldap_workers():
    client = await TemporalClientManager.get_temporal_client()
    
    queues = [
        ("ADLdapConfiguration-task-queue", [workflows_ldap.ad_ldap_configuration_workflow], [activities_ldap.ad_ldap_configuration_activity]),
        ("GetLDAPSFromKeycloak-task-queue", [workflows_ldap.get_LDAPs_from_keycloak_workflow], [activities_ldap.get_LDAPs_from_keycloak_activity]),
        ("TestLdapConnection-task-queue", [workflows_ldap.test_ldap_connection_workflow], [activities_ldap.test_ldap_connection_activity]),
        ("TestLdapAuthentication-task-queue", [workflows_ldap.test_ldap_authentication_workflow], [activities_ldap.test_ldap_authentication_activity]),
        ("DeleteLdapConfig-task-queue", [workflows_ldap.delete_ldap_config_workflow], [activities_ldap.delete_ldap_config_activity]),
        ("GetLdapById-task-queue", [workflows_ldap.get_LDAP_by_id_workflow], [activities_ldap.get_LDAP_by_id_activity]),
        ("SyncUserFromKeycloakById-task-queue", [workflows_ldap.sync_user_from_keycloak_Byid_workflow], [activities_ldap.sync_user_from_keycloak_Byid_activity]),
        ("SyncChangedUsers-task-queue", [workflows_ldap.sync_changed_users_from_keycloak_workflow], [activities_ldap.sync_changed_users_from_keycloak_activity]),
        ("UnlinkUsers-task-queue", [workflows_ldap.unlink_users_from_keycloak_workflow], [activities_ldap.unlink_users_from_keycloak_activity]),
        ("RemoveImportedUsers-task-queue", [workflows_ldap.remove_imported_users_from_keycloak_workflow], [activities_ldap.remove_imported_users_from_keycloak_activity]),
        ("UpdateLdapConfig-task-queue", [workflows_ldap.update_ldap_config_workflow], [activities_ldap.update_ldap_config_activity]),
    ]
    
    workers = []
    for queue_name, wfs, acts in queues:
        workers.append(Worker(client, task_queue=queue_name, workflows=wfs, activities=acts))
    
    await asyncio.gather(*[w.run() for w in workers])