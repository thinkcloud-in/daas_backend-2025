from temporalio.worker import Worker
from temporalio.client import Client
import os
from service.temporalResource.activity import activities_ldap
from service.temporalResource.workflows import workflows_ldap
from utils.temporal_client import TemporalClientManager

async def ad_ldap_configuration_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="ADLdapConfiguration-task-queue",
        workflows=[workflows_ldap.ad_ldap_configuration_workflow], 
        activities=[activities_ldap.ad_ldap_configuration_activity],
    )
    
    try:
        await worker.run()
    except Exception as e:
        raise e


async def get_lDAPS_from_keycloak_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="GetLDAPSFromKeycloak-task-queue",
        workflows=[workflows_ldap.get_LDAPs_from_keycloak_workflow], 
        activities=[activities_ldap.get_LDAPs_from_keycloak_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e

async def test_ldap_connection_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="TestLdapConnection-task-queue",
        workflows=[workflows_ldap.test_ldap_connection_workflow], 
        activities=[activities_ldap.test_ldap_connection_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e


async def test_ldap_authentication_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="TestLdapAuthentication-task-queue",
        workflows=[workflows_ldap.test_ldap_authentication_workflow], 
        activities=[activities_ldap.test_ldap_authentication_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e


async def delete_ldap_config_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="DeleteLdapConfig-task-queue",
        workflows=[workflows_ldap.delete_ldap_config_workflow], 
        activities=[activities_ldap.delete_ldap_config_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e


async def get_LDAP_by_id_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="GetLdapById-task-queue",
        workflows=[workflows_ldap.get_LDAP_by_id_workflow], 
        activities=[activities_ldap.get_LDAP_by_id_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e


async def sync_user_from_keycloak_Byid_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="SyncUserFromKeycloakById-task-queue",
        workflows=[workflows_ldap.sync_user_from_keycloak_Byid_workflow], 
        activities=[activities_ldap.sync_user_from_keycloak_Byid_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e


async def sync_changed_users_from_keycloak_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="SyncChangedUsers-task-queue",
        workflows=[workflows_ldap.sync_changed_users_from_keycloak_workflow], 
        activities=[activities_ldap.sync_changed_users_from_keycloak_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e

    
async def unlink_users_from_keycloak_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="UnlinkUsers-task-queue",
        workflows=[workflows_ldap.unlink_users_from_keycloak_workflow], 
        activities=[activities_ldap.unlink_users_from_keycloak_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e

    
async def remove_imported_users_from_keycloak_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="RemoveImportedUsers-task-queue",
        workflows=[workflows_ldap.remove_imported_users_from_keycloak_workflow], 
        activities=[activities_ldap.remove_imported_users_from_keycloak_activity],
    )
    
    try:
        await worker.run()
        
    except Exception as e:
        raise e


async def update_ldap_config_worker():
    client = await TemporalClientManager.get_temporal_client()
    worker = Worker(
        client,
        task_queue="UpdateLdapConfig-task-queue",
        workflows=[workflows_ldap.update_ldap_config_workflow], 
        activities=[activities_ldap.update_ldap_config_activity],
    )
    
    try:
        await worker.run()
    except Exception as e:
        raise e

async def run_all_ldap_workers():
    """Starts all LDAP-related workers concurrently in the same event loop."""
    import asyncio
    
    tasks = [
        ad_ldap_configuration_worker(),
        get_lDAPS_from_keycloak_worker(),
        test_ldap_connection_worker(),
        test_ldap_authentication_worker(),
        delete_ldap_config_worker(),
        get_LDAP_by_id_worker(),
        sync_user_from_keycloak_Byid_worker(),
        sync_changed_users_from_keycloak_worker(),
        unlink_users_from_keycloak_worker(),
        remove_imported_users_from_keycloak_worker(),
        update_ldap_config_worker()
    ]
    
    print("Starting all LDAP workers...")
    await asyncio.gather(*tasks)
    