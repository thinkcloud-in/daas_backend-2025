from temporalio.worker import Worker
from temporalio.client import Client
import os

from service.temporalResource.activity import activities_ldap
from service.temporalResource.workflows import workflows_ldap

async def connectionWithTemporal():
    print('Connecting to Temporal server...')
    try:
        client = await Client.connect(os.getenv('TEMPORAL_SERVER'))  
        print('Connected to Temporal server.')
        return client
    except Exception as e:
        print(f"Connection Refused to Temporal server: {e}")
        return None
async def ad_ldap_configuration_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="ADLdapConfiguration-task-queue",
        workflows=[workflows_ldap.ad_ldap_configuration_workflow], 
        activities=[activities_ldap.ad_ldap_configuration_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")


async def get_lDAPS_from_keycloak_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="GetLDAPSFromKeycloak-task-queue",
        workflows=[workflows_ldap.get_LDAPs_from_keycloak_workflow], 
        activities=[activities_ldap.get_LDAPs_from_keycloak_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")

async def test_ldap_connection_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="TestLdapConnection-task-queue",
        workflows=[workflows_ldap.test_ldap_connection_workflow], 
        activities=[activities_ldap.test_ldap_connection_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")


async def test_ldap_authentication_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="TestLdapAuthentication-task-queue",
        workflows=[workflows_ldap.test_ldap_authentication_workflow], 
        activities=[activities_ldap.test_ldap_authentication_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")


async def delete_ldap_config_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="DeleteLdapConfig-task-queue",
        workflows=[workflows_ldap.delete_ldap_config_workflow], 
        activities=[activities_ldap.delete_ldap_config_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")


async def get_LDAP_by_id_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="GetLdapById-task-queue",
        workflows=[workflows_ldap.get_LDAP_by_id_workflow], 
        activities=[activities_ldap.get_LDAP_by_id_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")


async def sync_user_from_keycloak_Byid_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="SyncUserFromKeycloakById-task-queue",
        workflows=[workflows_ldap.sync_user_from_keycloak_Byid_workflow], 
        activities=[activities_ldap.sync_user_from_keycloak_Byid_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")


async def sync_changed_users_from_keycloak_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="SyncChangedUsers-task-queue",
        workflows=[workflows_ldap.sync_changed_users_from_keycloak_workflow], 
        activities=[activities_ldap.sync_changed_users_from_keycloak_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")

    
async def unlink_users_from_keycloak_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="UnlinkUsers-task-queue",
        workflows=[workflows_ldap.unlink_users_from_keycloak_workflow], 
        activities=[activities_ldap.unlink_users_from_keycloak_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")

    
async def remove_imported_users_from_keycloak_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="RemoveImportedUsers-task-queue",
        workflows=[workflows_ldap.remove_imported_users_from_keycloak_workflow], 
        activities=[activities_ldap.remove_imported_users_from_keycloak_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")


async def update_ldap_config_worker():
    client = await connectionWithTemporal()
    worker = Worker(
        client,
        task_queue="UpdateLdapConfig-task-queue",
        workflows=[workflows_ldap.update_ldap_config_workflow], 
        activities=[activities_ldap.update_ldap_config_activity],
    )
    print('Worker starting...')
    try:
        await worker.run()
        print("Temporal worker started. Waiting for tasks...")
    except Exception as e:
        print(f"Error in worker: {e}")
    