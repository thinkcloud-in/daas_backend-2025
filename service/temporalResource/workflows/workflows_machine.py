from datetime import timedelta
import logging
from temporalio import workflow
from temporalio.common import RetryPolicy
from service.temporalResource.activity import activities_machine, activities_proxmox
from sqlalchemy.orm import Session

# Logging is configured centrally in utils/logging_config.py (called from
# main.py at startup) — do not reconfigure it per-module.
logger = logging.getLogger("create_machine_workflow")

@workflow.defn(sandboxed=False)
class CreateMachineWorkflow:
    @workflow.run
    async def run(self, machine_data: dict):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        try:
            logger.info("Running workflow to create machine...")
            result = await workflow.execute_activity(
                activities_machine.create_machine_activity,
                args=[machine_data],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )

            if result.get("msg") == "Machine created successfully":
                machine_info = result.get("machine", {})
                vmid = machine_info.get("vm_id")
                pool_id = machine_data.get("pool_id")
                
                logger.info(f"Machine {machine_info.get('name')} created. Attempting auto-start. vmid={vmid}, pool_id={pool_id}")

                if vmid and pool_id:
                    logger.info(f"Triggering PowerOnMachineWorkflow for vmid={vmid}")
                    try:
                        await workflow.execute_child_workflow(
                            PowerOnMachineWorkflow.run,
                            args=[str(vmid), str(pool_id)],
                            id=f"power-on-{vmid}-{workflow.info().run_id[:4]}",
                            retry_policy=retry_policy,
                        )
                        logger.info(f"PowerOnMachineWorkflow finished for vmid={vmid}")
                    except Exception as child_err:
                        logger.error(f"PowerOnMachineWorkflow failed for vmid={vmid}: {str(child_err)}")
                        # We don't necessarily want to fail the whole creation if only power-on fails
                else:
                    logger.warning(f"Could not auto-start: missing vmid ({vmid}) or pool_id ({pool_id})")

            logger.info("Workflow completed successfully.")
            return result
        except Exception as e:
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e

@workflow.defn(sandboxed=False)
class DeleteMachineWorkflow:
    
    @workflow.run
    async def run(self,machine_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            logger.info("Running workflow to create machine...")
            
            
            result = await workflow.execute_activity(
                activities_machine.delete_machine_activity,
                args= [machine_id],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            
            logger.info("Workflow completed successfully.")
            return result
        except Exception as e:
            
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e
        
@workflow.defn(sandboxed=False)
class UpdateMachineWorkflow:
    
    @workflow.run
    async def run(self,machine_identifier: str, machine_data: dict):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            logger.info("Running workflow to update machine...")
            
            
            result = await workflow.execute_activity(
                activities_machine.update_machine_activity,
                args= [machine_identifier,machine_data],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            
            logger.info("Workflow completed successfully.")
            return result
        except Exception as e:
            
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e
        

@workflow.defn(sandboxed=False)
class GetAllMachinesWorkflow:
    
    @workflow.run
    async def run(self):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            logger.info("Running workflow to get all machines...")
            
            
            result = await workflow.execute_activity(
                activities_machine.get_all_machines_activity,
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            
            logger.info("Workflow completed successfully.")
            return result
        except Exception as e:
            
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e
        

@workflow.defn(sandboxed=False)
class UpdateIsCustomMachineWorkflow:
    
    @workflow.run
    async def run(self,machine_identifier: str, machine_details: dict):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            logger.info("Running workflow to update is_custom_machine...")
            
            
            result = await workflow.execute_activity(
                activities_machine.update_is_custom_machine_activity,
                args = [machine_identifier, machine_details],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            
            logger.info("Workflow completed successfully.")
            return result
        except Exception as e:
            
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e
        
@workflow.defn(sandboxed=False)
class AddUserToMachineWorkflow:
    
    @workflow.run
    async def run(self,machine_identifier: str, username: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            logger.info("running workflow on AddUserToMachineWorkflow")
            
            
            result = await workflow.execute_activity(
                activities_machine.add_user_to_machine_activity,
                args = [machine_identifier, username],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            
            logger.info("Workflow completed successfully.")
            return result
        except Exception as e:
            
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e
        
@workflow.defn(sandboxed=False)
class DeleteUserFromMachineWorkflow:
    @workflow.run
    async def run(self,machine_identifier: str, user_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            logger.info("Running workflow on DeleteUserFromMachineWorkflow")
            
            
            result = await workflow.execute_activity(
                activities_machine.delete_user_from_machine_activity,
                args = [machine_identifier, user_id],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            
            logger.info("Workflow completed successfully.")
            return result
        except Exception as e:
            
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e
        

@workflow.defn(sandboxed=False)
class GetAllMachineInPoolWorkflow:
    @workflow.run
    async def run(self,pool_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            logger.info("Running workflow on ListAllMachineInPoolWorkflow")
            
            
            result = await workflow.execute_activity(
                activities_machine.list_all_machine_in_pool_activity,
                args = [pool_id],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            
            logger.info("Workflow completed successfully.")
            return result
        except Exception as e:
            
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e
        

@workflow.defn(sandboxed=False)
class ListOfAsignedUsersWorkflow:
    @workflow.run
    async def run(self,machine_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            logger.info("Running workflow on ListOfAsignedUsersWorkflow")
            
            
            result = await workflow.execute_activity(
                activities_machine.list_of_asigned_users_activity,
                args = [machine_id],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            
            logger.info("Workflow completed successfully.")
            return result
        except Exception as e:
            
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e
        

@workflow.defn(sandboxed=False)
class GetMachineDetailsWorkflow:
    @workflow.run
    async def run(self,machine_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2), 
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30), 
            maximum_attempts=5,
        )
        try:
            logger.info("Running workflow on GetMachineDetailsWorkflow")
            
            
            result = await workflow.execute_activity(
                activities_machine.get_machine_details_activity,
                args = [machine_id],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=60),
            )
            
            
            logger.info("Workflow completed successfully.")
            return result
        except Exception as e:
            
            logger.error(f"Error occurred in workflow: {str(e)}", exc_info=True)
            raise e

@workflow.defn(sandboxed=False)
class PowerOnMachineWorkflow:
    @workflow.run
    async def run(self, vmid: str, pool_id: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=5),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=60),
            maximum_attempts=3,
        )
        logger.info(f"PowerOnMachineWorkflow started for vmid={vmid}, pool_id={pool_id}")
        try:
            result = await workflow.execute_activity(
                activities_proxmox.start_vm_proxmox_activity,
                args=[vmid, pool_id],
                task_queue="vmpower-task-queue",
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(seconds=180),
            )
            logger.info(f"PowerOnMachineWorkflow completed for vmid={vmid}")
            return result
        except Exception as e:
            logger.error(f"PowerOnMachineWorkflow failed for vmid={vmid}: {str(e)}")
            raise e