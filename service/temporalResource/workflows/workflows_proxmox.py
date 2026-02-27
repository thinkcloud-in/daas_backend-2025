import asyncio
from temporalio import workflow
from datetime import timedelta
from service.temporalResource.activity import activities_proxmox
import logging
from temporalio.common import RetryPolicy
from temporalio.workflow import ParentClosePolicy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("create_machine_workflow")
 
@workflow.defn(sandboxed=False)
class WaitAndAssignIPsWorkflow:
    @workflow.run
    async def run(self, vms, ip_list, cluster_id):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=90),
            maximum_attempts=5,
        )
    
        for (name, vmid, node, upid, _), ip in zip(vms, ip_list):
            await workflow.execute_activity(
                activities_proxmox.wait_for_vm_ready_activity,
                args=[{
                    "cluster_id": cluster_id,
                    "node": node,
                    "upid": upid,
                    "timeout": 300,
                }],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(minutes=5),
            )
            await workflow.execute_activity(
                activities_proxmox.assign_ip_to_vm_activity,
                args=[{
                    "vmid": vmid,
                    "cluster_id": cluster_id,
                    "ip_address": ip,
                }],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(minutes=5),
            )
 
 
def batch_items(items, batch_size):
    for i in range(0, len(items), batch_size):
        yield items[i:i+batch_size]
 
@workflow.defn(sandboxed=False)
class CloneVMWorkflow:
    @workflow.run
    async def run(self, clone_payload: dict):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=90),
            maximum_attempts=5,
        )
 
        total_count = clone_payload["count"]
        ip_list = clone_payload["ip_list"]
 
        BATCH_SIZE = 10
        results = []
        for batch_start in range(0, total_count, BATCH_SIZE):
            batch_count = min(BATCH_SIZE, total_count - batch_start)
            batch_ip_list = ip_list[batch_start:batch_start + batch_count]
            batch_payload = clone_payload.copy()
            batch_payload["count"] = batch_count
            batch_payload["ip_list"] = batch_ip_list
            
            logger.info(
                f"Processing batch {batch_start // BATCH_SIZE + 1}: "
                f"VMs {batch_start} to {batch_start + batch_count - 1} "
                f"with IPs {batch_ip_list}"
            )
 
        
            result = await workflow.execute_activity(
                activities_proxmox.clone_vm_activity,
                args=[batch_payload],
                retry_policy=retry_policy,
                start_to_close_timeout=timedelta(minutes=10),
            )
            if isinstance(result, dict) and "error" in result:
            
                return result
            
            vms = result.get("vms", [])
        
            child_handle = await workflow.start_child_workflow(
                WaitAndAssignIPsWorkflow.run,
                args=[vms, batch_ip_list, clone_payload["cluster_id"]],
                id=f"wait-assign-{workflow.info().workflow_id}-batch-{batch_start // BATCH_SIZE + 1}",
                parent_close_policy=ParentClosePolicy.ABANDON
            )
            wait_assign_workflow_id = child_handle.id

        
            vms_out = []
            for (name, vmid, node, upid, clone_workflow_id), ip in zip(vms, batch_ip_list):
                vms_out.append({
                    "name": name,
                    "vmid": vmid,
                    "node": node,
                    "upid": upid,
                    "clone_workflow_id": clone_workflow_id,
                    "wait_assign_workflow_id": wait_assign_workflow_id,
                    "ip": ip
                })
            logger.info(
                f"Batch {batch_start // BATCH_SIZE + 1} cloned VMs: {[v['name'] for v in vms_out]}"
            )
            results.extend(vms_out)

        logger.info("All batches processed.")
        return {"vms": results}
   
 

@workflow.defn(sandboxed=False)
class LiveMigrateWorkflow:
    @workflow.run
    async def run(self, migration_payload: dict) -> str:
        poll_interval = migration_payload.get("poll_interval", 60)
        batch_size = migration_payload.get("batch_size", 5000)
    
        last_time = migration_payload.get("start_time")
        if not last_time:
        
            import datetime
            last_time = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        

        while True:
            migration_payload["time_range_start"] = last_time
            migration_payload["batch_size"] = batch_size
            result = await workflow.execute_activity(
                activities_proxmox.migrate_bucket_new_data_activity,
                args=[migration_payload],
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=2),
                    backoff_coefficient=2.0,
                    maximum_interval=timedelta(seconds=30),
                    maximum_attempts=5,
                ),
                start_to_close_timeout=timedelta(seconds=120),
            )
            new_time = result.get("max_time", last_time)
            if new_time > last_time:
                
                last_time = new_time
            else:
                await asyncio.sleep(poll_interval)




@workflow.defn(sandboxed=False)
class StartVMProxmoxWorkflow:
    @workflow.run
    async def run(self, vmid: int, pool_id: str, email:str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        result = await workflow.execute_activity(
            activities_proxmox.start_vm_proxmox_activity,
            args=[vmid,pool_id,email],
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=retry_policy
        )
        return result
    

@workflow.defn(sandboxed=False)
class StopVMProxmoxWorkflow:
    @workflow.run
    async def run(self, vmid: int, pool_id: str, email: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        result = await workflow.execute_activity(
            activities_proxmox.stop_vm_proxmox_activity,
            args=[vmid,pool_id,email],
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=retry_policy
        )
        return result
    

@workflow.defn(sandboxed=False)
class RebootVMProxmoxWorkflow:
    @workflow.run
    async def run(self, vmid: int, pool_id: str, email: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        result = await workflow.execute_activity(
            activities_proxmox.reboot_vm_proxmox_activity,
            args=[vmid,pool_id,email],
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=retry_policy
        )
        return result
    


@workflow.defn(sandboxed=False)
class ShutdownVMProxmoxWorkflow:
    @workflow.run
    async def run(self, vmid: int, pool_id: str, email: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        result = await workflow.execute_activity(
            activities_proxmox.shutdown_vm_proxmox_activity,
            args=[vmid,pool_id,email],
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=retry_policy
        )
        return result
    


@workflow.defn(sandboxed=False)
class VmRebuildWorkflow:
    @workflow.run
    async def run(self, vmid: int, pool_id: str, email: str):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )

    
        result = await workflow.execute_activity(
            activities_proxmox.vm_rebuild_activity,
            args=[vmid, pool_id, email],
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=120),
        )
        if result.get("status") != "success":
            return result

        upid = result["upid"]
        node = result["node"]
        if result.get("status") == "success":
            cluster_id = result["cluster_id"]
            machine_name = result["machine_name"]
            ip_address = result["ip_address"]
        else:
            return None
            
        vms = [(  
            machine_name,  
            vmid,
            node,
            upid,
            None,  
        )]
        ip_list = [ip_address]
        child_handle = await workflow.start_child_workflow(
            WaitAndAssignIPsWorkflow.run,
            args=[vms, ip_list, cluster_id],
            id=f"wait-assign-{workflow.info().workflow_id}-rebuild-{vmid}",
            parent_close_policy=ParentClosePolicy.ABANDON
        )
    

        return {
            "status": "success",
            "rebuild_result": result,
            "wait_and_assign_result": child_handle.id 
        }




