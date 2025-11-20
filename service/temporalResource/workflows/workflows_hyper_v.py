from temporalio import workflow
from temporalio.common import RetryPolicy
from datetime import timedelta
from service.temporalResource.activity import activities_hyper_v
import logging


@workflow.defn(sandboxed=False)
class CloneVMSingleNodeHyperVWorkflow:
    print("Clone VM Single Node HyperV Workflow started.")
    @workflow.run
    async def run(self, request, db):
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        result = await workflow.execute_activity(
            activities_hyper_v.clone_vm_singleNode_hyper_v_activity,
            args=[request, db],
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=retry_policy
        )
        print("Clone VM Single Node HyperV Workflow completed.")
        return result