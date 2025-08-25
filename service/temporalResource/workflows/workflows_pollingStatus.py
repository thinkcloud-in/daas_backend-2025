from datetime import timedelta
from temporalio import workflow
from service.temporalResource.activity import activities_pollingStatus
from temporalio.common import RetryPolicy


@workflow.defn(sandboxed=False)
class StatusPollerWorkflow:
    @workflow.run
    async def run(self):
        print('Running status poller workflow...')
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        result = await workflow.execute_activity(
            activities_pollingStatus.poll_and_update_machine_status_activity,  # Register this activity name in the worker
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=300),
        )
        # Expecting result like: (workflow_status, machine_status, error_msg)
        # for machine_id, statuses in result.get("statuses", {}).items():
        #     for status_obj in statuses:
        #         if status_obj.get("status") == "COMPLETED":
        #             # Optionally, log power_states
        #             power_states = result.get("power_states", {}).get(machine_id, [])
        #             workflow.logger.info(f"Machine {machine_id} is COMPLETED. Power states: {power_states}")

        return result
 
    

