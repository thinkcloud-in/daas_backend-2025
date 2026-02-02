# # ------------------------------------------------- Extract Status ---------------------------------------------
 
# import os
# from temporalio.client import Client
# from fastapi import HTTPException
# from datetime import datetime, timezone
# from dotenv import load_dotenv
# load_dotenv()
 
 
# async def get_temporal_client():
#     """Establish connection with the Temporal server."""
#     try:
#         client = await Client.connect(os.getenv('TEMPORAL_SERVER'))
#         return client
#     except Exception as e:
#         print(f"Connection Refused to Temporal server: {e}")
#         raise HTTPException(status_code=503, detail=f"Cannot connect to Temporal server: {str(e)}")
 
# async def get_workflow_status(workflow_id: str):
#     print(workflow_id,"..............")
#     client = await get_temporal_client()
 
#     try:
#         handle = client.get_workflow_handle(workflow_id)  #  retrieves the workflow handle
#         description = await handle.describe()
 
 
#         # print(f"DEBUG: Raw Temporal Status for {workflow_id} -> {description.status}")
 
#         status_mapping = {
#             0: "UNKNOWN",
#             1: "RUNNING",
#             2: "COMPLETED",
#             3: "FAILED",
#             4: "CANCELED",
#             5: "TERMINATED",
#             6: "TIMED_OUT",
#             7: "CONTINUED_AS_NEW"
#         }
 
#         raw_status = description.status
#         # print('------------------------>',description)
 
#         status = status_mapping.get(raw_status, str(raw_status))  # Convert unknown status to string
#         # start_time = description.start_time.strftime('%d/%m/%y %H:%M:%S:%f')[:-3]
#         # end_time = description.close_time.strftime('%d/%m/%y %H:%M:%S:%f')[:-3]
#         # time_took = description.execution_time.strftime('%S')
#         # time_taken = str(round((description.close_time - description.start_time).total_seconds()*100))+'ms'

       

# # Ensure timestamps are in UTC and properly formatted
#         start_time = (
#             description.start_time.replace(tzinfo=timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
#         if description.start_time else "N/A"
#         )
#         end_time = (
#             description.close_time.replace(tzinfo=timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
#         if description.close_time else "N/A"
#         )

# # Ensure correct time calculation, handle missing timestamps
#         time_taken = (
#             str(round((description.close_time - description.start_time).total_seconds() * 100)) + 'ms'
#         if description.start_time and description.close_time else "N/A"
# )

#         task_name = description.task_queue.split('-')[0] if '-' in description.task_queue else description.task_queue.split('_')[0]
#         # print("start_time",start_time)
#         # print("end_time",end_time)
      
        
#         return {
#             'status':status,
#             'start_time':start_time,
#             'end_time':end_time,
#             'time_taken':time_taken,
#             'task_name':task_name,
#             'workflow_id': workflow_id
#         }
    
#     except Exception as e:
#         print(f"Error getting workflow status: {e}")
#         return {
#             "workflow_id": workflow_id,
#             "status": "ERROR",
#             "error": f"Error retrieving status: {str(e)}"
#         }
    
 
# # async def main():
# #     workflow_id = "Retrieving-schedule-data-Vamanit-15:27:22"
# #     status = await get_workflow_status(workflow_id)
# #     print(status)
 
# # asyncio.run(main())
 

 