import asyncio
import smtplib
import aiohttp
from temporalio import activity, workflow
from temporalio.worker import Worker
from temporalio.client import Client, Schedule, ScheduleActionStartWorkflow, ScheduleSpec, ScheduleState, ScheduleCalendarSpec, ScheduleRange
from utils.temporal_client import TemporalClientManager
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import timedelta, datetime
from typing import Dict, List
from urllib.parse import quote
import base64

import os
from temporalio.common import RetryPolicy
from dotenv import load_dotenv
load_dotenv()

TEMPORAL_SERVER = os.getenv('TEMPORAL_SERVER')


def calculate_time_range(schedule_type: str) -> tuple[str, str]:
    now = workflow.now()
    
    if schedule_type.lower() == 'daily':

        start_time = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        end_time = (now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999).strftime("%Y-%m-%d %H:%M:%S")
    elif schedule_type.lower() == 'weekly':

        start_time = now - timedelta(days=now.weekday() + 1)
        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

        end_time = (start_time + timedelta(days=6)).replace(hour=23, minute=59, second=59, microsecond=999999).strftime("%Y-%m-%d %H:%M:%S")
    elif schedule_type.lower() == 'monthly':

        start_time = datetime(now.year, now.month, 1).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        
        if now.month == 12:
            end_time = datetime(now.year + 1, 1, 1) - timedelta(days=1) 
        else:
            end_time = datetime(now.year, now.month + 1, 1) - timedelta(days=1)  # Last day of current month
            end_time = end_time.replace(hour=23, minute=59, second=59, microsecond=999999).strftime("%Y-%m-%d %H:%M:%S")
    else:
        raise ValueError(f"Invalid schedule type: {schedule_type}")
    
    
    return start_time, end_time


@activity.defn
async def fetch_pdf_report(base_url: str, start_time: str, end_time: str, report_type: str) -> bytes:
    try:
        encoded_report_type = quote(report_type)
        encoded_start = quote(start_time)
        encoded_end = quote(end_time)
        formatted_url = f"{base_url}/{encoded_start}/{encoded_end}/{encoded_report_type}"       
        async with aiohttp.ClientSession() as session:
            async with session.post(formatted_url) as response:
                if response.status == 200:
                    data = await response.json()
                    # Validate the response structure before accessing nested keys
                    response_data = data.get("data") if isinstance(data, dict) else None
                    if isinstance(response_data, dict) and "pdf_data" in response_data:
                        pdf_base64 = response_data["pdf_data"]
                        pdf_bytes = base64.b64decode(pdf_base64)
                        return pdf_bytes
                    else:
                        raise Exception(f"Unexpected response format from report API. 'data.pdf_data' not found. Response: {data}")
                raise Exception(f"Failed to fetch PDF. Status: {response.status}, URL: {formatted_url}")
    except Exception as e:
        raise Exception(f"Error fetching PDF report: {e}")

@activity.defn
async def send_email_with_pdf_activity(
    smtp_config: Dict[str, str], 
    receiver_emails: List[str],
    pdf_url: str,
    start_time: str,
    end_time: str,
    report_type: str
) -> bool:
    try:
        sender_email = smtp_config.get('email')
        password = smtp_config.get('password')
        smtp_server = smtp_config.get('serverIP')
        smtp_port = int(smtp_config.get('serverPort', 587))
        conn_option = smtp_config.get('connOption', 'TLS')
        if not all([sender_email, password, smtp_server, smtp_port]):
            raise ValueError("Missing required SMTP configuration fields")

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = ", ".join(receiver_emails)
        msg['Subject'] = f"Guacamole Report: {report_type}"

        body = f"""
        Please find attached the {report_type} report.
        
        Report Period:
        From: {start_time}
        To: {end_time}
        
        This is an automated email sent by the scheduling system.
        """
        msg.attach(MIMEText(body, 'plain'))

        pdf_content = await fetch_pdf_report(pdf_url, start_time, end_time, report_type)
        pdf_attachment = MIMEApplication(pdf_content, _subtype="pdf")
        safe_start = start_time.replace(":", "-").replace(" ", "_")
        safe_end = end_time.replace(":", "-").replace(" ", "_")
        filename = f"{report_type}_{safe_start}_to_{safe_end}.pdf"
        pdf_attachment.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(pdf_attachment)

        if conn_option == 'SSL':
            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        else:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()

        smtp_username = smtp_config.get('userName', sender_email)
        server.login(smtp_username, password)
        server.sendmail(sender_email, receiver_emails, msg.as_string())
        
        server.quit()
        
        return True
    except Exception as e:
        raise Exception(f"Error sending email with PDF report: {e}")

@workflow.defn(sandboxed=False)
class EmailWorkflow:
    @workflow.run
    async def run(
        self, 
        smtp_config: Dict[str, str], 
        receiver_emails: List[str],
        schedule_type: str,
        report_type: str,
        pdf_url: str
    ) -> bool:
        start_time, end_time = calculate_time_range(schedule_type)
        retry_policy = RetryPolicy(
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
            maximum_interval=timedelta(seconds=30),
            maximum_attempts=5,
        )
        result = await workflow.execute_activity(
            send_email_with_pdf_activity,
            args=[smtp_config, receiver_emails, pdf_url, start_time, end_time, report_type],
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(minutes=5)
        )
        return result

async def create_pdf_email_schedule(
    client: Client,
    schedule_id: str,
    send_time: str,
    schedule_type: str,
    smtp_config: Dict[str, str],
    receiver_emails: List[str],
    pdf_url: str,
    report_type: str
) -> None:
    # Convert IST (UTC+5:30) to UTC for Temporal
    try:
        # Handle both HH:MM and HH:MM:SS
        if len(send_time.split(':')) == 3:
            input_time = datetime.strptime(send_time, "%H:%M:%S")
        else:
            input_time = datetime.strptime(send_time, "%H:%M")
    except Exception as e:
        # Fallback to current time if parsing fails to avoid total crash
        print(f"Error parsing time '{send_time}': {e}")
        input_time = datetime.now()

    total_minutes = input_time.hour * 60 + input_time.minute
    utc_total_minutes = (total_minutes - 330) % (24 * 60) # 330 mins = 5h 30m
    
    utc_hour = utc_total_minutes // 60
    utc_minute = utc_total_minutes % 60

    calendar_specs = {
        'daily': ScheduleCalendarSpec(
            minute=[ScheduleRange(start=utc_minute)],  
            hour=[ScheduleRange(start=utc_hour)],
            day_of_week=[ScheduleRange(start=i) for i in range(0, 7)] # All 7 days
         ),
         'weekly': ScheduleCalendarSpec(
            minute=[ScheduleRange(start=utc_minute)],  
             hour=[ScheduleRange(start=utc_hour)],
             day_of_week=[ScheduleRange(start=0)]  
         ),
         'monthly': ScheduleCalendarSpec(
             minute=[ScheduleRange(start=utc_minute)],  
             hour=[ScheduleRange(start=utc_hour)],
             day_of_month=[ScheduleRange(start=1)]  
         )
    }

    schedule_spec = calendar_specs.get(schedule_type.lower())
    if not schedule_spec:
        raise ValueError(f"Invalid schedule type: {schedule_type}")

    try:
        await client.create_schedule(
            schedule_id,
            Schedule(
                action=ScheduleActionStartWorkflow(
                    EmailWorkflow.run,
                    args=[smtp_config, receiver_emails, schedule_type, report_type, pdf_url],
                    id=f"{schedule_id}_workflow",
                    task_queue="schedules-task-queue",
                ),
                spec=ScheduleSpec(calendars=[schedule_spec]),
                state=ScheduleState(note=f"Scheduled PDF report email for {schedule_id}")
            )
        )
        
    except Exception as e:
        raise Exception(f"Error creating schedule: {e}")
        
async def run_email_worker():
    try:
        client = await TemporalClientManager.get_temporal_client()
        
        async with Worker(
            client,
            task_queue='schedules-task-queue',
            workflows=[EmailWorkflow],
            activities=[send_email_with_pdf_activity, fetch_pdf_report]
        ):
            await asyncio.Future()
    except Exception as e:
        raise Exception(f"Error in Temporal worker: {e}")

async def temporal_schedules(
    scheduleId: str,
    time: str,
    schedule_type: str,
    smtp_config: Dict[str, str],
    receiver_emails: List[str],
    pdf_url: str,
    report_type: str
) -> str:
    try:
        client = await TemporalClientManager.get_temporal_client()
        await create_pdf_email_schedule(
            client,
            scheduleId,
            time,
            schedule_type,
            smtp_config,
            receiver_emails,
            pdf_url,
            report_type
        )
        return "Schedule created successfully"
    except Exception as e:
        return f"Failed to schedule: {str(e)}"