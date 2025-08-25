import asyncio
import smtplib
import aiohttp
from temporalio import activity, workflow
from temporalio.worker import Worker
from temporalio.client import Client, Schedule, ScheduleActionStartWorkflow, ScheduleSpec, ScheduleState, ScheduleCalendarSpec, ScheduleRange
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import timedelta, datetime
from typing import Dict, List
from urllib.parse import quote
from dotenv import load_dotenv
import os

load_dotenv()

TEMPORAL_SERVER = os.getenv('TEMPORAL_SERVER')
print(f"Connecting to Temporal server at: {TEMPORAL_SERVER}")

def calculate_time_range(schedule_type: str) -> tuple[str, str]:
    now = workflow.now()
    
    if schedule_type.lower() == 'daily':

        start_time = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        end_time = (now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999).strftime("%Y-%m-%d %H:%M:%S")
    elif schedule_type.lower() == 'weekly':

        start_time = now - timedelta(days=now.weekday() + 1)  # Subtract days to reach Sunday
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
        
        print(f"Fetching PDF from URL: {formatted_url}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(formatted_url) as response:
                if response.status == 200:
                    return await response.read()
                raise Exception(f"Failed to fetch PDF. Status: {response.status}, URL: {formatted_url}")
    except Exception as e:
        print(f"Error fetching PDF: {e}")
        # raise

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
        filename = f"{report_type}_{start_time}_to_{end_time}.pdf"
        pdf_attachment.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(pdf_attachment)

        if conn_option == 'SSL':
            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        else:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()

        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_emails, msg.as_string())
        print(f"Email with PDF report sent successfully to {receiver_emails}...")
        server.quit()
        
        return True
    except Exception as e:
        print(f"Email sending error: {e}")
        # raise

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
        
        result = await workflow.execute_activity(
            send_email_with_pdf_activity,
            args=[smtp_config, receiver_emails, pdf_url, start_time, end_time, report_type],
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
    input_time = datetime.strptime(send_time, "%H:%M")
    updated_time = input_time - timedelta(hours=5, minutes=30)  # Time zone adjustment

    today = datetime.today().date()
    send_datetime = datetime.combine(today + timedelta(days=1), datetime.min.time()) + timedelta(
        hours=updated_time.hour, minutes=updated_time.minute
    )

    calendar_specs = {
        'daily': ScheduleCalendarSpec(
            minute=[ScheduleRange(start=send_datetime.minute)],  
            hour=[ScheduleRange(start=send_datetime.hour)],
             day_of_week=[ScheduleRange(start=i) for i in range(1, 6)]  
         ),
         'weekly': ScheduleCalendarSpec(
            minute=[ScheduleRange(start=send_datetime.minute)],  
             hour=[ScheduleRange(start=send_datetime.hour)],
             day_of_week=[ScheduleRange(start=0)]  
         ),
         'monthly': ScheduleCalendarSpec(
             minute=[ScheduleRange(start=send_datetime.minute)],  
             hour=[ScheduleRange(start=send_datetime.hour)],
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
        print(f"Schedule created with ID: {schedule_id} at {send_time}")
    except Exception as e:
        print(f"Error creating schedule: {e}")
        

async def run_temporal_worker(temporal_server: str):
    try:
        client = await Client.connect(temporal_server)
        print(f"Connecting to Temporal server at: {temporal_server}")
        
        async with Worker(
            client,
            task_queue='schedules-task-queue',
            workflows=[EmailWorkflow],
            activities=[send_email_with_pdf_activity, fetch_pdf_report]
        ):
            print("Temporal worker started. Waiting for tasks...")
            await asyncio.Future()
    except Exception as e:
        print(f"Error in Temporal worker: {e}")

async def temporal_schedules(
    scheduleId: str,
    time: str,
    schedule_type: str,
    smtp_config: Dict[str, str],
    receiver_emails: List[str],
    pdf_url: str,
    report_type: str,
    temporal_server: str = TEMPORAL_SERVER
) -> str:
    try:
        client = await Client.connect(temporal_server)
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
        await run_temporal_worker(temporal_server)
        return "Schedule created successfully"
    except Exception as e:
        print(f"Error in temporal_schedules: {e}")
        return f"Failed to schedule: {str(e)}"