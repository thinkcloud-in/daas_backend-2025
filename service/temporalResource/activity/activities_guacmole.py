import base64
from datetime import datetime, timedelta
from io import BytesIO
import os
from typing import Dict, List
import aiohttp
import logging  
import psycopg2
from temporalio import  activity
import service.gucamoleService as service
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph,Image
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from dotenv import load_dotenv
load_dotenv()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)



logger = logging.getLogger("Guacamole LOGGER")

def get_db_connection():
    connection = psycopg2.connect(
        user=os.getenv('USER_NAME'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST_NAME'),
        port=os.getenv("PORT"),
        database="thinkclouddb"
    )
    return connection

# Simple activity to log recipients
@activity.defn
async def login_with_guacamole_activity():
    url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/tokens"
    username = 'guacadmin'
    password = 'guacadmin'
    payload = 'username='+username+'&password='+password
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
   
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=payload) as response:
            if response.status == 200:
                return await response.json()
            else:
                raise Exception(f"Failed to authenticate with Guacamole: {response.status}")

@activity.defn
async def list_of_guaco_users_activity():
    gucamole_list_url= f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/users"
    from ...gucamoleService import login_with_guacamole
    gucamole_login_data =  await login_with_guacamole()
    url=gucamole_list_url+"?token="+ gucamole_login_data
    async with aiohttp.ClientSession()as session:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                raise Exception(f"Failed to load Users: {response.status}")

@activity.defn
async def get_userlist_from_keycloak_activity():
    import os
    import re
    import aiohttp

    try:
        # Sanitize variables from your YAML
        root_url = os.getenv('KEYCLOAK_ROOT_URL', '').strip().rstrip('/')
        realm_name = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        realm = realm_name.strip() if realm_name else 'guacamole'
        
        # Get access token
        async with aiohttp.ClientSession() as session:
            token_url = f"{root_url}/realms/master/protocol/openid-connect/token"
            async with session.post(
                token_url,
                data={
                    "client_id": "admin-cli",
                    "username": os.getenv('KEYCLOAK_ADMIN'),
                    "password": os.getenv('KEYCLOAK_PASSWORD'),
                    "grant_type": "password"
                }
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                access_token = data["access_token"]
            
            auth_headers = {
                "Authorization": f"Bearer {access_token}",
                "content-type": "application/json"
            }
            
            # Reverting back to your original endpoint
            users_url = f"{root_url}/admin/realms/{realm}/ui-ext/brute-force-user?briefRepresentation=true&first=0&max=11&q=&search=*"
            
            async with session.get(users_url, headers=auth_headers) as resp:
                resp.raise_for_status()
                json_data = await resp.text()

            # Original regex parsing
            username_pattern = r'"username"\s*:\s*"([^"]*)"'
            id_pattern = r'"id"\s*:\s*"([^"]*)"'
            usernames = re.findall(username_pattern, json_data)
            ids = re.findall(id_pattern, json_data)
            
            list_of_users_data = [
                {'username': username, 'userid': user_id}
                for username, user_id in zip(usernames, ids)
            ]
            return list_of_users_data

    except Exception as e:
        return [{
            "error": str(e),
            "type": e.__class__.__name__
        }]
@activity.defn
async def list_of_machine_activity():
    try:
        list_machines =f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/connections"
        from ...gucamoleService import login_with_guacamole
        guacamole_login =  await login_with_guacamole()
        url = list_machines + "?token=" + guacamole_login
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response: 
                if response.status == 200:
                    response_data = await response.json() # Await JSON response
                    return response_data
                else:
                    raise Exception("Failed to load User")

    except Exception as e:
        return {"msg": "Error occurred", "error": str(e)}


@activity.defn
async def creating_machine_activity(machine_data:dict):
    gucamole_connection_url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/connections"
    from ...gucamoleService import login_with_guacamole
    guacamole_login =  await login_with_guacamole()

    url = gucamole_connection_url + "?token=" + guacamole_login

    from ...gucamoleService import return_payload
    payload=return_payload(machine_data)
    headers = {
        'Content-Type': 'application/json'
    }  
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=payload) as response:   
                if response.status == 200:
                    return await response.json()
                else:
                    return await response.json()
    except Exception as e:
        return {"msg": "Error occurred", "error": str(e)}

@activity.defn
async def get_session_reports_activity(start_date_str: str, end_date_str: str):
    try:
        from ...gucamoleService import login_with_guacamole
        token = await login_with_guacamole()
        from ...gucamoleService import get_users_connection_history
        users_history = await get_users_connection_history(token)
        session_reports = []

        # Robust datetime parsing
        def parse_dt(dt_str):
            if isinstance(dt_str, datetime):
                return dt_str
            for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S.%f'):
                try:
                    return datetime.strptime(dt_str, fmt)
                except:
                    continue
            return datetime.fromisoformat(dt_str)

        try:
            start_date_range = parse_dt(start_date_str)
            end_date_range = parse_dt(end_date_str)
        except Exception as e:
            logger.error(f"Failed to parse dates: {start_date_str}, {end_date_str}. Error: {e}")
            return {"msg": "Error occurred", "error": f"Invalid date format: {e}"}

        logger.info(f"Filtering sessions from {start_date_range} to {end_date_range}")
        
        for entry in users_history:
            if entry.get('startDate') is not None:
                # Guacamole startDate is in milliseconds
                start_date = datetime.fromtimestamp(entry['startDate'] / 1000)
                end_date = datetime.fromtimestamp(entry['endDate'] / 1000) if entry.get('endDate') else None
                duration_seconds = (end_date - start_date).total_seconds() if end_date else None

                # Check if session falls within range
                if start_date_range <= start_date <= end_date_range:
                    # connectionName is the actual machine name, remoteHost is the user's IP
                    machine_name = entry.get('connectionName') or entry.get('remoteHost', 'Unknown')
                    session_reports.append({
                        'loginTime': start_date.strftime('%Y-%m-%d %H:%M:%S'),
                        'logoutTime': end_date.strftime('%Y-%m-%d %H:%M:%S') if end_date else 'Not Applicable',
                        'username': entry['username'],
                        'machineName': machine_name,
                        'sessionDuration': duration_seconds if duration_seconds is not None else 'Not Applicable'
                    })
            elif entry.get('endDate') is not None:
                end_date = datetime.fromtimestamp(entry['endDate'] / 1000)
                if start_date_range <= end_date <= end_date_range:
                    machine_name = entry.get('connectionName') or entry.get('remoteHost', 'Unknown')
                    session_reports.append({
                        'loginTime': 'Not Applicable',
                        'logoutTime': end_date.strftime('%Y-%m-%d %H:%M:%S'),
                        'username': entry['username'],
                        'machineName': machine_name,
                        'sessionDuration': 'Not Applicable'
                    })
        
        logger.info(f"Found {len(session_reports)} sessions in range")
        return session_reports
    except Exception as e:
        logger.error(f"Error in get_session_reports_activity: {e}", exc_info=True)
        return {"msg": "Error occurred", "error": str(e)}


@activity.defn
async def get_all_users_vamanit_activity(session_reports : list):
    try:
        users = set(entry['username'] for entry in session_reports)
        return list(users)
    except Exception as e:
        return {"msg": "Error occurred", "error": str(e)}

@activity.defn
async def get_perticular_user_session_report_activity(session_reports : list, username:str):
    try:
        user_sessions = [row for row in session_reports if row["username"] == username]
        return user_sessions
    except Exception as e:
        return {"msg": "Error occurred", "error": str(e)}

@activity.defn
async def get_daily_reports_activity(session_reports: list):
    if not isinstance(session_reports, list):
        logger.error(f"get_daily_reports_activity received non-list input: {type(session_reports)}")
        return []

    day_duration_map = {}

    for row in session_reports:
        if row.get("loginTime") == "Not Applicable" or row.get("logoutTime") == "Not Applicable" or row.get("sessionDuration") == "Not Applicable":
            continue
        
        try:
            username = row["username"]
            machine_name = row["machineName"]
            duration = float(row["sessionDuration"])
            login_date = datetime.strptime(row["loginTime"], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
            
            key = (username, machine_name, login_date)
            
            if key not in day_duration_map:
                day_duration_map[key] = {
                    "username": username,
                    "machine_name": machine_name,
                    "day_session_count": 0,
                    "daily_duration": 0.0,
                    "date": login_date
                }
            
            day_duration_map[key]["day_session_count"] += 1
            day_duration_map[key]["daily_duration"] += duration
        except Exception as e:
            logger.warning(f"Skipping record due to processing error: {e}")
            continue

    combined_day_duration = list(day_duration_map.values())
    combined_day_duration.sort(key=lambda x: (x['date'], x['username']))

    logger.info(f"Aggregated {len(session_reports)} sessions into {len(combined_day_duration)} daily records")
    return combined_day_duration

@activity.defn
async def get_perticular_user_daily_report_activity(daily_reports : list, username:str):
    try:
        user_sessions = [row for row in daily_reports if row["username"] == username]
        return user_sessions
    except Exception as e:
        return ('Error occured on: get_perticular_user_daily_report_activity :- ',e)

@activity.defn
async def get_companies_activity():
    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            select_query = "SELECT company_name, company_logo, report_type FROM reporttemplate;"
            cursor.execute(select_query)
            companies = cursor.fetchall()
            
            companies_data = [
                {
                    "company_name": row[0],
                    "company_logo": base64.b64encode(row[1]).decode("utf-8") if row[1] else None,
                    "report_type": row[2],
                }
                for row in companies
            ]
            
            return companies_data
    except Exception as error:
        return {"msg": "Error occurred", "error": str(error)}
    finally:
        db.close()

@activity.defn
async def get_companies_by_report_type_activity(report_type:str):
    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            select_query = "SELECT company_name, company_logo, report_type FROM reporttemplate WHERE report_type = %s;"
            cursor.execute(select_query, (report_type,))
            companies = cursor.fetchall()
            companies_data = []
            for company in companies:
                company_name = company[0]
                report_type = company[2]
                company_logo_bytes = company[1]
                company_logo_base64 = base64.b64encode(company_logo_bytes).decode('utf-8')
                companies_data.append({
                    "company_name": company_name,
                    "company_logo": company_logo_base64,
                    "report_type": report_type
                })
                
            return companies_data
    except Exception as error:
        return ('error',error)
    finally:
        db.close()

@activity.defn
async def delete_report_activity(report_type:str):
    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            delete_query = "DELETE FROM reporttemplate WHERE report_type = %s;"
            cursor.execute(delete_query, (report_type,))
            if cursor.rowcount == 0:
                raise ValueError("Company not found")
            db.commit()
            return ('Deleted Successfully',)
    except Exception as error:
        return ('error',error)
    finally:
        db.close()

@activity.defn
async def update_report_activity(company_name: str, company_logo: bytes, report_type: str):
    print(f"---------------[update_report_activity] Called with company_name={company_name}, report_type={report_type}, logo_type={type(company_logo)}, logo_len={len(company_logo) if company_logo else 0}")
    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            if report_type not in ["Session Reports", "Daily Reports","Consolidate Reports"]:
                raise ValueError("Invalid report type")

            update_query = """
                UPDATE reporttemplate
                SET company_name = %s, company_logo = %s 
                WHERE report_type = %s;
            """
            cursor.execute(update_query, (company_name, company_logo, report_type))
            print(f"[update_report_activity] UPDATE rowcount={cursor.rowcount}")

            if cursor.rowcount == 0:
                insert_query = "INSERT INTO reporttemplate (company_name, company_logo, report_type) VALUES (%s, %s, %s);"
                cursor.execute(insert_query, (company_name, company_logo, report_type))
                print(f"[update_report_activity] INSERT rowcount={cursor.rowcount}")
            db.commit()
            print(f"[update_report_activity] Committed successfully")

    except Exception as error:
        db.rollback()
        print(f"[update_report_activity] ERROR: {error}")
        raise
    finally:
        db.close()

@activity.defn
async def insert_report_activity(company_name: str, company_logo: bytes, report_type: str):
    print(f"[insert_report_activity] Called with company_name={company_name}, report_type={report_type}, logo_type={type(company_logo)}, logo_len={len(company_logo) if company_logo else 0}")
    db = get_db_connection()
    try:
        with db.cursor() as cursor:
            insert_query = "INSERT INTO reporttemplate (company_name, company_logo, report_type) VALUES (%s, %s, %s);"
            cursor.execute(insert_query, (company_name, company_logo, report_type))
            db.commit()
            print(f"[insert_report_activity] Committed successfully, rowcount={cursor.rowcount}")
    except Exception as error:
        print(f"[insert_report_activity] ERROR: {error}")
        raise error
    finally:
        db.close()


@activity.defn
async def generate_report_activity(start_date: str, end_date: str, report_type: str):

   
   
    pdf_file = f"{report_type.lower().replace(' ', '_')}.pdf"
    logger.info(f"Generating PDF report for type '{report_type}' from {start_date} to {end_date}")
    company_data = await service.get_companies_by_report_type(report_type)
     
    if not company_data or not isinstance(company_data, list) or len(company_data) == 0:
        raise Exception(f"No report template found for report type: '{report_type}'. Please configure a report template first.")
    company_name = company_data[0].get('company_name',"unknown company name")
    logger.info("successfully got {company_name}")
 
    logo_image = None
    try:
        raw_logo = company_data[0].get('company_logo')
        if raw_logo:
            if isinstance(raw_logo, str):
                try:
                    logo_bytes = base64.b64decode(raw_logo)
                except Exception as e:
                    logger.error(f"Error decoding base64 image: {e}")
                    logo_bytes = None
            else:
                logger.warning(f"Unexpected logo data type: {type(raw_logo)}")
                logo_bytes = None

            if logo_bytes:
                try:
                    logo_buffer = BytesIO(logo_bytes)
                    logo_image = logo_buffer
                except Exception as img_error:
                    logger.error(f"Invalid image data: {img_error}")
                    logo_image = None
    except Exception as e:
        logger.error(f"Error processing logo: {e}", exc_info=True)
        logo_image = None
    
    doc = SimpleDocTemplate(
        pdf_file,
        pagesize=A4,
        leftMargin=30,
        rightMargin=30,
        topMargin=30,
        bottomMargin=30
    )
 
    story = []
    styles = getSampleStyleSheet()
 
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Title'],
        fontSize=12,
        alignment=1,
        spaceAfter=20
    )
 
    header_data = [
        [
            Paragraph(company_name, title_style),
            Image(logo_image,  width=100, height=80) if logo_image else Paragraph("", styles['Normal'])
        ],
        [
            Paragraph(f"Date Range: {start_date} - {end_date}", styles['Normal']),
            Paragraph(f"User Name: All Users", styles['Normal']),
        ],
        [
            Paragraph(f"Report Type: {report_type}", styles['Normal']),
            Paragraph(f"Report Date: {datetime.now().strftime('%Y-%m-%d')}", styles['Normal']),
        ]
    ]
 
    column_widths = [doc.width * 0.5, doc.width * 0.5]
 
    header_table = Table(header_data, column_widths)
 
    header_table.setStyle(TableStyle([
        ('GRID', (0, 0), (-1, -1), 1, colors.gray),  # Add grid lines
        ('BACKGROUND', (0, 0), (1, 0), colors.lightgrey),  # Background for header row
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),  # Text color
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),  # Center align content
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  # Vertical alignment
        ('LEFTPADDING', (0, 0), (-1, -1), 10),  # Left padding
        ('RIGHTPADDING', (0, 0), (-1, -1), 10),  # Right padding
        ('TOPPADDING', (0, 0), (-1, -1), 5),  # Top padding
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),  # Bottom padding
    ]))
 
    story.append(header_table)
 
    if  report_type =="Session Reports":
        headers = ["Username", "Login Time", "Logout Time", "Machine Name", "Session Duration"]
        table_data = [headers]
       
        # Add session data
        session_reports = await service.get_session_reports(start_date, end_date)
        for report in session_reports:
            login_time = report.get("loginTime", "")
            logout_time = report.get("logoutTime", "")
           
           
            table_data.append([
                report.get("username", ""),
                service.format_datetime(login_time),
                service.format_datetime(logout_time),
                report.get("machineName", ""),
                service.calculate_duration(report.get("sessionDuration", ""),)
            ])
       
        col_widths = [
            doc.width * 0.2,  # Username
            doc.width * 0.2,  # Login Time
            doc.width * 0.2,  # Logout Time
            doc.width * 0.2,  # Machine Name
            doc.width * 0.2,  # Session Duration
        ]
       
        # Create main table
        main_table = Table(table_data, colWidths=col_widths, repeatRows=1)
        main_table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 1, colors.gray),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        story.append(main_table)
       
    elif report_type =="Daily Reports":
        headers = ["Username", "Machine Name", "Date", "Day Session Count", "Daily Duration"]
        table_data = [headers]
       
        # Add session data
        session_reports = await service.get_session_reports(start_date, end_date)
        logger.info("successfully got session reports")
        daily_reports = await service.get_daily_reports(session_reports)
        logger.info("successfully got daily reports")
       
        for report in daily_reports:         
            table_data.append([
                report.get("username", ""),
                report.get("machine_name", ""),
                report.get("date", ""),
                report.get("day_session_count", ""),          
                service.calculate_duration(report.get("daily_duration", ""))            
            ])
       
        col_widths = [
            doc.width * 0.2,  
            doc.width * 0.2,  
            doc.width * 0.2,  
            doc.width * 0.2,  
            doc.width * 0.2,  
        ]
       
        main_table = Table(table_data, colWidths=col_widths, repeatRows=1)
        main_table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 1, colors.gray),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        story.append(main_table)
    elif report_type =="Consolidate Reports":
        headers = ["Username", "Machine Name", "Day Session Count", "Total Duration"]
        table_data = [headers]
       
        session_reports = await service.get_session_reports(start_date, end_date)
        logger.info("successfully got session_reports")
        daily_reports = await service.get_daily_reports(session_reports)
        logger.info("Successfully got daily reports")
        consolidate_report= await service.get_users_total_duration_within_timerange(daily_reports)
        logger.info("Successfully got consolidate_report")
        for report in consolidate_report:           
            table_data.append([
                report.get("username", ""),
                report.get("machine_name", ""),  
                report.get("day_session_count", ""),
                service.calculate_duration(report.get("total_duration", ""))
 
               
            ])
       
        col_widths = [
            doc.width * 0.3,  
            doc.width * 0.2,  
            doc.width * 0.2,  
            doc.width * 0.3,  
             
        ]
       
        main_table = Table(table_data, colWidths=col_widths, repeatRows=1)
        main_table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 1, colors.gray),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        story.append(main_table)
    else:
        logger.warning(f"No matching report logic found for report type: '{report_type}'")
        error_style = ParagraphStyle('ErrorStyle', parent=styles['Normal'], textColor=colors.red)
        story.append(Paragraph(f"Error: Unsupported report type '{report_type}'", error_style))
   
    def footer(canvas, doc):
        canvas.saveState()
        canvas.setFont('Helvetica', 10)
        footer_text = "Generated By:"
        canvas.drawString(doc.leftMargin, doc.bottomMargin - 20, footer_text)
        from datetime import datetime
        current_time = datetime.now().strftime("Date: %m/%d/%Y %I:%M:%S %p")
        canvas.drawRightString(doc.pagesize[0] - doc.rightMargin,
                             doc.bottomMargin - 20,
                             current_time)
     
        canvas.restoreState()
   
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    return pdf_file
 
@activity.defn
async def generate_user_based_report_activity(start_date: str, end_date: str, report_type: str,username:str):   
   
    pdf_file = f"{report_type,username.lower().replace(' ', '_')}.pdf"
    company_data = await service.get_companies_by_report_type(report_type)
    logger.info("Generating PDF report for company  %s", company_data )
    company_name = company_data[0].get('company_name',"unknown company")
    logo_image = None
    try:
        raw_logo = company_data[0].get('company_logo')
    
        if raw_logo:
            if isinstance(raw_logo, str):
                try:
                    logo_bytes = base64.b64decode(raw_logo)  # Decode the base64 string to bytes
                except Exception as e:
                    logger.error(f"Error decoding base64 image: {e}")
                    logo_bytes = None
            else:
                logger.warning(f"Unexpected logo data type: {type(raw_logo)}")
                logo_bytes = None

            if logo_bytes:
                try:
                    logo_buffer = BytesIO(logo_bytes)
                    logo_image = logo_buffer
                except Exception as img_error:
                    logger.error(f"Invalid image data: {img_error}")
                    logo_image = None
    except Exception as e:
        logger.error(f"Error processing logo: {e}", exc_info=True)
        logo_image = None
    
    
 
    doc = SimpleDocTemplate(
        pdf_file,
        pagesize=A4,
        leftMargin=30,
        rightMargin=30,
        topMargin=30,
        bottomMargin=30
    )
 
    story = []
    styles = getSampleStyleSheet()
 
    # Add title style
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Title'],
        fontSize=12,
        alignment=1,
        spaceAfter=20
    )
 
    header_data = [
        [
            Paragraph(company_name, title_style),
            Image(logo_image, width=100, height=80) if logo_image else ""
        ],
        [
            Paragraph(f"Date Range: {start_date} - {end_date}", styles['Normal']),
            Paragraph(f"User Name: {username}", styles['Normal']),
        ],
        [
            Paragraph(f"Report Type: {report_type}", styles['Normal']),
            Paragraph(f"Report Date: {datetime.now().strftime('%Y-%m-%d')}", styles['Normal']),
        ]
    ]
 
    column_widths = [doc.width * 0.5, doc.width * 0.5]
 
    header_table = Table(header_data, column_widths)
 
    header_table.setStyle(TableStyle([
        ('GRID', (0, 0), (-1, -1), 1, colors.gray),  # Add grid lines
        ('BACKGROUND', (0, 0), (1, 0), colors.lightgrey),  # Background for header row
        ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),  # Text color
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),  # Center align content
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  # Vertical alignment
        ('LEFTPADDING', (0, 0), (-1, -1), 10),  # Left padding
        ('RIGHTPADDING', (0, 0), (-1, -1), 10),  # Right padding
        ('TOPPADDING', (0, 0), (-1, -1), 5),  # Top padding
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),  # Bottom padding
    ]))
 
    story.append(header_table)
 
    if  report_type =="Session Reports":
        headers = ["Username", "Login Time", "Logout Time", "Machine Name", "Session Duration"]
        table_data = [headers]
       
        # Add session data
        session_reports = await service.get_session_reports(start_date, end_date)
        logger.info("Session reports found ")
        session_reports_user= await service.get_perticular_user_sessionreports(session_reports,username)
        logger.info("Session reports for user found ")
        
    
        for report in session_reports_user:
            login_time = report.get("loginTime", "")
            logout_time = report.get("logoutTime", "")
           
           
            table_data.append([
                report.get("username", ""),
                service.format_datetime(login_time),
                service.format_datetime(logout_time),
                report.get("machineName", ""),
                service.calculate_duration(report.get("sessionDuration", ""),)
            ])
       
        # Calculate column widths proportionally
        col_widths = [
            doc.width * 0.2,  # Username
            doc.width * 0.2,  # Login Time
            doc.width * 0.2,  # Logout Time
            doc.width * 0.2,  # Machine Name
            doc.width * 0.2,  # Session Duration
        ]
       
        main_table = Table(table_data, colWidths=col_widths, repeatRows=1)
        main_table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 1, colors.gray),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        story.append(main_table)
   
     
    elif report_type =="Daily Reports":
        headers = ["Username", "Machine Name", "Date", "Day Session Count", "Daily Duration"]
        table_data = [headers]
       
        # Add session data
        session_reports = await service.get_session_reports(start_date, end_date)
        daily_reports = await service.get_daily_reports(session_reports)
        daily_reports_user= await service.get_perticular_user_daily_reports(daily_reports,username)
        
        for report in daily_reports_user:
           
           
            table_data.append([
                report.get("username", ""),
                report.get("machine_name", ""),
                report.get("date", ""),
                report.get("day_session_count", ""),
           
                service.calculate_duration(report.get("daily_duration", ""))
 
               
            ])
        col_widths = [
            doc.width * 0.2,  # Username
            doc.width * 0.2,  # Login Time
            doc.width * 0.2,  # Logout Time
            doc.width * 0.2,  # Machine Name
            doc.width * 0.2,  # Session Duration
        ]
       
         # Create main table
        main_table = Table(table_data, colWidths=col_widths, repeatRows=1)
        main_table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 1, colors.gray),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        story.append(main_table)
    elif report_type =="Consolidate Reports":
        # Prepare table headers and data
        headers = ["Username", "Machine Name", "Day Session Count", "Total Duration"]
        table_data = [headers]
       
        # Add session data
        session_reports = await service.get_session_reports(start_date, end_date)
        daily_reports = await service.get_daily_reports(session_reports)
        consolidate_report= await service.get_users_total_duration_within_timerange(daily_reports)
        consolidate_report_user= service.consolidate_report_perticular_user(consolidate_report,username)
        for report in consolidate_report_user:
           
           
            table_data.append([
                report.get("username", ""),
                report.get("machine_name", ""),  
                report.get("day_session_count", ""),
                service.calculate_duration(report.get("total_duration", ""))
 
               
            ])
       
       
        col_widths = [
            doc.width * 0.3,  
            doc.width * 0.2,  
            doc.width * 0.2,  
            doc.width * 0.3,  
             
        ]
       
       
        main_table = Table(table_data, colWidths=col_widths, repeatRows=1)
        main_table.setStyle(TableStyle([
            ('GRID', (0, 0), (-1, -1), 1, colors.gray),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        story.append(main_table)
   
    def footer(canvas, doc):
        canvas.saveState()
        canvas.setFont('Helvetica', 10)
        footer_text = "Generated By:"
        canvas.drawString(doc.leftMargin, doc.bottomMargin - 20, footer_text)
        from datetime import datetime
        current_time = datetime.now().strftime("Date: %m/%d/%Y %I:%M:%S %p")
        canvas.drawRightString(doc.pagesize[0] - doc.rightMargin,
                             doc.bottomMargin - 20,
                             current_time)
     
        canvas.restoreState()
   
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    return pdf_file



@activity.defn
async def get_users_total_duration_within_timerange_activity(day_duration: List[Dict]):
    user_sessions = []
 
    for entry in day_duration:
        username = entry['username']
        machine_name = entry['machine_name']
        day_session_count = entry['day_session_count']
        daily_duration = entry['daily_duration']
 
        session_found = False
        for session in user_sessions:
            if session['username'] == username and session['machine_name'] == machine_name:
                session['day_session_count'] += day_session_count
                session['total_duration'] += daily_duration
                session_found = True
                break
 
        if not session_found:
            user_sessions.append({
                'username': username,
                'machine_name': machine_name,
                'day_session_count': day_session_count,
                'total_duration': daily_duration
            })
 
    return user_sessions


@activity.defn
async def consolidate_report_perticular_user_activity(user_total_duration, user):
    user_sessions = []
    for row in user_total_duration:
        if row["username"] == user:
            user_sessions.append(row)
    logger.info("user Sessions found: %s", user_sessions)
    return user_sessions


@activity.defn
async def get_guacamole_history_activity():
    try:
        from ...gucamoleService import login_with_guacamole
        guacamole_login = await login_with_guacamole()
        base_url = os.getenv('GUCAMOLE_BASE_URL')
        datasource = os.getenv('GUCAMOLE_DATASOURCE')
        # Build the correct URL
        url = f"{base_url}/api/session/data/{datasource}/history/connections?contains=guac&order=-startDate&token={guacamole_login}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise Exception(f"Failed to load User History: {response.status}")
    except Exception as e:
        return {"error": str(e)}
    

@activity.defn
async def get_guacamole_ActiveSessions_activity():
    try:
        from ...gucamoleService import login_with_guacamole
        guacamole_login = await login_with_guacamole()
        base_url = os.getenv('GUCAMOLE_BASE_URL')
        datasource = os.getenv('GUCAMOLE_DATASOURCE')

        headers = {
            "Cookie": f"GUAC_AUTH={guacamole_login}"
        }

        connections_url = f"{base_url}/api/session/data/{datasource}/connections?token={guacamole_login}"
        async with aiohttp.ClientSession() as session:
            async with session.get(connections_url, headers=headers) as resp_conn:
                if resp_conn.status == 200:
                    connections_data = await resp_conn.json()
                else:
                    raise Exception(f"Failed to load connections: {resp_conn.status}")

            active_url = f"{base_url}/api/session/data/{datasource}/activeConnections?token={guacamole_login}" 
            async with session.get(active_url, headers=headers) as resp_active:
                if resp_active.status == 200:
                    active_data = await resp_active.json()
                else:
                    raise Exception(f"Failed to load activeConnections: {resp_active.status}")

        results = []
        for uuid, session in active_data.items():
            conn_id = session.get("connectionIdentifier")
            conn_info = connections_data.get(conn_id)
            connection_name = conn_info["name"] if conn_info else conn_id
            connection_uuid = conn_info["identifier"] if conn_info and "identifier" in conn_info else uuid

            guac_client_url = f"{base_url}/#/client/activeConnections/{datasource}/{session['identifier']}?token={guacamole_login}"
            results.append({
                "username": session.get("username"),
                "startDate": session.get("startDate"),
                "remoteHost": session.get("remoteHost"),
                "connectionName": connection_name,
                "connectionUUID": connection_uuid,
                "guacClientUrl": guac_client_url,
            })

        return results

    except Exception as e:
        return {"error": str(e)}