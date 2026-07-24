import base64
import logging
from dto.machineDto import MachineDto
import service.gucamoleService as service
from datetime import datetime
from fastapi import  File, UploadFile, Form,Query
from fastapi.responses import FileResponse
from fastapi.encoders import jsonable_encoder
from utils import response_format
from db_configuration.config import SessionLocal
from models.Rbac_models import RBAC

logger = logging.getLogger(__name__)
async def get_login():
    data = await service.login_with_guacamole()
    return response_format.success_response(200, "Successfully authenticated with Guacamole", data)
#testing..

async def  list_of_users():
    data = await service.list_of_users()
    return response_format.success_response(200, "All listed  Guacamole Users", data)



async def list_of_kecloak_users(
    first: int = Query(0, ge=0), 
    limit: int = Query(10, ge=1), 
    search: str = Query("", max_length=100)
):
    db = SessionLocal()
    try:
        keycloak_users = await service.get_userList_from_keycloak(first, limit, search=search)
        if not isinstance(keycloak_users, list):
            keycloak_users = []
        
        return response_format.success_response(200, "All listed Users", keycloak_users)
        
    except Exception as e:
        return response_format.error_response(500, "Failed to list users", str(e))
    finally:
        db.close()


async def list_machines():
    data = await service.list_machines()
    return response_format.success_response(200, "Successfully authenticated with Guacamole", data)


    
#------------------------------------------------- pending not usiing these two methods-------------------------------
async def updataeConnection(machine_data:MachineDto ):
    try:
        data = await service.modify_connection(machine_data)
        return response_format.success_response(200, "Machine updated successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to update connection", str(e))


async def assign_connection_to_user(usernames:list[str], connection:str):
    try:
        data = service.assign_connection_to_user(usernames,connection)
        return response_format.success_response(200, "Machine created successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to assign connection to user", str(e))


#------------------------------------------------- pending not usiing these two methods -------------------------------

def parse_datetime(date_str: str) -> datetime:
    formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%d-%m-%Y %H:%M:%S',"%Y-%m-%d %H:%M:%S.%f"]
    for fmt in formats:
        try:
            return jsonable_encoder(datetime.strptime(date_str, fmt))
        except ValueError:
            continue
    raise ValueError("Invalid date format")

async def get_reports(start_date: str, end_date: str):
    try:
        start_date_dt = parse_datetime(start_date)
        end_date_dt = parse_datetime(end_date)
    except ValueError as e:
        return response_format.error_response(400, "Failed to retrieve session reports", str(e))
    try:
        data = await service.get_session_reports(start_date_dt, end_date_dt)
        return response_format.success_response(200, "Session reports retrieved successfully", data)
    except ValueError as e:
        return response_format.error_response(400,"Failed to retrieve session reports", str(e))


async def get_usernames_endpoint(start_date: str, end_date: str):
    try:
        start_date_dt = parse_datetime(start_date)
        end_date_dt = parse_datetime(end_date)
    except ValueError as e:
        return response_format.error_response(400, "Failed to retrieve user names", str(e))
    try:
        session_reports = await service.get_session_reports(start_date_dt, end_date_dt)
        data = await service.get_users_in_timerange(session_reports)
        return response_format.success_response(200, "User names retrieved successfully", data)
    except ValueError as e:
        return response_format.error_response(400, "Failed to retrieve user names", str(e))


async def get_perticular_user_sessionreports_endpoint(start_date: str, end_date: str, username: str):
    try:
        start_date_dt = parse_datetime(start_date)
        end_date_dt = parse_datetime(end_date)
    except ValueError as e:
        return response_format.error_response(400, "Failed to retrieve session reports", str(e))
    try:
        session_reports = await service.get_session_reports(start_date_dt, end_date_dt)
        data = await service.get_perticular_user_sessionreports(session_reports, username)
        return response_format.success_response(200, "User session reports retrieved successfully", data)
    except ValueError as e:
        return response_format.error_response(400, "Failed to retrieve user session reports", str(e))


async def get_day_reports(start_date: str, end_date: str):
    try:
        start_date_dt = parse_datetime(start_date)
        end_date_dt = parse_datetime(end_date)
    except ValueError as e:
        return response_format.error_response(400, "Failed to retrieve daily reports", str(e))
    try:
        session_reports = await service.get_session_reports(start_date_dt, end_date_dt)
        data = await service.get_daily_reports(session_reports)
        return response_format.success_response(200, "Daily reports retrieved successfully", data)
    except ValueError as e:
        return response_format.error_response(400, "Failed to retrieve daily reports", str(e))



async def get_daily_reports_of_each_user(start_date: str, end_date: str, username: str):
    try:
        start_date_dt = parse_datetime(start_date)
        end_date_dt = parse_datetime(end_date)
    except ValueError as e:
        return response_format.error_response(400, "Failed to retrieve daily reports", str(e))
    try:
        session_reports = await service.get_session_reports(start_date_dt, end_date_dt)
        daily_reports = await service.get_daily_reports(session_reports)
        data = await service.get_perticular_user_daily_reports(daily_reports, username)
        return response_format.success_response(200, "User daily reports retrieved successfully", data)
    except ValueError as e:
        return response_format.error_response(400, "Failed to retrieve user daily reports", str(e))

async def fetch_companies():
    try:
        data = await service.get_companies()
        return  response_format.success_response(200, "Companies retrieved successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Internal server error", str(e))


async def read_companies_by_report_type(report_type: str ):
    try:
        data = await service.get_companies_by_report_type(report_type)
        return response_format.success_response(200, "Companies retrieved successfully", data)
    except Exception as error:
        return response_format.error_response(500, "Internal Server Error", str(error))


async def update_company(
    company_name,
    company_logo,
    report_type,
):
    MAX_LOGO_SIZE = 2 * 1024 * 1024  # 10MB
    ALLOWED_CONTENT_TYPES = ["image/jpeg", "image/jpg", "image/png", "image/svg+xml"]
    try:
        company_name_str = str(company_name)
        report_type_str = str(report_type)

        company_logo_base64 = None
        if isinstance(company_logo, UploadFile):
            # Validate file type
            if company_logo.content_type not in ALLOWED_CONTENT_TYPES:
                return response_format.error_response(
                    400,
                    "Invalid file type",
                    f"Only JPEG, JPG, PNG, and SVG files are allowed. Got: {company_logo.content_type}"
                )
            # Read and validate file size
            company_logo_bytes = await company_logo.read()
            if len(company_logo_bytes) > MAX_LOGO_SIZE:
                return response_format.error_response(
                    400,
                    "File too large",
                    "Company logo must be less than 2MB"
                )
            company_logo_base64 = base64.b64encode(company_logo_bytes).decode("utf-8")
        elif isinstance(company_logo, str):
            company_logo_base64 = company_logo  # already base64
        if report_type_str in ["Session Reports","Daily Reports","Consolidate Reports"]:
            await service.update_report(company_name_str, company_logo_base64, report_type_str)
            return response_format.success_response(200, "Company updated successfully", None)
        else:
            await service.insert_report(company_name_str, company_logo_base64, report_type_str)
            return response_format.success_response(200, "Company added successfully", None)

    # except ValueError as ve:
    #     return response_format.error_response(404, "Failed to update company", str(ve))
    except Exception as e:
        return response_format.error_response(500, "Internal server error", str(e))


async def delete_company_data(report_name: str ):
    try:
        await service.delete_report(report_name)
        return response_format.success_response(200, "Company deleted successfully", None)
    except ValueError as ve:
        return response_format.error_response(404, "Failed to delete company", str(ve))
    except Exception as e:
        return response_format.error_response(500, "Internal server error", str(e))


async def get_users_total_duration_within_timerange_endpoint(start_date: str, end_date: str):
    start_date_dt = parse_datetime(start_date)
    end_date_dt = parse_datetime(end_date)
    try:
        session_reports = await service.get_session_reports(start_date_dt, end_date_dt)
        daily_reports = await service.get_daily_reports(session_reports)
        data = await service.get_users_total_duration_within_timerange(daily_reports)
      
        return response_format.success_response(200, "User total durations retrieved successfully", data)
    except Exception as error:
        return response_format.error_response(500, "Internal server error", str(error))


async def get_perticular_users_total_duration_within_timerange_endpoint(start_date: str, end_date: str,user: str):
    start_date_dt = parse_datetime(start_date)
    end_date_dt = parse_datetime(end_date)
    try:
        session_reports =await service.get_session_reports(start_date_dt, end_date_dt)
        daily_reports = await service.get_daily_reports(session_reports)
        user_total_duration =await service.get_users_total_duration_within_timerange(daily_reports)
        consolidat_user= await service.consolidate_report_perticular_user(user_total_duration,user)
       
        return response_format.success_response(200, "User total durations retrieved successfully", consolidat_user)
    except Exception as error:
        return response_format.error_response(500, "Internal server error", str(error))


async def generate_pdf_report(start_date: str, end_date: str, report_type: str):

    start_date_dt = parse_datetime(start_date)
    end_date_dt = parse_datetime(end_date)
    try:
        pdf_path = await service.generate_report(start_date_dt, end_date_dt, report_type)
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
        encoded_pdf = base64.b64encode(pdf_bytes).decode("utf-8")

        return response_format.success_response(
            200,
            "PDF report generated successfully",
            {"pdf_data": encoded_pdf}
        )
    except Exception as e:
        return response_format.error_response(500, "Failed to generate PDF report", str(e))


async def generate_pdf_report_by_username(start_date: str, end_date: str, report_type: str,username: str):
    """
    API Endpoint to generate a PDF session report.
    """
   
    start_date_dt = parse_datetime(start_date)
    end_date_dt = parse_datetime(end_date)
    try:
        pdf_path = await service.generate_report(start_date_dt, end_date_dt, report_type)
        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()
        encoded_pdf = base64.b64encode(pdf_bytes).decode("utf-8")

        return response_format.success_response(
            200,
            "PDF report generated successfully",
            {"pdf_data": encoded_pdf}
        )
    except Exception as e:
        return response_format.error_response(500, "Failed to generate PDF report", str(e)) 

    

async def  get_client_id():
    try:
        data = await service.get_client() 
         
        return response_format.success_response(200, "Client ID retrieved successfully", data)
    except Exception as e:
        raise response_format.error_response(500, "Failed to retrieve client ID", str(e))


async def get_client_roles(request):
    db = SessionLocal()
    try:
        keycloak_roles = []

        try:
            keycloak_data = await service.get_keycloak_roles()
            if keycloak_data:
                ignored_roles = {"uma_authorization", "offline_access"}
                
                keycloak_roles = [
                    role.get("name") for role in keycloak_data 
                    if isinstance(role, dict) and role.get("name")
                    and role.get("name") not in ignored_roles
                    and not role.get("name").startswith("default-roles-")
                ]
        except Exception as e:
            logger.warning(f"Failed to fetch roles from Keycloak: {e}")

        db_roles = db.query(RBAC.role).all()
        role_list = [role[0] for role in db_roles]
        
        try:
            keycloak_data = await service.get_client_roles()
            keycloak_roles = [role["name"] for role in keycloak_data]
            role_list = list(set(role_list + keycloak_roles))
        except Exception as e:
            logger.warning(f"Failed to fetch roles from Keycloak: {e}")
        
        return response_format.success_response(200, "Role names retrieved successfully", role_list)
    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve roles", str(e))
    finally:
        db.close()


async def post_role(role_name: str, authorization: str):
    try:
        data = await service.posting_role(role_name, authorization)
        return response_format.success_response(201, "Role created successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to create role", str(e))
    

async def delete_role(role_name: str, authorization: str):
    try:
        data = await service.deleting_role(role_name, authorization)
        return response_format.success_response(200, "Role deleted successfully", data)

    except Exception as e:
        return response_format.error_response(500, "Failed to delete role", str(e))


async def submit_role_components(request, authorization):
    try:
        data = await service.updating_role_component(request, authorization)
        return response_format.success_response(200, "Role components submitted successfully", data)

    except Exception as e:
        return response_format.error_response(500, "Failed to submit role components", str(e))


async def get_role_components(role: str):
    try:
        data = await service.getting_role_component(role)
        return response_format.success_response(200, "Role components retrieved successfully", data)

    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve role components", str(e))


async def assign_user_role(request):
    try:
        await service.assignning_user_role(request)
        return response_format.success_response(200, "Role assigned to user successfully")
    except Exception as e:
        return response_format.error_response(500, "Failed to assign role to user", str(e))


async def get_user_permissions(request, username: str):
    try:
        data = await service.get_user_permissions(request, username)
        filtered_data = {
            "components": data.get("components", []),
            "roles": data.get("roles", [])
        }
        return response_format.success_response(200, "User permissions retrieved successfully", filtered_data)

    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve user permissions", str(e))


async def remove_role_from_user(request):
    data = await service.delete_role_from_user(request)
    return response_format.success_response(200, data['msg'])



async def get_guacamole_history():
    try:
        data = await service.get_guacamole_history()
        return response_format.success_response(200, "Guacamole history retrieved successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve guacamole history", str(e))


async def get_guacamole_active_sessions():
    try:
        data = await service.get_guacamole_ActiveSessions()
        return response_format.success_response(200, "Guacamole active sessions retrieved successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve guacamole active sessions", str(e))


async def guacamole_join_session(
    session_id: str = Query(...),
    datasource: str = Query(None)
):
    try:
        data = await service.generate_guacamole_session_url(session_id, datasource)
        return response_format.success_response(200, "Guacamole session URL generated successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to generate guacamole session URL", str(e))


async def get_recording_log(identifier: str, logUuid: str):
    try:
        data = await service.get_recording_log(identifier, logUuid)
        return response_format.success_response(200, "Recording log retrieved successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve recording log", str(e))

           
