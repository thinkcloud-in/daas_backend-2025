"""
Guacamole (remote-desktop gateway) admin/reporting/RBAC controller —
router/guacamole_router.py ("/v1/guacamole") delegates to this. The actual
Guacamole REST API / Keycloak Admin API calls are in
service/gucamoleService.py.
"""
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
    """Log in with the Guacamole service account and get an auth token (internal use — caches/refreshes it)."""
    data = await service.login_with_guacamole()
    return response_format.success_response(200, "Successfully authenticated with Guacamole", data)
#testing..

async def  list_of_users():
    """List users created directly in the Guacamole DB (legacy users, pre-Keycloak-sync). Used by: GET /v1/guacamole/list_guaca_users"""
    data = await service.list_of_users()
    return response_format.success_response(200, "All listed  Guacamole Users", data)



async def list_of_kecloak_users(
    first: int = Query(0, ge=0),
    limit: int = Query(10, ge=1),
    search: str = Query("", max_length=100)
):
    """
    List Keycloak users (paginated, searchable by name).

    Used by: GET /v1/guacamole/list_users
    Returns: success_response's `data` has [ {..Keycloak user..}, ... ]
    (the Keycloak Admin API shape as-is).
    """
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
    """List all configured RDP/SSH/VNC connections in Guacamole. Used by: GET /v1/guacamole/list_connections"""
    data = await service.list_machines()
    return response_format.success_response(200, "Successfully authenticated with Guacamole", data)



#------------------------------------------------- pending not usiing these two methods-------------------------------
async def updataeConnection(machine_data:MachineDto ):
    """Update Guacamole connection settings. Used by: POST /v1/guacamole/update_connection (⚠ currently unused/pending per original comment)."""
    try:
        data = await service.modify_connection(machine_data)
        return response_format.success_response(200, "Machine updated successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to update connection", str(e))


async def assign_connection_to_user(usernames:list[str], connection:str):
    """Assign a connection to users. Used by: PATCH /v1/guacamole/assign_connection_to_user (⚠ currently unused/pending per original comment)."""
    try:
        data = service.assign_connection_to_user(usernames,connection)
        return response_format.success_response(200, "Machine created successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to assign connection to user", str(e))


#------------------------------------------------- pending not usiing these two methods -------------------------------

def parse_datetime(date_str: str) -> datetime:
    """Try several possible date formats to parse the string into a datetime. Raises ValueError if none match."""
    formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%d-%m-%Y %H:%M:%S',"%Y-%m-%d %H:%M:%S.%f"]
    for fmt in formats:
        try:
            return jsonable_encoder(datetime.strptime(date_str, fmt))
        except ValueError:
            continue
    raise ValueError("Invalid date format")

async def get_reports(start_date: str, end_date: str):
    """
    Get all session reports (login/logout, duration) for a date range.

    Used by: GET /v1/guacamole/generate_reports/vamanit_session_reports/{start_date}/{end_date}
    Errors: 400 (error_response) if the date format is invalid.
    """
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
    """
    Get the list of all usernames with activity in a date range.

    Used by: GET /v1/guacamole/generate_reports/vamanit_allusers/{start_date}/{end_date}
    """
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
    """
    Get session reports for one specific user, for a date range.

    Used by: GET /v1/guacamole/generate_reports/vamanit_session_reports/{start_date}/{end_date}/{username}
    """
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
    """
    Get a day-wise summary report within a date range.

    Used by: GET /v1/guacamole/generate_reports/day_reports/{start_date}/{end_date}
    """
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
    """
    Get one user's day-wise session report for a date range.

    Used by: GET /v1/guacamole/generate_reports/day_reports/{start_date}/{end_date}/{username}
    """
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
    """List saved companies (name+logo) used for report branding. Used by: GET /v1/guacamole/reports"""
    try:
        data = await service.get_companies()
        return  response_format.success_response(200, "Companies retrieved successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Internal server error", str(e))


async def read_companies_by_report_type(report_type: str ):
    """Get a company's saved report config, by report_type. Used by: GET /v1/guacamole/generate_reports/{report_type}"""
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
    """
    Create/update company branding (name+logo) for a report_type (updates
    if the report_type already exists, otherwise inserts a new one).

    Used by: POST /v1/guacamole/reports/update_report
    Args: company_logo — an UploadFile (jpeg/jpg/png/svg, max 2MB) or an already-base64 string.
    Returns: success_response(200, ..., None) — data is always null.
    Errors: 400 invalid file type/too large, 500 unexpected error.
    """
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
    """Delete a company branding record. Used by: DELETE /v1/guacamole/delete_company/{report_name}. Errors: 404 if not found."""
    try:
        await service.delete_report(report_name)
        return response_format.success_response(200, "Company deleted successfully", None)
    except ValueError as ve:
        return response_format.error_response(404, "Failed to delete company", str(ve))
    except Exception as e:
        return response_format.error_response(500, "Internal server error", str(e))


async def get_users_total_duration_within_timerange_endpoint(start_date: str, end_date: str):
    """Get the total connected duration (sum) for all users in a date range. Used by: GET /v1/guacamole/generate_reports/total_durations_within_range/{start_date}/{end_date}"""
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
    """Get one user's total connected duration for a date range. Used by: GET /v1/guacamole/generate_reports/total_durations_within_range/{start_date}/{end_date}/{user}"""
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
    """
    Generate a PDF report for all users (base64-encoded).

    Used by: POST /v1/guacamole/generate_report/{start_date}/{end_date}/{report_type}
    Returns: success_response's `data` has {"pdf_data": str}  (base64 PDF bytes).
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


async def generate_pdf_report_by_username(start_date: str, end_date: str, report_type: str,username: str):
    """
    Generate a PDF report for one user (base64-encoded).

    Used by: POST /v1/guacamole/generate_report/{start_date}/{end_date}/{report_type}/{username}
    Returns: success_response's `data` has {"pdf_data": str}.
    NOTE: `username` is currently unused anywhere inside this function — it's
    never passed to `service.generate_report()` (the all-users report is
    generated instead) — this is a known limitation at the router level.
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
    """Get this app's OAuth client's internal ID in Keycloak. Used by: GET /v1/guacamole/get_client_id"""
    try:
        data = await service.get_client()

        return response_format.success_response(200, "Client ID retrieved successfully", data)
    except Exception as e:
        raise response_format.error_response(500, "Failed to retrieve client ID", str(e))


async def get_client_roles(request):
    """
    Union list of Keycloak client roles + local `rbac_table` roles (system
    default roles like "uma_authorization"/"offline_access"/
    "default-roles-*" are excluded).

    Used by: GET /v1/guacamole/get_client_roles
    Returns: success_response's `data` has [str, ...] (role names).
    """
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
    """Create a new role — synced into both the Keycloak client-role and the local rbac_table. Used by: POST /v1/guacamole/post_role/{role_name}"""
    try:
        data = await service.posting_role(role_name, authorization)
        return response_format.success_response(201, "Role created successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to create role", str(e))


async def delete_role(role_name: str, authorization: str):
    """Delete a role — from both Keycloak and the local DB. Used by: DELETE /v1/guacamole/delete_role/{role_name}"""
    try:
        data = await service.deleting_role(role_name, authorization)
        return response_format.success_response(200, "Role deleted successfully", data)

    except Exception as e:
        return response_format.error_response(500, "Failed to delete role", str(e))


async def submit_role_components(request, authorization):
    """Assign sidebar/UI components (permissions) to a role. Used by: POST /v1/guacamole/submit_role_components"""
    try:
        data = await service.updating_role_component(request, authorization)
        return response_format.success_response(200, "Role components submitted successfully", data)

    except Exception as e:
        return response_format.error_response(500, "Failed to submit role components", str(e))


async def get_role_components(role: str):
    """List the components assigned to a role. Used by: GET /v1/guacamole/get_role_components/{role}"""
    try:
        data = await service.getting_role_component(role)
        return response_format.success_response(200, "Role components retrieved successfully", data)

    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve role components", str(e))


async def assign_user_role(request):
    """Assign a role to user(s) — Keycloak + local DB. Used by: POST /v1/guacamole/assign_user_role. Response `data` is always None (only msg matters)."""
    try:
        result = await service.assignning_user_role(request)
        if result.get("status") == "Error" or result.get("status_code"):
            return response_format.error_response(
                result.get("status_code", 500),
                result.get("detail") or result.get("message") or "Failed to assign role to user",
            )
        return response_format.success_response(200, "Role assigned to user successfully")
    except Exception as e:
        return response_format.error_response(500, "Failed to assign role to user", str(e))


async def get_user_permissions(request, username: str):
    """
    Get a user's roles + combined components (permissions) — this is what
    the frontend calls to render the sidebar/menu.

    Used by: GET /v1/guacamole/get_user_permissions/{username}
    Returns: success_response's `data` has {"components": [str, ...], "roles": [str, ...]}
    """
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
    """Remove a role from a user. Used by: DELETE /v1/guacamole/remove_role_from_user. Response `data` is always None."""
    data = await service.delete_role_from_user(request)
    return response_format.success_response(200, data['msg'])



async def get_guacamole_history():
    """List all Guacamole connection-history records. Used by: GET /v1/guacamole/guacamole_history"""
    try:
        data = await service.get_guacamole_history()
        return response_format.success_response(200, "Guacamole history retrieved successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve guacamole history", str(e))


async def get_guacamole_active_sessions():
    """List currently-active Guacamole sessions. Used by: GET /v1/guacamole/guacamole_ActiveSessions"""
    try:
        data = await service.get_guacamole_ActiveSessions()
        return response_format.success_response(200, "Guacamole active sessions retrieved successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve guacamole active sessions", str(e))


async def guacamole_join_session(
    session_id: str = Query(...),
    datasource: str = Query(None)
):
    """
    Get the access info an admin needs to "shadow/join" an active session.

    Used by: GET /v1/guacamole/guacamole_join_session
    ⚠ The router (`guacamole_router.py`) calls this function with no params
    at all (`return await guacamole_controller.guacamole_join_session()`) —
    this module-level function's own `Query(...)` defaults would only work
    if it were itself a FastAPI route. As currently wired, `session_id`
    stays a `Query` object, not the actual query-string value — this
    endpoint looks BROKEN as-is. This is a documentation-only pass, not fixed.
    """
    try:
        data = await service.generate_guacamole_session_url(session_id, datasource)
        return response_format.success_response(200, "Guacamole session URL generated successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to generate guacamole session URL", str(e))


async def get_recording_log(identifier: str, logUuid: str):
    """Fetch a session-recording's playback log. Used by: GET /v1/guacamole/api/recording/{identifier}/{logUuid}"""
    try:
        data = await service.get_recording_log(identifier, logUuid)
        return response_format.success_response(200, "Recording log retrieved successfully", data)
    except Exception as e:
        return response_format.error_response(500, "Failed to retrieve recording log", str(e))
