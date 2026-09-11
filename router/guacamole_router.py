from fastapi import APIRouter, Form, File, Header, UploadFile, Query, Request, Depends
from typing import Any, List, Optional, Union
from controllers import guacamole_controller
from models.API_Response_model import APIResponse
from models.Rbac_models import RoleComponentSubmitRequest,RBACRequest
from starlette.formparsers import MultiPartParser

# Top of file, right after the imports
MultiPartParser.max_part_size = 20 * 1024 * 1024
guacamole_router = APIRouter(prefix="/v1/guacamole", tags=["guacamole"])

# NOTE: This router is the Guacamole (remote-desktop gateway) admin/reporting/RBAC
# layer. `data` shapes come directly from the Guacamole REST API / Keycloak Admin API
# JSON responses (this backend just proxies + injects the auth header), so the
# field-by-field shape depends on those upstream systems — below, each endpoint
# notes "which system" it talks to and "what it's for".

# ---------------------- BASIC ROUTES ----------------------
@guacamole_router.get("/login", response_model=APIResponse[Any])
async def login():
    """Log in with the service-account for Guacamole and get an auth token (internal use — caches/refreshes it)."""
    return await guacamole_controller.get_login()

@guacamole_router.get("/list_guaca_users", response_model=APIResponse[Any])
async def list_guaca_users():
    """List users created directly in the Guacamole DB (legacy users, pre-Keycloak-sync)."""
    return await guacamole_controller.list_of_users()

@guacamole_router.get("/list_users", response_model=APIResponse[Any])
async def list_users(
    first: int = Query(0, ge=0),
    limit: int = Query(10, ge=1),
    search: str = Query("", max_length=100)
):
    """
    List Keycloak users (paginated, searchable by name).

    Response `data`: [ {"username": str, "email": str, "userid": str, ...}, ... ]  (Keycloak Admin API shape)
    """
    return await guacamole_controller.list_of_kecloak_users(first=first, limit=limit, search=search)

@guacamole_router.get("/list_connections", response_model=APIResponse[Any])
async def list_connections():
    """List all configured RDP/SSH/VNC connections (machines) in Guacamole."""
    return await guacamole_controller.list_machines()

@guacamole_router.post("/update_connection", response_model=APIResponse[Any])
async def update_connection():
    """Update/sync Guacamole connection settings."""
    return await guacamole_controller.updataeConnection()

@guacamole_router.patch("/assign_connection_to_user", response_model=APIResponse[Any])
async def assign_connection_to_user():
    """Assign a Guacamole connection (machine) to a user (grant access)."""
    return await guacamole_controller.assign_connection_to_user()

@guacamole_router.get("/generate_reports/vamanit_session_reports/{start_date}/{end_date}", response_model=APIResponse[Any])
async def vamanit_session_reports(start_date: str, end_date: str):
    """
    Get session reports (login/logout, duration) for all users in the given date range.

    Path params: start_date, end_date — format "YYYY-MM-DD".
    Response `data`: [ {"username": str, "start_time": str, "duration": str, ...}, ... ]
    """
    return await guacamole_controller.get_reports(start_date, end_date)

@guacamole_router.get("/generate_reports/vamanit_allusers/{start_date}/{end_date}", response_model=APIResponse[Any])
async def vamanit_allusers(start_date: str, end_date: str):
    """Get the list of all usernames with activity in the date range."""
    return await guacamole_controller.get_usernames_endpoint(start_date, end_date)

@guacamole_router.get("/generate_reports/vamanit_session_reports/{start_date}/{end_date}/{username}", response_model=APIResponse[Any])
async def user_session_reports(start_date: str, end_date: str, username: str):
    """Get session reports for one specific user, for the date range."""
    return await guacamole_controller.get_perticular_user_sessionreports_endpoint(start_date, end_date, username)

@guacamole_router.get("/generate_reports/day_reports/{start_date}/{end_date}", response_model=APIResponse[Any])
async def day_reports(start_date: str, end_date: str):
    """Get a day-wise (per-day summary) session report within the date range."""
    return await guacamole_controller.get_day_reports(start_date, end_date)

@guacamole_router.get("/generate_reports/day_reports/{start_date}/{end_date}/{username}", response_model=APIResponse[Any])
async def daily_reports_of_user(start_date: str, end_date: str, username: str):
    """Get one user's day-wise session report for the date range."""
    return await guacamole_controller.get_daily_reports_of_each_user(start_date, end_date, username)

@guacamole_router.get("/reports", response_model=APIResponse[Any])
async def fetch_companies():
    """
    List saved companies (name + logo) used for report branding —
    for white-labeled PDF reports.
    """
    return await guacamole_controller.fetch_companies()

@guacamole_router.get("/generate_reports/{report_type}", response_model=APIResponse[Any])
async def reports_by_type(report_type: str):
    """Get a company's saved report config, by report_type (e.g. "vamanit")."""
    return await guacamole_controller.read_companies_by_report_type(report_type)

@guacamole_router.post("/reports/update_report", response_model=APIResponse[Any])
async def update_report(
    company_name: str = Form(...),
    company_logo: Union[UploadFile, str, None] = File(...),
    report_type: str = Form(...),
):
    """
    Create/update company branding (name + logo) for a report type.

    Request: multipart/form-data — `company_name`, `company_logo` (a file or
    an existing-logo-path string), `report_type`.
    """
    return await guacamole_controller.update_company(company_name, company_logo, report_type)

@guacamole_router.delete("/delete_company/{report_name}", response_model=APIResponse[Any])
async def delete_company(report_name: str):
    """Delete a company branding record."""
    return await guacamole_controller.delete_company_data(report_name)

@guacamole_router.get("/generate_reports/total_durations_within_range/{start_date}/{end_date}", response_model=APIResponse[Any])
async def total_durations(start_date: str, end_date: str):
    """Get the total connected duration (sum) for all users within the date range."""
    return await guacamole_controller.get_users_total_duration_within_timerange_endpoint(start_date, end_date)

@guacamole_router.get("/generate_reports/total_durations_within_range/{start_date}/{end_date}/{user}", response_model=APIResponse[Any])
async def total_durations_user(start_date: str, end_date: str, user: str):
    """Get one user's total connected duration for the date range."""
    return await guacamole_controller.get_perticular_users_total_duration_within_timerange_endpoint(start_date, end_date, user)

@guacamole_router.post("/generate_report/{start_date}/{end_date}/{report_type}", response_model=APIResponse[Any])
async def generate_report(start_date: str, end_date: str, report_type: str):
    """
    Generate a PDF report for all users, with the given branding (`report_type`).

    Response `data`: {"file_url": str, ...} — download URL or path.
    """
    return await guacamole_controller.generate_pdf_report(start_date, end_date, report_type)

@guacamole_router.post("/generate_report/{start_date}/{end_date}/{report_type}/{username}", response_model=APIResponse[Any])
async def generate_report_user(start_date: str, end_date: str, report_type: str, username: str):
    """Generate a PDF report for one user."""
    return await guacamole_controller.generate_pdf_report_by_username(start_date, end_date, report_type, username)

@guacamole_router.get("/get_client_id", response_model=APIResponse[Any])
async def get_client_id():
    """Get this app's OAuth client's internal ID in Keycloak (for RBAC setup)."""
    return await guacamole_controller.get_client_id()

@guacamole_router.get("/get_client_roles", response_model=APIResponse[List[str]])
async def get_client_roles(request: Request):
    """
    List all roles defined on the Keycloak client (e.g. "admin", "viewer").

    Response `data`: ["role1", "role2", ...]
    """
    return await guacamole_controller.get_client_roles(request)

@guacamole_router.post("/post_role/{role_name}")
async def post_role(role_name: str, authorization: Optional[str] = Header(None)):
    """
    Create a new role — synced into both Keycloak (client role) and the
    local `rbac_table`.

    Header: Authorization (Bearer token, for the Keycloak admin call).
    """
    return await guacamole_controller.post_role(role_name, authorization)

@guacamole_router.delete("/delete_role/{role_name}")
async def delete_role(role_name: str, authorization: Optional[str] = Header(None)):
    """Delete a role — from both Keycloak and the local DB."""
    return await guacamole_controller.delete_role(role_name, authorization)

@guacamole_router.post("/submit_role_components")
async def submit_role_components(request: RoleComponentSubmitRequest, authorization: Optional[str] = Header(None)):
    """
    Assign sidebar/UI "components" (permissions) to a role — updates both
    the Keycloak role attributes and the local DB.

    Request body: RoleComponentSubmitRequest = {"role": str, "components": [str, ...]}
    """
    return await guacamole_controller.submit_role_components(request, authorization)

@guacamole_router.get("/get_role_components/{role}")
async def get_role_components(role: str):
    """
    List the components (permissions) assigned to a role.

    Response `data`: {"components": [str, ...]}
    """
    return await guacamole_controller.get_role_components(role)

@guacamole_router.post("/assign_user_role")
async def assign_user_role(request: RBACRequest):
    """
    Assign a role to one or more users (Keycloak + local DB).

    Request body: RBACRequest = {"username": [str, ...], "role": str, "components": [str, ...]}
    """
    return await guacamole_controller.assign_user_role(request)

@guacamole_router.get("/get_user_permissions/{username}")
async def get_user_permissions(request: Request, username: str):
    """
    Get all of a user's roles plus the combined components (permissions)
    those roles grant — this is what the frontend calls to render the
    sidebar/menu.

    Response `data`: {"roles": [str, ...], "components": [str, ...]}
    """
    return await guacamole_controller.get_user_permissions(request, username)

@guacamole_router.delete("/remove_role_from_user", response_model=APIResponse[Any])
async def remove_role_from_user(request: RBACRequest):
    """Remove a role from a user (Keycloak + local DB)."""
    return await guacamole_controller.remove_role_from_user(request)

@guacamole_router.get("/guacamole_history", response_model=APIResponse[Any])
async def guacamole_history():
    """List all Guacamole connection-history records (past sessions)."""
    return await guacamole_controller.get_guacamole_history()

@guacamole_router.get("/guacamole_ActiveSessions", response_model=APIResponse[Any])
async def guacamole_active_sessions():
    """
    List currently-active (live) Guacamole sessions — who is connected, on which
    machine, and since when.
    """
    return await guacamole_controller.get_guacamole_active_sessions()

@guacamole_router.get("/guacamole_join_session", response_model=APIResponse[Any])
async def guacamole_join_session():
    """Get the access info an admin needs to "shadow/join" an active session."""
    return await guacamole_controller.guacamole_join_session()

@guacamole_router.get("/api/recording/{identifier}/{logUuid}", response_model=APIResponse[Any])
async def recording_log(identifier: str, logUuid: str):
    """
    Fetch a session-recording's playback log (Guacamole's recording format,
    for screen-recording replay).
    """
    return await guacamole_controller.get_recording_log(identifier, logUuid)
