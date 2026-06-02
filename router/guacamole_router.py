from fastapi import APIRouter, Form, File, Header, UploadFile, Query, Request, Depends
from typing import Any, List, Optional, Union
from controllers import guacamole_controller
from models.API_Response_model import APIResponse
from models.Rbac_models import RoleComponentSubmitRequest,RBACRequest
from starlette.formparsers import MultiPartParser

# File top pe — import ke baad
MultiPartParser.max_part_size = 20 * 1024 * 1024
guacamole_router = APIRouter(prefix="/v1/guacamole", tags=["guacamole"])

# ---------------------- BASIC ROUTES ----------------------
@guacamole_router.get("/login", response_model=APIResponse[Any])
async def login():
    return await guacamole_controller.get_login()

@guacamole_router.get("/list_guaca_users", response_model=APIResponse[Any])
async def list_guaca_users():
    return await guacamole_controller.list_of_users()

@guacamole_router.get("/list_users", response_model=APIResponse[Any])
async def list_users():
    return await guacamole_controller.list_of_kecloak_users()

@guacamole_router.get("/list_connections", response_model=APIResponse[Any])
async def list_connections():
    return await guacamole_controller.list_machines()

@guacamole_router.post("/update_connection", response_model=APIResponse[Any])
async def update_connection():
    return await guacamole_controller.updataeConnection()

@guacamole_router.patch("/assign_connection_to_user", response_model=APIResponse[Any])
async def assign_connection_to_user():
    return await guacamole_controller.assign_connection_to_user()

@guacamole_router.get("/generate_reports/vamanit_session_reports/{start_date}/{end_date}", response_model=APIResponse[Any])
async def vamanit_session_reports(start_date: str, end_date: str):
    return await guacamole_controller.get_reports(start_date, end_date)

@guacamole_router.get("/generate_reports/vamanit_allusers/{start_date}/{end_date}", response_model=APIResponse[Any])
async def vamanit_allusers(start_date: str, end_date: str):
    return await guacamole_controller.get_usernames_endpoint(start_date, end_date)

@guacamole_router.get("/generate_reports/vamanit_session_reports/{start_date}/{end_date}/{username}", response_model=APIResponse[Any])
async def user_session_reports(start_date: str, end_date: str, username: str):
    return await guacamole_controller.get_perticular_user_sessionreports_endpoint(start_date, end_date, username)

@guacamole_router.get("/generate_reports/day_reports/{start_date}/{end_date}", response_model=APIResponse[Any])
async def day_reports(start_date: str, end_date: str):
    return await guacamole_controller.get_day_reports(start_date, end_date)

@guacamole_router.get("/generate_reports/day_reports/{start_date}/{end_date}/{username}", response_model=APIResponse[Any])
async def daily_reports_of_user(start_date: str, end_date: str, username: str):
    return await guacamole_controller.get_daily_reports_of_each_user(start_date, end_date, username)

@guacamole_router.get("/reports", response_model=APIResponse[Any])
async def fetch_companies():
    return await guacamole_controller.fetch_companies()

@guacamole_router.get("/generate_reports/{report_type}", response_model=APIResponse[Any])
async def reports_by_type(report_type: str):
    return await guacamole_controller.read_companies_by_report_type(report_type)

@guacamole_router.post("/reports/update_report", response_model=APIResponse[Any])
async def update_report(
    company_name: str = Form(...),
    company_logo: Union[UploadFile, str, None] = File(...),
    report_type: str = Form(...),
):
    return await guacamole_controller.update_company(company_name, company_logo, report_type)

@guacamole_router.delete("/delete_company/{report_name}", response_model=APIResponse[Any])
async def delete_company(report_name: str):
    return await guacamole_controller.delete_company_data(report_name)

@guacamole_router.get("/generate_reports/total_durations_within_range/{start_date}/{end_date}", response_model=APIResponse[Any])
async def total_durations(start_date: str, end_date: str):
    return await guacamole_controller.get_users_total_duration_within_timerange_endpoint(start_date, end_date)

@guacamole_router.get("/generate_reports/total_durations_within_range/{start_date}/{end_date}/{user}", response_model=APIResponse[Any])
async def total_durations_user(start_date: str, end_date: str, user: str):
    return await guacamole_controller.get_perticular_users_total_duration_within_timerange_endpoint(start_date, end_date, user)

@guacamole_router.post("/generate_report/{start_date}/{end_date}/{report_type}", response_model=APIResponse[Any])
async def generate_report(start_date: str, end_date: str, report_type: str):
    return await guacamole_controller.generate_pdf_report(start_date, end_date, report_type)

@guacamole_router.post("/generate_report/{start_date}/{end_date}/{report_type}/{username}", response_model=APIResponse[Any])
async def generate_report_user(start_date: str, end_date: str, report_type: str, username: str):
    return await guacamole_controller.generate_pdf_report_by_username(start_date, end_date, report_type, username)

@guacamole_router.get("/get_client_id", response_model=APIResponse[Any])
async def get_client_id():
    return await guacamole_controller.get_client_id()

@guacamole_router.get("/get_client_roles", response_model=APIResponse[List[str]])
async def get_client_roles(request: Request):
    return await guacamole_controller.get_client_roles(request)

@guacamole_router.post("/post_role/{role_name}")
async def post_role(role_name: str):
    return await guacamole_controller.post_role(role_name)

@guacamole_router.delete("/delete_role/{role_name}")
async def delete_role(role_name: str, authorization: Optional[str] = Header(None)):
    return await guacamole_controller.delete_role(role_name, authorization)

@guacamole_router.post("/submit_role_components")
async def submit_role_components(request: RoleComponentSubmitRequest, authorization: Optional[str] = Header(None)):
    return await guacamole_controller.submit_role_components(request, authorization)

@guacamole_router.get("/get_role_components/{role}")
async def get_role_components(role: str):
    return await guacamole_controller.get_role_components(role)

@guacamole_router.post("/assign_user_role")
async def assign_user_role(request: RBACRequest):
    return await guacamole_controller.assign_user_role(request)

@guacamole_router.get("/get_user_permissions/{username}")
async def get_user_permissions(request: Request, username: str):
    return await guacamole_controller.get_user_permissions(request, username)

@guacamole_router.delete("/remove_role_from_user", response_model=APIResponse[Any])
async def remove_role_from_user(request: RBACRequest):
    return await guacamole_controller.remove_role_from_user(request)

@guacamole_router.get("/guacamole_history", response_model=APIResponse[Any])
async def guacamole_history():
    return await guacamole_controller.get_guacamole_history()

@guacamole_router.get("/guacamole_ActiveSessions", response_model=APIResponse[Any])
async def guacamole_active_sessions():
    return await guacamole_controller.get_guacamole_active_sessions()

@guacamole_router.get("/guacamole_join_session", response_model=APIResponse[Any])
async def guacamole_join_session():
    return await guacamole_controller.guacamole_join_session()

@guacamole_router.get("/api/recording/{identifier}/{logUuid}", response_model=APIResponse[Any])
async def recording_log(identifier: str, logUuid: str):
    return await guacamole_controller.get_recording_log(identifier, logUuid)
