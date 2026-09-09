from fastapi import APIRouter, Form, File, Header, UploadFile, Query, Request, Depends
from typing import Any, List, Optional, Union
from controllers import guacamole_controller
from models.API_Response_model import APIResponse
from models.Rbac_models import RoleComponentSubmitRequest,RBACRequest
from starlette.formparsers import MultiPartParser

# File top pe — import ke baad
MultiPartParser.max_part_size = 20 * 1024 * 1024
guacamole_router = APIRouter(prefix="/v1/guacamole", tags=["guacamole"])

# NOTE: Yeh router Guacamole (remote-desktop gateway) admin/reporting/RBAC
# ka layer hai. `data` shapes seedha Guacamole REST API / Keycloak Admin API
# ke JSON responses se aate hain (yeh backend sirf proxy + auth-header inject
# karta hai), isliye field-by-field shape un upstream systems par depend
# karta hai — niche har endpoint ka "kis system se" aur "kis kaam ke liye"
# note kiya gaya hai.

# ---------------------- BASIC ROUTES ----------------------
@guacamole_router.get("/login", response_model=APIResponse[Any])
async def login():
    """Guacamole ke liye service-account login karke auth token lo (internal use — cache/refresh karta hai)."""
    return await guacamole_controller.get_login()

@guacamole_router.get("/list_guaca_users", response_model=APIResponse[Any])
async def list_guaca_users():
    """Guacamole DB mein directly bane users list karo (Keycloak-sync se pehle ke legacy users)."""
    return await guacamole_controller.list_of_users()

@guacamole_router.get("/list_users", response_model=APIResponse[Any])
async def list_users(
    first: int = Query(0, ge=0),
    limit: int = Query(10, ge=1),
    search: str = Query("", max_length=100)
):
    """
    Keycloak users list karo (paginated, naam se search).

    Response `data`: [ {"username": str, "email": str, "userid": str, ...}, ... ]  (Keycloak Admin API shape)
    """
    return await guacamole_controller.list_of_kecloak_users(first=first, limit=limit, search=search)

@guacamole_router.get("/list_connections", response_model=APIResponse[Any])
async def list_connections():
    """Guacamole mein configured saari RDP/SSH/VNC connections (machines) list karo."""
    return await guacamole_controller.list_machines()

@guacamole_router.post("/update_connection", response_model=APIResponse[Any])
async def update_connection():
    """Guacamole connection settings update/sync karo."""
    return await guacamole_controller.updataeConnection()

@guacamole_router.patch("/assign_connection_to_user", response_model=APIResponse[Any])
async def assign_connection_to_user():
    """Ek Guacamole connection (machine) ko user se assign karo (access grant)."""
    return await guacamole_controller.assign_connection_to_user()

@guacamole_router.get("/generate_reports/vamanit_session_reports/{start_date}/{end_date}", response_model=APIResponse[Any])
async def vamanit_session_reports(start_date: str, end_date: str):
    """
    Diye gaye date-range ke saare users ke session-reports lo (login/logout,
    duration).

    Path params: start_date, end_date — format "YYYY-MM-DD".
    Response `data`: [ {"username": str, "start_time": str, "duration": str, ...}, ... ]
    """
    return await guacamole_controller.get_reports(start_date, end_date)

@guacamole_router.get("/generate_reports/vamanit_allusers/{start_date}/{end_date}", response_model=APIResponse[Any])
async def vamanit_allusers(start_date: str, end_date: str):
    """Date-range mein activity karne wale saare usernames ki list lo."""
    return await guacamole_controller.get_usernames_endpoint(start_date, end_date)

@guacamole_router.get("/generate_reports/vamanit_session_reports/{start_date}/{end_date}/{username}", response_model=APIResponse[Any])
async def user_session_reports(start_date: str, end_date: str, username: str):
    """Ek specific user ke session-reports lo, date-range ke liye."""
    return await guacamole_controller.get_perticular_user_sessionreports_endpoint(start_date, end_date, username)

@guacamole_router.get("/generate_reports/day_reports/{start_date}/{end_date}", response_model=APIResponse[Any])
async def day_reports(start_date: str, end_date: str):
    """Date-range ke andar day-wise (har din ka summary) session report lo."""
    return await guacamole_controller.get_day_reports(start_date, end_date)

@guacamole_router.get("/generate_reports/day_reports/{start_date}/{end_date}/{username}", response_model=APIResponse[Any])
async def daily_reports_of_user(start_date: str, end_date: str, username: str):
    """Ek user ka day-wise session report lo, date-range ke liye."""
    return await guacamole_controller.get_daily_reports_of_each_user(start_date, end_date, username)

@guacamole_router.get("/reports", response_model=APIResponse[Any])
async def fetch_companies():
    """
    Reports pe branding ke liye saved companies (naam + logo) list karo —
    white-labeled PDF reports ke liye.
    """
    return await guacamole_controller.fetch_companies()

@guacamole_router.get("/generate_reports/{report_type}", response_model=APIResponse[Any])
async def reports_by_type(report_type: str):
    """Ek company ki saved report-config lo, report_type se (e.g. "vamanit")."""
    return await guacamole_controller.read_companies_by_report_type(report_type)

@guacamole_router.post("/reports/update_report", response_model=APIResponse[Any])
async def update_report(
    company_name: str = Form(...),
    company_logo: Union[UploadFile, str, None] = File(...),
    report_type: str = Form(...),
):
    """
    Company branding (naam + logo) create/update karo report-type ke liye.

    Request: multipart/form-data — `company_name`, `company_logo` (file ya
    existing-logo-path string), `report_type`.
    """
    return await guacamole_controller.update_company(company_name, company_logo, report_type)

@guacamole_router.delete("/delete_company/{report_name}", response_model=APIResponse[Any])
async def delete_company(report_name: str):
    """Company branding record delete karo."""
    return await guacamole_controller.delete_company_data(report_name)

@guacamole_router.get("/generate_reports/total_durations_within_range/{start_date}/{end_date}", response_model=APIResponse[Any])
async def total_durations(start_date: str, end_date: str):
    """Date-range mein saare users ki total connected-duration (sum) lo."""
    return await guacamole_controller.get_users_total_duration_within_timerange_endpoint(start_date, end_date)

@guacamole_router.get("/generate_reports/total_durations_within_range/{start_date}/{end_date}/{user}", response_model=APIResponse[Any])
async def total_durations_user(start_date: str, end_date: str, user: str):
    """Ek user ki total connected-duration lo, date-range ke liye."""
    return await guacamole_controller.get_perticular_users_total_duration_within_timerange_endpoint(start_date, end_date, user)

@guacamole_router.post("/generate_report/{start_date}/{end_date}/{report_type}", response_model=APIResponse[Any])
async def generate_report(start_date: str, end_date: str, report_type: str):
    """
    Saare users ka PDF report generate karo, diye gaye branding
    (`report_type`) ke saath.

    Response `data`: {"file_url": str, ...} — download URL ya path.
    """
    return await guacamole_controller.generate_pdf_report(start_date, end_date, report_type)

@guacamole_router.post("/generate_report/{start_date}/{end_date}/{report_type}/{username}", response_model=APIResponse[Any])
async def generate_report_user(start_date: str, end_date: str, report_type: str, username: str):
    """Ek user ka PDF report generate karo."""
    return await guacamole_controller.generate_pdf_report_by_username(start_date, end_date, report_type, username)

@guacamole_router.get("/get_client_id", response_model=APIResponse[Any])
async def get_client_id():
    """Keycloak me is app ke OAuth client ka internal ID lo (RBAC setup ke liye)."""
    return await guacamole_controller.get_client_id()

@guacamole_router.get("/get_client_roles", response_model=APIResponse[List[str]])
async def get_client_roles(request: Request):
    """
    Keycloak client ke saare defined roles list karo (e.g. "admin", "viewer").

    Response `data`: ["role1", "role2", ...]
    """
    return await guacamole_controller.get_client_roles(request)

@guacamole_router.post("/post_role/{role_name}")
async def post_role(role_name: str, authorization: Optional[str] = Header(None)):
    """
    Naya role banao — Keycloak (client role) + local `rbac_table` dono mein
    sync ho jaata hai.

    Header: Authorization (Bearer token, Keycloak admin call ke liye).
    """
    return await guacamole_controller.post_role(role_name, authorization)

@guacamole_router.delete("/delete_role/{role_name}")
async def delete_role(role_name: str, authorization: Optional[str] = Header(None)):
    """Role delete karo — Keycloak + local DB dono se."""
    return await guacamole_controller.delete_role(role_name, authorization)

@guacamole_router.post("/submit_role_components")
async def submit_role_components(request: RoleComponentSubmitRequest, authorization: Optional[str] = Header(None)):
    """
    Ek role ko sidebar/UI "components" (permissions) assign karo — Keycloak
    role attributes + local DB dono update hote hain.

    Request body: RoleComponentSubmitRequest = {"role": str, "components": [str, ...]}
    """
    return await guacamole_controller.submit_role_components(request, authorization)

@guacamole_router.get("/get_role_components/{role}")
async def get_role_components(role: str):
    """
    Ek role ko assigned components (permissions) list karo.

    Response `data`: {"components": [str, ...]}
    """
    return await guacamole_controller.get_role_components(role)

@guacamole_router.post("/assign_user_role")
async def assign_user_role(request: RBACRequest):
    """
    Ek ya zyada users ko ek role assign karo (Keycloak + local DB).

    Request body: RBACRequest = {"username": [str, ...], "role": str, "components": [str, ...]}
    """
    return await guacamole_controller.assign_user_role(request)

@guacamole_router.get("/get_user_permissions/{username}")
async def get_user_permissions(request: Request, username: str):
    """
    Ek user ke saare roles + un roles se milne wale combined
    components (permissions) lo — sidebar/menu render karne ke liye
    frontend yehi call karta hai.

    Response `data`: {"roles": [str, ...], "components": [str, ...]}
    """
    return await guacamole_controller.get_user_permissions(request, username)

@guacamole_router.delete("/remove_role_from_user", response_model=APIResponse[Any])
async def remove_role_from_user(request: RBACRequest):
    """User se ek role remove karo (Keycloak + local DB)."""
    return await guacamole_controller.remove_role_from_user(request)

@guacamole_router.get("/guacamole_history", response_model=APIResponse[Any])
async def guacamole_history():
    """Saare Guacamole connection-history records (past sessions) list karo."""
    return await guacamole_controller.get_guacamole_history()

@guacamole_router.get("/guacamole_ActiveSessions", response_model=APIResponse[Any])
async def guacamole_active_sessions():
    """
    Abhi-active (live) Guacamole sessions list karo — kaun, kis machine pe,
    kabse connected hai.
    """
    return await guacamole_controller.get_guacamole_active_sessions()

@guacamole_router.get("/guacamole_join_session", response_model=APIResponse[Any])
async def guacamole_join_session():
    """Admin ke liye kisi active session mein "shadow/join" karne ka access-info lo."""
    return await guacamole_controller.guacamole_join_session()

@guacamole_router.get("/api/recording/{identifier}/{logUuid}", response_model=APIResponse[Any])
async def recording_log(identifier: str, logUuid: str):
    """
    Ek session-recording ka playback log fetch karo (screen-recording replay
    ke liye Guacamole recording format).
    """
    return await guacamole_controller.get_recording_log(identifier, logUuid)
