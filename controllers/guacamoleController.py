import base64
from  dto.machineDto import MachineDto
import  service.gucamoleService as service
from typing import List,Union
from datetime import datetime
from temporalio.worker import Worker

from fastapi import FastAPI, HTTPException, Depends, File, UploadFile, Form,Query,APIRouter, HTTPException, Path,Response # type: ignore,
from  dto.machineDto import MachineDto
from typing import List,Union
from datetime import datetime
from db_configuration.config import get_db
from sqlalchemy.orm import Session
from models.Rbac_models import RBAC,RoleComponentSubmitRequest,RBACRequest
from fastapi.responses import FileResponse
from fastapi.encoders import jsonable_encoder

guacarouter = APIRouter()

# def for_uniqu_id():
#     unique_id = datetime.now()
#     res = f"{unique_id.hour }{unique_id.minute}{unique_id.second}"
#     return res
# from temporalio.client import Client
# def get_db():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()
#         # skip: int = 0, limit: int = 100, db: Session = Depends(get_db)


@guacarouter.get("/login")
async def get_login():
    try:
        data = await service.login_with_guacamole()
        return {"status": "Ok", "code": 200, "message": "Successfully authenticated with Guacamole", "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
       
# LIST OF USER IN GUACAMOLE   
@guacarouter.get("/list_guaca_users")
async def  list_of_users():
    try:
        data = await service.list_of_users()
        return {"status": "Ok", "code": 200, "message": "All listed  Guacamole Users", "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@guacarouter.get("/list_users")
async def  list_of_kecloak_users():
    try:
        data = await service.get_userList_from_keycloak()
        return {"status": "Ok", "code": 200, "message": "All listed  Guacamole Users", "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))    



# LIST OF MACHINE OR CONNECTION
@guacarouter.get("/list_connection")
async def list_machines():
    try:
        data = await service.list_machines()
        return {"status": "Ok", "code": 200, "message": "Successfully authenticated with Guacamole", "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 

# # adding/createing a new machine
# @guacarouter.post('/create_connection')
# async def createConnection(pool_data:MachineDto ):
#     # ,db:Session=Depends(get_db)
#     try:
#          data = await service.creating_connection(pool_data)
#          return{"status":"ok","code":"200","message":"machine created successfully","machine":data}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
    
#------------------------------------------------- pending not usiing these two methods-------------------------------
@guacarouter.post('/update_connection')
async def updataeConnection(machine_data:MachineDto ):
    # print(machine_data)
    # ,db:Session=Depends(get_db)
    try:
        data = await service.modify_connection(machine_data)
        return{"status":"ok","code":"200","message":"machine created successfully","machine":data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))       


# Assign User to Connections/machine  / same  Create a pool manully
@guacarouter.patch('/assign_connection_to_user/')
async def assign_connection_to_user(usernames:list[str], connection:str):
    try:
        data = service.assign_connection_to_user(usernames,connection)
        return{"status":"ok","code":"200","message":"machine created successfully","machine":data}
    except Exception as e:
        return HTTPException(status_code=500,detail=str(e)) 
    
           
#------------------------------------------------- pending not usiing these two methods -------------------------------

# #  Add  Machine to users
def parse_datetime(date_str: str) -> datetime:
    formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%d-%m-%Y %H:%M:%S',"%Y-%m-%d %H:%M:%S.%f"]
    for fmt in formats:
        try:
            return jsonable_encoder(datetime.strptime(date_str, fmt))
        except ValueError:
            continue
    raise ValueError("Invalid date format")
@guacarouter.get("/vamanit_session_reports/{start_date}/{end_date}")
async def get_reports(start_date: str, end_date: str):
    try:
        start_date_dt = parse_datetime(start_date)
        end_date_dt = parse_datetime(end_date)
    except ValueError as e:
        print('Error parsing',e)
        raise HTTPException(status_code=400, detail=str(e))
        
    session_reports = await service.get_session_reports(start_date_dt, end_date_dt)

    return session_reports


@guacarouter.get("/vamanit_allusers/{start_date}/{end_date}")
async def get_usernames_endpoint(start_date: str, end_date: str):
    try:
        start_date_dt = parse_datetime(start_date)
        end_date_dt = parse_datetime(end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    session_reports = await service.get_session_reports(start_date_dt, end_date_dt)
    users = await service.get_users_in_timerange(session_reports)
    return users


@guacarouter.get("/vamanit_session_reports/{start_date}/{end_date}/{username}")
async def get_perticular_user_sessionreports_endpoint(start_date: str, end_date: str, username: str):
    try:
        start_date_dt = parse_datetime(start_date)
        end_date_dt = parse_datetime(end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    session_reports = await service.get_session_reports(start_date_dt, end_date_dt)
    user_session_report = await service.get_perticular_user_sessionreports(session_reports, username)
    return user_session_report


@guacarouter.get("/day_reports/{start_date}/{end_date}")
async def get_day_reports(start_date: str, end_date: str):
    try:
        start_date_dt = parse_datetime(start_date)
        end_date_dt = parse_datetime(end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    session_reports = await service.get_session_reports(start_date_dt, end_date_dt)
    daily_reports = await service.get_daily_reports(session_reports)
    return daily_reports


@guacarouter.get("/day_reports/{start_date}/{end_date}/{username}")
async def get_daily_reports_of_each_user(start_date: str, end_date: str, username: str):
    try:
        start_date_dt = parse_datetime(start_date)
        end_date_dt = parse_datetime(end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    session_reports = await service.get_session_reports(start_date_dt, end_date_dt)
    daily_reports = await service.get_daily_reports(session_reports)
    user_daily_reports = await service.get_perticular_user_daily_reports(daily_reports, username)
    return user_daily_reports

@guacarouter.get("/reports")
async def fetch_companies():
    try:
        companies = await service.get_companies()
        return  companies
    except Exception as e:
        print("Error fetching companies:", e)
        raise HTTPException(status_code=500, detail="Internal server error")
    

# Endpoint to get companies by report_type
@guacarouter.get("/reports/{report_type}")
async def read_companies_by_report_type(report_type: str ):
    try:
        companies = await service.get_companies_by_report_type(report_type)
     
        # print(companies)
        return companies
    except Exception as error:
        print("Error while fetching data:", error)
        raise HTTPException(status_code=500, detail="Internal Server Error")


@guacarouter.post("/update_report")
async def update_company(
    company_name: str = Form(...),
    company_logo: Union[UploadFile, str,None] = File(...),
    report_type: str = Form(...),
):
    # company_logo_bytes = None  # Initialize the variable
    # print(company_name,company_logo)
   
    try:
       
        company_logo_bytes = None
        if isinstance(company_logo, UploadFile):
            # Handle file upload
            company_logo_bytes = await company_logo.read()
        elif isinstance(company_logo, str):
            # Handle base64 string
            company_logo_bytes = base64.b64decode(company_logo)
       
        if report_type in ["Session Reports","Daily Reports","Consolidate Reports"]:
           
            await service.update_report(company_name, company_logo_bytes, report_type)
            return {"message": "Company updated successfully"}
        else:
           
            await service.insert_report(company_name, company_logo_bytes, report_type)
            return {"message": "Company added successfully"}
   
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
   


#  To delete company data
@guacarouter.delete("/delete_company/{report_name}")
async def delete_company_data(report_name: str ):
    try:
        await service.delete_report(report_name)
        return {"message": "Company deleted successfully"}
    except ValueError as ve:
        print("Error deleting company:", ve)
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        print("Error deleting company:", e)
        raise HTTPException(status_code=500, detail="Internal server error")

    
    


@guacarouter.get("/total_durations_within_range/{start_date}/{end_date}")
async def get_users_total_duration_within_timerange_endpoint(start_date: str, end_date: str):
    start_date_dt = parse_datetime(start_date)
    end_date_dt = parse_datetime(end_date)
    try:
        session_reports = await service.get_session_reports(start_date_dt, end_date_dt)
        daily_reports = await service.get_daily_reports(session_reports)
        user_total_duration = await service.get_users_total_duration_within_timerange(daily_reports)
       
        return user_total_duration
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))
    

@guacarouter.get("/total_durations_within_range/{start_date}/{end_date}/{user}")
async def get_perticular_users_total_duration_within_timerange_endpoint(start_date: str, end_date: str,user: str):
    start_date_dt = parse_datetime(start_date)
    end_date_dt = parse_datetime(end_date)
    try:
        session_reports =await service.get_session_reports(start_date_dt, end_date_dt)
        daily_reports = await service.get_daily_reports(session_reports)
        user_total_duration =await service.get_users_total_duration_within_timerange(daily_reports)
        consolidat_user= await service.consolidate_report_perticular_user(user_total_duration,user)
       
        return consolidat_user
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@guacarouter.post("/generate_report/{start_date}/{end_date}/{report_type}")

async def generate_pdf_report(start_date: str, end_date: str, report_type: str):
    """
    API Endpoint to generate a PDF session report.
    """
   
    start_date_dt = parse_datetime(start_date)
    end_date_dt = parse_datetime(end_date)
    pdf_file = await service.generate_report(start_date_dt, end_date_dt, report_type)
 
    return FileResponse(pdf_file, media_type="application/pdf", filename=f"{report_type}{start_date}.pdf")
@guacarouter.post("/generate_report/{start_date}/{end_date}/{report_type}/{username}")
async def generate_pdf_report(start_date: str, end_date: str, report_type: str,username: str):
    """
    API Endpoint to generate a PDF session report.
    """
   
    start_date_dt = parse_datetime(start_date)
    end_date_dt = parse_datetime(end_date)
    pdf_file = await service.generate_userbased_report(start_date_dt, end_date_dt, report_type,username)
   
    return FileResponse(pdf_file, media_type="application/pdf", filename=f"{report_type}{username}.pdf")

    

@guacarouter.get("/get_client_id")
async def  get_client_id():
    try:
        client_data = await service.get_client()  
        return {"status": "Ok", "code": 200, "message": "Client ID retrieved successfully", "client_id": client_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@guacarouter.get("/get_client_roles")
async def get_client_roles():
    try:
        client_roles = await service.get_client_roles()
        # Extract only the "name" field into an array
        role_names = [role["name"] for role in client_roles]
        return {
            "status": "Ok",
            "code": 200,
            "message": "Client role names retrieved successfully",
            "client_roles": role_names
        }
    except Exception as e:
        raise e
    

@guacarouter.post("/post_role/{role_name}")
async def post_role(role_name: str):
    try:
        result = await service.posting_role(role_name)
        # return {"status": "Ok", "code": 201, "message": "Role created successfully", "role_id": result.get("id")}
        return result

    except Exception as e:
        raise e
    

@guacarouter.delete('/delete_role/{role_name}')
async def delete_role(role_name: str):
    try:
        result = await service.deleting_role(role_name)
        # return {"status": "Ok", "code": 201, "message": "Role created successfully", "role_id": result.get("id")}
        return result

    except Exception as e:
        raise e
 


@guacarouter.post("/submit_role_components")
async def submit_role_components(request: RoleComponentSubmitRequest):
    try:
        result = await service.updating_role_component(request)
        # return {"status": "Ok", "code": 201, "message": "Role created successfully", "role_id": result.get("id")}
        return result

    except Exception as e:
        raise e
    

@guacarouter.get("/get_role_components/{role}")
async def get_role_components(role: str):
    try:
        result = await service.getting_role_component(role)
        # return {"status": "Ok", "code": 201, "message": "Role created successfully", "role_id": result.get("id")}
        return result

    except Exception as e:
        raise e
    

@guacarouter.post("/assign_user_role")
async def assign_user_role(request: RBACRequest):
    try:
        result = await service.assignning_user_role(request)
        # return {"status": "Ok", "code": 201, "message": "Role created successfully", "role_id": result.get("id")}
        return result

    except Exception as e:
        raise e



@guacarouter.get("/get_user_permissions/{username}")
async def get_userPermissions(username: str):
    try:
        result = await service.get_user_permissions(username)
        # return {"status": "Ok", "code": 201, "message": "Role created successfully", "role_id": result.get("id")}
        return result

    except Exception as e:
        raise e
    

@guacarouter.delete("/remove_role_from_user")
async def remove_role_from_user(request: RBACRequest):
    try:
        result = await service.delete_role_from_user(request)
        # return {"status": "Ok", "code": 201, "message": "Role created successfully", "role_id": result.get("id")}
        return result

    except Exception as e:
        raise e   
    


@guacarouter.get("/guacamole_history")
async def get_guacamole_history():
    return await service.get_guacamole_history()


@guacarouter.get("/guacamole_ActiveSessions")
async def get_guacamole_ActiveSessions():
    return await service.get_guacamole_ActiveSessions()  

@guacarouter.get("/guacamole_join_session")
async def guacamole_join_session(
    session_id: str = Query(...),
    datasource: str = Query(None)
):
    return await service.generate_guacamole_session_url(session_id, datasource)


@guacarouter.get("/api/recording/{identifier}/{logUuid}")
async def get_recording_log(identifier: str, logUuid: str):
    return await service.get_recording_log(identifier, logUuid)



# @guacarouter.get("/guacamole_join_session")
# async def guacamole_join_session(session_id: str = Query(...)):
#     return await service.generate_guacamole_session_url(session_id)




 #--------------------------------------------Done till now-----------------------------------------------

           
