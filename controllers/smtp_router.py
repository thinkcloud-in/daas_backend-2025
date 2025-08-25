from fastapi import Depends,APIRouter
import service.smtp_service as smtp_service
from models.SMTP_models import SMTP_Config
from sqlalchemy.orm import Session
from db_configuration.config import get_db

smtpRouter = APIRouter()

@smtpRouter.post("/smtp-post/")
def smtp_create(item:SMTP_Config, dp:Session = Depends(get_db)):
    try:
        return smtp_service.smtp_post(item, dp)
        print("smtp_create")
    except Exception as e:
        print('Error While Data Sending...',e)

@smtpRouter.get('/smtp-get')
def smtp_get(db : Session = Depends(get_db)):
    try:
        return smtp_service.smtp_get(db)
    except Exception as e:
        print('Error While Fetching Data...',e)

@smtpRouter.put('/smtp-update')
def smtp_update(item : SMTP_Config, db:Session = Depends(get_db)):
    try:
        return smtp_service.smtp_update_data(item,db)
        print('Successfully Updated')
    except Exception as e:
        print('Error While Updating Data...',e)

@smtpRouter.patch('/smtp-update-status')
def smtp_patch(data:dict,db:Session = Depends(get_db)):
    try:
        smtp_status = data.get("smtpStatus")
        response = smtp_service.smtp_status_update(smtp_status, db)

        return {"smtpStatus":response.smtpStatus}
        print('Successfully Updated Status')
    except Exception as e:
        print('Error While Updating Status...',e)

@smtpRouter.post('/smtp-test-mail')
def smtp_testmail(data:SMTP_Config,db:Session = Depends(get_db)):

    try:
        return smtp_service.smtp_test_mail(data, db)
    except Exception as e:
        print('Error While Testing Test Mail...',e)