from models.SMTP_models import SMTP
from fastapi import HTTPException
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
 
def smtp_post(item, db):
    try:
        existing = db.query(SMTP).first()
        if existing:
            raise HTTPException(status_code=400, detail="SMTP config already exists. Please use update instead.")
        db_item = SMTP(
            smtpStatus = item.smtpStatus,
            serverIP = item.serverIP,
            serverPort = item.serverPort,
            userName = item.userName,
            password = item.password,
            email = item.email,
            receiverMail = item.receiverMail,
            connOption = item.connOption,
            userAuthentication = item.userAuthentication
        )
        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        return db_item
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error while creating SMTP: {str(e)}")
    
def smtp_get(db):
    try:
        items = db.query(SMTP).all()
        return items
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error while fetching SMTPs: {str(e)}")
 
 
def smtp_update_data(item, db):
    try:
        db_item = db.query(SMTP).first()
        if db_item is None:
            db_item = SMTP(
                smtpStatus = item.smtpStatus,
                serverIP = item.serverIP,
                serverPort = item.serverPort,
                userName = item.userName,
                password = item.password,
                email = item.email,
                receiverMail = item.receiverMail,
                connOption = item.connOption,
                userAuthentication = item.userAuthentication
            )
            db.add(db_item)
        else:
            db_item.smtpStatus = item.smtpStatus
            db_item.serverIP = item.serverIP
            db_item.serverPort = item.serverPort
            db_item.userName = item.userName
            if item.password:
                db_item.password = item.password
            db_item.email = item.email
            db_item.receiverMail = item.receiverMail
            db_item.connOption = item.connOption
            db_item.userAuthentication = item.userAuthentication
        db.commit()
        db.refresh(db_item)
        return db_item
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error while updating/creating SMTP: {str(e)}")

    
def smtp_status_update(smtpStatus: bool, db):
    try:
        db_item = db.query(SMTP).first()
        if db_item is None:
            raise HTTPException(status_code=404, detail="SMTP configuration not found")
        
        # Explicit update and commit
        db_item.smtpStatus = bool(smtpStatus)
        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        return db_item
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error while updating SMTP status: {str(e)}")
    
def smtp_test_mail(data, db):
    try:
        config = db.query(SMTP).first()
        if config is None:
            raise HTTPException(status_code=404, detail="SMTP configuration not found")
        
        smtp_serverip = data.serverIP
        smtp_port = int(data.serverPort)
        smtp_mail = data.email
        smtp_username = data.userName
        smtp_password = data.password
        smtp_connOptions = data.connOption
        smtp_receiverMail = data.receiverMail
        smtp_userAuth = data.userAuthentication

        msg = MIMEMultipart()
        msg["From"] = smtp_mail.strip()
        msg["To"] = smtp_receiverMail.strip()
        msg['Subject'] = 'Test Mail'
        body = 'This is a test email sent from the registration system.'
        msg.attach(MIMEText(body, 'plain'))

        server = None
        if smtp_connOptions == "SSL":
            server = smtplib.SMTP_SSL(smtp_serverip, smtp_port)
        else:
            server = smtplib.SMTP(smtp_serverip, smtp_port)
            server.ehlo()
            server.starttls()
            server.ehlo()
        
        if smtp_userAuth == "true": # Only login if authentication is enabled
            server.login(smtp_username, smtp_password)
            
        text = msg.as_string()
        server.sendmail(smtp_mail, smtp_receiverMail, text)
        server.quit()
        return {"message": "Test email sent successfully"}
    except HTTPException as e:
        raise
    except Exception as e:
        if server is not None:
            try:
                server.quit()
            except:
                pass
        raise HTTPException(status_code=500, detail=f"Failed to send test email: {str(e)}")
 