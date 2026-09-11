"""
smtp_service — SMTP configuration CRUD (singleton row) + test-email sending.

All functions return raw ORM objects (password field included) — masking/stripping is
handled at the controller layer (controllers/smtp_controller.py), not here.
Used by: controllers/smtp_controller.py.
"""
from models.SMTP_models import SMTP
from fastapi import HTTPException
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def smtp_post(item, db):
    """
    Create a new SMTP config (singleton — rejected if a row already exists).
    Returns: SMTP ORM object (created).
    Raises: HTTPException 400 if config already exists; 500 on other DB error.
    """
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
    """
    Fetch all SMTP config rows (in practice there's only ever one, the singleton row).
    Returns: list[SMTP] ORM objects, raw (password included — controller strips it).
    Raises: HTTPException 500 on DB error.
    """
    try:
        items = db.query(SMTP).all()
        return items
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error while fetching SMTPs: {str(e)}")


def smtp_update_data(item, db):
    """
    Update the singleton SMTP config row, or create it if it doesn't exist yet (upsert).
    Password is only overwritten if a non-empty `item.password` is supplied — an empty/missing
    password on update leaves the existing stored password untouched.
    Returns: SMTP ORM object (created or updated).
    Raises: HTTPException 500 on DB error.
    """
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
    """
    Enable/disable the SMTP config (toggle `smtpStatus` on the singleton row).
    Returns: SMTP ORM object (updated).
    Raises: HTTPException 404 if no config exists yet; 500 on other DB error.
    """
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
    """
    Send a one-off test email using the given (not-yet-saved) SMTP settings, to verify they
    work before persisting them. SSL uses `SMTP_SSL` directly; otherwise plain SMTP + STARTTLS.
    Login only happens if `data.userAuthentication == "true"`.
    Returns: {"message": "Test email sent successfully"}.
    Raises: HTTPException 500 if sending fails (connection closed best-effort on failure too).
    """
    try:
        # config = db.query(SMTP).first()
        # if config is None:
        #     raise HTTPException(status_code=404, detail="SMTP configuration not found")
        
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
        body = 'This is a test email sent from the DevRaQ Server.'
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
 