from sqlalchemy import Column,Integer,String,Boolean
from pydantic import BaseModel
from sqlalchemy.ext.declarative import declarative_base

Base_smtp=declarative_base()


class SMTP(Base_smtp):
    __tablename__ = 'SMTP_config'
    id = Column(Integer,primary_key=True)
    smtpStatus = Column(Boolean)
    serverIP = Column(String,nullable=False)
    serverPort =  Column(String,nullable=False)
    userName = Column(String(100), nullable=False)
    password = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False)
    receiverMail = Column(String, nullable=False, default='')
    connOption = Column(String(100), nullable=False)
    userAuthentication = Column(String(100), nullable=False)


class SMTP_Config(BaseModel):

    smtpStatus : bool
    serverIP: str
    serverPort: str
    userName : str
    password : str
    email: str
    receiverMail: str
    connOption: str
    userAuthentication: str
