from db_configuration.config import engine, Base
from models.schedule_model import Base as schedule_Base
from models.SMTP_models import Base_smtp
from models.models import Base
from models.proxmox_model import Base as Proxmox_Base
from models.IPMI_models import Base as IPMI_Base
from models.IPs_model import Base as IPs_Base
from models.request_logger_model import Base as RequestLog_Base
from models import task_models

def create_tables():
    try:
        # Create tables in the database
        Base.metadata.create_all(bind=engine)
        schedule_Base.metadata.create_all(bind=engine)
        Base_smtp.metadata.create_all(bind=engine)
        task_models.Base_task.metadata.create_all(bind=engine)
        IPs_Base.metadata.create_all(bind=engine)
        Proxmox_Base.metadata.create_all(bind=engine)
        IPMI_Base.metadata.create_all(bind=engine)
        RequestLog_Base.metadata.create_all(bind=engine)
    except Exception as e:
        raise Exception(f"Failed to create tables: {str(e)}") from e
