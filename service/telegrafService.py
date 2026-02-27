import os
import logging
# Logging managed by utils.logger
logger = logging.getLogger(__name__)



conf_file_path = os.getenv('TELEGRAF_CONF_FILE', '/etc/telegraf/telegraf.conf')
metrics_file_path = os.getenv('TELEGRAF_METRIC_FILE', '/etc/telegraf/metrics.txt')

def verify_file_permissions():
    issues = []
    
    if not os.path.exists(conf_file_path):
        issues.append(f"Telegraf config file not found at {conf_file_path}")
    elif not os.access(conf_file_path, os.W_OK):
        issues.append(f"No write permission for telegraf config file at {conf_file_path}")
    
    if not os.path.exists(metrics_file_path):
        issues.append(f"Metrics file not found at {metrics_file_path}")
    elif not os.access(metrics_file_path, os.R_OK):
        issues.append(f"No read permission for metrics file at {metrics_file_path}")
    
    return issues

def create_telegraf_vsphere_input_plugin(vcenter_url, port, username, password, insecure_skip_verify, cluster_id):
    
    try:
        issues = verify_file_permissions()
        if issues:
            error_msg = "Permission issues: " + "; ".join(issues)
            logger.error(error_msg)
            return {"msg": error_msg, "error": True}

        if not all([vcenter_url, port, username, password, cluster_id]):
            error_msg = "Missing required parameters"
            logger.error(error_msg)
            return {"msg": error_msg, "error": True}

        insecure_skip_verify = str(insecure_skip_verify).lower()
        
        vcenter_full_url = f"https://{vcenter_url}:{port}/sdk"
        
        config_content = f"""

# vCenter Configuration start {cluster_id}

[[inputs.vsphere]]
  vcenters = ["{vcenter_full_url}"]
  username = "{username}"
  password = "{password}"
  insecure_skip_verify = {insecure_skip_verify}
  datacenter_metric_include = [] ## if omitted or empty, all metrics are collected
  datacenter_metric_exclude = [ '*' ] ## Datacenters are not collected by default
  

# Metrics Configuration
"""
        
        try:
            with open(metrics_file_path, "r") as f:
                metrics = f.readlines()
            logger.info(f"Read {len(metrics)} metrics from metrics.txt")
        except Exception as e:
            error_msg = f"Error reading metrics file: {str(e)}"
            logger.error(error_msg)
            return {"msg": error_msg, "error": True}

        for metric in metrics:
            config_content += f"  {metric.strip()}\n"
        
        config_content += f"\n# vCenter Configuration end {cluster_id}\n"

        with open(conf_file_path, "a") as f:
            f.write(config_content)

        success_msg = f"Configuration for cluster {cluster_id} has been successfully created"
        logger.info(success_msg)
        return {"msg": success_msg}

    except Exception as e:
        error_msg = f"Error creating configuration: {str(e)}"
        logger.error(error_msg)
        return {"msg": error_msg, "error": True}

def delete_telegraf_vsphere_input_plugin(cluster_id):
    try:
        # Verify file permissions first
        issues = verify_file_permissions()
        if issues:
            error_msg = "Permission issues: " + "; ".join(issues)
            logger.error(error_msg)
            return {"msg": error_msg, "error": True}

        with open(conf_file_path, "r") as f:
            lines = f.readlines()
            original_content = "".join(lines)

        start_comment = f"# vCenter Configuration start {cluster_id}"
        end_comment = f"# vCenter Configuration end {cluster_id}"
        
        start_index = None
        end_index = None

        for i, line in enumerate(lines):
            if start_comment in line.strip():
                start_index = i
            elif end_comment in line.strip() and start_index is not None:
                end_index = i
                break

        if start_index is not None and end_index is not None:
            backup_path = f"{conf_file_path}.bak"
            with open(backup_path, "w") as f:
                f.write(original_content)
            
            del lines[start_index:end_index + 1]

            with open(conf_file_path, "w") as f:
                f.writelines(lines)

            success_msg = f"Configuration for cluster {cluster_id} has been successfully deleted"
            logger.info(success_msg)
            return {"msg": success_msg}
        else:
            warn_msg = f"No configuration found for cluster {cluster_id}"
            logger.warning(warn_msg)
            return {"msg": warn_msg}

    except Exception as e:
        error_msg = f"Error deleting configuration: {str(e)}"
        logger.error(error_msg)
        return {"msg": error_msg, "error": True}

def modify_telegraf_vsphere_input_plugin(vcenter_url, new_username, new_password, new_port, cluster_id):
    try:
        issues = verify_file_permissions()
        if issues:
            error_msg = "Permission issues: " + "; ".join(issues)
            logger.error(error_msg)
            return {"msg": error_msg, "error": True}

        if not all([vcenter_url, new_username, new_password, new_port, cluster_id]):
            error_msg = "Missing required parameters"
            logger.error(error_msg)
            return {"msg": error_msg, "error": True}

        with open(conf_file_path, "r") as f:
            lines = f.readlines()
            original_content = "".join(lines)

        start_comment = f"# vCenter Configuration start {cluster_id}"
        end_comment = f"# vCenter Configuration end {cluster_id}"
        
        start_index = None
        end_index = None

        for i, line in enumerate(lines):
            if start_comment in line.strip():
                start_index = i
            elif end_comment in line.strip() and start_index is not None:
                end_index = i
                break

        if start_index is not None and end_index is not None:
            backup_path = f"{conf_file_path}.bak"
            with open(backup_path, "w") as f:
                f.write(original_content)

            for i in range(start_index, end_index + 1):
                line = lines[i]
                if "username" in line:
                    lines[i] = f'  username = "{new_username}"\n'
                elif "password" in line:
                    lines[i] = f'  password = "{new_password}"\n'
                elif "vcenters" in line:
                    lines[i] = f'  vcenters = ["https://{vcenter_url}:{new_port}/sdk"]\n'

            with open(conf_file_path, "w") as f:
                f.writelines(lines)

            success_msg = f"Configuration for cluster {cluster_id} has been successfully modified"
            logger.info(success_msg)
            return {"msg": success_msg}
        else:
            warn_msg = f"No configuration found for cluster {cluster_id}"
            logger.warning(warn_msg)
            return {"msg": warn_msg}

    except Exception as e:
        error_msg = f"Error modifying configuration: {str(e)}"
        logger.error(error_msg)
        return {"msg": error_msg, "error": True}
