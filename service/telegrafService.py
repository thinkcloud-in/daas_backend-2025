# import os
# from dotenv import load_dotenv


# #  Load the dotenv file
# # load_dotenv()

# conf_file_path=os.getenv('TELEGRAF_CONF_FILE')
# metrics_file_path=os.getenv('TELEGRAF_METRIC_FILE')

# # Function to add vsphere input plugin in telegraf.conf file
# def create_telegraf_vsphere_input_plugin(vcenter_url,port,username,password,insecure_skip_verify,cluster_id):
#     # if converted to lowercase, the value is appending as a string but not as boolean hence assigned value like this
#     if insecure_skip_verify == 'True':
#         insecure_skip_verify = 'true'
#     else:
#         insecure_skip_verify = 'false'
#     # Appending vCenter URL to vcenters list
#     vcenters = [f"https://{vcenter_url}:{port}/sdk"]

#     # Writing to telegraf.conf file
#     with open(conf_file_path, "a") as f:
#         # Writing vCenter configuration section start comment
#         f.write(f"\n\n# vCenter Configuration start {cluster_id}\n")

#         # Writing vCenter configuration section
#         f.write("\n[[inputs.vsphere]]\n")
#         f.write(f"  vcenters = {vcenters}\n")
#         f.write(f"  username = \"{username}\"\n")
#         f.write(f"  password = \"{password}\"\n")
#         f.write(f"  insecure_skip_verify = {insecure_skip_verify}\n")
#         f.write("  datacenter_metric_include = [] ## if omitted or empty, all metrics are collected\n")
#         f.write("  datacenter_metric_exclude = [ '*' ] ## Datacenters are not collected by default\n")

#     # Reading metrics from metrics.txt file and appending to telegraf.conf
#     with open(metrics_file_path, "r") as f:
#         metrics = f.readlines()

#     with open(conf_file_path, "a") as f:
#         # Writing metrics configuration
#         f.write("\n# Metrics Configuration\n")
#         for line in metrics:
#             f.write(f"  {line.strip()}\n")

#         # Writing vCenter configuration section end comment
#         f.write(f"\n\n# vCenter Configuration end {cluster_id}\n")

#     print("Configuration has been created in telegraf.conf")
#     return {"msg":"Configuration has been created in telegraf.conf"}


# # Function to delete vsphere input plugin from telegraf.conf file
# def delete_telegraf_vsphere_input_plugin(cluster_id):
#     # Open the telegraf.conf file for reading
#     with open(conf_file_path, "r") as f:
#         lines = f.readlines()

#     # Search for the start and end comments of the vCenter configuration section
#     start_comment = f"# vCenter Configuration start {cluster_id}\n"
#     end_comment = f"# vCenter Configuration end {cluster_id}\n"
#     start_index = None
#     end_index = None

#     for i, line in enumerate(lines):
#         if line == start_comment:
#             start_index = i
#         elif line == end_comment:
#             end_index = i
#             break

#     # If start and end comments are found, delete the lines in between
#     if start_index is not None and end_index is not None:
#         del lines[start_index:end_index + 1]

#         # Write the modified content back to the telegraf.conf file
#         with open(conf_file_path, "w") as f:
#             f.writelines(lines)

#         print("Configuration has been deleted.")
#         return {"msg":"Configuration has been deleted."}
#     else:
#         print("Configuration has been deleted.")
#         return {"msg":"Configuration has been deleted."}


# # Function to modify vsphere input plugin in telegraf.conf file
# def modify_telegraf_vsphere_input_plugin(vcenter_url, new_username, new_password, new_port,cluster_id):
#     # Open the telegraf.conf file for reading
#     with open(conf_file_path, "r") as f:
#         lines = f.readlines()

#     # Search for the start and end comments of the vCenter configuration section
#     start_comment = f"# vCenter Configuration start {cluster_id}\n"
#     end_comment = f"# vCenter Configuration end {cluster_id}\n"
#     start_index = None
#     end_index = None

#     for i, line in enumerate(lines):
#         if line == start_comment:
#             start_index = i
#         elif line == end_comment:
#             end_index = i
#             break

#     # If start and end comments are found, modify the username, password, and port lines in between
#     if start_index is not None and end_index is not None:
#         for i in range(start_index + 1, end_index):
#             if "username" in lines[i]:
#                 lines[i] = f"  username = \"{new_username}\"\n"
#             elif "password" in lines[i]:
#                 lines[i] = f"  password = \"{new_password}\"\n"
#             elif "vcenters" in lines[i]:
#                 lines[i] = f"  vcenters = [\"https://{vcenter_url}:{new_port}/sdk\"]\n"

#         # Write the modified content back to the telegraf.conf file
#         with open(conf_file_path, "w") as f:
#             f.writelines(lines)

#         print("Configuration has been modified.")
#         return {"msg":"Configuration has been modified."}
#     else:
#         print("Configuration has been modified.")
#         return {"msg":"Configuration has been modified."}

import os
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get configuration paths with defaults
conf_file_path = os.getenv('TELEGRAF_CONF_FILE', '/etc/telegraf/telegraf.conf')
metrics_file_path = os.getenv('TELEGRAF_METRIC_FILE', '/etc/telegraf/metrics.txt')

def verify_file_permissions():
    """Verify file permissions and existence"""
    issues = []
    
    # Check telegraf.conf
    if not os.path.exists(conf_file_path):
        issues.append(f"Telegraf config file not found at {conf_file_path}")
    elif not os.access(conf_file_path, os.W_OK):
        issues.append(f"No write permission for telegraf config file at {conf_file_path}")
    
    # Check metrics.txt
    if not os.path.exists(metrics_file_path):
        issues.append(f"Metrics file not found at {metrics_file_path}")
    elif not os.access(metrics_file_path, os.R_OK):
        issues.append(f"No read permission for metrics file at {metrics_file_path}")
    
    return issues

def create_telegraf_vsphere_input_plugin(vcenter_url, port, username, password, insecure_skip_verify, cluster_id):
    
    """Create vsphere input plugin configuration"""
    try:
        # Verify file permissions first
        issues = verify_file_permissions()
        if issues:
            print("Verifying file permissions for creating :,",issues)
            error_msg = "Permission issues: " + "; ".join(issues)
            logger.error(error_msg)
            return {"msg": error_msg, "error": True}

        # Validate inputs
        if not all([vcenter_url, port, username, password, cluster_id]):
            error_msg = "Missing required parameters"
            logger.error(error_msg)
            return {"msg": error_msg, "error": True}

        # Convert insecure_skip_verify to lowercase string boolean
        insecure_skip_verify = str(insecure_skip_verify).lower()
        
        # Construct vCenter URL
        vcenter_full_url = f"https://{vcenter_url}:{port}/sdk"
        
        # Create configuration content
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
        
        # Read metrics from metrics.txt
        try:
            with open(metrics_file_path, "r") as f:
                metrics = f.readlines()
            logger.info(f"Read {len(metrics)} metrics from metrics.txt")
        except Exception as e:
            error_msg = f"Error reading metrics file: {str(e)}"
            logger.error(error_msg)
            return {"msg": error_msg, "error": True}

        # Add metrics to configuration
        for metric in metrics:
            config_content += f"  {metric.strip()}\n"
        
        config_content += f"\n# vCenter Configuration end {cluster_id}\n"

        # Append to telegraf.conf
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
    """Delete vsphere input plugin configuration"""
    try:
        # Verify file permissions first
        issues = verify_file_permissions()
        if issues:
            print("issues deleting:", issues)
            error_msg = "Permission issues: " + "; ".join(issues)
            logger.error(error_msg)
            return {"msg": error_msg, "error": True}

        # Read current configuration
        with open(conf_file_path, "r") as f:
            lines = f.readlines()
            original_content = "".join(lines)

        # Search for the configuration block
        start_comment = f"# vCenter Configuration start {cluster_id}"
        end_comment = f"# vCenter Configuration end {cluster_id}"
        
        start_index = None
        end_index = None

        # Look for the comments, handling potential whitespace
        for i, line in enumerate(lines):
            if start_comment in line.strip():
                start_index = i
            elif end_comment in line.strip() and start_index is not None:
                end_index = i
                break

        if start_index is not None and end_index is not None:
            # Create backup of original content
            backup_path = f"{conf_file_path}.bak"
            with open(backup_path, "w") as f:
                f.write(original_content)
            
            # Delete the configuration block
            del lines[start_index:end_index + 1]

            # Write the modified content
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
    """Modify vsphere input plugin configuration"""
    try:
        # Verify file permissions first
        issues = verify_file_permissions()
        if issues:
            error_msg = "Permission issues: " + "; ".join(issues)
            logger.error(error_msg)
            return {"msg": error_msg, "error": True}

        # Validate inputs
        if not all([vcenter_url, new_username, new_password, new_port, cluster_id]):
            error_msg = "Missing required parameters"
            logger.error(error_msg)
            return {"msg": error_msg, "error": True}

        # Read current configuration
        with open(conf_file_path, "r") as f:
            lines = f.readlines()
            original_content = "".join(lines)

        # Search for the configuration block
        start_comment = f"# vCenter Configuration start {cluster_id}"
        end_comment = f"# vCenter Configuration end {cluster_id}"
        
        start_index = None
        end_index = None

        # Look for the comments, handling potential whitespace
        for i, line in enumerate(lines):
            if start_comment in line.strip():
                start_index = i
            elif end_comment in line.strip() and start_index is not None:
                end_index = i
                break

        if start_index is not None and end_index is not None:
            # Create backup of original content
            backup_path = f"{conf_file_path}.bak"
            with open(backup_path, "w") as f:
                f.write(original_content)

            # Modify the configuration lines
            for i in range(start_index, end_index + 1):
                line = lines[i]
                if "username" in line:
                    lines[i] = f'  username = "{new_username}"\n'
                elif "password" in line:
                    lines[i] = f'  password = "{new_password}"\n'
                elif "vcenters" in line:
                    lines[i] = f'  vcenters = ["https://{vcenter_url}:{new_port}/sdk"]\n'

            # Write the modified content
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
