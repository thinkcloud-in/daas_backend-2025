# --------------------------------------------------------------
# fetch_proxmox_templates_using_password.py
# --------------------------------------------------------------
# 1️⃣ Set your environment / constants (you can also export them)
# --------------------------------------------------------------
import os, requests, json, sys
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


PROXMOX_HOST = os.getenv("PROXMOX_HOST", "https://10.1.0.223:8006")
PROXMOX_USER = os.getenv("PROXMOX_USER", "root@pam")     # user@realm
PROXMOX_PASS = os.getenv("PROXMOX_PASS", "Teamw0rk@1")
VERIFY_SSL   = False                                     # set True if you have a valid cert

# --------------------------------------------------------------
# 2️⃣ Login – obtain ticket + CSRF token
# --------------------------------------------------------------
def login():
    url = f"{PROXMOX_HOST}/api2/json/access/ticket"
    payload = {"username": PROXMOX_USER, "password": PROXMOX_PASS}
    try:
        r = requests.post(url, data=payload, verify=VERIFY_SSL)
        r.raise_for_status()
    except Exception as exc:
        sys.exit(f"❌ Login failed – check user/password/host: {exc}")

    data = r.json()["data"]
    # data["ticket"] is the raw ticket value (no "PVEAuthCookie=" prefix)
    ticket_value = data["ticket"]
    csrf_token   = data["CSRFPreventionToken"]

    # Build a persistent Session with proper cookie
    sess = requests.Session()
    sess.headers.update({"CSRFPreventionToken": csrf_token})
    sess.cookies.set("PVEAuthCookie", ticket_value)
    sess.verify = VERIFY_SSL
    return sess

# --------------------------------------------------------------
# 3️⃣ Helper functions that reuse the session
# --------------------------------------------------------------
def list_nodes(sess):
    url = f"{PROXMOX_HOST}/api2/json/cluster/resources?type=node"
    r = sess.get(url)
    r.raise_for_status()

    data = r.json()["data"]
    # Proxmox returns either 'name' or 'node' as identifier; use whichever is present
    return [n.get("name") or n.get("node") for n in data]

def list_templates(sess, node, storage="local"):
    url = (
        f"{PROXMOX_HOST}/api2/json/nodes/{node}"
        f"/storage/{storage}/content?content=vztmpl"
    )
    r = sess.get(url)
    r.raise_for_status()
    # `volid` looks like "local:vztmpl/ubuntu-22.04-cloudinit.tar.gz"
    # We strip the storage prefix to get the actual template filename.
    return [t["volid"].split("/", 1)[1] for t in r.json()["data"]]

def get_node_hardware_summary(sess, node):
    # Fetch all PCI devices
    url = f"{PROXMOX_HOST}/api2/json/nodes/{node}/hardware/pci"
    r = sess.get(url)
    r.raise_for_status()
    
    # Filter for GPUs
    devices = r.json().get("data", [])
    gpus = [
        d for d in devices 
        if "VGA" in d.get("class", "") or "3D controller" in d.get("class", "")
    ]
    
    # Map GPU models to VRAM size (Your Lookup Table)
    vram_map = {"NVIDIA A100": "80GB", "RTX 3090": "24GB", "GeForce GT 1030": "2GB"}
    
    gpu_summary = []
    for g in gpus:
        model = g.get("device_name", "Unknown GPU")
        size = vram_map.get(model, "N/A")
        gpu_summary.append({"model": model, "size": size})
        
    return {
        "node": node,
        "gpu_count": len(gpu_summary),
        "gpus": gpu_summary
    }


# --------------------------------------------------------------
# 4️⃣ Main – print nodes + their OS‑templates
# --------------------------------------------------------------
if __name__ == "__main__":
    sess = login()
    try:
        
        for node in list_nodes(sess):
            try:
                templates = list_templates(sess, node)
                print(f"🖥️  Node {node} – templates: {templates}")
                print('-------------data----------------')
                res = get_node_hardware_summary(sess, node)
                print(res)
                print('---------------------------------')
            except Exception as exc:
                print(f"⚠️  Could not fetch templates for {node}: {exc}")
    finally:
        sess.close()
