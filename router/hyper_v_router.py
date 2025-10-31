from fastapi import APIRouter
import controllers.hyper_v_controller as controller
import subprocess

hyper_v_router = APIRouter(prefix="/v1/hyper_v", tags=["Hyper-V"])

# ----------------------- Hyper-V ROUTES -----------------------

@hyper_v_router.get("/get_vms_from_hyperv")
async def get_vms_from_hyperv():
        return await controller.get_vms_from_hyperv()

@hyper_v_router.post("/clone_vms")
async def clone_vms():
    return await controller.clone_vms()

# Cluster create karne ka endpoint
# @hyper_v_router.post("/create-cluster/")
# def create_cluster(cluster_name: str, nodes: list[str]):
#     try:
#         command = [
#             "powershell",
#             "-Command",
#             f"New-Cluster -Name {cluster_name} -Node {','.join(nodes)} -StaticAddress 192.168.1.100"
#         ]
#         result = subprocess.run(command, capture_output=True, text=True)
#         return {"output": result.stdout, "error": result.stderr}
#     except Exception as e:
#         return {"error": str(e)}


# VM create karne ka endpoint
@hyper_v_router.post("/create-vm/")
def create_vm(vm_name: str, path: str):
    try:
        command = [
            "powershell",
            "-Command",
            f"New-VM -Name {vm_name} -MemoryStartupBytes 2GB -Path '{path}'"
        ]
        result = subprocess.run(command, capture_output=True, text=True)
        return {"output": result.stdout, "error": result.stderr}
    except Exception as e:
        return {"error": str(e)}


# VM ko cluster me assign karne ka endpoint
@hyper_v_router.post("/add-vm-to-cluster/")
def add_vm_to_cluster(cluster_name: str, vm_name: str):
    try:
        command = [
            "powershell",
            "-Command",
            f"Add-ClusterVirtualMachineRole -Cluster {cluster_name} -VMName {vm_name}"
        ]
        result = subprocess.run(command, capture_output=True, text=True)
        return {"output": result.stdout, "error": result.stderr}
    except Exception as e:
        return {"error": str(e)}
