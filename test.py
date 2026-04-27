# # import httpx
# # url = "http://172.16.1.31:8765/v1/hyper-v/get_node_status_from_cluster"
# # response = httpx.get(url)
# # data = response.json()
# data = {
#   "status": "OK",
#   "code": 200,
#   "msg": "Node Status from Cluster",
#   "data": [
#     {
#       "Node_name": "hyperv-sr-1",
#       "Status": "Up",
#       "IP": "172.16.1.31",
#       "VMCount": 1
#     },
#     {
#       "Node_name": "hyperv-sr-2",
#       "Status": "Up",
#       "IP": "172.16.1.32",
#       "VMCount": 0
#     }
#   ]
# }
# node_name = ''
# node_ip = ''
# vm_count = 1
# raw_data = data.get('data')

# for i in range(len(raw_data)):
#     if raw_data[i].get('Status')=='Up' and raw_data[i].get('VMCount') <= vm_count:
#         vm_count = raw_data[i].get('VMCount')
#         node_ip = raw_data[i].get('IP')
#         node_name = raw_data[i].get('Node_name')

# print(node_ip,vm_count,node_name)


url = "http://172.16.1.31:8765"

print(url.split('//')[1].split(':')[1])
