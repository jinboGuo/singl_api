import urllib.parse
import sys, os

current_path = os.path.abspath(os.path.dirname(__file__))
root_path = os.path.split(current_path)[0]
sys.path.append(root_path)


def get_env_num(host):
    host_netloc = urllib.parse.urlparse(host).netloc
    # print(host_netloc)
    no_port_host = host_netloc.split(":")
    # print(no_port_host)
    env_num = no_port_host[0].split(".")[-1]
    # print(env_num)
    return env_num

def get_tenant(host):
    host_env_num = get_env_num(host)
    tenant_id_189 = "2d7ad891-41c5-4fba-9ff2-03aef3c729e5"  # 189环境default租戶ID
    tenant_id_81 = "55f7f910-b1c9-41d2-9771-e734e6b8285f"  # 81环境default租戶ID
    tenant_id_76 = "d545436b-3f53-4c89-b1b7-966faa5f2d13"
    #tenant_id_57 = "087e55ee-5ad4-451e-ba3d-0be93ec4546c"
    tenant_id_83 = "fc75a4d5-72af-486b-9a48-c35aeaefedca"
    tenant_id_84 = "8c488afc-e9d7-42af-b127-f8a1412ba50e"
    if host_env_num == '189':
        return tenant_id_189
    elif host_env_num == '81':
        return tenant_id_81
    elif host_env_num == "84":
        return tenant_id_84
    elif host_env_num == "76":
        return tenant_id_76
    elif host_env_num == "83":
        return tenant_id_83
    else:
        print("目前只处理189,81,76,57环境tenant，若不包含在内，请添加")