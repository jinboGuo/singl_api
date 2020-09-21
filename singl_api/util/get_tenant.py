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
    tenant_id_82 = "db09f359-1e4d-4b3c-872e-7775bd8eed8b"
    tenant_id_83 = "fc75a4d5-72af-486b-9a48-c35aeaefedca"
    tenant_id_84 = "8c488afc-e9d7-42af-b127-f8a1412ba50e"
    tenant_id_199 = "39823d2e-7998-4d0e-a3e7-5edeecba0dc2"
    if host_env_num == '189':
        return tenant_id_189
    elif host_env_num == '81':
        return tenant_id_81
    elif host_env_num == "84":
        return tenant_id_84
    elif host_env_num == "82":
        return tenant_id_82
    elif host_env_num == "83":
        return tenant_id_83
    elif host_env_num == "199":
        return tenant_id_199
    else:
        print("目前只处理189,81,82,83,84环境tenant，若不包含在内，请添加")