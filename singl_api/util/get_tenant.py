import urllib.parse
import sys, os
from basic_info.setting import Dw_MySQL_CONFIG
from util.Open_DB import MYSQL

current_path = os.path.abspath(os.path.dirname(__file__))
root_path = os.path.split(current_path)[0]
sys.path.append(root_path)

def get_env_num(host):
    host_netloc = urllib.parse.urlparse(host).netloc
    no_port_host = host_netloc.split(":")
    env_num = no_port_host[0].split(".")[-1]
    return env_num

def get_tenant(host):
    host_env_num = get_env_num(host)
    tenant_id_145 = "a5a4b81e-d2a6-498d-9ff0-3a627d3d5b5a"  # 145环境default租戶ID
    tenant_id_81 = "55f7f910-b1c9-41d2-9771-e734e6b8285f"
    tenant_id_62 = "958680869818597376"#
    tenant_id_82 = "926463668147716096"
    tenant_id_83 = "e5188f23-d472-4b2d-9cfa-97a0d65994cf"
    tenant_id_84 = "8c488afc-e9d7-42af-b127-f8a1412ba50e"
    tenant_id_65 = "1013879801501769728"
    if host_env_num == '145':
        return tenant_id_145
    elif host_env_num == '81':
        return tenant_id_81
    elif host_env_num == "84":
        return tenant_id_84
    elif host_env_num == "82":
        return tenant_id_82
    elif host_env_num == "83":
        return tenant_id_83
    elif host_env_num == "199":
        return tenant_id_65
    elif host_env_num == "62":
        return tenant_id_62
    else:
        print("目前只处理62,81,82,83,84,65环境tenant，若不包含在内，请添加")

def get_owner():
    ms = MYSQL(Dw_MySQL_CONFIG["HOST"], Dw_MySQL_CONFIG["USER"], Dw_MySQL_CONFIG["PASSWORD"], Dw_MySQL_CONFIG["DB"], Dw_MySQL_CONFIG["PORT"])
    try:
      osql = "select id from merce_user where creator='SYSTEM' and last_modifier='admin' and name='admin' "
      owners = ms.ExecuQuery(osql)
      if owners:
         owner = owners[0]["id"]
         return owner
    except TypeError:
      return "018479ae-01a7-4603-9e1f-77e11af63f68"