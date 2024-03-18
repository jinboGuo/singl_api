import urllib.parse
import sys, os
from basic_info.setting import MySQL_CONFIG
from util.Open_DB import MYSQL
from util.logs import Logger

log = Logger().get_log()
current_path = os.path.abspath(os.path.dirname(__file__))
root_path = os.path.split(current_path)[0]
sys.path.append(root_path)

def get_env_num(host):
    """
    :param host:
    :return: env_num
    """
    host_netloc = urllib.parse.urlparse(host).netloc
    no_port_host = host_netloc.split(":")
    env_num = no_port_host[0].split(".")[-1]
    return env_num

def get_tenant(host):
    """
    :param host:
    :return: 租户id
    """
    host_env_num = get_env_num(host)
    tenant_id_145 = "a5a4b81e-d2a6-498d-9ff0-3a627d3d5b5a"
    tenant_id_81 = "55f7f910-b1c9-41d2-9771-e734e6b8285f"
    tenant_id_62 = "958680869818597376"
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
        log.info("目前只处理62,81,82,83,84,65环境tenant，若不包含在内，请添加")

def get_owner():
    """
    :return: owner
    """
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"], MySQL_CONFIG["PORT"])
    try:
      sql = "select id from merce_user where login_id='admin' and name='admin' order by create_time desc limit 1"
      owners = ms.ExecuQuery(sql)
      if owners:
         owner = owners[0]["id"]
         return owner
      else:
          return "1013879801942171648"
    except Exception as e:
        return log.error("错误信息：%s" % e)


def get_tenant_id(tenant_name):
    """
    :return: tenant_id
    """
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"], MySQL_CONFIG["PORT"])
    try:
      sql = "select id from merce_tenant where name='%s' order by create_time desc limit 1" % tenant_name
      tenant_id = ms.ExecuQuery(sql)
      tenant_id = tenant_id[0]["id"]
      return str(tenant_id)
    except Exception as e:
        return log.error("错误信息：%s" % e)