# coding:utf-8
import os
from util.encrypt import encrypt_rf


BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__name__)))
DATA_PATH = os.path.join(BASE_PATH, 'test_cases')
REPORT_PATH = os.path.join(BASE_PATH, 'report')
email_user = 'ruifan_apitest@163.com'   '''发送者账号'''
email_pwd = 'ruifantest2018'       '''发送者密码'''
email_list = {
    "guojinbo": "jinbo.guo@inforefiner.com",
}
email_to = {
    "fengqian": "qian.feng@inforefiner.com",
    "daming": "zhiming.wang@inforefiner.com",
    "guojinbo": "jinbo.guo@inforefiner.com",
}


Compass_scheduler = {
    'HOST': '192.168.1.55',
    "USER": 'root',
    "PASSWORD": 'Inf0refiner'
}
hdfs_url = "hdfs://into1:8020"
#hdfs_url = "hdfs://mycluster"


"""
-------95-comapss环境使用-------
 sheet_name:scheduler
 HOST: "http://192.168.1.95:8515"
 Compass_MySQL_CONFIG:数据库连接信息
"""
compass_sheet = "scheduler"
compass_host = "http://192.168.1.95:8515"
Compass_MySQL_CONFIG = {
    'HOST': '192.168.1.67',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_156',
    'case_db': 'test'}


"""
-------95-dsp环境使用-------
 sheet_name:dsp
 HOST: "http://192.168.1.95:8515"
 Dsp_MySQL_CONFIG:数据库连接信息
"""
dsp_sheet = "dsp"
dsp_host = "http://192.168.1.95:8515"
Dsp_MySQL_CONFIG = {
     'HOST': '192.168.1.67',
     "PORT": 3306,
     "USER": 'merce',
     "PASSWORD": 'merce',
     "DB": 'merce_156'  # merce-scheduler
 }


"""
-------95-dw环境使用-------
 sheet_name:dw-asset
 HOST: "http://192.168.1.95:8515"
 Dw_MySQL_CONFIG:数据库连接信息
"""
dw_sheet = "dw-asset"
dw_host = "http://192.168.1.95:8515"
Dw_MySQL_CONFIG = {
    'HOST': '192.168.1.67',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_156',
    'case_db': 'test'}


"""
-------k8s环境使用-------
sheet name baymax_sheet = "k8s_149"
host = "http://192.168.1.145:40001"
MySQL_CONFIG = {
    'HOST': '192.168.1.145',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'baymax_test',
    'case_db': 'test'}
"""


"""
测试数据库的连接配置
"""
MySQL_CONFIG1 = {
    'HOST': '192.168.1.82',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'test',
    'case_db': 'test'}

"""
-------95-baymax环境使用-------
 sheet_name:baymax_master
 HOST: "http://192.168.1.95:8515"
 MySQL_CONFIG:数据库的连接配置，需要根据不同环境进行变更
"""
baymax_master = "baymax_master"
host = "http://192.168.1.95:8515"
MySQL_CONFIG = {
    'HOST': '192.168.1.67',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_156',
    'case_db': 'test'}

# """
# -------65-baymax环境使用-------
#  sheet_name:baymax_master
#  HOST: "http://192.168.1.65:8515"
#  MySQL_CONFIG:数据库的连接配置，需要根据不同环境进行变更
# """
# baymax_master = "baymax_master"
# host = "http://192.168.1.65:8515"
# MySQL_CONFIG = {
#     'HOST': '192.168.1.67',
#     "PORT": 3306,
#     "USER": 'merce',
#     "PASSWORD": 'merce',
#     "DB": 'merce_65',
#     'case_db': 'test'}


"""
租户信息
"""
tenant_id_65 = "1135945124792557568"
tenant_id_95 = "1093978681540575232"
tenant_id_62 = "1126179310354137088"
tenant_id_220 = "1135540528424501248"
tenant_id_82 = "926463668147716096"
tenant_id_61 = "1056906556591271936"
tenant_id_145 = "a5a4b81e-d2a6-498d-9ff0-3a627d3d5b5a"


"""
compass admin账户登录信息
"""
MY_LOGIN_INFO_compass = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded','Authorization':'Basic YmF5bWF4OjEyMzQ1Ng=='},
    "URL": "%s/api/auth/oauth/token" % compass_host,
    "DATA": {'name': 'admin', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default','grant_type': 'manager_password',"remember": False},
    "DATA_ERROR_NAME": {'name': encrypt_rf('roo'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('root')},
    "HOST": "%s" % compass_host
}

"""
dsp customer账户登录信息
"""
MY_LOGIN_INFO_dsp_customer = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic YmF5bWF4OjEyMzQ1Ng==','Accept': 'application/json'},
    "URL": "%s/api/auth/oauth/token" % dsp_host,
    "DATA": {'username': 'a$a67fba7b9f50eca1677f50c0d7eb0993~', 'password': 'a$0615f89cbee023498ebc2e31cc2c8fca~', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type':'manager_password'},
    "DATA_ERROR_NAME": {'username': 'customer3', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default',
                        'grant_type': 'customer_password'},
    "HOST": "%s" % dsp_host
}

"""
dsp admin账户登录信息
"""
MY_LOGIN_INFO_dsp_admin = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic YmF5bWF4OjEyMzQ1Ng==','Accept': 'application/json'},
    "URL": "%s/api/auth/oauth/token" % dsp_host,
    "DATA": {'username': 'a$a67fba7b9f50eca1677f50c0d7eb0993~', 'password': 'a$0615f89cbee023498ebc2e31cc2c8fca~', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type':'manager_password'},
    "DATA_ERROR_NAME": {'username': 'adminm', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type': 'manager_password'},
    "HOST": "%s" % dsp_host
}

"""
1.4-admin账户登录信息
"""
MY_LOGIN_INFO_dw = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic YmF5bWF4OjEyMzQ1Ng==','Accept': 'application/json'},
    "URL": "%s/api/auth/oauth/token" % dw_host,
    "DATA": {'username': 'a$a67fba7b9f50eca1677f50c0d7eb0993~', 'password': 'a$0615f89cbee023498ebc2e31cc2c8fca~', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type':'manager_password'},
    "DATA_ERROR_NAME": {'username': 'adminm', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606','tenant': 'default', 'grant_type': 'manager_password'},
    "HOST": "%s" % dw_host
}

"""
1.5-admin账户登录信息
"""
MY_LOGIN_INFO_DAM = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic YmF5bWF4OjEyMzQ1Ng==', 'Accept': 'application/json'},
    "URL": "%s/api/auth/oauth/token" % host,
    "DATA": {'username': 'a$a67fba7b9f50eca1677f50c0d7eb0993~', 'password': 'a$0615f89cbee023498ebc2e31cc2c8fca~', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type':'manager_password'},
    "DATA_ERROR_NAME": {'username': 'adminm', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type': 'manager_password'},
    "HOST": "%s" % host
}

"""
root使用
"""
MY_LOGIN_INFO_ROOT = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic YmF5bWF4OjEyMzQ1Ng==', 'Accept': 'application/json'},
    "URL": "%s/api/auth/oauth/token" % host,
    "DATA": {'username': 'a$1ff3cf01c2f095d0ab2bf9dd3bf00260~', 'password': 'a$0615f89cbee023498ebc2e31cc2c8fca~', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type':'manager_password'},
    "DATA_ERROR_NAME": {'username': 'adminm', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type': 'manager_password'},
    "HOST": "%s" % host
}


"""
elasticsearch集群服务器的地址
"""
ES = ['http://192.168.1.95:9200/']


"""
邮件接收地址
"""
receivers_list = ['jinbo.guo@inforefiner.com', 'qian.feng@inforefiner.com', 'zhiming.wang@inforefiner.com']
receivers_test = ['jinbo.guo@inforefiner.com']