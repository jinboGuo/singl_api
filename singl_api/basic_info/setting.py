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


"""
-------65-comapss环境使用-------
 sheet_name:scheduler
 HOST: "http://192.168.1.65:8515"
 Compass_MySQL_CONFIG:数据库连接信息
"""
compass_sheet = "scheduler"
compass_host = "http://192.168.1.65:8515"
Compass_MySQL_CONFIG = {
    'HOST': '192.168.1.63',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_65',
    'case_db': 'test'}


"""
-------65-dsp环境使用-------
 sheet_name:dsp
 HOST: "http://192.168.1.65:8515"
 Dsp_MySQL_CONFIG:数据库连接信息
"""
dsp_sheet = "dsp"
dsp_host = "http://192.168.1.65:8515"
Dsp_MySQL_CONFIG = {
     'HOST': '192.168.1.63',
     "PORT": 3306,
     "USER": 'merce',
     "PASSWORD": 'merce',
     "DB": 'merce_65'  # merce-scheduler
 }


"""
-------65-dw环境使用-------
 sheet_name:dw-asset
 HOST: "http://192.168.1.65:8515"
 Dw_MySQL_CONFIG:数据库连接信息
"""
dw_sheet = "dw-asset"
dw_host = "http://192.168.1.65:8515"
Dw_MySQL_CONFIG = {
    'HOST': '192.168.1.63',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_65',
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
    "DB": 'auto_apitest',
    'case_db': 'test'}



"""
-------65-baymax环境使用-------
 sheet_name:baymax_master
 HOST: "http://192.168.1.65:8515"
 MySQL_CONFIG:数据库的连接配置，需要根据不同环境进行变更
"""
baymax_master = "baymax_master"
host = "http://192.168.1.65:8515"
MySQL_CONFIG = {
    'HOST': '192.168.1.63',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_65',
    'case_db': 'test'}


"""
租户信息
"""
tenant_id_65 = "1013879801501769728"
tenant_id_81 = "55f7f910-b1c9-41d2-9771-e734e6b8285f"
tenant_id_62 = "966715467089575936"
tenant_id_83 = "e5188f23-d472-4b2d-9cfa-97a0d65994cf"
tenant_id_82 = "926463668147716096"
tenant_id_84 = "e5188f23-d472-4b2d-9cfa-97a0d65994cf"
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
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', "Authorization": 'Basic Y3VzdG9tZXI6MTIzNDU2', "Accept": "application/json"},
    "URL": "%s/api/auth/oauth/token" % dsp_host,
    "DATA": {'username': 'customer3', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default','grant_type':'customer_password'},
    "DATA_ERROR_NAME": {'username': 'customer3', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default',
                        'grant_type': 'customer_password'},
    "HOST": "%s" % dsp_host
}

"""
dsp admin账户登录信息
"""
MY_LOGIN_INFO_dsp_admin = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic ZHNwOjEyMzQ1Ng==', 'Accept': 'application/json'},
    "URL": "%s/api/auth/oauth/token" % dsp_host,
    "DATA": {'username': 'admin', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type':'manager_password'},
    "DATA_ERROR_NAME": {'username': 'adminm', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type': 'manager_password'},
    "HOST": "%s" % dsp_host
}

"""
base64加密使用该账户 admin，HOST信息和环境信息保持一致
"""
MY_LOGIN_INFO2 = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded'},
    "URL": "%s/api/auth/login" % host,
    "DATA": {'name': 'AES(414e48388a4ac93aa9707d5ac1b0d441)', 'password': 'AES(38cbd271dcecd23763faed1c4e6c9b37)', 'version': 'AES(13c1901e638cd722c268d21f45bf08ae)','tenant': 'AES(45cf55246a66a7817cca335b7b254312)'},
    "DATA_ERROR_NAME": {'name': encrypt_rf('adminn'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')},
    "HOST": "%s" % host
}

"""
1.4-admin账户登录信息
"""
MY_LOGIN_INFO_dw = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic YmF5bWF4OjEyMzQ1Ng==','Accept': 'application/json'},
    "URL": "%s/api/auth/oauth/token" % dw_host,
    "DATA": {'username': 'a$13ec4fe486e87d0b1145f2248a090db5~', 'password': 'a$3cde4fd05c58aee9937bfb2db12c9a91~','version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type': 'manager_password'},
    "DATA_ERROR_NAME": {'username': 'adminm', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606','tenant': 'default', 'grant_type': 'manager_password'},
    "HOST": "%s" % dw_host
}

"""
1.2.4-admin账户登录信息
"""
MY_LOGIN_INFO_root = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic YmF5bWF4OjEyMzQ1Ng==', 'Accept': 'application/json'},
    "URL": "%s/api/auth/oauth/token" % host,
    "DATA": {'username': 'a$13ec4fe486e87d0b1145f2248a090db5~', 'password': 'a$3cde4fd05c58aee9937bfb2db12c9a91~', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type':'manager_password'},
    "DATA_ERROR_NAME": {'username': 'adminm', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type': 'manager_password'},
    "HOST": "%s" % host
}

"""
DAM1.0使用
"""
MY_LOGIN_INFO_dam = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded'},
    "URL": "%s/api/auth/login" % host,
    "DATA": {'name': '13ec4fe486e87d0b1145f2248a090db5', 'password': '3cde4fd05c58aee9937bfb2db12c9a91', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': '1463a3ec85fbfbeb2fe07183d7518a48'},
    "DATA_ERROR_NAME": {'name': encrypt_rf('adminn'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')},
    "HOST": "%s" % host
}


"""
elasticsearch集群服务器的地址
"""
ES = ['http://192.168.1.65:9200/']


"""
邮件接收地址
"""
receivers_list = ['jinbo.guo@inforefiner.com', 'qian.feng@inforefiner.com', 'zhiming.wang@inforefiner.com']
receivers_test = ['jinbo.guo@inforefiner.com']