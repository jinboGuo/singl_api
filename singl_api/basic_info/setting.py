# coding:utf-8
import os
from util.encrypt import encrypt_rf


BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__name__)))
DATA_PATH = os.path.join(BASE_PATH, 'test_cases')
REPORT_PATH = os.path.join(BASE_PATH, 'report')
email_user = 'ruifan_apitest@163.com'  # 发送者账号
email_pwd = 'ruifantest2018'       # 发送者密码
email_list = {
    "guojinbo": "jinbo.guo@inforefiner.com",
}
email_to = {
    "fengqian": "qian.feng@inforefiner.com",
    "daming": "zhiming.wang@inforefiner.com",
    "guojinbo": "jinbo.guo@inforefiner.com",
}

ws_url = "ws://192.168.1.55:8020/compass/shell/executor"  # 远程执行脚本url
# 远程执行脚本发送data
exec_data = {"operation": "execute", "shellCommand":{"path":"/app/ruifan/baymax-commander/commander-scheduler","fileName":"createDataflow.sh","password":"Inf0refiner","serverIp":"192.168.1.55","username":"root"}}
Compass_scheduler = {
    'HOST': '192.168.1.55',
    "USER": 'root',
    "PASSWORD": 'Inf0refiner'
}
hdfs_url = "hdfs://into1:8020"
# # -------comapss环境使用-------
# 脚本sheet name
compass_sheet = "scheduler"  # compass scheduler
# # HOST
compass_host = "http://192.168.1.65:8515"
# # # # # 数据库连接信息
Compass_MySQL_CONFIG = {
    'HOST': '192.168.1.63',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_65',
    'case_db': 'test'}

# # -------dsp环境使用-------
# 脚本sheet name
dsp_sheet = "dsp"
# # # HOST
dsp_host = "http://192.168.1.62:8515"
# # # # # 数据库连接信息
Dsp_MySQL_CONFIG = {
     'HOST': '192.168.1.63',
     "PORT": 3306,
     "USER": 'merce',
     "PASSWORD": 'merce',
     "DB": 'merce_62'  # merce-scheduler
 }

# 脚本sheet name
# dsp_sheet="dsp"
# # # HOST
# dsp_host = "http://192.168.1.83:8008"
# # # # # # 数据库连接信息
# Dsp_MySQL_CONFIG = {
#     'HOST': '192.168.1.82',
#     "PORT": 3306,
#     "USER": 'merce',
#     "PASSWORD": 'merce',
#     "DB": 'merce-scheduler'
# }

# -------62-dw环境使用-------
# 脚本sheet name
# dw_sheet = "dw-asset"
# # # # HOST
# dw_host = "http://192.168.1.62:8515"
# # # # # # 数据库连接信息
# Dw_MySQL_CONFIG = {
#     'HOST': '192.168.1.62',
#     "PORT": 3306,
#     "USER": 'merce',
#     "PASSWORD": 'merce',
#     "DB": 'merce_62'
# }

# -------65-dw环境使用-------
# 脚本sheet name
dw_sheet = "dw-asset"  # dw-asset
# # HOST
dw_host = "http://192.168.1.65:8515"
# # # # # 数据库连接信息
Dw_MySQL_CONFIG = {
    'HOST': '192.168.1.63',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_65',
    'case_db': 'test'}

# -------k8s环境使用-------
# 脚本sheet name
# baymax_sheet = "k8s_149"
# HOST
# host = "http://192.168.1.145:40001"
# 数据库的连接配置，需要根据不同环境进行变更
MySQL_CONFIG1 = {
    'HOST': '192.168.1.82',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'auto_apitest',
    'case_db': 'test'}

# MySQL_CONFIG = {
#     'HOST': '192.168.1.145',
#     "PORT": 30307,
#     "USER": 'root',
#     "PASSWORD": 'root',
#     "DB": 'baymax_test',
#     'case_db': 'test'}


# -------baymax master环境使用-------
# 脚本sheet name
baymax_master = "baymax_master"  # "baymax_master"
# HOST
host = "http://192.168.1.62:8515"
# 数据库的连接配置，需要根据不同环境进行变更
MySQL_CONFIG = {
    'HOST': '192.168.1.63',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_62',
    'case_db': 'test'}

tenant_id_65 = "1013879801501769728"  #
tenant_id_81 = "55f7f910-b1c9-41d2-9771-e734e6b8285f"  # 81环境default租戶ID
tenant_id_199 = "39823d2e-7998-4d0e-a3e7-5edeecba0dc2"
tenant_id_62 = "966715467089575936"
tenant_id_83 = "e5188f23-d472-4b2d-9cfa-97a0d65994cf"
tenant_id_82 = "926463668147716096"
tenant_id_123 = 'db09f359-1e4d-4b3c-872e-7775bd8eed8b'
tenant_id_84 = "e5188f23-d472-4b2d-9cfa-97a0d65994cf"
tenant_id_145 = "a5a4b81e-d2a6-498d-9ff0-3a627d3d5b5a"

# compass admin账户登录信息
MY_LOGIN_INFO_compass = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded','Authorization':'Basic YmF5bWF4OjEyMzQ1Ng=='},
    "URL": "%s/api/auth/oauth/token" % compass_host,
    "DATA": {'name': 'admin', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default','grant_type': 'manager_password',"remember": False},
    "DATA_ERROR_NAME": {'name': encrypt_rf('roo'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('root')},
    "HOST": "%s" % compass_host
}

# dsp customer账户登录信息
MY_LOGIN_INFO_dsp_customer = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', "Authorization": 'Basic Y3VzdG9tZXI6MTIzNDU2', "Accept": "application/json"},
    "URL": "%s/api/auth/oauth/token" % dsp_host,
    "DATA": {'username': 'customer3', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default','grant_type':'customer_password'},
    "DATA_ERROR_NAME": {'username': 'customer3', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default',
                        'grant_type': 'customer_password'},
    "HOST": "%s" % dsp_host
}

# dsp admin账户登录信息
MY_LOGIN_INFO_dsp_admin = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic ZHNwOjEyMzQ1Ng==', 'Accept': 'application/json'},
    "URL": "%s/api/auth/oauth/token" % dsp_host,
    "DATA": {'username': 'admin', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type':'manager_password'},
    "DATA_ERROR_NAME": {'username': 'adminm', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type': 'manager_password'},
    "HOST": "%s" % dsp_host
}


# base64加密使用该账户 admin，HOST信息和环境信息保持一致
MY_LOGIN_INFO2 = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded'},
    "URL": "%s/api/auth/login" % host,
    "DATA": {'name': 'AES(414e48388a4ac93aa9707d5ac1b0d441)', 'password': 'AES(38cbd271dcecd23763faed1c4e6c9b37)', 'version': 'AES(13c1901e638cd722c268d21f45bf08ae)','tenant': 'AES(45cf55246a66a7817cca335b7b254312)'},
    "DATA_ERROR_NAME": {'name': encrypt_rf('adminn'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')},
    "HOST": "%s" % host
}

# 1.4-admin账户登录信息
MY_LOGIN_INFO_dw = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic YmF5bWF4OjEyMzQ1Ng==','Accept': 'application/json'},
    "URL": "%s/api/auth/oauth/token" % dw_host,
    "DATA": {'username': 'a$13ec4fe486e87d0b1145f2248a090db5~', 'password': 'a$3cde4fd05c58aee9937bfb2db12c9a91~','version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type': 'manager_password'},
    "DATA_ERROR_NAME": {'username': 'adminm', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606','tenant': 'default', 'grant_type': 'manager_password'},
    "HOST": "%s" % dw_host
}

# 1.2.4-admin账户登录信息
MY_LOGIN_INFO_root = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic YmF5bWF4OjEyMzQ1Ng==', 'Accept': 'application/json'},
    "URL": "%s/api/auth/oauth/token" % host,
    "DATA": {'username': 'a$13ec4fe486e87d0b1145f2248a090db5~', 'password': 'a$3cde4fd05c58aee9937bfb2db12c9a91~', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type':'manager_password'},
    "DATA_ERROR_NAME": {'username': 'adminm', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type': 'manager_password'},
    "HOST": "%s" % host
}

# AES加密方式使用以下登录信息

# DAM1.0不同的加密方式
MY_LOGIN_INFO_dam = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded'},
    "URL": "%s/api/auth/login" % host,
    "DATA": {'name': '13ec4fe486e87d0b1145f2248a090db5', 'password': '3cde4fd05c58aee9937bfb2db12c9a91', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': '1463a3ec85fbfbeb2fe07183d7518a48'},
    "DATA_ERROR_NAME": {'name': encrypt_rf('adminn'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')},
    "HOST": "%s" % host
}

#DAM1.0使用
MY_LOGIN_INFO_root_dam = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded'},
    "URL": "%s/api/auth/login" % host,
    "DATA": {'name': '5a8987fa3b9573f0708fe61f30fd2393', 'password': '3cde4fd05c58aee9937bfb2db12c9a91', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': '5a8987fa3b9573f0708fe61f30fd2393'},
    "DATA_ERROR_NAME": {'name': encrypt_rf('roo'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('root')},
    "HOST": "%s" % host
}

dataset_resource = {"id": "39386f75-9b28-43a6-a6bf-bd5e0e85d437"}
schema_resource = {"id": "9123ca72-ebd1-422b-b8b0-e150b7c69dc5"}

# ----------added by bingjie-----------------------------
# 创建flow和schedulers时可以使用的schema和dataset
schema_id = "6e1cf4b1-da97-4305-afe8-ed567b3ebe68"  # students_schema
dataset_id = "0f22c4ce-ce02-464d-a0e3-7f9fb430b6b2"  # students_dataset
dataset_for_sink_id = ""
# 查询scheduler时使用的name
scheduler_name = "students_flow39806"
# 查询schedulers时使用的id（和"students_flow39806"是同一个）
scheduler_id = "7988fdfe-df93-4249-9fdf-fe734f2b384c"
# 查询flow时使用的id： flow_name = students_flow
flow_id = "35033c8d-fadc-4628-abf9-6803953fba34"
# -----------------------------------------------------
# 查询flow时使用的resource的id
Flows_resourceid = "8cb5f399-ec5d-4236-98d3-88f0d1d19d2b"
# ------------------------------------------------------

# 给定一个flow_id的列表，使用{"flow_name":"flow_id"}的方式存储，在创建scheduler时循环调用---对比execution result时使用该list
flow_id_list = [{"flow_id": "35033c8d-fadc-4628-abf9-6803953fba34", "o_dataset": "0c012cd7-c4ad-4c3b-bfa0-5ece5cf293d9"},
                {"flow_id": "f2677db1-6923-42a1-8f18-f8674394580a","o_dataset":"b896ff9d-691e-4939-a860-38eb828b1ad2"}
                ]
flow_id_json = [{'flow_id': '35033c8d-fadc-4628-abf9-6803953fba34',
                 'dataset_json':
                     [{'subject': 'subject', 'grade': 'grade', 'name': 'name', 'id': 'id'},
                      {'subject': '语文', 'grade': '89', 'name': '张三', 'id': '1'},
                      {'subject': '英语', 'grade': '85', 'name': '张三', 'id': '2'},
                      {'subject': '数学', 'grade': '95', 'name': '李四', 'id': '3'},
                      {'subject': '英语', 'grade': '65', 'name': '李四', 'id': '4'},
                      {'subject': '语文', 'grade': '35', 'name': '李四', 'id': '5'},
                      {'subject': '数学', 'grade': '58', 'name': '小明', 'id': '6'},
                      {'subject': '英语', 'grade': '96', 'name': '小明', 'id': '7'},
                      {'subject': '语文', 'grade': '96', 'name': '小明', 'id': '8'},
                      {'subject': '数学', 'grade': '85', 'name': '小红', 'id': '9'},
                      {'subject': '英语', 'grade': '95', 'name': '小红', 'id': '10'},
                      {'subject': '语文', 'grade': '78', 'name': '小红', 'id': '11'},
                      {'subject': '数学', 'grade': '98', 'name': '小玲', 'id': '12'},
                      {'subject': '语文', 'grade': '46', 'name': '小玲', 'id': '13'},
                      {'subject': '英语', 'grade': '78', 'name': '小玲', 'id': '14'},
                      {'subject': '数学', 'grade': '68', 'name': '张三', 'id': '15'}]},
                {'flow_id': 'f2677db1-6923-42a1-8f18-f8674394580a',
                 'dataset_json':
                     [{'subject': 'subject', 'grade': 'grade', 'name': 'name', 'id': 'id'},
                      {'subject': '语文', 'grade': '89', 'name': '张三', 'id': '1'},
                      {'subject': '英语', 'grade': '85', 'name': '张三', 'id': '2'},
                      {'subject': '数学', 'grade': '95', 'name': '李四', 'id': '3'},
                      {'subject': '英语', 'grade': '65', 'name': '李四', 'id': '4'},
                      {'subject': '语文', 'grade': '35', 'name': '李四', 'id': '5'},
                      {'subject': '数学', 'grade': '58', 'name': '小明', 'id': '6'},
                      {'subject': '英语', 'grade': '96', 'name': '小明', 'id': '7'},
                      {'subject': '语文', 'grade': '96', 'name': '小明', 'id': '8'},
                      {'subject': '数学', 'grade': '85', 'name': '小红', 'id': '9'},
                      {'subject': '英语', 'grade': '95', 'name': '小红', 'id': '10'},
                      {'subject': '语文', 'grade': '78', 'name': '小红', 'id': '11'},
                      {'subject': '数学', 'grade': '98', 'name': '小玲', 'id': '12'},
                      {'subject': '语文', 'grade': '46', 'name': '小玲', 'id': '13'},
                      {'subject': '英语', 'grade': '78', 'name': '小玲', 'id': '14'},
                      {'subject': '数学', 'grade': '68', 'name': '张三', 'id': '15'}]}
                ]

#  used by cases_for_collectors.py
datasource_id = '2b5ff16f-ca1b-465e-8a6d-69b8b39f8d61&offset'  # datasource name : students_collector

# add by pengyuan
# 创建flow时使用的schema的name和id
idnameage_schema_name = 'idnameage'
idnameage_schema_id = '0a80565f-10ef-4bea-8563-0cb28cd0db27'
left_age_dataset_name = 'left_age'
left_age_dataset_id = '8bfcf577-ebb6-4f8d-ae43-eac671ad5364'
# 根据flowname查询流程使用的name,version
query_flow_name = 'test_df_supplement'
query_flow_version = 3
# 更新流程时使用的流程id
flow_update_id = 'cb0a37ea-de4a-495c-bae0-236fcbd08eaf'
# 根据流程id和计划id查询执行历史
flow_scheduler_id = '63b0a864-ce40-4f88-a25d-929164198087'

# add by bingjie 2019-01-23
# 创建分析模板使用的flow和dataset
preProcessFlowId = "aa5f83c6-aff0-4405-8473-8c09c0f167e4"
preProcessFlowName = "students_int_flow_filter"
processDataId = "students_dataset_copy_int"


# 创建分析任务使用的分析模板
zmod_id = ["e2dbfd88-0e2d-4fa2-b145-75c1a13ab455"]
# 分析任务执行信息execution
zmod_exectuion_id = "c462b867-db09-4adc-b49f-005c646960a6"
# API用例使用
collector_id = 'c9'

# elasticsearch集群服务器的地址
ES = [
    'http://192.168.1.82:9204/'
]

receivers_list = ['jinbo.guo@inforefiner.com', 'qian.feng@inforefiner.com', 'zhiming.wang@inforefiner.com']
receivers_test = ['jinbo.guo@inforefiner.com']