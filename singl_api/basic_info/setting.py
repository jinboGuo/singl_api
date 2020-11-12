# coding:utf-8
import os
from util import Open_DB
from util.encrypt import encrypt_rf


BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__name__)))
DATA_PATH = os.path.join(BASE_PATH, 'test_cases')
REPORT_PATH = os.path.join(BASE_PATH, 'report')
email_user = 'ruifan_test@163.com'  # 发送者账号
email_pwd = 'ruifantest'       # 发送者密码
email_list = {
    "guojinbo": "jinbo.guo@inforefiner.com",
}
email_to = {
    "fengqian": "qian.feng@inforefiner.com",
    "daming": "zhiming.wang@inforefiner.com",
    "guojinbo": "jinbo.guo@inforefiner.com",
            }

# ------84环境使用--------
#  HOST
# host = "http://192.168.1.84:8515"
# # 数据库连接信息
# MySQL_CONFIG = {
#     'HOST': '192.168.1.189',
#     "PORT": 3306,
#     "USER": 'merce',
#     "PASSWORD": 'merce',
#     "DB": 'info4_merce',
#     'case_db': 'test'
# }

# ------83环境使用--------
# #  HOST
# host = "http://192.168.1.83:8515"
# # 数据库连接信息
# MySQL_CONFIG = {
#     'HOST': '192.168.1.189',
#     "PORT": 3306,
#     "USER": 'merce',
#     "PASSWORD": 'merce',
#     "DB": 'wac',
#     'case_db': 'test'
# }

# # -------189环境使用-------
# HOST
# host = "http://192.168.1.189:8515"
# # 数据库的连接配置，需要根据不同环境进行变更
# MySQL_CONFIG = {
#     'HOST': '192.168.1.199',
#     "PORT": 3306,
#     "USER": 'merce',
#     "PASSWORD": '123456',
#     "DB": 'merce',
#     'case_db': 'test'}

# # -------57环境使用-------
# # HOST
# host = "http://192.168.1.57:8515"
# # # # # 数据库连接信息
# MySQL_CONFIG = {
#     'HOST': '192.168.1.57',
#     "PORT": 3306,
#     "USER": 'merce',
#     "PASSWORD": 'merce',
#     "DB": 'merce'
# }

# MySQL_CONFIG = {
#     'HOST': '192.168.1.82',
#     "PORT": 3306,
#     "USER": 'merce',
#     "PASSWORD": '123456',
#     "DB": 'merce_199',
#     'case_db': 'test'
# }
# # -------81环境使用-------
# # HOST
# host = "http://192.168.1.81:8515"
# # # # # 数据库连接信息
# MySQL_CONFIG = {
#     'HOST': '192.168.1.57',
#     "PORT": 3306,
#     "USER": 'merce',
#     "PASSWORD": 'merce',
#     "DB": 'database_81'
# }
# # -------dsp环境使用-------
# # # HOST
dsp_host = "http://192.168.1.82:8008"
# # # # # 数据库连接信息
Dsp_MySQL_CONFIG = {
     'HOST': '192.168.1.82',
     "PORT": 3306,
     "USER": 'merce',
     "PASSWORD": 'merce',
     "DB": 'merce_83'
 }

# # # HOST
# dsp_host = "http://192.168.1.199:8008"
# # # # # # 数据库连接信息
# Dsp_MySQL_CONFIG = {
#     'HOST': '192.168.1.82',
#     "PORT": 3306,
#     "USER": 'merce',
#     "PASSWORD": 'merce',
#     "DB": 'merce_199'
# }

# # -------dw环境使用-------
# # # HOST
dw_host = "http://192.168.1.83:8515"
# # # # # 数据库连接信息
Dw_MySQL_CONFIG = {
    'HOST': '192.168.1.82',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_83'
}
# -------83环境使用-------
# HOST
host = "http://192.168.1.199:8515"
#host = "http://192.168.21.123:8515"
# 数据库的连接配置，需要根据不同环境进行变更
# MySQL_CONFIG = {
#     'HOST': '192.168.1.199',
#     "PORT": 3306,
#     "USER": 'merce',
#     "PASSWORD": '123456',
#     "DB": 'wac666',
#     'case_db': 'test'}
MySQL_CONFIG1 = {
    'HOST': '192.168.1.75',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'test_flow',
    'case_db': 'test'}
# MySQL_CONFIG = {
#     'HOST': '192.168.21.123',
#     "PORT": 3306,
#     "USER": 'merce',
#     "PASSWORD": 'merce',
#     "DB": 'demo18',
#     'case_db': 'test'}
MySQL_CONFIG = {
    'HOST': '192.168.1.82',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_199',
    'case_db': 'test'}

tenant_id_189 = "2d7ad891-41c5-4fba-9ff2-03aef3c729e5"  # 189环境default租戶ID
tenant_id_81 = "55f7f910-b1c9-41d2-9771-e734e6b8285f"  # 81环境default租戶ID
tenant_id_199 = "39823d2e-7998-4d0e-a3e7-5edeecba0dc2"
tenant_id_57 = "087e55ee-5ad4-451e-ba3d-0be93ec4546c"
tenant_id_83 = "fc75a4d5-72af-486b-9a48-c35aeaefedca"
tenant_id_82 = "db09f359-1e4d-4b3c-872e-7775bd8eed8b"
tenant_id_123 = 'db09f359-1e4d-4b3c-872e-7775bd8eed8b' #yulijiang
tenant_id_84 = "8c488afc-e9d7-42af-b127-f8a1412ba50e"

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

MY_LOGIN_INFO = {
 "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded'},
 "URL": "http://192.168.1.189:8515/api/auth/login",
 "DATA": {'name': 'gbj_use', 'password': '123456', 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': 'default'},
 "HOST": "http://192.168.1.189:8515"
}

# base64加密使用该账户 admin，HOST信息和环境信息保持一致
MY_LOGIN_INFO2 = {
 "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded'},
 "URL": "%s/api/auth/login" % host,
 "DATA": {'name': encrypt_rf('admin'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')},
 "DATA_ERROR_NAME": {'name': encrypt_rf('adminn'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')},
 "HOST": "%s" % host
}

# root账户登录信息
MY_LOGIN_INFO_root = {
 "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded'},
 "URL": "%s/api/auth/login" % host,
 "DATA": {'name': encrypt_rf('root'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('root')},
 "DATA_ERROR_NAME": {'name': encrypt_rf('roo'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('root')},
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


# # upload_file使用该用户
# MY_LOGIN_INFO_upload_file_use = {
#  "HEADERS": {'Content-Type': 'multipart/form-data; boundary=----WebKitFormBoundaryqa6thIhrtfSBTQCH'},
#  "URL": "%s/api/auth/login" % host,
#  "DATA": {'name': encrypt_rf('admin'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')},
#  "DATA_ERROR_NAME": {'name': encrypt_rf('adminn'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')},
#  "HOST": "%s" % host
# }










# login user:admin
owner = "2059750c-a300-4b64-84a6-e8b086dbfd42"
# login user:gbj_use
owner2 = "d2fee4a4-d296-4db8-9b62-46bd9bc46a94"


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

ms = Open_DB.MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
# 查询最新创建的sql分析规则id
# # sql_rule_id_sql = 'select id from merce_zrule where build_type = "Custom" and custom_type = "SQL" and ' \
# #                   'name like "rule_for_SQL_students_copy%"  order by create_time desc limit 1  '
# # sql_rule_id_list = ms.ExecuQuery(sql_rule_id_sql)
# # sql_rule_id = [item[key] for item in sql_rule_id_list for key in item]
# sql_rule_id = cases_for_analysis_model.CasesForRule().test_create_rule_SQL()
# print(sql_rule_id)

# 查询最新创建的分析规则id
rule_id_sql = 'select id from merce_zrule ORDER BY create_time desc limit 1'
rule_id_list = ms.ExecuQuery(rule_id_sql)

try:
    rule_id = rule_id_list[0]["id"]
except IndexError:
    print('没有查询到最新的分析规则，返回空')

# print(rule_id_list)

# 创建分析任务使用的分析模板
zmod_id = ["e2dbfd88-0e2d-4fa2-b145-75c1a13ab455"]
# 分析任务执行信息execution
zmod_exectuion_id = "c462b867-db09-4adc-b49f-005c646960a6"
# API用例使用
collector_id = 'c9'


#receivers_list = ['jinbo.guo@inforefiner.com', 'zhiming.wang@inforefiner.com', 'qian.feng@inforefiner.com', 'haijun.wang@inforefiner.com']  # 定时任务使用
receivers_list = ['jinbo.guo@inforefiner.com', '289332729@qq.com', 'qian.feng@inforefiner.com']
receivers_test = ['jinbo.guo@inforefiner.com', 'guojinbo2006@126.com', '289332729@qq.com', 'qian.feng@inforefiner.com']
