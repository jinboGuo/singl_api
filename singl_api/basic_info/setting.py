# coding:utf-8
import os
import time
from util.logs import Logger
from util.Open_DB import MYSQL
from util.encrypt import encrypt_rf

log = Logger().get_log()
'''执行api脚本用例py'''
pattern = ["execute_cases.py", "execute_dsp_cases.py", "execute_dw_cases.py", "execute_compass_cases.py", "execute_qa_cases.py", "execute_collect_cases.py", "execute_alarm_cases.py","execute_indicator_cases.py"]
begin_times = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
BASE_PATH = os.path.dirname(os.path.dirname(__file__))
REPORT_PATH = os.path.join(BASE_PATH, 'Reports')
result_path = os.path.join(REPORT_PATH, "api_cases_report.html")
'''发送者账号'''
email_user = 'ruifan_apitest@163.com'
'''发送者密码'''
email_pwd = 'ruifantest2018'
'''163邮箱smtp服务器'''
host_server = "smtp.163.com"
'''发件人163邮箱的授权码'''
pwd = "OVTVEQJNXXUEKTVV"

"""
邮件接收地址
"""
receivers_list = ['jinbo.guo@inforefiner.com', 'qian.feng@inforefiner.com', 'zhiming.wang@inforefiner.com']
receivers_test = ["jinbo.guo@inforefiner.com"]

Compass_scheduler = {
    'HOST': '192.168.1.55',
    "USER": 'root',
    "PASSWORD": 'Inf0refiner'
}
hdfs_url = "hdfs://into1:8020"
datasource_url = "jdbc:mysql://192.168.1.82:3306/auto_apitest"

"""
-------95-comapss环境使用-------
 sheet_name:scheduler
 HOST: "http://192.168.1.95:8515"
 Compass_MySQL_CONFIG:数据库连接信息
"""
compass_cases_dir = os.path.join(os.path.abspath('.'), 'all_version_cases\\api_cases_1.6.x.xlsx').replace('\\', '/')
compass_sheet = "scheduler" #scheduler
compass_host = "http://192.168.1.62:8515"
Compass_MySQL_CONFIG = {
    'HOST': '192.168.1.67',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_168',
    'case_db': 'test'}

"""
-------95-alarm环境使用-------
 sheet_name:alarm
 HOST: "http://192.168.1.95:8515"
 alarm_MySQL_CONFIG:数据库连接信息
"""
alarm_cases_dir = os.path.join(os.path.abspath('.'), 'all_version_cases\\api_cases_1.6.x.xlsx').replace('\\', '/')
alarm_sheet = "alarm"  #alarm
alarm_host = "http://192.168.1.62:8515"
Alarm_MySQL_CONFIG = {
    'HOST': '192.168.1.67',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_168'
}

"""
-------95-dsp环境使用-------
 sheet_name:dsp
 HOST: "http://192.168.1.95:8515"
 Dsp_MySQL_CONFIG:数据库连接信息
"""
dsp_cases_dir = os.path.join(os.path.abspath('.'), 'all_version_cases\\api_cases_1.6.x.xlsx').replace('\\', '/')
dsp_sheet = "dsp"
dsp_host = "http://192.168.1.62:8515"
Dsp_MySQL_CONFIG = {
    'HOST': '192.168.1.67',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_168'
}

"""
-------95-dw环境使用-------
 sheet_name:dw-asset
 HOST: "http://192.168.1.95:8515"
 Dw_MySQL_CONFIG:数据库连接信息
"""
dw_cases_dir = os.path.join(os.path.abspath('.'), 'all_version_cases\\api_cases_1.6.x.xlsx').replace('\\', '/')
dw_sheet = "dw-asset"  #dw-asset
dw_host = "http://192.168.1.62:8515"
Dw_MySQL_CONFIG = {
    'HOST': '192.168.1.67',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_168',
    'case_db': 'test'}

"""
-------95-collect环境使用-------
 sheet_name:collect
 HOST: "http://192.168.1.95:8515"
 Dw_MySQL_CONFIG:数据库连接信息
"""
collect_cases_dir = os.path.join(os.path.abspath('.'), 'all_version_cases\\api_cases_1.6.x.xlsx').replace('\\', '/')
collect_sheet = "collect"
collect_host = "http://192.168.1.62:8515"
collect_MySQL_CONFIG = {
    'HOST': '192.168.1.67',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_168',
    'case_db': 'test'}

"""
-------95-indicator环境使用-------
 sheet_name:indicator
 HOST: "http://192.168.1.95:8515"
 Dw_MySQL_CONFIG:数据库连接信息
"""
indicator_cases_dir = os.path.join(os.path.abspath('.'), 'all_version_cases\\api_cases_1.6.x.xlsx').replace('\\', '/')
indicator_sheet = "indicator"
indicator_host = "http://192.168.1.62:8515"
indicator_MySQL_CONFIG = {
    'HOST': '192.168.1.67',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_168',
    'case_db': 'test'}

"""
-------95-quality环境使用-------
 sheet_name:quality
 HOST: "http://192.168.1.95:8515"
 quality_MySQL_CONFIG:数据库连接信息
"""
quality_cases_dir = os.path.join(os.path.abspath('.'),'all_version_cases\\api_cases_1.6.x.xlsx').replace('\\', '/')
quality_sheet = "quality"
quality_host = "http://192.168.1.62:8515"
quality_MySQL_CONFIG = {
    'HOST': '192.168.1.67',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_168',
    'case_db': 'test'}

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

"""获取数据库连接"""
ms_conn = MYSQL(MySQL_CONFIG1["HOST"], MySQL_CONFIG1["USER"], MySQL_CONFIG1["PASSWORD"], MySQL_CONFIG1["DB"],
                MySQL_CONFIG1["PORT"])

"""
-------95-baymax环境使用-------
 baymax_cases_dir:case文件的地址
 baymax_sheet:case文件的sheet页名称
 HOST: "http://192.168.1.95:8515"
 MySQL_CONFIG:数据库的连接配置，需要根据不同环境进行变更
"""
baymax_cases_dir = os.path.join(os.path.abspath('.'), 'all_version_cases\\api_cases_1.6.x.xlsx').replace('\\', '/')
baymax_sheet = "baymax_master"  # baymax_master
host = "http://192.168.1.62:8515"
MySQL_CONFIG = {
    'HOST': '192.168.1.67',
    "PORT": 3306,
    "USER": 'merce',
    "PASSWORD": 'merce',
    "DB": 'merce_168_95',
    'case_db': 'test'}

"""获取数据库连接"""
ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"],
           MySQL_CONFIG["PORT"])

"""elasticsearch集群服务器的地址"""
ES = ['http://192.168.1.95:9200/']

"""租户信息"""
tenant_name = "default"

""" 资源目录类型"""
resource_type = ["datasource_dir", "dataset_dir", "schema_dir", "flow_dir", "poseidon_collect_dir", "poseidon_task_dir",
                 "storage_dir", "jobview_dir", "assets_dir", "dataresource_dir", "datasafe_job_dir", "fileset_dir",
                 "standard_dir", "schema_collect_task_dir", "qa_job_dir", "schema_name_rule_dir", "tag_dir",
                 "indicator_dir", "indicator_dim_dir"]

"""元数据相关信息"""
data_source = ["datasource_id", "datasource_name", "schema_id", "schema_name", "dataset_id", "dataset_name",
               "tenant_id", "owner"]

"""标签检索类型"""
tag_type = ["like", "EQUAL"]

"""任务视图名称"""
job_view_name = "gjb_scheduler"

"""元数据采集任务名称"""
schema_collect_name = "gjb_schema_collect"

"""数据采集任务名称"""
collect_task_name = "gjb_collect_task"

"""数据质量任务名称"""
qa_task_name = "gjb_qa_task"

"""数据资源名称"""
dsp_data_source_name = "gjb_dsp_push_datasource"

"""数据资源相关信息"""
dsp_data_source = ["data_source_id", "data_source_name"]

""" 审批类型"""
approval_type = ["ASSETS_DIRECTORY", "DIMENSION_TABLE", "DATA_APPLICATION", "POSEIDON_TASK", "DATA_GRANT_RECORD",
                 "SDS_JOB", "STANDARD_JOB", "INDICATOR", "SUPPLEMENT", "DATATIER", "SUBJECTDOMAIN", "META_MODEL",
                 "DATA_DATARES", "QUALITY_JOB", "SCHEMA_COLLECT_TASK"]

"""数仓资产名称"""
dw_name = ["gjb_test_asset","gjb_dw_datatier","gjb_dw_subjectdomain","gjb_dw_dicgroup","gjb_dw_dic"]

"""数仓资产类型"""
dw_type= ["dic_group_id","dic_group_name","dw_datatier_id","dw_datatier_name","dw_subjectdomain_id","dw_subjectdomain_name","dic_id","dic_name","meta_data_id"]

"""dataflow名称"""
dataflow_name = ["gjb_type_dataflow","gjb_type_rtcflow"]

"""scheduler名称"""
scheduler_name=['mutil_sink_storage','multi_rtc_steps']

"""
compass admin账户登录信息
"""
MY_LOGIN_INFO_compass = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic YmF5bWF4OjEyMzQ1Ng=='},
    "URL": "%s/api/auth/oauth/token" % compass_host,
    "DATA": {'name': 'admin', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default',
             'grant_type': 'manager_password', "remember": False},
    "DATA_ERROR_NAME": {'name': encrypt_rf('roo'), 'password': encrypt_rf('123456'),
                        'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('root')},
    "HOST": "%s" % compass_host
}

"""
1.6-admin账户登录信息
"""
MY_LOGIN_INFO_DAM = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic YmF5bWF4OjEyMzQ1Ng==',
                'Accept': 'application/json'},
    "URL": "%s/api/auth/oauth2/token" % host,
    "DATA": {'username': 'a$a67fba7b9f50eca1677f50c0d7eb0993~', 'password': 'a$0615f89cbee023498ebc2e31cc2c8fca~',
             'version': 'Baymax-3.0.0.23-20180606', 'tenant': tenant_name, 'grant_type': 'manager_password'},
    "DATA_ERROR_NAME": {'username': 'adminm', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606',
                        'tenant': 'default', 'grant_type': 'manager_password'},
    "HOST": "%s" % host
}

"""
root使用
"""
MY_LOGIN_INFO_ROOT = {
    "HEADERS": {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': 'Basic YmF5bWF4OjEyMzQ1Ng==',
                'Accept': 'application/json'},
    "URL": "%s/api/auth/oauth/token" % host,
    "DATA": {'username': 'a$1ff3cf01c2f095d0ab2bf9dd3bf00260~', 'password': 'a$0615f89cbee023498ebc2e31cc2c8fca~',
             'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default', 'grant_type': 'manager_password'},
    "DATA_ERROR_NAME": {'username': 'adminm', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606',
                        'tenant': 'default', 'grant_type': 'manager_password'},
    "HOST": "%s" % host
}