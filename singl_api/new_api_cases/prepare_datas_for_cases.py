# coding:utf-8
import json
import os
import requests
from new_api_cases.compass_deal_parameters import deal_random
from basic_info.setting import ms, log, dataflow_name, organization_name
from util.get_deal_parameter import get_tenant_id
from util.timestamp_13 import data_now

woven_dataflow = os.path.join(os.path.abspath('.'),'attachment\\import_dataflow_steps.woven').replace('\\','/')
multi_sink_steps = os.path.join(os.path.abspath('.'),'attachment\\mutil_sink_storage.woven').replace('\\','/')
multi_rtc_steps = os.path.join(os.path.abspath('.'),'attachment\\multi_rtc_steps.woven').replace('\\','/')

def update_db_driver(data):
    try:
        sql = "select id, owner, tenant_id, creator, enabled, name, class_name, db_type, jar_name, parameterlist, process_config_type from merce_udf where name like '%s%%%%' order by create_time desc limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        parameterlist = job_info[0]["parameterlist"]
        attr_dict = json.loads(parameterlist)
        new_data = {"jarName":job_info[0]["jar_name"],"processConfigType":job_info[0]["process_config_type"],"name":job_info[0]["name"],"dbType":job_info[0]["db_type"],"className":job_info[0]["class_name"],"parameterlist":{"defaultPort":3306,"driver":"com.mysql.jdbc.Driver","name":"Mysql","paraPrefix":"?","comment":"DriverManager.getConnection(url);","paraSep":"&","url":"jdbc:mysql://[HOST]:[PORT]/[DB]","example":"jdbc:mysql://localhost:3306/test?user=root&password=&useUnicode=true&characterEncoding=gbk&autoReconnect=true&failOverReadOnly=false"},"tenantId":job_info[0]["tenant_id"],"owner":job_info[0]["owner"],"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"id":job_info[0]["id"],"version":1,"groupCount":None,"groupFieldValue":None,"returnType":None,"aliasName":None,"settings":None,"expiredPeriod":0}
        new_data['parameterlist'] = attr_dict
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)


def filesets_data(data):
    try:
        sql="select id from merce_resource_dir where creator='admin' and name='Filesets' and parent_id is NULL and path='Filesets;'"
        fileset_info = ms.ExecuQuery(sql)
        fileset_id=fileset_info[0]["id"]
        cluster_id=cluster_data()
        if "lq_fileset_hdfs_directory" in data:
            new_data = {"name": "lq_fileset_hdfs_directory_随机数", "storage": "HDFS",
                        "storageConfigurations": {"fileType": "DIRECTORY", "path": "/tmp/lisatest/collector_sink",
                                                  "clusterId": cluster_id, "cluster": "cluster1", "host": "",
                                                  "port": "", "username": "", "password": ""},
                        "resource": {"id": fileset_id}, "isShowButton": 'false'}
            deal_random(new_data)
            return new_data
        if "lq_fileset_hdfs_file" in data:
            new_data = {"name": "lq_fileset_hdfs_file_随机数", "storage": "HDFS",
                        "storageConfigurations": {"fileType": "FILE",
                                                  "path": "/tmp/lisatest/hdfs_click/hdfs_source.txt",
                                                  "clusterId": cluster_id, "cluster": "cluster1", "host": "",
                                                  "port": 22, "username": "admin", "password": "123456"},
                        "resource": {"id": fileset_id}, "isShowButton": 'false'}
            deal_random(new_data)
            return new_data
        if "lq_fileset_hdfs_recursive_dir" in data:
            new_data = {"name": "lq_fileset_hdfs_recursive_dir_随机数", "storage": "HDFS",
                        "storageConfigurations": {"fileType": "RECURSIVE_DIR", "path": "/tmp/lisatest/filesets",
                                                  "clusterId": cluster_id, "cluster": "cluster1", "host": "",
                                                  "port": 22, "username": "admin", "password": "123456"},
                        "resource": {"id": fileset_id}, "isShowButton": 'false'}
            deal_random(new_data)
            return new_data
        if "lq_fileset_sftp_directory" in data:
            new_data = {"name": "lq_fileset_sftp_directory_随机数", "storage": "SFTP",
                        "storageConfigurations": {"fileType": "DIRECTORY", "path": "/home/europa/lq_sftp/sftp_sub",
                                                  "clusterId": "", "cluster": "", "host": "192.168.1.84", "port": 22,
                                                  "username": "europa", "password": "europa"},
                        "resource": {"id": fileset_id}, "isShowButton": 'false'}
            deal_random(new_data)
            return new_data
        if "lq_fileset_sftp_recursive_dir" in data:
            new_data = {"name": "lq_fileset_sftp_recursive_dir_随机数", "storage": "SFTP",
                        "storageConfigurations": {"fileType": "RECURSIVE_DIR",
                                                  "path": "/home/europa/lq_sftp/sftp_sub1/", "clusterId": "",
                                                  "cluster": "", "host": "192.168.1.84", "port": 22,
                                                  "username": "europa", "password": "europa"},
                        "resource": {"id": fileset_id}, "isShowButton": 'false'}
            deal_random(new_data)
            return new_data
        if "lq_fileset_ftp_file" in data:
            new_data = {"name": "lq_fileset_ftp_file_随机数", "storage": "FTP",
                        "storageConfigurations": {"fileType": "FILE", "path": "/app/fq_bak/file/txt/lq.txt",
                                                  "clusterId": "7aed23c1-0d17-4613-b317-341df52def48",
                                                  "cluster": "cluster1", "host": "192.168.1.82", "port": "21",
                                                  "username": "merce", "password": "merce@82"},
                        "resource": {"id": fileset_id}, "isShowButton": False}
            deal_random(new_data)
            return new_data
        if "lq_fileset_local_file" in data:
            new_data = {"name": "lq_fileset_local_file_随机数", "storage": "LOCAL",
                        "storageConfigurations": {"fileType": "FILE", "path": "/root/baymax/test/filesearch.txt",
                                                  "clusterId": "", "cluster": "", "host": "192.168.1.149", "port": 22,
                                                  "username": "root", "password": "Inf0refiner"},
                        "resource": {"id": fileset_id}, "isShowButton": 'false'}
            deal_random(new_data)
            return new_data
        elif "lq_fileset_ozone_recursive_dir" in data:
            new_data = {"name": "lq_fileset_ozone_recursive_dir_随机数", "storage": "OZONE",
                        "storageConfigurations": {"fileType": "RECURSIVE_DIR", "path": "/info5/file", "clusterId": "",
                                                  "cluster": "", "host": "", "port": 22, "username": "",
                                                  "password": ""}, "resource": {"id": fileset_id},
                        "isShowButton": False}
            deal_random(new_data)
            return new_data
        elif "lq_fileset_minio_recursive_dir" in data:
            new_data = {"name": "lq_fileset_minio_recursive_dir_随机数", "storage": "MINIO",
                        "storageConfigurations": {"fileType": "RECURSIVE_DIR", "path": "test", "clusterId": "",
                                                  "cluster": "", "host": "192.168.1.81", "port": 9000,
                                                  "username": "minio", "password": "inforefiner"},
                        "resource": {"id": fileset_id}, "isShowButton": False}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("filesets_data出错{}".format(e))

def filesets_id(data):
    try:
        sql="select id from merce_fileset where name like '%s%%' ORDER BY create_time desc limit 1 "% data
        fileset_info = ms.ExecuQuery(sql)
        return fileset_info[0]["id"]
    except Exception as e:
        log.error("filesets_id出错{}".format(e))


def cluster_data():
    try:
        sql="select id from merce_cluster_info where name='cluster1'"
        cluster_id=ms.ExecuQuery(sql)[0]["id"]
        return cluster_id
    except Exception as e:
        log.error("cluster_data出错{}".format(e))

def update_custom_step(data):
    try:
        sql = "select id, tenant_id, owner, name, creator, last_modifier, `type`,step_group, step_setting_class, step_class_name from merce_custom_step where name like '%s%%%%' order by create_time desc limit 1" % data
        step_info = ms.ExecuQuery(sql.encode('utf-8'))
        new_data = {"jarName":"merce-custom-rtc-steps-1.2.4-Filter.jar","libs":["merce-custom-rtc-steps-1.2.4-Filter.jar"],"stepClassName":"com.inforefiner.example.rtcflow.steps.filter.FilterStep","name":step_info[0]["name"],"tags":["Custom","dataflow","streamflow","rtcflow","workflow"],"id":step_info[0]["id"],"type":step_info[0]["type"],"stepSettingClass":"com.inforefiner.example.rtcflow.steps.filter.FilterSettings","tenantId":step_info[0]["tenant_id"],"owner":step_info[0]["owner"],"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"version":1,"groupCount":None,"groupFieldValue":None,"uiConfigurations":{"output":["output"],"input":["input"]},"otherConfigurations":None,"inputConfigurations":{"input":[]},"outputConfigurations":{"output":[]},"group":"Custom","inputCount":"1","icon":"default.png","implementation":None,"outputCount":"1","settings":[{"name":"stepClassName","type":"String","defaultValue":"com.inforefiner.example.rtcflow.steps.filter.FilterStep","description":"step full class name","values":None,"required":True,"advanced":False,"hidden":False,"noTrim":False,"scope":None,"bind":None,"select":None,"selectType":None,"format":None},{"name":"keyIndex","type":"int","defaultValue":None,"description":"key","values":None,"required":True,"advanced":False,"hidden":False,"noTrim":True,"scope":None,"bind":None,"select":None,"selectType":None,"format":None},{"name":"keyValue","type":"String","defaultValue":None,"description":"key value","values":None,"required":True,"advanced":False,"hidden":False,"noTrim":True,"scope":None,"bind":None,"select":None,"selectType":None,"format":None}],"expiredPeriod":0}
        return new_data, step_info[0]["id"]
    except Exception as e:
        log.error("异常信息：%s" % e)

def update_rtcjob_setting(data):
    try:
        sql = "select id, tenant_id, owner, creator, last_modifier from merce_rtc_job_settings where name like '%s%%%%' order by create_time desc limit 1" % data
        rtcjob_info = ms.ExecuQuery(sql.encode('utf-8'))
        new_data = {"tenantId":rtcjob_info[0]["tenant_id"],"owner":rtcjob_info[0]["owner"],"name":"gjb_rtc随机数","enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"id":"","engine":"flink","debug":False,"description":"","runtimeSettings":{"driverMemory":1024,"executorMemory":1024,"kerberosKeytab":"","useLatestState":False,"executorCores":1,"parallelism":1,"clusterId":"cluster1","allowNonRestoredState":False,"flinkTableOpts":[],"savepointDir":"","kerberosJaasConf":"","master":"yarn","flinkOpts":[],"javaOpts":"","lineageEnable":True,"kerberosEnable":False,"proxyUser":"","nodeLabel":"","queue":"root.default","kerberosPrincipal":""},"checkpointSettings":{"checkpointStateBackend":"filesystem","checkpointEnable":True,"checkpointAsync":True,"checkpointInterval":10000,"checkpointUnaligned":False,"checkpointMinpause":5000,"checkpointIncremental":False,"checkpointExternalSave":True,"checkpointTimeout":600000,"checkpointMode":"exactly_once","checkpointDir":"hdfs:///tmp/flink/checkpoints"},"restartStrategySettings":{"restartDelayInterval":10,"restartStrategy":"FixedDelayRestart","restartInterval":60,"restartMaxAttempts":3},"latencyTrackingSettings":{"latencyTrackingEnable":False,"latencyTrackingInterval":60000}}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_fs(flag):
    """
    返回导入文件，根据flag判断
    :param flag:
    :return:
    """
    try:
        if flag == 'flag1':
            fs = {"file": open(woven_dataflow, 'rb')}
            return fs
        elif flag == 'flag2':
            fs = {"file": open(multi_sink_steps, 'rb')}
            return fs
        elif flag == 'flag3':
            fs = {"file": open(multi_rtc_steps, 'rb')}
            return fs
        else:
            return log.warn("请输入正确的flag1或者flag2")
    except Exception as e:
        log.error("异常信息：%s" % e)


def get_import_dataflow(headers, host, flag):
    """
    返回导入dataflow文件的请求体参数
    :param headers:
    :param host:
    :param flag:
    :return:
    """
    url = '%s/api/mis/upload' % host
    fs = get_fs(flag)
    headers.pop('Content-Type')
    res = requests.post(url=url, headers=headers, files=fs)
    try:
        cdf_list, cds_list, csm_list = [], [], []
        res = json.loads(res.text)
        for cds_id in res['cds']:
         cds_list.append(cds_id["id"])
        for csm_id in res['csm']:
         csm_list.append(csm_id["id"])
        cdf_list.append(res['cfd'][0]['id'])
        new_data = {"cfd": cdf_list, "cds": cds_list, "cmt": csm_list,"csm": csm_list, "tag":[], "uploadDirectory": res["uploadDir"],"overWrite":True,"flowResourceId":"","datasetResourceId":"","schemaResourceId":""}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_import_data(data):
    try:
        tenant_id = get_tenant_id()
        sql = "select id,name,owner,tenant_id,creator,import_status,task_type from merce_flow_import_task where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,data)
        merce_flow_import_task_info = ms.ExecuQuery(sql.encode('utf-8'))
        flow_import_task_info_id = merce_flow_import_task_info[0]["id"]
        new_data ={"tenantId":tenant_id,"owner":merce_flow_import_task_info[0]["owner"],"name":merce_flow_import_task_info[0]["name"],"enabled":None,"creator":merce_flow_import_task_info[0]["creator"],"createTime":data_now(),"lastModifier":merce_flow_import_task_info[0]["creator"],"lastModifiedTime":data_now(),"id":merce_flow_import_task_info[0]["id"],"flowCount":1,"importStatus":merce_flow_import_task_info[0]["import_status"],"remark":"gjb_type_df_import","flowImportParse":None,"taskType":merce_flow_import_task_info[0]["task_type"],"offlineDevCount":1,"realTimeDevCount":0,"workflowCount":0,"enable":False}
        return flow_import_task_info_id,new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_scheduler_online_data(data):
    try:
        tenant_id = get_tenant_id()
        sql = "select id from merce_flow_schedule where tenant_id='%s' and flow_name = '%s' ORDER BY create_time desc limit 1" %(tenant_id,data)
        flow_schedule_info = ms.ExecuQuery(sql.encode('utf-8'))
        flow_schedule_info_id = [flow_schedule_info[0]["id"]]
        new_data ={"status":"ONLINE","approverId":"","approverName":"","ids":flow_schedule_info_id,"publishStatus":"ONLINE"}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_scheduler_id(scheduler_name):
    try:
        tenant_id = get_tenant_id()
        sql = "select id from merce_flow_schedule where tenant_id='%s' and flow_name = '%s' ORDER BY create_time desc limit 1" %(tenant_id,scheduler_name)
        flow_schedule_info = ms.ExecuQuery(sql.encode('utf-8'))
        flow_schedule_info_id = flow_schedule_info[0]["id"]
        return str(flow_schedule_info_id)
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_execution_id(scheduler_name):
    try:
        tenant_id = get_tenant_id()
        sql = "select id from merce_flow_execution where tenant_id='%s' and flow_name = '%s' ORDER BY create_time desc limit 1" %(tenant_id,scheduler_name)
        flow_execution_info = ms.ExecuQuery(sql.encode('utf-8'))
        flow_execution_info_id = flow_execution_info[0]["id"]
        return str(flow_execution_info_id)
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_rtc_execution_id(scheduler_name):
    try:
        sql = "select id from merce_flow_execution where flow_name = '%s' ORDER BY create_time desc limit 1" %scheduler_name
        flow_execution_info = ms.ExecuQuery(sql.encode('utf-8'))
        flow_execution_info_id = flow_execution_info[0]["id"]
        return str(flow_execution_info_id)
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_rtcflow_id():
    """
    获取rtcflow id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from merce_flow where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,dataflow_name[1])
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        flow_info_id = flow_info[0]["id"]
        return str(flow_info_id)
    except Exception as e:
        log.error("没有获取dataflow的id：%s" % e)

def get_workflow_id():
    """
    获取workflow id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from merce_flow where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,dataflow_name[2])
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        flow_info_id = flow_info[0]["id"]
        return str(flow_info_id)
    except Exception as e:
        log.error("没有获取dataflow的id：%s" % e)

def get_safety_level():
    """
    获取safety_level id
    """
    try:
        sql = "select id from merce_safety_level where security_level ='一级'"
        safety_level = ms.ExecuQuery(sql.encode('utf-8'))
        safety_level_id = safety_level[0]["id"]
        return str(safety_level_id)
    except Exception as e:
        log.error("没有获取dataflow的id：%s" % e)

def get_role_id():
    """
    获取role id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from merce_role where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,organization_name[0])
        role_info = ms.ExecuQuery(sql.encode('utf-8'))
        role_info_id = role_info[0]["id"]
        return str(role_info_id)
    except Exception as e:
        log.error("没有获取角色的id：%s" % e)


def get_user_id():
    """
    获取user id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from merce_user where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,organization_name[1])
        user_info = ms.ExecuQuery(sql.encode('utf-8'))
        user_info_id = user_info[0]["id"]
        return str(user_info_id)
    except Exception as e:
        log.error("没有获取用户的id：%s" % e)


def get_menu_id():
    """
    获取menu id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from merce_permission where tenant_id='%s' and name like '%s%%%%' limit 1" %(tenant_id,organization_name[2])
        user_info = ms.ExecuQuery(sql.encode('utf-8'))
        user_info_id = user_info[0]["id"]
        return str(user_info_id)
    except Exception as e:
        log.error("没有获取菜单的id：%s" % e)


def get_explore_id():
    """
    获取数据探索能力配置 id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from merce_platform_capacity_data_explore where tenant_id='%s' " %tenant_id
        platform_capacity_data_explore = ms.ExecuQuery(sql.encode('utf-8'))
        platform_capacity_data_explore_id = platform_capacity_data_explore[0]["id"]
        return str(platform_capacity_data_explore_id)
    except Exception as e:
        log.error("没有获取到数据探索能力配置的id：%s" % e)