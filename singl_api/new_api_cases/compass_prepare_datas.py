# coding:utf-8
import json
import os
from new_api_cases.compass_deal_parameters import deal_random
from basic_info.setting import Compass_MySQL_CONFIG, MySQL_CONFIG1, hdfs_url
from util.Open_DB import MYSQL
from util.encrypt import parameter_ungzip
from util.timestamp_13 import data_now
from util.logs import Logger

ms = MYSQL(Compass_MySQL_CONFIG["HOST"], Compass_MySQL_CONFIG["USER"], Compass_MySQL_CONFIG["PASSWORD"], Compass_MySQL_CONFIG["DB"], Compass_MySQL_CONFIG["PORT"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))
log = Logger().get_log()

def update_job_pool(data):
    try:
        sql = "select job_pool_oid from s_c_job_pool where pool_name like '%s%%%%' order by pool_name desc limit 1" % data
        job_pool = ms.ExecuQuery(sql.encode('utf-8'))
        job_pool_oid = job_pool[0]["job_pool_oid"]
        new_data = {"id":  job_pool_oid, "poolName":  "autotest随机数", "poolSize":  10, "jobFilterClass":  "com.nokia.bighead.scheduler.function.job.JobFilter", "flowVer":  "2", "rePoolSize":  10}
        deal_random(new_data)
        return job_pool_oid, new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def update_job(data):
    try:
        sql = "select job_oid,job_name,job_pool_oid,re_oid,handle_oid,flow_name,flow_id,again_re_oid,cluster_name from s_c_job where job_name like '%s%%%%' order by create_time desc limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        job_oid, job_name, job_pool_oid, re_oid, handle_oid, flow_name, flow_id, again_re_oid, cluster_name = str(job_info[0]["job_oid"]), job_info[0]["job_name"], job_info[0]["job_pool_oid"], job_info[0]["re_oid"], job_info[0]["handle_oid"], job_info[0]["flow_name"], job_info[0]["flow_id"], job_info[0]["again_re_oid"], job_info[0]["cluster_name"]
        new_data = {"id": job_oid, "jobName": job_name, "jobType": 2, "sliceType": "H", "status": 1, "msgOid": "", "jsonOid": "", "strategyOid": "2", "delayTime": "60", "configStr": "1", "taskType": 1, "taskLevel": 5, "jobPoolOid": job_pool_oid, "timeoutTime": "", "eqTimeRun": "", "reOid": re_oid, "cdoFilterClass": "", "isTrigger": "N", "handleOid": handle_oid, "filterConf": "1", "flowName": flow_name, "flowId": flow_id, "flowVer": "0", "againType": "1", "againReOid": again_re_oid, "maxRun": 1, "flowConf": "", "clusterName": cluster_name, "runTime": "", "timeOrder": "1", "userExe": "", "otherPars": "", "jobDesc": "1", "createTime": data_now(), "planId": "", "delayType": 1, "clusterExeName": ""}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def add_job(data):
    data = data.split('&')
    try:
        re_sql = "select re_oid from s_c_re where re_name like '%s%%%%' limit 1" % data[0]
        re_info = ms.ExecuQuery(re_sql.encode('utf-8'))
        re_oid = str(re_info[0]["re_oid"])
        flow_sql = "select dataflow_name,dataflow_oid ,cluster_name from s_c_dataflow where dataflow_name like '%s%%%%' order by create_time desc limit 1" % data[1]
        flow_info = ms.ExecuQuery(flow_sql.encode('utf-8'))
        df_name, df_id, cluster_name = flow_info[0]["dataflow_name"], str(flow_info[0]["dataflow_oid"]),flow_info[0]["cluster_name"]
        pool_sql = "select job_pool_oid from s_c_job_pool where pool_name like '%s%%%%' limit 1" % data[2]
        pool_info = ms.ExecuQuery(pool_sql.encode('utf-8'))
        job_pool_oid = str(pool_info[0]["job_pool_oid"])
        handle_sql = "select handle_oid from s_c_job_handle where handle_class like '%s%%%%' limit 1" % data[3]
        handle_info = ms.ExecuQuery(handle_sql.encode('utf-8'))
        handle_oid = str(handle_info[0]["handle_oid"])
        if 'test_no_supp' in data:
            new_data = {"againReOid": re_oid, "againType": "1", "clusterExeName": "", "clusterName": cluster_name, "configStr": "1", "delayTime": "60", "delayType": 1, "filterConf": "1", "flowConf": "", "flowName": df_name, "flowVer": "0", "handleOid": handle_oid, "isTrigger": "N", "jobDesc": "1", "jobName": "autotest_随机数", "jobPoolOid":  job_pool_oid, "jobType": 2, "jsonOid": "", "maxRun": 1, "msgOid": "", "otherPars": "", "planId": "", "reOid": re_oid, "runTime": "", "sliceType": "H", "status": 1, "strategyOid": "2", "taskLevel": 5, "taskType": 1, "timeOrder": "1", "userExe": "", "flowId": df_id}
            deal_random(new_data)
            return new_data
        elif 'test_add_supp' in data:
            new_data = {"againReOid": re_oid, "againType": "1", "clusterExeName": "", "clusterName": cluster_name, "configStr": "1", "delayTime": "60", "delayType": 1, "filterConf": "1", "flowConf": "", "flowName": df_name, "flowVer": "0", "handleOid": handle_oid, "isTrigger": "A", "jobDesc": "1", "jobName": "autotest_随机数", "jobPoolOid":  job_pool_oid, "jobType": 2, "jsonOid": "", "maxRun": 1, "msgOid": "", "otherPars": "", "planId": "", "reOid": re_oid, "runTime": "", "sliceType": "H", "status": 1, "strategyOid": "2", "taskLevel": 5, "taskType": 1, "timeOrder": "1", "userExe": "", "flowId": df_id}
            deal_random(new_data)
            return new_data
        elif 'test_total_supp' in data:
            new_data = {"againReOid": re_oid, "againType": "1", "clusterExeName": "", "clusterName": cluster_name, "configStr": "1", "delayTime": "60", "delayType": 1, "filterConf": "1", "flowConf": "", "flowName": df_name, "flowVer": "0", "handleOid": handle_oid, "isTrigger": "Y", "jobDesc": "1", "jobName": "autotest_随机数", "jobPoolOid":  job_pool_oid, "jobType": 2, "jsonOid": "", "maxRun": 1, "msgOid": "", "otherPars": "", "planId": "", "reOid": re_oid, "runTime": "", "sliceType": "H", "status": 1, "strategyOid": "2", "taskLevel": 5, "taskType": 1, "timeOrder": "1", "userExe": "", "flowId": df_id}
            deal_random(new_data)
            return new_data
        elif 'test_cover_supp' in data:
            new_data = {"againReOid": re_oid, "againType": "1", "clusterExeName": "", "clusterName": cluster_name, "configStr": "1", "delayTime": "60", "delayType": 1, "filterConf": "1", "flowConf": "", "flowName": df_name, "flowVer": "0", "handleOid": handle_oid, "isTrigger": "R", "jobDesc": "1", "jobName": "autotest_随机数", "jobPoolOid":  job_pool_oid, "jobType": 2, "jsonOid": "", "maxRun": 1, "msgOid": "", "otherPars": "", "planId": "", "reOid": re_oid, "runTime": "", "sliceType": "H", "status": 1, "strategyOid": "2", "taskLevel": 5, "taskType": 1, "timeOrder": "1", "userExe": "", "flowId": df_id}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)

def add_jobSingle(data):
    try:
        sql = "select job_oid,job_name,slice_type from s_c_job where job_name = '%s' order by create_time desc limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        job_oid, job_name, slice_type = str(job_info[0]["job_oid"]), job_info[0]["job_name"], job_info[0]["slice_type"]
        if 'gdemo' == data:
            new_data = {"jobOid":  job_oid, "sliceType":  slice_type, "sliceTime":  data_now(), "singleType":  0, "jobLevel":  "1", "taskName":  job_name}
            deal_random(new_data)
            return new_data
        elif 'gdemo_add_supp' == data:
            new_data = {"jobOid":  job_oid, "sliceType":  slice_type, "sliceTime":  data_now(), "singleType":  1, "jobLevel":  "1", "taskName":  job_name}
            deal_random(new_data)
            return new_data
        elif 'gdemo_total_supp' == data:
            new_data = {"jobOid":  job_oid, "sliceType":  slice_type, "sliceTime":  data_now(), "singleType":  2, "jobLevel":  "1", "taskName":  job_name}
            deal_random(new_data)
            return new_data
        elif 'test_cover' == data:
            new_data = {"jobOid":  job_oid, "sliceType":  slice_type, "sliceTime":  data_now(), "singleType":  3, "jobLevel":  "1", "taskName":  job_name}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)

def update_jobSingle(data):
    try:
        sql = "select single_oid,job_oid,slice_time,create_time,status,single_type,task_name from s_r_job_single where task_name like '%s%%%%' order by create_time desc limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        single_oid, job_oid, slice_time, create_time, status, single_type = str(job_info[0]["single_oid"]), str(job_info[0]["job_oid"]), str(job_info[0]["slice_time"]), str(job_info[0]["create_time"]), job_info[0]["status"], job_info[0]["single_type"]
        new_data = {"id":  single_oid, "jobOid":  job_oid, "jobLevel":  1, "sliceTime":  slice_time, "createTime":  create_time, "status":  status , "singleType":  single_type, "taskName":  "gdemo随机数", "sliceType": "H"}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def add_jobMap(data):
    try:
        sql = "select job_oid,cluster_name from s_c_job where job_name like '%s%%%%' order by create_time desc limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        job_oid, cluster_name = str(job_info[0]["job_oid"]), job_info[0]["cluster_name"]
        if 'autotest' == data:
            new_data = {"jobDataOid": "", "jobOid":  job_oid, "clusterName":  cluster_name, "dataformatName":  "test_supp1211", "sliceTimeRegType": 1, "sliceTimeReg": "", "mustLevel": 3, "mustType": 1, "mustPars": "100", "mustLine": "", "mustOuttime": ""}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)

def update_jobMap(data):

    try:
        sql = "select job_map_oid ,job_oid,dataformat_Name,cluster_name,job_name from s_c_job_map where job_name like '%s%%%%' limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        job_map_oid, job_oid, dataformat_Name,cluster_name, job_name = str(job_info[0]["job_map_oid"]), str(job_info[0]["job_oid"]), job_info[0]["dataformat_Name"], job_info[0]["cluster_name"], job_info[0]["job_name"]
        new_data = {"jobDataOid":  "", "jobOid":  job_oid, "clusterName":  cluster_name, "dataformatName":  dataformat_Name, "sliceTimeRegType": 1, "sliceTimeReg": "", "mustLevel": 3, "mustType": 1, "mustPars": "100", "mustLine": "", "mustOuttime": "", "id":  job_map_oid, "jobName":  job_name, "jobType":  "2"}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def update_re(data):
    try:
        sql = "select re_oid from s_c_re where re_name like  '%s%%%%' limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_oid = str(job_info[0]["re_oid"])
        new_data = {"id":  re_oid, "reName":  "autotest随机数", "queueName":  "default", "status":  1, "clusterName":  "83"}
        deal_random(new_data)
        return re_oid, new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def query_reth(data):
    try:
        re_oid = []
        sql = "select re_oid from s_c_re where re_name like  '%s%%%%' limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_oid.append(str(job_info[0]["re_oid"]))
        new_data = {"fieldGroup":  {"fields":  [{"andOr":  "AND", "name":  "reOid", "oper":  "EQUAL", "value":  re_oid}]}, "ordSort": [], "pageable": {"pageNum": 0, "pageSize": 8, "pageable": "true"}}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def add_reth(data):
    try:
        sql = "select re_oid from s_c_re where re_name like  '%s%%%%' limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_oid = str(job_info[0]["re_oid"])
        new_data = {"reOid":  re_oid, "minValue":  "", "maxValue":  "10000", "exeMem":  "2G", "exeNum":  "2", "driverMem":  "2G"}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def update_reth(data):
    try:
        sql = "select re_th_oid, re_oid from s_c_re_th where re_oid in(select t.re_oid from (select * from s_c_re where re_name like '%s%%%%' limit 1) as t)" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_th_oid, re_oid = str(job_info[0]["re_th_oid"]), str(job_info[0]["re_oid"])
        new_data = {"id":  re_th_oid, "reOid":  re_oid, "minValue":  "", "maxValue":  "10000", "exeMem":  "2G", "exeNum":  "2", "driverMem":  "2G"}
        return re_th_oid, new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def query_rethExt(data):
    try:
        re_th_oid = []
        sql = "select re_th_oid from s_c_re_th where re_oid in(select t.re_oid from (select * from s_c_re where re_name like '%s%%%%' limit 1) as t)" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_th_oid.append(str(job_info[0]["re_th_oid"]))
        new_data = {"fieldGroup":  {"fields":  [{"andOr":  "AND", "name":  "reThOid", "oper":  "EQUAL", "value":  re_th_oid}]}, "ordSort":  [], "pageable":  {"pageNum":  0, "pageSize":  8, "pageable":  "true"}}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def add_rethExt(data):
    try:
        sql = "select re_th_oid from s_c_re_th where re_oid in(select t.re_oid from (select * from s_c_re where re_name like '%s%%%%' limit 1) as t)" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_th_oid = str(job_info[0]["re_th_oid"])
        new_data = {"reThOid":  re_th_oid, "extKey":  "spark.executor.cores", "extValue":  "4"}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def update_rethExt(data):
    try:
        sql = "select re_th_ext_oid ,re_th_oid from s_c_re_th_ext where re_th_oid in(select t.re_th_oid from s_c_re_th as t where re_oid in(select s.re_oid from (select * from s_c_re where re_name like '%s%%%%' limit 1) as s))" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_th_ext_oid, re_th_oid = str(job_info[0]["re_th_ext_oid"]), str(job_info[0]["re_th_oid"])
        new_data = {"id":  re_th_ext_oid, "reThOid":  re_th_oid, "extKey":  "spark.executor.cores", "extValue":  "4"}
        return re_th_ext_oid, new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_asset_directory(data):
    try:
        new_data = {"name":  "test_asset随机数", "parentId":  data, "resType":  "jobview_dir"}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def update_asset_directory(data):
    try:
        asset_directory = "select parent_id,tenant_id,id,ord,path from merce_resource_dir mrd where res_type ='jobview_dir' and creator='admin' and name like'%s%%%%' order by create_time desc limit 1" % data
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        new_data = {"name": "test_asset随机数", "parentId": asset_directory_info[0]["parent_id"], "resType": "jobview_dir", "tenantId": asset_directory_info[0]["tenant_id"], "owner": None, "enabled": None, "creator": None, "createTime": None, "lastModifier": None, "lastModifiedTime": None, "id": asset_directory_info[0]["id"], "version": None, "groupCount": None, "groupFieldValue": None, "order": asset_directory_info[0]["ord"], "isHide": None, "path": asset_directory_info[0]["path"], "children": [], "halfSelect": True, "hasChildren": False, "expiredPeriod": 0}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def move_asset_directory(data):
    try:
        asset_directory = "select parent_id,id from merce_resource_dir mrd where res_type ='jobview_dir' and creator='admin' and name like'%s%%%%' order by create_time desc limit 1" % data
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        new_data=[]
        new_data.append(asset_directory_info[0]["id"])
        return asset_directory_info[0]["parent_id"], new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def duplicate_asset_directory(data):
    try:
        asset_directory = "select parent_id,name from merce_resource_dir mrd where res_type ='jobview_dir' and creator='admin' and name like'%s%%%%' order by create_time desc limit 1" % data
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        new_data = {"name":  asset_directory_info[0]["name"], "parentId":  asset_directory_info[0]["parent_id"], "resType":  "jobview_dir"}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def duplicate_move_asset_directory(data):
    try:
        asset_directory = "select name,parent_id,tenant_id,id,ord,path from merce_resource_dir mrd where res_type ='jobview_dir' and creator='admin' and name like'%s%%%%' order by create_time desc limit 1" % data
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        new_data = {"name":  asset_directory_info[0]["name"], "parentId":  asset_directory_info[0]["parent_id"], "resType":  "jobview_dir", "tenantId":  asset_directory_info[0]["tenant_id"], "owner":  None, "enabled": None, "creator": None, "createTime": None, "lastModifier": None, "lastModifiedTime": None, "id": asset_directory_info[0]["id"], "version": None, "groupCount": None, "groupFieldValue": None, "order": asset_directory_info[0]["ord"], "isHide": None, "path": asset_directory_info[0]["path"], "children": [], "halfSelect": True, "hasChildren": False, "expiredPeriod": 0}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def delete_asset_directory(data):
    try:
        asset_directory = "select id from merce_resource_dir mrd where res_type ='jobview_dir' and creator='admin' and name like'%s%%%%' order by create_time desc limit 1" % data
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        new_data = {"data":  str(asset_directory_info[0]["id"])}
        return asset_directory_info[0]["id"], new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_jobview(data):
    try:
        data = data.split("&")
        asset_directory = "select id from merce_resource_dir where res_type ='jobview_dir' and creator='admin' and name like'%s%%%%' order by create_time desc limit 1" % data[0]
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        s_c_job_pool = "select job_pool_oid,pool_name from s_c_job_pool where pool_name like'%s%%%%' limit 1" % data[1]
        s_c_job_pool_info = ms.ExecuQuery(s_c_job_pool.encode('utf-8'))
        if data[2] == 'scheduler':
            new_data = {"name":  "gjb_scheduler随机数", "poolName":  s_c_job_pool_info[0]["pool_name"], "poolId":  s_c_job_pool_info[0]["job_pool_oid"], "sliceType":  "H", "enabled":  0, "description":  "自动化脚本", "resourceId":  asset_directory_info[0]["id"]}
            deal_random(new_data)
            return new_data
        elif data[2] == 'qa':
            new_data = {"name":  "gjb_qa_scheduler随机数", "poolName":  s_c_job_pool_info[0]["pool_name"], "poolId":  s_c_job_pool_info[0]["job_pool_oid"], "sliceType":  "H", "enabled":  0, "description":  "自动化脚本", "resourceId":  asset_directory_info[0]["id"]}
            deal_random(new_data)
            return new_data
        else:
            return None
    except Exception as e:
        log.error("异常信息：%s" % e)

def update_jobview(data):
    try:
        sql = "select id, owner, tenant_id, creator, enabled, last_modifier, name, flow_names, pool_id, pool_name, resource_id, slice_type from s_v_job_view where name like'%s%%%%' order by create_time desc limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        new_data = {"name": job_info[0]["name"], "poolName": job_info[0]["pool_name"], "poolId": job_info[0]["pool_id"], "sliceType": "H", "enabled": 0, "description": "自动化脚本", "tenantId": job_info[0]["tenant_id"], "owner": job_info[0]["owner"], "creator": job_info[0]["creator"], "createTime": data_now(), "lastModifier": job_info[0]["last_modifier"], "lastModifiedTime": data_now(), "id": job_info[0]["id"], "resourceId": job_info[0]["resource_id"], "releaseVersion": None, "releaseViewHistoryId": "", "flowNames":[]}
        return job_info[0]["id"], new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def query_jobview(data):
    try:
        asset_directory = "select id from merce_resource_dir where res_type ='jobview_dir' and creator='admin' and name like'%s%%%%' order by create_time desc limit 1" % data
        asset_directory = ms.ExecuQuery(asset_directory.encode('utf-8'))
        if data == 'test_asset':
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":asset_directory[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif data == 'test_asse':
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"name","comparatorOperator":"LIKE","fieldValue":"%sch%"},{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":asset_directory[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif data == 'test_ass':
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"poolName","comparatorOperator":"LIKE","fieldValue":"%auto%"},{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":asset_directory[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_jobview_exec(data):
    try:
        sql = "select id from s_v_job_view where name like'%s%%%%' order by create_time desc limit 1" % data.split("&")[0]
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        viewid = []
        viewid.append(job_info[0]["id"])
        if data =='gjb_scheduler&all':
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"viewId","oper":"EQUAL","value": viewid}]},"ordSort":[{"name":"startTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        elif data =='gjb_scheduler&state-2':
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"status","oper":"EQUAL","value":["-2"]},{"andOr":"AND","name":"viewId","oper":"EQUAL","value":viewid}]},"ordSort":[{"name":"startTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        elif data =='gjb_scheduler&state2':
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"status","oper":"EQUAL","value":["2"]},{"andOr":"AND","name":"viewId","oper":"EQUAL","value":viewid}]},"ordSort":[{"name":"startTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        elif data =='gjb_scheduler&state1':
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"status","oper":"EQUAL","value":["1"]},{"andOr":"AND","name":"viewId","oper":"EQUAL","value":viewid}]},"ordSort":[{"name":"startTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        elif data =='gjb_scheduler&jobtype1':
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"jobType","oper":"EQUAL","value":[1]},{"andOr":"AND","name":"viewId","oper":"EQUAL","value":viewid}]},"ordSort":[{"name":"startTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        elif data =='gjb_scheduler&jobtype2':
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"jobType","oper":"EQUAL","value":[2]},{"andOr":"AND","name":"viewId","oper":"EQUAL","value":viewid}]},"ordSort":[{"name":"startTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        elif data =='gjb_scheduler&jobtype3':
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"jobType","oper":"EQUAL","value":[3]},{"andOr":"AND","name":"viewId","oper":"EQUAL","value":viewid}]},"ordSort":[{"name":"startTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        elif data =='gjb_qa_scheduler&jobtype4':
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"jobType","oper":"EQUAL","value":[4]},{"andOr":"AND","name":"viewId","oper":"EQUAL","value":viewid}]},"ordSort":[{"name":"startTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        elif data =='gjb_scheduler&jobtype5':
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"jobType","oper":"EQUAL","value":[5]},{"andOr":"AND","name":"viewId","oper":"EQUAL","value":viewid}]},"ordSort":[{"name":"startTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        elif data =='gjb_scheduler&time':
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"startTime","oper":"GREATER_THAN","value":[1661961600000]},{"andOr":"AND","name":"startTime","oper":"LESS_THAN","value":[1667231999000]},{"andOr":"AND","name":"viewId","oper":"EQUAL","value":viewid}]},"ordSort":[{"name":"startTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        elif data =='gjb_scheduler&nodename':
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"nodeName","oper":"LIKE","value":["%scheduler%"]},{"andOr":"AND","name":"viewId","oper":"EQUAL","value":["1014552966163521536"]}]},"ordSort":[{"name":"startTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        elif data =='gjb_scheduler&nodename-jobtype2':
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"nodeName","oper":"LIKE","value":["%scheduler%"]},{"andOr":"AND","name":"jobType","oper":"EQUAL","value":[2]},{"andOr":"AND","name":"viewId","oper":"EQUAL","value":viewid}]},"ordSort":[{"name":"startTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_datasource(data):
    try:
        sql = "select id from merce_resource_dir where creator='admin' and name='Datasources' and parent_id is NULL"
        dir_info = ms.ExecuQuery(sql.encode('utf-8'))
        if data =='gjb_mysql':
            merce_udf = "select id, owner, tenant_id, creator, enabled, last_modifier, name, expired_time, version, alias_name, class_name, db_type, jar_name, parameterlist, process_config_type from merce_udf where name like'%s%%%%' order by create_time desc limit 1" % data
            merce_udf = ms.ExecuQuery(merce_udf.encode('utf-8'))
            dict_str =json.loads(merce_udf[0]["parameterlist"])
            url ="jdbc:"+ str(merce_udf[0]["db_type"]).lower()+"://"+MySQL_CONFIG1["HOST"]+":"+str(MySQL_CONFIG1["PORT"])+"/"+MySQL_CONFIG1["DB"]
            new_data = {"name":"gjb_mysql_datasource随机数","type":"DB","description":"gjb_mysql_datasource","resource":{"id":dir_info[0]["id"]},"tags":[],"attributes":{"name":merce_udf[0]["name"],"jarPath":merce_udf[0]["jar_name"],"DBType":merce_udf[0]["db_type"],"host":MySQL_CONFIG1["HOST"],"port":MySQL_CONFIG1["PORT"],"database":MySQL_CONFIG1["DB"],"user":MySQL_CONFIG1["USER"],"password":MySQL_CONFIG1["PASSWORD"],"driver":merce_udf[0]["class_name"],"properties":[],"url":url,"dateToTimestamp":False,"catalog":"","schema":"","batchsize":10000,"defaultUrl":dict_str["url"],"paraPrefix":dict_str["paraPrefix"],"paraSep":dict_str["paraSep"]}}
            deal_random(new_data)
            return new_data
        elif data =='gjb_hdfs':
            new_data = {"name":"gjb_hdfs随机数","type":"HDFS","description":"gjb_hdfs","resource":{"id":dir_info[0]["id"]},"tags":[],"attributes":{"encoder":"UTF-8","path":hdfs_url}}
            deal_random(new_data)
            return new_data
        else:
            return None
    except Exception as e:
        log.error("异常信息：%s" % e)

def dc_collecter_group(data):
    try:
        dc_collecter_group = "select id from dc_collecter_group where name like'%s%%%%' order by create_time desc limit 1" % data
        dc_collecter_group = ms.ExecuQuery(dc_collecter_group.encode('utf-8'))
        new_data = {"groupId":dc_collecter_group[0]["id"],"id":"","name":"gjb_c1随机数","secretKey":"","authType":0,"host":"192.168.1.62","port":22,"proxyPort":8989,"appPath":"/app/merce/poseidon/poseidon-root-1.0.0","restUri":"http://192.168.1.62:8989","user":"merce","password":"62@merce","status":0,"isCacheStatus":0}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def dc_collecter(data):
    try:
        dc_collecter = "select group_id from dc_collecter where name like '%s%%%%' order by create_time desc limit 1" % data.split("&")[0]
        dc_collecter = ms.ExecuQuery(dc_collecter.encode('utf-8'))
        merce_dss = "select id, owner, tenant_id, creator, enabled, last_modifier, name, `attributes`, description, resource_id from merce_dss where name like '%s%%%%' order by create_time desc limit 1" % data.split("&")[1]
        merce_dss = ms.ExecuQuery(merce_dss.encode('utf-8'))
        attributes = merce_dss[0]["attributes"]
        attribute_str = parameter_ungzip(attributes)
        attr_dict = json.loads(attribute_str)
        if data == 'gjb_c&gjb_mysql':
            new_data = {"dataSource":{"id":merce_dss[0]["id"],"name":merce_dss[0]["name"],"tenantId":merce_dss[0]["tenant_id"],"owner":merce_dss[0]["owner"],"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"version":1,"groupCount":None,"groupFieldValue":None,"tableName":"merce_dss","type":"DB","path":None,"resource":None,"resourceId":"1014244384917348352","tags":[],"description":"gjb_mysql_datasource","expiredPeriod":0,"showInput":True},"tables":["supp"]}
            new_data['dataSource']['attributes'] = attr_dict
            return dc_collecter[0]["group_id"], new_data
        elif data =='gjb_c1&gjb_mysql':
            new_data = {"id":merce_dss[0]["id"],"name":merce_dss[0]["name"],"tenantId":merce_dss[0]["tenant_id"],"owner":merce_dss[0]["owner"],"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"version":1,"groupCount":None,"groupFieldValue":None,"tableName":"merce_dss","type":"DB","path":None,"resource":None,"resourceId":"1014244384917348352","tags":[],"description":"gjb_mysql_datasource","expiredPeriod":0,"showInput":True}
            new_data['attributes'] = attr_dict
            return dc_collecter[0]["group_id"], new_data
        else:
            return None
    except Exception as e:
        log.error("异常信息：%s" % e)

def dc_task(data):
    try:
        datas = data.split("&")
        dc_collecter = "select id,group_id from dc_collecter where name like '%s%%%%' order by create_time desc limit 1" % datas[0]
        dc_collecter = ms.ExecuQuery(dc_collecter.encode('utf-8'))
        Datasets = "select id from merce_resource_dir where creator='admin' and name='Datasets' and parent_id is NULL"
        Datasets = ms.ExecuQuery(Datasets.encode('utf-8'))
        Schemas = "select id from merce_resource_dir where creator='admin' and name='Schemas' and parent_id is NULL"
        Schemas = ms.ExecuQuery(Schemas.encode('utf-8'))
        merce_dss_hdfs = "select id,name,db_type from merce_dss where name like '%s%%%%' order by create_time desc limit 1" % datas[1]
        merce_dss_hdfs = ms.ExecuQuery(merce_dss_hdfs.encode('utf-8'))
        merce_dataset = "select id, name, datasource_id, datasource_name, schema_id, slice_type, storage, storage_configurations from merce_dataset where storage='JDBC' and name like '%s%%%%' order by last_modified_time desc limit 1" % datas[2]
        merce_dataset = ms.ExecuQuery(merce_dataset.encode('utf-8'))
        new_data = {"name":"gjb_collect_task随机数","groupId":dc_collecter[0]["group_id"],"collectorId":dc_collecter[0]["id"],"source":{"id":merce_dataset[0]["datasource_id"],"name":merce_dataset[0]["datasource_name"]},"sinks":[{"id":merce_dss_hdfs[0]["id"],"name":merce_dss_hdfs[0]["name"],"storage":merce_dss_hdfs[0]["db_type"]}],"schemaDirId":Schemas[0]["id"],"schemaDirPath":"Schemas;","datasetDirId":Datasets[0]["id"],"datasetDirPath":"Datasets;","splitCount":1,"scheduleRule":"","subTasks":[{"source":{"id":merce_dataset[0]["id"],"name":"auto_apitest.default.supp","properties":{"targetNames":["auto_apitest.supp"],"schema":"","targetName":"auto_apitest.supp","catalog":"","DBType":"Mysql","batchsize":10000,"sql":"","database":"auto_apitest","password":"merce","countWrittenRecord":"false","host":"192.168.1.82","partitionColumn":"","id":merce_dataset[0]["datasource_id"],"table":"supp","dateToTimestamp":False,"showSelectSchema":"false","schemaResource":"","jarPath":"mysql-connector-java-8.0.2801.jar","timeField":"","url":"jdbc:mysql://192.168.1.82:3306/auto_apitest","expiredTime":0,"increaseColumn":"stime","isNow":True,"driver":"com.mysql.jdbc.Driver","port":3306,"timeFormat":"","name":merce_dataset[0]["datasource_name"],"chineseName":"","time":"s","user":"merce","username":"merce"},"resourceId":merce_dataset[0]["datasource_id"],"schemaId":merce_dataset[0]["schema_id"],"datasetId":merce_dataset[0]["id"],"columns":[{"name":"name","type":"string","alias":"","description":"","fieldCategory":None,"specId":None,"operate":"none","parameter":"","isExt":0},{"name":"age","type":"int","alias":"","description":"","fieldCategory":None,"specId":None,"operate":"none","parameter":"","isExt":0},{"name":"sex","type":"string","alias":"","description":"","fieldCategory":None,"specId":None,"operate":"none","parameter":"","isExt":0},{"name":"stime","type":"string","alias":"","description":"","fieldCategory":None,"specId":None,"operate":"none","parameter":"","isExt":0}],"extColumns":[]},"sinks":[{"sliceType":"H","resourceId":merce_dss_hdfs[0]["id"],"schemaName":"auto_apitest.default.supp","datasetName":"auto_apitest.default.supp","sourceSchema":merce_dataset[0]["schema_id"],"sourceDataset":merce_dataset[0]["id"],"columns":[{"name":"name","type":"string"},{"name":"age","type":"int"},{"name":"sex","type":"string"},{"name":"stime","type":"string"}]}]}],"setting":{"task":{"mode":"local","queue":"default","parallelism":1,"jobManagerMemoryMB":1024,"taskManagerMemoryMB":1024,"envJavaOpts":"","cacheStatusIp":"192.168.1.84","messageEnable":True}},"storage":{"path":"/tmp/gjb_collect/","fileType":"csv","fileMaxSizeMbs":1024,"fileTimeoutSecs":60,"fieldSeparatorHex":"2C","lineSeparatorHex":"0A"}}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def merce_dataflow(data):
    try:
        merce_resource_dir = "select id from merce_resource_dir where creator='admin' and name='Flows' and parent_id is NULL"
        merce_resource_dir = ms.ExecuQuery(merce_resource_dir.encode('utf-8'))
        new_data = {"name":"gjb_scheduler_source随机数","flowType":"dataflow","resource":{"id":merce_resource_dir[0]["id"]},"steps":[],"tags":[],"links":[]}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def publish_flow(data):
    try:
        data = data.split("#")
        sql = "select id, owner, tenant_id, creator, enabled, last_modifier, name, flow_type, resource_id from merce_flow where  name like'%s%%%%' order by create_time desc limit 1" % data[0]
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        dataset_sql = " select md.id,md.name,md.schema_id ,ms.name as schema_name from merce_dataset md inner join merce_schema ms on md.schema_id =ms.id where md.source='output' and md.name like '%s%%%%' order by md.last_modified_time desc limit 1"% data[1]
        dataset_info = ms.ExecuQuery(dataset_sql.encode('utf-8'))
        new_data = {"tenantId":job_info[0]["tenant_id"],"owner":job_info[0]["owner"],"name":job_info[0]["name"],"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"id":job_info[0]["id"],"version":0,"groupCount":None,"groupFieldValue":None,"resourceId":job_info[0]["resource_id"],"isHide":0,"resource":None,"source":None,"steps":[{"tenantId":None,"owner":None,"name":"source","enabled":1,"creator":None,"createTime":data_now(),"lastModifier":None,"lastModifiedTime":data_now(),"id":"source_1","version":None,"groupCount":None,"groupFieldValue":None,"type":"source","uiConfigurations":{"output":["output"]},"otherConfigurations":{"dataset-dateTo":"","dataset-dateFrom":"","dataset-paths":"","dataset-sql":"","interceptor":"","dataset":[{"rule":"set_1","dataset":dataset_info[0]["name"],"ignoreMissingPath":False,"datasetId":dataset_info[0]["id"],"storage":"HDFS"}],"schema":dataset_info[0]["schema_name"],"schemaId":dataset_info[0]["schema_id"]},"inputConfigurations":None,"outputConfigurations":{"output":[{"name":"name","type":"string","alias":""},{"name":"age","type":"int","alias":""},{"name":"sex","type":"string","alias":""},{"name":"stime","type":"string","alias":""}]},"tags":["IO","dataflow"],"group":"IO","inputCount":None,"icon":"default.png","libs":None,"implementation":None,"expiredPeriod":0,"x":260,"y":120},{"tenantId":None,"owner":None,"name":"transform","enabled":1,"creator":None,"createTime":data_now(),"lastModifier":None,"lastModifiedTime":data_now(),"id":"transform_1","version":None,"groupCount":None,"groupFieldValue":None,"type":"transform","uiConfigurations":{"output":["output"],"input":["input"]},"otherConfigurations":{"interceptor":"","expressions":[{"value":"from_unixtime(unix_timestamp(stime),'yyyyMMddHH') as stime1"}]},"inputConfigurations":{"input":[{"name":"name","type":"string","alias":""},{"name":"age","type":"int","alias":""},{"name":"sex","type":"string","alias":""},{"name":"stime","type":"string","alias":""}]},"outputConfigurations":{"output":[{"name":"stime1","type":"string","alias":""},{"name":"name","type":"string","alias":""},{"name":"age","type":"int","alias":""},{"name":"sex","type":"string","alias":""}]},"tags":["Transform","dataflow"],"group":"Transform","inputCount":None,"icon":"default.png","libs":None,"implementation":None,"expiredPeriod":0,"x":515,"y":126}],"links":[{"source":"source_1","sourceOutput":"output","target":"transform_1","targetInput":"input"},{"source":"transform_1","sourceOutput":"output","target":"sink_1","targetInput":"input"}],"flowType":"dataflow","parameters":[{"category":"ref","name":"dataset","refs":["source_1.dataset"],"defaultVal":dataset_info[0]["name"],"description":""},{"category":"ref","name":"schema","refs":["source_1.schema"],"defaultVal":dataset_info[0]["schema_name"],"description":""},{"category":"ref","name":"path","refs":["sink_1.path"],"defaultVal":"/tmp/gjb_scheduler_sink/$stime","description":""}],"inputs":[],"outputs":[],"dependencies":[],"tags":[],"description":None,"flowId":job_info[0]["id"],"editorId":job_info[0]["owner"],"editorName":"admin","locked":True,"startTime":None,"endTime":None,"expiredPeriod":0,"customParameter":[]}
        sink_data = {"tenantId":None,"owner":None,"name":"sink","enabled":1,"creator":None,"createTime":data_now(),"lastModifier":None,"lastModifiedTime":data_now(),"id":"sink_1","version":None,"groupCount":None,"groupFieldValue":None,"type":"sink","uiConfigurations":{"input":["input"]},"inputConfigurations":{"input":[{"name":"stime1","type":"string","alias":"stime"},{"name":"name","type":"string","alias":""},{"name":"age","type":"int","alias":""},{"name":"sex","type":"string","alias":""}]},"outputConfigurations":{},"tags":["IO","dataflow"],"group":"IO","inputCount":None,"icon":"default.png","libs":None,"implementation":None,"expiredPeriod":0,"x":740,"y":120}
        other = {"isDisable":False,"datasetType":"NORMAL","expiredTime":"","description":"","checkpointLocation":"","countWrittenRecord":"true","format":"csv","separator":",","quoteChar":"\"","escapeChar":"\\","path":"/tmp/gjb_scheduler_sink/$stime","mode":"append","outputMode":"","schedulerVal":"","schedulerUnit":"","trigger":"","nullValue":"","maxFileSize":"","maxFileNumber":"","sliceTimeColumn":"stime","idColumn":"","sliceType":"H","hdfsPartitionColumn":"","partitionType":"DateFormat","dateFormat":"","dateFrom":"","dateTo":"","datePeriod":"","dateUnit":"HOUR","partitionList":"","time":"s","schema":"gjb_scheduler_sink随机数","type":"HDFS","dataset":"gjb_scheduler_sink随机数","datasetId":"","schemaId":"","datasetResource":"","schemaResource":"","schemaVersion":"1"}
        deal_random(other)
        sink_data['otherConfigurations'] = other
        new_data['steps'].append(sink_data)
        return job_info[0]["id"], new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_jobview_history(data):
    try:
        data = data.split("&")
        dc_task = "select id,name from dc_task where name like'%s%%%%' order by create_time desc limit 1" % data[0]
        dc_task = ms.ExecuQuery(dc_task.encode('utf-8'))
        sql = "select id, owner, tenant_id, name, pool_id, resource_id, slice_type from s_v_job_view where name like'%s%%%%' order by create_time desc limit 1" % data[1]
        job_view = ms.ExecuQuery(sql.encode('utf-8'))
        source_flow_dataset_map = "select mfdm.dataset_id,mfdm.dataset_name,mfdm.flow_id,mfdm.flow_name from merce_flow_dataset_map mfdm inner join merce_flow mf on mf.id =mfdm.flow_id where mfdm.`type` ='source' and mf.name like '%s%%%%' order by mf.create_time DESC limit 1" % data[1]
        source_flow_dataset_map = ms.ExecuQuery(source_flow_dataset_map.encode('utf-8'))
        job_name = source_flow_dataset_map[0]["flow_name"]+"."+source_flow_dataset_map[0]["flow_id"]
        sink_flow_dataset_map = "select mfdm.dataset_id,mfdm.dataset_name,mfdm.flow_id,mfdm.flow_name from merce_flow_dataset_map mfdm inner join merce_flow mf on mf.id =mfdm.flow_id where mfdm.`type` ='sink' and mf.name like '%s%%%%' order by mf.create_time DESC limit 1" % data[1]
        sink_flow_dataset_map = ms.ExecuQuery(sink_flow_dataset_map.encode('utf-8'))
        s_c_re = "select re_oid from s_c_re where re_name like'%s%%%%' limit 1" % data[2]
        s_c_re = ms.ExecuQuery(s_c_re.encode('utf-8'))
        new_data = {"locked":True,"tenantId":job_view[0]["tenant_id"],"owner":job_view[0]["owner"],"name":job_view[0]["name"],"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"id":"1022572394668679168","viewId":job_view[0]["id"],"version":-2,"poolId":job_view[0]["pool_id"],"sliceType":"H","nodes":[{"tenantId":"","owner":"","name":dc_task[0]["name"],"enabled":None,"creator":"","createTime":None,"lastModifier":"","lastModifiedTime":None,"id":"","x":110,"y":50,"uiName":"collect_1","nodeType":1,"viewHistoryId":"","jobId":dc_task[0]["id"],"jobType":1,"config":{"id":dc_task[0]["id"],"jobName":"","jobType":None,"sliceType":"H","status":None,"msgOid":None,"jsonOid":None,"strategyOid":None,"delayTime":None,"configStr":"","taskType":None,"taskLevel":None,"jobPoolOid":job_view[0]["pool_id"],"timeoutTime":None,"eqTimeRun":None,"reOid":None,"cdoFilterClass":"","isTrigger":"","handleOid":None,"handleName":"","filterConf":"","flowName":dc_task[0]["name"],"flowId":dc_task[0]["id"],"flowVer":None,"againType":"","againReOid":None,"maxRun":None,"flowConf":"","clusterName":"","hdfsName":"default","runTime":"","timeOrder":"","userExe":"","otherPars":"","jobDesc":"","createTime":None,"planId":None,"delayType":None,"clusterExeName":"","userCreate":"","otherPars1":"","otherPars2":"","poolSize":None,"poolType":None,"queueName":"","againQueueName":"","jobVarList":[],"reThList":[],"againReThList":[],"jsonStr":"","toolboxSchema":"","outputPath":[],"parId":"","kafkaStr":"","supplementStr":"","supplementStrExt":"","dataFormatMap":{},"singleType":0,"sliceTimeList":[],"maxSliceTime":None,"jisuanFazhi":0,"outTimeList":[],"runTimeSet":[]},"firstNode":1,"lastRunTime":None,"sources":[],"sinks":[{"tenantId":"","owner":"","name":source_flow_dataset_map[0]["dataset_name"],"enabled":None,"creator":"","createTime":None,"lastModifier":"","lastModifiedTime":None,"id":"","x":110,"y":180,"uiName":"sink_1","nodeType":3,"viewHistoryId":"","jobNodeId":"","datasetId":source_flow_dataset_map[0]["dataset_id"],"schemaId":None,"config":{"dataType":1,"batchEnd":1,"datasetClean":30,"schemaName":"","clusterName":"default","sliceType":"H","maxRowNum":None,"maxFileNum":None,"ccMaxSec":None,"id":None,"resourceId":"","jobDataOid":"","jobPoolOid":job_view[0]["pool_id"],"collect_1":[]},"jobNodeName":""}]},{"tenantId":"","owner":"","name":source_flow_dataset_map[0]["flow_name"],"enabled":None,"creator":"","createTime":None,"lastModifier":"","lastModifiedTime":None,"id":"","x":110,"y":440,"uiName":"dataFlow_1","nodeType":2,"viewHistoryId":"","jobId":"","jobType":2,"config":{"id":None,"jobName":job_name,"jobType":2,"sliceType":"H","status":None,"msgOid":None,"jsonOid":None,"strategyOid":2,"delayTime":1,"configStr":"100","taskType":5,"taskLevel":5,"jobPoolOid":job_view[0]["pool_id"],"timeoutTime":None,"eqTimeRun":None,"reOid":str(s_c_re[0]["re_oid"]),"cdoFilterClass":"","isTrigger":"R","handleOid":None,"handleName":"HDFS-HDFS","filterConf":"5","flowName":source_flow_dataset_map[0]["flow_name"],"flowId":source_flow_dataset_map[0]["flow_id"],"flowVer":1,"againType":"2","againReOid":None,"maxRun":None,"flowConf":"","clusterName":"default","hdfsName":"default","runTime":"","timeOrder":"1","userExe":"","otherPars":"","jobDesc":"","createTime":None,"planId":None,"delayType":1,"clusterExeName":"","userCreate":"","otherPars1":"","otherPars2":"","poolSize":None,"poolType":None,"queueName":"","againQueueName":"","jobVarList":[],"reThList":[],"againReThList":[],"jsonStr":"","toolboxSchema":"","outputPath":[],"parId":"","kafkaStr":"","supplementStr":"","supplementStrExt":"","dataFormatMap":{},"singleType":0,"sliceTimeList":[],"maxSliceTime":None,"jisuanFazhi":0,"outTimeList":[],"runTimeSet":[],"source_1":[]},"firstNode":2,"lastRunTime":None,"sources":[{"tenantId":"","owner":"","name":source_flow_dataset_map[0]["dataset_name"],"enabled":None,"creator":"","createTime":None,"lastModifier":"","lastModifiedTime":None,"id":"","x":110,"y":310,"uiName":"source_1","nodeType":4,"viewHistoryId":"","jobNodeId":"","datasetId":source_flow_dataset_map[0]["dataset_id"],"jobMapId":None,"config":{"sliceTimeRegType":1,"sliceTimeReg":"","mustLevel":3,"mustType":1,"mustPars":"","mustLine":None,"mustOuttime":None,"jobType":"","clusterName":"default","hdfsName":"default","id":None,"jobDataOid":"","jobOid":None,"jobName":"","dataformatName":"","sliceType":"H","sliceTimeRegList":[],"sliceTimeRegList2":[],"jobPoolOid":job_view[0]["pool_id"],"sink_1":[]},"jobNodeName":""}],"sinks":[{"tenantId":"","owner":"","name":sink_flow_dataset_map[0]["dataset_name"],"enabled":None,"creator":"","createTime":None,"lastModifier":"","lastModifiedTime":None,"id":"","x":110,"y":570,"uiName":"sink_2","nodeType":5,"viewHistoryId":"","jobNodeId":"","datasetId":sink_flow_dataset_map[0]["dataset_id"],"schemaId":None,"config":{"dataType":1,"batchEnd":1,"datasetClean":30,"schemaName":"","clusterName":"default","sliceType":"H","maxRowNum":"","maxFileNum":"","ccMaxSec":"","jobPoolOid":job_view[0]["pool_id"],"dataFlow_1":[]},"jobNodeName":""}]}],"links":[{"source":"collect_1","target":"sink_1"},{"source":"source_1","target":"dataFlow_1"},{"source":"dataFlow_1","target":"sink_2"},{"source":"sink_1","target":"source_1"}],"steps":[{"tenantId":"","owner":"","name":dc_task[0]["name"],"enabled":None,"creator":"","createTime":None,"lastModifier":"","lastModifiedTime":None,"id":"","x":110,"y":50,"uiName":"collect_1","nodeType":1,"viewHistoryId":"","jobId":dc_task[0]["id"],"jobType":1,"config":{"id":dc_task[0]["id"],"jobName":"","jobType":None,"sliceType":"H","status":None,"msgOid":None,"jsonOid":None,"strategyOid":None,"delayTime":None,"configStr":"","taskType":None,"taskLevel":None,"jobPoolOid":job_view[0]["pool_id"],"timeoutTime":None,"eqTimeRun":None,"reOid":None,"cdoFilterClass":"","isTrigger":"","handleOid":None,"handleName":"","filterConf":"","flowName":dc_task[0]["name"],"flowId":dc_task[0]["id"],"flowVer":None,"againType":"","againReOid":None,"maxRun":None,"flowConf":"","clusterName":"","hdfsName":"default","runTime":"","timeOrder":"","userExe":"","otherPars":"","jobDesc":"","createTime":None,"planId":None,"delayType":None,"clusterExeName":"","userCreate":"","otherPars1":"","otherPars2":"","poolSize":None,"poolType":None,"queueName":"","againQueueName":"","jobVarList":[],"reThList":[],"againReThList":[],"jsonStr":"","toolboxSchema":"","outputPath":[],"parId":"","kafkaStr":"","supplementStr":"","supplementStrExt":"","dataFormatMap":{},"singleType":0,"sliceTimeList":[],"maxSliceTime":None,"jisuanFazhi":0,"outTimeList":[],"runTimeSet":[]},"firstNode":1,"sources":[],"sinks":[{"tenantId":"","owner":"","name":source_flow_dataset_map[0]["dataset_name"],"enabled":None,"creator":"","createTime":None,"lastModifier":"","lastModifiedTime":None,"id":"","x":None,"y":None,"uiName":"sink_1","nodeType":3,"viewHistoryId":"","jobNodeId":"","datasetId":source_flow_dataset_map[0]["dataset_id"],"schemaId":None,"config":{"dataType":1,"batchEnd":1,"datasetClean":30,"schemaName":"","clusterName":"default","sliceType":"H","maxRowNum":None,"maxFileNum":None,"ccMaxSec":None,"id":None,"resourceId":"","jobDataOid":"","jobPoolOid":job_view[0]["pool_id"]},"jobNodeName":""}],"lastRunTime":None},{"tenantId":"","owner":"","name":source_flow_dataset_map[0]["dataset_name"],"enabled":None,"creator":"","createTime":None,"lastModifier":"","lastModifiedTime":None,"id":"","x":110,"y":180,"uiName":"sink_1","nodeType":3,"viewHistoryId":"","jobNodeId":"","datasetId":source_flow_dataset_map[0]["dataset_id"],"schemaId":None,"config":{"dataType":1,"batchEnd":1,"datasetClean":30,"schemaName":"","clusterName":"default","sliceType":"H","maxRowNum":None,"maxFileNum":None,"ccMaxSec":None,"id":None,"resourceId":"","jobDataOid":"","jobPoolOid":job_view[0]["pool_id"],"collect_1":[]},"jobNodeName":""},{"tenantId":"","owner":"","name":source_flow_dataset_map[0]["flow_name"],"enabled":None,"creator":"","createTime":None,"lastModifier":"","lastModifiedTime":None,"id":"","x":110,"y":440,"uiName":"dataFlow_1","nodeType":2,"viewHistoryId":"","jobId":"","jobType":2,"config":{"id":None,"jobName":job_name,"jobType":2,"sliceType":"H","status":None,"msgOid":None,"jsonOid":None,"strategyOid":2,"delayTime":1,"configStr":"100","taskType":5,"taskLevel":5,"jobPoolOid":job_view[0]["pool_id"],"timeoutTime":None,"eqTimeRun":None,"reOid":str(s_c_re[0]["re_oid"]),"cdoFilterClass":"","isTrigger":"R","handleOid":None,"handleName":"HDFS-HDFS","filterConf":"5","flowName":source_flow_dataset_map[0]["flow_name"],"flowId":source_flow_dataset_map[0]["flow_id"],"flowVer":1,"againType":"2","againReOid":None,"maxRun":None,"flowConf":"","clusterName":"default","hdfsName":"default","runTime":"","timeOrder":"1","userExe":"","otherPars":"","jobDesc":"","createTime":None,"planId":None,"delayType":1,"clusterExeName":"","userCreate":"","otherPars1":"","otherPars2":"","poolSize":None,"poolType":None,"queueName":"","againQueueName":"","jobVarList":[],"reThList":[],"againReThList":[],"jsonStr":"","toolboxSchema":"","outputPath":[],"parId":"","kafkaStr":"","supplementStr":"","supplementStrExt":"","dataFormatMap":{},"singleType":0,"sliceTimeList":[],"maxSliceTime":None,"jisuanFazhi":0,"outTimeList":[],"runTimeSet":[],"source_1":[]},"firstNode":2,"sources":[{"tenantId":"","owner":"","name":source_flow_dataset_map[0]["dataset_name"],"enabled":None,"creator":"","createTime":None,"lastModifier":"","lastModifiedTime":None,"id":"","x":None,"y":None,"uiName":"source_1","nodeType":4,"viewHistoryId":"","jobNodeId":"","datasetId":source_flow_dataset_map[0]["dataset_id"],"jobMapId":None,"config":{"sliceTimeRegType":1,"sliceTimeReg":"","mustLevel":3,"mustType":1,"mustPars":"","mustLine":None,"mustOuttime":None,"jobType":"","clusterName":"default","hdfsName":"default","id":None,"jobDataOid":"","jobOid":None,"jobName":"","dataformatName":"","sliceType":"H","sliceTimeRegList":[],"sliceTimeRegList2":[],"jobPoolOid":job_view[0]["pool_id"]},"jobNodeName":""}],"sinks":[{"tenantId":"","owner":"","name":sink_flow_dataset_map[0]["dataset_name"],"enabled":None,"creator":"","createTime":None,"lastModifier":"","lastModifiedTime":None,"id":"","x":None,"y":None,"uiName":"sink","nodeType":5,"viewHistoryId":"","jobNodeId":"","datasetId":sink_flow_dataset_map[0]["dataset_id"],"schemaId":None,"config":None,"jobNodeName":""}],"lastRunTime":None},{"tenantId":"","owner":"","name":source_flow_dataset_map[0]["dataset_name"],"enabled":None,"creator":"","createTime":None,"lastModifier":"","lastModifiedTime":None,"id":"","x":110,"y":310,"uiName":"source_1","nodeType":4,"viewHistoryId":"","jobNodeId":"","datasetId":source_flow_dataset_map[0]["dataset_id"],"jobMapId":None,"config":{"sliceTimeRegType":1,"sliceTimeReg":"","mustLevel":3,"mustType":1,"mustPars":"","mustLine":None,"mustOuttime":None,"jobType":"","clusterName":"default","hdfsName":"default","id":None,"jobDataOid":"","jobOid":None,"jobName":"","dataformatName":"","sliceType":"H","sliceTimeRegList":[],"sliceTimeRegList2":[],"jobPoolOid":job_view[0]["pool_id"],"sink_1":[]},"jobNodeName":""},{"tenantId":"","owner":"","name":sink_flow_dataset_map[0]["dataset_name"],"enabled":None,"creator":"","createTime":None,"lastModifier":"","lastModifiedTime":None,"id":"","x":110,"y":570,"uiName":"sink_2","nodeType":5,"viewHistoryId":"","jobNodeId":"","datasetId":sink_flow_dataset_map[0]["dataset_id"],"schemaId":None,"config":{"dataType":1,"batchEnd":1,"datasetClean":30,"schemaName":"","clusterName":"default","sliceType":"H","maxRowNum":"","maxFileNum":"","ccMaxSec":"","jobPoolOid":job_view[0]["pool_id"],"dataFlow_1":[]},"jobNodeName":""}]}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_qa_jobview_history(data):
    try:
        data = data.split("&")
        dc_task = "select id,name from dc_task where name like'%s%%%%' order by create_time desc limit 1" % data[0]
        dc_task = ms.ExecuQuery(dc_task.encode('utf-8'))
        sql = "select id, owner, tenant_id, name, pool_id, resource_id, slice_type from s_v_job_view where name like'%s%%%%' order by create_time desc limit 1" % data[1]
        job_view = ms.ExecuQuery(sql.encode('utf-8'))
        source_flow_dataset_map = "select mfdm.dataset_id,mfdm.dataset_name,mfdm.flow_id,mfdm.flow_name from merce_flow_dataset_map mfdm inner join merce_flow mf on mf.id =mfdm.flow_id where mfdm.`type` ='source' and mf.name like '%s%%%%' order by mf.create_time DESC limit 1" % data[2]
        source_flow_dataset_map = ms.ExecuQuery(source_flow_dataset_map.encode('utf-8'))
        job_name = source_flow_dataset_map[0]["flow_name"]+"."+source_flow_dataset_map[0]["flow_id"]
        s_c_re = "select re_oid from s_c_re where re_name like'%s%%%%' limit 1" % data[3]
        s_c_re = ms.ExecuQuery(s_c_re.encode('utf-8'))
        new_data = {"locked":True,"tenantId":job_view[0]["tenant_id"],"owner":job_view[0]["owner"],"name":job_view[0]["name"],"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"id":"1024701663835512832","viewId":job_view[0]["id"],"version":-2,"poolId":job_view[0]["pool_id"],"sliceType":"H","nodes":[{"name":source_flow_dataset_map[0]["flow_name"],"id":"","uiName":"quality_1","nodeType":2,"jobType":4,"config":{"id":None,"jobName":job_name,"jobType":2,"sliceType":"H","status":None,"msgOid":None,"jsonOid":None,"strategyOid":0,"delayTime":1,"configStr":"100","taskType":5,"taskLevel":5,"jobPoolOid":job_view[0]["pool_id"],"timeoutTime":None,"eqTimeRun":None,"reOid":s_c_re[0]["re_oid"],"cdoFilterClass":"","isTrigger":"R","handleOid":None,"handleName":"HDFS-HDFS","filterConf":"5","flowName":source_flow_dataset_map[0]["flow_name"],"flowId":source_flow_dataset_map[0]["flow_id"],"flowVer":3,"againType":"2","againReOid":None,"maxRun":None,"flowConf":"","clusterName":"default","hdfsName":"default","runTime":"","timeOrder":"1","userExe":"","otherPars":"","jobDesc":"","createTime":None,"planId":None,"delayType":1,"clusterExeName":"","userCreate":"","otherPars1":"","otherPars2":"","jisuanFazhi":0,"source_1":[]},"firstNode":2,"x":110,"y":440,"sources":[{"name":source_flow_dataset_map[0]["dataset_name"],"uiName":"source_1","nodeType":4,"datasetId":source_flow_dataset_map[0]["dataset_id"],"config":{"sliceTimeRegType":1,"sliceTimeReg":"","mustLevel":3,"mustType":1,"mustPars":"","mustLine":None,"mustOuttime":None,"jobType":"","clusterName":"default","hdfsName":"default","id":None,"jobDataOid":"","jobOid":None,"jobName":"","dataformatName":"","jobPoolOid":job_view[0]["pool_id"],"sliceType":"H","sink_2":[]},"x":110,"y":310}],"sinks":[]},{"name":dc_task[0]["name"],"id":"","uiName":"collect_2","nodeType":1,"jobType":1,"config":{"id":dc_task[0]["id"],"jobName":"","jobType":None,"sliceType":"H","status":None,"msgOid":None,"jsonOid":None,"strategyOid":None,"delayTime":None,"configStr":"","taskType":None,"taskLevel":None,"jobPoolOid":job_view[0]["pool_id"],"timeoutTime":None,"eqTimeRun":None,"reOid":None,"cdoFilterClass":"","isTrigger":"","handleOid":None,"handleName":"","filterConf":"","flowName":dc_task[0]["name"],"flowId":dc_task[0]["id"],"flowVer":None,"againType":"","againReOid":None,"maxRun":None,"flowConf":"","clusterName":"","hdfsName":"default","runTime":"","timeOrder":"","userExe":"","otherPars":"","jobDesc":"","createTime":None,"planId":None,"delayType":None,"clusterExeName":"","userCreate":"","otherPars1":"","otherPars2":"","jisuanFazhi":0},"firstNode":1,"x":110,"y":50,"sources":[],"sinks":[{"name":source_flow_dataset_map[0]["dataset_name"],"uiName":"sink_2","nodeType":3,"datasetId":source_flow_dataset_map[0]["dataset_id"],"config":{"dataType":1,"batchEnd":1,"datasetClean":30,"schemaName":"","clusterName":"default","sliceType":"H","maxRowNum":None,"maxFileNum":None,"ccMaxSec":None,"id":None,"resourceId":"","jobDataOid":"","jobPoolOid":job_view[0]["pool_id"],"collect_2":[]},"x":110,"y":180}]}],"links":[{"source":"source_1","target":"quality_1"},{"source":"collect_2","target":"sink_2"},{"source":"sink_2","target":"source_1"}],"steps":[{"name":source_flow_dataset_map[0]["flow_name"],"id":"","uiName":"quality_1","nodeType":2,"jobType":4,"config":{"id":None,"jobName":job_name,"jobType":2,"sliceType":"H","status":None,"msgOid":None,"jsonOid":None,"strategyOid":0,"delayTime":1,"configStr":"100","taskType":5,"taskLevel":5,"jobPoolOid":job_view[0]["pool_id"],"timeoutTime":None,"eqTimeRun":None,"reOid":s_c_re[0]["re_oid"],"cdoFilterClass":"","isTrigger":"R","handleOid":None,"handleName":"HDFS-HDFS","filterConf":"5","flowName":source_flow_dataset_map[0]["flow_name"],"flowId":source_flow_dataset_map[0]["flow_id"],"flowVer":3,"againType":"2","againReOid":None,"maxRun":None,"flowConf":"","clusterName":"default","hdfsName":"default","runTime":"","timeOrder":"1","userExe":"","otherPars":"","jobDesc":"","createTime":None,"planId":None,"delayType":1,"clusterExeName":"","userCreate":"","otherPars1":"","otherPars2":"","jisuanFazhi":0,"source_1":[]},"firstNode":2,"sources":[{"name":source_flow_dataset_map[0]["dataset_name"],"uiName":"source_1","nodeType":4,"datasetId":source_flow_dataset_map[0]["dataset_id"],"config":{"sliceTimeRegType":1,"sliceTimeReg":"","mustLevel":3,"mustType":1,"mustPars":"","mustLine":None,"mustOuttime":None,"jobType":"","clusterName":"default","hdfsName":"default","id":None,"jobDataOid":"","jobOid":None,"jobName":"","dataformatName":"","jobPoolOid":job_view[0]["pool_id"],"sliceType":"H"}}],"sinks":[],"x":110,"y":440},{"name":source_flow_dataset_map[0]["dataset_name"],"uiName":"source_1","nodeType":4,"datasetId":source_flow_dataset_map[0]["dataset_id"],"config":{"sliceTimeRegType":1,"sliceTimeReg":"","mustLevel":3,"mustType":1,"mustPars":"","mustLine":None,"mustOuttime":None,"jobType":"","clusterName":"default","hdfsName":"default","id":None,"jobDataOid":"","jobOid":None,"jobName":"","dataformatName":"","jobPoolOid":job_view[0]['pool_id'],"sliceType":"H","sink_2":[]},"x":110,"y":310},{"name":dc_task[0]["name"],"id":"","uiName":"collect_2","nodeType":1,"jobType":1,"config":{"id":dc_task[0]["id"],"jobName":"","jobType":None,"sliceType":"H","status":None,"msgOid":None,"jsonOid":None,"strategyOid":None,"delayTime":None,"configStr":"","taskType":None,"taskLevel":None,"jobPoolOid":job_view[0]["pool_id"],"timeoutTime":None,"eqTimeRun":None,"reOid":None,"cdoFilterClass":"","isTrigger":"","handleOid":None,"handleName":"","filterConf":"","flowName":dc_task[0]["name"],"flowId":dc_task[0]["id"],"flowVer":None,"againType":"","againReOid":None,"maxRun":None,"flowConf":"","clusterName":"","hdfsName":"default","runTime":"","timeOrder":"","userExe":"","otherPars":"","jobDesc":"","createTime":None,"planId":None,"delayType":None,"clusterExeName":"","userCreate":"","otherPars1":"","otherPars2":"","jisuanFazhi":0},"firstNode":1,"sources":[],"sinks":[{"name":source_flow_dataset_map[0]["dataset_name"],"uiName":"sink_2","nodeType":3,"datasetId":source_flow_dataset_map[0]["dataset_id"],"config":{"dataType":1,"batchEnd":1,"datasetClean":30,"schemaName":"","clusterName":"default","sliceType":"H","maxRowNum":None,"maxFileNum":None,"ccMaxSec":None,"id":None,"resourceId":"","jobDataOid":"","jobPoolOid":job_view[0]["pool_id"]}}],"x":110,"y":50},{"name":source_flow_dataset_map[0]["dataset_name"],"uiName":"sink_2","nodeType":3,"datasetId":source_flow_dataset_map[0]["dataset_id"],"config":{"dataType":1,"batchEnd":1,"datasetClean":30,"schemaName":"","clusterName":"default","sliceType":"H","maxRowNum":None,"maxFileNum":None,"ccMaxSec":None,"id":None,"resourceId":"","jobDataOid":"","jobPoolOid":job_view[0]["pool_id"],"collect_2":[]},"x":110,"y":180}]}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def execution_task(data):
    try:
        dc_task = "select id from dc_task where name like'%s%%%%' order by create_time desc limit 1" % data
        dc_task = ms.ExecuQuery(dc_task.encode('utf-8'))
        taskid =[]
        taskid.append(dc_task[0]["id"])
        new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"taskId","oper":"EQUAL","value":taskid}]},"ordSort":[{"name":"startTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def qa_rule_task(data):
    try:
        merce_dataset = "select id, owner,name from merce_dataset where name like'%s%%%%' order by create_time desc limit 1" % data
        merce_dataset = ms.ExecuQuery(merce_dataset.encode('utf-8'))
        qa_rule = "select id from qa_rule where name ='数据空值规则' order by create_time desc limit 1"
        qa_rule = ms.ExecuQuery(qa_rule.encode('utf-8'))
        qa_rule1 = "select id from qa_rule where name ='字符长度校验' order by create_time desc limit 1"
        qa_rule1 = ms.ExecuQuery(qa_rule1.encode('utf-8'))
        new_data = {"jobType":"BATCH_COLUMN","name":"gjb_质检任务随机数","responsibleId":merce_dataset[0]["owner"],"responsibleName":"admin","description":"","schedulerType":"SINGLE","schedulerCycle":"","schedulerDelay":{"delay":0,"unit":"MINUTE"},"iterativeType":"FULL","ruleConf":{"datasetRuless":[],"datasetFieldRules":{"fieldRuless":[{"field":{"fieldName":"name","fieldType":"string"},"rules":[{"ruleId":str(qa_rule[0]["id"]),"ruleName":"数据空值规则","version":1,"weights":1,"buildIn":1,"parameters":{"templateType":"T2","alarmRanges":[0,25,50],"yellowValue":0,"orangeValue":25,"redValue":50}},{"ruleId":str(qa_rule1[0]["id"]),"ruleName":"字符长度校验","version":1,"weights":1,"buildIn":1,"parameters":{"templateType":"T5","operator":"<","value":50,"alarmRanges":[0,25,50],"yellowValue":0,"orangeValue":25,"redValue":50}}]}],"conditionSql":"","dimensions":[],"increment":{"incrType":"PARTITION","incrUnit":"DAY","incrField":"","incrFormat":""},"dataset":{"datasetId":merce_dataset[0]["id"],"datasetName":merce_dataset[0]["name"]}}},"notice":{"noticeTypes":[],"noticeLevels":[],"noticeMembers":[]},"isExceptionOut":1,"approverName":"admin","approverId":merce_dataset[0]["owner"],"tempEntity":{"noticeMembers":[],"datasets":merce_dataset[0]["name"]}}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def approval_qa_task(data):
    try:
        data = data.split("&")
        dw_approval_record = "select id,target_type from dw_approval_record where approval_type='%s' and approval_status ='PENDING' and target_name like '%s%%%%' order by create_time desc limit 1" % (data[2], data[0])
        dw_approval_record = ms.ExecuQuery(dw_approval_record.encode('utf-8'))
        ids = []
        ids.append(str(dw_approval_record[0]["id"]))
        if "PASS" in data:
            new_data = {"approvalComments":"通过","ids": ids,"approvalType":data[2],"approvalStatus":data[1],"targetType":dw_approval_record[0]["target_type"]}
            return new_data
        else:
            new_data = {"approvalComments": "不通过", "ids": ids, "approvalType": data[2], "approvalStatus": data[1],
                        "targetType": dw_approval_record[0]["target_type"]}
            return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def publish_qa_flow(data):
    try:
        data = data.split("#")
        sql = "select id, owner, tenant_id, creator, enabled, steps, name, flow_type, resource_id from merce_flow where source='qualityanalysisflow' and name like'%s%%%%' order by create_time desc limit 1" % data[0]
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        dataset_sql = " select md.id,md.name,md.schema_id ,ms.name as schema_name from merce_dataset md inner join merce_schema ms on md.schema_id =ms.id where md.source='output' and md.name like '%s%%%%' order by md.last_modified_time desc limit 1"% data[1]
        dataset_info = ms.ExecuQuery(dataset_sql.encode('utf-8'))
        steps = job_info[0]["steps"]
        steps_str = parameter_ungzip(steps)
        attr_dict = json.loads(steps_str)
        del attr_dict[0]
        new_data = {"tenantId":job_info[0]["tenant_id"],"owner":job_info[0]["owner"],"name":job_info[0]["name"],"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"id":job_info[0]["id"],"version":0,"groupCount":None,"groupFieldValue":None,"resourceId":job_info[0]["resource_id"],"isHide":0,"resource":None,"source":"qualityanalysisflow","steps":[{"id":"source_1","name":dataset_info[0]["name"],"type":"source","otherConfigurations":{"dataset-dateTo":"","schemaVersion":1,"dataset-dateFrom":"","dataset-paths":"","dataset-sql":"","schemaId":dataset_info[0]["schema_id"],"interceptor":"","schemaName":dataset_info[0]["schema_name"],"dataset":[{"datasetId":dataset_info[0]["id"],"ignoreMissingPath":False,"rule":"set_1","storage":"HDFS","dataset":dataset_info[0]["name"]}]},"inputConfigurations":None,"outputConfigurations":{"output":[{"name":"name","type":"string","alias":"name","description":"","fieldCategory":None,"specId":None},{"name":"age","type":"int","alias":"age","description":"","fieldCategory":None,"specId":None},{"name":"sex","type":"string","alias":"sex","description":"","fieldCategory":None,"specId":None},{"name":"stime","type":"string","alias":"stime","description":"","fieldCategory":None,"specId":None}]},"libs":None,"flowId":None,"x":120,"y":120,"implementation":None,"uiConfigurations":{"output":["output"]}},{"id":"sql_1","name":"sql_1_name","type":"sql","otherConfigurations":{"interceptor":"","sql":"select to_json(struct(*)) as value from( select #{executionId:0} as executionId,#{bizdate:20220430} as bizdate,coalesce(count(1),0) as inputRecords,'1024023560158896128' as sourceDatasetId,'name' as sourceFieldName,coalesce(sum(case when !(length(case when name is null then '' else cast(name as string) end) < 50) then 1 else 0 end),0) as rule_0,coalesce(sum(case when IsEmpty(name) then 1 else 0 end),0) as rule_1 from input )"},"inputConfigurations":{"input":[{"name":"name","type":"string","alias":"name","description":"","fieldCategory":None,"specId":None},{"name":"age","type":"int","alias":"age","description":"","fieldCategory":None,"specId":None},{"name":"sex","type":"string","alias":"sex","description":"","fieldCategory":None,"specId":None},{"name":"stime","type":"string","alias":"stime","description":"","fieldCategory":None,"specId":None}]},"outputConfigurations":{"output":[{"name":"value","type":"string","alias":"value","description":"输出字段的压缩值","fieldCategory":None,"specId":None}]},"libs":None,"flowId":None,"x":240,"y":120,"implementation":None,"uiConfigurations":{"output":["output"],"input":["input"]}},{"id":"sink_1","name":"sink_1","type":"sink","otherConfigurations":{"schema":"mq62_quality_analysis_sink","brokers":"192.168.1.82:9094","schemaVersion":1,"schemaResource":"Schemas;qa;","zookeeper":"locahost:2181","datasetResourceId":"1014865437431750656","format":"json","kerberosSupport":False,"storage":"KAFKA","type":"KAFKA","separator":",","nullValue":"","datasetResource":"Datasets;qa;","mode":"append","quote":"\"","countWrittenRecord":True,"schemaId":"1014865437750517760","topic":"mq62_quality_analysis_sink","datasetId":"1014931048560254976","datasetType":"NORMAL","escape":"\\","dataset":"mq62_quality_analysis_sink","schemaResourceId":"1014865437721157632"},"inputConfigurations":{"input":[{"name":"value","type":"string","alias":"value","description":"输出字段的压缩值","fieldCategory":None,"specId":None}]},"outputConfigurations":{},"libs":None,"flowId":None,"x":360,"y":120,"implementation":None,"uiConfigurations":{"input":["input"]}}],"links":[{"source":"source_1","sourceOutput":"output","target":"sql_1","targetInput":"input"},{"source":"sql_1","sourceOutput":"output","target":"sink_1","targetInput":"input"}],"flowType":"dataflow","parameters":[{"category":"ref","name":"source","refs":["source_1.dataset"],"defaultVal":dataset_info[0]["name"],"description":""},{"category":"ref","name":"schema","refs":["source_1.schemaName"],"defaultVal":dataset_info[0]["schema_name"],"description":""},{"name":"executionId","category":"var","refs":["executionId"],"defaultVal":"0","value":None,"description":"执行记录ID"},{"name":"bizdate","category":"var","refs":["bizdate"],"defaultVal":"19700101","value":None,"description":"业务时间"}],"inputs":[],"outputs":[],"dependencies":[],"tags":None,"description":None,"flowId":job_info[0]["id"],"editorId":job_info[0]["owner"],"editorName":"admin","locked":True,"startTime":None,"endTime":None,"expiredPeriod":0,"customParameter":[{"name":"executionId","category":"var","refs":["executionId"],"defaultVal":"0","value":None,"description":"执行记录ID"},{"name":"bizdate","category":"var","refs":["bizdate"],"defaultVal":"19700101","value":None,"description":"业务时间"}]}
        del new_data['steps'][1:3]
        new_data['steps'].extend(attr_dict)
        sink_data = {"dataset":"mq62_quality_analysis_sink随机数"}
        deal_random(sink_data)
        new_data['steps'][2]['otherConfigurations']['dataset'] = sink_data
        return job_info[0]["id"], new_data
    except Exception as e:
        log.error("异常信息：%s" % e)