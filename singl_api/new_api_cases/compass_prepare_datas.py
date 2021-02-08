# coding:utf-8
import os
from new_api_cases.compass_deal_parameters import deal_random
from basic_info.setting import Compass_MySQL_CONFIG
from util.Open_DB import MYSQL
from util.timestamp_13 import data_now

ms = MYSQL(Compass_MySQL_CONFIG["HOST"], Compass_MySQL_CONFIG["USER"], Compass_MySQL_CONFIG["PASSWORD"], Compass_MySQL_CONFIG["DB"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))


def update_job_pool(data):
    try:
        sql = "select job_pool_oid from s_c_job_pool where pool_name like '%s%%%%' order by pool_name desc limit 1" % data
        job_pool = ms.ExecuQuery(sql.encode('utf-8'))
        job_pool_oid = job_pool[0]["job_pool_oid"]
        print('job_pool_oid-id:', job_pool_oid)
        new_data = {"id": job_pool_oid, "poolName": "autotest", "poolSize": 10, "jobFilterClass": "com.nokia.bighead.scheduler.function.job.JobFilter", "flowVer": "2", "rePoolSize": 10}
        return new_data
    except Exception as e:
        print("异常：",e)

def update_job(data):
    try:
        sql = "select job_oid,job_name,job_pool_oid,re_oid,handle_oid,flow_name,flow_id,again_re_oid,cluster_name from s_c_job where job_name like '%s%%%%' order by create_time desc limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        job_oid, job_name, job_pool_oid, re_oid, handle_oid, flow_name, flow_id, again_re_oid, cluster_name = str(job_info[0]["job_oid"]), job_info[0]["job_name"], job_info[0]["job_pool_oid"], job_info[0]["re_oid"], job_info[0]["handle_oid"], job_info[0]["flow_name"], job_info[0]["flow_id"], job_info[0]["again_re_oid"], job_info[0]["cluster_name"]
        print('job_oid,job_name,job_pool_oid,re_oid,handle_oid,flow_name,flow_id,again_re_oid,cluster_name :', job_oid, job_name, job_pool_oid, re_oid, handle_oid, flow_name, flow_id, again_re_oid, cluster_name)
        new_data = {"id":job_oid,"jobName":job_name,"jobType":2,"sliceType":"H","status":1,"msgOid":"","jsonOid":"","strategyOid":"2","delayTime":"60","configStr":"1","taskType":1,"taskLevel":5,"jobPoolOid":job_pool_oid,"timeoutTime":"","eqTimeRun":"","reOid":re_oid,"cdoFilterClass":"","isTrigger":"N","handleOid":handle_oid,"filterConf":"1","flowName":flow_name,"flowId":flow_id,"flowVer":"0","againType":"1","againReOid":again_re_oid,"maxRun":1,"flowConf":"","clusterName":cluster_name,"runTime":"","timeOrder":"1","userExe":"","otherPars":"","jobDesc":"1","createTime":data_now(),"planId":"","delayType":1,"clusterExeName":""}
        return new_data
    except Exception as e:
        print("异常：",e)

def add_job(data):
    data = data.split('&')
    try:
        print("data: ", data)
        re_sql = "select re_oid from s_c_re where re_name like '%s%%%%' limit 1" % data[0]
        re_info = ms.ExecuQuery(re_sql.encode('utf-8'))
        re_oid = str(re_info[0]["re_oid"])
        print("re_oid :", re_oid)
        flow_sql = "select dataflow_name,dataflow_oid ,cluster_name from s_c_dataflow where dataflow_name like '%s%%%%' order by create_time desc limit 1" % data[1]
        flow_info = ms.ExecuQuery(flow_sql.encode('utf-8'))
        df_name, df_id, cluster_name = flow_info[0]["dataflow_name"], str(flow_info[0]["dataflow_oid"]),flow_info[0]["cluster_name"]
        print("df_name, df_id :", df_name, df_id)
        pool_sql = "select job_pool_oid from s_c_job_pool where pool_name like '%s%%%%' limit 1" % data[2]
        pool_info = ms.ExecuQuery(pool_sql.encode('utf-8'))
        job_pool_oid = str(pool_info[0]["job_pool_oid"])
        print("job_pool_oid :", job_pool_oid)
        handle_sql = "select handle_oid from s_c_job_handle where handle_class like '%s%%%%' limit 1" % data[3]
        handle_info = ms.ExecuQuery(handle_sql.encode('utf-8'))
        handle_oid = str(handle_info[0]["handle_oid"])
        print("handle_oid :", handle_oid)
        if 'test_no_supp' in data:
            new_data = {"againReOid":re_oid,"againType":"1","clusterExeName":"","clusterName":cluster_name,"configStr":"1","delayTime":"60","delayType":1,"filterConf":"1","flowConf":"","flowName":df_name,"flowVer":"0","handleOid":handle_oid,"isTrigger":"N","jobDesc":"1","jobName":"autotest_随机数","jobPoolOid": job_pool_oid,"jobType":2,"jsonOid":"","maxRun":1,"msgOid":"","otherPars":"","planId":"","reOid":re_oid,"runTime":"","sliceType":"H","status":1,"strategyOid":"2","taskLevel":5,"taskType":1,"timeOrder":"1","userExe":"","flowId":df_id}
            deal_random(new_data)
            return new_data
        elif 'test_add_supp' in data:
            new_data = {"againReOid":re_oid,"againType":"1","clusterExeName":"","clusterName":cluster_name,"configStr":"1","delayTime":"60","delayType":1,"filterConf":"1","flowConf":"","flowName":df_name,"flowVer":"0","handleOid":handle_oid,"isTrigger":"A","jobDesc":"1","jobName":"autotest_随机数","jobPoolOid": job_pool_oid,"jobType":2,"jsonOid":"","maxRun":1,"msgOid":"","otherPars":"","planId":"","reOid":re_oid,"runTime":"","sliceType":"H","status":1,"strategyOid":"2","taskLevel":5,"taskType":1,"timeOrder":"1","userExe":"","flowId":df_id}
            deal_random(new_data)
            return new_data
        elif 'test_total_supp' in data:
            new_data = {"againReOid":re_oid,"againType":"1","clusterExeName":"","clusterName":cluster_name,"configStr":"1","delayTime":"60","delayType":1,"filterConf":"1","flowConf":"","flowName":df_name,"flowVer":"0","handleOid":handle_oid,"isTrigger":"Y","jobDesc":"1","jobName":"autotest_随机数","jobPoolOid": job_pool_oid,"jobType":2,"jsonOid":"","maxRun":1,"msgOid":"","otherPars":"","planId":"","reOid":re_oid,"runTime":"","sliceType":"H","status":1,"strategyOid":"2","taskLevel":5,"taskType":1,"timeOrder":"1","userExe":"","flowId":df_id}
            deal_random(new_data)
            return new_data
        elif 'test_cover_supp' in data:
            new_data = {"againReOid":re_oid,"againType":"1","clusterExeName":"","clusterName":cluster_name,"configStr":"1","delayTime":"60","delayType":1,"filterConf":"1","flowConf":"","flowName":df_name,"flowVer":"0","handleOid":handle_oid,"isTrigger":"R","jobDesc":"1","jobName":"autotest_随机数","jobPoolOid": job_pool_oid,"jobType":2,"jsonOid":"","maxRun":1,"msgOid":"","otherPars":"","planId":"","reOid":re_oid,"runTime":"","sliceType":"H","status":1,"strategyOid":"2","taskLevel":5,"taskType":1,"timeOrder":"1","userExe":"","flowId":df_id}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        print("异常：",e)

def add_jobSingle(data):
    try:
        sql = "select job_oid,job_name,slice_type from s_c_job where job_name = '%s' order by create_time desc limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        job_oid, job_name, slice_type = str(job_info[0]["job_oid"]), job_info[0]["job_name"], job_info[0]["slice_type"]
        print('job_oid,job_name,slice_type :', job_oid, job_name, slice_type)
        if 'gdemo' == data:
            new_data = {"jobOid": job_oid, "sliceType": slice_type, "sliceTime": data_now(), "singleType": 0, "jobLevel": "1", "taskName": job_name}
            deal_random(new_data)
            return new_data
        elif 'gdemo_add_supp' == data:
            new_data = {"jobOid": job_oid, "sliceType": slice_type, "sliceTime": data_now(), "singleType": 1, "jobLevel": "1", "taskName": job_name}
            deal_random(new_data)
            return new_data
        elif 'gdemo_total_supp' == data:
            new_data = {"jobOid": job_oid, "sliceType": slice_type, "sliceTime": data_now(), "singleType": 2, "jobLevel": "1", "taskName": job_name}
            deal_random(new_data)
            return new_data
        elif 'test_cover' == data:
            new_data = {"jobOid": job_oid, "sliceType": slice_type, "sliceTime": data_now(), "singleType": 3, "jobLevel": "1", "taskName": job_name}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        print("异常：",e)

def update_jobSingle(data):

    try:
        sql = "select single_oid,job_oid,slice_time,create_time,status,single_type,task_name from s_r_job_single where task_name like '%s%%%%' order by create_time desc limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        single_oid, job_oid, slice_time, create_time, status, single_type = str(job_info[0]["single_oid"]), str(job_info[0]["job_oid"]), str(job_info[0]["slice_time"]), str(job_info[0]["create_time"]), job_info[0]["status"], job_info[0]["single_type"]
        print('single_oid,job_oid,slice_time,create_time,status,single_type :', single_oid, job_oid, slice_time, create_time, status, single_type)
        new_data = {"id": single_oid, "jobOid": job_oid, "jobLevel": 1, "sliceTime": slice_time, "createTime": create_time, "status": status , "singleType": single_type,"taskName": "gdemo随机数", "sliceType":"H"}
        deal_random(new_data)
        return new_data
    except Exception as e:
        print("异常：",e)

def add_jobMap(data):
    try:
        sql = "select job_oid,cluster_name from s_c_job where job_name like '%s%%%%' order by create_time desc limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        job_oid, cluster_name = str(job_info[0]["job_oid"]), job_info[0]["cluster_name"]
        print('job_oid,cluster_name :', job_oid, cluster_name)
        if 'autotest' == data:
            new_data = {"jobDataOid":"","jobOid": job_oid, "clusterName": cluster_name, "dataformatName": "test_supp1211","sliceTimeRegType":1,"sliceTimeReg":"","mustLevel":3,"mustType":1,"mustPars":"100","mustLine":"","mustOuttime":""}
            return new_data
        else:
            return
    except Exception as e:
        print("异常：",e)

def update_jobMap(data):

    try:
        sql = "select job_map_oid ,job_oid,dataformat_Name,cluster_name,job_name from s_c_job_map where job_name like '%s%%%%' limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        job_map_oid, job_oid, dataformat_Name,cluster_name, job_name = str(job_info[0]["job_map_oid"]), str(job_info[0]["job_oid"]), job_info[0]["dataformat_Name"], job_info[0]["cluster_name"], job_info[0]["job_name"]
        print('job_map_oid,job_oid,dataformat_Name,cluster_name,job_name :', job_map_oid, job_oid, dataformat_Name,cluster_name, job_name)
        new_data = {"jobDataOid": "", "jobOid": job_oid, "clusterName": cluster_name, "dataformatName": dataformat_Name,"sliceTimeRegType":1,"sliceTimeReg":"","mustLevel":3,"mustType":1,"mustPars":"100","mustLine":"","mustOuttime":"", "id": job_map_oid,"jobName": job_name, "jobType": "2"}
        return new_data
    except Exception as e:
        print("异常：",e)

def update_re(data):
    try:
        sql = "select re_oid from s_c_re where re_name like  '%s%%%%' limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_oid = str(job_info[0]["re_oid"])
        print('re_oid :', re_oid)
        new_data = {"id": re_oid, "reName": "autotest随机数", "queueName": "default", "status": 1, "clusterName": "83"}
        deal_random(new_data)
        return new_data
    except Exception as e:
        print("异常：",e)

def query_reth(data):
    try:
        re_oid = []
        sql = "select re_oid from s_c_re where re_name like  '%s%%%%' limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_oid.append(str(job_info[0]["re_oid"]))
        print('re_oid :', re_oid)
        new_data = {"fieldGroup": {"fields": [{"andOr": "AND", "name": "reOid", "oper": "EQUAL", "value": re_oid}]},"ordSort":[],"pageable":{"pageNum":0,"pageSize":8,"pageable":"true"}}
        return new_data
    except Exception as e:
        print("异常：",e)

def add_reth(data):
    try:
        sql = "select re_oid from s_c_re where re_name like  '%s%%%%' limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_oid = str(job_info[0]["re_oid"])
        print('re_oid :', re_oid)
        new_data = {"reOid": re_oid, "minValue": "", "maxValue": "10000", "exeMem": "2G", "exeNum": "2", "driverMem": "2G"}
        return new_data
    except Exception as e:
        print("异常：",e)

def update_reth(data):
    try:
        sql = "select re_th_oid, re_oid from s_c_re_th where re_oid in(select t.re_oid from (select * from s_c_re where re_name like '%s%%%%' limit 1) as t)" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_th_oid, re_oid = str(job_info[0]["re_th_oid"]), str(job_info[0]["re_oid"])
        print('re_th_oid, re_oid  :', re_th_oid, re_oid)
        new_data = {"id": re_th_oid, "reOid": re_oid, "minValue": "", "maxValue": "10000", "exeMem": "2G", "exeNum": "2", "driverMem": "2G"}
        return new_data
    except Exception as e:
        print("异常：",e)

def query_rethExt(data):
    try:
        re_th_oid = []
        sql = "select re_th_oid from s_c_re_th where re_oid in(select t.re_oid from (select * from s_c_re where re_name like '%s%%%%' limit 1) as t)" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_th_oid.append(str(job_info[0]["re_th_oid"]))
        print('re_th_oid :', re_th_oid)
        new_data = {"fieldGroup": {"fields": [{"andOr": "AND", "name": "reThOid", "oper": "EQUAL", "value": re_th_oid}]}, "ordSort": [], "pageable": {"pageNum": 0, "pageSize": 8, "pageable": "true"}}
        return new_data
    except Exception as e:
        print("异常：",e)

def add_rethExt(data):
    try:
        sql = "select re_th_oid from s_c_re_th where re_oid in(select t.re_oid from (select * from s_c_re where re_name like '%s%%%%' limit 1) as t)" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_th_oid = str(job_info[0]["re_th_oid"])
        print('re_th_oid :', re_th_oid)
        new_data = {"reThOid": re_th_oid, "extKey": "spark.executor.cores", "extValue": "4"}
        return new_data
    except Exception as e:
        print("异常：",e)

def update_rethExt(data):
    try:
        sql = "select re_th_ext_oid ,re_th_oid from s_c_re_th_ext where re_th_oid in(select t.re_th_oid from s_c_re_th as t where re_oid in(select s.re_oid from (select * from s_c_re where re_name like '%s%%%%' limit 1) as s))" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        re_th_ext_oid, re_th_oid = str(job_info[0]["re_th_ext_oid"]), str(job_info[0]["re_th_oid"])
        print('re_th_ext_oid, re_th_oid  :', re_th_ext_oid, re_th_oid)
        new_data = {"id": re_th_ext_oid, "reThOid": re_th_oid, "extKey": "spark.executor.cores", "extValue": "4"}
        return new_data
    except Exception as e:
        print("异常：",e)