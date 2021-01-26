# coding:utf-8
import os
import requests
from basic_info.get_auth_token import get_headers_compass
from new_api_cases.compass_deal_parameters import deal_random
from util.format_res import dict_res
from basic_info.setting import Compass_MySQL_CONFIG, compass_host
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
    except :
        return "803208446068391936"
    new_data = {"id": job_pool_oid, "poolName": "autotest", "poolSize": 10, "jobFilterClass": "com.nokia.bighead.scheduler.function.job.JobFilter", "flowVer": "2", "rePoolSize": 10}
    return new_data

def update_job(data):

    try:
        sql = "select job_oid,job_name,job_pool_oid,re_oid,handle_oid,flow_name,flow_id,again_re_oid,cluster_name from s_c_job where job_name like '%s%%%%' order by create_time desc limit 1" % data
        job_info = ms.ExecuQuery(sql.encode('utf-8'))
        job_oid, job_name, job_pool_oid, re_oid, handle_oid, flow_name, flow_id, again_re_oid, cluster_name = str(job_info[0]["job_oid"]), job_info[0]["job_name"], job_info[0]["job_pool_oid"], job_info[0]["re_oid"], job_info[0]["handle_oid"], job_info[0]["flow_name"], job_info[0]["flow_id"], job_info[0]["again_re_oid"], job_info[0]["cluster_name"]
        print('job_oid,job_name,job_pool_oid,re_oid,handle_oid,flow_name,flow_id,again_re_oid,cluster_name :', job_oid, job_name, job_pool_oid, re_oid, handle_oid, flow_name, flow_id, again_re_oid, cluster_name)
    except :
        return
    new_data = {"id":job_oid,"jobName":job_name,"jobType":2,"sliceType":"H","status":1,"msgOid":"","jsonOid":"","strategyOid":"2","delayTime":"60","configStr":"1","taskType":1,"taskLevel":5,"jobPoolOid":job_pool_oid,"timeoutTime":"","eqTimeRun":"","reOid":re_oid,"cdoFilterClass":"","isTrigger":"N","handleOid":handle_oid,"filterConf":"1","flowName":flow_name,"flowId":flow_id,"flowVer":"0","againType":"1","againReOid":again_re_oid,"maxRun":1,"flowConf":"","clusterName":cluster_name,"runTime":"","timeOrder":"1","userExe":"","otherPars":"","jobDesc":"1","createTime":data_now(),"planId":"","delayType":1,"clusterExeName":""}
    return new_data

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
    except :
        return

dir1 = ab_dir('woven-common-3.0.jar')



def upload_jar_file_workflow():
    url = "%s/api/processconfigs/uploadjar/workflow selector" % compass_host
    print(url)
    # files = {"file": open('./new_api_cases/woven-common-3.0.jar', 'rb')}
    files = {"file": open(dir1, 'rb')}
    headers = get_headers_compass(compass_host)
    headers.pop('Content-Type')
    try:
        response = requests.post(url, files=files, headers=headers)
        print(response.text)
        workflow_fileName = dict_res(response.text)["fileName"]
        print(workflow_fileName)
    except:
        return
    else:
        return workflow_fileName

#data='default&carpo&supp&default&com.nokia.bighead.scheduler.task.JobTaskRunThreadCdo'
#print(add_job(data))