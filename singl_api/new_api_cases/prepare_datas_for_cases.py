# coding:utf-8
import os
import time
from urllib import parse
import requests
from basic_info.get_auth_token import get_headers, get_headers_admin, get_headers_customer
from util.format_res import dict_res
from basic_info.setting import Dsp_MySQL_CONFIG, MySQL_CONFIG
from util.Open_DB import MYSQL
from basic_info.setting import host
from selenium import webdriver
import random

from util.timestamp_13 import get_now, get_tomorrow

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])

ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))


def get_job_tasks_id(job_id):
    url = '%s/api/woven/collectors/%s/tasks' % (host, job_id)
    data = {"fieldList": [], "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
    response = requests.post(url=url, headers=get_headers(host), json=data)

    all_task_id = []
    try:
        tasks = dict_res(response.text)['content']
        for item in tasks:
            task_id = item['id']
            all_task_id.append(task_id)
    except Exception as e:
        print(e)
        return
    else:
        # print(all_task_id)
        return all_task_id


# def stop_job_task(job_id):
#     url = '%s/api/woven/collectors/WOVEN-SERVER/stopTaskList' % host
#     task_id = get_job_tasks_id(job_id)
#     print(task_id)
#     response = requests.post(url=url, headers=get_headers(), json=task_id)
#     print(response.url)
#     print(response.status_code, response.text)


def create_new_user(data):
    url = '%s/api/woven/users' % host
    response = requests.post(url=url, headers=get_headers(host), json=data)
    user_id = dict_res(response.text)["id"]
    print(user_id)
    return user_id

def collector_schema_sync(data):
    """获取采集器元数据同步后返回的task id"""
    collector = 'c9'
    # data = '{"useSystemStore": true, "dataSource":{"id": "f8523e1f-b1ff-48cd-be8d-02ab91290d5b", "name": "mysql_test_bj", "type": "JDBC", "driver": "com.mysql.jdbc.Driver", "url": "jdbc:mysql://192.168.1.189:3306/test", "username": "merce", "password": "merce", "dateToTimestamp":false, "catalog": "", "schema": "", "table": "", "selectSQL": "", "dbType": "DB"}, "dataStore":{"path": "/tmp/c1/mysql_test_bj", "format": "csv", "separator": ",", "type": "HDFS"}}'
    # data = '{"useSystemStore":true,"dataSource":{"id":"874de010-c05a-4210-91bb-aca51f3b5619","name":"gbj_0523","type":"JDBC","driver":"com.mysql.jdbc.Driver","url":"jdbc:mysql://192.168.1.199:3306/test","username":"merce","password":"123456","dateToTimestamp":false,"catalog":"","schema":"","table":"","selectSQL":"","dbType":"DB"},"dataStore":{"path":"/tmp/c9/gbj_0523","format":"csv","separator":",","type":"HDFS"}}'
    url = '%s/api/woven/collectors/%s/schema/fetch' % (host, collector)
    response = requests.post(url=url, headers=get_headers(host), data=data)
    time.sleep(3)
    print(response.text)
    return response.text
# collector_schema_sync()



def get_flow_id():
    name = "gbj_for_project_removeList" + str(random.randint(0,999999999999))
    data = {"name": name, "flowType": "dataflow",
            "projectEntity": {"id": "e47fe6f4-6086-49ed-81d1-68704aa82f2d"}, "steps": [], "links": []}
    url = '%s/api/flows/create' % host
    response = requests.post(url=url, headers=get_headers(), json=data)
    flow_id = dict_res(response.text)['id']
    print(flow_id)
    return flow_id

def resource_data(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('service_id:', flow_info[0]["id"])
    except:
        return
    new_data = {"name": "test_hdfs_student2020","datasetName":"test_hdfs_student2020","storage":"HDFS","encoder":"UTF-8","incrementField":"age","openStatus":1,"categoryId":"0","datasetId":"82e50c27-8b4a-440d-8807-eb970e6a7571","expiredTime":0,"type":0,"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"id":flow_info[0]["id"]}
    print(new_data)
    return new_data

def appconfig_data(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('service_id:', flow_info[0]["id"])
    except:
        return
    new_data = {"accessIp":["192.168.2.142"],"name":"autotest_appconfig_随机数","enabled":1,"tenantId":"e5188f23-d472-4b2d-9cfa-97a0d65994cf","owner":"b398ff8e-8a90-436d-adc6-08ee08b42958","creator":"customer3","createTime":"2020-06-28 14:11:07","lastModifier":"customer3","lastModifiedTime":"2020-06-28 14:22:00","description":"","id":flow_info[0]["id"],"custId":"b398ff8e-8a90-436d-adc6-08ee08b42958","custName":"customer3","accessKey":"015a2147-34c4-4456-ad6d-a94b513ad6e0","publicKey":"MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAKvJ3JxEUIYtnPDK3Jcn+naxiDgEzCUWeUnM56dInCVTduhBuBPbkmi7Oor+4dZ/eF5+q/h7Ay/o1WHuFbwf6NUCAwEAAQ=="}
    print(new_data)
    return new_data
def cust_data_source(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_cust_data_source where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('service_id:', flow_info[0]["id"])
    except:
        return
    new_data = {"id":flow_info[0]["id"],"name":"autotest_hdfs_csv_随机数","type":"HDFS","description":"autotest_hdfs_csv_随机数","attributes":{"quoteChar":"\"","escapeChar":"\\","path":"/auto_test/out89","format":"csv","chineseName":"autotest_hdfs_csv","header":"false","separator":",","properties":[{"name":"","value":""}],"ignoreRow":0},"owner":"b398ff8e-8a90-436d-adc6-08ee08b42958","enabled":1,"tenantId":"e5188f23-d472-4b2d-9cfa-97a0d65994cf","creator":"customer3","createTime":"2020-06-24 19:25:05","lastModifier":"customer3","lastModifiedTime":"2020-06-24 19:25:05"}
    print(new_data)
    return new_data

def corn_application_oracle():
    try:
        new_data = {"name":"test_cron_oracle","description":"","dataResId":"715891501552369664","dataResName":"test_hdfs_student2020","serviceMode":1,"transferType":1,"custTableName":"autotest_student","custDataSourceId":"717026599119093760","custDataSourceName":"test_oracle","otherConfiguration":{"scheduleType":"cron","cron":"0 0/5 * * * ? ","startTime": get_now(), "endTime": get_tomorrow()},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
        print(new_data)
        return new_data
    except:
        return 1


def admin_flow_id(data):
    # ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    # try:
    #     sql = "select id from dsp_data_service where name like 'test_hdfs_student2020_%' ORDER BY create_time desc limit 1"
    #     flow_info = ms.ExecuQuery(sql.encode('utf-8'))
    #     print(sql)
    #     print('service_id:', flow_info[0]["id"])
    # except Exception as e:
    #     raise e
    try:
        url = '%s/api/dsp/platform/service/infoById?id=%s' % (host, data)
        response = requests.get(url=url, headers=get_headers_admin(host))
        flow_id = dict_res(response.text)["content"]["jobInfo"]['flowId']
        print(flow_id)
        return flow_id
    except:
        return 1

def customer_flow_id(data):
    try:
        url = '%s/api/dsp/consumer/service/infoById?id=%s' % (host, data)
        response = requests.get(url=url, headers=get_headers_customer(host))
        flow_id = dict_res(response.text)["content"]["jobInfo"]['flowId']
        print(flow_id)
        return flow_id
    except:
        return 1
def pull_data(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('service_id:', flow_info[0]["id"])
    except:
        return
    new_data = {"dataServiceId": flow_info[0]["id"], "accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1", "encrypted": "false", "offset": 0, "size": 100, "timestamp": 1}
    print(new_data)
    return new_data

def application_pull_data(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('service_id:', flow_info[0]["id"])
    except:
        return "722842814173413376"
    new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name": "sink_es", "status": 0, "type": 0, "auditMind": "yyy"}
    print(new_data)
    return new_data

def application_once_hdfs_csv(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('service_id:', flow_info[0]["id"])
    except :
        return "723150172099444736"
    new_data = {"applicationId":flow_info[0]["id"],"description":"","enabled":1,"expiredTime":"0","name":"test_once_hdfs_csv","status":0,"type":1,"scheduleType":"once","serviceConfiguration":{"cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
    print(new_data)
    return new_data
def application_once_mysql(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('service_id:', flow_info[0]["id"])
    except :
        return "723150172099444736"
    new_data = {"applicationId":flow_info[0]["id"],"description":"","enabled":1,"expiredTime":"0","name":"test_once_mysql","status":0,"type":1,"scheduleType":"once","serviceConfiguration":{"cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
    print(new_data)
    return new_data

def application_event_hdfs_txt(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('service_id:', flow_info[0]["id"])
    except:
        return "723180170952835072"
    new_data = {"applicationId":flow_info[0]["id"],"description":"","enabled":1,"expiredTime":"0","name":"test_hdfs_txt_event","status":0,"type":1,"scheduleType":"event","serviceConfiguration":{"cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
    print(new_data)
    return new_data

def application_cron_oracle(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('service_id:', flow_info[0]["id"])
    except:
        return "723189466339999744"
    new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name":"test_cron_oracle", "status":0,"type":1,"scheduleType":"cron","serviceConfiguration":{"cron":"0 0/5 * * * ? ","startTime": get_now(), "endTime": get_tomorrow()}, "fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
    print(new_data)
    return new_data

def application_cron_mysql(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('service_id:', flow_info[0]["id"])
    except:
        return "723189466339999744"
    new_data = {"applicationId":flow_info[0]["id"],"description":"","enabled":1,"expiredTime":"0","name":"test_mysql","status":0,"type":1,"scheduleType":"cron","serviceConfiguration":{"cron":"0 0/5 * * * ? ","startTime":"1592463600000","endTime":"1592496000000"},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
    print(new_data)
    return new_data

def application_cron_hdfs_csv(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('service_id:', flow_info[0]["id"])
    except:
        return "723189466339999744"
    new_data = {"applicationId":flow_info[0]["id"],"description":"","enabled":1,"expiredTime":"0","name":"hdfs_csv","status":0,"type":1,"scheduleType":"cron","serviceConfiguration":{"cron":"0 0/5 * * * ? ","startTime":"1592463600000","endTime":"1592496000000"},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
    print(new_data)
    return new_data
def get_applicationId():
    """进入yarn页面，获取状态为finished的application id"""
    # 进入yarn页面，获取状态为finished的application id
    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.implicitly_wait(10)
    # 进入ambari页面，然后进入yarn页面
    driver.get('http://192.168.1.81:8080/#/main/services/YARN/heatmaps')
    driver.find_element_by_xpath('.//div[@class="well login span4"]/input[1]').send_keys('admin')
    driver.find_element_by_xpath('.//div[@class="well login span4"]/input[2]').send_keys('admin')
    driver.find_element_by_xpath('.//div[@class="well login span4"]/button').click()
    driver.get('http://info2:8088/cluster')
    driver.get('http://info2:8088/cluster/apps/FINISHED')
    # 获取所有finished状态的application id
    all_applications = driver.find_elements_by_xpath('.//*[@id="apps"]/tbody/tr/td[1]/a')
    # 返回第一个application id，提供给case进行查询该applicationId的log
    application_id = all_applications[0].text
    time.sleep(3)
    # print(application_id)
    # print(type(application_id))
    return application_id


def get_woven_qaoutput_dataset_path():
    """查找woven/qaoutput下的所有数据集name，并组装成woven/qaoutput/datasetname的格式"""
    url = '%s/api/datasets/query' % host
    data = {"fieldList":[{"fieldName":"parentId","fieldValue":"4f4d687c-12b3-4e09-9ba9-bcf881249ea0","comparatorOperator":"EQUAL","logicalOperator":"AND"},{"fieldName":"owner","fieldValue":"2059750c-a300-4b64-84a6-e8b086dbfd42","comparatorOperator":"EQUAL","logicalOperator":"AND"}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
    response = requests.post(url=url,headers=get_headers(), json=data)
    contents = dict_res(response.text)["content"]
    path = []
    for content in contents:
        content_path = 'woven/qaoutput/' + content["name"]
        content_path.replace('/', '%252F')   # 应该使用parse.quote() 进行URL编码进行处理。稍后解决
        path.append(content_path.replace('/', '%252F'))
    # print(path)
    return path

dir1 = ab_dir('woven-common-3.0.jar')


def upload_jar_file_filter():
    url = "%s/api/processconfigs/uploadjar/filter class" % host
    # files = {"file": open('./new_api_cases/woven-common-3.0.jar', 'rb')}
    files = {"file": open(dir1, 'rb')}
    headers = get_headers(host)
    headers.pop('Content-Type')
    try:
        response = requests.post(url, files=files, headers=headers)
    # print(response.text)
        filter_fileName = dict_res(response.text)["fileName"]
    except:
        return
    else:
        return filter_fileName


def upload_jar_file_workflow():
    url = "%s/api/processconfigs/uploadjar/workflow selector" % host
    print(url)
    # files = {"file": open('./new_api_cases/woven-common-3.0.jar', 'rb')}
    files = {"file": open(dir1, 'rb')}
    headers = get_headers(host)
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

# upload_jar_file_workflow()

def upload_jar_file_dataflow():
    url = "%s/api/processconfigs/uploadjar/dataflow selector" % host
    unquote_url = parse.unquote(url)
    # files = {"file": open('./new_api_cases/woven-common-3.0.jar', 'rb')}
    files = {"file": open(dir1, 'rb')}
    headers = get_headers(host)
    headers.pop('Content-Type')
    try:
        response = requests.post(url, files=files, headers=headers)
    # print(response.text)
        data_fileName = dict_res(response.text)["fileName"]
        print(data_fileName)
    except:
        return
    else:
        return data_fileName

# upload_jar_file_dataflow()

def upload_file_standard(host,file,url):
    dir2 = ab_dir(file)
    # url = "%s/api/woven/upload/read/excel?maxSheet=1&maxRow=10000&maxColumn=3" % host
    unquote_url = parse.unquote(url)
    files = {"file": open(dir2, 'rb')}
    headers = get_headers(host)
    headers.pop('Content-Type')
    try:
        response = requests.post(url, files=files, headers=headers)
    except:
        return
    else:
        return response.status_code,response.text

# upload_file_standard()
#get_dsp_flow_id(data)
#get_service_id()