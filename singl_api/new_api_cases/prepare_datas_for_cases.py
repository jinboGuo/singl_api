# coding:utf-8
import os
import time
from urllib import parse
import requests
from basic_info.get_auth_token import get_headers, get_headers_admin, get_headers_customer
from util.format_res import dict_res
from basic_info.setting import MySQL_CONFIG
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
        return all_task_id


def create_new_user(data):
    url = '%s/api/woven/users' % host
    response = requests.post(url=url, headers=get_headers(host), json=data)
    user_id = dict_res(response.text)["id"]
    #print(user_id)
    return user_id

def collector_schema_sync(data):
    """获取采集器元数据同步后返回的task id"""
    collector = 'c9'
    url = '%s/api/woven/collectors/%s/schema/fetch' % (host, collector)
    response = requests.post(url=url, headers=get_headers(host), data=data)
    time.sleep(3)
    #print(response.text)
    return response.text


def get_flow_id():
    name = "gbj_for_project_removeList" + str(random.randint(0,999999999999))
    data = {"name": name, "flowType": "dataflow",
            "projectEntity": {"id": "e47fe6f4-6086-49ed-81d1-68704aa82f2d"}, "steps": [], "links": []}
    url = '%s/api/flows/create' % host
    response = requests.post(url=url, headers=get_headers(), json=data)
    flow_id = dict_res(response.text)['id']
    #print(flow_id)
    return flow_id


def admin_flow_id(data):
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
        return response.status_code, response.text

def dss_data(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id,owner from merce_resource_dir where creator='admin' and name='Datasources' and parent_id is null"
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('resource_id-owner:', flow_info[0]["id"], flow_info[0]["owner"])
    except:
        return
    if 'gjb_api_for_all_type_JDBC_datasource_test' in data:
        new_data = {"id":"","name":"gjb_api_for_all_type_JDBC_datasource_test_随机数","type":"DB","description":"","attributes":{"jarPath":"mysql-connector-java-5.1.48.jar","DBType":"Mysql","host":"192.168.1.75","port":3306,"database":"merce","user":"merce","password":"AES(cad2fb721d282f6e5151605a1874ffe4)","driver":"com.mysql.jdbc.Driver","properties":[{"name":"","value":""}],"url":"jdbc:mysql://192.168.1.75:3306/merce","chineseName":"","dateToTimestamp":"false","catalog":"","schema":"","batchsize":10000,"name":"mysql"},"resource":{"id": flow_info[0]["id"]}}
        deal_random(new_data)
        return new_data
    elif 'gjb_for_all_type_http_datasource_test' in data:
        new_data = {"id": "", "name": "gjb_for_all_type_http_datasource_test_随机数", "type": "HTTP", "description":"","attributes":{"method":"GET","rootPath":"gbj_http","parameters":"","url":"gbj_http","properties":[{"name":"","value":""}]}, "owner": flow_info[0]["owner"], "resource": {"id": flow_info[0]["id"]}}
        deal_random(new_data)
        return new_data
    elif 'gjb_for_all_type_ftp_datasource_test' in data:
        new_data = {"id":"","name":"gjb_for_all_type_ftp_datasource_test_随机数","type":"FTP","description":"","attributes":{"host":"info4","port":"22","username":"europa","password":"europa","recursive":"true","secure":"true","skipHeader": "false", "dir": "/home/europa/ftp_auto_import", "fieldsSeparator": ","}, "owner": flow_info[0]["owner"], "resource": {"id": flow_info[0]["id"]}}
        deal_random(new_data)
        return new_data
    elif 'gjb_for_all_type_socket_datasource_test' in data:
        new_data = {"id":"","name":"gjb_for_all_type_socket_datasource_test_随机数","type":"socket","description":"","attributes":{"charset":"utf-8","ipAddress":"gbj_socket","port":"gbj_socket", "protocol": "TCP"}, "owner": flow_info[0]["owner"], "resource": {"id": flow_info[0]["id"]}}
        deal_random(new_data)
        return new_data
    elif 'gjb_for_all_type_MANGODB_datasource_test' in data:
        new_data = {"id":"","name":"gjb_for_all_type_MANGODB_datasource_test_随机数","type":"MONGODB","description":"","attributes":{"address":"gbj_mangodb","port":"27017","username":"gbj_mangodb","password":"gbj_mangodb","database":"gbj_mangodb"}, "owner": flow_info[0]["owner"], "resource": {"id": flow_info[0]["id"]}}
        deal_random(new_data)
        return new_data
    elif 'gjb_for_all_type_es_datasource_test' in data:
        new_data = {"id":"","name":"gjb_for_all_type_es_datasource_test_随机数","type":"ES","description":"","attributes":{"clusterName":"es85","ipAddresses":"192.168.1.85:9200","index":"test","indexType":"test","version":"5.x"}, "owner": flow_info[0]["owner"], "resource": {"id": flow_info[0]["id"]}}
        deal_random(new_data)
        return new_data
    elif 'datasource_query' in data:
        new_data = {"fieldList": [{"fieldName":"parentId","fieldValue": flow_info[0]["id"], "comparatorOperator":"EQUAL","logicalOperator":"AND"}],"sortObject": {"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
        return new_data
    else:
        return

def upddss_data(data):
    try:
        sql = "select id,owner,name,tenant_id,resource_id from merce_dss where name like '%s%%%%' ORDER BY create_time limit 1" % data
        dss_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        dss_id = dss_info[0]["id"]
        print('dss_id-owner-name:', dss_info[0]["id"], dss_info[0]["owner"], dss_info[0]["name"])
    except:
        return
    if 'test_Mysql' in data:
        print("dss_id-name:",dss_id, dss_info[0]["name"])
        new_data = {"id": dss_id, "name": dss_info[0]["name"], "type": "DB","description":"","owner": dss_info[0]["owner"],"attributes":{"schema":"","lastSyncTime":1587440780432,"useSystemStore":"true","jarPath":"mysql-connector-java-5.1.48.jar","catalog":"","lastSyncTaskId":"","DBType":"Mysql","batchsize":10000,"url":"jdbc:mysql://192.168.1.75:3306/merce","database":"merce","password":"AES(cad2fb721d282f6e5151605a1874ffe4)","driver":"com.mysql.jdbc.Driver","port":3306,"host":"192.168.1.75","chineseName":"","name":"mysql-connector-5.1.48","dataStore":{"path":"/tmp/collecter/c1/test_Mysql","schemaResource":"","datasetResourceId":"","format":"csv","clusterId":"","fields":[],"type":"HDFS","separator":",","schemaResourceId":"","sliceTime":"","dataResource":""},"user":"merce","properties":[{"name":"","value":""}],"dateToTimestamp":"false"}, "tenantId": dss_info[0]["tenant_id"],"creator":"admin","createTime":1587439760000,"lastModifier":"admin","lastModifiedTime":1593769577000,"version":4,"enabled":1,"resourceId": dss_info[0]["resource_id"],"expiredPeriod":0}
        return dss_id, new_data
    else:
       return

def dataset_data(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id,tenant_id,owner from merce_resource_dir where creator='admin' and name='Datasets' and parent_id is null"
        dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('resource_id-owner-tenant_id:', dataset_info[0]["id"], dataset_info[0]["owner"], dataset_info[0]["tenant_id"])
        schema_id, schema_resourceid,schema_name = schema_data(data)
    except:
        return
    if 'gjb_test_ftp_dataset' in data:
        new_data = {"id":"","name":"gjb_test_ftp_dataset_随机数","schema":{"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "owner": "SYSTEM", "name": schema_name, "creator": "admin", "createTime": 1587370296000, "lastModifier":"admin","lastModifiedTime":1587370296000, "version": 1, "enabled": 1, "description":"gjb_ttest_mysql0420_training","resourceId": schema_resourceid,"fields":[{"name":"id","type":"int","alias":"","description":""},{"name":"ts","type":"timestamp","alias":"","description":""},{"name":"code","type":"string","alias":"","description":""},{"name":"total","type":"float","alias":"","description":""},{"name":"forward_total","type":"float","alias":"","description":""},{"name":"reverse_total","type":"float","alias":"","description":""},{"name":"sum_flow","type":"float","alias":"","description":""}],"oid": schema_id, "newest":1,"isHide":0,"expiredPeriod":0},"storage":"FTP","expiredPeriod":0,"storageConfigurations":{"user":"europa","password":"AES(11b5a9d816c0a4fd8f99ef1e7de42d32)","format":"csv","path":"ftp://info4/home/europa/gbj_ftp/demo.csv","relativePath":"ftp://info4/home/europa/gbj_ftp/demo.csv","header":"false","ignoreRow":0,"separator":",","quoteChar":"\"","escapeChar":"\\","csv":"csv"},"sliceTime":"","sliceType":"H","schemaVersion":1,"clusterId":"","resource":{"id": dataset_info[0]["id"]},"description":"gjb_ttest_mysql0420_training","schemaId": schema_id}
        deal_random(new_data)
        return new_data
    elif 'gjb_test_hbase' in data:
        new_data = {"id": "", "name": "gjb_test_hbase_随机数", "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "owner": "SYSTEM", "name": schema_name, "creator": "admin", "createTime": 1587370296000, "lastModifier":"admin","lastModifiedTime":1587370296000, "version": 1, "enabled": 1, "description": "tester_mysql0420_training", "resourceId": schema_resourceid, "fields":[{"name":"id","type":"int","alias":"","description":""},{"name":"ts","type":"timestamp","alias":"","description":""},{"name":"code","type":"string","alias":"","description":""},{"name":"total","type":"float","alias":"","description":""},{"name":"forward_total","type":"float","alias":"","description":""},{"name":"reverse_total","type":"float","alias":"","description":""},{"name":"sum_flow","type":"float","alias":"","description":""}],"oid": schema_id, "newest":1,"isHide":0,"expiredPeriod":0},"storage":"HBASE","expiredPeriod":0,"storageConfigurations":{"table":"test_hbase2020","namespace":"default","columns":"rowKey:key,:ts,:code,:total,:forward_total,:reverse_total,:sum_flow","columnsKey":"id","columnsColumns":"","isSingle":"true","columnsItems":0,"undefined":"csv"},"sliceTime":"","sliceType":"H","schemaVersion":1,"clusterId":"","resource": {"id": dataset_info[0]["id"]},"description":"gjb_test-hbase_随机数","schemaId": schema_id}
        deal_random(new_data)
        return new_data
    elif 'gjb_test_es_dataset' in data:
        new_data = {"id":"","name":"gjb_test_es_dataset_随机数","schema":{"id": schema_id, "tenantId": dataset_info[0]["tenant_id"],"owner":"SYSTEM", "name": schema_name, "creator": "admin", "createTime": 1587370889000, "lastModifier":"admin","lastModifiedTime":1587370889000, "version": 1, "enabled": 1, "description":"","resourceId": schema_resourceid, "fields":[{"name":"Name","type":"string","alias":"","description":""},{"name":"Sex","type":"string","alias":"","description":""},{"name":"Age","type":"int","alias":"","description":""},{"name":"Identity_code","type":"string","alias":"","description":""},{"name":"C_time","type":"string","alias":"","description":""},{"name":"Data_long","type":"bigint","alias":"","description":""},{"name":"Data_double","type":"double","alias":"","description":""},{"name":"Data_boolean","type":"boolean","alias":"","description":""},{"name":"time_col","type":"timestamp","alias":"","description":""},{"name":"Str_time","type":"bigint","alias":"","description":""},{"name":"Salary","type":"string","alias":"","description":""},{"name":"Null_data","type":"string","alias":"","description":""},{"name":"City","type":"string","alias":"","description":""},{"name":"data1","type":"string","alias":"","description":""},{"name":"data2","type":"string","alias":"","description":""},{"name":"data3","type":"string","alias":"","description":""},{"name":"data4","type":"string","alias":"","description":""},{"name":"data5","type":"string","alias":"","description":""},{"name":"data6","type":"string","alias":"","description":""},{"name":"data7","type":"string","alias":"","description":""},{"name":"data8","type":"string","alias":"","description":""},{"name":"data9","type":"string","alias":"","description":""}],"oid": schema_id,"newest":1,"isHide":0,"expiredPeriod":0},"storage":"ElasticSearch","expiredPeriod":0,"storageConfigurations":{"clusterName":"elasticsearch","ipAddresses":"info5:9203","index":"test_stre","indexType":"test_stre"},"sliceTime":"","sliceType":"H","schemaVersion":1, "clusterId":"", "resource": {"id": dataset_info[0]["id"]}, "schemaId": schema_id}
        deal_random(new_data)
        return new_data
    elif 'gjb_test_SearchOne_dataset' in data:
        new_data = {"id":"","name":"gjb_test_SearchOne_dataset_随机数","schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "owner":"SYSTEM","name": schema_name, "creator": "admin", "createTime": 1587370296000, "lastModifier":"admin","lastModifiedTime":1587370296000, "version": 1, "enabled":1,"description":"tester_mysql0420_training", "resourceId": schema_resourceid, "fields": [{"name":"id","type":"int","alias":"","description":""},{"name":"ts","type":"timestamp","alias":"","description":""},{"name":"code","type":"string","alias":"","description":""},{"name":"total","type":"float","alias":"","description":""},{"name":"forward_total","type":"float","alias":"","description":""},{"name":"reverse_total","type":"float","alias":"","description":""},{"name":"sum_flow","type":"float","alias":"","description":""}],"oid": schema_id, "newest":1,"isHide":0,"expiredPeriod":0},"storage":"SearchOne","expiredPeriod":0,"storageConfigurations":{"clusterName":"my-cluster","ipAddresses":"192.168.1.81:9200,192.168.1.82:9200,192.168.1.84:9200","index":"test_new_0103","indexType":"test_new_0103"},"sliceTime":"","sliceType":"H","owner": dataset_info[0]["owner"], "resource": {"id": dataset_info[0]["id"]}}
        deal_random(new_data)
        return new_data
    elif 'gjb_test_HDFS_dataset' in data:
        new_data = {"id":"","name":"gjb_test_HDFS_dataset_随机数", "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"],"owner": "SYSTEM", "name": schema_name, "creator":"admin","createTime": 1587370296000, "lastModifier":"admin","lastModifiedTime":1587370296000, "version": 1, "enabled": 1, "description":"tester_mysql0420_training", "resourceId": schema_resourceid, "fields": [{"name":"id","type":"int","alias":"","description":""},{"name":"ts","type":"timestamp","alias":"","description":""},{"name":"code","type":"string","alias":"","description":""},{"name":"total","type":"float","alias":"","description":""},{"name":"forward_total","type":"float","alias":"","description":""},{"name":"reverse_total","type":"float","alias":"","description":""},{"name":"sum_flow","type":"float","alias":"","description":""}],"oid": schema_id, "newest":1,"isHide":0,"expiredPeriod":0},"storage":"HDFS","expiredPeriod":0,"storageConfigurations":{"format":"csv","path":"/tmp/gbj/datas_for_test/students.txt","relativePath":"/tmp/gbj/datas_for_test/students.txt","pathMode":"exact","header":"false","separator":",","quoteChar":"\"","escapeChar":"\\"},"sliceTime":"","sliceType":"H","owner": dataset_info[0]["owner"], "resource": {"id": dataset_info[0]["id"]}}
        deal_random(new_data)
        return new_data
    elif 'gjb_test_kafka_dataset' in data:
        new_data = {"id": "","name": "gjb_test_kafka_dataset_随机数", "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "owner": "SYSTEM", "name": schema_name, "creator":"admin","createTime": 1587370296000, "lastModifier":"admin","lastModifiedTime":1587370296000, "version": 1, "enabled": 1,"description":"tester_mysql0420_training", "resourceId": schema_resourceid, "fields": [{"name":"id","type":"int","alias":"","description":""},{"name":"ts","type":"timestamp","alias":"","description":""},{"name":"code","type":"string","alias":"","description":""},{"name":"total","type":"float","alias":"","description":""},{"name":"forward_total","type":"float","alias":"","description":""},{"name":"reverse_total","type":"float","alias":"","description":""},{"name":"sum_flow","type":"float","alias":"","description":""}], "oid": schema_id, "newest":1,"isHide":0,"expiredPeriod":0},"storage":"KAFKA","expiredPeriod":0,"storageConfigurations":{"format":"csv","zookeeper":"info1:2181,info2:2181,info3:2181/europa/app/kafka","brokers":"info3:9093","topic":"kafka_new610","groupId":"kafka_new610","version":"0.10","reader":"","separator":",","header":"false","quoteChar":"\"","escapeChar":"\\"},"sliceTime":"","sliceType":"H","owner": dataset_info[0]["owner"], "resource": {"id": dataset_info[0]["id"]}}
        deal_random(new_data)
        return new_data
    elif 'gjb_test_hive_dataset' in data:
        new_data = {"id":"","name": "gjb_test_hive_dataset_随机数", "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"],"owner": "SYSTEM", "name": schema_name, "creator":"admin","createTime": 1587370296000, "lastModifier": "admin","lastModifiedTime":1587370296000, "version": 1, "enabled": 1, "description":"tester_mysql0420_training", "resourceId": schema_resourceid, "fields": [{"name":"id","type":"int","alias":"","description":""},{"name":"ts","type":"timestamp","alias":"","description":""},{"name":"code","type":"string","alias":"","description":""},{"name":"total","type":"float","alias":"","description":""},{"name":"forward_total","type":"float","alias":"","description":""},{"name":"reverse_total","type":"float","alias":"","description":""},{"name":"sum_flow","type":"float","alias":"","description":""}],"oid": schema_id, "newest":1,"isHide":0,"expiredPeriod":0},"storage":"HIVE","expiredPeriod":0,"storageConfigurations":{"sql":"","table":"students_info_hive_sink_0617","partitionColumns":""},"sliceTime":"","sliceType":"H","owner": dataset_info[0]["owner"], "resource": {"id": dataset_info[0]["id"]}}
        deal_random(new_data)
        return new_data
    elif 'gjb_ttest_hdfs' == data.split("&")[0]:
        dataset_id, dataset_name = get_dataset_data(data)
        new_data = {"id": dataset_id, "name": dataset_name, "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "owner": "SYSTEM", "name": schema_name, "creator": "admin", "createTime": 1587370296000, "lastModifier": "admin", "lastModifiedTime": 1587985137000, "version": 1, "enabled": 1, "description":"tester_mysql0420_training", "resourceId": schema_resourceid, "fields":[{"name":"id","type":"int","alias":"","description":""},{"name":"ts","type":"timestamp","alias":"","description":""},{"name":"code","type":"string","alias":"","description":""},{"name":"total","type":"float","alias":"","description":""},{"name":"forward_total","type":"float","alias":"","description":""},{"name":"reverse_total","type":"float","alias":"","description":""},{"name":"sum_flow","type":"float","alias":"","description":""}],"oid": schema_id, "newest":0,"isHide":0,"expiredPeriod":0},"storage":"HDFS","expiredPeriod":0,"storageConfigurations":{"quoteChar":"\"","escapeChar":"\\","encryptColumns":"","csv":"csv","format":"csv","clusterId":"cluster1","encryptKey":"","separator":",","path":"/auto_test/","relativePath":"/home/auto_test/gjb/out8","pathMode":"exact","header":"false","ignoreRow":"0"},"sliceTime":"","sliceType":"H","owner":"SYSTEM","schemaVersion":1,"tenantId": dataset_info[0]["tenant_id"], "creator":"admin","createTime":1587551105000,"lastModifier":"admin","lastModifiedTime":1589939697000,"version":3,"enabled":1,"resourceId": dataset_info[0]["id"],"schemaId": schema_id, "recordNumber":0,"byteSize":0,"analysisTime":0,"isRelated":1,"isHide":0}
        deal_random(new_data)
        return dataset_id, new_data
    else:
        return

def upddataset_data(data):
    try:
        data = data.split("&")
        sql = "select id,owner,name,tenant_id,resource_id from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data[0]
        dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        dataset_id = dataset_info[0]["id"]
        print('dataset_id-owner-name:', dataset_info[0]["id"], dataset_info[0]["owner"], dataset_info[0]["name"])
        schema_id, schema_resourceid,schema_name = schema_data(data)
    except:
        return
    if 'gjb_ttest_hdfs' in data:
        print("dataset_id-name:",dataset_info, dataset_info[0]["name"])
        new_data = {"id": dataset_id,"name":dataset_info[0]["name"],"schema":{"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "owner":"SYSTEM", "name": schema_name,"creator":"admin","createTime":1587370296000,"lastModifier":"admin","lastModifiedTime":1587985137000,"version":1,"enabled":1,"description":"tester_mysql0420_training","resourceId": schema_resourceid,"fields":[{"name":"id","type":"int","alias":"","description":""},{"name":"ts","type":"timestamp","alias":"","description":""},{"name":"code","type":"string","alias":"","description":""},{"name":"total","type":"float","alias":"","description":""},{"name":"forward_total","type":"float","alias":"","description":""},{"name":"reverse_total","type":"float","alias":"","description":""},{"name":"sum_flow","type":"float","alias":"","description":""}],"oid":"4e4b83b8-4a11-4bc9-ab0f-0de993e4851a","newest":0,"isHide":0,"expiredPeriod":0},"storage":"HDFS","expiredPeriod":0,"storageConfigurations":{"quoteChar":"\"","escapeChar":"\\","encryptColumns":"","csv":"csv","format":"csv","clusterId":"cluster1","encryptKey":"","separator":",","path":"/auto_test/","relativePath":"/home/auto_test/gjb/out8","pathMode":"exact","header":"false","ignoreRow":"0"},"sliceTime":"","sliceType":"H","owner":"SYSTEM","schemaVersion":1,"tenantId":dataset_info[0]["tenant_id"],"creator":"admin","createTime":1587551105000,"lastModifier":"admin","lastModifiedTime":1589939697000,"version":3,"enabled":1,"resourceId":dataset_info[0]["resource_id"], "schemaId": schema_id,"recordNumber":0,"byteSize":0,"analysisTime":0,"isRelated":1,"isHide":0}
        return dataset_id, new_data
    else:
        return

def get_dataset_data(data):
    try:
        if '&' in data:
            data = data.split("&")
            sql = "select id,name from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data[0]
            dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
            print(sql)
            dataset_id = dataset_info[0]["id"]
            dataset_name = dataset_info[0]["name"]
            print('dataset_id-owner-name:', dataset_info[0]["id"], dataset_info[0]["name"])
            return dataset_id, dataset_name
        else:
            sql = "select id,name from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data[0]
            dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
            print(sql)
            dataset_id = dataset_info[0]["id"]
            dataset_name = dataset_info[0]["name"]
            print('dataset_id-owner-name:', dataset_info[0]["id"], dataset_info[0]["name"])
            return dataset_id, dataset_name
    except:
        return

def schema_data(data):
    try:
        if '&' in data:
            data = data.split('&')
            sql = "select id,name,resource_id from merce_schema where name like '%s%%%%' ORDER BY create_time limit 1" % data[1]
            schema_info = ms.ExecuQuery(sql.encode('utf-8'))
            print(sql)
            print('schema_id-resource_id-name:', schema_info[0]["id"], schema_info[0]["resource_id"], schema_info[0]["name"])
            return schema_info[0]["id"], schema_info[0]["resource_id"], schema_info[0]["name"]
        else:
            sql = "select id,name,resource_id from merce_schema where name like '%s%%%%' ORDER BY create_time limit 1" % data[1]
            schema_info = ms.ExecuQuery(sql.encode('utf-8'))
            print(sql)
            print('schema_id-resource_id-name:', schema_info[0]["id"], schema_info[0]["resource_id"], schema_info[0]["name"])
            return schema_info[0]["id"], schema_info[0]["resource_id"], schema_info[0]["name"]
    except:
        return

def create_schema_data(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id,tenant_id from merce_resource_dir where creator='admin' and name='Schemas' and parent_id is null"
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('resource_id-tenant_id:', resource_info[0]["id"], resource_info[0]["tenant_id"])
    except:
        return
    if 'gtest_mysql_0428_training' in data:
        new_data = {"id": "", "name": "gtest_mysql_0428_training_随机数", "alias": "", "description":"gtest_mysql_0428_training_随机数","fields": [{"name":"id","type":"int","alias":"","description":""},{"name":"ts","type":"timestamp","alias":"","description":""},{"name":"code","type":"string","alias":"","description":""},{"name":"total","type":"float","alias":"","description":""},{"name":"forward_total","type":"float","alias":"","description":""},{"name":"reverse_total","type":"float","alias":"","description":""},{"name":"sum_flow","type":"float","alias":"","description":""}],"owner":"","tenantId":resource_info[0]["tenant_id"],"creator":"admin","lastModifier":"admin","version":1,"enabled":1,"resourceId": resource_info[0]["id"],"oid":"","newest":1,"isHide":0,"expiredPeriod":0,"resource":{"id": resource_info[0]["id"]}}
        deal_random(new_data)
        return new_data
    else:
        return

def updschema_data(data):
    try:
        sql = "select id,owner,tenant_id,resource_id from merce_schema where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        schema_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        schema_id = schema_info[0]["id"]
        print('schema_id-owner-tenant_id:', schema_info[0]["id"], schema_info[0]["owner"], schema_info[0]["tenant_id"])
    except:
        return
    if 'gtest_mysql_0428_training' in data:
        new_data = {"id": schema_info[0]["id"], "name":"gtest_mysql_0428_training_随机数","alias":"","description":"gtest_mysql_0428_training","fields":[{"name":"id","type":"int","alias":"","description":""},{"name":"ts","type":"timestamp","alias":"","description":""},{"name":"code","type":"string","alias":"","description":""},{"name":"total","type":"float","alias":"","description":""},{"name":"forward_total","type":"float","alias":"","description":""},{"name":"reverse_total","type":"float","alias":"","description":""},{"name":"sum_flow","type":"float","alias":"","description":""}],"owner": schema_info[0]["owner"], "tenantId": schema_info[0]["tenant_id"],"creator":"admin","createTime":1587370296000,"lastModifier":"admin","lastModifiedTime":1603140944000, "version": 1, "enabled": 1, "resourceId": schema_info[0]["resource_id"], "oid": schema_info[0]["id"],"newest":1,"isHide":0,"expiredPeriod":0}
        from new_api_cases.dw_deal_parameters import deal_random
        deal_random(new_data)
        return schema_id, new_data
    else:
        return

def create_flow_data(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id,owner,tenant_id from merce_resource_dir where creator='admin' and name='Flows' and parent_id is null"
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('resource_id-owner-tenant_id:', resource_info[0]["id"], resource_info[0]["owner"], resource_info[0]["tenant_id"])
        schema_id, schema_resourceid,schema_name = schema_data(data)
        dataset_id, dataset_name = flow_dataset_data(data)
    except:
        return
    if 'gjb_api_create_flow_dataflow' in data:
        new_data = {"name": "gjb_api_create_flow_dataflow_随机数", "flowType": "dataflow", "resource": {"id": resource_info[0]["id"]}, "steps": [{"id":"source_1","name":"source_1","type":"source","otherConfigurations":{"schema": "schema_name","dataset-paths":"","schemaId": schema_id, "sessionCache":"","interceptor":"","dataset":[{"rule":"set_1","dataset":dataset_name,"ignoreMissingPath":"false","datasetId":dataset_id,"storage":"HDFS"}]},"outputConfigurations":{"output":[{"name":"Name","alias":""},{"name":"name01","alias":""}]},"x":355,"y":113,"uiConfigurations":{"output":["output"]}},{"id":"sink_1","name":"sink_1","type":"sink","otherConfigurations":{"schema":"test_pivot","description":"","outputMode":"","type":"HDFS","autoSchema":"true","nullValue":"","mode":"append","path":"/auto_test/gjb/pivot/","isDisable":"false","countWrittenRecord":"false","datasetId":"","dataResource":"","schedulerUnit":"","quoteChar":"\"","escapeChar":"\\","schemaResource":"","schemaVersion":"1","expiredTemp":"","sliceTimeColumn":"","format":"csv","trigger":"","maxFileSize":"","maxFileNumber":"","separator":",","expiredTime":"","schedulerVal":"","checkpointLocation":"","schemaId":schema_id,"time":"s","dataset":"test_pivot","sliceType":"H","idColumn":""},"inputConfigurations":{"input":[{"name":"Name","alias":""},{"name":"name01","alias":""}]},"outputConfigurations":{},"x":916,"y":135,"uiConfigurations":{"input":["input"]}}],"links":[{"name":"","source":"source_1","sourceOutput":"output","target":"sink_1","targetInput":"input","input":"input"}],"oid":"$null","creator":"admin","createTime":1603187474000,"lastModifier":"admin","lastModifiedTime":1603189710000,"owner":resource_info[0]["owner"],"version":1,"enabled":1,"tenantId":resource_info[0]["tenant_id"],"resourceId": resource_info[0]["id"],"isHide":0,"parameters":[],"expiredPeriod":0}
        deal_random(new_data)
        return new_data
    elif 'gjb_api_create_flow_workflow' in data:
        new_data = {"name": "gjb_api_create_flow_workflow_随机数", "flowType": "workflow", "resource": {"id": resource_info[0]["id"]}, "steps": [], "links": []}
        deal_random(new_data)
        return new_data
    elif 'gjb_api_create_flow_streamflow' in data:
        new_data = {"name": "gjb_api_create_flow_streamflow_随机数", "flowType": "streamflow", "resource": {"id": resource_info[0]["id"]}, "steps": [], "links": []}
        deal_random(new_data)
        return new_data
    else:
        return

def flow_data(data):
    try:
        if '&' in data:
            data = data.split('&')
            sql = "select id,name from merce_flow where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
            flow_info = ms.ExecuQuery(sql.encode('utf-8'))
            print(sql)
            print('flow_id-name:', flow_info[0]["id"], flow_info[0]["name"])
            return flow_info[0]["id"], flow_info[0]["name"]
        else:
            sql = "select id,name from merce_flow where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
            flow_info = ms.ExecuQuery(sql.encode('utf-8'))
            print(sql)
            print('flow_id-name:', flow_info[0]["id"], flow_info[0]["name"])
            return flow_info[0]["id"], flow_info[0]["name"]
    except:
        return

def update_flow_data(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id,owner,tenant_id from merce_resource_dir where creator='admin' and name='Flows' and parent_id is null"
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('resource_id-owner-tenant_id:', resource_info[0]["id"], resource_info[0]["owner"], resource_info[0]["tenant_id"])
        schema_id, schema_resourceid,schema_name = schema_data(data)
        flow_id, flow_name = flow_data(data)
        dataset_id, dataset_name = flow_dataset_data(data)
    except:
        return
    if 'gjb_api_create_flow_dataflow' in data:
        new_data = {"steps":[{"id":"source_1","name":"source_1","type":"source","otherConfigurations":{"schema":"pivot","dataset-paths":"","schemaId": schema_id,"sessionCache":"","interceptor":"","dataset":[{"rule":"set_1","dataset":dataset_name,"ignoreMissingPath":"false","datasetId":dataset_id,"storage":"HDFS"}]},"outputConfigurations":{"output":[{"name":"Name","alias":""},{"name":"name01","alias":""}]},"x":355,"y":113,"uiConfigurations":{"output":["output"]}},{"id":"sink_1","name":"sink_1","type":"sink","otherConfigurations":{"schema":"test_pivot","description":"","outputMode":"","type":"HDFS","autoSchema":"true","nullValue":"","mode":"append","path":"/auto_test/gjb/pivot/","isDisable":"false","countWrittenRecord":"false","datasetId":"","dataResource":"","schedulerUnit":"","quoteChar":"\"","escapeChar":"\\","schemaResource":"","schemaVersion":"1","expiredTemp":"","sliceTimeColumn":"","format":"csv","trigger":"","maxFileSize":"","maxFileNumber":"","separator":",","expiredTime":"","schedulerVal":"","checkpointLocation":"","schemaId":schema_id,"time":"s","dataset":"test_pivot","sliceType":"H","idColumn":""},"inputConfigurations":{"input":[{"name":"Name","alias":""},{"name":"name01","alias":""}]},"outputConfigurations":{},"x":916,"y":135,"uiConfigurations":{"input":["input"]}}],"links":[{"target":"sink_1","source":"source_1","sourceOutput":"output","targetInput":"input","linkStrategy":""}], "id": flow_id, "name": flow_name,"flowType":"dataflow","oid":"$null","creator":"admin","createTime":1603187474000,"lastModifier":"admin","lastModifiedTime":1603189710000,"owner":resource_info[0]["owner"],"version":1,"enabled":1,"tenantId":resource_info[0]["tenant_id"],"resourceId":resource_info[0]["id"],"isHide":0,"parameters":[],"expiredPeriod":0}
        deal_random(new_data)
        return flow_id, new_data
    else:
        return

def flow_dataset_data(data):
    try:
        if '&' in data:
            data = data.split("&")
            sql = "select id,name from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data[2]
            dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
            print(sql)
            dataset_id = dataset_info[0]["id"]
            dataset_name = dataset_info[0]["name"]
            print('dataset_id-owner-name:', dataset_info[0]["id"], dataset_info[0]["name"])
            return dataset_id, dataset_name
        else:
            sql = "select id,name from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data[2]
            dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
            print(sql)
            dataset_id = dataset_info[0]["id"]
            dataset_name = dataset_info[0]["name"]
            print('dataset_id-owner-name:', dataset_info[0]["id"], dataset_info[0]["name"])
            return dataset_id, dataset_name
    except:
        return