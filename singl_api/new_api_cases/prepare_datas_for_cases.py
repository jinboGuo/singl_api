# coding:utf-8
import json
import os
import time
import requests
from basic_info.get_auth_token import get_headers
from new_api_cases.compass_deal_parameters import deal_random
from util.format_res import dict_res
from basic_info.setting import ms, log
from basic_info.setting import host
from selenium import webdriver
from util.timestamp_13 import data_now

woven_dataflow = os.path.join(os.path.abspath('.'),'attachment\import_dataflow_steps.woven')
multi_sink_steps = os.path.join(os.path.abspath('.'),'attachment\mutil_sink_storage.woven')
multi_rtc_steps = os.path.join(os.path.abspath('.'),'attachment\multi_rtc_steps.woven')
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))


def get_job_tasks_id(job_id):
    url = '%s/api/woven/collectors/%s/tasks' % (host, job_id)
    data = {"fieldList": [], "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
    response = requests.post(url=url, headers=get_headers(), json=data)

    all_task_id = []
    try:
        tasks = dict_res(response.text)['content']
        for item in tasks:
            task_id = item['id']
            all_task_id.append(task_id)
    except Exception as e:
        return
    else:
        return all_task_id


def create_new_user(data):
    url = '%s/api/woven/users' % host
    response = requests.post(url=url, headers=get_headers(host), json=data)
    user_id = dict_res(response.text)["id"]
    return user_id

def collector_schema_sync(data):
    """获取采集器元数据同步后返回的task id"""
    collector = 'c9'
    url = '%s/api/woven/collectors/%s/schema/fetch' % (host, collector)
    response = requests.post(url=url, headers=get_headers(host), data=data)
    time.sleep(3)
    return response.text



def admin_flow_id(data):
    try:
        url = '%s/api/dsp/platform/service/infoById?id=%s' % (host, data)
        response = requests.get(url=url, headers=get_headers())
        flow_id = dict_res(response.text)["content"]["jobInfo"]['flowId']
        return flow_id
    except:
        return 1

def customer_flow_id(data):
    try:
        url = '%s/api/dsp/consumer/service/infoById?id=%s' % (host, data)
        response = requests.get(url=url, headers=get_headers())
        flow_id = dict_res(response.text)["content"]["jobInfo"]['flowId']
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
    return path

def dss_data(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id from merce_resource_dir where creator='admin' and name='Datasources' and parent_id is NULL"
        Datasources = ms.ExecuQuery(sql.encode('utf-8'))
        if 'gjb_type_JDBC_datasource_test' in data:
            new_data = {"name":"gjb_type_JDBC_datasource_test_随机数","type":"DB","description":"","resourceId":Datasources[0]["id"],"attributes":{"DBType":"Mysql","directConnection":True,"collectorId":"","collectorName":"","urls":"jdbc:mysql://192.168.1.82:3306/auto_apitest","database":"auto_apitest","tags":[],"tagNames":[],"user":"merce","password":"merce","inOrganization":"test","inSystem":"test","inReporter":"test","inPhoneNumber":"test","businessCompany":"test","name":"mysql","driver":"com.mysql.cj.jdbc.Driver","url":"jdbc:mysql://192.168.1.82:3306/auto_apitest","defaultUrl":"jdbc:mysql://[HOST]:[PORT]/[DB]","port":3306,"paraPrefix":"?","paraSep":"&","jarPath":"mysql-connector-java-8.0.28.jar"}}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_http_datasource_test' in data:
            new_data = {"name":"gjb_type_http_datasource_test_随机数","type":"HTTP","description":"","resourceId":Datasources[0]["id"],"attributes":{"urls":"http://1.2.3.4/api","inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":"","DBType":"Http"}}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_ftp_datasource_test' in data:
            new_data = {"name":"gjb_type_ftp_datasource_test_随机数","type":"FTP","description":"","resourceId":Datasources[0]["id"],"attributes":{"urls":"192.168.1.84:21","username":"europa","password":"europa","inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":"","DBType":"FTP"}}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_sftp_datasource_test' in data:
            new_data = {"name":"gjb_type_sftp_datasource_test_随机数","type":"SFTP","description":"","resourceId":Datasources[0]["id"],"attributes":{"urls":"192.168.1.82:22","username":"merce","password":"merce@82","inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":"","DBType":"SFTP"}}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_websocket_datasource_test' in data:
            new_data = {"name":"gjb_type_websocket_datasource_test_随机数","type":"WEBSOCKET","description":"","resourceId":Datasources[0]["id"],"attributes":{"urls":"http://218.84.39.208:12056","authType":"token","charset":"","tokenUri":"http://218.84.39.208:12056/api/v1/basic/key?username=slmh&password=slmh123!","tokenMethod":"GET","tokenAlias":"data->key","tokenPeriod":30,"contentType":"","tokenHeaders":[],"tokenParamBody":"","tokenJsonBody":"","inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":"","DBType":"WEBSOCKET"},"attributes.tokenHeaders":[]}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_mongodb_datasource_test' in data:
            new_data = {"name":"gjb_type_mongodb_datasource_test_随机数","type":"DB","description":"","resourceId":Datasources[0]["id"],"attributes":{"DBType":"MongoDB","directConnection":True,"collectorId":"","collectorName":"","urls":"192.168.1.67:27017","database":"admin","tags":[],"tagNames":[],"user":"root","password":"123456","inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":""}}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_rest_datasource_test' in data:
            new_data = {"name":"gjb_type_rest_datasource_test_随机数","type":"REST","description":"","resourceId":Datasources[0]["id"],"attributes":{"urls":"http://192.168.1.220:8515","authType":"token","tokenUri":"http://192.168.1.220:8515/api/auth/oauth/token","tokenMethod":"POST","tokenAlias":"content->access_token","tokenPeriod":30,"contentType":"x-www-form-urlencoded","tokenHeaders":[{"key":"Content-Type","value":"Content-Type: application/x-www-form-urlencoded","desc":""},{"key":"Host","value":"192.168.1.220:8515","desc":""},{"key":"Authorization","value":"Authorization","desc":""}],"tokenParamBody":[{"key":"vsersion","value":"Baymax-3.0.0.23-20180606","desc":"version"},{"key":"tenant","value":"default","desc":"tenant"},{"key":"grant_type","value":"manager_password","desc":""},{"key":"username","value":"a$a67fba7b9f50eca1677f50c0d7eb0993~","desc":""},{"key":"password","value":"a$0615f89cbee023498ebc2e31cc2c8fca~","desc":""}],"tokenJsonBody":"","inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":"","DBType":"REST"},"attributes.tokenHeaders":[{"key":"Content-Type","value":"Content-Type: application/x-www-form-urlencoded","desc":""},{"key":"Host","value":"192.168.1.220:8515","desc":""},{"key":"Authorization","value":"Authorization","desc":""}],"attributes.tokenParamBody":[{"key":"vsersion","value":"Baymax-3.0.0.23-20180606","desc":"version"},{"key":"tenant","value":"default","desc":"tenant"},{"key":"grant_type","value":"manager_password","desc":""},{"key":"username","value":"a$a67fba7b9f50eca1677f50c0d7eb0993~","desc":""},{"key":"password","value":"a$0615f89cbee023498ebc2e31cc2c8fca~","desc":""}]}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_HDFS_datasource_test' in data:
            new_data = {"name":"gjb_type_HDFS_datasource_test_随机数","type":"HDFS","description":"","resourceId":Datasources[0]["id"],"attributes":{"urls":"hdfs://into1:8020","isKerberosSupport":True,"krb5ConfigFile":"/etc/krb5.conf","keytabPrincipal":"merce@HADOOP.COM","keytabPath":"/app/merce/kerberos/merce.keytab","inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":"","DBType":"HDFS"}}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_kafka_datasource_test_SASL' in data:
            kafka_data =data.split("&")
            new_data = {"name":"gjb_type_kafka_datasource_test"+kafka_data[2]+"随机数","type":"KAFKA","description":"","resourceId":Datasources[0]["id"],"attributes":{"urls":"192.168.1.65:2282","isKerberosSupport":False,"authLoginConfigFile":"","krb5ConfigFile":"","kafkaVersion":"1.0+","protocol":"password","security.protocol":kafka_data[1],"sasl.mechanism":kafka_data[2],"username":"admin","password":"admin","inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":"","DBType":"KAFKA"}}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_kafka_datasource_test_kerberos' in data:
            kafka_data =data.split("&")
            new_data = {"name":"gjb_type_kafka_datasource_test_kerberos_随机数","type":"KAFKA","description":"","resourceId":Datasources[0]["id"],"attributes":{"urls":"192.168.1.65:2282","isKerberosSupport":True,"authLoginConfigFile":"/app/merce/kerberos/java_client_jaas.conf","krb5ConfigFile":"/etc/krb5","kafkaVersion":"1.0+","protocol":"kerberos","security.protocol":kafka_data[1],"sasl.mechanism":kafka_data[2],"username":"","password":"","inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":"","DBType":"KAFKA"}}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_kafka_datasource_test_no' in data:
            new_data = {"name":"gjb_type_kafka_datasource_test_no_随机数","type":"KAFKA","description":"","resourceId":Datasources[0]["id"],"attributes":{"urls":"192.168.1.65:2282","isKerberosSupport":False,"authLoginConfigFile":"","krb5ConfigFile":"","kafkaVersion":"1.0+","protocol":"no","security.protocol":"PLAINTEXT","sasl.mechanism":"SCRAM-SHA-512","username":"","password":"","inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":"","DBType":"KAFKA"}}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_hbase_datasource_test_no' in data:
            new_data = {"name":"gjb_type_hbase_datasource_test_no_随机数","type":"HBASE","description":"","resourceId":Datasources[0]["id"],"attributes":{"urls":"192.168.1.61:2181,192.168.1.62:2181","hbaseVersion":"1.0.0","zkNodeParent":"/hbase-unsecure","securityMode":"no","krb5confPath":"","keytabPath":"","principal":"","securityExtParams":[],"cluster":"","tagNames":[],"tags":[],"inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":"","DBType":"Hbase"},"attributes.securityExtParams":[]}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_hbase_datasource_test_kerberos' in data:
            new_data = {"name":"gjb_type_hbase_datasource_test_kerberos_随机数","type":"HBASE","description":"","resourceId":Datasources[0]["id"],"attributes":{"urls":"192.168.1.61:2181,192.168.1.62:2181","hbaseVersion":"1.0.0","zkNodeParent":"/hbase-unsecure","securityMode":"kerberos","krb5confPath":"/etc/krb5.conf","keytabPath":"/app/merce/kerberos/merce.keytab","principal":"merce@HADOOP.COM","securityExtParams":[],"cluster":"","tagNames":[],"tags":[],"inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":"","DBType":"Hbase"},"attributes.securityExtParams":[]}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_hive_datasource_test_no' in data:
            new_data = {"name":"gjb_type_hive_datasource_test_no_随机数","type":"DB","description":"","resourceId":Datasources[0]["id"],"attributes":{"DBType":"HIVE","directConnection":True,"collectorId":"","collectorName":"","urls":"jdbc:hive2://192.168.1.62:10000/default;principal=hive/into2@HADOOP.COM?hive.resultset.use.unique.column.names=false","database":"default","tags":[],"tagNames":[],"user":"merce","password":"merce","inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":"","name":"hive","driver":"org.apache.hive.jdbc.HiveDriver","url":"jdbc:hive2://192.168.1.62:10000/default;principal=hive/into2@HADOOP.COM?hive.resultset.use.unique.column.names=false","defaultUrl":"jdbc:hive2://[HOST]:[PORT]/[DB]","port":10000,"paraPrefix":";","paraSep":";","jarPath":"hive-jdbc-3.1.0.3.1.5.0-152_driver.jar","securityMode":"no","krb5confPath":"","keytabPath":"","principal":""}}
            deal_random(new_data)
            return new_data
        elif 'gjb_type_hive_datasource_test_kerberos' in data:
            new_data = {"name":"gjb_type_hive_datasource_test_kerberos_随机数","type":"DB","description":"","resourceId":Datasources[0]["id"],"attributes":{"DBType":"HIVE","directConnection":True,"collectorId":"","collectorName":"","urls":"jdbc:hive2://192.168.1.62:10000/default;principal=hive/into2@HADOOP.COM?hive.resultset.use.unique.column.names=false","database":"default","tags":[],"tagNames":[],"user":"merce","password":"merce","inOrganization":"","inSystem":"","inReporter":"","inPhoneNumber":"","businessCompany":"","name":"hive","driver":"org.apache.hive.jdbc.HiveDriver","url":"jdbc:hive2://192.168.1.62:10000/default;principal=hive/into2@HADOOP.COM?hive.resultset.use.unique.column.names=false","defaultUrl":"jdbc:hive2://[HOST]:[PORT]/[DB]","port":10000,"paraPrefix":";","paraSep":";","jarPath":"hive-jdbc-3.1.0.3.1.5.0-152_driver.jar","securityMode":"kerberos","keytabPath":"/app/merce/kerberos/merce.keytab","krb5confPath":"/etc/krb5.conf","principal":"merce@HADOOP.COM"}}
            deal_random(new_data)
            return new_data
        elif "gjb_type_localfs_datasource_test" in data:
            new_data = {"name":"gjb_type_localfs_datasource_test_随机数","type":"LOCALFS","description":"localfs","resourceId":Datasources[0]["id"],"attributes":{"urls":"/app/data","encoder":"UTF-8","inOrganization":"localfs","inSystem":"localfs","inReporter":"localfs","inPhoneNumber":"xxxxx","businessCompany":"localfs","DBType":"LOCALFS"}}
            deal_random(new_data)
            return new_data
        elif 'datasource_query' == data:
            new_data = {"fieldList": [
                {"fieldName": "parentId", "fieldValue": Datasources[0]["id"], "comparatorOperator": "EQUAL",
                 "logicalOperator": "AND"}], "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"},
                        "offset": 0, "limit": 8}
            return new_data
        elif 'lastModifiedTime' == data:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"GREATER_THAN","fieldValue":1635696000000},{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"LESS_THAN","fieldValue":1704038399000},{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":Datasources[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif 'name' == data:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"name","comparatorOperator":"LIKE","fieldValue":"%gjb%"},{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":Datasources[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif 'name_lastModifiedTime' == data:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"name","comparatorOperator":"LIKE","fieldValue":"%gjb%"},{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"GREATER_THAN","fieldValue":1635696000000},{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"LESS_THAN","fieldValue":1704038399000},{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":Datasources[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        else:
            return
    except Exception as e:
        log.error("dss_data查询出错{}".format(e))



def upddss_data(data):
    try:
        sql = "select id,name,resource_id,tenant_id,owner from merce_dss where name like '%s%%%%' ORDER BY create_time limit 1" % data
        dss_info = ms.ExecuQuery(sql.encode('utf-8'))
        dss_id = dss_info[0]["id"]
        if 'gjb_type_JDBC_datasource_test' in data:
            new_data = {"tenantId":dss_info[0]["tenant_id"],"owner":dss_info[0]["owner"],"name":dss_info[0]["name"],"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"id":dss_info[0]["id"],"version":2,"groupCount":None,"groupFieldValue":None,"tableName":"merce_dss","type":"DB","path":None,"attributes":{"jarPath":"mysql-connector-java-8.0.28.jar","collectorId":"","businessCompany":"test","DBType":"Mysql","inReporter":"test","inOrganization":"test","paraSep":"&","tagNames":[],"url":"jdbc:mysql://192.168.1.82:3306/auto_apitest","tags":[],"urls":"jdbc:mysql://192.168.1.82:3306/auto_apitest","database":"auto_apitest","password":"a$eabcabc6a2d09f494202d24670c15e15~","inPhoneNumber":"test","driver":"com.mysql.cj.jdbc.Driver","port":3306,"name":"mysql","paraPrefix":"?","collectorName":"","user":"merce","defaultUrl":"jdbc:mysql://[HOST]:[PORT]/[DB]","inSystem":"test","directConnection":True},"resourceId":dss_info[0]["resource_id"],"resource":None,"tags":None,"description":"","tagObjs":None,"expiredPeriod":0}
            #deal_random(new_data)
            print("---------------",new_data,dss_id)
            return dss_id, new_data
        else:
            return
    except Exception as e:
        log.error("upddss_data查询出错{}".format(e))
        return

tenant_id,owner=None,None

def dataset_data(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        list_data = data.split("&")
        schema_id, schema_resourceid,schema_name,schema_info,schema_hisid = schema_data(list_data[1])
        sql = "select id,tenant_id,owner from merce_resource_dir where creator='admin' and name='Datasets' and parent_id is NULL"
        dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
        resource_id,tenant_id,owner=dataset_info[0]["id"], dataset_info[0]["owner"], dataset_info[0]["tenant_id"]
        if 'gjb_test_ftp_dataset' in data:
            new_data = {"id":"","name":"gjb_test_ftp_dataset_随机数","alias":"","description":"","resource":{"id":resource_id},"resourceId":resource_id,"schema":{"id":schema_id,"hisId":schema_hisid,"name":schema_name,"type":"input","description":None,"version":1,"hisVersion":1,"resourceId":schema_resourceid,"resource":None,"datasourceIds":"","datasourceNames":None,"fields":[{"name":"id","type":"int","types":None,"alias":"","description":"","fieldCategory":None,"specId":None,"id":"1141789505080606720","formula":"","isCalc":0},{"name":"ts","type":"timestamp","types":None,"alias":"","description":"","fieldCategory":None,"specId":None,"id":"1141789505080606721","formula":"","isCalc":0},{"name":"code","type":"string","types":None,"alias":"","description":"","fieldCategory":None,"specId":None,"id":"1141789505080606722","formula":"","isCalc":0},{"name":"total","type":"float","types":None,"alias":"","description":"","fieldCategory":None,"specId":None,"id":"1141789505080606723","formula":"","isCalc":0},{"name":"forward_total","type":"float","types":None,"alias":"","description":"","fieldCategory":None,"specId":None,"id":"1141789505080606724","formula":"","isCalc":0},{"name":"reverse_total","type":"float","types":None,"alias":"","description":"","fieldCategory":None,"specId":None,"id":"1141789505080606725","formula":"","isCalc":0},{"name":"sum_flow","type":"float","types":None,"alias":"","description":"","fieldCategory":None,"specId":None,"id":"1141789505080606726","formula":"","isCalc":0}],"changeStr":None,"change":None,"mappings":None,"schemaInfo":None,"oid":schema_id,"tenantId":tenant_id,"expiredTime":253402214400,"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"owner":owner,"tags":None,"tagObjs":None,"showInput":True},"storage":"FTP","expiredPeriod":0,"tags":[],"tagNames":[],"source":"create","storageConfigurations":{"expiredTime":0,"time":"","user":"europa","password":"a$01ba63d2e701965dea27a50d9fad5113~","format":"csv","pathMode":"exact","recursive":"false","scanPathRegular":"","scanFileRegular":"","dataFilterRegular":"","startFilterTime":"","endFilterTime":"","path":"ftp://192.168.1.84/home/europa/guojinbo","relativePath":"","header":"false","ignoreRow":0,"separator":",","quoteChar":"\"","clusterId":"","escapeChar":"\\","lineSeparator":"0A","host":"192.168.1.84","isNow":True,"timeField":"","timeFormat":""},"sliceTime":"","sliceType":"H","owner":"","schemaVersion":1,"timestampAsOf":"","specialField":{},"datasourceId":"","datasourceName":"","schemaId":schema_id,"oid":schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'gjb_test_sftp' in data:
            new_data = {"datasourceId":"","datasourceName":"","name":"gjb_test_sftp_dataset_随机数","alias":"","description":"","resource":None,"resourceId":resource_id,"schema":{"tenantId":tenant_id,"owner":owner,"name":schema_name,"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"id":schema_id,"version":2,"resourceId":schema_resourceid,"fields":[],"fieldCount":3,"primaryKeys":None,"resource":None,"oid":schema_id,"newest":1,"isHide":0,"tags":None,"description":None,"hisVersion":1,"status":0,"dataSourceIds":None,"dataSourceNames":None,"type":None,"tagObjs":None,"expiredPeriod":0},"storage":"SFTP","source":"create","expiredPeriod":0,"tags":[],"tagNames":"","storageConfigurations":{"expiredTime":0,"time":"","format":"csv","scanPathRegular":"","scanFileRegular":"","dataFilterRegular":"","startFilterTime":"","endFilterTime":"","path":"/home/europa/test_sftp/sftp_sink_csv1","host":"192.168.1.84","port":"22","user":"europa","password":"a$01ba63d2e701965dea27a50d9fad5113~","relativePath":"","pathMode":"exact","recursive":"false","header":"false","ignoreRow":0,"separator":",","quoteChar":"\"","localTempDir":"","clusterId":"","escapeChar":"\\","lineSeparator":"0A","isNow":True,"timeField":"","timeFormat":""},"sliceTime":"","sliceType":"H","owner":owner,"schemaVersion":2,"specialField":{"name":None,"type":None,"alias":None,"description":None,"currVal":None},"tenantId":"1093978681540575232","enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"version":1,"hasSafetyLevel":False,"schemaId":schema_id,"recordNumber":0,"byteSize":0,"type":"NORMAL","analysisTime":0,"formatConfigurations":None,"isHide":0,"oid":schema_id,"properties":None,"ctype":0,"schemaHisId":None,"isShare":0,"queryFeature":"${format:csv}","tagObjs":None}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'gjb_test_hudi_dataset' in data:
            new_data = {"id": "", "name": "gjb_test_hudi_dataset_随机数", "description": "",
                        "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"],
                        "schema": {"tenantId": tenant_id, "owner": owner, "name": schema_name, "enabled": 1,
                                   "creator": "admin", "createTime": data_now(), "lastModifier": "admin",
                                   "lastModifiedTime": data_now(), "id": schema_id, "version": 1, "groupCount": None,
                                   "groupFieldValue": None, "resourceId": schema_resourceid, "fields": [
                                {"name": "a", "type": "string", "alias": "", "description": "", "fieldCategory": None,
                                 "specId": None},
                                {"name": "b", "type": "string", "alias": "", "description": "", "fieldCategory": None,
                                 "specId": None}, {"name": "c", "type": "timestamp", "alias": "", "description": "",
                                                   "fieldCategory": None, "specId": None}], "primaryKeys": None,
                                   "resource": None, "oid": "966722185580249088", "newest": 1, "isHide": 0, "tags": [],
                                   "description": "自动化用，勿删出", "expiredPeriod": 0, "showInput": "true"},
                        "storage": "HUDI", "expiredPeriod": 0, "tags": [],
                        "storageConfigurations": {"time": "s", "path": "/auto_test/sink_hudi", "queryType": "",
                                                  "endTime": "", "expiredTime": 0, "beginTime": "", "clusterId": ""},
                        "sliceTime": "", "sliceType": "H", "owner": "", "schemaVersion": 1, "timestampAsOf": "",
                        "specialField": {}, "schemaId": schema_id, "oid": schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'gjb_test_neo4j' in data:
            new_data = {"id":"","name":"gjb_test_neo4j_随机数","alias":"","description":"","resource":{"id":resource_id},"resourceId":resource_id,"schema":{"id":schema_id,"hisId":schema_hisid,"name":schema_name,"type":"input","description":None,"version":1,"hisVersion":1,"resourceId":schema_resourceid,"resource":None,"datasourceIds":"","datasourceNames":None,"fields":[],"changeStr":None,"change":None,"mappings":None,"schemaInfo":None,"oid":schema_id,"tenantId":tenant_id,"expiredTime":253402214400,"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"owner":owner,"tags":None,"tagObjs":None,"showInput":None},"storage":"Neo4j","expiredPeriod":0,"tags":[],"tagNames":[],"source":"create","storageConfigurations":{"expiredTime":0,"time":"","url":"bolt://192.168.1.61:7687","user":"neo4j","password":"a$fdc6d1b091cff90a6507590654a0b39d~","src":"all","source":"all","edge":"all","target":"all","sourceFields":[{"name":"id","value":"id","_uuid_":"dynamic_1"}],"edgeFields":[{"name":"ts","value":"ts","_uuid_":"dynamic_2"}],"clusterId":"","targetFields":[{"name":"code","value":"code","_uuid_":"dynamic_3"}]},"sliceTime":"","sliceType":"H","owner":"","schemaVersion":1,"timestampAsOf":"","specialField":{},"schemaId":schema_id,"oid":schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'gjb_test_redis' in data:
            new_data = {"id": "", "name": "gjb_test_redis_dataset_随机数", "description": "",
                        "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"],
                        "schema": {"tenantId": tenant_id, "owner": owner, "name": schema_name, "enabled": 1,
                                   "creator": "admin", "createTime": data_now(), "lastModifier": "admin",
                                   "lastModifiedTime": data_now(), "id": schema_id, "version": 1, "groupCount": None,
                                   "groupFieldValue": None, "resourceId": schema_resourceid, "fields": [
                                {"name": "id", "type": "int", "alias": "", "description": "", "fieldCategory": None,
                                 "specId": None}, {"name": "ts", "type": "timestamp", "alias": "", "description": "",
                                                   "fieldCategory": None, "specId": None},
                                {"name": "code", "type": "string", "alias": "", "description": "",
                                 "fieldCategory": None, "specId": None},
                                {"name": "total", "type": "float", "alias": "", "description": "",
                                 "fieldCategory": None, "specId": None},
                                {"name": "forward_total", "type": "float", "alias": "", "description": "",
                                 "fieldCategory": None, "specId": None},
                                {"name": "reverse_total", "type": "float", "alias": "", "description": "",
                                 "fieldCategory": None, "specId": None},
                                {"name": "sum_flow", "type": "float", "alias": "", "description": "",
                                 "fieldCategory": None, "specId": None}], "primaryKeys": None, "resource": None,
                                   "oid": schema_id, "newest": 1, "isHide": 0, "tags": [],
                                   "description": "gjb_ttest_mysql0420_training", "expiredPeriod": 0,
                                   "showInput": "true"}, "storage": "REDIS", "expiredPeriod": 0, "tags": [],
                        "storageConfigurations": {"expiredTime": 0, "time": "", "url": "info4:6379",
                                                  "keyColumn": "code", "password": "", "clusterId": "",
                                                  "table": "test"}, "sliceTime": "", "sliceType": "H", "owner": "",
                        "schemaVersion": 1, "timestampAsOf": "", "specialField": {}, "schemaId": schema_id,
                        "oid": schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'gjb_test_hbase' in data:
            new_data = {"id": "", "name": "gjb_test_hbase_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "owner": "SYSTEM",
                                   "name": schema_name, "creator": "admin", "createTime": 1587370296000,
                                   "lastModifier": "admin", "lastModifiedTime": 1587370296000, "version": 1,
                                   "enabled": 1, "description": "tester_mysql0420_training",
                                   "resourceId": schema_resourceid,
                                   "fields": [{"name": "id", "type": "int", "alias": "", "description": ""},
                                              {"name": "ts", "type": "timestamp", "alias": "", "description": ""},
                                              {"name": "code", "type": "string", "alias": "", "description": ""},
                                              {"name": "total", "type": "float", "alias": "", "description": ""},
                                              {"name": "forward_total", "type": "float", "alias": "",
                                               "description": ""},
                                              {"name": "reverse_total", "type": "float", "alias": "",
                                               "description": ""},
                                              {"name": "sum_flow", "type": "float", "alias": "", "description": ""}],
                                   "oid": schema_id, "newest": 1, "isHide": 0, "expiredPeriod": 0}, "storage": "HBASE",
                        "expiredPeriod": 0, "storageConfigurations": {"table": "test_hbase2020", "namespace": "default",
                                                                      "columns": "rowKey:key,:ts,:code,:total,:forward_total,:reverse_total,:sum_flow",
                                                                      "columnsKey": "id", "columnsColumns": "",
                                                                      "isSingle": "true", "columnsItems": 0,
                                                                      "undefined": "csv"}, "sliceTime": "",
                        "sliceType": "H", "schemaVersion": 1, "clusterId": "",
                        "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"], "description": "gjb_test-hbase_随机数",
                        "schemaId": schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'gjb_test_es_dataset' in data:
            new_data = {"name": "gjb_test_es_dataset_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "owner": "SYSTEM",
                                   "name": schema_name, "creator": "admin", "createTime": 1587370889000,
                                   "lastModifier": "admin", "lastModifiedTime": 1587370889000, "version": 1,
                                   "enabled": 1, "description": "", "resourceId": schema_resourceid,
                                   "fields": [{"name": "Name", "type": "string", "alias": "", "description": ""},
                                              {"name": "Sex", "type": "string", "alias": "", "description": ""},
                                              {"name": "Age", "type": "int", "alias": "", "description": ""},
                                              {"name": "Identity_code", "type": "string", "alias": "",
                                               "description": ""},
                                              {"name": "C_time", "type": "string", "alias": "", "description": ""},
                                              {"name": "Data_long", "type": "bigint", "alias": "", "description": ""},
                                              {"name": "Data_double", "type": "double", "alias": "", "description": ""},
                                              {"name": "Data_boolean", "type": "boolean", "alias": "",
                                               "description": ""},
                                              {"name": "time_col", "type": "timestamp", "alias": "", "description": ""},
                                              {"name": "Str_time", "type": "bigint", "alias": "", "description": ""},
                                              {"name": "Salary", "type": "string", "alias": "", "description": ""},
                                              {"name": "None_data", "type": "string", "alias": "", "description": ""},
                                              {"name": "City", "type": "string", "alias": "", "description": ""},
                                              {"name": "data1", "type": "string", "alias": "", "description": ""},
                                              {"name": "data2", "type": "string", "alias": "", "description": ""},
                                              {"name": "data3", "type": "string", "alias": "", "description": ""},
                                              {"name": "data4", "type": "string", "alias": "", "description": ""},
                                              {"name": "data5", "type": "string", "alias": "", "description": ""},
                                              {"name": "data6", "type": "string", "alias": "", "description": ""},
                                              {"name": "data7", "type": "string", "alias": "", "description": ""},
                                              {"name": "data8", "type": "string", "alias": "", "description": ""},
                                              {"name": "data9", "type": "string", "alias": "", "description": ""}],
                                   "oid": schema_id, "newest": 1, "isHide": 0, "expiredPeriod": 0},
                        "storage": "ElasticSearch", "expiredPeriod": 0,
                        "storageConfigurations": {"clusterName": "elasticsearch", "ipAddresses": "info5:9203",
                                                  "index": "test_stre", "indexType": "test_stre"}, "sliceTime": "",
                        "sliceType": "H", "schemaVersion": 1, "clusterId": "",
                        "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"], "schemaId": schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'gjb_test_SearchOne_dataset' in data:
            new_data = {"name": "gjb_test_SearchOne_dataset_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "owner": "SYSTEM",
                                   "name": schema_name, "creator": "admin", "createTime": 1587370296000,
                                   "lastModifier": "admin", "lastModifiedTime": 1587370296000, "version": 1,
                                   "enabled": 1, "description": "tester_mysql0420_training",
                                   "resourceId": schema_resourceid,
                                   "fields": [{"name": "id", "type": "int", "alias": "", "description": ""},
                                              {"name": "ts", "type": "timestamp", "alias": "", "description": ""},
                                              {"name": "code", "type": "string", "alias": "", "description": ""},
                                              {"name": "total", "type": "float", "alias": "", "description": ""},
                                              {"name": "forward_total", "type": "float", "alias": "",
                                               "description": ""},
                                              {"name": "reverse_total", "type": "float", "alias": "",
                                               "description": ""},
                                              {"name": "sum_flow", "type": "float", "alias": "", "description": ""}],
                                   "oid": schema_id, "newest": 1, "isHide": 0, "expiredPeriod": 0},
                        "storage": "SearchOne", "expiredPeriod": 0,
                        "storageConfigurations": {"clusterName": "my-cluster",
                                                  "ipAddresses": "192.168.1.81:9200,192.168.1.82:9200,192.168.1.84:9200",
                                                  "index": "test_new_0103", "indexType": "test_new_0103"},
                        "sliceTime": "", "sliceType": "H", "owner": dataset_info[0]["owner"],
                        "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"]}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'gjb_test_HDFS_dataset' in data:
            new_data = {"name":"gjb_ttest_hdfs042219_随机数","alias":None,"description":"","resource":None,"resourceId":resource_id,"schema":{"tenantId":tenant_id,"owner":owner,"name":schema_name,"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"id":schema_id,"version":1,"resourceId":schema_resourceid,"fields":[],"fieldCount":7,"primaryKeys":None,"resource":None,"oid":schema_id,"newest":1,"isHide":0,"tags":None,"description":None,"hisVersion":1,"status":0,"dataSourceIds":None,"dataSourceNames":None,"type":None,"tagObjs":None,"expiredPeriod":0},"storage":"HDFS","expiredPeriod":0,"tags":[],"tagNames":[],"source":None,"storageConfigurations":{"flatten":True,"expiredTime":0,"time":"s","format":"csv","path":"/auto_test/api","relativePath":"/auto_test/api","pathMode":"exact","header":"false","ignoreRow":"0","separator":",","quoteChar":"\"","escapeChar":"\\","encryptKey":"","encryptColumnsTemplate":[],"encryptColumns":"","hdfsPartitionColumn":"","specialFieldSelect":"","partitionType":"DateFormat","dateFormat":"","dateFrom":"","dateTo":"","cycle":"","externalTableName":"","datePeriod":"","dateUnit":"HOUR","clusterId":"cluster1","storageOutSlicePath":True,"storageDynamicPath":"","partitionList":"","pathFilter":"(.inprogress|.pending)","csv":"csv"},"sliceTime":"","sliceType":"H","owner":owner,"schemaVersion":1,"timestampAsOf":"","specialField":{"name":None,"type":None,"alias":None,"description":None,"currVal":None},"tenantId":tenant_id,"enabled":1,"creator":"admin","createTime":data_now(),"lastModifier":"admin","lastModifiedTime":data_now(),"version":2,"hasSafetyLevel":None,"schemaId":schema_id,"recordNumber":0,"byteSize":0,"type":"NORMAL","analysisTime":0,"formatConfigurations":None,"isHide":0,"datasourceId":"","datasourceName":"","oid":None,"properties":None,"ctype":0,"schemaHisId":None,"isShare":0,"queryFeature":"${format:csv}","tagObjs":None}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'gjb_test_kafka_dataset' in data:
            new_data = {"id": "", "name": "gjb_test_kafka_dataset_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "owner": "SYSTEM",
                                   "name": schema_name, "creator": "admin", "createTime": 1587370296000,
                                   "lastModifier": "admin", "lastModifiedTime": 1587370296000, "version": 1,
                                   "enabled": 1, "description": "tester_mysql0420_training",
                                   "resourceId": schema_resourceid,
                                   "fields": [{"name": "id", "type": "int", "alias": "", "description": ""},
                                              {"name": "ts", "type": "timestamp", "alias": "", "description": ""},
                                              {"name": "code", "type": "string", "alias": "", "description": ""},
                                              {"name": "total", "type": "float", "alias": "", "description": ""},
                                              {"name": "forward_total", "type": "float", "alias": "",
                                               "description": ""},
                                              {"name": "reverse_total", "type": "float", "alias": "",
                                               "description": ""},
                                              {"name": "sum_flow", "type": "float", "alias": "", "description": ""}],
                                   "oid": schema_id, "newest": 1, "isHide": 0, "expiredPeriod": 0}, "storage": "KAFKA",
                        "expiredPeriod": 0, "storageConfigurations": {"format": "csv",
                                                                      "zookeeper": "info1:2181,info2:2181,info3:2181/europa/app/kafka",
                                                                      "brokers": "info3:9093", "topic": "kafka_new610",
                                                                      "groupId": "kafka_new610", "version": "0.10",
                                                                      "reader": "", "separator": ",", "header": "false",
                                                                      "quoteChar": "\"", "escapeChar": "\\"},
                        "sliceTime": "", "sliceType": "H", "owner": dataset_info[0]["owner"],
                        "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"]}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'gjb_test_hive_dataset' in data:
            new_data = {"name": "gjb_test_hive_dataset_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "owner": "SYSTEM",
                                   "name": schema_name, "creator": "admin", "createTime": 1587370296000,
                                   "lastModifier": "admin", "lastModifiedTime": 1587370296000, "version": 1,
                                   "enabled": 1, "description": "tester_mysql0420_training",
                                   "resourceId": schema_resourceid,
                                   "fields": [{"name": "id", "type": "int", "alias": "", "description": ""},
                                              {"name": "ts", "type": "timestamp", "alias": "", "description": ""},
                                              {"name": "code", "type": "string", "alias": "", "description": ""},
                                              {"name": "total", "type": "float", "alias": "", "description": ""},
                                              {"name": "forward_total", "type": "float", "alias": "",
                                               "description": ""},
                                              {"name": "reverse_total", "type": "float", "alias": "",
                                               "description": ""},
                                              {"name": "sum_flow", "type": "float", "alias": "", "description": ""}],
                                   "oid": schema_id, "newest": 1, "isHide": 0, "expiredPeriod": 0}, "storage": "HIVE",
                        "expiredPeriod": 0,
                        "storageConfigurations": {"sql": "", "table": "students_info_hive_sink_0617",
                                                  "partitionColumns": ""}, "sliceTime": "", "sliceType": "H",
                        "owner": dataset_info[0]["owner"], "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"]}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'gjb_ttest_hdfs042219' == data.split("&")[0]:
            dataset_id, dataset_name, resource_id = get_dataset_data(list_data[0])
            new_data = {"id": dataset_id, "name": dataset_name, "alias": None, "description": "", "resource": None,
                         "schema": {"tenantId": dataset_info[0]["tenant_id"], "owner": dataset_info[0]["owner"],
                                    "name": schema_name, "enabled": 1, "creator": "admin", "createTime": data_now(),
                                    "lastModifier": "admin", "lastModifiedTime": data_now(), "id": schema_id,
                                    "version": 2, "groupCount": None, "groupFieldValue": None,
                                    "resourceId": schema_resourceid, "fields": [
                                 {"name": "id", "type": "int", "alias": "", "description": "", "fieldCategory": None,
                                  "specId": None}, {"name": "ts", "type": "timestamp", "alias": "", "description": "",
                                                    "fieldCategory": None, "specId": None},
                                 {"name": "code", "type": "string", "alias": "", "description": "",
                                  "fieldCategory": None, "specId": None},
                                 {"name": "total", "type": "float", "alias": "", "description": "",
                                  "fieldCategory": None, "specId": None},
                                 {"name": "forward_total", "type": "float", "alias": "", "description": "",
                                  "fieldCategory": None, "specId": None},
                                 {"name": "reverse_total", "type": "float", "alias": "", "description": "",
                                  "fieldCategory": None, "specId": None},
                                 {"name": "sum_flow", "type": "float", "alias": "", "description": "",
                                  "fieldCategory": None, "specId": None}], "primaryKeys": None, "resource": None,
                                    "oid": "966718205856841728", "newest": 0, "isHide": 0, "tags": None,
                                    "description": None, "expiredPeriod": 0}, "storage": "HDFS", "expiredPeriod": 0,
                         "tags": [], "source": None,
                         "storageConfigurations": {"flatten": True, "expiredTime": 0, "time": "s", "format": "csv",
                                                   "path": "/auto_test/api", "relativePath": "/auto_test/api",
                                                   "pathMode": "exact", "header": "false", "ignoreRow": "0",
                                                   "separator": ",", "quoteChar": "\"", "escapeChar": "\\",
                                                   "encryptKey": "", "encryptColumnsTemplate": [], "encryptColumns": "",
                                                   "hdfsPartitionColumn": "", "specialFieldSelect": "",
                                                   "partitionType": "DateFormat", "dateFormat": "", "dateFrom": "",
                                                   "dateTo": "", "cycle": "", "datePeriod": "", "dateUnit": "HOUR",
                                                   "clusterId": "cluster1", "partitionList": "",
                                                   "pathFilter": "(.inprogress|.pending)", "csv": "csv"},
                         "sliceTime": "", "sliceType": "H", "owner": dataset_info[0]["owner"], "schemaVersion": 1,
                         "timestampAsOf": "",
                         "specialField": {"name": None, "type": None, "alias": None, "description": None,
                                          "currVal": None}, "tenantId": dataset_info[0]["tenant_id"], "enabled": 1,
                         "creator": "admin", "createTime": data_now(), "lastModifier": "admin",
                         "lastModifiedTime": data_now(), "version": 1, "groupCount": None, "groupFieldValue": None,
                         "resourceId": resource_id, "schemaId": schema_id, "recordNumber": 0, "byteSize": 0,
                         "type": "NORMAL", "analysisTime": 0, "formatConfigurations": None, "isRelated": 1, "isHide": 0,
                         "datasourceId": None, "datasourceName": None, "oid": None}
            new_data["schema"]["fields"].append(schema_info)
            return new_data
        elif 'lq_dataset_hdfs' == data.split("&")[0]:
            new_data = {"name": "lq_dataset_hdfs_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "name": schema_name,
                                   "version": 1, "enabled": 1, "resourceId": schema_resourceid,
                                   "fields": [{"name": "Name", "type": "string", "alias": ""},
                                              {"name": "Sex", "type": "string", "alias": ""},
                                              {"name": "Age", "type": "int", "alias": ""},
                                              {"name": "Identity_code", "type": "string", "alias": ""},
                                              {"name": "C_time", "type": "string", "alias": ""},
                                              {"name": "Data_long", "type": "bigint", "alias": ""},
                                              {"name": "Data_double", "type": "double", "alias": ""},
                                              {"name": "Data_boolean", "type": "boolean", "alias": ""},
                                              {"name": "time_col", "type": "timestamp", "alias": ""},
                                              {"name": "Str_time", "type": "bigint", "alias": ""},
                                              {"name": "Salary", "type": "string", "alias": ""},
                                              {"name": "None_data", "type": "string", "alias": ""},
                                              {"name": "City", "type": "string", "alias": ""},
                                              {"name": "data1", "type": "string", "alias": ""},
                                              {"name": "data2", "type": "string", "alias": ""},
                                              {"name": "data3", "type": "string", "alias": ""},
                                              {"name": "data4", "type": "string", "alias": ""},
                                              {"name": "data5", "type": "string", "alias": ""},
                                              {"name": "data6", "type": "string", "alias": ""},
                                              {"name": "data7", "type": "string", "alias": ""},
                                              {"name": "data8", "type": "string", "alias": ""},
                                              {"name": "data9", "type": "string", "alias": ""}], "newest": 1,
                                   "isHide": 0, "expiredPeriod": 0}, "storage": "HDFS", "expiredPeriod": 0,
                        "storageConfigurations": {"format": "csv", "path": "/merce_57/auto/hdfs",
                                                  "relativePath": "/merce_57/auto/hdfs", "pathMode": "exact",
                                                  "header": "false", "ignoreRow": 0, "separator": ",",
                                                  "quoteChar": "\"", "escapeChar": "\\", "hdfsPartitionColumn": "",
                                                  "partitionType": "DateFormat", "dateFormat": "", "dateFrom": "",
                                                  "dateTo": "", "datePeriod": "", "dateUnit": "HOUR",
                                                  "partitionList": "", "encryptKey": "", "encryptColumns": "",
                                                  "columnsItems": 0}, "sliceTime": "", "sliceType": "H",
                        "schemaVersion": 1, "clusterId": "",
                        "resource": {"id": dataset_info[0]["id"],"resourceId":dataset_info[0]["id"], "schemaId": schema_id}}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'lq_sink_dataset_hdfs' == data.split("&")[0]:
            new_data = {"name": "lq_sink_dataset_hdfs_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "name": schema_name,
                                   "version": 1, "enabled": 1, "resourceId": schema_resourceid,
                                   "fields": [{"name": "Name", "type": "string", "alias": ""},
                                              {"name": "Sex", "type": "string", "alias": ""},
                                              {"name": "Age", "type": "int", "alias": ""},
                                              {"name": "Identity_code", "type": "string", "alias": ""},
                                              {"name": "C_time", "type": "string", "alias": ""},
                                              {"name": "Data_long", "type": "bigint", "alias": ""},
                                              {"name": "Data_double", "type": "double", "alias": ""},
                                              {"name": "Data_boolean", "type": "boolean", "alias": ""},
                                              {"name": "time_col", "type": "timestamp", "alias": ""},
                                              {"name": "Str_time", "type": "bigint", "alias": ""},
                                              {"name": "Salary", "type": "string", "alias": ""},
                                              {"name": "None_data", "type": "string", "alias": ""},
                                              {"name": "City", "type": "string", "alias": ""},
                                              {"name": "data1", "type": "string", "alias": ""},
                                              {"name": "data2", "type": "string", "alias": ""},
                                              {"name": "data3", "type": "string", "alias": ""},
                                              {"name": "data4", "type": "string", "alias": ""},
                                              {"name": "data5", "type": "string", "alias": ""},
                                              {"name": "data6", "type": "string", "alias": ""},
                                              {"name": "data7", "type": "string", "alias": ""},
                                              {"name": "data8", "type": "string", "alias": ""},
                                              {"name": "data9", "type": "string", "alias": ""}], "newest": 1,
                                   "isHide": 0, "expiredPeriod": 0}, "storage": "HDFS", "expiredPeriod": 0,
                        "storageConfigurations": {"format": "csv", "path": "/tmp/lisatest/sink_hdfs",
                                                  "relativePath": "/tmp/lisatest/sink_hdfs", "pathMode": "exact",
                                                  "header": "false", "ignoreRow": 0, "separator": ",",
                                                  "quoteChar": "\"", "escapeChar": "\\", "hdfsPartitionColumn": "",
                                                  "partitionType": "DateFormat", "dateFormat": "", "dateFrom": "",
                                                  "dateTo": "", "datePeriod": "", "dateUnit": "HOUR",
                                                  "partitionList": "", "encryptKey": "", "encryptColumns": "",
                                                  "columnsItems": 0}, "sliceTime": "", "sliceType": "H",
                        "schemaVersion": 1, "clusterId": "",
                        "resource": {"id": dataset_info[0]["id"],"resourceId":dataset_info[0]["id"], "schemaId": schema_id}}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'lq_sink_dataset_kafka' == data.split("&")[0]:
            new_data = {"name": "lq_sink_dataset_kafka_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "groupCount": None,
                                   "groupFieldValue": None, "name": schema_name, "creator": "admin",
                                   "createTime": "2021-03-16 09:49:46", "lastModifier": "admin",
                                   "lastModifiedTime": "2021-03-16 09:49:46", "version": 1, "enabled": 1,
                                   "description": None, "resourceId": schema_resourceid,
                                   "fields": [{"name": "Name", "type": "string", "alias": "", "description": None},
                                              {"name": "Sex", "type": "string", "alias": "", "description": None},
                                              {"name": "Age", "type": "int", "alias": "", "description": None},
                                              {"name": "Identity_code", "type": "string", "alias": "",
                                               "description": None},
                                              {"name": "C_time", "type": "string", "alias": "", "description": None},
                                              {"name": "Data_long", "type": "bigint", "alias": "", "description": None},
                                              {"name": "Data_double", "type": "double", "alias": "",
                                               "description": None},
                                              {"name": "Data_boolean", "type": "boolean", "alias": "",
                                               "description": None},
                                              {"name": "time_col", "type": "timestamp", "alias": "",
                                               "description": None},
                                              {"name": "Str_time", "type": "bigint", "alias": "", "description": None},
                                              {"name": "Salary", "type": "string", "alias": "", "description": None},
                                              {"name": "None_data", "type": "string", "alias": "", "description": None},
                                              {"name": "City", "type": "string", "alias": "", "description": None},
                                              {"name": "data1", "type": "string", "alias": "", "description": None},
                                              {"name": "data2", "type": "string", "alias": "", "description": None},
                                              {"name": "data3", "type": "string", "alias": "", "description": None},
                                              {"name": "data4", "type": "string", "alias": "", "description": None},
                                              {"name": "data5", "type": "string", "alias": "", "description": None},
                                              {"name": "data6", "type": "string", "alias": "", "description": None},
                                              {"name": "data7", "type": "string", "alias": "", "description": None},
                                              {"name": "data8", "type": "string", "alias": "", "description": None},
                                              {"name": "data9", "type": "string", "alias": "", "description": None}],
                                   "mode": None, "primaryKeys": None, "resource": None, "projectEntity": None,
                                   "newest": 1, "isHide": 0, "expiredPeriod": 0}, "storage": "KAFKA",
                        "expiredPeriod": 0, "storageConfigurations": {"format": "csv",
                                                                      "zookeeper": "info1:2181,info3:2181,info2:2181/info2_kafka",
                                                                      "brokers": "192.168.1.82:9094",
                                                                      "isKerberosSupport": False, "topic": "lq_sss",
                                                                      "groupId": "lq_sss", "version": "", "reader": "",
                                                                      "separator": ",", "quoteChar": "\"",
                                                                      "escapeChar": "\\", "encryptKey": "",
                                                                      "encryptColumns": "", "columnsItems": 0},
                        "sliceTime": "", "sliceType": "H", "owner": None, "schemaVersion": 1, "clusterId": "",
                        "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"], "schemaId": schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'lq_sink_dataset_hive' == data.split("&")[0]:
            new_data = {"name": "lq_sink_dataset_hive_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "groupCount": None,
                                   "groupFieldValue": None, "name": schema_name, "creator": "admin",
                                   "createTime": "2021-03-18 02:56:10", "lastModifier": "admin",
                                   "lastModifiedTime": "2021-03-18 02:56:10", "version": 1, "enabled": 1,
                                   "description": "", "resourceId": schema_resourceid,
                                   "fields": [{"name": "age_count", "type": "bigint", "alias": "", "description": None},
                                              {"name": "age_sum", "type": "bigint", "alias": "", "description": None},
                                              {"name": "avg_age", "type": "double", "alias": "", "description": None},
                                              {"name": "Name", "type": "string", "alias": "", "description": None},
                                              {"name": "Sex", "type": "string", "alias": "", "description": None}],
                                   "mode": None, "primaryKeys": None, "resource": None, "projectEntity": None,
                                   "newest": 1, "isHide": 0, "expiredPeriod": 0}, "storage": "HIVE", "expiredPeriod": 0,
                        "storageConfigurations": {"sql": "", "table": "lq_test_sink_随机数", "partitionColumns": "",
                                                  "columnsItems": 0}, "sliceTime": "", "sliceType": "H", "owner": None,
                        "schemaVersion": 1, "clusterId": "", "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"],
                        "schemaId": schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'lq_sink_dataset_neo4j' == data.split("&")[0]:
            new_data = {"name": "lq_sink_dataset_neo4j_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "groupCount": None,
                                   "groupFieldValue": None, "name": schema_name, "creator": "admin",
                                   "createTime": "2021-04-01 03:12:48", "lastModifier": "admin",
                                   "lastModifiedTime": "2021-04-01 03:12:48", "version": 4, "enabled": 1,
                                   "description": "", "resourceId": schema_resourceid,
                                   "fields": [{"name": "name", "type": "string", "alias": "", "description": ""},
                                              {"name": "born", "type": "int", "alias": "", "description": ""}],
                                   "mode": None, "primaryKeys": None, "resource": None, "projectEntity": None,
                                   "newest": 1, "isHide": 0, "expiredPeriod": 0}, "storage": "Neo4j",
                        "expiredPeriod": 0,
                        "storageConfigurations": {"url": "bolt://192.168.1.75:7687", "user": "neo4j",
                                                  "password": "AES(a6e8d9ec31c9aac578554474d5f27383)", "src": "Person",
                                                  "edge": "all", "target": "all",
                                                  "sourceFields": [{"name": "name", "value": "name"},
                                                                   {"name": "born", "value": "born"}], "edgeFields": [],
                                                  "targetFields": [], "columnsItems": 0}, "sliceTime": "",
                        "sliceType": "H", "owner": None, "schemaVersion": 4, "clusterId": "",
                        "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"], "schemaId": schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'lq_source_dataset_neo4j' == data.split("&")[0]:
            new_data = {"name": "lq_source_dataset_neo4j_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "groupCount": None,
                                   "groupFieldValue": None, "name": schema_name, "creator": "admin",
                                   "createTime": "2021-04-01 03:12:48", "lastModifier": "admin",
                                   "lastModifiedTime": "2021-04-01 03:12:48", "version": 4, "enabled": 1,
                                   "description": "", "resourceId": schema_resourceid,
                                   "fields": [{"name": "name", "type": "string", "alias": "", "description": ""},
                                              {"name": "born", "type": "int", "alias": "", "description": ""}],
                                   "mode": None, "primaryKeys": None, "resource": None, "projectEntity": None,
                                   "newest": 1, "isHide": 0, "expiredPeriod": 0}, "storage": "HDFS", "expiredPeriod": 0,
                        "storageConfigurations": {"format": "csv", "path": "/tmp/lisatest/neo4j",
                                                  "relativePath": "/tmp/lisatest/neo4j", "pathMode": "exact",
                                                  "header": "false", "ignoreRow": 0, "separator": ",",
                                                  "quoteChar": "\"", "escapeChar": "\\", "hdfsPartitionColumn": "",
                                                  "partitionType": "DateFormat", "dateFormat": "", "dateFrom": "",
                                                  "dateTo": "", "datePeriod": "", "dateUnit": "HOUR",
                                                  "partitionList": "", "encryptKey": "", "encryptColumns": "",
                                                  "columnsItems": 0}, "sliceTime": "", "sliceType": "H", "owner": None,
                        "schemaVersion": 4, "clusterId": "", "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"],
                        "schemaId": schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'lq_dataset_jdbc' == data.split("&")[0]:
            new_data = {"name": "lq_dataset_jdbc_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "groupCount": None,
                                   "groupFieldValue": None, "name": "jdbc_source", "creator": "admin",
                                   "createTime": "2021-04-01 06:25:02", "lastModifier": "admin",
                                   "lastModifiedTime": "2021-04-01 06:25:02", "version": 1, "enabled": 1,
                                   "description": "", "resourceId": schema_resourceid,
                                   "fields": [{"name": "name", "type": "string", "alias": "", "description": ""},
                                              {"name": "col", "type": "string", "alias": "", "description": ""},
                                              {"name": "age", "type": "int", "alias": "", "description": ""},
                                              {"name": "id", "type": "int", "alias": "", "description": ""}],
                                   "mode": None, "primaryKeys": None, "resource": None, "projectEntity": None,
                                   "newest": 1, "isHide": 0, "expiredPeriod": 0}, "storage": "HDFS", "expiredPeriod": 0,
                        "storageConfigurations": {"format": "csv", "path": "/tmp/lisatest/jdbc_sink",
                                                  "relativePath": "/tmp/lisatest/jdbc_sink", "pathMode": "exact",
                                                  "header": "false", "ignoreRow": 0, "separator": ",",
                                                  "quoteChar": "\"", "escapeChar": "\\", "hdfsPartitionColumn": "",
                                                  "partitionType": "DateFormat", "dateFormat": "", "dateFrom": "",
                                                  "dateTo": "", "datePeriod": "", "dateUnit": "HOUR",
                                                  "partitionList": "", "encryptKey": "", "encryptColumns": "",
                                                  "columnsItems": 0}, "sliceTime": "", "sliceType": "H", "owner": None,
                        "schemaVersion": 1, "clusterId": "", "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"],
                        "schemaId": schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'lq_sink_dataset_jdbc' == data.split("&")[0]:
            new_data = {"name": "lq_sink_dataset_jdbc_随机数", "description": "", "expiredPeriod": 0, "storage": "JDBC",
                        "storageConfigurations": {"name": "Mysql", "table": "jdbc_sink", "schema": "",
                                                  "jarPath": "mysql-connector-java-5.1.48.jar", "catalog": "",
                                                  "DBType": "Mysql", "batchsize": 10000,
                                                  "url": "jdbc:mysql://192.168.1.75:3306/merce", "database": "merce",
                                                  "password": "AES(cad2fb721d282f6e5151605a1874ffe4)",
                                                  "driver": "com.mysql.jdbc.Driver", "port": 3306,
                                                  "host": "192.168.1.75", "chineseName": "", "user": "merce",
                                                  "dateToTimestamp": False, "username": "merce", "resType": "DB",
                                                  "id": "76146d45-fcef-40da-b32b-6490f7a08321"},
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "groupCount": None,
                                   "groupFieldValue": None, "name": schema_name, "creator": "admin",
                                   "createTime": "2021-04-01T08:48:13.000+0000", "lastModifier": "admin",
                                   "lastModifiedTime": "2021-04-01T08:48:13.000+0000", "version": 1, "enabled": 1,
                                   "description": "", "resourceId": schema_resourceid,
                                   "fields": [{"name": "name", "type": "string", "alias": "", "description": ""},
                                              {"name": "col", "type": "string", "alias": "", "description": ""},
                                              {"name": "age", "type": "int", "alias": "", "description": ""},
                                              {"name": "id", "type": "int", "alias": "", "description": ""}],
                                   "mode": None, "primaryKeys": None, "resource": None, "projectEntity": None,
                                   "newest": 1, "isHide": 0, "expiredPeriod": 0}, "owner": None, "schemaVersion": 1,
                        "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"], "schemaId": schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'lq_sink_dataset_hbase' == data.split("&")[0]:
            new_data = {"name": "lq_sink_dataset_hbase_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "groupCount": None,
                                   "groupFieldValue": None, "name": schema_name, "creator": "admin",
                                   "createTime": "2021-04-01 09:34:51", "lastModifier": "admin",
                                   "lastModifiedTime": "2021-04-01 09:34:51", "version": 1, "enabled": 1,
                                   "description": None, "resourceId": schema_resourceid,
                                   "fields": [{"name": "Name", "type": "string", "alias": "", "description": None},
                                              {"name": "Sex", "type": "string", "alias": "", "description": None},
                                              {"name": "Age", "type": "int", "alias": "", "description": None},
                                              {"name": "Identity_code", "type": "string", "alias": "",
                                               "description": None},
                                              {"name": "C_time", "type": "string", "alias": "", "description": None},
                                              {"name": "Data_long", "type": "bigint", "alias": "", "description": None},
                                              {"name": "Data_double", "type": "double", "alias": "",
                                               "description": None},
                                              {"name": "Data_boolean", "type": "boolean", "alias": "",
                                               "description": None},
                                              {"name": "time_col", "type": "timestamp", "alias": "",
                                               "description": None},
                                              {"name": "Str_time", "type": "bigint", "alias": "", "description": None},
                                              {"name": "Salary", "type": "string", "alias": "", "description": None},
                                              {"name": "None_data", "type": "string", "alias": "", "description": None},
                                              {"name": "City", "type": "string", "alias": "", "description": None},
                                              {"name": "data1", "type": "string", "alias": "", "description": None},
                                              {"name": "data2", "type": "string", "alias": "", "description": None},
                                              {"name": "data3", "type": "string", "alias": "", "description": None},
                                              {"name": "data4", "type": "string", "alias": "", "description": None},
                                              {"name": "data5", "type": "string", "alias": "", "description": None},
                                              {"name": "data6", "type": "string", "alias": "", "description": None},
                                              {"name": "data7", "type": "string", "alias": "", "description": None},
                                              {"name": "data8", "type": "string", "alias": "", "description": None},
                                              {"name": "data9", "type": "string", "alias": "", "description": None}],
                                   "mode": None, "primaryKeys": None, "resource": None, "projectEntity": None,
                                   "isHide": 0, "expiredPeriod": 0}, "storage": "HBASE", "expiredPeriod": 0,
                        "storageConfigurations": {"table": "0401", "namespace": "default",
                                                  "columns": "rowKey:key,columns:Sex,columns:Age,columns:Identity_code,columns:C_time,columns:Data_long,columns:Data_double,columns:Data_boolean,columns:time_col,columns:Str_time,columns:Salary,columns:None_data,columns:City,columns:data1,columns:data2,columns:data3,columns:data4,columns:data5,columns:data6,columns:data7,columns:data8,columns:data9",
                                                  "columnsKey": "Name", "isSingle": True, "columnsItems": 0},
                        "sliceTime": "", "sliceType": "H", "owner": None, "schemaVersion": 1, "clusterId": "",
                        "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"], "schemaId": schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'lq_sink_dataset_redis' == data.split("&")[0]:
            new_data = {"name": "lq_sink_dataset_redis_随机数",
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "groupCount": None,
                                   "groupFieldValue": None, "name": schema_name, "creator": "admin",
                                   "createTime": "2021-04-01 10:06:03", "lastModifier": "admin",
                                   "lastModifiedTime": "2021-04-01 10:06:03", "version": 1, "enabled": 1,
                                   "description": None, "resourceId": schema_resourceid,
                                   "fields": [{"name": "Name", "type": "string", "alias": "", "description": None},
                                              {"name": "Sex", "type": "string", "alias": "", "description": None},
                                              {"name": "Age", "type": "int", "alias": "", "description": None},
                                              {"name": "Identity_code", "type": "string", "alias": "",
                                               "description": None},
                                              {"name": "C_time", "type": "string", "alias": "", "description": None},
                                              {"name": "Data_long", "type": "bigint", "alias": "", "description": None},
                                              {"name": "Data_double", "type": "double", "alias": "",
                                               "description": None},
                                              {"name": "Data_boolean", "type": "boolean", "alias": "",
                                               "description": None},
                                              {"name": "time_col", "type": "timestamp", "alias": "",
                                               "description": None},
                                              {"name": "Str_time", "type": "bigint", "alias": "", "description": None},
                                              {"name": "Salary", "type": "string", "alias": "", "description": None},
                                              {"name": "None_data", "type": "string", "alias": "", "description": None},
                                              {"name": "City", "type": "string", "alias": "", "description": None},
                                              {"name": "data1", "type": "string", "alias": "", "description": None},
                                              {"name": "data2", "type": "string", "alias": "", "description": None},
                                              {"name": "data3", "type": "string", "alias": "", "description": None},
                                              {"name": "data4", "type": "string", "alias": "", "description": None},
                                              {"name": "data5", "type": "string", "alias": "", "description": None},
                                              {"name": "data6", "type": "string", "alias": "", "description": None},
                                              {"name": "data7", "type": "string", "alias": "", "description": None},
                                              {"name": "data8", "type": "string", "alias": "", "description": None},
                                              {"name": "data9", "type": "string", "alias": "", "description": None}],
                                   "mode": None, "primaryKeys": None, "resource": None, "projectEntity": None,
                                   "newest": 1, "isHide": 0, "expiredPeriod": 0}, "storage": "REDIS",
                        "expiredPeriod": 0,
                        "storageConfigurations": {"url": "info4:6379", "keyColumn": "", "password": "", "table": "ssss",
                                                  "columnsItems": 0}, "sliceTime": "", "sliceType": "H", "owner": None,
                        "schemaVersion": 1, "clusterId": "", "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"],
                        "schemaId": schema_id}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        elif 'lq_dataset_kafka' == data.split("&")[0]:
            new_data = {"name": "lq_dataset_kafka_随机数", "description": "", "resource": {"id": dataset_info[0]["id"]},"resourceId":dataset_info[0]["id"],
                        "schema": {"id": schema_id, "tenantId": dataset_info[0]["tenant_id"], "groupCount": None,
                                   "groupFieldValue": None, "name": "click", "creator": "admin",
                                   "createTime": "2021-06-07 12:10:56", "lastModifier": "admin",
                                   "lastModifiedTime": "2021-06-07 12:10:56", "version": 1, "enabled": 1,
                                   "description": "", "resourceId": schema_resourceid,
                                   "fields": [{"name": "field_1", "type": "string", "alias": "", "description": ""},
                                              {"name": "userId", "type": "int", "alias": "", "description": ""},
                                              {"name": "username", "type": "string", "alias": "", "description": ""},
                                              {"name": "url", "type": "string", "alias": "", "description": ""},
                                              {"name": "clickTime", "type": "string", "alias": "", "description": ""},
                                              {"name": "user_rank", "type": "string", "alias": "", "description": ""},
                                              {"name": "uuid", "type": "string", "alias": "", "description": ""},
                                              {"name": "date_str", "type": "string", "alias": "", "description": ""},
                                              {"name": "time_str", "type": "string", "alias": "", "description": ""}],
                                   "mode": None, "primaryKeys": None, "resource": None, "projectEntity": None,
                                   "oid": "851433040017096704", "newest": 1, "isHide": 0, "expiredPeriod": 0},
                        "storage": "KAFKA", "expiredPeriod": 0,
                        "storageConfigurations": {"expiredTime": 0, "time": "", "format": "csv",
                                                  "zookeeper": "info1:2181,info3:2181,info2:2181/info2_kafka",
                                                  "brokers": "info2:9094", "topic": "flink_1217",
                                                  "groupId": "flink_1217", "version": "", "reader": "",
                                                  "separator": ",", "quoteChar": "\"", "escapeChar": "\\",
                                                  "isKerberosSupport": False, "encryptKey": "",
                                                  "encryptColumnsTemplate": [], "clusterId": "", "encryptColumns": ""},
                        "sliceTime": "", "sliceType": "H", "owner": "", "schemaVersion": 1, "timestampAsOf": "",
                        "specialField": {}, "schemaId": schema_id, "oid": "851433040017096704"}
            new_data["schema"]["fields"].append(schema_info)
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("dataset_data查询出错{}".format(e))
        return


def query_dataset(data):
    try:
        sql = "select id from merce_resource_dir where creator='admin' and name='Datasets' and parent_id is NULL"
        Datasets = ms.ExecuQuery(sql.encode('utf-8'))
        resource_id=[]
        resource_id.append(Datasets[0]["id"])
        if 'name' == data:
            new_data = {"fieldGroup":{"type":"FieldGroup","group":True,"andOr":"AND","fields":[{"type":"Field","group":False,"andOr":"AND","name":"name","oper":"LIKE","value":["%multi%"]},{"type":"Field","group":False,"andOr":"AND","name":"parentId","oper":"EQUAL","value":resource_id}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":20,"pageable":True}}
            return new_data
        elif 'lastModifiedTime' == data:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"GREATER_THAN","fieldValue":1635696000000},{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"LESS_THAN","fieldValue":1704038399000},{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":Datasets[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif 'tags' == data:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"tags","comparatorOperator":"LIKE","fieldValue":"%mysql%"},{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":Datasets[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif 'name_lastModifiedTime' == data:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"name","comparatorOperator":"LIKE","fieldValue":"%gjb%"},{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"GREATER_THAN","fieldValue":1638201600000},{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"LESS_THAN","fieldValue":1704038399000},{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":Datasets[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        else:
            return
    except Exception as e:
        log.error("dss_data查询出错{}".format(e))

def upddataset_data(data):
    try:
        data = data.split("&")
        sql = "select id,owner,name,tenant_id,resource_id from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data[0]
        dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
        dataset_id = dataset_info[0]["id"]
        schema_id, schema_resourceid,schema_name,schema_info,schema_hisid = schema_data(data[1])
        if 'gjb_ttest_hdfs' in data:
            new_datas = {"id": dataset_id, "name": dataset_info[0]["name"], "alias": None, "description": "",
                         "resource": None,
                         "schema": {"tenantId": dataset_info[0]["tenant_id"], "owner": dataset_info[0]["owner"],
                                    "name": schema_name, "enabled": 1, "creator": "admin", "createTime": data_now(),
                                    "lastModifier": "admin", "lastModifiedTime": data_now(), "id": schema_id,
                                    "version": 2, "groupCount": None, "groupFieldValue": None,
                                    "resourceId": schema_resourceid, "fields": [
                                 {"name": "id", "type": "int", "alias": "", "description": "", "fieldCategory": None,
                                  "specId": None}, {"name": "ts", "type": "timestamp", "alias": "", "description": "",
                                                    "fieldCategory": None, "specId": None},
                                 {"name": "code", "type": "string", "alias": "", "description": "",
                                  "fieldCategory": None, "specId": None},
                                 {"name": "total", "type": "float", "alias": "", "description": "",
                                  "fieldCategory": None, "specId": None},
                                 {"name": "forward_total", "type": "float", "alias": "", "description": "",
                                  "fieldCategory": None, "specId": None},
                                 {"name": "reverse_total", "type": "float", "alias": "", "description": "",
                                  "fieldCategory": None, "specId": None},
                                 {"name": "sum_flow", "type": "float", "alias": "", "description": "",
                                  "fieldCategory": None, "specId": None}], "primaryKeys": None, "resource": None,
                                    "oid": "966718205856841728", "newest": 0, "isHide": 0, "tags": None,
                                    "description": None, "expiredPeriod": 0}, "storage": "HDFS", "expiredPeriod": 0,
                         "tags": [], "source": None,
                         "storageConfigurations": {"flatten": True, "expiredTime": 0, "time": "s", "format": "csv",
                                                   "path": "/auto_test/api", "relativePath": "/auto_test/api",
                                                   "pathMode": "exact", "header": "false", "ignoreRow": "0",
                                                   "separator": ",", "quoteChar": "\"", "escapeChar": "\\",
                                                   "encryptKey": "", "encryptColumnsTemplate": [], "encryptColumns": "",
                                                   "hdfsPartitionColumn": "", "specialFieldSelect": "",
                                                   "partitionType": "DateFormat", "dateFormat": "", "dateFrom": "",
                                                   "dateTo": "", "cycle": "", "datePeriod": "", "dateUnit": "HOUR",
                                                   "clusterId": "cluster1", "partitionList": "",
                                                   "pathFilter": "(.inprogress|.pending)", "csv": "csv"},
                         "sliceTime": "", "sliceType": "H", "owner": dataset_info[0]["owner"], "schemaVersion": 1,
                         "timestampAsOf": "",
                         "specialField": {"name": None, "type": None, "alias": None, "description": None,
                                          "currVal": None}, "tenantId": dataset_info[0]["tenant_id"], "enabled": 1,
                         "creator": "admin", "createTime": data_now(), "lastModifier": "admin",
                         "lastModifiedTime": data_now(), "version": 1, "groupCount": None, "groupFieldValue": None,
                         "resourceId": dataset_info[0]["resource_id"], "schemaId": schema_id, "recordNumber": 0,
                         "byteSize": 0, "type": "NORMAL", "analysisTime": 0, "formatConfigurations": None,
                         "isRelated": 1, "isHide": 0, "datasourceId": None, "datasourceName": None, "oid": None}
            return dataset_id, new_datas
        else:
            return
    except Exception as e:
        log.error("upddataset_data查询出错{}".format(e))
        return


def get_dataset_data(data):
    try:
        if '&' in data:
            data = data.split("&")
            sql = "select id,name,resource_id from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data[0]
            dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
            dataset_id = dataset_info[0]["id"]
            dataset_name = dataset_info[0]["name"]
            resource_id = dataset_info[0]["resource_id"]
            return dataset_id, dataset_name, resource_id
        else:
            sql = "select id,name,resource_id from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data
            dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
            dataset_id = dataset_info[0]["id"]
            dataset_name = dataset_info[0]["name"]
            resource_id = dataset_info[0]["resource_id"]
            return dataset_id, dataset_name, resource_id
    except Exception as e:
        log.error("get_dataset_data查询出错{}".format(e))
        return

def schema_data(data):
    try:
        if '&' in data:
            data = data.split('&')
            sql = "select ms.id,ms.name,ms.resource_id,msh.schema_info,msh.id as hisid from merce_schema ms inner join merce_schema_history msh on msh.schema_id =ms.id where ms.name like '%s%%%%' limit 1"  % data[1]
            schema_info = ms.ExecuQuery(sql.encode('utf-8'))
            return schema_info[0]["id"], schema_info[0]["resource_id"], schema_info[0]["name"], schema_info[0]["schema_info"], str(schema_info[0]["hisid"])
        else:
            sql = "select ms.id,ms.name,ms.resource_id,msh.schema_info,msh.id as hisid from merce_schema ms inner join merce_schema_history msh on msh.schema_id =ms.id where ms.name like '%s%%%%' limit 1" % data
            schema_info = ms.ExecuQuery(sql.encode('utf-8'))
            return schema_info[0]["id"], schema_info[0]["resource_id"], schema_info[0]["name"], schema_info[0]["schema_info"], str(schema_info[0]["hisid"])
    except Exception as e:
        log.error("schema_data查询出错{}".format(e))

def schema_data_sink(data):
    try:
        if '&' in data:
            data = data.split('&')
            sql = "select id,name,resource_id from merce_schema where name like '%s%%%%' ORDER BY create_time limit 1" % data[2]
            schema_info = ms.ExecuQuery(sql.encode('utf-8'))
            return schema_info[0]["id"], schema_info[0]["resource_id"], schema_info[0]["name"]
        else:
            sql = "select id,name,resource_id from merce_schema where name like '%s%%%%' ORDER BY create_time limit 1" % data[2]
            schema_info = ms.ExecuQuery(sql.encode('utf-8'))
            return schema_info[0]["id"], schema_info[0]["resource_id"], schema_info[0]["name"]
    except Exception as e:
        log.error("schema_data查询出错{}".format(e))


def create_schema_data(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id,tenant_id from merce_resource_dir where creator='admin' and name='Schemas' and parent_id is NULL"
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'gtest_mysql_0428_training' in data:
            new_data = {"id": "", "name": "gtest_mysql_0428_training_随机数", "alias": "",
                        "description": "gtest_mysql_0428_training_随机数",
                        "fields": [{"name": "id", "type": "int", "alias": "", "description": ""},
                                   {"name": "ts", "type": "timestamp", "alias": "", "description": ""},
                                   {"name": "code", "type": "string", "alias": "", "description": ""},
                                   {"name": "total", "type": "float", "alias": "", "description": ""},
                                   {"name": "forward_total", "type": "float", "alias": "", "description": ""},
                                   {"name": "reverse_total", "type": "float", "alias": "", "description": ""},
                                   {"name": "sum_flow", "type": "float", "alias": "", "description": ""}], "owner": "",
                        "tenantId": resource_info[0]["tenant_id"], "creator": "admin", "lastModifier": "admin",
                        "version": 1, "enabled": 1, "resourceId": resource_info[0]["id"], "oid": "", "newest": 1,
                        "isHide": 0, "expiredPeriod": 0, "resource": {"id": resource_info[0]["id"]}}
            deal_random(new_data)
            return new_data
        if 'lq_schema_hdfs' in data:
            new_data = {"tenantId": resource_info[0]["tenant_id"], "name": "lq_schema_hdfs_随机数",
                        "resourceId": resource_info[0]["id"],
                        "fields": [{"name": "Name", "type": "string", "alias": "", "types": "string"},
                                   {"name": "Sex", "type": "string", "alias": "", "types": "string"},
                                   {"name": "Age", "type": "int", "alias": "", "types": "int"},
                                   {"name": "Identity_code", "type": "string", "alias": "", "types": "string"},
                                   {"name": "C_time", "type": "string", "alias": "", "types": "string"},
                                   {"name": "Data_long", "type": "bigint", "alias": "", "types": "bigint"},
                                   {"name": "Data_double", "type": "double", "alias": "", "types": "double"},
                                   {"name": "Data_boolean", "type": "boolean", "alias": "", "types": "boolean"},
                                   {"name": "time_col", "type": "timestamp", "alias": "", "types": "timestamp"},
                                   {"name": "Str_time", "type": "bigint", "alias": "", "types": "bigint"},
                                   {"name": "Salary", "type": "string", "alias": "", "types": "string"},
                                   {"name": "None_data", "type": "string", "alias": "", "types": "string"},
                                   {"name": "City", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data1", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data2", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data3", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data4", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data5", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data6", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data7", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data8", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data9", "type": "string", "alias": "", "types": "string"}],
                        "resource": {"id": resource_info[0]["id"]}}
            deal_random(new_data)
            return new_data
        if 'lq_sink_schema_hdfs' in data:
            new_data = {"tenantId": resource_info[0]["tenant_id"], "name": "lq_sink_schema_hdfs_随机数",
                        "resourceId": resource_info[0]["id"],
                        "fields": [{"name": "Name", "type": "string", "alias": "", "types": "string"},
                                   {"name": "Sex", "type": "string", "alias": "", "types": "string"},
                                   {"name": "Age", "type": "int", "alias": "", "types": "int"},
                                   {"name": "Identity_code", "type": "string", "alias": "", "types": "string"},
                                   {"name": "C_time", "type": "string", "alias": "", "types": "string"},
                                   {"name": "Data_long", "type": "bigint", "alias": "", "types": "bigint"},
                                   {"name": "Data_double", "type": "double", "alias": "", "types": "double"},
                                   {"name": "Data_boolean", "type": "boolean", "alias": "", "types": "boolean"},
                                   {"name": "time_col", "type": "timestamp", "alias": "", "types": "timestamp"},
                                   {"name": "Str_time", "type": "bigint", "alias": "", "types": "bigint"},
                                   {"name": "Salary", "type": "string", "alias": "", "types": "string"},
                                   {"name": "None_data", "type": "string", "alias": "", "types": "string"},
                                   {"name": "City", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data1", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data2", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data3", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data4", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data5", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data6", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data7", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data8", "type": "string", "alias": "", "types": "string"},
                                   {"name": "data9", "type": "string", "alias": "", "types": "string"}],
                        "resource": {"id": resource_info[0]["id"]}}
            deal_random(new_data)
            return new_data
        if 'lq_sink_schema_hive' in data:
            new_data = {"name": "lq_sink_schema_hive_随机数", "alias": "", "description": "",
                        "fields": [{"name": "Name", "type": "string", "alias": "", "description": ""},
                                   {"name": "Sex", "type": "string", "alias": "", "description": ""},
                                   {"name": "Age", "type": "int", "alias": "", "description": ""},
                                   {"name": "Identity_code", "type": "string", "alias": "", "description": ""},
                                   {"name": "C_time", "type": "string", "alias": "", "description": ""},
                                   {"name": "Data_long", "type": "bigint", "alias": "", "description": ""},
                                   {"name": "Data_double", "type": "double", "alias": "", "description": ""},
                                   {"name": "Data_boolean", "type": "boolean", "alias": "", "description": ""},
                                   {"name": "time_col", "type": "timestamp", "alias": "", "description": ""},
                                   {"name": "Str_time", "type": "bigint", "alias": "", "description": ""},
                                   {"name": "Salary", "type": "string", "alias": "", "description": ""},
                                   {"name": "None_data", "type": "string", "alias": "", "description": ""},
                                   {"name": "City", "type": "string", "alias": "", "description": ""},
                                   {"name": "data1", "type": "string", "alias": "", "description": ""},
                                   {"name": "data2", "type": "string", "alias": "", "description": ""},
                                   {"name": "data3", "type": "string", "alias": "", "description": ""},
                                   {"name": "data4", "type": "string", "alias": "", "description": ""},
                                   {"name": "data5", "type": "string", "alias": "", "description": ""},
                                   {"name": "data6", "type": "string", "alias": "", "description": ""},
                                   {"name": "data7", "type": "string", "alias": "", "description": ""},
                                   {"name": "data8", "type": "string", "alias": "", "description": ""},
                                   {"name": "data9", "type": "string", "alias": "", "description": ""}], "owner": None,
                        "resource": {"id": resource_info[0]["id"]}}
            deal_random(new_data)
            return new_data
        if 'lq_schema_neo4j' in data:
            new_data = {"name": "lq_schema_neo4j_随机数", "alias": "", "description": "",
                        "fields": [{"name": "name", "type": "string", "alias": "", "description": ""},
                                   {"name": "born", "type": "int", "alias": "", "description": ""}],
                        "resource": {"id": resource_info[0]["id"]}}
            deal_random(new_data)
            return new_data
        if 'lq_schema_jdbc' in data:
            new_data = {"name": "lq_schema_jdbc_随机数", "alias": "", "description": "",
                        "fields": [{"name": "name", "type": "string", "alias": "", "description": ""},
                                   {"name": "col", "type": "string", "alias": "", "description": ""},
                                   {"name": "age", "type": "int", "alias": "", "description": ""},
                                   {"name": "id", "type": "int", "alias": "", "description": ""}], "owner": None,
                        "resource": {"id": resource_info[0]["id"]}}
            deal_random(new_data)
            return new_data
        if 'lq_sink_schema_jdbc' in data:
            new_data = {"name": "lq_sink_schema_jdbc_随机数", "alias": "", "description": "",
                        "fields": [{"name": "name", "type": "string", "alias": "", "description": ""},
                                   {"name": "col", "type": "string", "alias": "", "description": ""},
                                   {"name": "age", "type": "int", "alias": "", "description": ""},
                                   {"name": "id", "type": "int", "alias": "", "description": ""}], "owner": None,
                        "resource": {"id": resource_info[0]["id"]}}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("create_schema_data查询出错{}".format(e))


def query_schema(data):
    try:
        sql = "select id,tenant_id from merce_resource_dir where creator='admin' and name='Schemas' and parent_id is NULL"
        Schemas = ms.ExecuQuery(sql.encode('utf-8'))
        if 'offset' == data:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":Schemas[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":8,"limit":8}
            return new_data
        elif 'name' in data:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"name","comparatorOperator":"LIKE","fieldValue":"%gjb%"},{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":Schemas[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif 'tags' in data:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"tags","comparatorOperator":"LIKE","fieldValue":"%mysql%"},{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":Schemas[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif 'lastModifiedTime' in data:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"GREATER_THAN","fieldValue":1667145600000},{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"LESS_THAN","fieldValue":1701359999000},{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":Schemas[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        else:
            return
    except Exception as e:
        log.error("create_schema_data查询出错{}".format(e))

def updschema_data(data):
    try:
        sql = "select id,owner,tenant_id,resource_id from merce_schema where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        schema_info = ms.ExecuQuery(sql.encode('utf-8'))
        schema_id = schema_info[0]["id"]
        if 'gtest_mysql_0428_training' in data:
            new_data = {"id": schema_info[0]["id"], "name": "gtest_mysql_0428_training_随机数", "alias": "",
                        "description": "gtest_mysql_0428_training",
                        "fields": [{"name": "id", "type": "int", "alias": "", "description": ""},
                                   {"name": "ts", "type": "timestamp", "alias": "", "description": ""},
                                   {"name": "code", "type": "string", "alias": "", "description": ""},
                                   {"name": "total", "type": "float", "alias": "", "description": ""},
                                   {"name": "forward_total", "type": "float", "alias": "", "description": ""},
                                   {"name": "reverse_total", "type": "float", "alias": "", "description": ""},
                                   {"name": "sum_flow", "type": "float", "alias": "", "description": ""}],
                        "owner": schema_info[0]["owner"], "tenantId": schema_info[0]["tenant_id"], "creator": "admin",
                        "createTime": 1587370296000, "lastModifier": "admin", "lastModifiedTime": 1603140944000,
                        "version": 1, "enabled": 1, "resourceId": schema_info[0]["resource_id"],
                        "oid": schema_info[0]["id"], "newest": 1, "isHide": 0, "expiredPeriod": 0}
            from new_api_cases.dw_deal_parameters import deal_random
            deal_random(new_data)
            return schema_id, new_data
        else:
            return
    except Exception as e:
        log.error("updschema_data查询出错{}".format(e))
        return

def create_standard_data(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id from merce_resource_dir where creator='admin' and name='Standards' and parent_id is NULL"
        stand_data = ms.ExecuQuery(sql.encode('utf-8'))
        if 'create' == data:
            new_data = {"name":"gjb_standard随机数","type":"standard","description":"","isTree":False,"tags":[],"resourceId":stand_data[0]["id"]}
            deal_random(new_data)
            return new_data
        else:
            new_data = {"name":"gjb_standard随机数","type":"standard","description":"","isTree":False,"tags":[],"resourceId":stand_data[0]["id"]}
            deal_random(new_data)
            return new_data
    except Exception as e:
        log.error("stand_data查询出错{}".format(e))

def stand_data(data):
    try:
        sql = "select id from merce_resource_dir where creator='admin' and name='Standards' and parent_id is NULL"
        stand_data = ms.ExecuQuery(sql.encode('utf-8'))
        if 'all' == data:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":stand_data[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif 'name' == data:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"name","comparatorOperator":"LIKE","fieldValue":"%gjb%"},{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":stand_data[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif 'lastModifiedTime' == data:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"name","comparatorOperator":"LIKE","fieldValue":"%gjb%"},{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"GREATER_THAN","fieldValue":1633017600000},{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"LESS_THAN","fieldValue":1701359999000},{"logicalOperator":"AND","fieldName":"parentId","comparatorOperator":"EQUAL","fieldValue":stand_data[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        else:
            return
    except Exception as e:
        log.error("stand_data查询出错{}".format(e))


def create_flow_data(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id,owner,tenant_id from merce_resource_dir where creator='admin' and name='Flows' and parent_id is NULL"
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'gjb_api_create_flow_dataflow' in data:
            new_data = {"name": "gjb_api_create_flow_dataflow_随机数", "flowType": "dataflow",
                        "resource": {"id": resource_info[0]["id"]}, "steps": [], "tags": [], "links": []}
            deal_random(new_data)
            return new_data
        elif 'gjb_api_create_flow_workflow' in data:
            new_data = {"name": "gjb_api_create_flow_workflow_随机数", "flowType": "workflow",
                        "resource": {"id": resource_info[0]["id"]}, "steps": [], "links": []}
            deal_random(new_data)
            return new_data
        elif 'gjb_api_create_flow_streamflow' in data:
            new_data = {"name": "gjb_api_create_flow_streamflow_随机数", "flowType": "streamflow",
                        "resource": {"id": resource_info[0]["id"]}, "steps": [], "links": []}
            deal_random(new_data)
            return new_data
        elif 'gjb_api_create_flow_rtcflow' in data:
            new_data = {"name": "gjb_api_create_flow_rtcflow_随机数", "flowType": "rtcflow",
                        "resource": {"id": resource_info[0]["id"]}, "steps": [], "links": []}
            deal_random(new_data)
            return new_data
        elif 'gjb_api_create_flow_sparksql' in data:
            new_data = {"name": "gjb_api_create_flow_sparksql_随机数", "flowType": "sparksql",
                        "resource": {"id": resource_info[0]["id"]}, "steps": [], "links": []}
            deal_random(new_data)
            return new_data
        elif 'gjb_api_create_flow_sparkjar' in data:
            new_data = {"name": "gjb_api_create_flow_sparkjar_随机数", "flowType": "sparkjar",
                        "resource": {"id": resource_info[0]["id"]}, "steps": [], "links": []}
            deal_random(new_data)
            return new_data
        elif 'gjb_api_create_flow_flinksql' in data:
            new_data = {"name": "gjb_api_create_flow_flinksql_随机数", "flowType": "flinksql",
                        "resource": {"id": resource_info[0]["id"]}, "steps": [], "links": []}
            deal_random(new_data)
            return new_data
        elif 'gjb_api_create_flow_flinkjar' in data:
            new_data = {"name": "gjb_api_create_flow_flinkjar_随机数", "flowType": "flinkjar",
                        "resource": {"id": resource_info[0]["id"]}, "steps": [], "links": []}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("create_flow_data出错{}".format(e))


def query_flow_data(data):
    try:
        sql = "select id from merce_resource_dir where creator='admin' and name='Flows' and parent_id is NULL"
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'dataflow' in data:
            new_data = {"fieldList": [{"logicalOperator": "AND", "fieldName": "flowType", "comparatorOperator": "EQUAL",
                                       "fieldValue": "dataflow"},
                                      {"logicalOperator": "AND", "fieldName": "parentId", "comparatorOperator": "EQUAL",
                                       "fieldValue": resource_info[0]["id"]}],
                        "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
            return new_data
        elif 'workflow' in data:
            new_data = {"fieldList": [{"logicalOperator": "AND", "fieldName": "flowType", "comparatorOperator": "EQUAL",
                                       "fieldValue": "workflow"},
                                      {"logicalOperator": "AND", "fieldName": "parentId", "comparatorOperator": "EQUAL",
                                       "fieldValue": resource_info[0]["id"]}],
                        "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
            return new_data
        elif 'streamflow' in data:
            new_data = {"fieldList": [{"logicalOperator": "AND", "fieldName": "flowType", "comparatorOperator": "EQUAL",
                                       "fieldValue": "streamflow"},
                                      {"logicalOperator": "AND", "fieldName": "parentId", "comparatorOperator": "EQUAL",
                                       "fieldValue": resource_info[0]["id"]}],
                        "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
            return new_data
        elif 'rtcflow' in data:
            new_data = {"fieldList": [{"logicalOperator": "AND", "fieldName": "flowType", "comparatorOperator": "EQUAL",
                                       "fieldValue": "rtcflow"},
                                      {"logicalOperator": "AND", "fieldName": "parentId", "comparatorOperator": "EQUAL",
                                       "fieldValue": resource_info[0]["id"]}],
                        "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
            return new_data
        elif 'sparksql' in data:
            new_data = {"fieldList": [{"logicalOperator": "AND", "fieldName": "flowType", "comparatorOperator": "EQUAL",
                                       "fieldValue": "sparksql"},
                                      {"logicalOperator": "AND", "fieldName": "parentId", "comparatorOperator": "EQUAL",
                                       "fieldValue": resource_info[0]["id"]}],
                        "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
            return new_data
        elif 'sparkjar' in data:
            new_data = {"fieldList": [{"logicalOperator": "AND", "fieldName": "flowType", "comparatorOperator": "EQUAL",
                                       "fieldValue": "sparkjar"},
                                      {"logicalOperator": "AND", "fieldName": "parentId", "comparatorOperator": "EQUAL",
                                       "fieldValue": resource_info[0]["id"]}],
                        "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
            return new_data
        elif 'flinksql' in data:
            new_data = {"fieldList": [{"logicalOperator": "AND", "fieldName": "flowType", "comparatorOperator": "EQUAL",
                                       "fieldValue": "flinksql"},
                                      {"logicalOperator": "AND", "fieldName": "parentId", "comparatorOperator": "EQUAL",
                                       "fieldValue": resource_info[0]["id"]}],
                        "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
            return new_data
        elif 'flinkjar' in data:
            new_data = {"fieldList": [{"logicalOperator": "AND", "fieldName": "flowType", "comparatorOperator": "EQUAL",
                                       "fieldValue": "flinkjar"},
                                      {"logicalOperator": "AND", "fieldName": "parentId", "comparatorOperator": "EQUAL",
                                       "fieldValue": resource_info[0]["id"]}],
                        "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
            return new_data
        elif 'create_time' in data:
            new_data = {"fieldList": [
                {"logicalOperator": "AND", "fieldName": "lastModifiedTime", "comparatorOperator": "GREATER_THAN",
                 "fieldValue": 1664553600000},
                {"logicalOperator": "AND", "fieldName": "lastModifiedTime", "comparatorOperator": "LESS_THAN",
                 "fieldValue": 1701359999000},
                {"logicalOperator": "AND", "fieldName": "parentId", "comparatorOperator": "EQUAL",
                 "fieldValue": resource_info[0]["id"]}],
                        "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
            return new_data
        elif 'offset' in data:
            new_data = {"fieldList": [{"logicalOperator": "AND", "fieldName": "parentId", "comparatorOperator": "EQUAL",
                                       "fieldValue": resource_info[0]["id"]}],
                        "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 8, "limit": 8}
            return new_data
        else:
            new_data = {"fieldList": [
                {"logicalOperator": "AND", "fieldName": "name", "comparatorOperator": "LIKE", "fieldValue": "%gjb%"},
                {"logicalOperator": "AND", "fieldName": "parentId", "comparatorOperator": "EQUAL",
                 "fieldValue": resource_info[0]["id"]}],
                        "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
            return new_data
    except Exception as e:
        log.error("create_flow_data出错{}".format(e))


def flow_data(data):
    try:
        if '&' in data:
            data = data.split('&')
            sql = "select id,name from merce_flow where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
            flow_info = ms.ExecuQuery(sql.encode('utf-8'))
            return flow_info[0]["id"], flow_info[0]["name"]
        else:
            sql = "select id,name from merce_flow where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
            flow_info = ms.ExecuQuery(sql.encode('utf-8'))
            return flow_info[0]["id"], flow_info[0]["name"]
    except Exception as e:
        log.error("flow_data出错{}".format(e))

def update_flow_data(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id,owner,tenant_id from merce_resource_dir where creator='admin' and name='Flows' and parent_id is NULL"
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        schema_id, schema_resourceid,schema_name,schema_info,schema_hisid = schema_data(data)
        flow_id, flow_name = flow_data(data)
        dataset_id, dataset_name = flow_dataset_data(data)
        if 'gjb_api_create_flow_dataflow' in data:
            new_data = {"steps": [{"id": "source_1", "name": "source_1", "type": "source",
                                   "otherConfigurations": {"schema": "pivot", "dataset-paths": "",
                                                           "schemaId": schema_id, "sessionCache": "", "interceptor": "",
                                                           "dataset": [{"rule": "set_1", "dataset": dataset_name,
                                                                        "ignoreMissingPath": "false",
                                                                        "datasetId": dataset_id, "storage": "HDFS"}]},
                                   "outputConfigurations": {
                                       "output": [{"name": "Name", "alias": ""}, {"name": "name01", "alias": ""}]},
                                   "x": 355, "y": 113, "uiConfigurations": {"output": ["output"]}},
                                  {"id": "sink_1", "name": "sink_1", "type": "sink",
                                   "otherConfigurations": {"schema": "test_pivot", "description": "", "outputMode": "",
                                                           "type": "HDFS", "autoSchema": "true", "NoneValue": "",
                                                           "mode": "append", "path": "/auto_test/gjb/pivot/",
                                                           "isDisable": "false", "countWrittenRecord": "false",
                                                           "datasetId": "", "dataResource": "", "schedulerUnit": "",
                                                           "quoteChar": "\"", "escapeChar": "\\", "schemaResource": "",
                                                           "schemaVersion": "1", "expiredTemp": "",
                                                           "sliceTimeColumn": "", "format": "csv", "trigger": "",
                                                           "maxFileSize": "", "maxFileNumber": "", "separator": ",",
                                                           "expiredTime": "", "schedulerVal": "",
                                                           "checkpointLocation": "", "schemaId": schema_id, "time": "s",
                                                           "dataset": "test_pivot", "sliceType": "H", "idColumn": ""},
                                   "inputConfigurations": {
                                       "input": [{"name": "Name", "alias": ""}, {"name": "name01", "alias": ""}]},
                                   "outputConfigurations": {}, "x": 916, "y": 135,
                                   "uiConfigurations": {"input": ["input"]}}], "links": [
                {"target": "sink_1", "source": "source_1", "sourceOutput": "output", "targetInput": "input",
                 "linkStrategy": ""}], "id": flow_id, "name": flow_name, "flowType": "dataflow", "oid": "$None",
                        "creator": "admin", "createTime": 1603187474000, "lastModifier": "admin",
                        "lastModifiedTime": 1603189710000, "owner": resource_info[0]["owner"], "version": 1,
                        "enabled": 1, "tenantId": resource_info[0]["tenant_id"], "resourceId": resource_info[0]["id"],
                        "isHide": 0, "parameters": [], "expiredPeriod": 0}
            deal_random(new_data)
            return flow_id, new_data
        else:
            return
    except Exception as e:
        log.error("update_flow_data出错{}".format(e))


def flow_dataset_data(data):
    try:
        if '&' in data:
            data = data.split("&")
            sql = "select id,name from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data[0]
            dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
            dataset_id = dataset_info[0]["id"]
            dataset_name = dataset_info[0]["name"]
            return dataset_id, dataset_name
        else:
            sql = "select id,name from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data[2]
            dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
            dataset_id = dataset_info[0]["id"]
            dataset_name = dataset_info[0]["name"]
            return dataset_id, dataset_name
    except Exception as e:
        log.error("flow_dataset_data出错{}".format(e))

def flow_dataset_data_sink(data):
    try:
        if '&' in data:
            data = data.split("&")
            sql = "select id,name from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data[3]
            dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
            dataset_id = dataset_info[0]["id"]
            dataset_name = dataset_info[0]["name"]
            return dataset_id, dataset_name
        else:
            sql = "select id,name from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data[2]
            dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
            dataset_id = dataset_info[0]["id"]
            dataset_name = dataset_info[0]["name"]
            return dataset_id, dataset_name
    except Exception as e:
        log.error("flow_dataset_data出错{}".format(e))

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


def get_old_id_name(data):
    try:
        sql= "select id,name from merce_flow where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_id=ms.ExecuQuery(sql)[0]["id"]
        old_name=ms.ExecuQuery(sql)[0]["name"]
        new_data = {
            "configurations": {"startTime": "当前时间戳", "arguments": [], "dependencies": [], "extraConfigurations": {},
                               "properties": [{"name": "all.debug", "value": "false", "input": "false"},
                                              {"name": "all.dataset-Noneable", "value": "false", "input": "false"},
                                              {"name": "all.optimized.enable", "value": "true", "input": "true"},
                                              {"name": "all.lineage.enable", "value": "false", "input": "false"},
                                              {"name": "all.debug-rows", "value": "20", "input": "20"},
                                              {"name": "all.runtime.cluster-id", "value": "cluster1",
                                               "input": ["cluster1"]},
                                              {"name": "dataflow.master", "value": "yarn", "input": "yarn"},
                                              {"name": "dataflow.deploy-mode", "value": "cluster",
                                               "input": ["client", "cluster"]},
                                              {"name": "dataflow.queue", "value": "default", "input": ["default"]},
                                              {"name": "dataflow.num-executors", "value": "2", "input": "2"},
                                              {"name": "dataflow.driver-memory", "value": "512M", "input": "512M"},
                                              {"name": "dataflow.executor-memory", "value": "1G", "input": "1G"},
                                              {"name": "dataflow.executor-cores", "value": "2", "input": "2"},
                                              {"name": "dataflow.verbose", "value": "true", "input": "true"},
                                              {"name": "dataflow.local-dirs", "value": "", "input": ""},
                                              {"name": "dataflow.sink.concat-files", "value": "true", "input": "true"},
                                              {"name": "dataflow.tempDirectory", "value": "/tmp/dataflow/spark",
                                               "input": "/tmp/dataflow/spark"}],
                               "retry": {"enable": False, "limit": 1, "timeInterval": 1, "intervalUnit": "MINUTES"}},
            "schedulerId": "once", "ource": "rhinos", "version": 0, "flowId": flow_id, "flowType": "dataflow",
            "name": data + "_随机数", "creator": "admin", "oldName": old_name}
        return deal_random(new_data)
    except Exception as e:
        log.error("get_old_name执行出错{}".format(e))
        return


def get_collector_data(data):
    try:
        sql= "select name from sync_job where name like 'collector_ftp%s' order by create_time desc limit 1"
        name=ms.ExecuQuery(sql)[0]["name"]
        if 'collector_ftp' in data:
            new_data = {"name": name,
                        "dataSource": {"id": "570f7cab-ccbe-4798-8854-dfe427062b14", "name": "collector_ftp",
                                       "schemaName": "collector_schema", "url": "", "dbType": "", "tableExt": "",
                                       "fetchSize": 0, "queryTimeout": 0, "collection": "", "condition": "",
                                       "filename": ".*", "regex": "", "table": "", "referenceName": "ftp_source",
                                       "type": "FTP", "properties": {},
                                       "column": [{"name": "userId", "type": "int", "alias": "", "description": ""},
                                                  {"name": "username", "type": "string", "alias": "",
                                                   "description": ""},
                                                  {"name": "url", "type": "string", "alias": "", "description": ""},
                                                  {"name": "clickTime", "type": "string", "alias": "",
                                                   "description": ""}], "host": "192.168.1.84", "port": 21,
                                       "username": "europa", "password": "AES(11b5a9d816c0a4fd8f99ef1e7de42d32)",
                                       "fieldsSeparator": ",", "dir": "shiy/flink/data",
                                       "schemaId": "be3d5b05-5928-4fe7-8d60-41d11100f798", "recursive": True,
                                       "secure": False, "skipHeader": False, "object": "shiy/flink/data",
                                       "readerName": "ftp"}, "dimensions": [
                    {"type": "REDIS", "id": "be90a847-4005-467e-87eb-0ce985ca73a7", "name": "sink_rediss",
                     "referenceName": "REDIS_LOOKUP",
                     "column": [{"name": "userId", "type": "int", "alias": "", "description": ""},
                                {"name": "username", "type": "string", "alias": "", "description": ""},
                                {"name": "url", "type": "string", "alias": "", "description": ""},
                                {"name": "clickTime", "type": "string", "alias": "", "description": ""}],
                     "url": "info4:6379", "table": "sink_redis423", "keyColumn": "userId", "password": "",
                     "database": 0, "timeout": 2000, "maxTotal": 8, "maxIdle": 8, "minIdle": 0, "maxRedirections": 5,
                     "readerName": "redis"}], "schemaId": "be3d5b05-5928-4fe7-8d60-41d11100f798", "dataStores": [
                    {"type": "HDFS", "id": "b8bbd172-0235-467a-a637-f685f58bf547", "name": "collector_sink",
                     "referenceName": "test",
                     "column": [{"name": "userId", "type": "int", "alias": "", "description": ""},
                                {"name": "username", "type": "string", "alias": "", "description": ""},
                                {"name": "url", "type": "string", "alias": "", "description": ""},
                                {"name": "clickTime", "type": "string", "alias": "", "description": ""}],
                     "encryptKey": "", "encryptColumns": "", "path": "/tmp/lisatest/collector_sink", "format": "csv",
                     "separator": ",", "mode": "overwrite", "sliceFormat": "", "clusterId": ""}], "transform": {
                    "sql": "select t1.userId,t1.username,t1.url,t1.username || '#' || t1.url as col1, SUBSTRING(t1.username, 3) as col2,t2.userId as t2_userId,t2.username as t2_username,t2.url as t2_url,t2.clickTime as t2_clickTime from ftp_source t1, LATERAL TABLE(REDIS_LOOKUP(CAST(userId as varchar))) as t2(userId,username,url,clickTime)"},
                        "type": "SyncDataTask", "parallelism": 1, "trigger": "", "cursorCol": "", "errorNumber": 0,
                        "partitionKey": "", "stopOnSchemaChanged": "false", "partitionPattern": "",
                        "opts": "-Xss256k -Xms1G -Xmx1G -Xmn512M", "collecterId": "WOVEN-SERVER", "bufferSize": 5000,
                        "flushPaddingTime": 30000, "restore": False, "restoreColumn": "", "checkpointPath": "",
                        "mode": "", "setCron": "", "testText": "", "createTime": 0, "status": 0,
                        "taskType": "SYNC_DATA", "async": False, "exclusive": False, "syncType": "data",
                        "serviceType": "DTS"}
            return new_data
    except Exception as e:
        log.error("get_collector_data执行出错{}".format(e))

name=None

def tag_data(dataType,id):
    try:
        sql= "select name from merce_tag where name like \'lq_"+dataType+"_tag%\' order by last_modified_time limit 1"
        name=ms.ExecuQuery(sql)[0]["name"]
    except Exception as e:
        log.error("tag_data执行出错{}".format(e))
        return
    return get_tag_data(name,dataType,id)
def get_tag_data(name,types,id):
    return {"name":name,"description":"","tagType":types,"color":"#781DA0","tenantId":tenant_id,"owner":owner,"enabled":1,"creator":"admin","createTime":"2021-07-21 12:09:55","lastModifier":"admin","lastModifiedTime":"2021-07-21 12:09:55","id":id,"parentId":None,"children":None,"selfCode":id,"parentCode":None,"hasChildren":False}

def update_user(data):
    try:
        sql = "select id from merce_user where  name like '%s%%%%' order by create_time desc limit 1" % data
        user_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'dsp' in data:
            new_data = {"name":"dsp随机数","loginId":"dsp随机数","phone":"15801232688","email":"15801232688@139.com","id":user_info[0]["id"],"resourceQueues":["default"],"disable":"true"}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_role(data):
    try:
        sql = "select id,name from merce_role where name like '%s%%%%' order by create_time desc limit 1" % data
        role_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'dsp' in data:
            new_data = {"name":"dsp随机数","permissions":[],"id":role_info[0]["id"]}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def enable_role(data):
    try:
        sql = "select enabled,id from merce_role where name like '%s%%%%' order by create_time desc limit 1" % data
        role_info = ms.ExecuQuery(sql.encode('utf-8'))
        ids=[]
        ids.append(str(role_info[0]["id"]))
        if role_info[0]["enabled"]==1:
            new_data = {"enabled":0,"ids":ids}
            return new_data
        elif role_info[0]["enabled"]==0:
            new_data = {"enabled":1,"ids":ids}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def enable_user(data):
    try:
        sql = "select enabled,id from merce_user where name like '%s%%%%' order by create_time desc limit 1" % data
        user_info = ms.ExecuQuery(sql.encode('utf-8'))
        ids=[]
        ids.append(str(user_info[0]["id"]))
        if user_info[0]["enabled"]==1:
            new_data = {"enabled":0,"ids":ids}
            return new_data
        elif user_info[0]["enabled"]==0:
            new_data = {"enabled":1,"ids":ids}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def set_user_role(data):
    try:
        user,role=[],[]
        user_sql = "select id from merce_user where name like '%s%%%%' order by create_time desc limit 1" % data
        user_info = ms.ExecuQuery(user_sql.encode('utf-8'))
        user.append(str(user_info[0]["id"]))
        sql = "select id from merce_role where name like '%s%%%%' order by create_time desc limit 1" % data
        role_info = ms.ExecuQuery(sql.encode('utf-8'))
        role.append(str(role_info[0]["id"]))
        user.append(role)
        data={"accountExpiredTime":"2022-07-21","id":user_info[0]["id"],"pwdExpiredTime":"2022-04-21"}
        user.append(data)
        return user
    except Exception as e:
        log.error("异常信息：%s" %e)

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


def get_improt_dataflow(headers, HOST, flag):
    """
    返回导入dataflow文件的请求体参数
    :param headers:
    :param HOST:
    :return:
    """
    url = '%s/api/mis/upload' % HOST
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
        new_data = {"cfd": cdf_list, "cds": cds_list, "csm": csm_list, "tag":[], "uploadDirectory": res["uploadDir"],"overWrite":True,"flowResourceId":"","datasetResourceId":"","schemaResourceId":""}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)