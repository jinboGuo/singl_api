# coding:utf-8
import os
import time
from urllib import parse
import requests
from basic_info.get_auth_token import get_headers, get_headers_admin, get_headers_customer
from new_api_cases.dw_deal_parameters import deal_parameters
from util.format_res import dict_res
from basic_info.setting import Dw_MySQL_CONFIG
from util.Open_DB import MYSQL
from basic_info.setting import host
from selenium import webdriver
import random

from util.timestamp_13 import get_now, get_tomorrow

ms = MYSQL(Dw_MySQL_CONFIG["HOST"], Dw_MySQL_CONFIG["USER"], Dw_MySQL_CONFIG["PASSWORD"], Dw_MySQL_CONFIG["DB"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))


def query_subject_data(data):
    try:
        sql = "select id from dw_business where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        business_id = flow_info[0]["id"]
        print('business-id:', business_id)
    except :
        return "722830072351817728"
    new_data = {"params": {"pageable": {"pageNum": 0, "pageSize": 8, "pageable": "true"}}}
    return new_data, business_id

def update_business_data(data):
    try:
        sql = "select id from dw_business where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        business_id = flow_info[0]["id"]
        print('business-id:', business_id)
    except :
        return "722830072351817728", 0
    new_data = {"id": business_id, "tenantId":"e5188f23-d472-4b2d-9cfa-97a0d65994cf","owner":"83f2ad7f-1d9f-4ad0-953f-db8e7d285320","creator":"admin","createTime":"2020-09-23T09:37:24.000+0000","lastModifier":"admin","lastModifiedTime":"2020-09-23T09:37:24.000+0000","name":"api_auto_business随机数","alias":"api_business随机数","abbr":"api_auto_business随机数","description":"api_auto_business","dt":"dt","bizDate":"yyyyMMddHH","flowId":"","flowName":"","schedulerId":"","physicalStatus":"READY","deployStatus":"offline"}
    return new_data, business_id

def add_subject_data(data):

    try:
        sql = "select id from dw_business where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        business_id = flow_info[0]["id"]
        print('business-id:', business_id)
    except :
        return "722830072351817728"
    new_data = {"alias": "api_subject", "abbr": "api_subject随机数", "name": "api_auto_subject随机数", "description":"api_auto_subject","bussinessId": business_id}
    return new_data, business_id

def update_subject_data(data):

    try:
        sql = "select id,business_id from dw_subject where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        subject_id, business_id = flow_info[0]["id"], flow_info[0]["business_id"]
        print('subject-id:', subject_id)
    except :
        return "722830072351817728"
    new_data = {"id": subject_id, "tenantId": "e5188f23-d472-4b2d-9cfa-97a0d65994cf", "owner":"83f2ad7f-1d9f-4ad0-953f-db8e7d285320","creator": "admin", "createTime": "2020-09-24T02:43:04.000+0000", "lastModifier": "admin","lastModifiedTime":"2020-09-24T02:43:04.000+0000","name":"api_auto_subject随机数","alias":"api_subject随机数","abbr":"api_subject随机数", "businessId": business_id, "parentId":"0","description":"api_auto_subject","children":[],"selfCode":"758639635533398016","parentCode":"0"}
    return new_data, subject_id

def add_projects_data(data):

    try:
        sql = "select id from dw_business where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        business_id = flow_info[0]["id"]
        print('business-id:', business_id)
    except :
        return "722830072351817728"
    new_data = {"alias": "api_projects", "name": "api_auto_projects随机数", "type": "", "description": "api_auto_projects","businessId": business_id, "abbr": "api_projects随机数"}
    return new_data, business_id

def update_projects_data(data):

    try:
        sql = "select id,business_id from dw_project where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        project_id, business_id = flow_info[0]["id"], flow_info[0]["business_id"]
        print('project-id:', project_id)
    except :
        return "722830072351817728"
    new_data = {"id": project_id, "tenantId": "e5188f23-d472-4b2d-9cfa-97a0d65994cf", "owner":"83f2ad7f-1d9f-4ad0-953f-db8e7d285320","creator": "admin", "createTime": "2020-09-24T05:52:09.000+0000", "lastModifier": "admin","lastModifiedTime": "2020-09-24T05:52:09.000+0000", "name": "api_auto_projects随机数", "alias": "api_projects", "abbr": "api_projects随机数", "businessId": business_id, "description": "api_auto_projects"}
    return new_data, project_id

def update_tag_data(data):

    try:
        sql = "select id from dw_tagdef where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        tag_id = flow_info[0]["id"]
        print('tag-id:', tag_id)
    except :
        return "722830072351817728"
    new_data = {"id": tag_id, "tenantId": "e5188f23-d472-4b2d-9cfa-97a0d65994cf", "owner":"83f2ad7f-1d9f-4ad0-953f-db8e7d285320", "creator": "admin", "createTime": "2020-09-24T08:21:53.000+0000", "lastModifier": "admin", "lastModifiedTime": "2020-09-24T08:21:53.000+0000","name":"api_auto_tag随机数","alias":"api_tag随机数","abbr":"","parentTagOption":"","options":[{"name":"大","alias":"big","orderNum":""},{"name":"小","alias":"small","orderNum":""},{"name":"长","alias":"long","orderNum":""}],"description":"api_auto_tag","scope":"","isSetName":1}
    return new_data, tag_id

def add_taggroup_data(data):
    try:
        sql = "select id from dw_tagdef where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        tag_id = flow_info[0]["id"]
        print('tag-id:', tag_id)
    except :
        return "722830072351817728"
    new_data = {"alias": "api_taggroup随机数", "createTime": "", "description": "api_auto_taggroup", "tagIds": tag_id, "name": "api_auto_taggroup随机数"}
    return new_data

def update_taggroup_data(data):

    try:
        sql = "select id,tag_ids from dw_taggroup where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        taggroup_id, tag_id = flow_info[0]["id"], flow_info[0]["tag_ids"]
        print('taggroup-id tag-id:', taggroup_id, tag_id)
    except :
        return "722830072351817728",0
    new_data = {"id": taggroup_id, "tenantId": "e5188f23-d472-4b2d-9cfa-97a0d65994cf", "owner":"83f2ad7f-1d9f-4ad0-953f-db8e7d285320","creator": "admin", "createTime": "2020-09-24T09:39:41.000+0000", "lastModifier": "admin", "lastModifiedTime": "2020-09-24T10:13:34.000+0000","name":"api_auto_taggroup随机数","alias":"api_taggroup随机数", "abbr": "", "description": "", "tagIds": tag_id}
    return new_data, taggroup_id

def update_namerule_data(data):

    try:
        sql = "select id from dw_name_rules where alias like '%s%%%%'  order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        namerule_id = flow_info[0]["id"]
        print('namerule-id:', namerule_id)
    except :
        return "722830072351817728",0
    new_data = {"id": namerule_id, "tenantId":"e5188f23-d472-4b2d-9cfa-97a0d65994cf", "owner":"83f2ad7f-1d9f-4ad0-953f-db8e7d285320","creator":"admin","createTime":"2020-09-25T03:29:45.000+0000","lastModifier":"admin","lastModifiedTime":"2020-09-25T03:44:53.000+0000","name":"metadata_subject","alias":"api_auto_namerule随机数","abbr":"api_rule随机数","description":"","rules":"metadata"}
    return new_data, namerule_id

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

    try:
        sql = "select id from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('service_id:', flow_info[0]["id"])
    except:
        return
    if 'snow_dataset_dsp' in data:
        new_data = {
            "accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1",
            "dataServiceId": flow_info[0]["id"],
            "timestamp": "1594026624140",
            "encrypted": "false",
            "query": {
                "fieldGroup": {
                    "index": 0,
                    "group": "false",
                    "andOr": "AND",
                    "fields": [{
                        "index": 1,
                        "group": "false",
                        "andOr": "AND",
                        "name": "DestCityName",
                        "oper": "LIKE",
                        "value": [
                            '%CA%'
                        ]
                    },
                        {
                            "index": 2,
                            "group": "false",
                            "andOr": "OR",
                            "name": "FlightNum",
                            "oper": "NOT_EQUAL",
                            "value": [
                                '108'
                            ]
                        }],
                    "fieldGroups": []
                },
                "ordSort": [
                    {
                        "name": "FlightNum",
                        "order": "DESC"
                    }],
                "pageable": {
                    "pageable": "true",
                    "pageNum": 1,
                    "pageSize": 91,
                    "offset": "0"
                },
                "selectProperties": [
                    "FlightNum",
                    "TailNum",
                    "OriginState",
                    "DestCityName"
                ]
            }
        }
        return new_data
    elif 'gjb_sink_es' in data:
        new_data = {
            "accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1",
            "dataServiceId": flow_info[0]["id"],
            "timestamp": "1594026624140",
            "encrypted": "false",
            "query": {
                "fieldGroup": {
                    "index": 0,
                    "group": "false",
                    "andOr": "AND",
                    "fields": [

                        {
                            "index": 1,
                            "group": "false",
                            "andOr": "AND",
                            "name": "AGE",
                            "oper": "IN",
                            "value": [
                                2
                            ]
                        }],
                    "fieldGroups": []
                },
                "pageable": {
                    "pageable": "true",
                    "pageNum": 1,
                    "pageSize": 91,
                    "offset": "0"
                },
                "selectProperties": [
                    "TIME",
                    "NAME",
                    "AGE",
                    "GENDER"
                ]
            }
        }
        return new_data
    else:
        return

def pull_Aggs(data):

    try:
        sql = "select id from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('service_id:', flow_info[0]["id"])
    except:
        return
    if 'gjb_sink_es' in data:
        new_data = {
            "accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1",
            "dataServiceId": flow_info[0]["id"],
            "encrypted": "false",
            "query": {
                "aggFields": [{
                    "aggType": "AVG",
                    "alias": "AVG_DestWac",
                    "name": "AGE"
                },
                    {
                        "aggType": "SUM",
                        "alias": "SUM_DestWac",
                        "name": "AGE"
                    },
                    {
                        "aggType": "MIN",
                        "alias": "MIN_DestWac",
                        "name": "AGE"
                    },
                    {
                        "aggType": "MAX",
                        "alias": "MAX_DestWac",
                        "name": "AGE"
                    },
                    {
                        "aggType": "COUNT",
                        "alias": "COUNT_DestWac",
                        "name": "AGE"
                    }
                ],
                "fieldGroup": {
                    "index": 0,
                    "group": "false",
                    "andOr": "AND",
                    "fields": [{
                        "index": 1,
                        "andOr": "AND",
                        "name": "GENDER",
                        "oper": "EQUAL",
                        "value": [
                            "GENDER"
                        ]
                    }]
                },
                "groupFields": [
                    "NAME"
                ],
                "havingFieldGroup": {
                    "index": 0,
                    "andOr": "AND",
                    "fields": [{
                        "index": 0,
                        "andOr": "AND",
                        "name": "MAX_DestWac",
                        "oper": "BETWEEN",
                        "value": [
                            0, 1000000
                        ]
                    }]
                },
                "ordSort": [{
                    "name": "MIN_DestWac"
                }],
                "pageable": {
                    "pageNum": 1,
                    "pageSize": 500,
                    "pageable": "true"
                }
            },
            "timestamp": 1
        }
        return new_data
    elif 'snow_dataset_dsp' in data:
        new_data = {
            "accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1",
            "dataServiceId": flow_info[0]["id"],
            "encrypted": "false",
            "query": {
                "aggFields": [{
                    "aggType": "AVG",
                    "alias": "AVG_DestWac",
                    "distinct": "false",
                    "name": "DestWac"
                },
                    {
                        "aggType": "SUM",
                        "alias": "SUM_DestWac",
                        "name": "DestWac"
                    },
                    {
                        "aggType": "MIN",
                        "alias": "MIN_DestWac",
                        "distinct": "false",
                        "name": "DestWac"
                    },
                    {
                        "aggType": "MAX",
                        "alias": "MAX_DestWac",
                        "name": "DestWac"
                    },
                    {
                        "aggType": "COUNT",
                        "alias": "COUNT_DestWac",
                        "distinct": "false",
                        "name": "DestWac"
                    }
                ],
                "fieldGroup": {
                    "index": 0,
                    "group": "false",
                    "andOr": "AND",
                    "fields": [{
                        "index": 0,
                        "andOr": "AND",
                        "name": "OriginCityName",
                        "oper": "NOT_LIKE",
                        "value": [
                            "%CA%"
                        ]
                    },
                        {
                            "index": 1,
                            "andOr": "AND",
                            "name": "OriginCityName",
                            "oper": "NOT_LIKE",
                            "value": [
                                "%CA%"
                            ]
                        }]
                },
                "groupFields": [
                    "FlightNum",
                    "Year",
                    "Month",
                    "FlightDate"
                ],
                "havingFieldGroup": {
                    "index": 0,
                    "andOr": "AND",
                    "fields": [{
                        "index": 0,
                        "andOr": "AND",
                        "name": "MAX_DestWac",
                        "oper": "BETWEEN",
                        "value": [
                            0, 10000
                        ]
                    }]
                },
                "ordSort": [{
                    "name": "MIN_DestWac"
                },
                    {
                        "name": "COUNT_DestWac",
                        "order": "DESC"
                    },
                    {
                        "name": "FlightNum",
                        "order": "ASC"
                    },
                    {
                        "name": "FlightDate",
                        "order": "ASC"
                    }],
                "pageable": {
                    "pageNum": 1,
                    "pageSize": 500,
                    "pageable": "true"
                }
            },
            "timestamp": 1
        }
        return new_data
    else:
        return

def application_pull_approval(data):
    try:
        sql = "select id from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('dsp_data_application:', flow_info[0]["id"])

        if 'gjb_sink_es' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name": "gjb_sink_es", "status": 0, "type": 0, "auditMind": "yyy"}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name":"snow_dataset_dsp_","status": 0, "type": 0, "auditMind": "yyy"}
            return new_data
        else:
            return
    except:
        return "722842814173413376"
def application_push_approval(data):

    try:
        sql = "select id from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('dsp_data_application:', flow_info[0]["id"])
        if 'test_once_hdfs_csv' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name":"test_once_hdfs_csv","status": 0, "type": 1, "scheduleType": "once", "serviceConfiguration":{"cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
            return new_data
        elif 'test_once_mysql' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description":"", "enabled":1, "expiredTime": "0", "name":"test_once_mysql","status": 0, "type": 1, "scheduleType": "once", "serviceConfiguration":{"cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
            return new_data
        elif 'test_hdfs_txt_event' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name":"test_hdfs_txt_event", "status": 0, "type": 1, "scheduleType": "event", "serviceConfiguration":{"cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
            return new_data
        elif 'test_cron_oracle' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name":"test_cron_oracle", "status": 0, "type": 1, "scheduleType": "cron", "serviceConfiguration":{"cron":"0 0/5 * * * ? ","startTime": get_now(), "endTime": get_tomorrow()}, "fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
            return new_data
        else:
            return
    except:
       return "723150172099444736"
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