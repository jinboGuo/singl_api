# coding:utf-8
import os
import requests
from basic_info.get_auth_token import get_headers
from new_api_cases.dw_deal_parameters import deal_random
from util.format_res import dict_res
from basic_info.setting import dsp_host, ms, log
from util.logs import Logger
from util.timestamp_13 import get_now, get_tomorrow, data_now

ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))


def resource_data_save(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        data = data.split("&")
        dir_sql = "select id from merce_resource_dir where name like '%%%%%s%%%%' ORDER BY create_time desc limit 1" % \
                  data[1]
        dir_info = ms.ExecuQuery(dir_sql.encode('utf-8'))
        sql = "select id,name,resource_id from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % \
              data[0]
        dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'gjb_ttest_hdfs_student2020' in data:
            new_data = {"categoryId": 0, "resourceId": dir_info[0]["id"], "openStatus": 0, "type": 0,
                        "expiredTime": "0", "name": "gjb_ttest_hdfs_student_随机数", "encoder": "UTF-8",
                        "sourceType": "DATASET", "datasetName": dataset_info[0]["name"], "datasourceName": "",
                        "storage": "HDFS", "fieldMappings": [
                    {"sourceField": "age", "sourceType": "int", "targetField": "age", "targetType": "int",
                     "encrypt": "", "index": 0, "transformRule": {"type": "", "expression": ""}},
                    {"sourceField": "sName", "sourceType": "string", "targetField": "sName", "targetType": "string",
                     "encrypt": "", "index": 0, "transformRule": {"type": "", "expression": ""}},
                    {"sourceField": "sId", "sourceType": "string", "targetField": "sId", "targetType": "string",
                     "encrypt": "", "index": 0, "transformRule": {"type": "", "expression": ""}},
                    {"sourceField": "class", "sourceType": "string", "targetField": "class", "targetType": "string",
                     "encrypt": "", "index": 0, "transformRule": {"type": "", "expression": ""}},
                    {"sourceField": "sex", "sourceType": "string", "targetField": "sex", "targetType": "string",
                     "encrypt": "", "index": 0, "transformRule": {"type": "", "expression": ""}}], "timeStamp": "永久有效",
                        "isIncrementField": "true", "incrementField": "age", "query": {"sqlTemplate": ""},
                        "datasetId": dataset_info[0]["id"], "category_id": dataset_info[0]["resource_id"]}
            deal_random(new_data)
            return new_data
        elif 'gjb_sink_es_one' in data:
            new_data = {"categoryId": 0, "resourceId": dir_info[0]["id"], "openStatus": 0, "type": 0,
                        "expiredTime": "0", "name": "gjb_ttest_es_student_随机数", "encoder": "UTF-8",
                        "sourceType": "DATASET", "datasetName": dataset_info[0]["name"], "datasourceName": "",
                        "storage": "ElasticSearch", "fieldMappings": [
                    {"sourceField": "age", "sourceType": "int", "targetField": "age", "targetType": "int",
                     "encrypt": "", "index": 0, "transformRule": {"type": "", "expression": ""}},
                    {"sourceField": "sName", "sourceType": "string", "targetField": "sName", "targetType": "string",
                     "encrypt": "", "index": 0, "transformRule": {"type": "", "expression": ""}},
                    {"sourceField": "sId", "sourceType": "string", "targetField": "sId", "targetType": "string",
                     "encrypt": "", "index": 0, "transformRule": {"type": "", "expression": ""}},
                    {"sourceField": "class", "sourceType": "string", "targetField": "class", "targetType": "string",
                     "encrypt": "", "index": 0, "transformRule": {"type": "", "expression": ""}},
                    {"sourceField": "sex", "sourceType": "string", "targetField": "sex", "targetType": "string",
                     "encrypt": "", "index": 0, "transformRule": {"type": "", "expression": ""}}], "timeStamp": "永久有效",
                        "isIncrementField": "true", "incrementField": "age", "query": {"sqlTemplate": ""},
                        "datasetId": dataset_info[0]["id"], "category_id": dataset_info[0]["resource_id"]}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def resource_data_push(data):
    try:
        data = data.split("&")
        sql = "select name,id,resource_id from dsp_data_resource where name like '%%%%%s%%%%' ORDER BY create_time desc limit 1" % \
              data[0]
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        dss_sql = "select name,id from dsp_cust_data_source where creator='customer3' and name like '%%%%%s%%%%' ORDER BY create_time desc limit 1" % \
                  data[1]
        dss_info = ms.ExecuQuery(dss_sql.encode('utf-8'))
        if 'test_hdfs_csv' in data:
            new_data = {"name": "test_once_hdfs_csv", "resourceId": resource_info[0]["resource_id"], "description": "",
                        "dataResId": resource_info[0]["id"], "dataResName": resource_info[0]["name"], "serviceMode": 0,
                        "transferType": 1, "custTableName": "", "custDataSourceId": dss_info[0]["id"],
                        "custDataSourceName": dss_info[0]["name"],
                        "otherConfiguration": {"scheduleType": "once", "cron": "0 0/5 * * * ? *", "startTime": "",
                                               "endTime": ""}, "fieldMappings": [
                    {"index": 0, "sourceField": "age", "sourceType": "int", "targetField": "age", "targetType": "int",
                     "encrypt": "", "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                    {"index": 0, "sourceField": "sName", "sourceType": "string", "targetField": "sName",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sId", "sourceType": "string", "targetField": "sId",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "class", "sourceType": "string", "targetField": "class",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sex", "sourceType": "string", "targetField": "sex",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []}]}
            return new_data
        elif 'test_mysql_dss' in data:
            new_data = {"name": "test_once_hive_mysql", "resourceId": resource_info[0]["resource_id"],
                        "description": "", "dataResId": resource_info[0]["id"], "dataResName": resource_info[0]["name"],
                        "serviceMode": 0, "transferType": 1, "custTableName": "student_dsp",
                        "custDataSourceId": dss_info[0]["id"], "custDataSourceName": dss_info[0]["name"],
                        "otherConfiguration": {"scheduleType": "once", "cron": "0 0/5 * * * ? *", "startTime": "",
                                               "endTime": ""}, "fieldMappings": [
                    {"index": 0, "sourceField": "age", "sourceType": "int", "targetField": "age", "targetType": "int",
                     "encrypt": "", "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                    {"index": 0, "sourceField": "sName", "sourceType": "string", "targetField": "sName",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sId", "sourceType": "string", "targetField": "sId",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "class", "sourceType": "string", "targetField": "class",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sex", "sourceType": "string", "targetField": "sex",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []}]}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def push_resource_data_open(data):
    try:
        sql = "select name,id,tenant_id,owner,dataset_id,dataset_name,increment_field,resource_id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'gjb_ttest_es_student_' in data:
            new_data = {"name": resource_info[0]["name"], "resourceId": resource_info[0]["resource_id"],
                        "pullServiceMode": [2], "pushServiceMode": [0, 1], "description": "", "expiredTime": "",
                        "storage": "ElasticSearch", "enabled": 1, "tenantId": resource_info[0]["tenant_id"],
                        "owner": resource_info[0]["owner"], "creator": "admin", "createTime": data_now(),
                        "lastModifier": "admin", "lastModifiedTime": data_now(), "id": resource_info[0]["id"],
                        "isPull": 1, "isPush": 1, "type": 0, "datasetId": resource_info[0]["dataset_id"],
                        "datasetName": resource_info[0]["dataset_name"],
                        "incrementField": resource_info[0]["increment_field"], "categoryId": "0", "encoder": "UTF-8",
                        "fieldMappings": [{"index": 0, "sourceField": "age", "sourceType": "int", "targetField": "age",
                                           "targetType": "int", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "sName", "sourceType": "string",
                                           "targetField": "sName", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "sId", "sourceType": "string",
                                           "targetField": "sId", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "class", "sourceType": "string",
                                           "targetField": "class", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "sex", "sourceType": "string",
                                           "targetField": "sex", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []}],
                        "baseConfiguration": None, "publishConfiguration": None, "openStatus": 1, "accessTimes": 0,
                        "source": "Baymax", "sourceType": "DATASET", "query": {
                    "pageable": {"pageable": "false", "pageNum": 1, "pageSize": 2147483647, "orderByClause": "",
                                 "offset": "0"}, "sqlTemplate": "", "parameters": [], "parametersMap": {}}}
            return new_data
        elif 'gjb_ttest_hdfs_student_' in data:
            new_data = {"name": resource_info[0]["name"], "resourceId": resource_info[0]["resource_id"],
                        "pullServiceMode": [], "pushServiceMode": [0, 1], "description": "", "expiredTime": "",
                        "storage": "HDFS", "enabled": 1, "tenantId": resource_info[0]["tenant_id"],
                        "owner": resource_info[0]["owner"], "creator": "admin", "createTime": data_now(),
                        "lastModifier": "admin", "lastModifiedTime": data_now(), "id": resource_info[0]["id"],
                        "isPull": None, "isPush": 1, "type": 0, "datasetId": resource_info[0]["dataset_id"],
                        "datasetName": resource_info[0]["dataset_name"],
                        "incrementField": resource_info[0]["increment_field"], "categoryId": "0", "encoder": "UTF-8",
                        "fieldMappings": [{"index": 0, "sourceField": "age", "sourceType": "int", "targetField": "age",
                                           "targetType": "int", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "sName", "sourceType": "string",
                                           "targetField": "sName", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "sId", "sourceType": "string",
                                           "targetField": "sId", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "class", "sourceType": "string",
                                           "targetField": "class", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "sex", "sourceType": "string",
                                           "targetField": "sex", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []}],
                        "baseConfiguration": None, "publishConfiguration": None, "openStatus": 1, "accessTimes": 0,
                        "source": "Baymax", "sourceType": "DATASET", "query": {
                    "pageable": {"pageable": "false", "pageNum": 1, "pageSize": 2147483647, "orderByClause": "",
                                 "offset": "0"}, "sqlTemplate": "", "parameters": [], "parametersMap": {}}}
            return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)


def pull_resource_data_open(data):
    try:
        sql = "select name,id,tenant_id,owner,dataset_id,dataset_name,increment_field,resource_id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'gjb_ttest_es_student_' in data:
            new_data = {"name": resource_info[0]["name"], "resourceId": resource_info[0]["resource_id"],
                        "pullServiceMode": [2], "pushServiceMode": [0, 1], "description": "", "expiredTime": "",
                        "storage": "ElasticSearch", "enabled": 1, "tenantId": resource_info[0]["tenant_id"],
                        "owner": resource_info[0]["owner"], "creator": "admin", "createTime": data_now(),
                        "lastModifier": "admin", "lastModifiedTime": data_now(), "id": resource_info[0]["id"],
                        "isPull": 1, "isPush": 1, "type": 0, "datasetId": resource_info[0]["dataset_id"],
                        "datasetName": resource_info[0]["dataset_name"],
                        "incrementField": resource_info[0]["increment_field"], "categoryId": "0", "encoder": "UTF-8",
                        "fieldMappings": [{"index": 0, "sourceField": "age", "sourceType": "int", "targetField": "age",
                                           "targetType": "int", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "sName", "sourceType": "string",
                                           "targetField": "sName", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "sId", "sourceType": "string",
                                           "targetField": "sId", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "class", "sourceType": "string",
                                           "targetField": "class", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "sex", "sourceType": "string",
                                           "targetField": "sex", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []}],
                        "baseConfiguration": None, "publishConfiguration": None, "openStatus": 1, "accessTimes": 0,
                        "source": "Baymax", "sourceType": "DATASET", "query": {
                    "pageable": {"pageable": "false", "pageNum": 1, "pageSize": 2147483647, "orderByClause": "",
                                 "offset": "0"}, "sqlTemplate": "", "parameters": [], "parametersMap": {}}}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"name": resource_info[0]["name"], "resourceId": resource_info[0]["resource_id"],
                        "pullServiceMode": [2], "pushServiceMode": [], "description": "", "expiredTime": "",
                        "storage": "JDBC", "enabled": 1, "tenantId": resource_info[0]["tenant_id"],
                        "owner": resource_info[0]["owner"], "creator": "admin", "createTime": data_now(),
                        "lastModifier": "admin", "lastModifiedTime": data_now(), "id": resource_info[0]["id"],
                        "isPull": 1, "isPush": "", "type": 0, "datasetId": resource_info[0]["dataset_id"],
                        "datasetName": resource_info[0]["dataset_name"],
                        "incrementField": resource_info[0]["increment_field"], "categoryId": "0", "encoder": "UTF-8",
                        "fieldMappings": [{"index": 0, "sourceField": "age", "sourceType": "int", "targetField": "age",
                                           "targetType": "int", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "sName", "sourceType": "string",
                                           "targetField": "sName", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "sId", "sourceType": "string",
                                           "targetField": "sId", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "class", "sourceType": "string",
                                           "targetField": "class", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                                          {"index": 0, "sourceField": "sex", "sourceType": "string",
                                           "targetField": "sex", "targetType": "string", "encrypt": "",
                                           "transformRule": {"type": "", "expression": ""}, "supportAggs": []}],
                        "baseConfiguration": None, "publishConfiguration": None, "openStatus": 1, "accessTimes": 0,
                        "source": "Baymax", "sourceType": "DATASET", "query": {
                    "pageable": {"pageable": "false", "pageNum": 1, "pageSize": 2147483647, "orderByClause": "",
                                 "offset": "0"}, "sqlTemplate": "", "parameters": [], "parametersMap": {}}}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def resource_data_pull_es(data):
    try:
        data = data.split("&")
        sql = "select name,id,resource_id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % \
              data[0]
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        app_sql = "select name,id from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % \
                  data[1]
        appconfig_info = ms.ExecuQuery(app_sql.encode('utf-8'))
        if 'gjb_ttest_es_student_' in data:
            new_data = {"custAppId": appconfig_info[0]["id"], "resourceId": resource_info[0]["resource_id"],
                        "custAppName": appconfig_info[0]["name"], "name": "test_once_es_txt", "description": "",
                        "serviceMode": 2, "transferType": 0, "dataResId": resource_info[0]["id"],
                        "dataResName": resource_info[0]["name"], "sourceType": "DATASET", "fieldMappings": [
                    {"index": 0, "sourceField": "age", "sourceType": "int", "targetField": "age", "targetType": "int",
                     "encrypt": "", "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                    {"index": 0, "sourceField": "sName", "sourceType": "string", "targetField": "sName",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sId", "sourceType": "string", "targetField": "sId",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "class", "sourceType": "string", "targetField": "class",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sex", "sourceType": "string", "targetField": "sex",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []}]}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"custAppId": appconfig_info[0]["id"], "resourceId": resource_info[0]["resource_id"],
                        "custAppName": appconfig_info[0]["name"], "name": "snow_dataset_dsp", "description": "",
                        "serviceMode": 2, "transferType": 0, "dataResId": resource_info[0]["id"],
                        "dataResName": resource_info[0]["name"], "sourceType": "DATASET", "fieldMappings": [
                    {"index": 0, "sourceField": "age", "sourceType": "int", "targetField": "age", "targetType": "int",
                     "encrypt": "", "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                    {"index": 0, "sourceField": "sName", "sourceType": "string", "targetField": "sName",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sId", "sourceType": "string", "targetField": "sId",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "class", "sourceType": "string", "targetField": "class",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sex", "sourceType": "string", "targetField": "sex",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []}]}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def resource_data(data):
    try:
        sql = "select id ,dataset_id, dataset_name,resource_id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time limit 1" % data
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        new_data = {"name": "gjb_test_hdfs_student2020随机数", "resourceId": resource_info[0]["resource_id"],
                    "datasetName": resource_info[0]["dataset_name"], "storage": "HDFS", "encoder": "UTF-8",
                    "incrementField": "age", "openStatus": 1, "categoryId": "0",
                    "datasetId": resource_info[0]["dataset_id"], "expiredTime": 0, "type": 0, "fieldMappings": [
                {"index": 0, "sourceField": "sId", "sourceType": "string", "targetField": "sId", "targetType": "string",
                 "encrypt": "", "transformRule": {"type": "", "expression": ""}},
                {"index": 0, "sourceField": "sName", "sourceType": "string", "targetField": "sName",
                 "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""}},
                {"index": 0, "sourceField": "sex", "sourceType": "string", "targetField": "sex", "targetType": "string",
                 "encrypt": "", "transformRule": {"type": "", "expression": ""}},
                {"index": 0, "sourceField": "age", "sourceType": "int", "targetField": "age", "targetType": "int",
                 "encrypt": "", "transformRule": {"type": "", "expression": ""}},
                {"index": 0, "sourceField": "class", "sourceType": "string", "targetField": "class",
                 "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""}}],
                    "id": resource_info[0]["id"]}
        from new_api_cases.dw_deal_parameters import deal_random
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)


def resource_data_dss(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        data = data.split("&")
        dir_sql = "select id from merce_resource_dir where name like '%%%%%s%%%%' ORDER BY create_time desc limit 1" % \
                  data[1]
        dir_info = ms.ExecuQuery(dir_sql.encode('utf-8'))
        sql = "select id,name from merce_dss where name like '%s%%%%' ORDER BY create_time limit 1" % data[0]
        dss_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'autotest_mysql' in data:
            new_data = {"name": "api_datasource随机数", "resourceId": dir_info[0]["id"], "sourceType": "DATASOURCE",
                        "datasetName": dss_info[0]["name"], "datasetId": dss_info[0]["id"], "categoryId": 0,
                        "expiredTime": 0, "fieldMappings": [], "openStatus": 0, "type": 0, "isIncrementField": "false",
                        "incrementField": "", "encoder": "UTF-8", "timeStamp": "", "storage": "DB", "dataset": "",
                        "query": {"parameters": [{"content": "", "value": "18", "name": "age"}],
                                  "sqlTemplate": "select\n  *\nfrom\n  student_2020\nwhere\n  age > #{age}"}}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def appconfig_data(data):
    try:
        sql = "select id ,tenant_id, owner, access_key ,access_ip,cust_id ,cust_name,public_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        config_info = ms.ExecuQuery(sql.encode('utf-8'))
        access_ip = [str(config_info[0]["access_ip"])]
        new_data = {"accessIp": access_ip, "name": "autotest_appconfig_随机数", "enabled": 1,
                    "tenantId": config_info[0]["tenant_id"], "owner": config_info[0]["owner"], "creator": "customer3",
                    "createTime": data_now(), "lastModifier": "customer3", "lastModifiedTime": data_now(),
                    "description": "", "id": config_info[0]["id"], "custId": config_info[0]["cust_id"],
                    "custName": config_info[0]["cust_name"], "accessKey": config_info[0]["access_key"],
                    "publicKey": config_info[0]["public_key"]}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)


def cust_data_source(data):
    try:
        sql = "select id,owner,tenant_id from dsp_cust_data_source where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        source_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'autotest_hdfs_csv' in data:
            new_data = {"id": source_info[0]["id"], "name": "autotest_hdfs_csv_随机数", "type": "HDFS",
                        "description": "autotest_hdfs_csv_随机数",
                        "attributes": {"quoteChar": "\"", "escapeChar": "\\", "path": "/auto_test/out89",
                                       "format": "csv", "chineseName": "autotest_hdfs_csv", "header": "false",
                                       "separator": ",", "properties": [{"name": "", "value": ""}], "ignoreRow": 0},
                        "owner": source_info[0]["owner"], "enabled": 1, "tenantId": source_info[0]["tenant_id"],
                        "creator": "customer3", "createTime": data_now(), "lastModifier": "customer3",
                        "lastModifiedTime": data_now()}
            deal_random(new_data)
            return new_data
        elif 'autotest_ftp' in data:
            new_data = {"id": source_info[0]["id"], "name": "autotest_ftp_随机数", "type": "FTP",
                        "description": "autotest_ftp",
                        "attributes": {"chineseName": "autotest_ftp", "host": "192.168.1.84", "port": "21",
                                       "username": "europa", "password": "europa", "recursive": "true",
                                       "secure": "false", "skipHeader": "false", "dir": "/home/europa/dsp",
                                       "fieldsSeparator": ","}, "owner": source_info[0]["owner"], "enabled": 1,
                        "tenantId": source_info[0]["tenant_id"], "creator": "customer3", "createTime": data_now(),
                        "lastModifier": "customer3", "lastModifiedTime": data_now()}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def admin_flow_id(data):
    try:
        url = '%s/api/dsp/platform/service/infoById?id=%s' % (dsp_host, data)
        response = requests.get(url=url, headers=get_headers())
        flow_id = dict_res(response.text)["content"]["jobInfo"]['flowId']
        return flow_id
    except Exception as e:
        log.error("异常信息：%s" % e)


def customer_flow_id(data):
    try:
        url = '%s/api/dsp/consumer/service/infoById?id=%s' % (dsp_host, data)
        response = requests.get(url=url, headers=get_headers())
        flow_id = dict_res(response.text)["content"]["jobInfo"]['flowId']
        return flow_id
    except Exception as e:
        log.error("异常信息：%s" % e)


def pull_data(data):
    try:
        data = data.split("&")
        sql = "select id ,name from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[
            0]
        service_info = ms.ExecuQuery(sql.encode('utf-8'))
        config_sql = "select access_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % \
                     data[1]
        config_info = ms.ExecuQuery(config_sql.encode('utf-8'))
        if 'snow_dataset_dsp' in data:
            new_data = {"sourceType": "DATASET", "accessKey": config_info[0]["access_key"],
                        "severName": service_info[0]["name"], "dataServiceId": service_info[0]["id"],
                        "encrypted": "false", "query": {"fieldGroup": {"fields": [], "fieldGroups": []},
                                                        "pageable": {"pageSize": 10, "pageNum": 1},
                                                        "selectProperties": ["age", "sName", "sId", "class", "sex"]},
                        "timestamp": 1626432427}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def pull_data_sql(data):
    try:
        data = data.split("&")
        sql = "select id ,name from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[
            0]
        service_info = ms.ExecuQuery(sql.encode('utf-8'))
        config_sql = "select access_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % \
                     data[1]
        config_info = ms.ExecuQuery(config_sql.encode('utf-8'))
        if 'api_mysqldataset' in data:
            new_data = {"sourceType": "DATASET", "accessKey": config_info[0]["access_key"],
                        "severName": service_info[0]["name"], "dataServiceId": service_info[0]["id"],
                        "encrypted": "false", "query": {"fieldGroup": {"fields": [], "fieldGroups": []},
                                                        "pageable": {"pageSize": 10, "pageNum": 1},
                                                        "selectProperties": ["id", "user_id", "number", "createtime",
                                                                             "note", "dt"]}, "timestamp": 1613987662}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"sourceType": "DATASET", "accessKey": config_info[0]["access_key"],
                        "severName": service_info[0]["name"], "dataServiceId": service_info[0]["id"],
                        "encrypted": "false", "query": {"fieldGroup": {"fields": [], "fieldGroups": []},
                                                        "pageable": {"pageSize": 10, "pageNum": 1},
                                                        "selectProperties": ["age", "sName", "sId", "class", "sex"]},
                        "timestamp": 1626432427}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def pull_aggs_sql(data):
    try:
        data = data.split("&")
        sql = "select id ,name from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[
            0]
        service_info = ms.ExecuQuery(sql.encode('utf-8'))
        config_sql = "select access_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % \
                     data[1]
        config_info = ms.ExecuQuery(config_sql.encode('utf-8'))
        if 'api_mysqldataset' in data:
            new_data = {"sourceType": "DATASET", "accessKey": config_info[0]["access_key"],
                        "severName": service_info[0]["name"], "dataServiceId": service_info[0]["id"],
                        "encrypted": "false", "query": {"groupFields": ["createtime"], "aggFields": [
                    {"name": "number", "alias": "number_sum", "aggType": "SUM", "distinct": "false"},
                    {"name": "number", "alias": "number_max", "aggType": "MAX", "distinct": "false"},
                    {"name": "number", "alias": "number_min", "aggType": "MIN", "distinct": "false"},
                    {"name": "number", "alias": "number_avg", "aggType": "AVG", "distinct": "false"}],
                                                        "havingFieldGroup": {"fields": [
                                                            {"andOr": "AND", "name": "number_min", "oper": "NOT_EQUAL",
                                                             "value": ["33"]}], "fieldGroups": []}, "fieldGroup": {
                        "fields": [{"andOr": "AND", "name": "id", "oper": "NOT_EQUAL", "value": ["22"]}],
                        "fieldGroups": []}, "ordSort": [{"name": "number_sum", "order": "ASC"}],
                                                        "pageable": {"pageSize": 10, "pageNum": 1}},
                        "timestamp": 1602677876}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"sourceType": "DATASET", "accessKey": config_info[0]["access_key"],
                        "severName": service_info[0]["name"], "dataServiceId": service_info[0]["id"],
                        "encrypted": "false", "query": {"groupFields": ["class"], "aggFields": [
                    {"name": "age", "alias": "age_max", "aggType": "MAX", "distinct": "false"},
                    {"name": "age", "alias": "age_min", "aggType": "MIN", "distinct": "false"},
                    {"name": "age", "alias": "age_count", "aggType": "COUNT", "distinct": "false"},
                    {"name": "age", "alias": "age_sum", "aggType": "SUM", "distinct": "false"},
                    {"name": "age", "alias": "age_avg", "aggType": "AVG", "distinct": "false"}],
                                                        "havingFieldGroup": {"fields": [], "fieldGroups": []},
                                                        "fieldGroup": {"fields": [], "fieldGroups": []},
                                                        "pageable": {"pageSize": 10, "pageNum": 1}},
                        "timestamp": 1626432684}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def pull_aggs(data):
    try:
        data = data.split("&")
        sql = "select id ,name from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[
            0]
        service_info = ms.ExecuQuery(sql.encode('utf-8'))
        config_sql = "select access_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % \
                     data[1]
        config_info = ms.ExecuQuery(config_sql.encode('utf-8'))
        if 'test_once_es_txt' in data:
            new_data = {"sourceType": "DATASET", "accessKey": config_info[0]["access_key"],
                        "severName": service_info[0]["name"], "dataServiceId": service_info[0]["id"],
                        "encrypted": "false", "query": {"groupFields": ["class"], "aggFields": [
                    {"name": "age", "alias": "age_avg", "aggType": "AVG", "distinct": "false"}],
                                                        "havingFieldGroup": {"fields": [], "fieldGroups": []},
                                                        "fieldGroup": {"fields": [], "fieldGroups": []},
                                                        "pageable": {"pageSize": 10, "pageNum": 1}},
                        "timestamp": 1626418291}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"sourceType": "DATASET", "accessKey": config_info[0]["access_key"],
                        "severName": service_info[0]["name"], "dataServiceId": service_info[0]["id"],
                        "encrypted": "false", "query": {"groupFields": ["class"], "aggFields": [
                    {"name": "age", "alias": "age_max", "aggType": "MAX", "distinct": "false"},
                    {"name": "age", "alias": "age_min", "aggType": "MIN", "distinct": "false"},
                    {"name": "age", "alias": "age_count", "aggType": "COUNT", "distinct": "false"},
                    {"name": "age", "alias": "age_sum", "aggType": "SUM", "distinct": "false"},
                    {"name": "age", "alias": "age_avg", "aggType": "AVG", "distinct": "false"}],
                                                        "havingFieldGroup": {"fields": [], "fieldGroups": []},
                                                        "fieldGroup": {"fields": [], "fieldGroups": []},
                                                        "pageable": {"pageSize": 10, "pageNum": 1}},
                        "timestamp": 1626432684}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def application_pull_approval(data):
    try:
        sql = "select id,name,owner,tenant_id,data_res_name,data_res_id,cust_app_id ,cust_app_name from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        application = ms.ExecuQuery(sql.encode('utf-8'))
        if 'test_once_es_txt' in data:
            new_data = {"applicationId": application[0]["id"], "name": application[0]["name"],
                        "dataResName": application[0]["data_res_name"], "lastModifier": "customer3",
                        "custDataSourceName": "", "otherConfiguration": "", "fieldMappings": [
                    {"index": 0, "sourceField": "age", "sourceType": "int", "targetField": "age", "targetType": "int",
                     "encrypt": "", "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                    {"index": 0, "sourceField": "sName", "sourceType": "string", "targetField": "sName",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sId", "sourceType": "string", "targetField": "sId",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "class", "sourceType": "string", "targetField": "class",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sex", "sourceType": "string", "targetField": "sex",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []}], "transferType": 0, "auditMind": "", "enabled": 1,
                        "tenantId": application[0]["tenant_id"], "owner": application[0]["owner"],
                        "creator": "customer3", "createTime": "2021-07-16 14:09:42",
                        "lastModifiedTime": "2021-07-16 14:09:42", "description": "", "id": application[0]["id"],
                        "dataResId": application[0]["data_res_id"], "custDataSourceId": "", "custTableName": "",
                        "custAppId": application[0]["cust_app_id"], "custAppName": application[0]["cust_app_name"],
                        "serviceMode": 2, "expiredTime": "0", "status": 0, "sourceType": "DATASET", "type": 0}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"applicationId": application[0]["id"], "name": application[0]["name"],
                        "dataResName": application[0]["data_res_name"], "lastModifier": "customer3",
                        "custDataSourceName": "", "otherConfiguration": "", "fieldMappings": [
                    {"index": 0, "sourceField": "age", "sourceType": "int", "targetField": "age", "targetType": "int",
                     "encrypt": "", "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                    {"index": 0, "sourceField": "sName", "sourceType": "string", "targetField": "sName",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sId", "sourceType": "string", "targetField": "sId",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "class", "sourceType": "string", "targetField": "class",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sex", "sourceType": "string", "targetField": "sex",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []}], "transferType": 0, "auditMind": "yyy", "enabled": 1,
                        "tenantId": application[0]["tenant_id"], "owner": application[0]["owner"],
                        "creator": "customer3", "createTime": "2021-07-16 18:40:02",
                        "lastModifiedTime": "2021-07-16 18:40:02", "description": "", "id": application[0]["id"],
                        "dataResId": application[0]["data_res_id"], "custDataSourceId": "", "custTableName": "",
                        "custAppId": application[0]["cust_app_id"], "custAppName": application[0]["cust_app_name"],
                        "serviceMode": 2, "expiredTime": "0", "status": 0, "sourceType": "DATASET", "type": 0}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def application_push_approval(data):
    try:
        sql = "select id,name,owner,tenant_id,data_res_name,cust_data_source_name,data_res_id,cust_data_source_id,cust_table_name,creator from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        application = ms.ExecuQuery(sql.encode('utf-8'))
        if 'test_once_hdfs_csv' in data:
            new_data = {"applicationId": application[0]["id"], "name": application[0]["name"],
                        "dataResName": application[0]["data_res_name"], "lastModifier": "customer3",
                        "custDataSourceName": application[0]["cust_data_source_name"],
                        "otherConfiguration": {"cron": "0 0/5 * * * ? *", "scheduleType": "once", "startTime": "",
                                               "endTime": ""}, "fieldMappings": [
                    {"index": 0, "sourceField": "age", "sourceType": "int", "targetField": "age", "targetType": "int",
                     "encrypt": "", "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                    {"index": 0, "sourceField": "sName", "sourceType": "string", "targetField": "sName",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sId", "sourceType": "string", "targetField": "sId",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "class", "sourceType": "string", "targetField": "class",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sex", "sourceType": "string", "targetField": "sex",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []}], "transferType": 1, "auditMind": "yyy", "enabled": 1,
                        "tenantId": application[0]["tenant_id"], "owner": application[0]["owner"],
                        "creator": application[0]["creator"], "createTime": data_now(), "lastModifiedTime": data_now(),
                        "description": "", "id": application[0]["id"], "dataResId": application[0]["data_res_id"],
                        "custDataSourceId": application[0]["cust_data_source_id"], "custTableName": "", "custAppId": "",
                        "custAppName": "", "serviceMode": 0, "expiredTime": "0", "status": 0, "sourceType": "DATASET",
                        "type": 1}
            return new_data
        elif 'test_once_hive_mysql' in data:
            new_data = {"applicationId": application[0]["id"], "name": application[0]["name"],
                        "dataResName": application[0]["data_res_name"], "lastModifier": "admin",
                        "custDataSourceName": application[0]["cust_data_source_name"],
                        "otherConfiguration": {"cron": "0 * * * * ? ", "scheduleType": "once", "startTime": "",
                                               "endTime": ""}, "fieldMappings": [
                    {"index": 0, "sourceField": "sId", "sourceType": "string", "targetField": "sId",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sName", "sourceType": "string", "targetField": "sName",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "sex", "sourceType": "string", "targetField": "sex",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []},
                    {"index": 0, "sourceField": "age", "sourceType": "int", "targetField": "age", "targetType": "int",
                     "encrypt": "", "transformRule": {"type": "", "expression": ""}, "supportAggs": []},
                    {"index": 0, "sourceField": "class", "sourceType": "string", "targetField": "class",
                     "targetType": "string", "encrypt": "", "transformRule": {"type": "", "expression": ""},
                     "supportAggs": []}], "transferType": 1, "auditMind": "yyy", "enabled": 1,
                        "tenantId": application[0]["tenant_id"], "owner": application[0]["owner"],
                        "creator": application[0]["creator"], "createTime": data_now(), "lastModifiedTime": data_now(),
                        "description": "", "id": application[0]["id"], "dataResId": application[0]["data_res_id"],
                        "custDataSourceId": application[0]["cust_data_source_id"], "custTableName": "student_dsp",
                        "custAppId": "", "custAppName": "", "serviceMode": 1, "expiredTime": "0", "status": 0,
                        "sourceType": "DATASET", "type": 1}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)

def update_customer(data):
    try:
        sql = "select id,owner,tenant_id,username from dsp_customer where enabled=0 and name like '%s%%%%' order by create_time desc limit 1" % data
        customer_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'gjb_test009_' in data:
            new_data = {"username": customer_info[0]["username"], "name": "gjb_test009_随机数",
                        "password": "e10adc3949ba59abbe56e057f20f883e",
                        "checkPassword": "e10adc3949ba59abbe56e057f20f883e", "enabled": 0,
                        "tenantId": customer_info[0]["tenant_id"], "owner": customer_info[0]["owner"],
                        "creator": "admin", "createTime": data_now(), "lastModifier": "admin",
                        "lastModifiedTime": data_now(), "id": customer_info[0]["id"], "expiredPeriod": "0"}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def update_user(data):
    try:
        sql = "select id from merce_user where  name like '%s%%%%' order by create_time desc limit 1" % data
        user_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'dsp' in data:
            new_data = {"name": "dsp随机数", "loginId": "dsp随机数", "phone": "15801232688", "email": "15801232688@139.com",
                        "id": user_info[0]["id"], "resourceQueues": ["default"], "disable": "true"}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def update_role(data):
    try:
        sql = "select id,name from merce_role where name like '%s%%%%' order by create_time desc limit 1" % data
        role_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'dsp' in data:
            new_data = {"name": "dsp随机数", "permissions": [], "id": role_info[0]["id"]}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def enable_role(data):
    try:
        sql = "select enabled,id from merce_role where name like '%s%%%%' order by create_time desc limit 1" % data
        role_info = ms.ExecuQuery(sql.encode('utf-8'))
        ids = [str(role_info[0]["id"])]
        if role_info[0]["enabled"] == 1:
            new_data = {"enabled": 0, "ids": ids}
            return new_data
        elif role_info[0]["enabled"] == 0:
            new_data = {"enabled": 1, "ids": ids}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def enable_user(data):
    try:
        sql = "select enabled,id from merce_user where name like '%s%%%%' order by create_time desc limit 2" % data
        user_info = ms.ExecuQuery(sql.encode('utf-8'))
        ids = []
        new_data = {}
        for i in range(len(user_info)):
            ids.append(str(user_info[i]["id"]))
            if user_info[i]["enabled"] == 1:
                new_data = {"enabled": 0, "ids": ids}
            elif user_info[i]["enabled"] == 0:
                new_data = {"enabled": 1, "ids": ids}
            else:
                return
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)


def set_user_role(data):
    try:
        user, role = [], []
        user_sql = "select id from merce_user where name like '%s%%%%' order by create_time desc limit 1" % data
        user_info = ms.ExecuQuery(user_sql.encode('utf-8'))
        user.append(str(user_info[0]["id"]))
        sql = "select id from merce_role where name like '%s%%%%' order by create_time desc limit 1" % data
        role_info = ms.ExecuQuery(sql.encode('utf-8'))
        role.append(str(role_info[0]["id"]))
        user.append(role)
        data = {"accountExpiredTime": "2022-07-21", "id": user_info[0]["id"], "pwdExpiredTime": "2022-04-21"}
        user.append(data)
        return user
    except Exception as e:
        log.error("异常信息：%s" % e)


def new_dir(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        dir_sql = "select id,res_type from merce_resource_dir where name like '%%%%%s%%%%' ORDER BY create_time desc limit 1" % data
        dir_info = ms.ExecuQuery(dir_sql.encode('utf-8'))
        new_data = {"name": "autoapi_test随机数", "parentId": dir_info[0]["id"], "resType": dir_info[0]["res_type"]}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)


def rename_dir(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        dir_sql = "select id,res_type,owner,tenant_id,create_time,last_modified_time,parent_id,path from merce_resource_dir where name like '%%%%%s%%%%' ORDER BY create_time desc limit 1" % data
        dir_info = ms.ExecuQuery(dir_sql.encode('utf-8'))
        new_data = {"name": "autoapi_two随机数", "parentId": dir_info[0]["parent_id"], "resType": dir_info[0]["res_type"],
                    "tenantId": dir_info[0]["tenant_id"], "owner": dir_info[0]["owner"], "enabled": 1,
                    "creator": "admin", "createTime": str(dir_info[0]["create_time"]), "lastModifier": "admin",
                    "lastModifiedTime": str(dir_info[0]["last_modified_time"]), "id": dir_info[0]["id"],
                    "version": None, "groupCount": None, "groupFieldValue": "", "order": 1, "isHide": 0,
                    "path": dir_info[0]["path"], "children": [], "halfSelect": "true",
                    "parentCode": dir_info[0]["parent_id"], "selfCode": dir_info[0]["id"], "hasChildren": "false",
                    "expiredPeriod": 0}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)
