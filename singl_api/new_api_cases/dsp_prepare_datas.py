# coding:utf-8
import os
import requests
from basic_info.get_auth_token import get_headers_admin, get_headers_customer
from new_api_cases.dw_deal_parameters import deal_random
from util.format_res import dict_res
from basic_info.setting import Dsp_MySQL_CONFIG, dsp_host
from util.Open_DB import MYSQL
from util.logs import Logger
from util.timestamp_13 import get_now, get_tomorrow, data_now

ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"], Dsp_MySQL_CONFIG["PORT"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))
log = Logger().get_log()

def resource_data_push(data):
    try:
        data = data.split("&")
        sql = "select name,id from dsp_data_resource where name like '%%%%%s%%%%' ORDER BY create_time desc limit 1" % data[0]
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        dss_sql = "select name,id from dsp_cust_data_source where name like '%%%%%s%%%%' ORDER BY create_time desc limit 1" % data[1]
        dss_info = ms.ExecuQuery(dss_sql.encode('utf-8'))
        if 'test_hdfs_csv' in data:
            new_data = {"name": "test_once_hdfs_csv","description":"","dataResId": resource_info[0]["id"],"dataResName":resource_info[0]["name"], "serviceMode":1,"transferType":1,"custTableName":"", "custDataSourceId": dss_info[0]["id"],"custDataSourceName":dss_info[0]["name"], "otherConfiguration": {"scheduleType": "once","cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
            return new_data
        elif 'test_mysql' in data:
            new_data = {"name": "test_once_mysql", "description":"", "dataResId": resource_info[0]["id"], "dataResName": resource_info[0]["name"], "serviceMode":1,"transferType":1,"custTableName": "student_2020", "custDataSourceId": dss_info[0]["id"],"custDataSourceName": dss_info[0]["name"], "otherConfiguration": {"scheduleType":"once","cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
            return new_data
        elif 'test_hdfs_txt' in data:
            new_data = {"name": "test_hdfs_txt_event", "description":"", "dataResId": resource_info[0]["id"], "dataResName":resource_info[0]["name"], "serviceMode":0,"transferType":1,"custTableName": "", "custDataSourceId": dss_info[0]["id"], "custDataSourceName": dss_info[0]["name"], "otherConfiguration": {"scheduleType": "event","cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
            return new_data
        elif 'test_oracle' in data:
            new_data = {"name": "test_cron_oracle", "description":"", "dataResId": resource_info[0]["id"], "dataResName": resource_info[0]["name"], "serviceMode":1,"transferType":1,"custTableName": "autotest_student", "custDataSourceId": dss_info[0]["id"], "custDataSourceName": dss_info[0]["name"], "otherConfiguration": {"scheduleType":"cron","cron":"0 0/55 * * * ? ","startTime": get_now(), "endTime": get_tomorrow()},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
            return new_data
        elif 'test_hdfs_hive' in data:
            new_data = {"name": "test_once_hive_txt","description":"","dataResId": resource_info[0]["id"],"dataResName":resource_info[0]["name"], "serviceMode":1,"transferType":1,"custTableName":"", "custDataSourceId": dss_info[0]["id"],"custDataSourceName":dss_info[0]["name"], "otherConfiguration": {"scheduleType": "once","cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
            return new_data
        elif 'test_hdfs_hbase' in data:
            new_data = {"name":"test_event_hbase_txt","description":"","dataResId":resource_info[0]["id"],"dataResName":resource_info[0]["name"],"serviceMode":0,"transferType":1,"custTableName":"","custDataSourceId":dss_info[0]["id"],"custDataSourceName":dss_info[0]["name"],"otherConfiguration":{"scheduleType":"event","cron":"0 0/5 * * * ? *","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"id","sourceType":"int","targetField":"id","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ts","sourceType":"timestamp","targetField":"ts","targetType":"timestamp","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"code","sourceType":"string","targetField":"code","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"total","sourceType":"float","targetField":"total","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"forward_total","sourceType":"float","targetField":"forward_total","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"reverse_total","sourceType":"float","targetField":"reverse_total","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sum_flow","sourceType":"float","targetField":"sum_flow","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sum_inst","sourceType":"float","targetField":"sum_inst","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"inst_num","sourceType":"int","targetField":"inst_num","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"max_inst","sourceType":"float","targetField":"max_inst","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"max_inst_ts","sourceType":"string","targetField":"max_inst_ts","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"min_inst","sourceType":"float","targetField":"min_inst","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"min_inst_ts","sourceType":"string","targetField":"min_inst_ts","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}]}
            return new_data
        elif 'test_hdfs_es' in data:
            new_data = {"name":"test_once_es_txt","description":"","dataResId":resource_info[0]["id"],"dataResName":resource_info[0]["name"],"serviceMode":0,"transferType":1,"custTableName":"","custDataSourceId":dss_info[0]["id"],"custDataSourceName":dss_info[0]["name"],"otherConfiguration":{"scheduleType":"once","cron":"0 0/5 * * * ? *","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}]}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def push_resource_data_open(data):
    try:
        sql = "select name,id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'gjb_ttest_es_student_' in data:
           new_data = {"name":resource_info[0]["name"],"pullServiceMode":[2],"pushServiceMode":[0,1],"description":"","expiredTime":"","storage":"ElasticSearch","enabled":1,"tenantId":resource_info[0]["tenant_id"],"owner":resource_info[0]["owner"],"creator":"admin","createTime":"2021-07-15 19:51:09","lastModifier":"admin","lastModifiedTime":"2021-07-15 20:04:02","id":resource_info[0]["id"],"isPull":1,"isPush":1,"type":0,"datasetId":resource_info[0]["dataset_id"],"datasetName":resource_info[0]["dataset_name"],"incrementField":resource_info[0]["increment_field"],"categoryId":"0","encoder":"UTF-8","fieldMappings":[{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}],"baseConfiguration":"","publishConfiguration":"","openStatus":1,"accessTimes":0,"source":"Baymax","sourceType":"DATASET","query":{"pageable":{"pageable":"false","pageNum":1,"pageSize":2147483647,"orderByClause":"","offset":"0"},"sqlTemplate":"","parameters":[],"parametersMap":{}}}
           return new_data

        else:
            new_data = {"name": resource_info[0]["name"], "id": resource_info[0]["id"], "isPull":0,"isPush":1,"pullServiceMode":[],"pushServiceMode":["1","0"],"expiredTime":"","openStatus":1,"description":""}
            return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)

def pull_resource_data_open(data):
    try:
        sql = "select name,id,tenant_id,owner,dataset_id,dataset_name,increment_field from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'gjb_ttest_es_student_' in data:
            new_data = {"name":resource_info[0]["name"],"pullServiceMode":[2],"pushServiceMode":[0,1],"description":"","expiredTime":"","storage":"ElasticSearch","enabled":1,"tenantId":resource_info[0]["tenant_id"],"owner":resource_info[0]["owner"],"creator":"admin","createTime":"2021-07-15 19:51:09","lastModifier":"admin","lastModifiedTime":"2021-07-15 20:04:02","id":resource_info[0]["id"],"isPull":1,"isPush":1,"type":0,"datasetId":resource_info[0]["dataset_id"],"datasetName":resource_info[0]["dataset_name"],"incrementField":resource_info[0]["increment_field"],"categoryId":"0","encoder":"UTF-8","fieldMappings":[{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}],"baseConfiguration":"","publishConfiguration":"","openStatus":1,"accessTimes":0,"source":"Baymax","sourceType":"DATASET","query":{"pageable":{"pageable":"false","pageNum":1,"pageSize":2147483647,"orderByClause":"","offset":"0"},"sqlTemplate":"","parameters":[],"parametersMap":{}}}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"name":resource_info[0]["name"],"pullServiceMode":[2],"pushServiceMode":[],"description":"","expiredTime":"","storage":"JDBC","enabled":1,"tenantId":resource_info[0]["tenant_id"],"owner":resource_info[0]["owner"],"creator":"admin","createTime":"2021-07-16 18:34:18","lastModifier":"admin","lastModifiedTime":"2021-07-16 18:34:18","id":resource_info[0]["id"],"isPull":1,"isPush":"","type":0,"datasetId":resource_info[0]["dataset_id"],"datasetName":resource_info[0]["dataset_name"],"incrementField":resource_info[0]["increment_field"],"categoryId":"0","encoder":"UTF-8","fieldMappings":[{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}],"baseConfiguration":"","publishConfiguration":"","openStatus":1,"accessTimes":0,"source":"Baymax","sourceType":"DATASET","query":{"pageable":{"pageable":"false","pageNum":1,"pageSize":2147483647,"orderByClause":"","offset":"0"},"sqlTemplate":"","parameters":[],"parametersMap":{}}}
            return new_data
        elif 'api_datasource' in data:
            new_data = {"name": resource_info[0]["name"], "id": resource_info[0]["id"], "isPull": 1, "isPush": 0, "pullServiceMode": ["2"], "pushServiceMode": [], "expiredTime":"","openStatus":1,"description":""}
            return new_data
        elif 'api_mysqldataset' in data:
            new_data = {"name": resource_info[0]["name"], "id": resource_info[0]["id"], "isPull": 1, "isPush": 0, "pullServiceMode": ["2"], "pushServiceMode": [], "expiredTime":"","openStatus":1,"description":""}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def resource_data_pull_es(data):
    try:
        data = data.split("&")
        sql = "select name,id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        app_sql = "select name,id from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[1]
        appconfig_info = ms.ExecuQuery(app_sql.encode('utf-8'))
        if 'gjb_ttest_es_student_' in data:
            new_data = {"custAppId":appconfig_info[0]["id"],"custAppName":appconfig_info[0]["name"],"name":"test_once_es_txt","description":"","serviceMode":2,"transferType":0,"dataResId":resource_info[0]["id"],"dataResName":resource_info[0]["name"],"sourceType":"DATASET","fieldMappings":[{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}]}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"custAppId":appconfig_info[0]["id"],"custAppName":appconfig_info[0]["name"],"name":"snow_dataset_dsp","description":"","serviceMode":2,"transferType":0,"dataResId":resource_info[0]["id"],"dataResName":resource_info[0]["name"],"sourceType":"DATASET","fieldMappings":[{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}]}
            return new_data
        elif 'api_datasource' in data:
            new_data = {"custAppId": appconfig_info[0]["id"], "custAppName": appconfig_info[0]["name"], "name": "api_datasource", "description": "", "serviceMode": 2, "transferType": 0, "dataResId": resource_info[0]["id"], "dataResName": resource_info[0]["name"], "sourceType": "DATASOURCE", "fieldMappings": []}
            return new_data
        elif 'api_mysqldataset' in data:
            new_data = {"custAppId": appconfig_info[0]["id"], "custAppName": appconfig_info[0]["name"], "name": "api_mysqldataset", "description":"", "serviceMode": 2, "transferType": 0, "dataResId": resource_info[0]["id"], "dataResName": resource_info[0]["name"], "sourceType": "DATASET", "fieldMappings": [{"index":0,"sourceField":"id","sourceType":"int","targetField":"id","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"user_id","sourceType":"int","targetField":"user_id","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"number","sourceType":"string","targetField":"number","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"createtime","sourceType":"timestamp","targetField":"createtime","targetType":"timestamp","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"note","sourceType":"string","targetField":"note","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"dt","sourceType":"string","targetField":"dt","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}]}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def resource_data(data):
    try:
        sql = "select id ,dataset_id, dataset_name from dsp_data_resource where name like '%s%%%%' ORDER BY create_time limit 1" % data
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        new_data = {"name": "gjb_test_hdfs_student2020随机数", "datasetName": resource_info[0]["dataset_name"],"storage":"HDFS","encoder":"UTF-8","incrementField":"age","openStatus":1,"categoryId":"0","datasetId": resource_info[0]["dataset_id"], "expiredTime":0,"type":0,"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"id":resource_info[0]["id"]}
        from new_api_cases.dw_deal_parameters import deal_random
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)

def resource_data_save(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id,name,resource_id from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data
        dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'gjb_ttest_hdfs_student2020' in data:
            new_data = {"categoryId":0,"openStatus":0,"type":0,"expiredTime":"0","name":"gjb_ttest_hdfs_student_随机数","encoder":"UTF-8","sourceType":"DATASET","datasetName":dataset_info[0]["name"],"datasourceName":"","storage":"HDFS","fieldMappings":[{"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}}],"timeStamp":"永久有效","isIncrementField":"true","incrementField":"age","query":{"sqlTemplate":""},"datasetId":dataset_info[0]["id"],"category_id":dataset_info[0]["resource_id"]}
            deal_random(new_data)
            return new_data
        elif 'gjb_sink_es' in data:
            new_data = {"categoryId":0,"openStatus":0,"type":0,"expiredTime":"0","name":"gjb_ttest_es_student_随机数","encoder":"UTF-8","sourceType":"DATASET","datasetName":dataset_info[0]["name"],"datasourceName":"","storage":"ElasticSearch","fieldMappings":[{"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}}],"timeStamp":"永久有效","isIncrementField":"true","incrementField":"age","query":{"sqlTemplate":""},"datasetId":dataset_info[0]["id"],"category_id":dataset_info[0]["resource_id"]}
            deal_random(new_data)
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"categoryId":0,"openStatus":0,"type":0,"expiredTime":"0","name":"snow_dataset_dsp随机数","encoder":"UTF-8","sourceType":"DATASET","datasetName":dataset_info[0]["name"],"datasourceName":"","storage":"JDBC","fieldMappings":[{"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}}],"timeStamp":"永久有效","isIncrementField":"false","incrementField":"age","query":{"sqlTemplate":""},"datasetId":dataset_info[0]["id"],"category_id":dataset_info[0]["resource_id"]}
            deal_random(new_data)
            return new_data
        elif 'dw-订单表' in data:
            new_data = {"name": "api_mysqldataset_随机数", "sourceType": "DATASET", "datasetName": dataset_info[0]["name"],"datasetId":dataset_info[0]["id"], "categoryId": 0, "expiredTime": 0, "fieldMappings": [{"sourceField":"id","sourceType":"int","targetField":"id","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"user_id","sourceType":"int","targetField":"user_id","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"number","sourceType":"string","targetField":"number","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"createtime","sourceType":"timestamp","targetField":"createtime","targetType":"timestamp","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"note","sourceType":"string","targetField":"note","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"dt","sourceType":"string","targetField":"dt","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}}],"openStatus":0,"type":0,"isIncrementField":"false","incrementField":"","encoder":"UTF-8","timeStamp":"","storage":"JDBC","dataset":"","query":{"parameters":[],"sqlTemplate":""}}
            deal_random(new_data)
            return new_data
        elif 'hive_dsp' in data:
            new_data = {"categoryId":0,"openStatus":0,"type":0,"expiredTime":"0","name":"gjb_ttest_hive_student_随机数","encoder":"UTF-8","sourceType":"DATASET","datasetName":dataset_info[0]["name"],"datasourceName":"","storage":"HIVE","fieldMappings":[{"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}}],"timeStamp":"永久有效","isIncrementField":"false","incrementField":"","query":{"sqlTemplate":""},"datasetId":dataset_info[0]["id"],"category_id":dataset_info[0]["resource_id"]}
            deal_random(new_data)
            return new_data
        elif 'dsp_hbase_push' in data:
            new_data = {"categoryId":0,"openStatus":0,"type":0,"expiredTime":"0","name":"gjb_ttest_hbase_student_随机数","encoder":"UTF-8","sourceType":"DATASET","datasetName":dataset_info[0]["name"],"datasourceName":"","storage":"HBASE","fieldMappings":[{"sourceField":"id","sourceType":"int","targetField":"id","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"ts","sourceType":"timestamp","targetField":"ts","targetType":"timestamp","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"code","sourceType":"string","targetField":"code","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"total","sourceType":"float","targetField":"total","targetType":"float","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"forward_total","sourceType":"float","targetField":"forward_total","targetType":"float","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"reverse_total","sourceType":"float","targetField":"reverse_total","targetType":"float","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sum_flow","sourceType":"float","targetField":"sum_flow","targetType":"float","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sum_inst","sourceType":"float","targetField":"sum_inst","targetType":"float","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"inst_num","sourceType":"int","targetField":"inst_num","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"max_inst","sourceType":"float","targetField":"max_inst","targetType":"float","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"max_inst_ts","sourceType":"string","targetField":"max_inst_ts","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"min_inst","sourceType":"float","targetField":"min_inst","targetType":"float","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"min_inst_ts","sourceType":"string","targetField":"min_inst_ts","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}}],"timeStamp":"永久有效","isIncrementField":"true","incrementField":"id","query":{"sqlTemplate":""},"datasetId":dataset_info[0]["id"],"category_id":dataset_info[0]["resource_id"]}
            deal_random(new_data)
            return new_data
        else:
           return
    except Exception as e:
        log.error("异常信息：%s" %e)

def resource_data_dss(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id,name from merce_dss where name like '%s%%%%' ORDER BY create_time limit 1" % data
        dss_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'autotest_mysql' in data:
            new_data = {"name": "api_datasource随机数", "sourceType": "DATASOURCE", "datasetName": dss_info[0]["name"], "datasetId":dss_info[0]["id"], "categoryId": 0, "expiredTime": 0, "fieldMappings": [], "openStatus":0,"type":0,"isIncrementField":"false","incrementField":"","encoder":"UTF-8","timeStamp":"","storage":"DB","dataset":"","query":{"parameters":[{"content":"","value":"18","name":"age"}],"sqlTemplate":"select\n  *\nfrom\n  student_2020\nwhere\n  age > #{age}"}}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def appconfig_data(data):
    try:
        sql = "select id ,tenant_id, owner, access_key ,cust_id ,cust_name,public_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        config_info = ms.ExecuQuery(sql.encode('utf-8'))
        new_data = {"accessIp":["192.168.2.142"], "name": "autotest_appconfig_随机数", "enabled":1,"tenantId": config_info[0]["tenant_id"], "owner": config_info[0]["owner"],"creator":"customer3","createTime": data_now(),"lastModifier":"customer3","lastModifiedTime": data_now(),"description":"","id": config_info[0]["id"],"custId":config_info[0]["cust_id"],"custName":config_info[0]["cust_name"],"accessKey": config_info[0]["access_key"],"publicKey": config_info[0]["public_key"]}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)

def cust_data_source(data):
    try:
        sql = "select id,owner,tenant_id from dsp_cust_data_source where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        source_info = ms.ExecuQuery(sql.encode('utf-8'))
        new_data = {"id":source_info[0]["id"],"name":"autotest_hdfs_csv_随机数","type":"HDFS","description":"autotest_hdfs_csv_随机数","attributes":{"quoteChar":"\"","escapeChar":"\\","path":"/auto_test/out89","format":"csv","chineseName":"autotest_hdfs_csv","header":"false","separator":",","properties":[{"name":"","value":""}],"ignoreRow":0},"owner":source_info[0]["owner"],"enabled":1,"tenantId":source_info[0]["tenant_id"],"creator":"customer3","createTime": data_now(),"lastModifier":"customer3","lastModifiedTime": data_now()}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)

def admin_flow_id(data):
    try:
        url = '%s/api/dsp/platform/service/infoById?id=%s' % (dsp_host, data)
        response = requests.get(url=url, headers=get_headers_admin(dsp_host))
        flow_id = dict_res(response.text)["content"]["jobInfo"]['flowId']
        return flow_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def customer_flow_id(data):
    try:
        url = '%s/api/dsp/consumer/service/infoById?id=%s' % (dsp_host, data)
        response = requests.get(url=url, headers=get_headers_customer(dsp_host))
        flow_id = dict_res(response.text)["content"]["jobInfo"]['flowId']
        return flow_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def pull_data(data):
    try:
        data = data.split("&")
        sql = "select id ,name from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
        service_info = ms.ExecuQuery(sql.encode('utf-8'))
        config_sql = "select access_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[1]
        config_info = ms.ExecuQuery(config_sql.encode('utf-8'))
        if 'snow_dataset_dsp' in data:
            new_data = {"sourceType":"DATASET","accessKey":config_info[0]["access_key"],"severName":service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"fieldGroup":{"fields":[],"fieldGroups":[]},"pageable":{"pageSize":10,"pageNum":1},"selectProperties":["age","sName","sId","class","sex"]},"timestamp":1626432427}
            return new_data
        elif 'test_once_es_txt' in data:
            new_data = {"sourceType":"DATASET","accessKey":config_info[0]["access_key"],"severName":service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"fieldGroup":{"fields":[],"fieldGroups":[]},"pageable":{"pageSize":10,"pageNum":1},"selectProperties":["age","sName","sId","class","sex"]},"timestamp":1626416549}
            return new_data
        elif 'api_datasource' in data:
            new_data = {"sourceType":"DATASOURCE","accessKey":config_info[0]["access_key"],"severName":service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"pageable":{"pageNum":1,"pageSize":10,"pageable":"true"},"parameters":[{"name":"age","value":"18","defaultValue":"","content":""}],"sqlTemplate":""},"timestamp":1613987427}
            return new_data
        elif 'api_mysqldataset' in data:
            new_data = {"sourceType":"DATASET","accessKey":config_info[0]["access_key"],"severName":service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"fieldGroup":{"fields":[],"fieldGroups":[]},"pageable":{"pageSize":10,"pageNum":1},"selectProperties":["id","user_id","number","createtime","note","dt"]},"timestamp":1613987662}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def pull_data_sql(data):
    try:
        data = data.split("&")
        sql = "select id ,name from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
        service_info = ms.ExecuQuery(sql.encode('utf-8'))
        config_sql = "select access_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[1]
        config_info = ms.ExecuQuery(config_sql.encode('utf-8'))
        if 'api_mysqldataset' in data:
            new_data = {"sourceType":"DATASET","accessKey":config_info[0]["access_key"],"severName":service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"fieldGroup":{"fields":[],"fieldGroups":[]},"pageable":{"pageSize":10,"pageNum":1},"selectProperties":["id","user_id","number","createtime","note","dt"]},"timestamp":1613987662}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"sourceType":"DATASET","accessKey":config_info[0]["access_key"],"severName":service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"fieldGroup":{"fields":[],"fieldGroups":[]},"pageable":{"pageSize":10,"pageNum":1},"selectProperties":["age","sName","sId","class","sex"]},"timestamp":1626432427}
            return new_data
        elif 'test_once_es_txt' in data:
            new_data = {"sourceType":"DATASET","accessKey":config_info[0]["access_key"],"severName":service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"fieldGroup":{"fields":[],"fieldGroups":[]},"pageable":{"pageSize":10,"pageNum":1},"selectProperties":["age","sName","sId","class","sex"]},"timestamp":1626416549}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def pull_Aggs_sql(data):
    try:
        data = data.split("&")
        sql = "select id ,name from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
        service_info = ms.ExecuQuery(sql.encode('utf-8'))
        config_sql = "select access_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[1]
        config_info = ms.ExecuQuery(config_sql.encode('utf-8'))
        if 'api_mysqldataset' in data:
            new_data = {"sourceType":"DATASET","accessKey": config_info[0]["access_key"],"severName":service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"groupFields":["createtime"],"aggFields":[{"name":"number","alias":"number_sum","aggType":"SUM","distinct":"false"},{"name":"number","alias":"number_max","aggType":"MAX","distinct":"false"},{"name":"number","alias":"number_min","aggType":"MIN","distinct":"false"},{"name":"number","alias":"number_avg","aggType":"AVG","distinct":"false"}],"havingFieldGroup":{"fields":[{"andOr":"AND","name":"number_min","oper":"NOT_EQUAL","value":["33"]}],"fieldGroups":[]},"fieldGroup":{"fields":[{"andOr":"AND","name":"id","oper":"NOT_EQUAL","value":["22"]}],"fieldGroups":[]},"ordSort":[{"name":"number_sum","order":"ASC"}],"pageable":{"pageSize":10,"pageNum":1}},"timestamp":1602677876}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"sourceType":"DATASET","accessKey":config_info[0]["access_key"],"severName":service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"groupFields":["class"],"aggFields":[{"name":"age","alias":"age_max","aggType":"MAX","distinct":"false"},{"name":"age","alias":"age_min","aggType":"MIN","distinct":"false"},{"name":"age","alias":"age_count","aggType":"COUNT","distinct":"false"},{"name":"age","alias":"age_sum","aggType":"SUM","distinct":"false"},{"name":"age","alias":"age_avg","aggType":"AVG","distinct":"false"}],"havingFieldGroup":{"fields":[],"fieldGroups":[]},"fieldGroup":{"fields":[],"fieldGroups":[]},"pageable":{"pageSize":10,"pageNum":1}},"timestamp":1626432684}
            return new_data
        elif 'test_once_es_txt' in data:
            new_data = {"sourceType":"DATASET","accessKey":config_info[0]["access_key"],"severName":service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"groupFields":["class"],"aggFields":[{"name":"age","alias":"age_avg","aggType":"AVG","distinct":"false"}],"havingFieldGroup":{"fields":[],"fieldGroups":[]},"fieldGroup":{"fields":[],"fieldGroups":[]},"pageable":{"pageSize":10,"pageNum":1}},"timestamp":1626418291}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def pull_Aggs(data):
    try:
        data = data.split("&")
        sql = "select id ,name from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
        service_info = ms.ExecuQuery(sql.encode('utf-8'))
        config_sql = "select access_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[1]
        config_info = ms.ExecuQuery(config_sql.encode('utf-8'))
        if 'test_once_es_txt' in data:
            new_data = {"sourceType":"DATASET","accessKey":config_info[0]["access_key"],"severName":service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"groupFields":["class"],"aggFields":[{"name":"age","alias":"age_avg","aggType":"AVG","distinct":"false"}],"havingFieldGroup":{"fields":[],"fieldGroups":[]},"fieldGroup":{"fields":[],"fieldGroups":[]},"pageable":{"pageSize":10,"pageNum":1}},"timestamp":1626418291}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"sourceType":"DATASET","accessKey":config_info[0]["access_key"],"severName":service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"groupFields":["class"],"aggFields":[{"name":"age","alias":"age_max","aggType":"MAX","distinct":"false"},{"name":"age","alias":"age_min","aggType":"MIN","distinct":"false"},{"name":"age","alias":"age_count","aggType":"COUNT","distinct":"false"},{"name":"age","alias":"age_sum","aggType":"SUM","distinct":"false"},{"name":"age","alias":"age_avg","aggType":"AVG","distinct":"false"}],"havingFieldGroup":{"fields":[],"fieldGroups":[]},"fieldGroup":{"fields":[],"fieldGroups":[]},"pageable":{"pageSize":10,"pageNum":1}},"timestamp":1626432684}
            return new_data
        elif 'api_mysqldataset' in data:
            new_data = {"sourceType": "DATASET", "accessKey": config_info[0]["access_key"], "severName": service_info[0]["name"], "dataServiceId": service_info[0]["id"], "encrypted": "false", "query": {"groupFields":["createtime"],"aggFields":[{"name":"number","alias":"number_sum","aggType":"SUM","distinct":"false"},{"name":"number","alias":"number_max","aggType":"MAX","distinct":"false"},{"name":"number","alias":"number_min","aggType":"MIN","distinct":"false"},{"name":"number","alias":"number_avg","aggType":"AVG","distinct":"false"}],"havingFieldGroup":{"fields":[{"andOr":"AND","name":"number_min","oper":"NOT_EQUAL","value":["33"]}],"fieldGroups":[]},"fieldGroup":{"fields":[{"andOr":"AND","name":"id","oper":"NOT_EQUAL","value":["22"]}],"fieldGroups":[]},"ordSort":[{"name":"number_sum","order":"ASC"}],"pageable":{"pageSize":10,"pageNum":1}},"timestamp":1602677876}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def application_pull_approval(data):
    try:
        sql = "select id,name,owner,tenant_id,data_res_name,data_res_id,cust_app_id ,cust_app_name from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        application = ms.ExecuQuery(sql.encode('utf-8'))
        if 'test_once_es_txt' in data:
            new_data = {"applicationId":application[0]["id"],"name":application[0]["name"],"dataResName":application[0]["data_res_name"],"lastModifier":"customer3","custDataSourceName":"","otherConfiguration":"","fieldMappings":[{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}],"transferType":0,"auditMind":"","enabled":1,"tenantId":application[0]["tenant_id"],"owner":application[0]["owner"],"creator":"customer3","createTime":"2021-07-16 14:09:42","lastModifiedTime":"2021-07-16 14:09:42","description":"","id":application[0]["id"],"dataResId":application[0]["data_res_id"],"custDataSourceId":"","custTableName":"","custAppId":application[0]["cust_app_id"],"custAppName":application[0]["cust_app_name"],"serviceMode":2,"expiredTime":"0","status":0,"sourceType":"DATASET","type":0}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"applicationId":application[0]["id"],"name":application[0]["name"],"dataResName":application[0]["data_res_name"],"lastModifier":"customer3","custDataSourceName":"","otherConfiguration":"","fieldMappings":[{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}],"transferType":0,"auditMind":"yyy","enabled":1,"tenantId":application[0]["tenant_id"],"owner":application[0]["owner"],"creator":"customer3","createTime":"2021-07-16 18:40:02","lastModifiedTime":"2021-07-16 18:40:02","description":"","id":application[0]["id"],"dataResId":application[0]["data_res_id"],"custDataSourceId":"","custTableName":"","custAppId":application[0]["cust_app_id"],"custAppName":application[0]["cust_app_name"],"serviceMode":2,"expiredTime":"0","status":0,"sourceType":"DATASET","type":0}
            return new_data
        elif 'api_datasource' in data:
            new_data = {"applicationId":application[0]["id"],"name":application[0]["name"],"dataResName":application[0]["data_res_name"],"lastModifier":"admin","custDataSourceName":"","otherConfiguration":"","fieldMappings":[],"transferType":0,"auditMind":"yyy","enabled":1,"tenantId":application[0]["tenant_id"],"owner":application[0]["owner"],"creator":"customer3","createTime":"2021-02-22 16:18:02","lastModifiedTime":"2021-02-22 16:18:02","description":"","id":application[0]["id"],"dataResId":application[0]["data_res_id"],"custDataSourceId":"","custTableName":"","custAppId":application[0]["cust_app_id"],"custAppName":application[0]["cust_app_name"],"serviceMode":2,"expiredTime":"0","status":0,"sourceType":"DATASOURCE","type":0}
            return new_data
        elif 'api_mysqldataset' in data:
            new_data = {"applicationId":application[0]["id"],"name":application[0]["name"],"dataResName":application[0]["data_res_name"],"lastModifier":"admin","custDataSourceName":"","otherConfiguration":"","fieldMappings":[{"index":0,"sourceField":"id","sourceType":"int","targetField":"id","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"user_id","sourceType":"int","targetField":"user_id","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"number","sourceType":"string","targetField":"number","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"createtime","sourceType":"timestamp","targetField":"createtime","targetType":"timestamp","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"note","sourceType":"string","targetField":"note","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"dt","sourceType":"string","targetField":"dt","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}],"transferType":0,"auditMind":"yyy","enabled":1,"tenantId":application[0]["tenant_id"],"owner":application[0]["owner"],"creator":"customer3","createTime":"2021-02-21 23:25:35","lastModifiedTime":"2021-02-21 23:25:35","description":"","id":application[0]["id"],"dataResId":application[0]["data_res_id"],"custDataSourceId":"","custTableName":"","custAppId":application[0]["cust_app_id"],"custAppName":application[0]["cust_app_name"],"serviceMode":2,"expiredTime":"0","status":0,"sourceType":"DATASET","type":0}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def application_push_approval(data):
    try:
        sql = "select id,name,owner,tenant_id,data_res_name,cust_data_source_name,data_res_id,cust_data_source_id,cust_table_name,creator from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        application = ms.ExecuQuery(sql.encode('utf-8'))
        if 'test_once_hdfs_csv' in data:
            new_data = {"applicationId":application[0]["id"],"name":application[0]["name"],"dataResName":application[0]["data_res_name"],"lastModifier":"admin","custDataSourceName":application[0]["cust_data_source_name"],"otherConfiguration":{"cron":"0 * * * * ? ","scheduleType":"once","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}],"transferType":1,"auditMind":"yyy","enabled":1,"tenantId":application[0]["tenant_id"],"owner":application[0]["owner"],"creator":application[0]["creator"],"createTime":"2021-07-15 18:19:03","lastModifiedTime":"2021-07-15 18:19:03","description":"","id":application[0]["id"],"dataResId":application[0]["data_res_id"],"custDataSourceId":application[0]["cust_data_source_id"],"custTableName":"","custAppId":"","custAppName":"","serviceMode":1,"expiredTime":"0","status":0,"sourceType":"DATASET","type":1}
            return new_data
        elif 'test_once_mysql' in data:
            new_data = {"applicationId":application[0]["id"],"name":application[0]["name"],"dataResName":application[0]["data_res_name"],"lastModifier":"admin","custDataSourceName":application[0]["cust_data_source_name"],"otherConfiguration":{"cron":"0 * * * * ? ","scheduleType":"once","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}],"transferType":1,"auditMind":"yyy","enabled":1,"tenantId":application[0]["tenant_id"],"owner":application[0]["owner"],"creator":application[0]["creator"],"createTime":"2021-07-15 18:19:03","lastModifiedTime":"2021-07-15 18:19:03","description":"","id":application[0]["id"],"dataResId":application[0]["data_res_id"],"custDataSourceId":application[0]["cust_data_source_id"],"custTableName":application[0]["cust_table_name"],"custAppId":"","custAppName":"","serviceMode":1,"expiredTime":"0","status":0,"sourceType":"DATASET","type":1}
            return new_data
        elif 'test_hdfs_txt_event' in data:
            new_data = {"applicationId":application[0]["id"],"name":application[0]["name"],"dataResName":application[0]["data_res_name"],"lastModifier":"admin","custDataSourceName":application[0]["cust_data_source_name"],"otherConfiguration":{"cron":"0 * * * * ? ","scheduleType":"event","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}],"transferType":1,"auditMind":"yyy","enabled":1,"tenantId":application[0]["tenant_id"],"owner":application[0]["owner"],"creator":application[0]["creator"],"createTime":"2021-07-15 18:19:03","lastModifiedTime":"2021-07-15 18:19:03","description":"","id":application[0]["id"],"dataResId":application[0]["data_res_id"],"custDataSourceId":application[0]["cust_data_source_id"],"custTableName":"","custAppId":"","custAppName":"","serviceMode":0,"expiredTime":"0","status":0,"sourceType":"DATASET","type":1}
            return new_data
        elif 'test_cron_oracle' in data:
            new_data = {"applicationId":application[0]["id"],"name":application[0]["name"],"dataResName":application[0]["data_res_name"],"lastModifier":"admin","custDataSourceName":application[0]["cust_data_source_name"],"otherConfiguration":{"cron":"0 0/5 * * * ? ","scheduleType":"cron","startTime":"1613836798051","endTime":"1613923198051"},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}],"transferType":1,"auditMind":"yyy","enabled":1,"tenantId":application[0]["tenant_id"],"owner":application[0]["owner"],"creator":application[0]["creator"],"createTime":"2021-07-15 18:19:03","lastModifiedTime":"2021-07-15 18:19:03","description":"","id":application[0]["id"],"dataResId":application[0]["data_res_id"],"custDataSourceId":application[0]["cust_data_source_id"],"custTableName":application[0]["cust_table_name"],"custAppId":"","custAppName":"","serviceMode":1,"expiredTime":"0","status":0,"sourceType":"DATASET","type":1}
            return new_data
        elif 'test_once_hive_txt' in data:
            new_data = {"applicationId":application[0]["id"],"name":application[0]["name"],"dataResName":application[0]["data_res_name"],"lastModifier":"admin","custDataSourceName":application[0]["cust_data_source_name"],"otherConfiguration":{"cron":"0 * * * * ? ","scheduleType":"once","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}],"transferType":1,"auditMind":"yyy","enabled":1,"tenantId":application[0]["tenant_id"],"owner":application[0]["owner"],"creator":application[0]["creator"],"createTime":"2021-07-15 18:19:03","lastModifiedTime":"2021-07-15 18:19:03","description":"","id":application[0]["id"],"dataResId":application[0]["data_res_id"],"custDataSourceId":application[0]["cust_data_source_id"],"custTableName":"","custAppId":"","custAppName":"","serviceMode":1,"expiredTime":"0","status":0,"sourceType":"DATASET","type":1}
            return new_data
        elif 'test_event_hbase_txt' in data:
            new_data = {"applicationId":application[0]["id"],"name":application[0]["name"],"dataResName":application[0]["data_res_name"],"lastModifier":"customer3","custDataSourceName":application[0]["cust_data_source_name"],"otherConfiguration":{"cron":"0 0/5 * * * ? *","scheduleType":"event","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"id","sourceType":"int","targetField":"id","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ts","sourceType":"timestamp","targetField":"ts","targetType":"timestamp","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"code","sourceType":"string","targetField":"code","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"total","sourceType":"float","targetField":"total","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"forward_total","sourceType":"float","targetField":"forward_total","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"reverse_total","sourceType":"float","targetField":"reverse_total","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sum_flow","sourceType":"float","targetField":"sum_flow","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sum_inst","sourceType":"float","targetField":"sum_inst","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"inst_num","sourceType":"int","targetField":"inst_num","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"max_inst","sourceType":"float","targetField":"max_inst","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"max_inst_ts","sourceType":"string","targetField":"max_inst_ts","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"min_inst","sourceType":"float","targetField":"min_inst","targetType":"float","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"min_inst_ts","sourceType":"string","targetField":"min_inst_ts","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}],"transferType":1,"auditMind":"yyy","enabled":1,"tenantId":application[0]["tenant_id"],"owner":application[0]["owner"],"creator":application[0]["creator"],"createTime":"2021-07-15 18:19:03","lastModifiedTime":"2021-07-15 18:19:03","description":"","id":application[0]["id"],"dataResId":application[0]["data_res_id"],"custDataSourceId":application[0]["cust_data_source_id"],"custTableName":"","custAppId":"","custAppName":"","serviceMode":0,"expiredTime":"0","status":0,"sourceType":"DATASET","type":1}
            return new_data
        elif 'test_once_es_txt' in data:
            new_data = {"applicationId":application[0]["id"],"name":application[0]["name"],"dataResName":application[0]["data_res_name"],"lastModifier":"customer3","custDataSourceName":application[0]["cust_data_source_name"],"otherConfiguration":{"cron":"0 0/5 * * * ? *","scheduleType":"once","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}],"transferType":1,"auditMind":"","enabled":1,"tenantId":application[0]["tenant_id"],"owner":application[0]["owner"],"creator":application[0]["creator"],"createTime":"2021-07-15 20:17:29","lastModifiedTime":"2021-07-15 20:17:29","description":"","id":application[0]["id"],"dataResId":application[0]["data_res_id"],"custDataSourceId":application[0]["cust_data_source_id"],"custTableName":"","custAppId":"","custAppName":"","serviceMode":0,"expiredTime":"0","status":0,"sourceType":"DATASET","type":1}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_customer(data):
    try:
        sql = "select id,owner,tenant_id,username from dsp_customer where enabled=0 and name like '%s%%%%' order by create_time desc limit 1" % data
        customer_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'gjb_test009_' in data:
            new_data = {"username":customer_info[0]["username"],"name":"gjb_test009_随机数","password":"e10adc3949ba59abbe56e057f20f883e","checkPassword":"e10adc3949ba59abbe56e057f20f883e","enabled":0,"tenantId":customer_info[0]["tenant_id"],"owner":customer_info[0]["owner"],"creator":"admin","createTime":"2020-11-03 16:18:04","lastModifier":"admin","lastModifiedTime":"2021-02-20 18:16:37","id":customer_info[0]["id"],"expiredPeriod":"0"}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_user(data):
    try:
        sql = "select id,owner,tenant_id,login_id from merce_user where enabled=0 and name like '%s%%%%' order by create_time desc limit 1" % data
        user_info = ms.ExecuQuery(sql.encode('utf-8'))
        if 'autotest_dsp_' in data:
            new_data = {"confirmPassword":"","email":"11@11.com","loginId":user_info[0]["login_id"],"name":"autotest_dsp_随机数","password":"AES(aa920aacdab0d8f75bc3d04b3d84586d9825e2b2b2842d7a480a3e06c888c2d848d1144f4813e55d5c0807dae20acd80)","phone":"13111111111","resourceQueues":["default"],"enabled":0,"tenantId":user_info[0]["tenant_id"],"owner":user_info[0]["owner"],"creator":"admin","createTime":"2021-02-15 00:20:49","lastModifier":"admin","lastModifiedTime":"2021-02-20 19:03:42","id":user_info[0]["id"],"pwdExpiredTime":"2022-05-15","accountExpiredTime":"2022-08-15","hdfsSpaceQuota":"0","admin":0,"clientIds":"dsp","roles":[],"expiredPeriod":"0"}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" %e)