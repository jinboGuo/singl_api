# coding:utf-8
import json
import os
import random
import requests
from basic_info.get_auth_token import get_headers
from new_api_cases.dw_deal_parameters import deal_random
from basic_info.setting import log, ms
from util.get_deal_parameter import get_tenant_id, get_owner
from util.timestamp_13 import datatime_now, data_now

woven_dir = os.path.join(os.path.abspath('.'),'attachment\\import_autotest_api_df.woven').replace('\\','/')


def get_asset_directory(data):
    try:
        merce_resource_dir = "delete from merce_resource_dir where res_type ='assets_dir' and name like 'test_asset%' order by create_time desc"
        ms.ExecuNoQuery(merce_resource_dir.encode('utf-8'))
        assets_info = "delete from assets_info where name like 'test_asset%'"
        ms.ExecuNoQuery(assets_info.encode('utf-8'))
        assets_info = "delete from assets_info where name like 'test_sql_asset_gjb%'"
        ms.ExecuNoQuery(assets_info.encode('utf-8'))
        assets_info = "delete from assets_info where name like 'gjb_ttest_hdfs042219%'"
        ms.ExecuNoQuery(assets_info.encode('utf-8'))
        assets_info = "delete from assets_info where name like 'training%'"
        ms.ExecuNoQuery(assets_info.encode('utf-8'))
        new_data = {"name": "test_asset随机数", "parentId": data, "resType": "assets_dir"}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def update_asset_directory(data):
    try:
        asset_directory = "select parent_id,tenant_id,id,ord,path from merce_resource_dir mrd where res_type ='assets_dir' and creator='admin' and name like'%s%%%%' order by create_time desc limit 1" %data
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        new_data = {"name":"test_asset随机数","parentId":asset_directory_info[0]["parent_id"],"resType":"assets_dir","tenantId":asset_directory_info[0]["tenant_id"],"owner":None,"enabled":None,"creator":None,"createTime":None,"lastModifier":None,"lastModifiedTime":None,"id":asset_directory_info[0]["id"],"version":None,"groupCount":None,"groupFieldValue":None,"order":asset_directory_info[0]["ord"],"isHide":None,"path":asset_directory_info[0]["path"],"children":[],"halfSelect":True,"hasChildren":False,"expiredPeriod":0}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def move_asset_directory(data):
    try:
        asset_directory = "select parent_id,id from merce_resource_dir mrd where res_type ='assets_dir' and creator='admin' and name like'%s%%%%' order by create_time desc limit 1" %data
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        new_data= [asset_directory_info[0]["id"]]
        return asset_directory_info[0]["parent_id"], new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def duplicate_asset_directory(data):
    try:
        asset_directory = "select parent_id,name from merce_resource_dir mrd where res_type ='assets_dir' and creator='admin' and name like'%s%%%%' order by create_time desc limit 1" %data
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        new_data = {"name": asset_directory_info[0]["name"], "parentId": asset_directory_info[0]["parent_id"], "resType": "assets_dir"}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def duplicate_move_asset_directory(data):
    try:
        asset_directory = "select name,parent_id,tenant_id,id,ord,path from merce_resource_dir mrd where res_type ='assets_dir' and creator='admin' and name like'%s%%%%' order by create_time desc limit 1" %data
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        new_data = {"name":asset_directory_info[0]["name"],"parentId":asset_directory_info[0]["parent_id"],"resType":"assets_dir","tenantId":asset_directory_info[0]["tenant_id"],"owner":None,"enabled":None,"creator":None,"createTime":None,"lastModifier":None,"lastModifiedTime":None,"id":asset_directory_info[0]["id"],"version":None,"groupCount":None,"groupFieldValue":None,"order":asset_directory_info[0]["ord"],"isHide":None,"path":asset_directory_info[0]["path"],"children":[],"halfSelect":True,"hasChildren":False,"expiredPeriod":0}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def delete_asset_directory(data):
    try:
        asset_directory = "select id from merce_resource_dir where res_type ='assets_dir' and name like'%s%%%%' order by create_time desc limit 1" % data
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        new_data = {"data": str(asset_directory_info[0]["id"])}
        return asset_directory_info[0]["id"], new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def query_data_tier():
    try:
        dw_data_tier = "SELECT id FROM dw_data_tier where status ='ONLINE' order by create_time desc limit 1"
        dw_data_tier = ms.ExecuQuery(dw_data_tier.encode('utf-8'))
        if len(dw_data_tier) == 0:
            data = ["123456"]
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"dataTierIds","oper":"EQUAL","value":data}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        else:
            data = [str(dw_data_tier[0]["id"])]
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"dataTierIds","oper":"EQUAL","value":data}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def query_subject_domain():
    try:
        dw_subject_domain = "SELECT id FROM dw_subject_domain where status ='ONLINE' order by create_time desc limit 1"
        dw_subject_domain = ms.ExecuQuery(dw_subject_domain.encode('utf-8'))
        if len(dw_subject_domain) == 0:
            data = ["123456"]
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"domainIds","oper":"EQUAL","value":data}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        else:
            data = [str(dw_subject_domain[0]["id"])]
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"domainIds","oper":"EQUAL","value":data}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)


def create_sql_asset(data):
    try:
        data = data.split("&")
        asset_directory = "select id,name,owner,creator from merce_resource_dir where res_type ='assets_dir' and name like'%s%%%%' order by create_time desc limit 1" %data[1]
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        dataset = "select id,name from merce_dataset where name like'%s%%%%' order by create_time desc limit 1" %data[0]
        dataset = ms.ExecuQuery(dataset.encode('utf-8'))
        new_data = {"assetModel":1,"resourceId":asset_directory_info[0]["id"],"updateFrequency":"","resourceName":asset_directory_info[0]["name"],"name":"gjb_test_asset_sql随机数","sqlStr":"select * from `gjb_ttest_hdfs042219`","isShare":0,"datasetName":dataset[0]["name"],"datasetId":dataset[0]["id"],"description":"","tags":[],"tagIds":[],"tagsList":[],"providerDepartment":"","businessDirector":"","businessTelephone":"","technicalDepartment":"","technicalDirector":"","technicalTelephone":"","resourceFormat":"","approverName":asset_directory_info[0]["creator"],"approverId":asset_directory_info[0]["owner"],"isSave":True}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def batch_create_asset(data):
    try:
        data = data.split("&")
        asset_directory = "select id,name,owner,creator from merce_resource_dir where res_type ='assets_dir' and name like'%s%%%%' order by create_time desc limit 1" %data[2]
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        dataset1 = "select id,name from merce_dataset where name like'%s%%%%' order by create_time desc limit 1" %data[0]
        dataset1 = ms.ExecuQuery(dataset1.encode('utf-8'))
        dataset2 = "select id,name from merce_dataset where name like'%s%%%%' order by create_time desc limit 1" %data[1]
        dataset2 = ms.ExecuQuery(dataset2.encode('utf-8'))
        new_data = [{"assetModel":0,"resourceId":asset_directory_info[0]["id"],"resourceName":asset_directory_info[0]["name"],"updateFrequency":"","name":"gjb_test_asset随机数","isShare":0,"datasetName":dataset2[0]["name"],"datasetId":dataset2[0]["id"],"description":None,"tags":[],"tagIds":[],"tagsList":[],"providerDepartment":"","businessDirector":"","businessTelephone":"","technicalDepartment":"","technicalDirector":"","technicalTelephone":"","resourceFormat":"JDBC","approverName":asset_directory_info[0]["creator"],"approverId":asset_directory_info[0]["owner"]},{"assetModel":0,"resourceId":asset_directory_info[0]["id"],"resourceName":asset_directory_info[0]["name"],"updateFrequency":"","name":"gjb_test_asset随机数","isShare":0,"datasetName":dataset1[0]["name"],"datasetId":dataset1[0]["id"],"description":None,"tags":[],"tagIds":[],"tagsList":[],"providerDepartment":"","businessDirector":"","businessTelephone":"","technicalDepartment":"","technicalDirector":"","technicalTelephone":"","resourceFormat":"FILE","approverName":asset_directory_info[0]["creator"],"approverId":asset_directory_info[0]["owner"]}]
        for data in new_data:
         deal_random(data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def update_asset(data):
    try:
        assets_info = "select asset_model,id,resource_id,resource_name,dataset_id,dataset_name,tenant_id,owner,status,approver_name,approver_id,creator,last_modifier from assets_info where status in('OFFLINE','SAVED') and name like '%s%%%%' order by create_time desc limit 1" %data
        assets_info = ms.ExecuQuery(assets_info.encode('utf-8'))
        if "gjb_test_asset" == data:
            new_data = {"assetModel":assets_info[0]["asset_model"],"storage":"HDFS","resourceId":assets_info[0]["resource_id"],"resourceName":assets_info[0]["resource_name"],"name":"gjb_test_asset随机数","isShare":0,"updateFrequency":None,"datasetName":assets_info[0]["dataset_name"],"datasetId":assets_info[0]["dataset_id"],"description":"创建数据集资产","tags":[],"tagIds":[],"tagsList":[],"providerDepartment":"","businessDirector":"","businessTelephone":"","technicalDepartment":"","technicalDirector":"","technicalTelephone":"","resourceFormat":"FILE","approverName":assets_info[0]["approver_name"],"approverId":assets_info[0]["approver_id"],"id":assets_info[0]["id"],"tenantId":assets_info[0]["tenant_id"],"owner":assets_info[0]["owner"],"enabled":None,"creator":assets_info[0]["creator"],"createTime":data_now(),"lastModifier":assets_info[0]["last_modifier"],"lastModifiedTime":data_now(),"visibleToAll":False,"approverRemarks":"","approvalRecordId":None,"approvalComments":"","status":assets_info[0]["status"],"publishTime":None,"sqlStr":"","order":None,"expiredPeriod":0,"recordNumber":0,"byteSize":0,"type":"FLOW","resource":None,"isHide":0,"scheduleType":"","serviceMode":None,"dataTierIds":[],"dataTierName":"","domainIds":[],"domainNames":[],"lifeCycleType":"","shareStatus":None,"assetsShareId":None,"assetsShare":None,"shareMode":"","collectionTimes":0,"shareTimes":0,"departmentTimes":0,"organizationName":"","keywords":"","isCollection":0,"viewNum":1,"readNum":0,"dataPullNum":0,"dataPushNum":0,"dataShareNum":None,"recentViewNum":0}
            deal_random(new_data)
            return assets_info[0]["id"], new_data
        elif "gjb_test_asset_sql" == data:
            new_data = {"assetModel":assets_info[0]["asset_model"],"resourceId":assets_info[0]["resource_id"],"updateFrequency":None,"resourceName":assets_info[0]["resource_name"],"name":"gjb_test_asset_sql随机数","sqlStr":"select * from `gjb_ttest_hdfs042219`","isShare":0,"datasetName":assets_info[0]["dataset_name"],"datasetId":assets_info[0]["dataset_id"],"description":"","tags":[],"tagIds":[],"tagsList":[],"providerDepartment":"","businessDirector":"","businessTelephone":"","technicalDepartment":"","technicalDirector":"","technicalTelephone":"","resourceFormat":None,"approverName":assets_info[0]["approver_name"],"approverId":assets_info[0]["approver_id"],"isSave":True,"id":assets_info[0]["id"],"tenantId":assets_info[0]["tenant_id"],"owner":assets_info[0]["owner"],"enabled":None,"creator":assets_info[0]["creator"],"createTime":data_now(),"lastModifier":assets_info[0]["last_modifier"],"lastModifiedTime":data_now(),"visibleToAll":False,"approverRemarks":"","approvalRecordId":None,"approvalComments":"","status":assets_info[0]["status"],"publishTime":None,"order":None,"expiredPeriod":0,"recordNumber":0,"byteSize":0,"storage":"HDFS","type":"FLOW","resource":None,"isHide":0,"scheduleType":"","serviceMode":None,"dataTierIds":[],"dataTierName":"","domainIds":[],"domainNames":[],"lifeCycleType":"","shareStatus":None,"assetsShareId":None,"assetsShare":None,"shareMode":"","collectionTimes":0,"shareTimes":0,"departmentTimes":0,"organizationName":"","keywords":"","isCollection":0,"viewNum":1,"readNum":0,"dataPullNum":0,"dataPushNum":0,"dataShareNum":None,"recentViewNum":0}
            deal_random(new_data)
            return assets_info[0]["id"], new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)

def sql_analyse_data(data):
    try:
        dataset = "select id from merce_dataset where name like '%s%%%%' order by create_time desc limit 1" %data
        dataset_info = ms.ExecuQuery(dataset.encode('utf-8'))
        new_data = {"sql":"select `gjb_ttest_hdfs042219`. `id`  , `gjb_ttest_hdfs042219`. `ts`  , `gjb_ttest_hdfs042219`. `code`  , `gjb_ttest_hdfs042219`. `total`  , `gjb_ttest_hdfs042219`. `forward_total`  , `gjb_ttest_hdfs042219`. `reverse_total`  , `gjb_ttest_hdfs042219`. `sum_flow`  from `gjb_ttest_hdfs042219`","ids":dataset_info[0]["id"]}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)

# 解析SQL字段后，初始化Sql任务，返回statement id，执行SQL语句使用
def get_improt_data(headers, host):
    url = '%s/api/mis/upload' % host
    fs = {"file": open(woven_dir, 'rb')}
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
    except KeyError:
        return