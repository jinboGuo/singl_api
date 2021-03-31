# coding:utf-8
import os
from new_api_cases.dw_deal_parameters import deal_random
from basic_info.setting import Dw_MySQL_CONFIG, dw_host
from util.Open_DB import MYSQL
from util.get_tenant import get_tenant, get_owner
from util.timestamp_13 import datatime_now
from util.logs import Logger

ms = MYSQL(Dw_MySQL_CONFIG["HOST"], Dw_MySQL_CONFIG["USER"], Dw_MySQL_CONFIG["PASSWORD"], Dw_MySQL_CONFIG["DB"])
log = Logger().get_log()

def query_subject_data(data):
    try:
        sql = "select id from dw_business where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        business_id = flow_info[0]["id"]
        new_data = {"params": {"pageable": {"pageNum": 0, "pageSize": 8, "pageable": "true"}}}
        return new_data, business_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_business_data(data):
    try:
        sql = "select id from dw_business where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        business_id = flow_info[0]["id"]
        new_data = {"id": business_id, "tenantId": get_tenant(dw_host), "owner": get_owner(), "creator": "admin", "createTime": datatime_now(), "lastModifier": "admin", "lastModifiedTime": datatime_now(), "name": "api_auto_business随机数", "alias": "api_business随机数","abbr":"api_auto_business随机数","description":"api_auto_business","dt":"dt","bizDate":"yyyyMMddHH","flowId":"","flowName":"","schedulerId":"","physicalStatus":"READY","deployStatus":"offline"}
        deal_random(new_data)
        return new_data, business_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def add_subject_data(data):
    try:
        sql = "select id from dw_business where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        business_id = flow_info[0]["id"]
        new_data = {"alias": "api_subject", "abbr": "api_subject随机数", "name": "api_auto_subject随机数", "description":"api_auto_subject","bussinessId": business_id}
        deal_random(new_data)
        return new_data, business_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_subject_data(data):
    try:
        sql = "select id,business_id from dw_subject where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        subject_id, business_id = flow_info[0]["id"], flow_info[0]["business_id"]
        new_data = {"id": subject_id, "tenantId": get_tenant(dw_host), "owner": get_owner(), "creator": "admin", "createTime": datatime_now(), "lastModifier": "admin","lastModifiedTime": datatime_now(), "name": "api_auto_subject随机数", "alias": "api_subject随机数","abbr":"api_subject随机数", "businessId": business_id, "parentId":"0","description":"api_auto_subject","children":[],"selfCode":"758639635533398016","parentCode":"0"}
        deal_random(new_data)
        return new_data, subject_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def add_projects_data(data):
    try:
        sql = "select id from dw_business where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        business_id = flow_info[0]["id"]
        new_data = {"alias": "api_projects", "name": "api_auto_projects随机数", "type": "", "description": "api_auto_projects","businessId": business_id, "abbr": "api_projects随机数"}
        deal_random(new_data)
        return new_data, business_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_projects_data(data):
    try:
        sql = "select id,business_id from dw_project where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        project_id, business_id = flow_info[0]["id"], flow_info[0]["business_id"]
        new_data = {"id": project_id, "tenantId": get_tenant(dw_host), "owner": get_owner(), "creator": "admin", "createTime": datatime_now(), "lastModifier": "admin","lastModifiedTime": datatime_now(), "name": "api_auto_projects随机数", "alias": "api_projects", "abbr": "api_projects随机数", "businessId": business_id, "description": "api_auto_projects"}
        deal_random(new_data)
        return new_data, project_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_tag_data(data):
    try:
        sql = "select id from dw_tagdef where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        tag_id = flow_info[0]["id"]
        new_data = {"id": tag_id, "tenantId": get_tenant(dw_host), "owner": get_owner(), "creator": "admin", "createTime": datatime_now(), "lastModifier": "admin", "lastModifiedTime": datatime_now(), "name": "api_auto_tag随机数", "alias": "api_tag随机数", "abbr": "", "parentTagOption": "", "options": [{"name":"大","alias":"big","orderNum":""},{"name":"小","alias":"small","orderNum":""},{"name":"长","alias":"long","orderNum":""}],"description":"api_auto_tag","scope":"","isSetName":1}
        deal_random(new_data)
        return new_data, tag_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def add_taggroup_data(data):
    try:
        sql = "select id from dw_tagdef where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        tag_id = flow_info[0]["id"]
        new_data = {"alias": "api_taggroup随机数", "createTime": "", "description": "api_auto_taggroup", "tagIds": tag_id, "name": "api_auto_taggroup随机数"}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_taggroup_data(data):
    try:
        sql = "select id,tag_ids from dw_taggroup where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        taggroup_id, tag_id = flow_info[0]["id"], flow_info[0]["tag_ids"]
        new_data = {"id": taggroup_id, "tenantId": get_tenant(dw_host), "owner": get_owner(), "creator": "admin", "createTime": datatime_now(), "lastModifier": "admin", "lastModifiedTime": datatime_now(), "name":"api_auto_taggroup随机数","alias":"api_taggroup随机数", "abbr": "", "description": "", "tagIds": tag_id}
        deal_random(new_data)
        return new_data, taggroup_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_namerule_data(data):
    try:
        sql = "select id from dw_name_rules where alias like '%s%%%%'  order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        namerule_id = flow_info[0]["id"]
        new_data = {"id": namerule_id, "tenantId": get_tenant(dw_host), "owner": get_owner(), "creator": "admin", "createTime": datatime_now(), "lastModifier": "admin", "lastModifiedTime": datatime_now(), "name": "metadata_subject", "alias": "api_auto_namerule随机数","abbr":"api_rule随机数","description":"","rules":"metadata"}
        deal_random(new_data)
        return new_data, namerule_id
    except Exception as e:
        log.error("异常信息：%s" %e)