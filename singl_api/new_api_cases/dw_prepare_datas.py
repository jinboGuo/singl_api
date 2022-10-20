# coding:utf-8
import json
import os
import random
import requests
from basic_info.get_auth_token import get_headers, get_headers_dw
from new_api_cases.dw_deal_parameters import deal_random
from basic_info.setting import Dw_MySQL_CONFIG, dw_host
from util.Open_DB import MYSQL
from util.get_tenant import get_tenant, get_owner
from util.timestamp_13 import datatime_now, data_now
from util.logs import Logger

ms = MYSQL(Dw_MySQL_CONFIG["HOST"], Dw_MySQL_CONFIG["USER"], Dw_MySQL_CONFIG["PASSWORD"], Dw_MySQL_CONFIG["DB"], Dw_MySQL_CONFIG["PORT"])
log = Logger().get_log()
woven_dir = os.path.join(os.path.abspath('.'),'attachment\import_autotest_api_df.woven')

def query_subject_data(data):
    try:
        sql = "select id from dw_business where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        business_id = flow_info[0]["id"]
        new_data = {"params": {"pageable": {"pageNum": 0, "pageSize": 8, "pageable": "True"}}}
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
        new_data = {"alias": "api_auto_subject", "abbr": "api_auto_subject随机数", "name": "api_auto_subject随机数", "description":"api_auto_subject","bussinessId": business_id}
        deal_random(new_data)
        return new_data, business_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_subject_data(data):
    try:
        sql = "select id,business_id from dw_subject where name like '%s%%%%' order by create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        subject_id, business_id = flow_info[0]["id"], flow_info[0]["business_id"]
        new_data = {"id": subject_id, "tenantId": get_tenant(dw_host), "owner": get_owner(), "creator": "admin", "createTime": datatime_now(), "lastModifier": "admin","lastModifiedTime": datatime_now(), "name": "api_auto_subject随机数", "alias": "api_auto_subject随机数","abbr":"api_auto_subject随机数", "businessId": business_id, "parentId":"0","description":"api_auto_subject","children":[],"selfCode":"758639635533398016","parentCode":"0"}
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

def rel_product_taggroup(data):
    try:
        data = data.split("#")
        tag_group = "select id from dw_taggroup where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        tag_group_info = ms.ExecuQuery(tag_group.encode('utf-8'))
        project = "select id from dw_project where name like '%s%%%%' order by create_time desc limit 1" %data[1]
        project_info = ms.ExecuQuery(project.encode('utf-8'))
        nameRule = "select id from dw_name_rules where alias like '%s%%%%'  order by create_time desc limit 1" %data[2]
        nameRule_info = ms.ExecuQuery(nameRule.encode('utf-8'))
        new_data = {"aggregateId":tag_group_info[0]["id"],"dimensionId":tag_group_info[0]["id"],"projectId":project_info[0]["id"],"nameRuleId":nameRule_info[0]["id"],"subjectId":"0","consolidatedId":tag_group_info[0]["id"],"transactionId":tag_group_info[0]["id"]}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)

def add_model_category(data):
    try:
        project = "select id from dw_project where name like '%s%%%%'order by create_time desc limit 1" %data
        project_info=ms.ExecuQuery(project.encode('utf-8'))
        new_data={"abbr":"","alias":"","name":"api_model随机数","categorySource":"model","subjectId":"0","description":"","projectId":project_info[0]["id"]}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_model_category(data):
    try:
        model_category = "select id,business_id,project_id,parent_id from dw_category where name like '%s%%%%' order by create_time desc limit 1" %data
        model_category_info = ms.ExecuQuery(model_category.encode('utf-8'))
        model_category_id = model_category_info[0]["id"]
        new_data = {"id":model_category_info[0]["id"],"tenantId":get_tenant(dw_host),"owner":get_owner(),"creator":"admin","createTime":datatime_now(),"lastModifier":"admin","lastModifiedTime":datatime_now(),"name":"api_model随机数","alias":"","abbr":"","description":"","businessId":model_category_info[0]["business_id"],"projectId":model_category_info[0]["project_id"],"subjectId":"0","parentId":model_category_info[0]["parent_id"],"categorySource":"model","order":1,"children":[],"parentCode":model_category_info[0]["parent_id"],"selfCode":model_category_info[0]["id"]}
        deal_random(new_data)
        return new_data, model_category_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def add_child_model_category(data):
    try:
        data = data.split("#")
        project = "select id from dw_project where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        project_info = ms.ExecuQuery(project.encode('utf-8'))
        model_category = "select id from dw_category where name like '%s%%%%' order by create_time desc limit 1" %data[1]
        model_category_info = ms.ExecuQuery(model_category.encode('utf-8'))
        model_category_id = model_category_info[0]["id"]
        new_data = {"abbr":"","alias":"","name":"api_model随机数","categorySource":"model","subjectId":"0","description":"","projectId":project_info[0]["id"]}
        deal_random(new_data)
        return new_data,model_category_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def add_standard_category(data):
    try:
        project = "select id from dw_project where name like '%s%%%%'order by create_time desc limit 1" %data
        project_info=ms.ExecuQuery(project.encode('utf-8'))
        new_data={"abbr":"","alias":"","name":"api_standard随机数","categorySource":"standard","subjectId":"0","description":"","projectId":project_info[0]["id"]}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_standard_category(data):
    try:
        standard_category = "select id,business_id,project_id,parent_id from dw_category where name like '%s%%%%' order by create_time desc limit 1" %data
        standard_category_info = ms.ExecuQuery(standard_category.encode('utf-8'))
        standard_category_id = standard_category_info[0]["id"]
        new_data = {"id":standard_category_info[0]["id"],"tenantId":get_tenant(dw_host),"owner":get_owner(),"creator":"admin","createTime":datatime_now(),"lastModifier":"admin","lastModifiedTime":datatime_now(),"name":"api_standard随机数","alias":"","abbr":"","description":"","businessId":standard_category_info[0]["business_id"],"projectId":standard_category_info[0]["project_id"],"subjectId":"0","parentId":standard_category_info[0]["parent_id"],"categorySource":"standard","order":1,"children":[],"parentCode":standard_category_info[0]["parent_id"],"selfCode":standard_category_info[0]["id"]}
        deal_random(new_data)
        return new_data, standard_category_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def add_child_standard_category(data):
    try:
        data = data.split("#")
        project = "select id from dw_project where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        project_info = ms.ExecuQuery(project.encode('utf-8'))
        standard_category = "select id from dw_category where name like '%s%%%%' order by create_time desc limit 1" %data[1]
        standard_category_info = ms.ExecuQuery(standard_category.encode('utf-8'))
        standard_category_id = standard_category_info[0]["id"]
        new_data = {"abbr":"","alias":"","name":"api_standard随机数","categorySource":"standard","subjectId":"0","description":"","projectId":project_info[0]["id"]}
        deal_random(new_data)
        return new_data,standard_category_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def rel_physical_dataset(data):
    try:
        data = data.split("#")
        category = "select id,business_id,project_id from dw_category where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        category_info = ms.ExecuQuery(category.encode('utf-8'))
        subject = "select id from dw_subject where name like '%s%%%%' order by create_time desc limit 1" %data[1]
        subject_info = ms.ExecuQuery(subject.encode('utf-8'))
        dataset = "select id,name from merce_dataset where name like '%s%%%%'  order by create_time desc limit 1" %data[2]
        dataset_info = ms.ExecuQuery(dataset.encode('utf-8'))
        new_data = {"categoryId":category_info[0]["id"],"alias":"订单明细","name":"api_order_physical随机数","datasetId":dataset_info[0]["id"],"datasetName":dataset_info[0]["name"],"subjectId":subject_info[0]["id"],"projectId":category_info[0]["project_id"],"businessId":category_info[0]["business_id"]}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_physical_dataset(data):
    try:
        ref_dataset = "select id,business_id,project_id,subject_id,dataset_id,dataset_name,category_id from dw_ref_dataset where name like '%s%%%%' order by create_time desc limit 1" %data
        ref_dataset_info = ms.ExecuQuery(ref_dataset.encode('utf-8'))
        ref_dataset_id = ref_dataset_info[0]["id"]
        new_data = {"id":ref_dataset_info[0]["id"],"tenantId":get_tenant(dw_host),"owner":get_owner(),"creator":"admin","createTime":datatime_now(),"lastModifier":"admin","lastModifiedTime":datatime_now(),"name":"api_order_physical随机数","alias":"订单明细","businessId":ref_dataset_info[0]["business_id"],"subjectId":ref_dataset_info[0]["subject_id"],"projectId":ref_dataset_info[0]["project_id"],"datasetId":ref_dataset_info[0]["dataset_id"],"description":"","datasetName":ref_dataset_info[0]["dataset_name"],"categoryId":ref_dataset_info[0]["category_id"],"fields":[]}
        deal_random(new_data)
        return new_data, ref_dataset_id
    except Exception as e:
        log.error("异常信息：%s" %e)


def add_physical_field(data):
    try:
        data = data.split("#")
        metadata = "select id from dw_ref_dataset where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        metadata_id = metadata_info[0]["id"]
        dw_field_defined = "select id,alias,field_type,name from dw_field_defined where name like '%s%%%%' and field_spec='%s' order by create_time desc limit 1" %(data[1],data[3])
        field_defined_info = ms.ExecuQuery(dw_field_defined.encode('utf-8'))
        new_data = [{"alias":field_defined_info[0]["alias"],"fieldSpec":data[3],"fieldType":field_defined_info[0]["field_type"],"length":22,"name":field_defined_info[0]["name"],"fieldSource":data[2],"associatedFieldName":field_defined_info[0]["alias"],"precision":"None","unit":"None","objectId":field_defined_info[0]["id"]}]
        return new_data,metadata_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def del_physical_field(data):
    try:
        data = data.split("#")
        metadata = "select id from dw_ref_dataset where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        metadata_id = metadata_info[0]["id"]
        dw_field_defined = "select name from dw_field_defined where name like '%s%%%%' and field_spec='%s' order by create_time desc limit 1" %(data[1],data[3])
        field_defined_info = ms.ExecuQuery(dw_field_defined.encode('utf-8'))
        new_data = []
        comp =field_defined_info[0]["name"]+","+data[2]
        new_data.append(str(comp))
        return new_data,metadata_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_physical_dataset(data):
    try:
        ref_dataset = "select project_id,category_id from dw_ref_dataset where name like '%s%%%%' order by create_time desc limit 1" %data
        ref_dataset_info = ms.ExecuQuery(ref_dataset.encode('utf-8'))
        ref_project_id ,ref_category_id= ref_dataset_info[0]["project_id"],ref_dataset_info[0]["category_id"]
        new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"True"}}
        deal_random(new_data)
        return new_data, ref_project_id ,ref_category_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_physical_dataset_by_name(data):
    try:
        ref_dataset = "select project_id,category_id from dw_ref_dataset where name like '%s%%%%' order by create_time desc limit 1" %data
        ref_dataset_info = ms.ExecuQuery(ref_dataset.encode('utf-8'))
        ref_project_id ,ref_category_id= ref_dataset_info[0]["project_id"],ref_dataset_info[0]["category_id"]
        new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%订单%"]}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"True"}}
        deal_random(new_data)
        return new_data, ref_project_id ,ref_category_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_physical_dataset_by_subject(data):
    try:
        ref_dataset = "select project_id,category_id,subject_id from dw_ref_dataset where name like '%s%%%%' order by create_time desc limit 1" %data
        ref_dataset_info = ms.ExecuQuery(ref_dataset.encode('utf-8'))
        ref_project_id ,ref_category_id= ref_dataset_info[0]["project_id"],ref_dataset_info[0]["category_id"]
        subject=[]
        subject.append(ref_dataset_info[0]["subject_id"])
        new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"AND","name":"subjectId","oper":"EQUAL","value":subject}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"True"}}
        deal_random(new_data)
        return new_data, ref_project_id ,ref_category_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def add_model_metadata(data):
    try:
        data = data.split("#")
        category = "select id,project_id from dw_category where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        category_info = ms.ExecuQuery(category.encode('utf-8'))
        subject = "select id from dw_subject where name like '%s%%%%' order by create_time desc limit 1" %data[1]
        subject_info = ms.ExecuQuery(subject.encode('utf-8'))
        project_id,subject_id = category_info[0]["project_id"],subject_info[0]["id"]
        new_data = {"alias":"api_order_transaction随机数","abbr":"api_order_transaction随机数","definition":"","description":"","tableSpec":"transaction","subjectId":subject_id,"categoryId":category_info[0]["id"]}
        deal_random(new_data)
        return new_data,project_id,subject_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_model_metadata(data):
    try:
        metadata = "select id,business_id,project_id,subject_id,current_info_id,table_spec,category_id from dw_metadata where name like '%s%%%%' order by create_time desc limit 1" %data
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        metadata_id = metadata_info[0]["id"]
        new_data = {"id":metadata_id,"name":"api_order_transaction随机数","tenantId":get_tenant(dw_host),"owner":get_owner(),"creator":"admin","createTime":datatime_now(),"lastModifier":"admin","lastModifiedTime":datatime_now(),"alias":"api_order_transaction随机数","abbr":"api_order_transaction随机数","businessId":metadata_info[0]["business_id"],"subjectId":metadata_info[0]["subject_id"],"projectId":metadata_info[0]["project_id"],"lastVersion":1,"currentVersion":1,"currentInfoId":metadata_info[0]["current_info_id"],"tableSpec":metadata_info[0]["table_spec"],"description":"","subjectName":"","categoryId":metadata_info[0]["category_id"]}
        deal_random(new_data)
        return new_data,metadata_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_model_metadata(data):
    try:
        metadata = "select project_id from dw_metadata where name like '%s%%%%' order by create_time desc limit 1" %data
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        project_id = metadata_info[0]["project_id"]
        new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"True"}}
        deal_random(new_data)
        return new_data, project_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_model_metadata_by_name(data):
    try:
        metadata = "select project_id from dw_metadata where name like '%s%%%%' order by create_time desc limit 1" %data
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        project_id = metadata_info[0]["project_id"]
        new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%a%"]}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"True"}}
        deal_random(new_data)
        return new_data, project_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_model_metadata_by_subject(data):
    try:
        metadata = "select project_id,subject_id from dw_metadata where name like '%s%%%%' order by create_time desc limit 1" %data
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        project_id = metadata_info[0]["project_id"]
        subject=[]
        subject.append(metadata_info[0]["subject_id"])
        new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%a%"]},{"andOr":"AND","name":"subjectId","oper":"EQUAL","value":subject}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"True"}}
        deal_random(new_data)
        return new_data, project_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def save_model_metadata_info(data):
    try:
        data = data.split("#")
        metadata = "select id,subject_id,name,abbr,alias from dw_metadata where abbr like '%s%%%%' order by create_time desc limit 1" %data[0]
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        metadata_id,subject_id = metadata_info[0]["id"],metadata_info[0]["subject_id"]
        taggroup = "select a.id,a.tag_ids,b.name from dw_taggroup a inner join dw_tagdef b on a.tag_ids=b.id where a.name like '%s%%%%' order by a.create_time desc limit 1" %data[1]
        taggroup_info = ms.ExecuQuery(taggroup.encode('utf-8'))
        ref_dataset = "select id from dw_ref_dataset where name like '%s%%%%' order by create_time desc limit 1" %data[2]
        ref_dataset_info = ms.ExecuQuery(ref_dataset.encode('utf-8'))
        new_data = {"alias":metadata_info[0]["alias"],"name":metadata_info[0]["name"],"sourceTableIds":ref_dataset_info[0]["id"],"abbr":metadata_info[0]["abbr"],"description":"","tagValue":[{"group":taggroup_info[0]["id"],"tagId":taggroup_info[0]["tag_ids"],"tagName":taggroup_info[0]["name"],"tagType":"","tagValues":"big"}]}
        deal_random(new_data)
        return new_data,metadata_id,subject_id
    except Exception as e:
        log.error("异常信息：%s" %e)


def query_timedim(data):
    try:
        field_defined = "select project_id,category_id from dw_field_defined where name like '%s%%%%' order by create_time desc limit 1" %data
        field_defined_info = ms.ExecuQuery(field_defined.encode('utf-8'))
        project_id,category_id = field_defined_info[0]["project_id"],field_defined_info[0]["category_id"]
        new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"True"}}
        deal_random(new_data)
        return new_data,project_id,category_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_timedim_by_name(data):
    try:
        field_defined = "select project_id,category_id from dw_field_defined where name like '%s%%%%' order by create_time desc limit 1" %data
        field_defined_info = ms.ExecuQuery(field_defined.encode('utf-8'))
        project_id,category_id = field_defined_info[0]["project_id"],field_defined_info[0]["category_id"]
        new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%时间%"]}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"True"}}
        deal_random(new_data)
        return new_data,project_id,category_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_timedim_by_subject(data):
    try:
        field_defined = "select project_id,category_id,subject_id from dw_field_defined where name like '%s%%%%' order by create_time desc limit 1" %data
        field_defined_info = ms.ExecuQuery(field_defined.encode('utf-8'))
        project_id,category_id = field_defined_info[0]["project_id"],field_defined_info[0]["category_id"]
        subject_id=[]
        subject_id.append(field_defined_info[0]["subject_id"])
        new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"AND","name":"subjectId","oper":"EQUAL","value":subject_id}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"True"}}
        deal_random(new_data)
        return new_data,project_id,category_id
    except Exception as e:
        log.error("异常信息：%s" %e)


def add_primary(data):
    try:
        data = data.split("#")
        category = "select id,project_id from dw_category where name like '%s%%%%' and category_source='standard' order by create_time desc limit 1" %data[0]
        category_info = ms.ExecuQuery(category.encode('utf-8'))
        subject = "select id from dw_subject where name like '%s%%%%' order by create_time desc limit 1" %data[1]
        subject_info = ms.ExecuQuery(subject.encode('utf-8'))
        project_id,subject_id = category_info[0]["project_id"],subject_info[0]["id"]
        new_data = {"name":data[2],"alias":data[3],"subjectId":subject_info[0]["id"],"sourceTableId":"None","fieldType":data[4],"sourceFieldName":"","definition":"","description":"","fieldSource":data[6],"categoryId":category_info[0]["id"],"length":"22","projectId":category_info[0]["project_id"],"fieldSpec":data[5]}
        deal_random(new_data)
        return new_data,project_id,subject_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_primary(data):
    try:
        data = data.split("#")
        field_defined = "select id,business_id,project_id,subject_id,field_spec,category_id from dw_field_defined where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        field_defined_info = ms.ExecuQuery(field_defined.encode('utf-8'))
        field_defined_id = field_defined_info[0]["id"]
        new_data = {"id":field_defined_info[0]["id"],"tenantId":get_tenant(dw_host),"owner":get_owner(),"creator":"admin","createTime":datatime_now(),"lastModifier":"admin","lastModifiedTime":datatime_now(),"name":data[1],"alias":data[2],"businessId":field_defined_info[0]["business_id"],"subjectId":field_defined_info[0]["subject_id"],"projectId":field_defined_info[0]["project_id"],"sourceTableName":"","sourceFieldName":"","fieldSpec":field_defined_info[0]["field_spec"],"fieldType":data[3],"description":"","categoryId":field_defined_info[0]["category_id"],"categorySource":"standard","length":22}
        deal_random(new_data)
        return new_data,field_defined_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def add_physical(data):
    try:
        metadata = "select id from dw_metadata where name like '%s%%%%' order by create_time desc limit 1" %data
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        metadata_id = metadata_info[0]["id"]
        new_data = {"physicalCycle":"M_5","partitionField":"","partitionGrain":"300","storageEngine":"hdfs","datasetName":"","datasetId":"None","metadataId":"","description":""}
        deal_random(new_data)
        return new_data,metadata_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_physical(data):
    try:
        metadata = "select id from dw_metadata where name like '%s%%%%' order by create_time desc limit 1" %data
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        metadata_id = metadata_info[0]["id"]
        new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"True"}}
        deal_random(new_data)
        return new_data,metadata_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_physical_by_name(data):
    try:
        metadata = "select id from dw_metadata where name like '%s%%%%' order by create_time desc limit 1" %data
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        metadata_id = metadata_info[0]["id"]
        new_data = {"pageable":{"pageNum":0,"pageSize":8,"pageable":"False"},"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%a%"]}]},"ordSort":[{"name":"createTime","order":"DESC"}]}
        deal_random(new_data)
        return new_data,metadata_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def update_physical(data):
    try:
        data = data.split("#")
        physical = "select id,metadata_id,name,alias,relation_id,table_spec,dataset_id,dataset_name,flow_id,flow_name from dw_metadata_to_physical where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        physical_info = ms.ExecuQuery(physical.encode('utf-8'))
        physical_id,metadata_id = physical_info[0]["id"],physical_info[0]["metadata_id"]
        new_data = {"id":physical_info[0]["id"],"tenantId":get_tenant(dw_host),"owner":get_owner(),"creator":"admin","createTime":datatime_now(),"lastModifier":"admin","lastModifiedTime":datatime_now(),"name":physical_info[0]["name"],"alias":physical_info[0]["alias"],"metadataId":physical_info[0]["metadata_id"],"datasetId":physical_info[0]["dataset_id"],"datasetName":physical_info[0]["dataset_name"],"flowId":physical_info[0]["flow_id"],"flowName":physical_info[0]["flow_name"],"relationId":physical_info[0]["relation_id"],"partitionGrain":"300","partitionField":"","storageEngine":"hdfs","physicalCycle":"M_5","description":"","enabled":0,"tableSpec":physical_info[0]["table_spec"]}
        deal_random(new_data)
        return new_data,metadata_id,physical_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def add_indicator(data):
    try:
        data = data.split("#")
        metadata = "select id,project_id,subject_id,alias from dw_metadata where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        project_id,subject_id = metadata_info[0]["project_id"],metadata_info[0]["subject_id"]
        category = "select id from dw_category where name like '%s%%%%' order by create_time desc limit 1" %data[5]
        category_info = ms.ExecuQuery(category.encode('utf-8'))
        dw_field_defined = "select name,field_type from dw_field_defined where name like '%s%%%%' order by create_time desc limit 1" %data[3]
        field_defined_info = ms.ExecuQuery(dw_field_defined.encode('utf-8'))
        new_data = {"name":data[1],"alias":data[2],"subjectId":metadata_info[0]["subject_id"],"sourceTableId":metadata_info[0]["id"],"fieldType":field_defined_info[0]["field_type"],"sourceFieldName":field_defined_info[0]["name"],"fieldSpec":"metric","definition":"","description":"","categoryId":category_info[0]["id"],"aggrMethod":data[4],"sourceTableName":metadata_info[0]["alias"],"projectId":metadata_info[0]["project_id"],"length":"33"}
        deal_random(new_data)
        return new_data,project_id,subject_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def add_dimension(data):
    try:
        data = data.split("#")
        metadata = "select id,project_id,subject_id,alias from dw_metadata where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        project_id,subject_id = metadata_info[0]["project_id"],metadata_info[0]["subject_id"]
        category = "select id from dw_category where name like '%s%%%%' order by create_time desc limit 1" %data[4]
        category_info = ms.ExecuQuery(category.encode('utf-8'))
        dw_field_defined = "select name,field_type from dw_field_defined where name like '%s%%%%' order by create_time desc limit 1" %data[3]
        field_defined_info = ms.ExecuQuery(dw_field_defined.encode('utf-8'))
        new_data = {"name":data[1],"alias":data[2],"subjectId":metadata_info[0]["subject_id"],"sourceTableId":metadata_info[0]["id"],"sourceTableName":metadata_info[0]["alias"],"sourceFieldId":"None","length":"30","categoryId":category_info[0]["id"],"sourceFieldName":field_defined_info[0]["name"],"fieldSpec":"dimension","description":"","fieldType":field_defined_info[0]["field_type"],"projectId":metadata_info[0]["project_id"]}
        deal_random(new_data)
        return new_data,project_id,subject_id
    except Exception as e:
        log.error("异常信息：%s" %e)


def add_metadata_field(data):
    try:
        data = data.split("#")
        metadata = "select metadata_id,source_table_ids from dw_metadata_info where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        metadata_id = metadata_info[0]["metadata_id"]
        dw_field_defined = "select id,alias,field_type,name from dw_field_defined where name like '%s%%%%' and field_spec='%s' order by create_time desc limit 1" %(data[1],data[3])
        field_defined_info = ms.ExecuQuery(dw_field_defined.encode('utf-8'))
        new_data = [{"alias":field_defined_info[0]["alias"],"fieldSpec":data[3],"fieldType":field_defined_info[0]["field_type"],"length":22,"name":field_defined_info[0]["name"],"fieldSource":data[2],"associatedFieldName":field_defined_info[0]["alias"],"precision":"None","unit":"None","tableSourceId":metadata_info[0]["metadata_id"],"objectId":field_defined_info[0]["id"]}]
        deal_random(new_data)
        return new_data,metadata_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def metadata_field(data):
    try:
        data = data.split("#")
        metadata = "select id from dw_metadata where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        metadata_id = metadata_info[0]["id"]
        dw_field_defined = "select name from dw_field_defined where name like '%s%%%%' and field_spec='%s' order by create_time desc limit 1" %(data[1],data[2])
        field_defined_info = ms.ExecuQuery(dw_field_defined.encode('utf-8'))
        name = field_defined_info[0]["name"]
        return metadata_id,name
    except Exception as e:
        log.error("异常信息：%s" %e)


def query_metadata_model(data):
    try:
        data = data.split("#")
        category = "select id,project_id from dw_category where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        category_info = ms.ExecuQuery(category.encode('utf-8'))
        subject = "select id from dw_subject where name like '%s%%%%' order by create_time desc limit 1" %data[1]
        subject_info = ms.ExecuQuery(subject.encode('utf-8'))
        project_id,category_id,subject_id = category_info[0]["project_id"],category_info[0]["id"],subject_info[0]["id"]
        new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"pageNum","comparatorOperator":"EQUAL","fieldValue":0},{"logicalOperator":"AND","fieldName":"pageSize","comparatorOperator":"EQUAL","fieldValue":10},{"logicalOperator":"AND","fieldName":"tableSpec","comparatorOperator":"EQUAL","fieldValue":data[2]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
        deal_random(new_data)
        return new_data,project_id,subject_id,category_id
    except Exception as e:
        log.error("异常信息：%s" %e)


def query_metadata_model_by_name(data):
    try:
        data = data.split("#")
        category = "select id,project_id from dw_category where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        category_info = ms.ExecuQuery(category.encode('utf-8'))
        subject = "select id from dw_subject where name like '%s%%%%' order by create_time desc limit 1" %data[1]
        subject_info = ms.ExecuQuery(subject.encode('utf-8'))
        project_id,category_id,subject_id = category_info[0]["project_id"],category_info[0]["id"],subject_info[0]["id"]
        new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"pageNum","comparatorOperator":"EQUAL","fieldValue":0},{"logicalOperator":"AND","fieldName":"pageSize","comparatorOperator":"EQUAL","fieldValue":10},{"logicalOperator":"AND","fieldName":"alias","comparatorOperator":"LIKE","fieldValue":"%a%"},{"logicalOperator":"AND","fieldName":"tableSpec","comparatorOperator":"EQUAL","fieldValue":data[2]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
        deal_random(new_data)
        return new_data,project_id,subject_id,category_id
    except Exception as e:
        log.error("异常信息：%s" %e)


def update_dimension(data):
    try:
        data = data.split("#")
        field_defined = "select id,alias,business_id,project_id,subject_id,field_spec,category_id,source_table_id,source_table_name,source_field_name,field_type from dw_field_defined where name like '%s%%%%' order by create_time desc limit 1" %data[0]
        field_defined_info = ms.ExecuQuery(field_defined.encode('utf-8'))
        field_defined_id = field_defined_info[0]["id"]
        new_data = {"id":field_defined_info[0]["id"],"tenantId":get_tenant(dw_host),"owner":get_owner(),"creator":"admin","createTime":datatime_now(),"lastModifier":"admin","lastModifiedTime":datatime_now(),"name":data[1],"alias":field_defined_info[0]["alias"],"businessId":field_defined_info[0]["business_id"],"subjectId":field_defined_info[0]["subject_id"],"projectId":field_defined_info[0]["project_id"],"sourceTableId":field_defined_info[0]["source_table_id"],"sourceTableName":field_defined_info[0]["source_table_name"],"sourceFieldName":field_defined_info[0]["source_field_name"],"fieldSpec":field_defined_info[0]["field_spec"],"fieldType":field_defined_info[0]["field_type"],"description":"","categoryId":field_defined_info[0]["category_id"],"categorySource":"standard","length":22}
        deal_random(new_data)
        return new_data,field_defined_id
    except Exception as e:
        log.error("异常信息：%s" %e)


def get_target_metadata(data):
    try:
        data = data.split("#")
        metadata = "select abbr,alias,id,source_table_ids,metadata_id,name,owner,tenant_id from dw_metadata_info where name like '%s%%%%' and table_spec='transaction' order by create_time desc limit 1" %data[0]
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        count_items_id = "select name,aggr_method,alias,source_field_name from dw_field_defined where name like '%s%%%%' and field_spec='metric' order by create_time desc limit 1" %data[1]
        count_items_info = ms.ExecuQuery(count_items_id.encode('utf-8'))
        count_orders_id = "select name,aggr_method,alias,source_field_name from dw_field_defined where name like '%s%%%%' and field_spec='metric' order by create_time desc limit 1" %data[2]
        count_orders_info = ms.ExecuQuery(count_orders_id.encode('utf-8'))
        max_items_num = "select name,aggr_method,alias,source_field_name from dw_field_defined where name like '%s%%%%' and field_spec='metric' order by create_time desc limit 1" %data[3]
        max_items_num_info = ms.ExecuQuery(max_items_num.encode('utf-8'))
        avg_items_num = "select name,aggr_method,alias,source_field_name from dw_field_defined where name like '%s%%%%' and field_spec='metric' order by create_time desc limit 1" %data[4]
        avg_items_num_info = ms.ExecuQuery(avg_items_num.encode('utf-8'))
        time_dim = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='timedim' order by create_time desc limit 1" %data[5]
        time_dim_info = ms.ExecuQuery(time_dim.encode('utf-8'))
        items_id = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='attribute' order by create_time desc limit 1" %data[6]
        items_id_info = ms.ExecuQuery(items_id.encode('utf-8'))
        primary = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='primary' order by create_time desc limit 1" %data[7]
        primary_info = ms.ExecuQuery(primary.encode('utf-8'))
        orders_id = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='attribute' order by create_time desc limit 1" %data[8]
        orders_id_info = ms.ExecuQuery(orders_id.encode('utf-8'))
        measure = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='measure' order by create_time desc limit 1" %data[9]
        measure_info = ms.ExecuQuery(measure.encode('utf-8'))
        orders_id_dimension = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='dimension' order by create_time desc limit 1" %data[10]
        orders_id_dimension_info = ms.ExecuQuery(orders_id_dimension.encode('utf-8'))
        items_id_dimension = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='dimension' order by create_time desc limit 1" %data[11]
        items_id_dimension_info = ms.ExecuQuery(items_id_dimension.encode('utf-8'))
        group_name =[]
        group_name.append(time_dim_info[0]["name"])
        new_data = {"aggrFields":[{"alias":count_items_info[0]["name"],"function":count_items_info[0]["aggr_method"],"name":count_items_info[0]["source_field_name"],"metric":count_items_info[0]["alias"]},{"alias":count_orders_info[0]["name"],"function":count_orders_info[0]["aggr_method"],"name":count_orders_info[0]["source_field_name"],"metric":count_orders_info[0]["alias"]},{"alias":max_items_num_info[0]["name"],"function":max_items_num_info[0]["aggr_method"],"name":max_items_num_info[0]["source_field_name"],"metric":max_items_num_info[0]["alias"]},{"alias":avg_items_num_info[0]["name"],"function":avg_items_num_info[0]["aggr_method"],"name":avg_items_num_info[0]["source_field_name"],"metric":avg_items_num_info[0]["alias"]}],"groupByFields":group_name,"sources":[{"data":{"fields":[{"name":time_dim_info[0]["name"],"alias":time_dim_info[0]["alias"],"abbr":"","fieldType":time_dim_info[0]["field_type"],"length":time_dim_info[0]["length"],"precision":"","unit":"","fieldSpec":time_dim_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":time_dim_info[0]["id"],"associatedFieldName":time_dim_info[0]["name"],"fieldSource":data[12],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":items_id_info[0]["name"],"alias":items_id_info[0]["alias"],"abbr":"","fieldType":items_id_info[0]["field_type"],"length":items_id_info[0]["length"],"precision":"","unit":"","fieldSpec":items_id_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":items_id_info[0]["id"],"associatedFieldName":items_id_info[0]["name"],"fieldSource":data[13],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":primary_info[0]["name"],"alias":primary_info[0]["alias"],"abbr":"","fieldType":primary_info[0]["field_type"],"length":primary_info[0]["length"],"precision":"","unit":"","fieldSpec":primary_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":primary_info[0]["id"],"associatedFieldName":primary_info[0]["name"],"fieldSource":data[14],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":orders_id_info[0]["name"],"alias":orders_id_info[0]["alias"],"abbr":"","fieldType":orders_id_info[0]["field_type"],"length":orders_id_info[0]["length"],"precision":"","unit":"","fieldSpec":orders_id_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":orders_id_info[0]["id"],"associatedFieldName":orders_id_info[0]["name"],"fieldSource":data[15],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":measure_info[0]["name"],"alias":measure_info[0]["alias"],"abbr":"","fieldType":measure_info[0]["field_type"],"length":measure_info[0]["length"],"precision":"","unit":"","fieldSpec":measure_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":measure_info[0]["id"],"associatedFieldName":measure_info[0]["name"],"fieldSource":data[16],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":orders_id_dimension_info[0]["name"],"alias":orders_id_dimension_info[0]["alias"],"abbr":"","fieldType":orders_id_dimension_info[0]["field_type"],"length":orders_id_dimension_info[0]["length"],"precision":"","unit":"","fieldSpec":orders_id_dimension_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":orders_id_dimension_info[0]["id"],"associatedFieldName":orders_id_dimension_info[0]["name"],"fieldSource":data[17],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":items_id_dimension_info[0]["name"],"alias":items_id_dimension_info[0]["alias"],"abbr":"","fieldType":items_id_dimension_info[0]["field_type"],"length":items_id_dimension_info[0]["length"],"precision":"","unit":"","fieldSpec":items_id_dimension_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":items_id_dimension_info[0]["id"],"associatedFieldName":items_id_dimension_info[0]["name"],"fieldSource":data[18],"tableSourceId":metadata_info[0]["source_table_ids"]}],"currentInfoId":metadata_info[0]["id"],"metadataId":metadata_info[0]["metadata_id"],"name":metadata_info[0]["name"],"id":metadata_info[0]["id"],"tenantId":metadata_info[0]["tenant_id"],"owner":metadata_info[0]["owner"],"creator":"admin","createTime":datatime_now(),"lastModifier":"admin","lastModifiedTime":datatime_now(),"modelId":"842418354869239808","nodeId":"metadata_3","alias":metadata_info[0]["alias"],"abbr":metadata_info[0]["abbr"],"tableSpec":"transaction"},"id":"metadata_1","type":"metadata","x":195,"y":50,"name":"metadata_1"}],"target":{"data":{"fields":[],"metadataId":"","name":"","type":"metadata"},"id":"metadata_2","name":"metadata_2","type":"metadata","x":1058,"y":129},"type":"aggregate"}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)


def new_data_model(data):
    try:
        data = data.split("#")
        metadata = "select abbr,alias,id,source_table_ids,metadata_id,name,owner,tenant_id from dw_metadata_info where name like '%s%%%%' and table_spec='transaction' order by create_time desc limit 1" %data[0]
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        metadata_model = "select subject_id,category_id,project_id from dw_metadata where name like '%s%%%%' and table_spec='transaction' order by create_time desc limit 1" %data[0]
        metadata_model_info = ms.ExecuQuery(metadata_model.encode('utf-8'))
        count_items_id = "select name,aggr_method,alias,source_field_name,field_spec,field_type,id,length from dw_field_defined where name like '%s%%%%' and field_spec='metric' order by create_time desc limit 1" %data[1]
        count_items_info = ms.ExecuQuery(count_items_id.encode('utf-8'))
        count_orders_id = "select name,aggr_method,alias,source_field_name,field_spec,field_type,id,length from dw_field_defined where name like '%s%%%%' and field_spec='metric' order by create_time desc limit 1" %data[2]
        count_orders_info = ms.ExecuQuery(count_orders_id.encode('utf-8'))
        max_items_num = "select name,aggr_method,alias,source_field_name,field_spec,field_type,id,length from dw_field_defined where name like '%s%%%%' and field_spec='metric' order by create_time desc limit 1" %data[3]
        max_items_num_info = ms.ExecuQuery(max_items_num.encode('utf-8'))
        avg_items_num = "select name,aggr_method,alias,source_field_name,field_spec,field_type,id,length from dw_field_defined where name like '%s%%%%' and field_spec='metric' order by create_time desc limit 1" %data[4]
        avg_items_num_info = ms.ExecuQuery(avg_items_num.encode('utf-8'))
        time_dim = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='timedim' order by create_time desc limit 1" %data[5]
        time_dim_info = ms.ExecuQuery(time_dim.encode('utf-8'))
        items_id = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='attribute' order by create_time desc limit 1" %data[6]
        items_id_info = ms.ExecuQuery(items_id.encode('utf-8'))
        primary = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='primary' order by create_time desc limit 1" %data[7]
        primary_info = ms.ExecuQuery(primary.encode('utf-8'))
        orders_id = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='attribute' order by create_time desc limit 1" %data[8]
        orders_id_info = ms.ExecuQuery(orders_id.encode('utf-8'))
        measure = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='measure' order by create_time desc limit 1" %data[9]
        measure_info = ms.ExecuQuery(measure.encode('utf-8'))
        orders_id_dimension = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='dimension' order by create_time desc limit 1" %data[10]
        orders_id_dimension_info = ms.ExecuQuery(orders_id_dimension.encode('utf-8'))
        items_id_dimension = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='dimension' order by create_time desc limit 1" %data[11]
        items_id_dimension_info = ms.ExecuQuery(items_id_dimension.encode('utf-8'))
        group_name =[]
        group_name.append(time_dim_info[0]["name"])
        new_data = {"name":"api_aggr随机数","description":"api_aggr","nodes":[{"data":{"fields":[{"name":time_dim_info[0]["name"],"alias":time_dim_info[0]["alias"],"abbr":"","fieldType":time_dim_info[0]["field_type"],"length":time_dim_info[0]["length"],"precision":"","unit":"","fieldSpec":time_dim_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":time_dim_info[0]["id"],"associatedFieldName":time_dim_info[0]["name"],"fieldSource":data[12],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":items_id_info[0]["name"],"alias":items_id_info[0]["alias"],"abbr":"","fieldType":items_id_info[0]["field_type"],"length":items_id_info[0]["length"],"precision":"","unit":"","fieldSpec":items_id_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":items_id_info[0]["id"],"associatedFieldName":items_id_info[0]["name"],"fieldSource":data[13],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":primary_info[0]["name"],"alias":primary_info[0]["alias"],"abbr":"","fieldType":primary_info[0]["field_type"],"length":primary_info[0]["length"],"precision":"","unit":"","fieldSpec":primary_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":primary_info[0]["id"],"associatedFieldName":primary_info[0]["name"],"fieldSource":data[14],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":orders_id_info[0]["name"],"alias":orders_id_info[0]["alias"],"abbr":"","fieldType":orders_id_info[0]["field_type"],"length":orders_id_info[0]["length"],"precision":"","unit":"","fieldSpec":orders_id_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":orders_id_info[0]["id"],"associatedFieldName":orders_id_info[0]["name"],"fieldSource":data[15],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":measure_info[0]["name"],"alias":measure_info[0]["alias"],"abbr":"","fieldType":measure_info[0]["field_type"],"length":measure_info[0]["length"],"precision":"","unit":"","fieldSpec":measure_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":measure_info[0]["id"],"associatedFieldName":measure_info[0]["name"],"fieldSource":data[16],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":orders_id_dimension_info[0]["name"],"alias":orders_id_dimension_info[0]["alias"],"abbr":"","fieldType":orders_id_dimension_info[0]["field_type"],"length":orders_id_dimension_info[0]["length"],"precision":"","unit":"","fieldSpec":orders_id_dimension_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":orders_id_dimension_info[0]["id"],"associatedFieldName":orders_id_dimension_info[0]["name"],"fieldSource":data[17],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":items_id_dimension_info[0]["name"],"alias":items_id_dimension_info[0]["alias"],"abbr":"","fieldType":items_id_dimension_info[0]["field_type"],"length":items_id_dimension_info[0]["length"],"precision":"","unit":"","fieldSpec":items_id_dimension_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":items_id_dimension_info[0]["id"],"associatedFieldName":items_id_dimension_info[0]["name"],"fieldSource":data[18],"tableSourceId":metadata_info[0]["source_table_ids"]}],"currentInfoId":metadata_info[0]["id"],"metadataId":metadata_info[0]["metadata_id"],"name":metadata_info[0]["name"],"id":metadata_info[0]["id"],"tenantId":metadata_info[0]["tenant_id"],"owner":metadata_info[0]["owner"],"creator":"admin","createTime":datatime_now(),"lastModifier":"admin","lastModifiedTime":datatime_now(),"modelId":"842418354869239808","nodeId":"metadata_3","alias":metadata_info[0]["alias"],"abbr":metadata_info[0]["abbr"],"tableSpec":"transaction"},"id":"metadata_1","type":"metadata","x":362,"y":62,"name":"metadata_1"},{"data":{"aggrFields":[{"alias":count_items_info[0]["name"],"function":count_items_info[0]["aggr_method"],"name":count_items_info[0]["source_field_name"],"metric":count_items_info[0]["alias"]},{"alias":count_orders_info[0]["name"],"function":count_orders_info[0]["aggr_method"],"name":count_orders_info[0]["source_field_name"],"metric":count_orders_info[0]["alias"]},{"alias":max_items_num_info[0]["name"],"function":max_items_num_info[0]["aggr_method"],"name":max_items_num_info[0]["source_field_name"],"metric":max_items_num_info[0]["alias"]},{"alias":avg_items_num_info[0]["name"],"function":avg_items_num_info[0]["aggr_method"],"name":avg_items_num_info[0]["source_field_name"],"metric":avg_items_num_info[0]["alias"]}],"groupByFields":group_name,"sources":[{"data":{"fields":[{"name":time_dim_info[0]["name"],"alias":time_dim_info[0]["alias"],"abbr":"","fieldType":time_dim_info[0]["field_type"],"length":time_dim_info[0]["length"],"precision":"","unit":"","fieldSpec":time_dim_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":time_dim_info[0]["id"],"associatedFieldName":time_dim_info[0]["name"],"fieldSource":data[12],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":items_id_info[0]["name"],"alias":items_id_info[0]["alias"],"abbr":"","fieldType":items_id_info[0]["field_type"],"length":items_id_info[0]["length"],"precision":"","unit":"","fieldSpec":items_id_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":items_id_info[0]["id"],"associatedFieldName":items_id_info[0]["name"],"fieldSource":data[13],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":primary_info[0]["name"],"alias":primary_info[0]["alias"],"abbr":"","fieldType":primary_info[0]["field_type"],"length":primary_info[0]["length"],"precision":"","unit":"","fieldSpec":primary_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":primary_info[0]["id"],"associatedFieldName":primary_info[0]["name"],"fieldSource":data[14],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":orders_id_info[0]["name"],"alias":orders_id_info[0]["alias"],"abbr":"","fieldType":orders_id_info[0]["field_type"],"length":orders_id_info[0]["length"],"precision":"","unit":"","fieldSpec":orders_id_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":orders_id_info[0]["id"],"associatedFieldName":orders_id_info[0]["name"],"fieldSource":data[15],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":measure_info[0]["name"],"alias":measure_info[0]["alias"],"abbr":"","fieldType":measure_info[0]["field_type"],"length":measure_info[0]["length"],"precision":"","unit":"","fieldSpec":measure_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":measure_info[0]["id"],"associatedFieldName":measure_info[0]["name"],"fieldSource":data[16],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":orders_id_dimension_info[0]["name"],"alias":orders_id_dimension_info[0]["alias"],"abbr":"","fieldType":orders_id_dimension_info[0]["field_type"],"length":orders_id_dimension_info[0]["length"],"precision":"","unit":"","fieldSpec":orders_id_dimension_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":orders_id_dimension_info[0]["id"],"associatedFieldName":orders_id_dimension_info[0]["name"],"fieldSource":data[17],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":items_id_dimension_info[0]["name"],"alias":items_id_dimension_info[0]["alias"],"abbr":"","fieldType":items_id_dimension_info[0]["field_type"],"length":items_id_dimension_info[0]["length"],"precision":"","unit":"","fieldSpec":items_id_dimension_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":items_id_dimension_info[0]["id"],"associatedFieldName":items_id_dimension_info[0]["name"],"fieldSource":data[18],"tableSourceId":metadata_info[0]["source_table_ids"]}],"currentInfoId":metadata_info[0]["id"],"metadataId":metadata_info[0]["metadata_id"],"name":metadata_info[0]["name"],"id":metadata_info[0]["id"],"tenantId":metadata_info[0]["tenant_id"],"owner":metadata_info[0]["owner"],"creator":"admin","createTime":datatime_now(),"lastModifier":"admin","lastModifiedTime":datatime_now(),"modelId":"842418354869239808","nodeId":"metadata_3","alias":metadata_info[0]["alias"],"abbr":metadata_info[0]["abbr"],"tableSpec":"transaction"},"id":"metadata_1","type":"metadata","x":362,"y":62,"name":"metadata_1"}],"target":{"data":{"tenantId":"","owner":"","creator":"","lastModifier":"","nodeId":"","name":"api_aggr"+ str(random.randint(0, 999)),"alias":"api_aggr"+ str(random.randint(0, 999)),"abbr":"","tableSpec":"aggregate","fields":[{"name":time_dim_info[0]["name"],"alias":time_dim_info[0]["alias"],"abbr":"","fieldType":time_dim_info[0]["field_type"],"length":time_dim_info[0]["length"],"precision":"","unit":"","fieldSpec":time_dim_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":time_dim_info[0]["id"],"associatedFieldName":time_dim_info[0]["name"],"fieldSource":data[12],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":count_items_info[0]["name"],"alias":count_items_info[0]["alias"],"abbr":"","fieldType":count_items_info[0]["field_type"],"length":count_items_info[0]["length"],"precision":"","unit":"","fieldSpec":count_items_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":count_items_info[0]["id"],"associatedFieldName":count_items_info[0]["name"],"fieldSource":data[13],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":count_orders_info[0]["name"],"alias":count_orders_info[0]["alias"],"abbr":"","fieldType":count_orders_info[0]["field_type"],"length":count_orders_info[0]["length"],"precision":"","unit":"","fieldSpec":count_orders_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":count_orders_info[0]["id"],"associatedFieldName":count_orders_info[0]["name"],"fieldSource":data[15],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":max_items_num_info[0]["name"],"alias":max_items_num_info[0]["alias"],"abbr":"","fieldType":max_items_num_info[0]["field_type"],"length":max_items_num_info[0]["length"],"precision":"","unit":"","fieldSpec":max_items_num_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":max_items_num_info[0]["id"],"associatedFieldName":max_items_num_info[0]["name"],"fieldSource":data[16],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":avg_items_num_info[0]["name"],"alias":avg_items_num_info[0]["alias"],"abbr":"","fieldType":avg_items_num_info[0]["field_type"],"length":avg_items_num_info[0]["length"],"precision":"","unit":"","fieldSpec":avg_items_num_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":avg_items_num_info[0]["id"],"associatedFieldName":avg_items_num_info[0]["name"],"fieldSource":data[16],"tableSourceId":metadata_info[0]["source_table_ids"]}]},"id":"metadata_2","name":"metadata_2","type":"metadata","x":1219,"y":131},"type":"aggregate"},"id":"relation_1","name":"relation_1","type":"relation","x":859,"y":145},{"data":{"tenantId":"","owner":"","creator":"","lastModifier":"","nodeId":"","name":"api_aggr"+str(random.randint(0, 999)),"alias":"api_aggr"+str(random.randint(0, 999)),"abbr":"","tableSpec":"aggregate","fields":[{"name":time_dim_info[0]["name"],"alias":time_dim_info[0]["alias"],"abbr":"","fieldType":time_dim_info[0]["field_type"],"length":time_dim_info[0]["length"],"precision":"","unit":"","fieldSpec":time_dim_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":time_dim_info[0]["id"],"associatedFieldName":time_dim_info[0]["name"],"fieldSource":data[12],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":count_items_info[0]["name"],"alias":count_items_info[0]["alias"],"abbr":"","fieldType":count_items_info[0]["field_type"],"length":count_items_info[0]["length"],"precision":"","unit":"","fieldSpec":count_items_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":count_items_info[0]["id"],"associatedFieldName":count_items_info[0]["name"],"fieldSource":data[13],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":count_orders_info[0]["name"],"alias":count_orders_info[0]["alias"],"abbr":"","fieldType":count_orders_info[0]["field_type"],"length":count_orders_info[0]["length"],"precision":"","unit":"","fieldSpec":count_orders_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":count_orders_info[0]["id"],"associatedFieldName":count_orders_info[0]["name"],"fieldSource":data[15],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":max_items_num_info[0]["name"],"alias":max_items_num_info[0]["alias"],"abbr":"","fieldType":max_items_num_info[0]["field_type"],"length":max_items_num_info[0]["length"],"precision":"","unit":"","fieldSpec":max_items_num_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":max_items_num_info[0]["id"],"associatedFieldName":max_items_num_info[0]["name"],"fieldSource":data[16],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":avg_items_num_info[0]["name"],"alias":avg_items_num_info[0]["alias"],"abbr":"","fieldType":avg_items_num_info[0]["field_type"],"length":avg_items_num_info[0]["length"],"precision":"","unit":"","fieldSpec":avg_items_num_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":avg_items_num_info[0]["id"],"associatedFieldName":avg_items_num_info[0]["name"],"fieldSource":data[16],"tableSourceId":metadata_info[0]["source_table_ids"]}]},"id":"metadata_2","name":"metadata_2","type":"metadata","x":1219,"y":131}],"links":[{"targetId":"relation_1","sourceId":"metadata_1"},{"targetId":"metadata_2","sourceId":"relation_1"}],"subjectId":metadata_model_info[0]["subject_id"]}
        deal_random(new_data)
        return new_data,metadata_model_info[0]["project_id"],metadata_model_info[0]["category_id"]
    except Exception as e:
        log.error("异常信息：%s" % e)


def publish_data_model(data):
    try:
        data = data.split("#")
        metadata = "select abbr,alias,id,source_table_ids,metadata_id,name,owner,tenant_id from dw_metadata_info where name like '%s%%%%' and table_spec='transaction' order by create_time desc limit 1" %data[0]
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        metadata_model = "select id,business_id,category_source,version,name,subject_id,category_id,project_id from dw_model where name like '%s%%%%' order by create_time desc limit 1" %data[19]
        metadata_model_info = ms.ExecuQuery(metadata_model.encode('utf-8'))
        count_items_id = "select name,aggr_method,alias,source_field_name,field_spec,field_type,id,length from dw_field_defined where name like '%s%%%%' and field_spec='metric' order by create_time desc limit 1" %data[1]
        count_items_info = ms.ExecuQuery(count_items_id.encode('utf-8'))
        count_orders_id = "select name,aggr_method,alias,source_field_name,field_spec,field_type,id,length from dw_field_defined where name like '%s%%%%' and field_spec='metric' order by create_time desc limit 1" %data[2]
        count_orders_info = ms.ExecuQuery(count_orders_id.encode('utf-8'))
        max_items_num = "select name,aggr_method,alias,source_field_name,field_spec,field_type,id,length from dw_field_defined where name like '%s%%%%' and field_spec='metric' order by create_time desc limit 1" %data[3]
        max_items_num_info = ms.ExecuQuery(max_items_num.encode('utf-8'))
        avg_items_num = "select name,aggr_method,alias,source_field_name,field_spec,field_type,id,length from dw_field_defined where name like '%s%%%%' and field_spec='metric' order by create_time desc limit 1" %data[4]
        avg_items_num_info = ms.ExecuQuery(avg_items_num.encode('utf-8'))
        time_dim = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='timedim' order by create_time desc limit 1" %data[5]
        time_dim_info = ms.ExecuQuery(time_dim.encode('utf-8'))
        items_id = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='attribute' order by create_time desc limit 1" %data[6]
        items_id_info = ms.ExecuQuery(items_id.encode('utf-8'))
        primary = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='primary' order by create_time desc limit 1" %data[7]
        primary_info = ms.ExecuQuery(primary.encode('utf-8'))
        orders_id = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='attribute' order by create_time desc limit 1" %data[8]
        orders_id_info = ms.ExecuQuery(orders_id.encode('utf-8'))
        measure = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='measure' order by create_time desc limit 1" %data[9]
        measure_info = ms.ExecuQuery(measure.encode('utf-8'))
        orders_id_dimension = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='dimension' order by create_time desc limit 1" %data[10]
        orders_id_dimension_info = ms.ExecuQuery(orders_id_dimension.encode('utf-8'))
        items_id_dimension = "select id,alias,field_spec,field_type,name,length from dw_field_defined where name like '%s%%%%' and field_spec='dimension' order by create_time desc limit 1" %data[11]
        items_id_dimension_info = ms.ExecuQuery(items_id_dimension.encode('utf-8'))
        group_name =[]
        group_name.append(time_dim_info[0]["name"])
        new_data = {"id":metadata_model_info[0]["id"],"tenantId":metadata_info[0]["tenant_id"],"owner":metadata_info[0]["owner"],"creator":"admin","createTime":datatime_now(),"lastModifier":"admin","lastModifiedTime":datatime_now(),"name":metadata_model_info[0]["name"],"businessId":metadata_model_info[0]["business_id"],"subjectId":metadata_model_info[0]["subject_id"],"projectId":metadata_model_info[0]["project_id"],"description":"api_aggr","version":metadata_model_info[0]["version"],"links":[{"sourceId":"metadata_1","targetId":"relation_1"},{"sourceId":"relation_1","targetId":"metadata_2"}],"nodes":[{"type":"metadata","id":"metadata_1","name":"metadata_1","x":362,"y":62,"data":{"id":metadata_info[0]["id"],"tenantId":metadata_info[0]["tenant_id"],"owner":metadata_info[0]["owner"],"creator":"admin","createTime":"2021-05-19T18:08:20.000+0000","lastModifier":"admin","lastModifiedTime":datatime_now(),"metadataId":metadata_info[0]["metadata_id"],"modelId":"842418354869239808","nodeId":"metadata_3","name":metadata_info[0]["name"],"alias":metadata_info[0]["alias"],"abbr":metadata_info[0]["abbr"],"tableSpec":"transaction","fields":[{"name":time_dim_info[0]["name"],"alias":time_dim_info[0]["alias"],"abbr":"","fieldType":time_dim_info[0]["field_type"],"length":time_dim_info[0]["length"],"precision":"","unit":"","fieldSpec":time_dim_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":time_dim_info[0]["id"],"associatedFieldName":time_dim_info[0]["name"],"fieldSource":data[12],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":items_id_info[0]["name"],"alias":items_id_info[0]["alias"],"abbr":"","fieldType":items_id_info[0]["field_type"],"length":items_id_info[0]["length"],"precision":"","unit":"","fieldSpec":items_id_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":items_id_info[0]["id"],"associatedFieldName":items_id_info[0]["name"],"fieldSource":data[13],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":primary_info[0]["name"],"alias":primary_info[0]["alias"],"abbr":"","fieldType":primary_info[0]["field_type"],"length":primary_info[0]["length"],"precision":"","unit":"","fieldSpec":primary_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":primary_info[0]["id"],"associatedFieldName":primary_info[0]["name"],"fieldSource":data[14],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":orders_id_info[0]["name"],"alias":orders_id_info[0]["alias"],"abbr":"","fieldType":orders_id_info[0]["field_type"],"length":orders_id_info[0]["length"],"precision":"","unit":"","fieldSpec":orders_id_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":orders_id_info[0]["id"],"associatedFieldName":orders_id_info[0]["name"],"fieldSource":data[15],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":measure_info[0]["name"],"alias":measure_info[0]["alias"],"abbr":"","fieldType":measure_info[0]["field_type"],"length":measure_info[0]["length"],"precision":"","unit":"","fieldSpec":measure_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":measure_info[0]["id"],"associatedFieldName":measure_info[0]["name"],"fieldSource":data[16],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":orders_id_dimension_info[0]["name"],"alias":orders_id_dimension_info[0]["alias"],"abbr":"","fieldType":orders_id_dimension_info[0]["field_type"],"length":orders_id_dimension_info[0]["length"],"precision":"","unit":"","fieldSpec":orders_id_dimension_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":orders_id_dimension_info[0]["id"],"associatedFieldName":orders_id_dimension_info[0]["name"],"fieldSource":data[17],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":items_id_dimension_info[0]["name"],"alias":items_id_dimension_info[0]["alias"],"abbr":"","fieldType":items_id_dimension_info[0]["field_type"],"length":items_id_dimension_info[0]["length"],"precision":"","unit":"","fieldSpec":items_id_dimension_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":items_id_dimension_info[0]["id"],"associatedFieldName":items_id_dimension_info[0]["name"],"fieldSource":data[18],"tableSourceId":metadata_info[0]["source_table_ids"]}]}},{"type":"relation","id":"relation_1","name":"relation_1","x":859,"y":145,"data":{"type":"aggregate","sources":[],"aggrFields":[{"alias":count_items_info[0]["name"],"function":count_items_info[0]["aggr_method"],"name":count_items_info[0]["source_field_name"],"metric":count_items_info[0]["alias"]},{"alias":count_orders_info[0]["name"],"function":count_orders_info[0]["aggr_method"],"name":count_orders_info[0]["source_field_name"],"metric":count_orders_info[0]["alias"]},{"alias":max_items_num_info[0]["name"],"function":max_items_num_info[0]["aggr_method"],"name":max_items_num_info[0]["source_field_name"],"metric":max_items_num_info[0]["alias"]},{"alias":avg_items_num_info[0]["name"],"function":avg_items_num_info[0]["aggr_method"],"name":avg_items_num_info[0]["source_field_name"],"metric":avg_items_num_info[0]["alias"]}],"groupByFields":group_name}},{"type":"metadata","id":"metadata_2","name":"metadata_2","x":1219,"y":131,"data":{"tenantId":"","owner":"","creator":"","lastModifier":"","nodeId":"","name":"api_aggr"+str(random.randint(0, 999)),"alias":"api_aggr"+str(random.randint(0, 999)),"abbr":"","tableSpec":"aggregate","fields":[{"name":time_dim_info[0]["name"],"alias":time_dim_info[0]["alias"],"abbr":"","fieldType":time_dim_info[0]["field_type"],"length":time_dim_info[0]["length"],"precision":"","unit":"","fieldSpec":time_dim_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":time_dim_info[0]["id"],"associatedFieldName":time_dim_info[0]["name"],"fieldSource":data[12],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":count_items_info[0]["name"],"alias":count_items_info[0]["alias"],"abbr":"","fieldType":count_items_info[0]["field_type"],"length":count_items_info[0]["length"],"precision":"","unit":"","fieldSpec":count_items_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":count_items_info[0]["id"],"associatedFieldName":count_items_info[0]["name"],"fieldSource":data[13],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":count_orders_info[0]["name"],"alias":count_orders_info[0]["alias"],"abbr":"","fieldType":count_orders_info[0]["field_type"],"length":count_orders_info[0]["length"],"precision":"","unit":"","fieldSpec":count_orders_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":count_orders_info[0]["id"],"associatedFieldName":count_orders_info[0]["name"],"fieldSource":data[15],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":max_items_num_info[0]["name"],"alias":max_items_num_info[0]["alias"],"abbr":"","fieldType":max_items_num_info[0]["field_type"],"length":max_items_num_info[0]["length"],"precision":"","unit":"","fieldSpec":max_items_num_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":max_items_num_info[0]["id"],"associatedFieldName":max_items_num_info[0]["name"],"fieldSource":data[16],"tableSourceId":metadata_info[0]["source_table_ids"]},{"name":avg_items_num_info[0]["name"],"alias":avg_items_num_info[0]["alias"],"abbr":"","fieldType":avg_items_num_info[0]["field_type"],"length":avg_items_num_info[0]["length"],"precision":"","unit":"","fieldSpec":avg_items_num_info[0]["field_spec"],"comment":"","createTime":datatime_now(),"objectId":avg_items_num_info[0]["id"],"associatedFieldName":avg_items_num_info[0]["name"],"fieldSource":data[16],"tableSourceId":metadata_info[0]["source_table_ids"]}]}}],"categoryId":metadata_model_info[0]["category_id"],"categorySource":metadata_model_info[0]["category_source"]}
        deal_random(new_data)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

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
        new_data=[]
        new_data.append(asset_directory_info[0]["id"])
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
        asset_directory = "select id from merce_resource_dir mrd where res_type ='%s' and creator='admin' and name like'%s%%%%' order by create_time desc limit 1" % (data[1], data[0])
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
            data = []
            data.append("123456")
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"dataTierIds","oper":"EQUAL","value":data}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        else:
            data = []
            data.append(str(dw_data_tier[0]["id"]))
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"dataTierIds","oper":"EQUAL","value":data}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def query_subject_domain():
    try:
        dw_subject_domain = "SELECT id FROM dw_subject_domain where status ='ONLINE' order by create_time desc limit 1"
        dw_subject_domain = ms.ExecuQuery(dw_subject_domain.encode('utf-8'))
        if len(dw_subject_domain) == 0:
            data = []
            data.append("123456")
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"domainIds","oper":"EQUAL","value":data}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
        else:
            data = []
            data.append(str(dw_subject_domain[0]["id"]))
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"domainIds","oper":"EQUAL","value":data}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
            return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def query_tags():
    try:
        merce_tag = "SELECT id FROM merce_tag order by create_time desc limit 1"
        merce_tag = ms.ExecuQuery(merce_tag.encode('utf-8'))
        if len(merce_tag) == 0:
            data = []
            data.append("123456")
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"tagIds","oper":"EQUAL","value":data}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":2,"pageSize":8,"pageable":True}}
            return new_data
        else:
            data = []
            data.append(str(merce_tag[0]["id"]))
            new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"tagIds","oper":"EQUAL","value":data}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":2,"pageSize":8,"pageable":True}}
            return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def create_asset(data):
    try:
        data = data.split("&")
        asset_directory = "select id,name,owner,creator from merce_resource_dir where res_type ='assets_dir' and name like'%s%%%%' order by create_time desc limit 1" %data[1]
        asset_directory_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        dataset = "select id,name from merce_dataset where name like'%s%%%%' order by create_time desc limit 1" %data[0]
        dataset = ms.ExecuQuery(dataset.encode('utf-8'))
        new_data = {"assetModel":0,"storage":"HDFS","resourceId":asset_directory_info[0]["id"],"resourceName":asset_directory_info[0]["name"],"name":"test_asset_gjb随机数","isShare":0,"updateFrequency":"","datasetName":dataset[0]["name"],"datasetId":dataset[0]["id"],"description":"创建数据集资产","tags":[],"tagIds":[],"tagsList":[],"providerDepartment":"","businessDirector":"","businessTelephone":"","technicalDepartment":"","technicalDirector":"","technicalTelephone":"","resourceFormat":"FILE","approverName":asset_directory_info[0]["creator"],"approverId":asset_directory_info[0]["owner"]}
        deal_random(new_data)
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
        new_data = {"assetModel":1,"resourceId":asset_directory_info[0]["id"],"updateFrequency":"","resourceName":asset_directory_info[0]["name"],"name":"test_sql_asset_gjb随机数","sqlStr":"select * from `gjb_ttest_hdfs042219`","isShare":0,"datasetName":dataset[0]["name"],"datasetId":dataset[0]["id"],"description":"","tags":[],"tagIds":[],"tagsList":[],"providerDepartment":"","businessDirector":"","businessTelephone":"","technicalDepartment":"","technicalDirector":"","technicalTelephone":"","resourceFormat":"","approverName":asset_directory_info[0]["creator"],"approverId":asset_directory_info[0]["owner"],"isSave":True}
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
        new_data = [{"assetModel":0,"resourceId":asset_directory_info[0]["id"],"resourceName":asset_directory_info[0]["name"],"updateFrequency":"","name":"training","isShare":0,"datasetName":dataset2[0]["name"],"datasetId":dataset2[0]["id"],"description":None,"tags":[],"tagIds":[],"tagsList":[],"providerDepartment":"","businessDirector":"","businessTelephone":"","technicalDepartment":"","technicalDirector":"","technicalTelephone":"","resourceFormat":"JDBC","approverName":asset_directory_info[0]["creator"],"approverId":asset_directory_info[0]["owner"]},{"assetModel":0,"resourceId":asset_directory_info[0]["id"],"resourceName":asset_directory_info[0]["name"],"updateFrequency":"","name":"gjb_ttest_hdfs042219","isShare":0,"datasetName":dataset1[0]["name"],"datasetId":dataset1[0]["id"],"description":None,"tags":[],"tagIds":[],"tagsList":[],"providerDepartment":"","businessDirector":"","businessTelephone":"","technicalDepartment":"","technicalDirector":"","technicalTelephone":"","resourceFormat":"FILE","approverName":asset_directory_info[0]["creator"],"approverId":asset_directory_info[0]["owner"]}]
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def approval_asset(data):
    try:
        data = data.split("&")
        assets_approval_record = "select id from assets_approval_record where approval_type='%s' and approval_status ='PENDING' and target_name like '%s%%%%' order by create_time desc limit 1" % (data[2], data[0])
        assets_approval_record = ms.ExecuQuery(assets_approval_record.encode('utf-8'))
        ids = []
        ids.append(str(assets_approval_record[0]["id"]))
        if "PASS" in data:
            new_data = {"approvalComments":"通过","ids": ids,"approvalType":data[2],"approvalStatus":data[1],"targetType":"ASSETS_DIRECTORY"}
            return new_data
        else:
            new_data = {"approvalComments": "不通过", "ids": ids, "approvalType": data[2], "approvalStatus": data[1],
                        "targetType": "ASSETS_DIRECTORY"}
            return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def update_asset(data):
    try:
        assets_info = "select asset_model,id,resource_id,resource_name,dataset_id,dataset_name,tenant_id,owner,status,approver_name,approver_id,creator,last_modifier from assets_info where status in('OFFLINE','SAVED') and name like '%s%%%%' order by create_time desc limit 1" %data
        assets_info = ms.ExecuQuery(assets_info.encode('utf-8'))
        if "test_asset_gjb" in data:
            new_data = {"assetModel":assets_info[0]["asset_model"],"storage":"HDFS","resourceId":assets_info[0]["resource_id"],"resourceName":assets_info[0]["resource_name"],"name":"test_asset_gjb随机数","isShare":0,"updateFrequency":None,"datasetName":assets_info[0]["dataset_name"],"datasetId":assets_info[0]["dataset_id"],"description":"创建数据集资产","tags":[],"tagIds":[],"tagsList":[],"providerDepartment":"","businessDirector":"","businessTelephone":"","technicalDepartment":"","technicalDirector":"","technicalTelephone":"","resourceFormat":"FILE","approverName":assets_info[0]["approver_name"],"approverId":assets_info[0]["approver_id"],"id":assets_info[0]["id"],"tenantId":assets_info[0]["tenant_id"],"owner":assets_info[0]["owner"],"enabled":None,"creator":assets_info[0]["creator"],"createTime":data_now(),"lastModifier":assets_info[0]["last_modifier"],"lastModifiedTime":data_now(),"visibleToAll":False,"approverRemarks":"","approvalRecordId":None,"approvalComments":"","status":assets_info[0]["status"],"publishTime":None,"sqlStr":"","order":None,"expiredPeriod":0,"recordNumber":0,"byteSize":0,"type":"FLOW","resource":None,"isHide":0,"scheduleType":"","serviceMode":None,"dataTierIds":[],"dataTierName":"","domainIds":[],"domainNames":[],"lifeCycleType":"","shareStatus":None,"assetsShareId":None,"assetsShare":None,"shareMode":"","collectionTimes":0,"shareTimes":0,"departmentTimes":0,"organizationName":"","keywords":"","isCollection":0,"viewNum":1,"readNum":0,"dataPullNum":0,"dataPushNum":0,"dataShareNum":None,"recentViewNum":0}
            deal_random(new_data)
            return assets_info[0]["id"], new_data
        elif "test_sql_asset_gjb" in data:
            new_data = {"assetModel":assets_info[0]["asset_model"],"resourceId":assets_info[0]["resource_id"],"updateFrequency":None,"resourceName":assets_info[0]["resource_name"],"name":"test_sql_asset_gjb随机数","sqlStr":"select * from `gjb_ttest_hdfs042219`","isShare":0,"datasetName":assets_info[0]["dataset_name"],"datasetId":assets_info[0]["dataset_id"],"description":"","tags":[],"tagIds":[],"tagsList":[],"providerDepartment":"","businessDirector":"","businessTelephone":"","technicalDepartment":"","technicalDirector":"","technicalTelephone":"","resourceFormat":None,"approverName":assets_info[0]["approver_name"],"approverId":assets_info[0]["approver_id"],"isSave":True,"id":assets_info[0]["id"],"tenantId":assets_info[0]["tenant_id"],"owner":assets_info[0]["owner"],"enabled":None,"creator":assets_info[0]["creator"],"createTime":data_now(),"lastModifier":assets_info[0]["last_modifier"],"lastModifiedTime":data_now(),"visibleToAll":False,"approverRemarks":"","approvalRecordId":None,"approvalComments":"","status":assets_info[0]["status"],"publishTime":None,"order":None,"expiredPeriod":0,"recordNumber":0,"byteSize":0,"storage":"HDFS","type":"FLOW","resource":None,"isHide":0,"scheduleType":"","serviceMode":None,"dataTierIds":[],"dataTierName":"","domainIds":[],"domainNames":[],"lifeCycleType":"","shareStatus":None,"assetsShareId":None,"assetsShare":None,"shareMode":"","collectionTimes":0,"shareTimes":0,"departmentTimes":0,"organizationName":"","keywords":"","isCollection":0,"viewNum":1,"readNum":0,"dataPullNum":0,"dataPushNum":0,"dataShareNum":None,"recentViewNum":0}
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

# 初始化Sql Analyze(解析数据集输出字段)，返回statement id，获取数据集字段给分析任务使用
def get_sql_analyse_statement_id(host, param):
    url = ' %s/api/assets/sqlAssets/sql/analyzeinit' % host
    param = sql_analyse_data(param)
    new_data = json.dumps(param, separators=(',', ':'))
    res = requests.post(url=url, headers=get_headers_dw(host), data=new_data)
    try:
        res_statementId = json.loads(res.text)
        sql_analyse_statement_id = res_statementId['statementId']
        log.info("statmentid：%s" % sql_analyse_statement_id)
        return sql_analyse_statement_id
    except KeyError:
        return

# 根据初始化SQL Analyze返回的statement id,获取数据集字段(获取输出字段)
def get_sql_analyse_dataset_info(host, params):
    sql_analyse_statement_id = get_sql_analyse_statement_id(host, params)
    url = ' %s/api/assets/sqlAssets/sql/analyzeresult?statementId=%s&clusterId=cluster1' % (host, sql_analyse_statement_id)
    res = requests.get(url=url, headers=get_headers_dw(host))
    count_num = 0
    while ("waiting") in res.text or ("running") in res.text:
        log.info("再次查询前：%s %s" % (res.status_code, res.text))
        res = requests.get(url=url, headers=get_headers_dw(host))
        count_num += 1
        if count_num == 100:
            return
    # 返回的是str类型
    log.info("查询：%s %s" % (res.status_code, res.text))
    if '"statement":"available"' in res.text:
        text_dict = json.loads(res.text)
        text_dict_content = text_dict["content"]
        return text_dict_content
    else:
        print('获取数据集输出字段失败')
        return

# 解析SQL字段后，初始化Sql任务，返回statement id，执行SQL语句使用
def get_sql_execte_statement_id(HOST,param):
    url = '%s/api/assets/sqlAssets/sql/executeinit' % HOST
    param = sql_analyse_data(param)
    new_data = json.dumps(param, separators=(',', ':'))
    res = requests.post(url=url, headers=get_headers_dw(HOST), data=new_data)
    try:
        res_statementId = json.loads(res.text)
        sql_analyse_statement_id = res_statementId['statementId']
        return sql_analyse_statement_id
    except KeyError:
        return

def query_asset(data):
    try:
        data = data.split("&")
        assets_info = "select dataset_id from assets_info where status in('OFFLINE','SAVED') and name like '%s%%%%' order by create_time desc limit 1" % data[1]
        assets_info = ms.ExecuQuery(assets_info.encode('utf-8'))
        asset_directory = "select parent_id from merce_resource_dir mrd where res_type ='assets_dir' and creator='admin' and name like'%s%%%%' order by create_time desc limit 1" % data[0]
        assets_info = ms.ExecuQuery(asset_directory.encode('utf-8'))
        resource_id = []
        dataset_id = []
        resource_id.append(str(asset_directory[0]["parent_id"]))
        dataset_id.append(str(assets_info[0]["dataset_id"]))
        new_data = {"fieldGroup":{"fields":[{"andOr":"AND","name":"resourceId","oper":"EQUAL","value":resource_id},{"andOr":"AND","name":"datasetId","oper":"EQUAL","value":dataset_id},{"andOr":"AND","name":"assetModel","oper":"EQUAL","value":[0]}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":True}}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

# 解析SQL字段后，初始化Sql任务，返回statement id，执行SQL语句使用
def get_improt_data(headers, HOST):
    dataset_sql = "delete from merce_dataset where name like 'gjb_ttest_hdfs042219%' or  name like 'training%'"
    ms.ExecuNoQuery(dataset_sql)
    schema_sql = "delete from merce_schema where name like 'gjb_ttest_mysql0420_training%' or  name like 'training%'"
    ms.ExecuNoQuery(schema_sql)
    url = '%s/api/mis/upload' % HOST
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