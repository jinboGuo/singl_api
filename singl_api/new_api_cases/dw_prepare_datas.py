# coding:utf-8
import os
import random

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
        new_data = [{"alias":field_defined_info[0]["alias"],"fieldSpec":data[3],"fieldType":field_defined_info[0]["field_type"],"length":22,"name":field_defined_info[0]["name"],"fieldSource":data[2],"associatedFieldName":field_defined_info[0]["alias"],"precision":"null","unit":"null","objectId":field_defined_info[0]["id"]}]
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
        new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
        deal_random(new_data)
        return new_data, ref_project_id ,ref_category_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_physical_dataset_by_name(data):
    try:
        ref_dataset = "select project_id,category_id from dw_ref_dataset where name like '%s%%%%' order by create_time desc limit 1" %data
        ref_dataset_info = ms.ExecuQuery(ref_dataset.encode('utf-8'))
        ref_project_id ,ref_category_id= ref_dataset_info[0]["project_id"],ref_dataset_info[0]["category_id"]
        new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%订单%"]}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
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
        new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"AND","name":"subjectId","oper":"EQUAL","value":subject}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
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
        new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
        deal_random(new_data)
        return new_data, project_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_model_metadata_by_name(data):
    try:
        metadata = "select project_id from dw_metadata where name like '%s%%%%' order by create_time desc limit 1" %data
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        project_id = metadata_info[0]["project_id"]
        new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%a%"]}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
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
        new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%a%"]},{"andOr":"AND","name":"subjectId","oper":"EQUAL","value":subject}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
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
        new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
        deal_random(new_data)
        return new_data,project_id,category_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_timedim_by_name(data):
    try:
        field_defined = "select project_id,category_id from dw_field_defined where name like '%s%%%%' order by create_time desc limit 1" %data
        field_defined_info = ms.ExecuQuery(field_defined.encode('utf-8'))
        project_id,category_id = field_defined_info[0]["project_id"],field_defined_info[0]["category_id"]
        new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%时间%"]}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
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
        new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"AND","name":"subjectId","oper":"EQUAL","value":subject_id}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
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
        new_data = {"name":data[2],"alias":data[3],"subjectId":subject_info[0]["id"],"sourceTableId":"null","fieldType":data[4],"sourceFieldName":"","definition":"","description":"","fieldSource":data[6],"categoryId":category_info[0]["id"],"length":"22","projectId":category_info[0]["project_id"],"fieldSpec":data[5]}
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
        new_data = {"physicalCycle":"M_5","partitionField":"","partitionGrain":"300","storageEngine":"hdfs","datasetName":"","datasetId":"null","metadataId":"","description":""}
        deal_random(new_data)
        return new_data,metadata_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_physical(data):
    try:
        metadata = "select id from dw_metadata where name like '%s%%%%' order by create_time desc limit 1" %data
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        metadata_id = metadata_info[0]["id"]
        new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
        deal_random(new_data)
        return new_data,metadata_id
    except Exception as e:
        log.error("异常信息：%s" %e)

def query_physical_by_name(data):
    try:
        metadata = "select id from dw_metadata where name like '%s%%%%' order by create_time desc limit 1" %data
        metadata_info = ms.ExecuQuery(metadata.encode('utf-8'))
        metadata_id = metadata_info[0]["id"]
        new_data = {"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%a%"]}]},"ordSort":[{"name":"createTime","order":"DESC"}]}
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
        new_data = {"name":data[1],"alias":data[2],"subjectId":metadata_info[0]["subject_id"],"sourceTableId":metadata_info[0]["id"],"sourceTableName":metadata_info[0]["alias"],"sourceFieldId":"null","length":"30","categoryId":category_info[0]["id"],"sourceFieldName":field_defined_info[0]["name"],"fieldSpec":"dimension","description":"","fieldType":field_defined_info[0]["field_type"],"projectId":metadata_info[0]["project_id"]}
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
        new_data = [{"alias":field_defined_info[0]["alias"],"fieldSpec":data[3],"fieldType":field_defined_info[0]["field_type"],"length":22,"name":field_defined_info[0]["name"],"fieldSource":data[2],"associatedFieldName":field_defined_info[0]["alias"],"precision":"null","unit":"null","tableSourceId":metadata_info[0]["metadata_id"],"objectId":field_defined_info[0]["id"]}]
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
        log.error("异常信息：%s" %e)


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
        log.error("异常信息：%s" %e)