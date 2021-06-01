# coding:utf-8
import json
import os
import re
from openpyxl import load_workbook
import requests

from basic_info.ready_dataflow_data import delete_autotest_dw
from util.format_res import dict_res
from basic_info.setting import Dw_MySQL_CONFIG, dw_host
from util.Open_DB import MYSQL
from basic_info.get_auth_token import get_headers, get_headers_root
from new_api_cases.dw_deal_parameters import deal_parameters
import unittest
from util.logs import Logger
from new_api_cases.dw_prepare_datas import update_business_data, \
    add_subject_data, update_subject_data, query_subject_data, add_projects_data, update_projects_data, update_tag_data, \
    add_taggroup_data, update_taggroup_data, update_namerule_data, rel_product_taggroup, update_model_category, add_child_model_category, add_model_category, add_standard_category, \
    add_child_standard_category, update_standard_category, rel_physical_dataset, update_physical_dataset, query_physical_dataset, query_physical_dataset_by_name, \
    query_physical_dataset_by_subject, add_model_metadata, update_model_metadata, query_model_metadata, query_model_metadata_by_name, query_model_metadata_by_subject, \
    save_model_metadata_info, query_timedim, query_timedim_by_name, query_timedim_by_subject, add_primary, update_primary, add_physical, query_physical, query_physical_by_name, \
    update_physical, add_indicator, add_dimension, add_metadata_field, metadata_field, query_metadata_model_by_name, query_metadata_model, update_dimension, add_physical_field, \
    del_physical_field, get_target_metadata, new_data_model, publish_data_model

ms = MYSQL(Dw_MySQL_CONFIG["HOST"], Dw_MySQL_CONFIG["USER"], Dw_MySQL_CONFIG["PASSWORD"], Dw_MySQL_CONFIG["DB"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))
case_table = load_workbook(ab_dir("api_cases.xlsx"))
case_table_sheet = case_table.get_sheet_by_name('dw')
all_rows = case_table_sheet.max_row
jar_dir = ab_dir('woven-common-3.0.jar')
log = Logger().get_log()
host = dw_host

# 判断请求方法，并根据不同的请求方法调用不同的处理方式
def deal_request_method():
    for i in range(2, all_rows+1):
        request_method = case_table_sheet.cell(row=i, column=4).value
        old_request_url = case_table_sheet.cell(row=i, column=5).value
        old_request_url = host + old_request_url
        request_url = deal_parameters(old_request_url)
        old_data = case_table_sheet.cell(row=i, column=6).value
        request_data = deal_parameters(old_data)
        log.info("request  data：%s" %request_data)
        api_name = case_table_sheet.cell(row=i, column=1).value
        # 请求方法转大写
        if request_method:
            request_method_upper = request_method.upper()
            if api_name == 'tenants':  # 租户的用例需要使用root用户登录后操作
                # 根据不同的请求方法，进行分发
                if request_method_upper == 'POST':
                    # 调用post方法发送请求
                    post_request_result_check(row=i, column=8, url=request_url, headers=get_headers_root(host), data=request_data, table_sheet_name=case_table_sheet)

                elif request_method_upper == 'GET':
                    # 调用GET请求
                    get_request_result_check(url=request_url, headers=get_headers_root(host), data=request_data, table_sheet_name=case_table_sheet, row=i, column=8)

                elif request_method_upper == 'PUT':
                    put_request_result_check(url=request_url, row=i, data=request_data, table_sheet_name=case_table_sheet, column=8, headers=get_headers_root(host))

                elif request_method_upper == 'DELETE':
                    delete_request_result_check(request_url, data=request_data, table_sheet_name=case_table_sheet, row=i, column=8, headers=get_headers_root(host))
                else:
                    log.info("请求方法%s不在处理范围内" % request_method)
            else:
                # 根据不同的请求方法，进行分发
                if request_method_upper == 'POST':
                    # 调用post方法发送请求
                    post_request_result_check(row=i,  column=8, url=request_url, headers=get_headers(host), data=request_data, table_sheet_name=case_table_sheet)

                elif request_method_upper == 'GET':
                    # 调用GET请求
                    get_request_result_check(url=request_url, headers=get_headers(host), data=request_data, table_sheet_name=case_table_sheet, row=i, column=8)

                elif request_method_upper == 'PUT':
                    put_request_result_check(url=request_url, row=i, data=request_data, table_sheet_name=case_table_sheet, column=8, headers=get_headers(host))

                elif request_method_upper == 'DELETE':
                    delete_request_result_check(url=request_url, data=request_data, table_sheet_name=case_table_sheet, row=i, column=8, headers=get_headers(host))

                else:
                    log.info("请求方法%s不在处理范围内" % request_method)
        else:
            log.info("第 %d 行请求方法为空" % i)
    #  执行结束后保存表格
    case_table.save(ab_dir("api_cases.xlsx"))


# POST请求
def post_request_result_check(row, column, url, headers, data, table_sheet_name):
    try:
    # if isinstance(data, str):
        case_detail = case_table_sheet.cell(row=row, column=2).value
        log.info("开始执行：%s " %case_detail)
        if case_detail == '根据id查询主题域':
            new_data, business_id = query_subject_data(data)
            new_url = url.format(business_id)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改业务模块':
            new_data,business = update_business_data(data)
            new_url = url.format(business)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改主题域':
            new_data, subject_id = update_subject_data(data)
            new_url = url.format(subject_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改项目':
            new_data, protect_id = update_projects_data(data)
            new_url = url.format(protect_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改标签':
            new_data, tag_id = update_tag_data(data)
            new_url = url.format(tag_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改标签组':
            new_data, taggroup_id = update_taggroup_data(data)
            new_url = url.format(taggroup_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改已绑定标签组的标签':
            new_data, tag_id = update_tag_data(data)
            new_url = url.format(tag_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改命名规则':
            new_data, namerule_id = update_namerule_data(data)
            new_url = url.format(namerule_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据业务模块查询数据地图':
            new_data = {"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询数据地图':
            new_data = {"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
            new_url = url.format(data[0],data[1])
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询模型字段详情':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据字段名称查询字段详情':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%id%"]}]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改模型目录':
            new_data, model_category_id = update_model_category(data)
            new_url = url.format(model_category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改子模型目录':
            new_data, model_category_id = update_model_category(data)
            new_url = url.format(model_category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改标准目录':
            new_data, standard_category_id = update_standard_category(data)
            new_url = url.format(standard_category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改子标准目录':
            new_data, standard_category_id = update_standard_category(data)
            new_url = url.format(standard_category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改资源配置物理表':
            new_data, ref_dataset_id = update_physical_dataset(data)
            new_url = url.format(ref_dataset_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '添加物理表字段' in case_detail:
            new_data,metadata_id = add_physical_field(data)
            new_url = url.format(metadata_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '删除物理表字段' in case_detail:
            new_data,metadata_id = del_physical_field(data)
            new_url = url.format(metadata_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询资源配置物理表':
            new_data, ref_project_id ,ref_category_id = query_physical_dataset(data)
            new_url = url.format(ref_project_id ,ref_category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '按照名称查询资源配置物理表':
            new_data, ref_project_id ,ref_category_id = query_physical_dataset_by_name(data)
            new_url = url.format(ref_project_id ,ref_category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '按照主题域查询资源配置物理表':
            new_data, ref_project_id ,ref_category_id = query_physical_dataset_by_subject(data)
            new_url = url.format(ref_project_id ,ref_category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改逻辑模型transaction':
            new_data,metadata_id = update_model_metadata(data)
            new_url = url.format(metadata_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询逻辑模型transaction':
            new_data, project_id = query_model_metadata(data)
            new_url = url.format(project_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '按照名称查询逻辑模型transaction':
            new_data, project_id = query_model_metadata_by_name(data)
            new_url = url.format(project_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '按照主题域查询逻辑模型transaction':
            new_data, project_id = query_model_metadata_by_subject(data)
            new_url = url.format(project_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '保存逻辑模型-' in case_detail:
            new_data,metadata_id,subject_id = save_model_metadata_info(data)
            new_url = url.format(metadata_id,subject_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改时间维度':
            new_data,field_defined_id = update_primary(data)
            new_url = url.format(field_defined_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询时间维度':
            new_data,project_id,category_id = query_timedim(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询时间维度':
            new_data,project_id,category_id = query_timedim_by_name(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询时间维度':
            new_data,project_id,category_id = query_timedim_by_subject(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改主键':
            new_data,field_defined_id = update_primary(data)
            new_url = url.format(field_defined_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询主键':
            new_data,project_id,category_id = query_timedim(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询主键':
            new_data,project_id,category_id = query_timedim_by_name(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询主键':
            new_data,project_id,category_id = query_timedim_by_subject(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改属性':
            new_data,field_defined_id = update_primary(data)
            new_url = url.format(field_defined_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询属性':
            new_data,project_id,category_id = query_timedim(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询属性':
            new_data,project_id,category_id = query_timedim_by_name(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询属性':
            new_data,project_id,category_id = query_timedim_by_subject(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改度量':
            new_data,field_defined_id = update_primary(data)
            new_url = url.format(field_defined_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询度量':
            new_data,project_id,category_id = query_timedim(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询度量':
            new_data,project_id,category_id = query_timedim_by_name(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询度量':
            new_data,project_id,category_id = query_timedim_by_subject(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询指标':
            new_data,project_id,category_id = query_timedim(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询指标':
            new_data,project_id,category_id = query_timedim_by_name(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询指标':
            new_data,project_id,category_id = query_timedim_by_subject(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询维度':
            new_data,project_id,category_id = query_timedim(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询维度':
            new_data,project_id,category_id = query_timedim_by_name(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询维度':
            new_data,project_id,category_id = query_timedim_by_subject(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改指标':
            new_data,field_defined_id = update_primary(data)
            new_url = url.format(field_defined_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改维度':
            new_data,field_defined_id = update_dimension(data)
            new_url = url.format(field_defined_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询物化配置':
            new_data,metadata_id = query_physical(data)
            new_url = url.format(metadata_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询物化配置':
            new_data,metadata_id = query_physical_by_name(data)
            new_url = url.format(metadata_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改物化配置':
            new_data,metadata_id,physical_id = update_physical(data)
            new_url = url.format(metadata_id,physical_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询物化记录':
            new_url = url.format(data[0],data[1])
            log.info("request   url：%s " %new_url)
            new_data = {"pageable":{"pageNum":0,"pageSize":10,"pageable":"true"}}
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询逻辑模型字段':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询逻辑模型字段':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%主%"]}]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询逻辑模型版本':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询逻辑模型版本':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%a%"]}]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '查询数据建模' in case_detail:
            new_data,project_id,subject_id,category_id = query_metadata_model(data)
            new_url = url.format(project_id,subject_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '根据名称查询数据建模' in case_detail:
            new_data,project_id,subject_id,category_id = query_metadata_model_by_name(data)
            new_url = url.format(project_id,subject_id,category_id)
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询model信息':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data[0],data[1])
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询model信息':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"name","oper":"LIKE","value":["%o%"]}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
            new_url = url.format(data[0],data[1])
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询model信息':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"name","oper":"LIKE","value":["%o%"]},{"andOr":"AND","name":"subjectId","oper":"EQUAL","value":["836179258303840256"]}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
            new_url = url.format(data[0],data[1])
            log.info("request   url：%s " %new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '获取数据建模目标逻辑表':
            new_data = get_target_metadata(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        else:
            if data:
                data = str(data)
                # 字典形式作为参数，如{"id":"7135cf6e-2b12-4282-90c4-bed9e2097d57","name":"gbj_for_jdbcDatasource_create_0301_1_0688","creator":"admin"}
                if data.startswith('{') and data.endswith('}'):
                    data_dict = dict_res(data)
                    log.info("request   url：%s " %url)
                    response = requests.post(url=url, headers=headers, json=data_dict)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                # 列表作为参数， 如["9d3639f0-02bc-44cd-ac71-9a6d0f572632"]
                elif data.startswith('[') and data.endswith(']'):
                    if "'" in data:
                        data = data.replace("'", '"')
                        log.info("request   data：%s " %data)
                        log.info("request   url：%s " %url)
                        response = requests.post(url=url, headers=headers, data=data)
                        log.info("response data：%s %s" %(response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                    else:
                        log.info("request   data：%s " %data)
                        log.info("request   url：%s " %url)
                        response = requests.post(url=url, headers=headers, data=data)
                        log.info("response data：%s %s" %(response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:  # 处理参数是一个字符串id的情况,按照接口格式，放入list中处理
                    new_data = []
                    new_data.append(data)
                    new_data = str(new_data)
                    if "'" in new_data:
                        new_data = new_data.replace("'", '"')
                        log.info("request   data：%s " %new_data)
                        response = requests.post(url=url, headers=headers, data=new_data)
                        log.info("response data：%s %s" %(response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                    else:
                        log.info("request   url：%s " %url)
                        response = requests.post(url=url, headers=headers, data=new_data)
                        log.info("response data：%s %s" %(response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            else: #data为空
                log.info("request   url：%s " %url)
                response = requests.post(url=url, headers=headers, data=data)
                log.info("response data：%s %s" %(response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    except Exception as e:
        log.error("异常信息：%s" %e)


# GET请求
def get_request_result_check(url, headers, data, table_sheet_name, row, column):
    try:
        case_detail = case_table_sheet.cell(row=row, column=2).value
        log.info("开始执行：%s " %case_detail)
        # GET请求需要从parameter中获取参数,并把参数拼装到URL中，
        if data:
            if '根据id查询项目' in case_detail:
                new_url = url.format(data)
                log.info("request   url：%s " %new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" %(response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif '查询标签组关联标签' in case_detail:
                new_url = url.format(data)
                log.info("request   url：%s " %new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" %(response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif '查询资源配置物理表字段' == case_detail:
                new_url = url.format(data[0],data[1])
                log.info("request   url：%s " %new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" %(response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif '查询资源配置物理表字段实体' == case_detail:
                new_url = url.format(data)
                log.info("request   url：%s " %new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" %(response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            else:
                if '&' in str(data):  # 包含多个参数并以&分割
                    parameters = data.split('&')
                    # 处理存在select语句中的参数，并重新赋值
                    for i in range(len(parameters)):
                        if parameters[i].startswith('select id from'):
                            try:
                                select_result = ms.ExecuQuery(parameters[i])
                                parameters[i] = select_result[0]["id"]
                            except:
                                log.info("第%s行参数没有返回结果" %row)
    
                        elif parameters[i].startswith('select name from'):
                            try:
                                select_result = ms.ExecuQuery(parameters[i])
                                parameters[i] = select_result[0]["name"]
                            except:
                                log.info("第%s行参数没有返回结果" %row)
                        elif parameters[i].startswith('select execution_id from'):
                            try:
                                select_result = ms.ExecuQuery(parameters[i])
                                parameters[i] = select_result[0]["execution_id"]
                            except:
                                log.info("第%s行参数没有返回结果" %row)
                    # 判断URL中需要的参数个数，并比较和data中的参数个数是否相等
                    if len(parameters) == 1:
                        try:
                            url_new = url.format(parameters[0])
                            log.info("request   url：%s " %url_new)
                            response = requests.get(url=url_new, headers=headers)
                            log.info("response data：%s %s" %(response.status_code, response.text))
                        except Exception:
                            return
                        log.info("response data：%s %s" %(response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                    elif len(parameters) == 2:
                        url_new = url.format(parameters[0], parameters[1])
                        log.info("request   url：%s " %url_new)
                        response = requests.get(url=url_new, headers=headers)
                        log.info("response data：%s %s" %(response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                    elif len(parameters) == 3:
                        url_new = url.format(parameters[0], parameters[1], parameters[2])
                        log.info("request   url：%s " %url_new)
                        response = requests.get(url=url_new, headers=headers)
                        log.info("response data：%s %s" %(response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                    else:
                        log.info("请确认第%d行parameters" %row)
                elif len(data) == 2:
                    url_new = url.format(data[0], data[1])
                    log.info("request   url：%s " %url_new)
                    response = requests.get(url=url_new, headers=headers)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:  # 参数中不包含&，只有一个参数
                    url_new = url.format(data)
                    log.info("request   url：%s " %url_new)
                    response = requests.get(url=url_new, headers=headers)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        # GET 请求参数写在URL中，直接发送请求
        else:
            log.info("request   url：%s " %url)
            response = requests.get(url=url, headers=headers)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    except Exception as e:
        log.error("异常信息：%s" %e)

# PUT请求
def put_request_result_check(url, row, data, table_sheet_name, column, headers):
    try:
        case_detail = case_table_sheet.cell(row=row, column=2).value
        log.info("开始执行：%s " %case_detail)
        #if data and isinstance(data, str):
        if data :
            if '&' in str(data):
                # 分隔参数
                parameters = data.split('&')
                # 拼接URL
                new_url = url.format(parameters[0])
                log.info("request   url：%s " %new_url)
                # 发送的参数体
                parameters_data = parameters[-1]
                if parameters_data.startswith('{'):
                    response = requests.put(url=new_url, headers=headers, json=dict_res(parameters_data))
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(table_sheet_name, row, column, response.status_code)
                    write_result(table_sheet_name, row, column+4, response.text)
                else:
                    log.info("请确认第%d行parameters中需要update的值格式，应为id&{data}" %row)
            else:
                if data.startswith('select id'):
                    result = ms.ExecuQuery(data)
                    new_data = result[0]["id"]
                    new_url = url.format(new_data)
                    log.info("request   url：%s " %url)
                    response = requests.put(url=new_url, headers=headers)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(table_sheet_name, row, column, response.status_code)
                    write_result(table_sheet_name, row, column + 4, response.text)
                elif data.startswith('{') and data.endswith('}'):
                    log.info("request   url：%s " %url)
                    response = requests.put(url=url, headers=headers, data=data.encode('utf-8'))
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(table_sheet_name, row, column, response.status_code)
                    write_result(table_sheet_name, row, column + 4, response.text)
                elif data.startswith('[') and data.endswith(']'):
                    pass
                elif case_detail == '新增主题域':
                    delete_autotest_dw() #清理dw测试数据
                    new_data, business_id = add_subject_data(data)
                    new_url = url.format(business_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增项目':
                    new_data, business_id = add_projects_data(data)
                    new_url = url.format(business_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增标签组':
                    new_data = add_taggroup_data(data)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '项目关联标签组规则':
                    new_data = rel_product_taggroup(data)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增模型目录':
                    new_data=add_model_category(data)
                    new_data=json.dumps(new_data, separators=(',', ':'))
                    response=requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增子模型目录':
                    new_data,model_category_id = add_child_model_category(data)
                    new_url = url.format(model_category_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增标准目录':
                    new_data=add_standard_category(data)
                    new_data=json.dumps(new_data, separators=(',', ':'))
                    response=requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增子标准目录':
                    new_data,standard_category_id = add_child_standard_category(data)
                    new_url = url.format(standard_category_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增资源配置物理表':
                    new_data=rel_physical_dataset(data)
                    new_data=json.dumps(new_data, separators=(',', ':'))
                    response=requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增逻辑模型transaction':
                    new_data,project_id,subject_id = add_model_metadata(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '保存逻辑模型新版本':
                    new_data,metadata_id,subject_id = save_model_metadata_info(data)
                    new_url = url.format(metadata_id,subject_id)
                    log.info("request   url：%s " %new_url)
                    log.info("request   data：%s " %new_data)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增时间维度':
                    new_data,project_id,subject_id = add_primary(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增主键':
                    new_data,project_id,subject_id = add_primary(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增属性':
                    new_data,project_id,subject_id = add_primary(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增度量':
                    new_data,project_id,subject_id = add_primary(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增指标':
                    new_data,project_id,subject_id = add_indicator(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增维度':
                    new_data,project_id,subject_id = add_dimension(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增物化配置':
                    new_data,metadata_id = add_physical(data)
                    new_url = url.format(metadata_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif '新增逻辑模型字段' in case_detail:
                    new_data,metadata_id = add_metadata_field(data)
                    new_url = url.format(metadata_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增数据建模逻辑表':
                    new_data,project_id,category_id = new_data_model(data)
                    new_url = url.format(project_id,category_id)
                    log.info("request   url：%s " %new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '提交数据建模逻辑模型':
                    new_data = publish_data_model(data)
                    log.info("request   url：%s " %url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:
                    new_url = url.format(data)
                    log.info("request   url：%s " %url)
                    response = requests.put(url=new_url, headers=headers)
                    log.info("response data：%s %s" %(response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(table_sheet_name, row, column, response.status_code)
                    write_result(table_sheet_name, row, column + 4, response.text)

        else:
            log.info("request   url：%s " %url)
            response = requests.put(url=url, headers=headers)
            log.info("response data：%s %s" %(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(table_sheet_name, row, column, response.status_code)
            write_result(table_sheet_name, row, column + 4, response.text)
    except Exception as e:
        log.error("异常信息：%s" %e)
        
def delete_request_result_check(url, data, table_sheet_name, row, column, headers):
    try:
        case_detail = case_table_sheet.cell(row=row, column=2).value
        log.info("开始执行：%s " %case_detail)
        #if isinstance(data, str):
        if data:
            if str(data).startswith('select id'):  # sql语句的查询结果当做参数
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                datas = []
                if data_select_result:
                    try:
                        for i in range(len(data_select_result)):
                            datas.append(data_select_result[i]["id"])
                    except:
                        log.info("请确认第%d行SQL语句" %row)
                    else:
                        if len(datas) == 1:
                            new_url = url.format(datas[0])
                            log.info("request   url：%s " %new_url)
                            response = requests.delete(url=new_url, headers=headers)
                            log.info("response data：%s %s" %(response.status_code, response.text))
                            # 将返回的status_code和response.text分别写入第10列和第14列
                            clean_vaule(table_sheet_name, row, column)
                            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                        else:
                            log.info("请确认 select 语句查询返回值是不是只有一个")
                else:
                    log.info("第%d行参数查询无结果" %row)
            elif "删除逻辑模型字段" in case_detail:
                metadata_id,name = metadata_field(data)
                new_url = url.format(metadata_id,name)
                log.info("request   url：%s " %new_url)
                response = requests.delete(url=new_url, headers=headers)
                log.info("response data：%s %s" %(response.status_code, response.text))
                # 将返回的status_code和response.text分别写入第10列和第14列
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)# 字典形式作为参数，如{"id":"7135cf6e-2b12-4282-90c4-bed9e2097d57","name":"gbj_for_jdbcDatasource_create_0301_1_0688","creator":"admin"}
            elif len(data)==2:
                new_url = url.format(data[0],data[1])
                log.info("request   url：%s " %new_url)
                response = requests.delete(url=new_url, headers=headers)
                log.info("response data：%s %s" %(response.status_code, response.text))
                # 将返回的status_code和response.text分别写入第10列和第14列
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            else:
                new_url = url.format(data)
                log.info("request   url：%s " %new_url)
                response = requests.delete(url=new_url, headers=headers)
                log.info("response data：%s %s" %(response.status_code, response.text))
                # 将返回的status_code和response.text分别写入第10列和第14列
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        else:
            new_url = url.format(data)
            log.info("request   url：%s " %new_url)
            response = requests.delete(url=new_url, headers=headers)
            log.info("response data：%s %s" %(response.status_code, response.text))
            # 将返回的status_code和response.text分别写入第10列和第14列
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    except Exception as e:
        log.error("异常信息：%s" %e)          


#  写入返回结果
def write_result(sheet, row, column, value):
    sheet.cell(row=row, column=column, value=value)


#  写入结果前，先把结果和对比结果全部清空
def clean_vaule(sheet, row, column):
    sheet.cell(row=row, column=column, value='')
    sheet.cell(row=row, column=column+1, value='')
    sheet.cell(row=row, column=column + 4, value='')
    sheet.cell(row=row, column=column + 5, value='')
    sheet.cell(row=row, column=column + 6, value='')
    sheet.cell(row=row, column=column + 7, value='')


# 对比code和text
class CheckResult(unittest.TestCase):

    def compare_code_result(self):
        """1.对比预期code和接口响应返回的status code"""
        for row in range(2, all_rows+1):
            # 预期status code和接口返回status code
            ex_status_code = case_table_sheet.cell(row=row, column=7).value
            ac_status_code = case_table_sheet.cell(row=row, column=8).value
            # 判断两个status code是否相等
            if ex_status_code and ac_status_code != '':
                # code相等时，pass
                if ex_status_code == ac_status_code:
                    case_table_sheet.cell(row=row, column=9, value='pass')
                else:
                    case_table_sheet.cell(row=row, column=9, value='fail') # code不等时，用例结果直接判断为失败
                    case_table_sheet.cell(row=row, column=15, value='%s--->失败原因：返回status_code对比失败,预期为%s,实际为%s' %
                                                                    (case_table_sheet.cell(row=row, column=2).value, case_table_sheet.cell(row=row, column=7).value, case_table_sheet.cell(row=row, column=8).value))
            else:
                log.info("第 %d 行 status_code为空" %row)
        case_table.save(ab_dir('api_cases.xlsx'))

    # 对比预期response和实际返回的response.text，根据预期和实际结果的关系进行处理
    def compare_text_result(self):
        for row in range(2, all_rows+1):
            response_text = case_table_sheet.cell(row=row, column=12).value  # 接口返回的response.text
            response_text_dict = dict_res(response_text)
            expect_text = case_table_sheet.cell(row=row, column=10).value  # 预期结果
            key_word = case_table_sheet.cell(row=row, column=3).value  # 接口关键字
            code_result = case_table_sheet.cell(row=row, column=9).value  # status_code对比结果
            relation = case_table_sheet.cell(row=row, column=11).value  # 预期text和response.text的关系
            #  1.status_code 对比结果pass的前提下，判断response.text断言是否正确,
            #  2.status_code 对比结果fail时，用例整体结果设为fail
            if code_result == 'pass':
                if key_word in ('create', 'query', 'update', 'delete'):
                    self.assert_deal(key_word, relation, expect_text, response_text, response_text_dict, row, 13)
                else:
                    log.info("请确认第%d行的key_word" %row)
            elif code_result == 'fail':
                # case 结果列
                case_table_sheet.cell(row=row, column=14, value='fail')
                # case失败原因
                case_table_sheet.cell(row=row, column=15, value='%s--->失败原因：返回status_code对比失败,预期为%s,实际为%s' %
                                                                (case_table_sheet.cell(row=row, column=2).value, case_table_sheet.cell(row=row, column=7).value, case_table_sheet.cell(row=row, column=8).value))
            else:
                log.info("请确认第 %d 行 status_code对比结果" %row)

        case_table.save(ab_dir('api_cases.xlsx'))

    #  根据expect_text, response_text的关系，进行断言, 目前只处理了等于和包含两种关系
    def assert_deal(self, key_word, relation, expect_text, response_text, response_text_dict, row, column):
        if key_word == 'create':
            if relation == '=':   # 只返回id时，判断返回内容中包含id属性，id长度为36
                if isinstance(response_text_dict, dict):
                    if response_text_dict.get("id"):
                        # 返回的内容中包含 id属性，判断返回的id长度和预期给定的id长度一致
                        try:
                            self.assertEqual(expect_text, len(response_text_dict['id']), '第%d行的response_text长度和预期不一致' % row)
                        except:
                            log.info("第 %d 行 response_text返回的id和预期id长度不一致" %row)
                            case_table_sheet.cell(row=row, column=column, value='fail')
                        else:
                            case_table_sheet.cell(row=row, column=column, value='pass')
                    else:
                        try:
                            self.assertEqual(expect_text, response_text, '第%d行的response_text长度和预期不一致' % row)
                        except:
                            log.info("第 %d 行 response_text和预期text不相等" %row)
                            case_table_sheet.cell(row=row, column=column, value='fail')
                        else:
                            case_table_sheet.cell(row=row, column=column, value='pass')
                else:  # 只返回一个id串的情况下，判断预期长度和id长度一致
                    try:
                        self.assertEqual(expect_text, len(response_text), '第%d行的response_text长度和预期不一致' % row)
                    except:
                        log.info("第 %d 行 response_text和预期text不相等" %row)
                        case_table_sheet.cell(row=row, column=column, value='fail')
                    else:
                        case_table_sheet.cell(row=row, column=column, value='pass')

            elif relation == 'in':  # 返回多内容时，判断返回内容中包含id属性，并且expect_text包含在response_text中
                try:
                    # self.assertIsNotNone(response_text_dict.get("id"), '第 %d 行 response_text没有返回id' % row)
                    self.assertIn(expect_text, response_text, '第 %d 行 expect_text没有包含在接口返回的response_text中' % row)
                except:
                    log.info("第 %d 行 expect_text没有包含在response_text中， 结果对比失败" %row)
                    case_table_sheet.cell(row=row, column=column, value='fail')
                else:
                    case_table_sheet.cell(row=row, column=column, value='pass')
            else:
                log.info("请确认第 %d 行 预期expect_text和response_text的relatrion" %row)
                case_table_sheet.cell(row=row, column=column, value='请确认预期text和接口response.text的relatrion')
        elif key_word in ('query', 'update', 'delete'):
            if relation == '=':
                compare_result = re.findall('[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}', '%s' % (response_text))
                response_text_list = []
                response_text_list.append(response_text)
                # 返回值是id 串，字母和数字的组合
                if compare_result == response_text_list:
                    try:
                        self.assertEqual(expect_text, len(response_text), '第%s行expect_text和response_text不相等' % row)
                    except:
                        case_table_sheet.cell(row=row, column=column, value='fail')
                    else:
                        case_table_sheet.cell(row=row, column=column, value='pass')
                # 返回空值
                elif expect_text == None and response_text == "":
                    case_table_sheet.cell(row=row, column=column, value='pass')

                else:
                    try:
                        self.assertEqual(expect_text, response_text, '第%s行expect_text和response_text不相等' % row)
                    except:
                        case_table_sheet.cell(row=row, column=column, value='fail')
                    else:
                        case_table_sheet.cell(row=row, column=column, value='pass')

            elif relation == 'in':
                try:
                    self.assertIn(expect_text, response_text, '第 %d 行 expect_text没有包含在接口返回的response_text中' % row)
                except:
                    log.info("第 %d 行 expect_text和response_text不相等， 结果对比失败" % row)
                    case_table_sheet.cell(row=row, column=column, value='fail')
                else:
                    case_table_sheet.cell(row=row, column=column, value='pass')
            else:
                log.info("请确认第 %d 行 预期expect_text和response_text的relation" % row)
                case_table_sheet.cell(row=row, column=column, value='请确认预期text和接口response.text的relation')
        else:
            log.info("请确认第 %d 行 的key_word" % row)
        case_table.save(ab_dir('api_cases.xlsx'))

    # 对比case最终的结果
    def deal_result(self):
        # 对比code
        self.compare_code_result()
        # 对比text
        self.compare_text_result()
        # 根据code result和text result判断case最终结果
        for row in range(2, all_rows + 1):
            status_code_result = case_table_sheet.cell(row=row, column=9).value
            response_text_result = case_table_sheet.cell(row=row, column=13).value
            if status_code_result == 'pass' and response_text_result == 'pass':
                log.info("测试用例-%s pass" % case_table_sheet.cell(row=row, column=2).value)
                case_table_sheet.cell(row=row, column=14, value='pass')
                case_table_sheet.cell(row=row, column=15, value='')
            elif status_code_result == 'fail' or response_text_result in ('fail',''):
                log.info("测试用例-%s fail" % case_table_sheet.cell(row=row, column=2).value)
                case_table_sheet.cell(row=row, column=14, value='fail')
                case_table_sheet.cell(row=row, column=15, value='%s--->失败原因：status code对比失败,预期为%s,实际为%s' \
                                                                % (case_table_sheet.cell(row=row, column=2).value, case_table_sheet.cell(row=row, column=7).value, case_table_sheet.cell(row=row, column=8).value))
            elif status_code_result == 'pass' and response_text_result == 'fail':
                log.info("测试用例-%s fail" % case_table_sheet.cell(row=row, column=2).value)
                case_table_sheet.cell(row=row, column=14, value='fail')
                case_table_sheet.cell(row=row, column=15, value='%s--->失败原因：返回内容对比失败,预期为%s,实际为%s' %
                                                                (case_table_sheet.cell(row=row, column=2).value, case_table_sheet.cell(row=row, column=10).value, case_table_sheet.cell(row=row, column=12).value))
            else:
                log.info("请确认status code或response.text对比结果")
        case_table.save(ab_dir('api_cases.xlsx'))