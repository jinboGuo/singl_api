# coding:utf-8
import json
import os
import re
import time
from new_api_cases.get_statementId import get_sql_analyse_dataset_info, get_sql_analyse_statement_id, \
    get_sql_execte_statement_id
from util import myddt
import xlrd
from openpyxl import load_workbook
import requests
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from basic_info.ready_dataflow_data import delete_autotest_dw
from util.format_res import dict_res
from basic_info.setting import dw_host, dw_sheet, dw_cases_dir, log
from basic_info.get_auth_token import get_headers_root, get_headers
from new_api_cases.dw_deal_parameters import deal_parameters
import unittest
from new_api_cases.dw_prepare_datas import update_business_data, \
    add_subject_data, update_subject_data, query_subject_data, add_projects_data, update_projects_data, update_tag_data, \
    add_taggroup_data, update_taggroup_data, update_namerule_data, rel_product_taggroup, update_model_category, \
    add_child_model_category, add_model_category, add_standard_category, \
    add_child_standard_category, update_standard_category, rel_physical_dataset, update_physical_dataset, \
    query_physical_dataset, query_physical_dataset_by_name, \
    query_physical_dataset_by_subject, add_model_metadata, update_model_metadata, query_model_metadata, \
    query_model_metadata_by_name, query_model_metadata_by_subject, \
    save_model_metadata_info, query_timedim, query_timedim_by_name, query_timedim_by_subject, add_primary, \
    update_primary, add_physical, query_physical, query_physical_by_name, \
    update_physical, add_indicator, add_dimension, add_metadata_field, metadata_field, query_metadata_model_by_name, \
    query_metadata_model, update_dimension, add_physical_field, \
    del_physical_field, get_target_metadata, new_data_model, publish_data_model, get_asset_directory, \
    update_asset_directory, move_asset_directory, duplicate_asset_directory, duplicate_move_asset_directory, \
    delete_asset_directory,update_asset,create_sql_asset, sql_analyse_data, batch_create_asset, get_improt_data

cases_dir = dw_cases_dir
case_table = load_workbook(cases_dir)
dw_master=dw_sheet
case_table_sheet = case_table.get_sheet_by_name(dw_master)
all_rows = case_table_sheet.max_row
host = dw_host
woven_dir = os.path.join(os.path.abspath('.'),'attachment\import_autotest_api_df.woven')


def deal_request_method():
    """
    判断请求方法，并根据不同的请求方法调用不同的处理方式
    :return:
    """
    for i in range(2, all_rows+1):
        request_method = case_table_sheet.cell(row=i, column=4).value
        request_method_upper = request_method.upper()
        request_url = host+case_table_sheet.cell(row=i, column=5).value
        old_data = case_table_sheet.cell(row=i, column=6).value
        request_data = deal_parameters(old_data,request_method_upper,request_url)
        log.info("request  data：%s" % request_data)
        api_name = case_table_sheet.cell(row=i, column=1).value
        is_run = case_table_sheet.cell(row=i, column=16).value

        if request_method_upper:
            if is_run =='Y' or is_run=='y':
                if api_name == 'tenants':
                    """
                    租户的用例需要使用root用户登录后操作
                    根据不同的请求方法，进行分发
                    """
                    if request_method_upper == 'POST':
                        post_request_result_check(row=i, column=8, url=request_url, headers=get_headers_root(), data=request_data, table_sheet_name=case_table_sheet)
                    elif request_method_upper == 'GET':
                        get_request_result_check(url=request_url, headers=get_headers_root(), data=request_data, table_sheet_name=case_table_sheet, row=i, column=8)
                    elif request_method_upper == 'PUT':
                        put_request_result_check(url=request_url, row=i, data=request_data, table_sheet_name=case_table_sheet, column=8, headers=get_headers_root())
                    elif request_method_upper == 'DELETE':
                        delete_request_result_check(url=request_url, data=request_data, table_sheet_name=case_table_sheet, row=i, column=8, headers=get_headers_root())
                    else:
                        log.info("请求方法%s不在处理范围内" % request_method)
                else:
                    """根据不同的请求方法，进行分发"""
                    if request_method_upper == 'POST':
                        post_request_result_check(row=i, column=8, url=request_url, headers=get_headers(), data=request_data, table_sheet_name=case_table_sheet)
                    elif request_method_upper == 'GET':
                        get_request_result_check(url=request_url, headers=get_headers(), data=request_data, table_sheet_name=case_table_sheet, row=i, column=8)
                    elif request_method_upper == 'PUT':
                        put_request_result_check(url=request_url, row=i, data=request_data, table_sheet_name=case_table_sheet, column=8, headers=get_headers())
                    elif request_method_upper == 'DELETE':
                        delete_request_result_check(url=request_url, data=request_data, table_sheet_name=case_table_sheet, row=i, column=8, headers=get_headers())
                    else:
                        log.info("请求方法%s不在处理范围内" % request_method)
            else:
                log.info(" 第%d 行脚本未执行，请查看isRun是否为Y或者y！"% i)
        else:
            log.info("第 %d 行请求方法为空" % i)
    '''执行结束后保存表格'''
    case_table.save(cases_dir)



def post_request_result_check(row, column, url, headers, data, table_sheet_name):
    """
    POST接口请求，脚本里post请求的处理
    :param row:
    :param column:
    :param url:
    :param headers:
    :param data:
    :param table_sheet_name:
    :return:
    """
    try:
        case_detail = case_table_sheet.cell(row=row, column=2).value
        log.info("开始执行：%s " % case_detail)
        if case_detail == '根据id查询主题域':
            new_data, business_id = query_subject_data(data)
            new_url = url.format(business_id)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改业务模块':
            new_data,business = update_business_data(data)
            new_url = url.format(business)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改主题域':
            new_data, subject_id = update_subject_data(data)
            new_url = url.format(subject_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改项目':
            new_data, protect_id = update_projects_data(data)
            new_url = url.format(protect_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改标签':
            new_data, tag_id = update_tag_data(data)
            new_url = url.format(tag_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改标签组':
            new_data, taggroup_id = update_taggroup_data(data)
            new_url = url.format(taggroup_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改已绑定标签组的标签':
            new_data, tag_id = update_tag_data(data)
            new_url = url.format(tag_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改命名规则':
            new_data, namerule_id = update_namerule_data(data)
            new_url = url.format(namerule_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据业务模块查询数据地图':
            new_data = {"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询数据地图':
            new_data = {"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
            new_url = url.format(data[0], data[1])
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询模型字段详情':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据字段名称查询字段详情':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%id%"]}]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改模型目录':
            new_data, model_category_id = update_model_category(data)
            new_url = url.format(model_category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改子模型目录':
            new_data, model_category_id = update_model_category(data)
            new_url = url.format(model_category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改标准目录':
            new_data, standard_category_id = update_standard_category(data)
            new_url = url.format(standard_category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改子标准目录':
            new_data, standard_category_id = update_standard_category(data)
            new_url = url.format(standard_category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改资源配置物理表':
            new_data, ref_dataset_id = update_physical_dataset(data)
            new_url = url.format(ref_dataset_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '添加物理表字段' in case_detail:
            new_data,metadata_id = add_physical_field(data)
            new_url = url.format(metadata_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '删除物理表字段' in case_detail:
            new_data,metadata_id = del_physical_field(data)
            new_url = url.format(metadata_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询资源配置物理表':
            new_data, ref_project_id ,ref_category_id = query_physical_dataset(data)
            new_url = url.format(ref_project_id ,ref_category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '按照名称查询资源配置物理表':
            new_data, ref_project_id, ref_category_id = query_physical_dataset_by_name(data)
            new_url = url.format(ref_project_id, ref_category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '按照主题域查询资源配置物理表':
            new_data, ref_project_id ,ref_category_id = query_physical_dataset_by_subject(data)
            new_url = url.format(ref_project_id ,ref_category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改逻辑模型transaction':
            new_data,metadata_id = update_model_metadata(data)
            new_url = url.format(metadata_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询逻辑模型transaction':
            new_data, project_id = query_model_metadata(data)
            new_url = url.format(project_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '按照名称查询逻辑模型transaction':
            new_data, project_id = query_model_metadata_by_name(data)
            new_url = url.format(project_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '按照主题域查询逻辑模型transaction':
            new_data, project_id = query_model_metadata_by_subject(data)
            new_url = url.format(project_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '保存逻辑模型-' in case_detail:
            new_data,metadata_id,subject_id = save_model_metadata_info(data)
            new_url = url.format(metadata_id,subject_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改时间维度':
            new_data,field_defined_id = update_primary(data)
            new_url = url.format(field_defined_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询时间维度':
            new_data,project_id,category_id = query_timedim(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询时间维度':
            new_data,project_id,category_id = query_timedim_by_name(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询时间维度':
            new_data,project_id,category_id = query_timedim_by_subject(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改主键':
            new_data,field_defined_id = update_primary(data)
            new_url = url.format(field_defined_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询主键':
            new_data,project_id,category_id = query_timedim(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询主键':
            new_data,project_id,category_id = query_timedim_by_name(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询主键':
            new_data,project_id,category_id = query_timedim_by_subject(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改属性':
            new_data,field_defined_id = update_primary(data)
            new_url = url.format(field_defined_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询属性':
            new_data,project_id,category_id = query_timedim(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询属性':
            new_data,project_id,category_id = query_timedim_by_name(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询属性':
            new_data,project_id,category_id = query_timedim_by_subject(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改度量':
            new_data,field_defined_id = update_primary(data)
            new_url = url.format(field_defined_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询度量':
            new_data,project_id,category_id = query_timedim(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询度量':
            new_data,project_id,category_id = query_timedim_by_name(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询度量':
            new_data,project_id,category_id = query_timedim_by_subject(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询指标':
            new_data,project_id,category_id = query_timedim(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询指标':
            new_data,project_id,category_id = query_timedim_by_name(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询指标':
            new_data,project_id,category_id = query_timedim_by_subject(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询维度':
            new_data,project_id,category_id = query_timedim(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询维度':
            new_data,project_id,category_id = query_timedim_by_name(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询维度':
            new_data,project_id,category_id = query_timedim_by_subject(data)
            new_url = url.format(project_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改指标':
            new_data,field_defined_id = update_primary(data)
            new_url = url.format(field_defined_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改维度':
            new_data,field_defined_id = update_dimension(data)
            new_url = url.format(field_defined_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询物化配置':
            new_data,metadata_id = query_physical(data)
            new_url = url.format(metadata_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询物化配置':
            new_data,metadata_id = query_physical_by_name(data)
            new_url = url.format(metadata_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '修改物化配置':
            new_data,metadata_id,physical_id = update_physical(data)
            new_url = url.format(metadata_id,physical_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询物化记录':
            new_url = url.format(data[0], data[1])
            log.info("request   url：%s " % new_url)
            new_data = {"pageable":{"pageNum":0,"pageSize":10,"pageable":"true"}}
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询逻辑模型字段':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询逻辑模型字段':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%主%"]}]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询逻辑模型版本':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询逻辑模型版本':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"alias","oper":"LIKE","value":["%a%"]}]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '查询数据建模' in case_detail:
            new_data,project_id,subject_id,category_id = query_metadata_model(data)
            new_url = url.format(project_id,subject_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '根据名称查询数据建模' in case_detail:
            new_data,project_id,subject_id,category_id = query_metadata_model_by_name(data)
            new_url = url.format(project_id,subject_id,category_id)
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查询model信息':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[]},"pageable":{"pageNum":0,"pageSize":8,"pageable":"false"},"ordSort":[{"name":"createTime","order":"DESC"}]}
            new_url = url.format(data[0], data[1])
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据名称查询model信息':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"name","oper":"LIKE","value":["%o%"]}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
            new_url = url.format(data[0], data[1])
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据主题域查询model信息':
            new_data = {"fieldGroup":{"andOr":"AND","fields":[{"andOr":"OR","name":"name","oper":"LIKE","value":["%o%"]},{"andOr":"AND","name":"subjectId","oper":"EQUAL","value":["836179258303840256"]}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":8,"pageable":"true"}}
            new_url = url.format(data[0], data[1])
            log.info("request   url：%s " % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '获取数据建模目标逻辑表':
            new_data = get_target_metadata(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '添加资产目录':
            new_data = get_asset_directory(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '资产目录移动':
            parent_id, new_data = move_asset_directory(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            new_url = url.format(parent_id)
            log.info("request   url：%s " % new_url)
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '添加重名资产目录':
            new_data = duplicate_asset_directory(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '创建服务视图资产' in case_detail:
            new_data = create_sql_asset(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '初始化Sql' in case_detail:
            new_data = sql_analyse_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '获取SQL执行任务结果':
            exec_use_params = get_sql_analyse_dataset_info(host, data)
            exec_use_params = json.dumps(exec_use_params, separators=(',', ':'))
            statement_id, session_id, cluster_id = get_sql_execte_statement_id(host, data)
            new_url = url.format(statement_id, session_id, cluster_id)
            log.info("request   url：%s " % new_url)
            response = requests.post(url=new_url, headers=headers, data=exec_use_params)
            count_num = 0
            while "waiting" in response.text or "running" in response.text:
                log.info("再次查询前：%s %s" % (response.status_code, response.text))
                response = requests.post(url=new_url, headers=headers, data=exec_use_params)
                time.sleep(5)
                count_num += 1
                if count_num == 50:
                    return
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '批量创建数据集资产':
            new_data = batch_create_asset(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif "上传woven文件" in case_detail:
            fs = {"file": open(woven_dir, 'rb')}
            headers.pop('Content-Type')
            response = requests.post(url=url, headers=headers, files=fs)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        elif "导入woven文件" in case_detail:
            new_data = get_improt_data(headers, dw_host)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=get_headers(), data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        else:
            if data:
                data = str(data)
                if data.startswith('{') and data.endswith('}'):
                    data_dict = dict_res(data)
                    response = requests.post(url=url, headers=headers, json=data_dict)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif data.startswith('[') and data.endswith(']'):
                    response = requests.post(url=url, headers=headers, data=data)
                    time.sleep(3)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:
                    log.error("请求体数据错误！")
            else:
                response = requests.post(url=url, headers=headers, data=data)
                time.sleep(1)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    except Exception as e:
        log.error("测试用例{}执行过程中出错{}".format(case_detail, e))



def get_request_result_check(url, headers, data, table_sheet_name, row, column):
    """
    GET接口请求需要从parameter中获取参数,并把参数拼装到URL中
    :param url:
    :param headers:
    :param data:
    :param table_sheet_name:
    :param row:
    :param column:
    :return:
    """
    try:
        case_detail = case_table_sheet.cell(row=row, column=2).value
        log.info("开始执行：%s " %case_detail)
        if data:
            if '根据id查询项目' in case_detail:
                new_url = url.format(data)
                log.info("request   url：%s " % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif '查询标签组关联标签' in case_detail:
                new_url = url.format(data)
                log.info("request   url：%s " % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif '查询资源配置物理表字段' == case_detail:
                new_url = url.format(data[0], data[1])
                log.info("request   url：%s " % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif '查询资源配置物理表字段实体' == case_detail:
                new_url = url.format(data)
                log.info("request   url：%s " % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '根据statement id,获取Sql Analyze结果(获取输出字段)':
                statement_id, sessionId, clusterId = get_sql_analyse_statement_id(host, data)
                new_url = url.format(statement_id, sessionId, clusterId)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                count_num = 0
                while "waiting" in response.text or "running" in response.text:
                    log.info("再次查询前：%s %s" % (response.status_code, response.text))
                    response = requests.get(url=new_url, headers=headers)
                    count_num += 1
                    if count_num == 100:
                        return
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '结束指定statementId对应的查询任务':  # 取消SQL analyse接口
                cancel_statement_id = get_sql_analyse_statement_id(host, data)
                new_url = url.format(cancel_statement_id)
                log.info("request   url：%s " % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            else:
                new_url = url.format(data)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        else:
            response = requests.get(url=url, headers=headers)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    except Exception as e:
        log.error("{}执行过程中出错{}".format(case_detail, e))


def put_request_result_check(url, row, data, table_sheet_name, column, headers):
    """
    PUT接口请求
                if '&' in str(data):
                # 分隔参数
                parameters = data.split('&')
                # 拼接URL
                new_url = url.format(parameters[0])
    :param url:
    :param row:
    :param data:
    :param table_sheet_name:
    :param column:
    :param headers:
    :return:
    """
    try:
        case_detail = case_table_sheet.cell(row=row, column=2).value
        log.info("开始执行：%s " %case_detail)
        #if data and isinstance(data, str):
        if data :
                if case_detail == '新增业务模块':
                    delete_autotest_dw()
                    response = requests.put(url=url, headers=headers, data=data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(table_sheet_name, row, column + 4, response.text)
                elif data.startswith('{') and data.endswith('}'):
                    log.info("request   url：%s " %url)
                    response = requests.put(url=url, headers=headers, data=data.encode('utf-8'))
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(table_sheet_name, row, column, response.status_code)
                    write_result(table_sheet_name, row, column + 4, response.text)
                elif case_detail == '新增主题域':
                    new_data, business_id = add_subject_data(data)
                    new_url = url.format(business_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增项目':
                    new_data, business_id = add_projects_data(data)
                    new_url = url.format(business_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增标签组':
                    new_data = add_taggroup_data(data)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '项目关联标签组规则':
                    new_data = rel_product_taggroup(data)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增模型目录':
                    new_data=add_model_category(data)
                    new_data=json.dumps(new_data, separators=(',', ':'))
                    response=requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增子模型目录':
                    new_data,model_category_id = add_child_model_category(data)
                    new_url = url.format(model_category_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增标准目录':
                    new_data=add_standard_category(data)
                    new_data=json.dumps(new_data, separators=(',', ':'))
                    response=requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增子标准目录':
                    new_data,standard_category_id = add_child_standard_category(data)
                    new_url = url.format(standard_category_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增资源配置物理表':
                    new_data=rel_physical_dataset(data)
                    new_data=json.dumps(new_data, separators=(',', ':'))
                    response=requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增逻辑模型transaction':
                    new_data,project_id,subject_id = add_model_metadata(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '保存逻辑模型新版本':
                    new_data,metadata_id,subject_id = save_model_metadata_info(data)
                    new_url = url.format(metadata_id,subject_id)
                    log.info("request   url：%s " % new_url)
                    log.info("request   data：%s " %new_data)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增时间维度':
                    new_data,project_id,subject_id = add_primary(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增主键':
                    new_data,project_id,subject_id = add_primary(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增属性':
                    new_data,project_id,subject_id = add_primary(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增度量':
                    new_data,project_id,subject_id = add_primary(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增指标':
                    new_data,project_id,subject_id = add_indicator(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增维度':
                    new_data,project_id,subject_id = add_dimension(data)
                    new_url = url.format(project_id,subject_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增物化配置':
                    new_data,metadata_id = add_physical(data)
                    new_url = url.format(metadata_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif '新增逻辑模型字段' in case_detail:
                    new_data,metadata_id = add_metadata_field(data)
                    new_url = url.format(metadata_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '新增数据建模逻辑表':
                    new_data,project_id,category_id = new_data_model(data)
                    new_url = url.format(project_id,category_id)
                    log.info("request   url：%s " % new_url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=new_url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '提交数据建模逻辑模型':
                    new_data = publish_data_model(data)
                    log.info("request   url：%s " %url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '提交数据建模逻辑模型':
                    new_data = publish_data_model(data)
                    log.info("request   url：%s " %url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '资产目录重命名':
                    new_data = update_asset_directory(data)
                    log.info("request   url：%s " %url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif case_detail == '资产目录重命名-名称重复':
                    new_data = duplicate_move_asset_directory(data)
                    log.info("request   url：%s " %url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif '更新资产' in case_detail:
                    asset_id, new_data = update_asset(data)
                    url = url.format(asset_id)
                    log.info("request   url：%s " % url)
                    new_data = json.dumps(new_data, separators=(',', ':'))
                    response = requests.put(url=url, headers=headers, data=new_data)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif '&&' in str(data):
                    '''分隔参数'''
                    parameters = data.split('&&')
                    '''拼接URL'''
                    new_url = url.format(parameters[0])
                    log.info("new_url：%s" % new_url)
                    '''发送的参数体'''
                    parameters_data = parameters[1]
                    if parameters_data.startswith('{'):
                        response = requests.put(url=new_url, headers=headers, json=dict_res(parameters_data))
                        log.info("response data：%s %s" % (response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(table_sheet_name, row, column, response.status_code)
                        write_result(table_sheet_name, row, column + 4, response.text)
                    else:
                        log.info('请确认第%d行parameters中需要update的值格式，应为id&{data}' % row)
                else:
                    if "{}" in url:
                        new_url = url.format(data["id"])
                        log.info("new_url：%s" % new_url)
                        response = requests.put(url=new_url, headers=headers, data=json.dumps(data))
                        log.info("response data：%s %s" % (response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(table_sheet_name, row, column, response.status_code)
                        write_result(table_sheet_name, row, column + 4, response.text)
                    else:
                        response = requests.put(url=url, headers=headers, json=dict_res(data))
                        log.info("response data：%s %s" % (response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(table_sheet_name, row, column, response.status_code)
                        write_result(table_sheet_name, row, column + 4, response.text)
    except Exception as e:
            log.error("{}执行过程中出错{}".format(case_detail, e))
        
def delete_request_result_check(url, data, table_sheet_name, row, column, headers):
    """
    delete接口请求
    :param url:
    :param data:
    :param table_sheet_name:
    :param row:
    :param column:
    :param headers:
    :return:
    """
    try:
        case_detail = case_table_sheet.cell(row=row, column=2).value
        log.info("开始执行：%s " %case_detail)
        if data:
            if "删除逻辑模型字段" in case_detail:
                metadata_id,name = metadata_field(data)
                new_url = url.format(metadata_id,name)
                log.info("request   url：%s " % new_url)
                response = requests.delete(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)# 字典形式作为参数，如{"id":"7135cf6e-2b12-4282-90c4-bed9e2097d57","name":"gbj_for_jdbcDatasource_create_0301_1_0688","creator":"admin"}
            elif "删除资产目录" == case_detail:
                asset_id, new_data = delete_asset_directory(data)
                log.info("request   data：%s " % new_data)
                new_url = url.format(asset_id)
                log.info("request   url：%s " % new_url)
                response = requests.delete(url=new_url, headers=headers, data=new_data)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif isinstance(data, str):
                log.info("data：%s" % data)
                if "{}" in url:
                    new_url = url.format(data)
                    log.info("new_url：%s" % new_url)
                    response = requests.delete(url=new_url, headers=headers)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:
                    response = requests.delete(url=url, headers=headers, data=json.dumps(data))
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(table_sheet_name, row, column, response.status_code)
                    write_result(table_sheet_name, row, column + 4, response.text)
            elif isinstance(data, list):
                response = requests.delete(url=url, headers=headers, data=json.dumps(data))
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            else:
                log.error("{}执行过程中出错".format(case_detail))
    except Exception as e:
        log.error("{}执行过程中出错{}".format(case_detail, e))



def write_result(sheet, row, column, value):
    """
    写入返回结果
    :param sheet:
    :param row:
    :param column:
    :param value:
    :return:
    """
    sheet.cell(row=row, column=column, value=value)



def clean_vaule(sheet, row, column):
    """
    写入结果前，先把结果和对比结果全部清空
    :param sheet:
    :param row:
    :param column:
    :return:
    """
    sheet.cell(row=row, column=column, value='')
    sheet.cell(row=row, column=column+1, value='')
    sheet.cell(row=row, column=column + 4, value='')
    sheet.cell(row=row, column=column + 5, value='')
    sheet.cell(row=row, column=column + 6, value='')
    sheet.cell(row=row, column=column + 7, value='')



def read_data():
        data = xlrd.open_workbook(cases_dir)
        table = data.sheet_by_name(dw_master)
        """获取总行数"""
        nrows = table.nrows
        if nrows > 1:
            """获取第一行的内容，列表格式"""
            keys = table.row_values(0)
            list_api_data = []
            """获取每一行的内容，列表格式"""
            for col in range(1, nrows):
                values = table.row_values(col)
                """ keys，values组合转换为字典"""
                api_dict = dict(zip(keys, values))
                if api_dict['is_run']=="y" or api_dict['is_run']=="Y":
                    list_api_data.append(api_dict)
            return list_api_data
        else:
            log.info("表格是空数据!")
            return None

testdata = read_data()


@myddt.ddt
class CheckResult(unittest.TestCase):

    def compare_code_result(self):
        """1.对比预期code和接口响应返回的status code"""
        for row in range(2, all_rows+1):
            is_run = case_table_sheet.cell(row=row, column=16).value
            """预期status code和接口返回status code"""
            ex_status_code = case_table_sheet.cell(row=row, column=7).value
            ac_status_code = case_table_sheet.cell(row=row, column=8).value
            """判断两个status code是否相等"""
            if is_run == 'Y' or is_run == 'y':
                if ex_status_code and ac_status_code != '':
                    if ex_status_code == ac_status_code:
                        case_table_sheet.cell(row=row, column=9, value='pass')
                    else:
                        case_table_sheet.cell(row=row, column=9, value='fail') # code不等时，用例结果直接判断为失败
                        case_table_sheet.cell(row=row, column=15, value='%s--->失败原因：返回status_code对比失败,预期为%s,实际为%s' %
                                                                        (case_table_sheet.cell(row=row, column=2).value, case_table_sheet.cell(row=row, column=7).value, case_table_sheet.cell(row=row, column=8).value))
                else:
                    log.info("第 %d 行 status_code为空" %row)
            else:
                log.info("第 %d 行脚本未执行，请查看isRun是否为Y或者y！"%row)
        case_table.save(cases_dir)


    def compare_text_result(self):
        """对比预期response和实际返回的response.text，根据预期和实际结果的关系进行处理"""
        for row in range(2, all_rows+1):
            """接口返回的response.text"""
            response_text = case_table_sheet.cell(row=row, column=12).value
            response_text_dict = dict_res(response_text)
            """预期结果"""
            expect_text = case_table_sheet.cell(row=row, column=10).value
            """接口关键字"""
            key_word = case_table_sheet.cell(row=row, column=3).value
            """status_code对比结果"""
            code_result = case_table_sheet.cell(row=row, column=9).value
            """预期text和response.text的关系"""
            relation = case_table_sheet.cell(row=row, column=11).value
            is_run = case_table_sheet.cell(row=row, column=16).value
            if is_run == 'Y' or is_run == 'y':
                """
                1.status_code 对比结果pass的前提下，判断response.text断言是否正确,
                2.status_code 对比结果fail时，用例整体结果设为fail
                """
                if code_result == 'pass':
                    if key_word in ('create', 'query', 'update', 'delete'):
                        self.assert_deal(key_word, relation, expect_text, response_text, response_text_dict, row, 13)
                    else:
                        log.info("请确认第%d行的key_word" % row)
                elif code_result == 'fail':
                    case_table_sheet.cell(row=row, column=14, value='fail')
                    case_table_sheet.cell(row=row, column=15, value='%s--->失败原因：返回status_code对比失败,预期为%s,实际为%s' %
                                                                    (case_table_sheet.cell(row=row, column=2).value, case_table_sheet.cell(row=row, column=7).value, case_table_sheet.cell(row=row, column=8).value))
                else:
                    log.info("请确认第 %d 行 status_code对比结果" % row)
            else:
                log.info("第 %d 行脚本未执行，请查看isRun是否为Y或者y！"%row)

        case_table.save(cases_dir)


    def assert_deal(self, key_word, relation, expect_text, response_text, response_text_dict, row, column):
        """
        :param key_word:
        :param relation:
        :param expect_text:
        :param response_text:
        :param response_text_dict:
        :param row:
        :param column:
        :return: 根据expect_text, response_text的关系，进行断言, 目前只处理了等于和包含两种关系
        """
        if key_word == 'create':
            if relation == '=':
                if isinstance(response_text_dict, dict):
                    if response_text_dict.get("id"):
                        try:
                            self.assertEqual(expect_text, len(response_text_dict['id']), '第%d行的response_text长度和预期不一致' % row)
                        except:
                            log.info("第 %d 行 response_text返回的id和预期id长度不一致" %row)
                            case_table_sheet.cell(row=row, column=column, value='fail')
                        else:
                            case_table_sheet.cell(row=row, column=column, value='pass')
                    else:
                        try:
                            self.assertEqual(expect_text, response_text, '第%d行的expect_text:%s和response_text:%s不相等' % (row,expect_text, response_text))
                        except:
                            log.info("第%d行的expect_text:%s和response_text:%s不相等" %(row,expect_text, response_text))
                            case_table_sheet.cell(row=row, column=column, value='fail')
                        else:
                            case_table_sheet.cell(row=row, column=column, value='pass')
                else:
                    '''只返回一个id串的情况下，判断预期长度和id长度一致'''
                    try:
                        self.assertEqual(expect_text, len(response_text), '第%d行的response_text长度和预期不一致' % row)
                    except:
                        log.info("第 %d 行 response_text和预期text不相等" %row)
                        case_table_sheet.cell(row=row, column=column, value='fail')
                    else:
                        case_table_sheet.cell(row=row, column=column, value='pass')

            elif relation == 'in':
                """返回多内容时，断言多个值可以用&连接，并且expect_text包含在response_text中"""
                if "&&" in expect_text:
                    for i in expect_text.split("&&"):
                        try:
                            self.assertIn(i, response_text, '第 %d 行 预期结果：%s没有包含在response_text中' %(row,i))
                        except:
                            log.info("第 %d 行 预期结果：%s没有包含在response_text中， 结果对比失败" %(row,i))
                            case_table_sheet.cell(row=row, column=column, value='fail')
                            break
                        else:
                            case_table_sheet.cell(row=row, column=column, value='pass')
                else:
                    try:
                        self.assertIn(expect_text, response_text, '第 %d 行 预期结果：%s没有包含在response_text中'%(row,expect_text))
                    except:
                        log.info("第 %d 行 预期结果：%s没有包含在response_text中， 结果对比失败" %(row,expect_text))
                        case_table_sheet.cell(row=row, column=column, value='fail')
                    else:
                        case_table_sheet.cell(row=row, column=column, value='pass')
            else:
                log.info("请确认第 %d 行 预期expect_text和response_text的relatrion" %row)
                case_table_sheet.cell(row=row, column=column, value='请确认%d行 的预期text和接口response.text的relatrion'%row)
        elif key_word in ('query', 'update', 'delete'):
            if relation == '=':
                compare_result = re.findall('[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}', '%s' % (response_text))
                response_text_list = [response_text]
                if compare_result == response_text_list:
                    try:
                        self.assertEqual(expect_text, len(response_text), '第%d行expect_text和response_text不相等' % row)
                    except:
                        case_table_sheet.cell(row=row, column=column, value='fail')
                    else:
                        case_table_sheet.cell(row=row, column=column, value='pass')
                elif expect_text == None and response_text == "":
                    case_table_sheet.cell(row=row, column=column, value='pass')
                else:
                    try:
                        self.assertEqual(expect_text, response_text, '第%d行expect_text:%s和response_text:%s不相等' % (row,expect_text,response_text))
                    except:
                        log.info("第 %d 行 response_text和预期text不相等" %row)
                        case_table_sheet.cell(row=row, column=column, value='fail')
                    else:
                        case_table_sheet.cell(row=row, column=column, value='pass')

            elif relation == 'in':
                if "&&" in expect_text:
                    for i in expect_text.split("&&"):
                        try:
                            self.assertIn(i, response_text, '第 %d 行 预期结果：%s没有包含在response_text中' %(row,i))
                        except:
                            log.info("第 %d 行 预期结果：%s没有包含在response_text中， 结果对比失败" %(row,i))
                            case_table_sheet.cell(row=row, column=column, value='fail')
                            break
                        else:
                            case_table_sheet.cell(row=row, column=column, value='pass')
                else:
                    try:
                        self.assertIn(expect_text, response_text, '第 %d 行 预期结果：%s没有包含在response_text中'%(row,expect_text))
                    except:
                        log.info("第 %d 行 预期结果：%s没有包含在response_text中， 结果对比失败" %(row,expect_text))
                        case_table_sheet.cell(row=row, column=column, value='fail')
                    else:
                        case_table_sheet.cell(row=row, column=column, value='pass')
            else:
                log.info("请确认第 %d 行 预期expect_text和response_text的relation" % row)
                case_table_sheet.cell(row=row, column=column, value='请确认第 %d 行 预期expect_text和response_text的relation'%row)
        else:
            log.info("请确认第 %d 行 的key_word" % row)
        case_table.save(cases_dir)


    def deal_result(self):
        """
        对比code
        self.compare_code_result()
        对比text
        self.compare_text_result()
        根据code result和text result判断case最终结果
        :return: 对比case最终的结果
        """
        self.compare_code_result()
        self.compare_text_result()
        for row in range(2, all_rows + 1):
            is_run = case_table_sheet.cell(row=row, column=16).value
            status_code_result = case_table_sheet.cell(row=row, column=9).value
            response_text_result = case_table_sheet.cell(row=row, column=13).value
            if is_run=='Y' or is_run=='y':
                if status_code_result == 'pass' and response_text_result == 'pass':
                    log.info("测试用例-%s pass" % case_table_sheet.cell(row=row, column=2).value)
                    case_table_sheet.cell(row=row, column=14, value='pass')
                    case_table_sheet.cell(row=row, column=15, value='')
                elif status_code_result == 'fail':
                    log.info("测试用例-%s fail" % case_table_sheet.cell(row=row, column=2).value)
                    case_table_sheet.cell(row=row, column=14, value='fail')
                    case_table_sheet.cell(row=row, column=15, value='')
                    case_table_sheet.cell(row=row, column=15, value='%s--->失败原因：status code对比失败,预期为%s,实际为%s' \
                                                                    % (case_table_sheet.cell(row=row, column=2).value, case_table_sheet.cell(row=row, column=7).value, case_table_sheet.cell(row=row, column=8).value))
                elif status_code_result == 'pass' and response_text_result == 'fail':
                    log.info("测试用例-%s fail" % case_table_sheet.cell(row=row, column=2).value)
                    case_table_sheet.cell(row=row, column=14, value='fail')
                    case_table_sheet.cell(row=row, column=15, value='')
                    case_table_sheet.cell(row=row, column=15, value='%s--->失败原因：返回内容对比失败,预期为%s,实际为%s' %
                                                                    (case_table_sheet.cell(row=row, column=2).value, case_table_sheet.cell(row=row, column=10).value, case_table_sheet.cell(row=row, column=12).value))
                else:
                    log.info("请确认status code或response.text对比结果")
            else:
                log.info("第 %d 行脚本未执行，请查看isRun是否为Y或者y！"%row)
        case_table.save(cases_dir)


    @myddt.data(*testdata)
    def test_api(self,data):
        self.case_name = data['case_detail']
        self.url=host+data['url']
        self.method=data['method']
        self.case_result = data['case_result']
        self.result2 = data['result2']
        self.header=get_headers()
        self.body=data['parameters']
        self.expect_text = data['expect_text']
        self.extract_data=data['response_text']
        self.readData_code =data["response__status_code"]
        self.fail_detail = data['fail_detail']
        print("******* 执行用例 ->{0} *********".format(self.case_name))
        print("请求URL: {0}".format(self.url))
        print("请求方式: {0}".format(self.method))
        print("请求header:{0}".format(self.header))
        print("请求body:{0}".format(self.body))
        if self.case_result == 'pass':
            print("返回状态码：%d 响应信息：%s" % (self.readData_code,self.extract_data))
            self.assertIn(self.expect_text,self.extract_data,"返回实际结果是->:%s" % self.extract_data)
        else:
             self.assertEqual(self.readData_code, 200,"返回状态码status_code:{} 失败详情fail_detail:{}".format(str(self.readData_code),self.fail_detail))
