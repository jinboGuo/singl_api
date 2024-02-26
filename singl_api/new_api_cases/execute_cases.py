# coding:utf-8
import json
import os
import re
import time
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from httpop.Httpop import Httpop
from new_api_cases.dw_prepare_datas import sql_analyse_data, woven_dir, get_improt_data
from openpyxl import load_workbook
import requests
from basic_info.get_auth_token import get_headers, get_headers_root,get_auth_token
from util.comm_util import operateKafka
#from util.elasticsearch7 import get_es_data, get_es_data_for_thumbnailMode
from util.encrypt import encrypt_rf
from util.format_res import dict_res, get_time
from basic_info.setting import MySQL_CONFIG, baymax_master
from util.Open_DB import MYSQL
from basic_info.ready_dataflow_data import get_dataflow_data, get_executions_data, query_dataflow_data, \
    get_schedulers_data
from basic_info.setting import host
from new_api_cases.deal_parameters import deal_parameters
import unittest
from new_api_cases.get_statementId import statementId, statementId_no_dataset, get_sql_analyse_statement_id, \
    get_sql_analyse_dataset_info, get_sql_execte_statement_id, steps_sql_parseinit_statemenId, \
    steps_sql_analyzeinit_statementId, get_step_output_init_statementId, get_step_output_ensure_statementId, \
    step_sql_analyse_data, step_sql_analyse_flow
from new_api_cases.prepare_datas_for_cases import get_job_tasks_id, collector_schema_sync, get_applicationId, \
    get_woven_qaoutput_dataset_path, \
    dss_data, upddss_data, dataset_data, upddataset_data, create_schema_data, updschema_data, create_flow_data, \
    update_flow_data, filesets_data, get_old_id_name, get_collector_data, tag_data, set_user_role, update_role, \
    update_user, enable_user, enable_role, query_flow_data, update_db_driver, update_custom_step, get_improt_dataflow, \
    stand_data, create_standard_data, query_dataset, query_schema
from util.logs import Logger

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"], MySQL_CONFIG["PORT"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))
case_table = load_workbook(ab_dir("api_cases.xlsx"))
case_table_sheet = case_table.get_sheet_by_name(baymax_master)
all_rows = case_table_sheet.max_row
fileset_dir = os.path.join(os.path.abspath('.'),'attachment\Capture001.png')
jar_dir = os.path.join(os.path.abspath('.'),'attachment\woven-common-1.4.0.jar')
jar_driver = os.path.join(os.path.abspath('.'),'attachment\mysql-connector-java-8.0.2801.jar')
jar_custom = os.path.join(os.path.abspath('.'),'attachment\merce-custom-rtc-steps-1.2.4-Filter.jar')
woven_dataflow = os.path.join(os.path.abspath('.'),'attachment\import_dataflow_steps.woven')
multi_sink_steps = os.path.join(os.path.abspath('.'),'attachment\mutil_sink_storage.woven')
multi_rtc_steps = os.path.join(os.path.abspath('.'),'attachment\multi_rtc_steps.woven')
log = Logger().get_log()
minio_data = []
httpop = Httpop()
host = host


def deal_request_method():
    """
    判断请求方法，并根据不同的请求方法调用不同的处理方式
    :return:
    """
    for i in range(2, all_rows+1):
        request_method = case_table_sheet.cell(row=i, column=4).value
        old_request_url = host+case_table_sheet.cell(row=i, column=5).value
        request_url = deal_parameters(old_request_url)
        old_data = case_table_sheet.cell(row=i, column=6).value
        request_data = deal_parameters(old_data)
        log.info("request  data：%s" % request_data)
        api_name = case_table_sheet.cell(row=i, column=1).value
        """请求方法转大写"""
        if request_method:
            request_method_upper = request_method.upper()
            if api_name == 'tenants':
                """
                租户的用例需要使用root用户登录后操作
                根据不同的请求方法，进行分发
                """
                if request_method_upper == 'POST':
                    post_request_result_check(row=i, host=host, column=8, url=request_url, headers=get_headers_root(host), data=request_data, table_sheet_name=case_table_sheet)
                elif request_method_upper == 'GET':
                    get_request_result_check(url=request_url, host=host, headers=get_headers_root(host), data=request_data, table_sheet_name=case_table_sheet, row=i, column=8)
                elif request_method_upper == 'PUT':
                    put_request_result_check(url=request_url, row=i, data=request_data, table_sheet_name=case_table_sheet, column=8, headers=get_headers_root(host))
                elif request_method_upper == 'DELETE':
                    delete_request_result_check(url=request_url, data=request_data, table_sheet_name=case_table_sheet, row=i, column=8, headers=get_headers_root(host))
                else:
                    log.info("请求方法%s不在处理范围内" % request_method)
            else:
                """根据不同的请求方法，进行分发"""
                if request_method_upper == 'POST':
                    post_request_result_check(row=i, host=host, column=8, url=request_url, headers=get_headers(host), data=request_data, table_sheet_name=case_table_sheet)
                elif request_method_upper == 'GET':
                    get_request_result_check(url=request_url, host=host, headers=get_headers(host), data=request_data, table_sheet_name=case_table_sheet, row=i, column=8)
                elif request_method_upper == 'PUT':
                    put_request_result_check(url=request_url, row=i, data=request_data, table_sheet_name=case_table_sheet, column=8, headers=get_headers(host))
                elif request_method_upper == 'DELETE':
                    delete_request_result_check(url=request_url, data=request_data, table_sheet_name=case_table_sheet, row=i, column=8, headers=get_headers(host))
                else:
                    log.info("请求方法%s不在处理范围内" % request_method)
        else:
            log.info("第 %d 行请求方法为空" % i)
    '''执行结束后保存表格'''
    case_table.save(ab_dir("api_cases.xlsx"))



def post_request_result_check(row, column, url, host, headers, data, table_sheet_name):
    """
    POST接口请求，脚本里post请求的处理
    :param row:
    :param column:
    :param url:
    :param host:
    :param headers:
    :param data:
    :param table_sheet_name:
    :return:
    """
    global case_detail
    try:
        case_detail = case_table_sheet.cell(row=row, column=2).value
        log.info("开始执行：%s" % case_detail)
        log.info("请求url：%s" % url)
        if '(Id不存在)' in case_detail:
            '''先获取statementId,然后格式化URL，再发送请求'''
            statement = statementId_no_dataset(host, dict_res(data))
            new_url = url.format(statement)
            data = data.encode('utf-8')
            response = requests.post(url=new_url, headers=headers, data=data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            '''将返回的status_code和response.text分别写入第10列和第14列'''
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif "上传woven文件" == case_detail:
            fs = {"file": open(woven_dir, 'rb')}
            headers.pop('Content-Type')
            response = requests.post(url=url, headers=headers, files=fs)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        elif "导入woven文件" == case_detail:
            new_data = get_improt_data(headers, host)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=get_headers(host), data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        elif '创建数据源' in case_detail:
            new_data = dss_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response=requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '查询数据源' in case_detail:
            new_data = dss_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '创建schema' in case_detail:
            new_data = create_schema_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '查询schema' in case_detail:
            new_data = query_schema(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '创建数据标准' in case_detail:
            new_data = create_standard_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '查询数据标准' in case_detail:
            new_data = stand_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif "上传dataflow-woven文件" == case_detail:
            fs = {"file": open(woven_dataflow, 'rb')}
            headers.pop('Content-Type')
            response = requests.post(url=url, headers=headers, files=fs)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        elif "导入dataflow-woven文件" == case_detail:
            new_data = get_improt_dataflow(headers, host, data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=get_headers(host), data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        elif "上传multi-sink_steps文件" == case_detail:
            fs = {"file": open(multi_sink_steps, 'rb')}
            headers.pop('Content-Type')
            response = requests.post(url=url, headers=headers, files=fs)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        elif "导入multi-sink_steps文件" == case_detail:
            new_data = get_improt_dataflow(headers, host, data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=get_headers(host), data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        elif "上传multi-rtc_steps文件" == case_detail:
            fs = {"file": open(multi_rtc_steps, 'rb')}
            headers.pop('Content-Type')
            response = requests.post(url=url, headers=headers, files=fs)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        elif "导入multi-rtc_steps文件" == case_detail:
            new_data = get_improt_dataflow(headers, host, data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=get_headers(host), data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        elif '创建flow' in case_detail:
            new_data = create_flow_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '查询flow' in case_detail:
            new_data = query_flow_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '测试JDBC数据库连接':            
            dss_id, new_data = upddss_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            print("new: ",new_data)
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(table_sheet_name, row, column, response.status_code)
            write_result(table_sheet_name, row, column+4, response.text)
        elif 'datasetId不存在'in case_detail:
            new_data = dataset_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.get(url=url, headers=headers)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '新Dataset预览接口,得到statement  id(datasetId存在)':
            new_data = dataset_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据statement id,获取预览Dataset的结果数据(datasetId存在)':
            statement_id, new_data = statementId(host, data)
            new_url = url.format(statement_id)
            res = requests.post(url=new_url, headers=headers,data=new_data)
            count_num = 0
            while ("waiting") in res.text or ("running") in res.text:
                log.info("再次查询前：%s %s" % (res.status_code, res.text))
                res = requests.post(url=new_url, headers=headers,data=new_data)
                time.sleep(5)
                count_num += 1
                if count_num == 50:
                    return
            if '"statement":"available"' in res.text:
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=res.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=res.text)
            else:
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=res.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=res.text)
        elif case_detail == '获取SQL执行任务结果':
            execte_use_params = get_sql_analyse_dataset_info(host, data)
            execte_use_params = json.dumps(execte_use_params, separators=(',', ':'))
            log.info("request   data：%s " % execte_use_params)
            execte_statement_id = get_sql_execte_statement_id(host, data)
            new_url = url.format(execte_statement_id)
            log.info("request   url：%s " % new_url)
            response = requests.post(url=new_url, headers=headers, data=execte_use_params)
            count_num = 0
            while ("waiting") in response.text or ("running") in response.text:
                log.info("再次查询前：%s %s" % (response.status_code, response.text))
                response = requests.post(url=new_url, headers=headers, data=execte_use_params)
                time.sleep(5)
                count_num += 1
                if count_num == 50:
                    return
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '创建dataset'in case_detail:
            new_data = dataset_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '查询数据集' in case_detail:
            new_data = query_dataset(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '获取step的输出字段分析任务的statementID':
            new_data = step_sql_analyse_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '获取初始化确认step任务的statementID':
            new_data = step_sql_analyse_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '获取Sql语句解析表名,初始化ParseSql任务的statementID' == case_detail:
            new_data = step_sql_analyse_flow(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '获取初始化AnalyzeSq,返回statementId' == case_detail:
            new_data = step_sql_analyse_flow(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '更新用户':
            log.info("request   url：%s" % url)
            new_data = update_user(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '更新角色':
            log.info("request   url：%s" % url)
            new_data = update_role(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '设置用户角色':
            new_datas = set_user_role(data)
            new_url = url.format(new_datas[0])
            log.info("new_url：%s" % new_url)
            new_data = json.dumps(new_datas[1], separators=(',', ':'))
            response = requests.post(url=new_url, data=new_data, headers=headers)
            log.info("response data：%s %s"%(response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '设置用户有效期':
            new_datas = set_user_role(data)
            new_url = url.format(new_datas[0])
            log.info("new_url：%s" % new_url)
            new_data = json.dumps(new_datas[2], separators=(',', ':'))
            response = requests.post(url=new_url, data=new_data, headers=headers)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '提交上传woven-dataflow':
            new_data = get_dataflow_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            time.sleep(100)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查找executions-woven-dataflow':
            new_data = query_dataflow_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            count_num = 0
            time.sleep(5)
            while "waiting" in response.text or "READY" in response.text or "RUNNING" in response.text:
                log.info("再次查询前：%s %s" % (response.status_code, response.text))
                response = requests.post(url=url, headers=headers, data=new_data)
                time.sleep(5)
                count_num += 1
                if count_num == 50:
                    return
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '提交上传woven-rtcflow':
            new_data = get_dataflow_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '查找executions-woven-rtcflow':
            new_data = query_dataflow_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            count_num = 0
            time.sleep(5)
            while "waiting" in response.text or "READY" in response.text:
                log.info("再次查询前：%s %s" % (response.status_code, response.text))
                response = requests.post(url=url, headers=headers, data=new_data)
                time.sleep(5)
                count_num += 1
                if count_num == 50:
                    return
            else:
                #log.info("开始往kafka插入数据...")
                operateKafka().send_str_kafka()
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '查询指定流程计划' in case_detail:
            new_data = get_schedulers_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '查询流程执行历史' in case_detail:
            new_data = get_executions_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '根据execution id查询输出结果' == case_detail:
            new_data = get_executions_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '批量删除execution':
            data=json.dumps(data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '停止一个采集器任务的执行':
            task_id = get_job_tasks_id(data)
            response = requests.post(url=url, headers=headers, json=task_id)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '删除schema'in case_detail:
            new_data = json.dumps(data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '初始化Sql'in case_detail:
            new_data = sql_analyse_data(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '指定目录下创建子目录':
            response = requests.post(url=url, headers=headers, json=dict_res(data))
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '项目目录指定目录创建dataset':
            response = requests.post(url=url, headers=headers, data=data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '数据源状态监控分析图数据':
            data = {"fieldList":[{"fieldName":"createTime","fieldValue":get_time(),"comparatorOperator":"GREATER_THAN","logicalOperator":"AND"},{"fieldName":"createTime","fieldValue":1555516800000,"comparatorOperator":"LESS_THAN"}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8,"groupBy":"testTime"}
            response = requests.post(url=url,headers=headers, json=data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '上传驱动文件':
            files = {"file": open(jar_driver, 'rb')}
            headers.pop('Content-Type')
            response = requests.post(url=url, files=files, headers=headers)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '上传选择器文件':
            files = {"file": open(jar_dir, 'rb')}
            headers.pop('Content-Type')
            response = requests.post(url=url, files=files, headers=headers)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '上传自定义节点文件':
            files = {"file": open(jar_custom, 'rb')}
            headers.pop('Content-Type')
            response = requests.post(url=url, files=files, headers=headers)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据statement id,获取预览Dataset的结果数据(datasetId存在)':
            statementId1, new_data = statementId(host, data)
            new_url = url.format(statementId1)
            response = requests.post(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            count_num = 0
            while "running" in response.text or "waiting" in response.text:
                time.sleep(5)
                response = requests.post(url=new_url, headers=headers, data=new_data)
                count_num += 1
                if count_num == 100:
                    return
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '数据标准导入文件':
            dir2 = ab_dir('sex.xls')
            files = {"file": open(dir2, 'rb')}
            headers = get_headers(host)
            headers.pop('Content-Type')
            response = requests.post(url, files=files, headers=headers)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == "获取令牌":
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            headers.pop('X-AUTH-TOKEN')
            response = requests.post(url, headers=headers, data=dict_res(data))
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '登录' in case_detail :
            new_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            if case_detail == '登录':
                data = {'name': encrypt_rf('admin'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
                response = requests.post(url=url, headers=new_headers, data=data)
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '密码错误的账户登录':
                data = {'name': encrypt_rf('admin'), 'password': encrypt_rf('123456555'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
                response = requests.post(url=url, headers=new_headers, data=data)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '不存在的账户登录':
                data = {'name': encrypt_rf('admin12399999999999999'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
                response = requests.post(url=url, headers=new_headers, data=data)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '没有权限的账户登录':
                data = {'name': encrypt_rf('user_without_pression'), 'password': encrypt_rf('123456'),
                        'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
                response = requests.post(url=url, headers=new_headers, data=data)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '有权限，密码过期的账户登录':
                data = {'name': encrypt_rf('user_pwd_expired'), 'password': encrypt_rf('123456'),
                        'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
                response = requests.post(url=url, headers=new_headers, data=data)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '有权限，用户有效期过期的账户登录':
                data = {'name': encrypt_rf('user_time_expired'), 'password': encrypt_rf('123456'),
                        'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
                response = requests.post(url=url, headers=new_headers, data=data)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '有权限，密码和用户有效期均过期的账户登录':
                data = {'name': encrypt_rf('user_expired'), 'password': encrypt_rf('123456'),
                        'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
                response = requests.post(url=url, headers=new_headers, data=data)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '创建非结构化文件集合' in case_detail:
            new_data = filesets_data(data)
            response=requests.post(url=url, headers=headers, json=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '提交flow' in case_detail:
            new_data=get_old_id_name(data)
            response=requests.post(url=url, headers=headers, json=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '启用数据导入任务' in case_detail:
            new_data = json.dumps(data)
            response = requests.post(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '编辑停用状态的数据导入任务' in case_detail:
            new_url = url.format(data)
            data = get_collector_data(data)
            response = requests.post(url=new_url, headers=headers, data=data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(table_sheet_name, row, column, response.status_code)
            write_result(table_sheet_name, row, column+4, response.text)
        elif ('根据ID查询数据同步任务信息' or '查看指定job id的task') in case_detail:
            new_url = url.format(data)
            data = {"fieldList":[],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            response = requests.post(url=new_url, headers=headers, json=data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(table_sheet_name, row, column, response.status_code)
            write_result(table_sheet_name, row, column+4, response.text)
        # elif ('分页查询文件内容') in case_detail:
        #     para = data.split('&')
        #     es_id = get_es_data(para[0],para[1],para[2],eval(para[3]))
        #     content = para[4]
        #     new_data={"content":content,"offset":0,"limit":8,"ids":es_id}
        #     response = requests.post(url=url, headers=headers, json=new_data)
        #     log.info("response data：%s %s" % (response.status_code, response.text))
        #     clean_vaule(table_sheet_name, row, column)
        #     write_result(table_sheet_name, row, column, response.status_code)
        #     write_result(table_sheet_name, row, column+4, response.text)
        # elif "下载ES索引文件" in case_detail:
        #     para = data.split("&")
        #     es_id = get_es_data(para[0],para[1],para[2],eval(para[3]))
        #     new_data = json.dumps(es_id)
        #     response = requests.post(url=url, headers=headers, data=new_data)
        #     log.info("response data：%s %s" % (response.status_code, ILLEGAL_CHARACTERS_RE.sub(r'', response.text)))
        #     clean_vaule(table_sheet_name, row, column)
        #     write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        #     write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        # elif ("新建标签" or "更新标签") in case_detail:
        #     para = data.split("&")
        #     es_id = get_es_data(para[0],para[1],para[2],eval(para[3]))
        #     data = {"ids":es_id,"tags":eval(para[4])}
        #     response = requests.post(url=url, headers=headers, json=data)
        #     log.info("response data：%s %s" % (response.status_code, ILLEGAL_CHARACTERS_RE.sub(r'', response.text)))
        #     clean_vaule(table_sheet_name, row, column)
        #     write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        #     write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        # elif "清空标签" in case_detail:
        #     para=data.split("&")
        #     es_id=get_es_data(para[0],para[1],para[2],eval(para[3]))
        #     data={"ids":es_id}
        #     response = requests.post(url=url, headers=headers, json=data)
        #     log.info("response data：%s %s" % (response.status_code, ILLEGAL_CHARACTERS_RE.sub(r'', response.text)))
        #     clean_vaule(table_sheet_name, row, column)
        #     write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        #     write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        # elif case_detail == "下载文件":
        #     para=data.split("&")
        #     es_id=get_es_data(para[0],para[1],para[2],eval(para[3]))
        #     new_data=json.dumps(es_id)
        #     response = requests.post(url=url, headers=headers, data=new_data)
        #     log.info("response data：%s %s" % (response.status_code, ILLEGAL_CHARACTERS_RE.sub(r'', response.text)))
        #     clean_vaule(table_sheet_name, row, column)
        #     write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        #     write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        # elif "预览缩略图" in case_detail:
        #     para=data.split("&")
        #     es_id=get_es_data(para[0],para[1],para[2],eval(para[3]))
        #     new_url=url.format(es_id[0])
        #     response = requests.post(url=new_url, headers=headers, json=data)
        #     log.info("response data：%s %s" % (response.status_code, ILLEGAL_CHARACTERS_RE.sub(r'', response.text)))
        #     clean_vaule(table_sheet_name, row, column)
        #     write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        #     write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        # elif "编辑图片" in case_detail:
        #     para=data.split("&")
        #     es_id=get_es_data(para[0],para[1],para[2],eval(para[3]))
        #     new_url=url.format(es_id[0])
        #     fs = {"file": open(fileset_dir, 'rb')}
        #     headers.pop('Content-Type')
        #     headers["Accept"]='*/*'
        #     response = requests.post(url=new_url, headers=headers, files=fs)
        #     log.info("response data：%s %s" % (response.status_code, ILLEGAL_CHARACTERS_RE.sub(r'', response.text)))
        #     clean_vaule(table_sheet_name, row, column)
        #     write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        #     write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        # elif ("缩略图和列表") in case_detail:
        #     para=data.split("&")
        #     es_ids=get_es_data_for_thumbnailMode(para[0],para[1],para[2])
        #     es_ids=json.dumps(es_ids)
        #     response = requests.post(url=url, headers=headers, data=es_ids)
        #     log.info("response data：%s %s" % (response.status_code, ILLEGAL_CHARACTERS_RE.sub(r'', response.text)))
        #     clean_vaule(table_sheet_name, row, column)
        #     write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        #     write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        elif ("服务器创建新目录") in case_detail:
            new_url=url.format(data)
            minio_data.append(data)
            if "MINIO" in case_detail:
                data={"password":"inforefiner","port":"9000","host":"192.168.1.81","region":"","username":"minio"}
            elif "OZONE" in case_detail:
                data= {}
            response = requests.post(url=new_url, headers=headers, json=data)
            log.info("response data：%s %s" % (response.status_code, ILLEGAL_CHARACTERS_RE.sub(r'', response.text)))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        elif "上传图片_MINIO" in case_detail:
            fs = {"file": open(fileset_dir, 'rb')}
            headers.pop('Content-Type')
            response = requests.post(url=url, headers=headers, files=fs)
            log.info("response data：%s %s" % (response.status_code, ILLEGAL_CHARACTERS_RE.sub(r'', response.text)))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        elif "上传图片_OZONE" in case_detail:
            fs = {"file": open(fileset_dir, 'rb')}
            headers.pop('Content-Type')
            response = requests.post(url=url, headers=headers, files=fs)
            log.info("response data：%s %s" % (response.status_code, ILLEGAL_CHARACTERS_RE.sub(r'', response.text)))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        else:
            if data:
                data = str(data)
                if data.startswith('select id'):
                    result = ms.ExecuQuery(data)
                    if result:
                        new_data = result[0]["id"]
                        new_url = url.format(new_data)
                        response = requests.post(url=url, headers=headers, data=new_data)
                        log.info("response data：%s %s" % (response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(table_sheet_name, row, column, response.status_code)
                        write_result(table_sheet_name, row, column + 4, response.text)
                    else:
                        log.info('请确认result:%s！' % result)
                elif data.startswith('{') and data.endswith('}'):
                    data_dict = dict_res(data)
                    response = requests.post(url=url, headers=headers, json=data_dict)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif data.startswith('[') and data.endswith(']'):
                    response = requests.post(url=url, headers=headers, data=data)
                    time.sleep(5)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:
                    new_data = []
                    new_data.append(data)
                    new_data = str(new_data)
                    if "'" in new_data:
                        new_data = new_data.replace("'", '"')
                        response = requests.post(url=url, headers=headers, data=new_data)
                        log.info("response data：%s %s" % (response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                    else:
                        response = requests.post(url=url, headers=headers, data=new_data)
                        log.info("response data：%s %s" % (response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            else:
                response = requests.post(url=url, headers=headers, data=data)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    except Exception as e:
        log.error("测试用例{}执行过程中出错{}".format(case_detail, e))



def get_request_result_check(url, headers, host, data, table_sheet_name, row, column):
    """
    GET接口请求，GET请求需要从parameter中获取参数,并把参数拼装到URL中
    :param url:
    :param headers:
    :param host:
    :param data:
    :param table_sheet_name:
    :param row:
    :param column:
    :return:
    """
    case_detail = case_table_sheet.cell(row=row, column=2).value
    log.info("开始执行：%s" % case_detail)
    log.info("请求url：%s" % url)
    try:
        if data:
            if 'Id存在' in case_detail:
                statement_id = statementId(host, data)
                parameter_list = []
                parameter_list.append(data)
                parameter_list.append(statement_id)
                new_url = url.format(parameter_list[0], parameter_list[1])
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                count_num = 0
                while response.text in ('{"statement":"waiting"}', '{"statement":"running"}'):
                    response = requests.get(url=new_url, headers=headers)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    count_num += 1
                    if count_num == 100:
                        return
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '根据statement id,获取Sql Analyze结果(获取输出字段)':
                sql_analyse_statement_id = get_sql_analyse_statement_id(host, data)
                new_url = url.format(sql_analyse_statement_id)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                count_num = 0
                while ("waiting") in response.text or ("running") in response.text:
                    log.info("再次查询前：%s %s" % (response.status_code, response.text))
                    response = requests.get(url=new_url, headers=headers)
                    count_num += 1
                    if count_num == 100:
                        return
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '结束指定statementId对应的查询任务':
                cancel_statement_id = get_sql_analyse_statement_id(host, data)
                new_url = url.format(cancel_statement_id)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '根据解析sql parse接口返回的statementId,获取dataset name':
                datasetName_statementId = steps_sql_parseinit_statemenId(host,data)
                new_url = url.format(datasetName_statementId)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                count_num = 0
                while ("waiting") in response.text or ("running") in response.text:
                    response = requests.get(url=new_url, headers=headers)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    time.sleep(5)
                    count_num += 1
                    if count_num == 50:
                        return
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '根据Sql Analyze返回的statementId,获取SqlAnalyze结果':
                steps_sql_analyse_statementId = steps_sql_analyzeinit_statementId(host,data)
                new_url = url.format(steps_sql_analyse_statementId)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                count_num = 0
                while ("waiting") in response.text or ("running") in response.text:
                    response = requests.get(url=new_url, headers=headers)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    time.sleep(5)
                    count_num += 1
                    if count_num == 50:
                        return
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail in ('查看元数据同步任务的日志进度','拉取元数据同步任务的日志','根据tasks id 查看完整log'):
                time.sleep(10)
                task_id = collector_schema_sync(data)
                time.sleep(5)
                new_url = url.format(task_id)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '导出flow':
                token = get_auth_token(host)
                new_url = url.format(token)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '查看指定id数据源':
                new_url = url.format(data)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '根据statementID获取step的输出字段':
                init_statementId = get_step_output_init_statementId(host, data)
                new_url = url.format(init_statementId)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                count_num = 1
                while "running" in response.text or "waiting" in response.text:
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    time.sleep(5)
                    response = requests.get(url=new_url, headers=headers)
                    count_num += 1
                    if count_num == 50:
                        return
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '根据statementID确认step':
                ensure_statementId = get_step_output_ensure_statementId(host, data)
                new_url = url.format(ensure_statementId)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                count_num = 0
                while "running" in response.text or "waiting" in response.text:
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    time.sleep(5)
                    response = requests.get(url=new_url, headers=headers)
                    count_num += 1
                    if count_num == 50:
                        return
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif "运行成功" in case_detail:
                new_url=url.format(data)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url,headers=headers)
                status=json.loads(response.text)["statusType"]
                while status == "WAITTING" or status =="RUNNING" or status =="READY":
                      log.info("------进入while循环------\n")
                      response = requests.get(url=new_url,headers=headers)
                      status=json.loads(response.text)["statusType"]
                      log.info("------再次查询后的状态为: %s------\n" % status)
                      time.sleep(10)
                if status == "SUCCEEDED":
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:
                    log.info("flow执行状态出错")
                    return
            elif 'datasetId不存在'in case_detail:
                response = requests.get(url=url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif '根据statementId取Dataset数据'in case_detail:
                statement_id, new_data = statementId_no_dataset(host, data)
                new_url = url.format(statement_id)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif '查询详情'in case_detail:
                new_url = url.format(data)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            else:
                if '&' in str(data):
                    parameters = data.split('&')
                    for i in range(len(parameters)):
                        if parameters[i].startswith('select id from'):
                            try:
                                select_result = ms.ExecuQuery(parameters[i])
                                parameters[i] = select_result[0]["id"]
                            except:
                                log.info('第%s行参数没有返回结果' % row)

                        elif parameters[i].startswith('select name from'):
                            try:
                                select_result = ms.ExecuQuery(parameters[i])
                                parameters[i] = select_result[0]["name"]
                            except:
                                log.info('第%s行参数没有返回结果' % row)
                        elif parameters[i].startswith('select execution_id from'):
                            try:
                                select_result = ms.ExecuQuery(parameters[i])
                                parameters[i] = select_result[0]["execution_id"]
                            except:
                                log.info('第%s行参数没有返回结果' % row)
                    '''判断URL中需要的参数个数，并比较和data中的参数个数是否相等'''
                    if len(parameters) == 1:
                        try:
                            url_new = url.format(parameters[0])
                            response = requests.get(url=url_new, headers=headers)
                            log.info("response data：%s %s" % (response.status_code, response.text))
                        except Exception:
                            return
                        log.info("response data：%s %s" % (response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                    elif len(parameters) == 2:
                        url_new = url.format(parameters[0], parameters[1])
                        response = requests.get(url=url_new, headers=headers)
                        log.info("response data：%s %s" % (response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                    elif len(parameters) == 3:
                        url_new = url.format(parameters[0], parameters[1], parameters[2])
                        response = requests.get(url=url_new, headers=headers)
                        log.info("response data：%s %s" % (response.status_code, response.text))
                        clean_vaule(table_sheet_name, row, column)
                        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                    else:
                        log.info('请确认第%d行parameters' % row)
                else:
                    new_url = url.format(data)
                    log.info('new_url:%s' % new_url)
                    response = requests.get(url=new_url, headers=headers)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        else:
            if case_detail in('根据applicationId获取yarnAppliction任务运行状态','根据applicationId获取yarnAppliction任务的日志command line log'):
                application_id = get_applicationId()
                new_url = url.format(application_id)
                log.info('new_url:%s' % new_url)
                response = requests.get(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '根据质量分析结果path预览dataset-获取datasetId':
                dataset_path = get_woven_qaoutput_dataset_path()[0]
                new_url = url.format(dataset_path)
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
    :param url:
    :param host:
    :param row:
    :param data:
    :param table_sheet_name:
    :param column:
    :param headers:
    :return:
    """
    case_detail = case_table_sheet.cell(row=row, column=2).value
    log.info("开始执行：%s" % case_detail)
    log.info("请求url：%s" % url)
    try:
        if case_detail == '项目目录改名':
            response = requests.put(url=url, headers=headers, data=data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(table_sheet_name, row, column, response.status_code)
            write_result(table_sheet_name, row, column+4, response.text)
        elif case_detail == '更新schema':
            schema_id, new_data = updschema_data(data)
            new_url = url.format(schema_id)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.put(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(table_sheet_name, row, column, response.status_code)
            write_result(table_sheet_name, row, column+4, response.text)
        elif case_detail == '更新数据源':
            dss_id, new_data = upddss_data(data)
            new_url = url.format(dss_id)
            new_data = json.dumps(new_data, separators=(',', ':'))
            print("new_url: ",new_url,new_data)
            response = requests.put(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(table_sheet_name, row, column, response.status_code)
            write_result(table_sheet_name, row, column+4, response.text)
        elif case_detail == '更新dataset':
            dataset_id, new_data = upddataset_data(data)
            new_url = url.format(dataset_id)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.put(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(table_sheet_name, row, column, response.status_code)
            write_result(table_sheet_name, row, column+4, response.text)
        elif '更新flow'in case_detail:
            dataset_id, new_data = update_flow_data(data)
            new_url = url.format(dataset_id)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.put(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(table_sheet_name, row, column, response.status_code)
            write_result(table_sheet_name, row, column+4, response.text)
        elif '修改记录' in case_detail:
            types=case_detail.split("_")[1]
            url = url.format(data)
            tag_data_result=tag_data(types,data)
            response = requests.put(url=url, headers=headers, json=tag_data_result)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(table_sheet_name, row, column, response.status_code)
            write_result(table_sheet_name, row, column+4, response.text)
        elif case_detail == '禁用角色':
            new_data = enable_role(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.put(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '启用角色':
            new_data = enable_role(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.put(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '禁用用户':
            new_data = enable_user(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.put(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '启用用户':
            new_data = enable_user(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.put(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '更新数据库驱动' == case_detail:
            new_data = update_db_driver(data)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.put(url=url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '更新自定义节点':
            new_data, step_id = update_custom_step(data)
            new_url = url.format(step_id)
            log.info("new_url: %s" % new_url)
            new_data = json.dumps(new_data, separators=(',', ':'))
            response = requests.put(url=new_url, headers=headers, data=new_data)
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '&' in str(data):
            '''分隔参数'''
            parameters = data.split('&')
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
                write_result(table_sheet_name, row, column+4, response.text)
            else:
                log.info('请确认第%d行parameters中需要update的值格式，应为id&{data}' % row)
        else:
            if data.startswith('{') and data.endswith('}'):
                response = requests.put(url=url, headers=headers, data=data.encode('utf-8'))
                log.info("response data：%s %s" % (response.status_code, response.text))
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
            elif data.startswith('[') and data.endswith(']'):
                pass
            else:
                new_url = url.format(data)
                log.info("new_url：%s" % new_url)
                response = requests.put(url=new_url, headers=headers)
                log.info("response data：%s %s" % (response.status_code, response.text))
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
    except Exception as e:
        log.error("{}执行过程中出错{}".format(case_detail,e))


def delete_request_result_check(url, data, table_sheet_name, row, column, headers):
    """
    delete接口请求
    :param url:
    :param host:
    :param data:
    :param table_sheet_name:
    :param row:
    :param column:
    :param headers:
    :return:
    """
    case_detail = case_table_sheet.cell(row=row, column=2).value
    log.info("开始执行：%s" % case_detail)
    log.info("请求url：%s" % url)
    try:
        if isinstance(data, str):
            if case_detail == '':
                pass
            # elif ("删除标签") in case_detail:
            #     para=data.split("&")
            #     es_id=get_es_data(para[0],para[1],para[2],eval(para[3]))
            #     data={para[4]:es_id}
            #     response = requests.delete(url=url, headers=headers, json=data)
            #     log.info("response data：%s %s" % (response.status_code, ILLEGAL_CHARACTERS_RE.sub(r'', response.text)))
            #     clean_vaule(table_sheet_name, row, column)
            #     write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            #     write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
            else:
                if data.startswith('select id'):  # sql语句的查询结果当做参数
                    data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                    datas = []
                    if data_select_result:
                        try:
                            for i in range(len(data_select_result)):
                                datas.append(data_select_result[i]["id"])
                        except:
                            log.info('请确认第%d行SQL语句' % row)
                        else:
                            if len(datas) == 1:
                                new_url = url.format(datas[0])
                                response = requests.delete(url=new_url, headers=headers)
                                log.info("response data：%s %s" % (response.status_code, response.text))
                                clean_vaule(table_sheet_name, row, column)
                                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                            else:
                                log.info('请确认 select 语句查询返回值是不是只有一个')
                    else:
                        log.info('第%d行参数查询无结果' % row)
                else:
                    new_url = url.format(data)
                    log.info("new_url：%s" % new_url)
                    response = requests.delete(url=new_url, headers=headers)
                    log.info("response data：%s %s" % (response.status_code, response.text))
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '删除非结构化文件集合':
            response = requests.delete(url=url, headers=headers,json=data)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif ("服务器删除新目录") in case_detail:
            if "_MINIO" in case_detail:
                data={"conf":{"password":"inforefiner","port":"9000","host":"192.168.1.81","region":"","username":"minio"},"name":minio_data}
            elif "_OZONE" in case_detail:
                data={"conf":{},"name":minio_data}
            response = requests.delete(url=url, headers=headers, json=data)
            log.info("response data：%s %s" % (response.status_code, ILLEGAL_CHARACTERS_RE.sub(r'', response.text)))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=ILLEGAL_CHARACTERS_RE.sub(r'', response.text))
        elif isinstance(data,list):
            response = requests.delete(url=url, headers=headers,data=json.dumps(data))
            log.info("response data：%s %s" % (response.status_code, response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        else:
            log.info('请确认第%d行的data形式' % row)
    except Exception as e:
        log.error("{}执行过程中出错{}".format(case_detail,e))



def write_result(sheet, row, column, value):
    """
    写入返回结果
    :param sheet:
    :param row:
    :param column:
    :param value:
    :return: 写入返回结果
    """
    sheet.cell(row=row, column=column, value=value)



def clean_vaule(sheet, row, column):
    """
    :param sheet:
    :param row:
    :param column:
    :return: 写入结果前，先把结果和对比结果全部清空
    """
    sheet.cell(row=row, column=column, value='')
    sheet.cell(row=row, column=column+1, value='')
    sheet.cell(row=row, column=column + 4, value='')
    sheet.cell(row=row, column=column + 5, value='')
    sheet.cell(row=row, column=column + 6, value='')
    sheet.cell(row=row, column=column + 7, value='')



class CheckResult(unittest.TestCase):

    def compare_code_result(self):
        """1.对比预期code和接口响应返回的status code"""
        for row in range(2, all_rows+1):
            """预期status code和接口返回status code"""
            ex_status_code = case_table_sheet.cell(row=row, column=7).value
            ac_status_code = case_table_sheet.cell(row=row, column=8).value
            """判断两个status code是否相等"""
            if ex_status_code and ac_status_code != '':
                if ex_status_code == ac_status_code:
                    case_table_sheet.cell(row=row, column=9, value='pass')
                else:
                    case_table_sheet.cell(row=row, column=9, value='fail') # code不等时，用例结果直接判断为失败
                    case_table_sheet.cell(row=row, column=15, value='%s--->失败原因：返回status_code对比失败,预期为%s,实际为%s' %
                                                                    (case_table_sheet.cell(row=row, column=2).value, case_table_sheet.cell(row=row, column=7).value, case_table_sheet.cell(row=row, column=8).value))
            else:
                log.info("第 %d 行 status_code为空" %row)
        case_table.save(ab_dir('api_cases.xlsx'))


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

        case_table.save(ab_dir('api_cases.xlsx'))


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
                if "&" in expect_text:
                    for i in expect_text.split("&"):
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
                response_text_list = []
                response_text_list.append(response_text)
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
                if "&" in expect_text:
                    for i in expect_text.split("&"):
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
        case_table.save(ab_dir('api_cases.xlsx'))


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
            status_code_result = case_table_sheet.cell(row=row, column=9).value
            response_text_result = case_table_sheet.cell(row=row, column=13).value
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
        case_table.save(ab_dir('api_cases.xlsx'))