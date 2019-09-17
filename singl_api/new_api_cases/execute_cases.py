# coding:utf-8
import os
import re
import time
# from selenium import webdriver
from util import get_host
from openpyxl import load_workbook
import requests
from basic_info.get_auth_token import get_headers, get_headers_root,get_auth_token
from util.encrypt import encrypt_rf
from util.format_res import dict_res, get_time
from basic_info.setting import MySQL_CONFIG, MY_LOGIN_INFO2
from util.Open_DB import MYSQL
from basic_info.setting import host
from basic_info.setting import tenant_id_83
from new_api_cases.deal_parameters import deal_parameters
import random, unittest
from new_api_cases.get_statementId import statementId_flow_use
from new_api_cases.get_statementId import statementId, statementId_no_dataset, get_sql_analyse_statement_id, \
    get_sql_analyse_dataset_info, get_sql_execte_statement_id, steps_sql_parseinit_statemenId, \
    steps_sql_analyzeinit_statementId,get_step_output_init_statementId,get_step_output_ensure_statementId
from new_api_cases.prepare_datas_for_cases import get_job_tasks_id,collector_schema_sync, get_applicationId,\
    get_woven_qaoutput_dataset_path,upload_jar_file_workflow,upload_jar_file_dataflow,upload_jar_file_filter


ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))
case_table = load_workbook(ab_dir("api_cases.xlsx"))
case_table_sheet = case_table.get_sheet_by_name('tester')
all_rows = case_table_sheet.max_row
# print(case_table_sheet.cell(row=2, column=10).value, case_table_sheet.cell(row=2,column=12).value)
jar_dir = ab_dir('woven-common-3.0.jar')


# 判断请求方法，并根据不同的请求方法调用不同的处理方式
def deal_request_method():
    for i in range(2, all_rows+1):
        request_method = case_table_sheet.cell(row=i, column=4).value
        old_request_url = case_table_sheet.cell(row=i, column=5).value
        request_url = deal_parameters(old_request_url)
        # print(request_url, type(request_url))
        host = get_host.get_host(request_url)
        old_data = case_table_sheet.cell(row=i, column=6).value
        request_data = deal_parameters(old_data)
        # request_data = data.encode('utf-8')
        # print(request_data)
        key_word = case_table_sheet.cell(row=i, column=3).value
        api_name = case_table_sheet.cell(row=i, column=1).value
        # 请求方法转大写
        if request_method:
            request_method_upper = request_method.upper()
            if api_name == 'tenants':  # 租户的用例需要使用root用户登录后操作
                # 根据不同的请求方法，进行分发
                if request_method_upper == 'POST':
                    # 调用post方法发送请求
                    post_request_result_check(row=i, column=8, url=request_url, host=host, headers=get_headers_root(host),
                                              data=request_data, table_sheet_name=case_table_sheet)

                elif request_method_upper == 'GET':
                    # 调用GET请求
                    get_request_result_check(url=request_url, host=host, headers=get_headers_root(host), data=request_data,
                                             table_sheet_name=case_table_sheet, row=i, column=8)

                elif request_method_upper == 'PUT':
                    put_request_result_check(url=request_url, host=host, row=i, data=request_data,
                                             table_sheet_name=case_table_sheet, column=8, headers=get_headers_root(host))

                elif request_method_upper == 'DELETE':
                    delete_request_result_check(request_url, request_data, host=host, table_sheet_name=case_table_sheet, row=i,
                                                column=8, headers=get_headers_root(host))
                else:
                    print('请求方法%s不在处理范围内' % request_method)
            else:
                # 根据不同的请求方法，进行分发
                if request_method_upper == 'POST':
                    # 调用post方法发送请求
                    post_request_result_check(row=i, host=host, column=8, url=request_url, headers=get_headers(host),
                                                    data=request_data, table_sheet_name=case_table_sheet)

                elif request_method_upper == 'GET':
                    # 调用GET请求
                    get_request_result_check(url=request_url, host=host, headers=get_headers(host), data=request_data,
                                             table_sheet_name=case_table_sheet, row=i, column=8)

                elif request_method_upper == 'PUT':
                    put_request_result_check(url=request_url, host=host, row=i, data=request_data, table_sheet_name=case_table_sheet, column=8, headers=get_headers(host))

                elif request_method_upper == 'DELETE':
                    delete_request_result_check(url=request_url, host=host, data=request_data,table_sheet_name=case_table_sheet,row=i,column=8, headers=get_headers(host))

                else:
                    print('请求方法%s不在处理范围内' % request_method)
        else:
            print('第 %d 行请求方法为空' % i)
    #  执行结束后保存表格
    case_table.save(ab_dir("api_cases.xlsx"))


# POST请求
def post_request_result_check(row, column, url, host, headers, data, table_sheet_name):
    # if isinstance(data, str):
    case_detail = case_table_sheet.cell(row=row, column=2).value
    if '(Id不存在)' in case_detail:
        # 先获取statementId,然后格式化URL，再发送请求
        print('开始执行：', case_detail)
        statement = statementId_no_dataset(host, dict_res(data))
        new_url = url.format(statement)
        data = data.encode('utf-8')
        response = requests.post(url=new_url, headers=headers, data=data)
        print(response.text, response.status_code)
        # print(response.url)
        # 将返回的status_code和response.text分别写入第10列和第14列
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)

    elif case_detail == '获取SQL执行任务结果':
        print('开始执行：', case_detail)
        # 先获取接口需要使用的statement id 和 数据集分析字段
        execte_statement_id = get_sql_execte_statement_id(host,data)  # statement id
        new_url = url.format(execte_statement_id)
        # print('获取SQL执行任务结果URL:', new_url)
        execte_use_params = get_sql_analyse_dataset_info(host,data)  # 数据集分析字段
        # print(execte_use_params)
        response = requests.post(url=new_url, headers=headers, json=execte_use_params)
        count_num = 0
        while ("waiting") in response.text or ("running") in response.text:
            # print('再次查询前',res.text)
            response = requests.post(url=new_url, headers=headers, json=execte_use_params)
            count_num += 1
            if count_num == 100:
                return
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        # print(response.status_code)
        # print(response.text)
    elif case_detail == '批量删除execution':
        print('开始执行：', case_detail)
        # 需要先查询指定flow下的所有execution，从中取出execution id，拼装成list，传递给删除接口
        query_execution_url = '%s/api/executions/query' % host
        all_exectuions = requests.post(url=query_execution_url, headers=headers, data=data)
        executions_dict = dict_res(all_exectuions.text)
        # print(executions_dict)
        try:
            executions_content = executions_dict['content']
            all_ids = [] # 该list用来存储所有的execution id
            for item in executions_content:
                executions_content_id = item['id']
                all_ids.append(executions_content_id)
        except Exception as e:
            print(e)
        else:  # 取出一个id放入一个新的list，作为传递给removeLIst接口的参数
            removelist_data = []
            removelist_data.append(all_ids[-1])
            # 执行删除操作
            removeList_result = requests.post(url=url,headers=headers, json=removelist_data)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=removeList_result.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=removeList_result.text)
    elif case_detail == '停止一个采集器任务的执行':
        print('开始执行：', case_detail)
        task_id = get_job_tasks_id(data)
        response = requests.post(url=url, headers=headers, json=task_id)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '指定目录下创建子目录':
        print('开始执行：', case_detail)
        response = requests.post(url=url, headers=headers, json=dict_res(data))
        # print(response.text)
        # print(response.status_code)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '数据源状态监控分析图数据':  # 查看当前时间至向前一周的数据源状态
        data = {"fieldList":[{"fieldName":"createTime","fieldValue":get_time(),"comparatorOperator":"GREATER_THAN","logicalOperator":"AND"},{"fieldName":"createTime","fieldValue":1555516800000,"comparatorOperator":"LESS_THAN"}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8,"groupBy":"testTime"}
        response = requests.post(url=url,headers=headers, json=data)
        # print(response.status_code,response.text)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail in ('配置工作流选择器-上传jar包', '配置过滤器-上传jar包', '配置批处理选择器-上传jar包'):
        files = {"file": open(jar_dir, 'rb')}
        headers.pop('Content-Type')
        response = requests.post(url=url, files=files, headers=headers)
        # print(response.text)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '注册工作流选择器':
        fileName = upload_jar_file_workflow()
        new_url = url.format(fileName)
        # print(new_url)
        # print(data)
        response = requests.post(url=new_url, headers=headers, data=data)
        # print(response.text)
        # print(response.content)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '注册批处理选择器':
        fileName = upload_jar_file_dataflow()
        new_url = url.format(fileName)
        response = requests.post(url=new_url, headers=headers, data=data)
        # print(response.text)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '注册过滤器':
        fileName = upload_jar_file_filter()
        new_url = url.format(fileName)
        response = requests.post(url=new_url, headers=headers, data=data)
        # print(response.text)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '数据标准导入文件':
        dir2 = ab_dir('sex.xls')
        files = {"file": open(dir2, 'rb')}
        headers = get_headers(host)
        headers.pop('Content-Type')

        response = requests.post(url, files=files, headers=headers)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)

    elif case_detail == "获取令牌":
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        headers.pop('X-AUTH-TOKEN')
        response = requests.post(url, headers=headers, data=dict_res(data))
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '多条件查询流程的输出':
        try:
            print(data)
            new_data = {"fieldList": [
                {"fieldName": "executionId", "fieldValue": data, "comparatorOperator": "EQUAL",
                 "logicalOperator": "AND"}], "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"},
                        "offset": 0, "limit": 8}
            print(new_data)
            response = requests.post(url=url, headers=headers, json=new_data)
            print(response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        except Exception:
            print('没有查到flow：tc_df_top_基本测试 的最新一条execution id')
    elif case_detail == '删除停用状态的用户':
        # 停用所选id的user
        new_data = [{"id": data, "enabled": 0}]
        del_url = '%s/api/user/resetStatus' % host
        res = requests.post(url=del_url, headers=headers, json=new_data)
        # 删除指定id的user
        del_user_id = []
        del_user_id.append(data)
        response = requests.post(url=url, headers=headers, json=del_user_id)
        print('删除后：',response.status_code, response.content)
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
            print(response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '不存在的账户登录':
            data = {'name': encrypt_rf('admin12399999999999999'), 'password': encrypt_rf('123456'), 'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
            response = requests.post(url=url, headers=new_headers, data=data)
            print(response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '没有权限的账户登录':
            data = {'name': encrypt_rf('user_without_pression'), 'password': encrypt_rf('123456'),
                    'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
            response = requests.post(url=url, headers=new_headers, data=data)
            print(response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '有权限，密码过期的账户登录':
            data = {'name': encrypt_rf('user_pwd_expired'), 'password': encrypt_rf('123456'),
                    'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
            response = requests.post(url=url, headers=new_headers, data=data)
            print(response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '有权限，用户有效期过期的账户登录':
            data = {'name': encrypt_rf('user_time_expired'), 'password': encrypt_rf('123456'),
                    'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
            response = requests.post(url=url, headers=new_headers, data=data)
            print(response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '有权限，密码和用户有效期均过期的账户登录':
            data = {'name': encrypt_rf('user_expired'), 'password': encrypt_rf('123456'),
                    'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
            response = requests.post(url=url, headers=new_headers, data=data)
            print(response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    else:
        print('开始执行：', case_detail)
        if data:
            data = str(data)
            # 字典形式作为参数，如{"id":"7135cf6e-2b12-4282-90c4-bed9e2097d57","name":"gbj_for_jdbcDatasource_create_0301_1_0688","creator":"admin"}
            if data.startswith('{') and data.endswith('}'):
                # print('data startswith {:', data)
                data_dict = dict_res(data)
                # print(data_dict)
                response = requests.post(url=url, headers=headers, json=data_dict)
                print(response.url)
                print(response.status_code, response.content)
                print(response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            # 列表作为参数， 如["9d3639f0-02bc-44cd-ac71-9a6d0f572632"]
            elif data.startswith('[') and data.endswith(']'):
                if "'" in data:
                    data = data.replace("'", '"')
                    print(data)
                    response = requests.post(url=url, headers=headers, data=data)
                    print(response.content)
                    print(response.status_code, response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:
                    print(data)
                    response = requests.post(url=url, headers=headers, data=data)
                    print(response.content)
                    print(response.status_code,response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            else:  # 处理参数是一个字符串id的情况,按照接口格式，放入list中处理
                new_data = []
                new_data.append(data)
                new_data = str(new_data)
                if "'" in new_data:
                    new_data = new_data.replace("'", '"')
                    response = requests.post(url=url, headers=headers, data=new_data)
                    print(response.status_code, response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:
                    response = requests.post(url=url, headers=headers, data=new_data)
                    print(response.status_code,response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        else:
            response = requests.post(url=url, headers=headers, data=data)
            print(response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)

    # else:
    #     print('请确认第%d行的data形式' % row)


# GET请求
def get_request_result_check(url, headers, host, data, table_sheet_name, row, column):
    case_detail = case_table_sheet.cell(row=row, column=2).value
    # GET请求需要从parameter中获取参数,并把参数拼装到URL中，
    if data:
        if '(Id存在)' in case_detail:
            # print(data)
            print('开始执行：', case_detail)
            # data = deal_parameters(data)
            statement_id = statementId(host, data)
            parameter_list = []
            parameter_list.append(data)
            parameter_list.append(statement_id)
            # print(parameter_list)
            url_new = url.format(parameter_list[0], parameter_list[1])
            # print(url_new)
            response = requests.get(url=url_new, headers=headers)
            # print(response.status_code, response.text)
            count_num = 0
            while response.text in ('{"statement":"waiting"}', '{"statement":"running"}'):
                response = requests.get(url=url_new, headers=headers)
                count_num += 1
                if count_num == 100:
                    return
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == ('根据statement id,获取Sql Analyze结果(获取输出字段)'):
            print('开始执行：', case_detail)
            sql_analyse_statement_id = get_sql_analyse_statement_id(host, data)
            new_url = url.format(sql_analyse_statement_id)
            # print(new_url)
            response = requests.get(url=new_url, headers=headers)
            # print(response.url, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == ('结束指定statementId对应的查询任务'):  # 取消SQL analyse接口
            print('开始执行：', case_detail)
            cancel_statement_id = get_sql_analyse_statement_id(host, data)
            new_url = url.format(cancel_statement_id)
            response = requests.get(url=new_url, headers=headers)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)

        elif case_detail == ('根据解析sql parse接口返回的statementId,获取dataset name'):
            print('开始执行：', case_detail)
            data = data.encode('utf-8')
            print('测试：',data, type(data))
            datasetName_statementId = steps_sql_parseinit_statemenId(host, data)
            new_url = url.format(datasetName_statementId)
            response = requests.get(url=new_url, headers=headers)
            # print(response.text)
            count_num = 0
            while response.text in ('{"statement":"waiting"}', '{"statement":"running"}'):
                response = requests.get(url=new_url, headers=headers)
                count_num += 1
                if count_num == 100:
                    return
            # print(response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == ('根据Sql Analyze返回的statementId,获取SqlAnalyze结果'):
            print('开始执行：', case_detail)
            steps_sql_analyse_statementId = steps_sql_analyzeinit_statementId(host, data)
            new_url = url.format(steps_sql_analyse_statementId)
            response = requests.get(url=new_url, headers=headers)
            # print(response.text)
            count_num = 0
            while "waiting" in response.text or "running"in response.text:
                response = requests.get(url=new_url, headers=headers)
                count_num += 1
                if count_num == 100:
                    return
            # print(response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == ('结束sqlsource step中指定statementId对应任务'):
            print('开始执行：', case_detail)
            data = data.encode('utf-8')
            cancel_sql_parseinit_statementId = steps_sql_parseinit_statemenId(host, data)
            new_url = url.format(cancel_sql_parseinit_statementId)
            response = requests.get(url=new_url, headers=headers)
            # print(response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail in ('查看元数据同步任务的日志进度','拉取元数据同步任务的日志','根据tasks id 查看完整log'):
            print('开始执行：', case_detail)
            time.sleep(10)
            task_id = collector_schema_sync(data)
            # print(task_id)
            # print(task_id)
            time.sleep(5)
            new_url = url.format(task_id)
            # time.sleep(2)
            response = requests.get(url=new_url, headers=headers)
            # print(response.url, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '导出flow':
            print('开始执行：', case_detail)
            token = get_auth_token(host)
            new_url = url.format(token)
            # print(token)
            response = requests.get(url=new_url,headers=headers)
            # print(response.url, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据statementID获取step的输出字段':
            print('开始执行：', case_detail)
            init_statementId = get_step_output_init_statementId(host, data)
            # print(init_statementId)
            new_url = url.format(init_statementId)
            response = requests.get(url=new_url, headers=headers)
            count_num = 1
            while "running" in response.text or "waiting" in response.text:
                time.sleep(5)
                response = requests.get(url=new_url, headers=headers)
                count_num += 1
                if count_num == 100:
                    return
            # print(response.url, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据statementID确认dataflow step':
            print('开始执行：', case_detail)
            ensure_statementId = get_step_output_ensure_statementId(host,data)
            # print(ensure_statementId)
            new_url = url.format(ensure_statementId)
            response = requests.get(url=new_url, headers=headers)
            # print(response.url)
            # print(response.status_code,response.text)
            count_num = 0
            while "running" in response.text or "waiting" in response.text:
                time.sleep(5)
                response = requests.get(url=new_url, headers=headers)
                count_num += 1
                if count_num == 100:
                    return
            # print(response.url, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == "查看students_info_zmode_测试用分析模板的成功任务执行结果":
            print(host, data)
            statement_id = statementId_flow_use(host, data,tenant_id_83)
            print(statement_id)
            url = url.format(data,statement_id)
            response = requests.get(url=url,headers=headers)
            print(response.status_code, response.text)
            count_num = 0
            while ("waiting") in response.text or ("running") in response.text:
                # print('再次查询前',res.text)
                response = response = requests.get(url=url,headers=headers)
                print(response.status_code, response.text)
                count_num += 1
                if count_num == 100:
                    return
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        else:
            print('开始执行：', case_detail)
            print(data)
            if '&' in data:  # 包含多个参数并以&分割
                parameters = data.split('&')
                # print('parameters:', parameters)
                # 处理存在select语句中的参数，并重新赋值
                for i in range(len(parameters)):
                    if parameters[i].startswith('select id from'):
                        # select_result = ms.ExecuQuery(parameters[i])
                        try:
                            select_result = ms.ExecuQuery(parameters[i])
                            parameters[i] = select_result[0]["id"]
                        except:
                            print('第%s行参数没有返回结果' % row)

                    elif parameters[i].startswith('select name from'):
                        try:
                            select_result = ms.ExecuQuery(parameters[i])
                            parameters[i] = select_result[0]["name"]
                        except:
                            print('第%s行参数没有返回结果' % row)
                    elif parameters[i].startswith('select execution_id from'):
                        try:
                            select_result = ms.ExecuQuery(parameters[i])
                            parameters[i] = select_result[0]["execution_id"]
                        except:
                            print('第%s行参数没有返回结果' % row)
                # 判断URL中需要的参数个数，并比较和data中的参数个数是否相等
                if len(parameters) == 1:
                    try:
                        url_new = url.format(parameters[0])
                        print(url_new)
                        response = requests.get(url=url_new, headers=headers)
                        # print(response.content, response.status_code, response.text)
                    except Exception:
                        return
                    # print(response.url, response.status_code,response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif len(parameters) == 2:
                    url_new = url.format(parameters[0], parameters[1])
                    # print(url_new)
                    response = requests.get(url=url_new, headers=headers)
                    # print(response.url, response.status_code, response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif len(parameters) == 3:
                    url_new = url.format(parameters[0], parameters[1], parameters[2])
                    response = requests.get(url=url_new, headers=headers)
                    # print(response.url, response.status_code, response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:
                    print('请确认第%d行parameters' % row)
            else:  # 参数中不包含&，只有一个参数
                parameters = data
                url_new = url.format(data)
                print(url_new)
                response = requests.get(url=url_new, headers=headers)
                print(response.status_code, response.text)
                print(response.status_code)
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    # GET 请求参数写在URL中，直接发送请求
    else:
        if case_detail in('根据applicationId获取yarnAppliction任务运行状态','根据applicationId获取yarnAppliction任务的日志command line log'):
            print('开始执行：', case_detail)
            application_id = get_applicationId()
            new_url = url.format(application_id)
            response = requests.get(url=new_url, headers=headers)
            # print(response.status_code, response.text, type(response.text))
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '根据质量分析结果path预览dataset-获取datasetId':
            print('开始执行：', case_detail)
            dataset_path = get_woven_qaoutput_dataset_path()[0]
            new_url = url.format(dataset_path)
            # print(new_url)
            response = requests.get(url=new_url, headers=headers)
            # print(response.url)
            # print(response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '刷新令牌':
            res = requests.post(url=MY_LOGIN_INFO2["URL"], headers=MY_LOGIN_INFO2["HEADERS"],
                                data=MY_LOGIN_INFO2["DATA"])
            login_info = dict_res(res.text)
            token = login_info["content"]["accessToken"]
            new_url = url.format(token)
            print(new_url)
            response = requests.get(url=new_url,headers=headers)
            print(response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)


        else:
            print('开始执行：', case_detail)
            response = requests.get(url=url, headers=headers)
            # print(response.url)
            # print(response.content)
            # print(response.status_code,response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)


# PUT请求
def put_request_result_check(url, host, row, data, table_sheet_name, column, headers):
    if data and isinstance(data, str):
        if '&' in data:
            # 分隔参数
            parameters = data.split('&')
            # 拼接URL
            new_url = url.format(parameters[0])
            # print(new_url)
            # print(parameters)
            # 发送的参数体
            parameters_data = parameters[-1]
            if parameters_data.startswith('{'):
                response = requests.put(url=new_url, headers=headers, json=dict_res(parameters_data))
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column+4, response.text)
            else:
                print('请确认第%d行parameters中需要update的值格式，应为id&{data}' % row)
        else:
            if data.startswith('select id'):
                result = ms.ExecuQuery(data)
                new_data = result[0]["id"]
                # print(new_data, type(new_data))
                new_url = url.format(new_data)
                # print('new_url:', new_url)
                response = requests.put(url=new_url, headers=headers)
                # print(response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
            elif data.startswith('{') and data.endswith('}'):
                print(data)
                response = requests.put(url=url, headers=headers, data=data.encode('utf-8'))
                print(response.status_code, response.text)
                # print(response.url, response.content)
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
            elif data.startswith('[') and data.endswith(']'):
                pass
            else:
                new_url = url.format(data)
                # print('new_url:', new_url)
                response = requests.put(url=new_url, headers=headers)
                # print(response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
    else:
        print('第%s行的参数为空或格式异常' % row)


def delete_request_result_check(url, host, data, table_sheet_name, row, column, headers):
    case_detail = case_table_sheet.cell(row=row, column=2).value
    if isinstance(data, str):
        if case_detail == '':
            pass
        else:
            if data.startswith('select id'):  # sql语句的查询结果当做参数
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                # print(data_select_result)
                # print(type(data_select_result))
                datas = []
                if data_select_result:
                    try:
                        for i in range(len(data_select_result)):
                            datas.append(data_select_result[i]["id"])
                    except:
                        print('请确认第%d行SQL语句' % row)
                    else:
                        if len(datas) == 1:
                            # print(datas)
                            new_url = url.format(datas[0])
                            response = requests.delete(url=new_url, headers=headers)
                            # print(response.url, response.status_code)
                            # 将返回的status_code和response.text分别写入第10列和第14列
                            clean_vaule(table_sheet_name, row, column)
                            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                        else:
                            print('请确认 select 语句查询返回值是不是只有一个')
                else:
                    print('第%d行参数查询无结果' % row)
                # 字典形式作为参数，如{"id":"7135cf6e-2b12-4282-90c4-bed9e2097d57","name":"gbj_for_jdbcDatasource_create_0301_1_0688","creator":"admin"}

            else:
                new_url = url.format(data)
                print(new_url)
                response = requests.delete(url=new_url, headers=headers)
                print(response.status_code, response.text)
                # 将返回的status_code和response.text分别写入第10列和第14列
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    else:
        # print(data)
        # print(type(data))
        print('请确认第%d行的data形式' % row)


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
                    print('预期结果：%s, 实际结果：%s' % (ex_status_code, ac_status_code))
            else:
                print('第 %d 行 status_code为空' % row)
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
                    print('请确认第%d行的key_word' % row)
            elif code_result == 'fail':
                # case 结果列
                case_table_sheet.cell(row=row, column=14, value='fail')
                # case失败原因
                case_table_sheet.cell(row=row, column=15, value='status_code对比结果为%s' % code_result)
            else:
                print('请确认第 %d 行 status_code对比结果' % row)

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
                            print('第 %d 行 response_text返回的id和预期id长度不一致' % row)
                            case_table_sheet.cell(row=row, column=column, value='fail')
                        else:
                            case_table_sheet.cell(row=row, column=column, value='pass')
                    else:
                        try:
                            self.assertEqual(expect_text, response_text, '第%d行的response_text长度和预期不一致' % row)
                        except:
                            print('第 %d 行 response_text和预期text不相等' % row)
                            case_table_sheet.cell(row=row, column=column, value='fail')
                        else:
                            case_table_sheet.cell(row=row, column=column, value='pass')
                else:  # 只返回一个id串的情况下，判断预期长度和id长度一致
                    try:
                        self.assertEqual(expect_text, len(response_text), '第%d行的response_text长度和预期不一致' % row)
                    except:
                        print('第 %d 行 response_text和预期text不相等' % row)
                        case_table_sheet.cell(row=row, column=column, value='fail')
                    else:
                        case_table_sheet.cell(row=row, column=column, value='pass')

            elif relation == 'in':  # 返回多内容时，判断返回内容中包含id属性，并且expect_text包含在response_text中
                try:
                    # self.assertIsNotNone(response_text_dict.get("id"), '第 %d 行 response_text没有返回id' % row)
                    self.assertIn(expect_text, response_text, '第 %d 行 expect_text没有包含在接口返回的response_text中' % row)
                except:
                    print('第 %d 行 expect_text没有包含在response_text中， 结果对比失败' % row)
                    case_table_sheet.cell(row=row, column=column, value='fail')
                else:
                    case_table_sheet.cell(row=row, column=column, value='pass')
            else:
                print('请确认第 %d 行 预期expect_text和response_text的relatrion' % row)
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
                    print('第 %d 行 expect_text和response_text不相等， 结果对比失败' % row)
                    case_table_sheet.cell(row=row, column=column, value='fail')
                else:
                    case_table_sheet.cell(row=row, column=column, value='pass')
            else:
                print('请确认第 %d 行 预期expect_text和response_text的relatrion' % row)
                case_table_sheet.cell(row=row, column=column, value='请确认预期text和接口response.text的relatrion')
        else:
            print('请确认第 %d 行 的key_word' % row)
        case_table.save(ab_dir('api_cases.xlsx'))
    # 对比case最终的结果
    def deal_result(self):
        # 执行测试用例
        # deal_request_method()
        # 对比code
        self.compare_code_result()
        # 对比text
        self.compare_text_result()
        # 根据code result和text result判断case最终结果
        for row in range(2, all_rows + 1):
            status_code_result = case_table_sheet.cell(row=row, column=9).value
            response_text_result = case_table_sheet.cell(row=row, column=13).value
            if status_code_result == 'pass' and response_text_result == 'pass':
                # print('测试用例:%s 测试通过' % case_table_sheet.cell(row=row, column=3).value)
                case_table_sheet.cell(row=row, column=14, value='pass')
                case_table_sheet.cell(row=row, column=15, value='')
            elif status_code_result == 'fail' and response_text_result == 'pass':
                case_table_sheet.cell(row=row, column=14, value='fail')
                case_table_sheet.cell(row=row, column=15, value='%s--->失败原因：status code对比失败,预期为%s,实际为%s'
                                                                % (case_table_sheet.cell(row=row, column=2).value, case_table_sheet.cell(row=row, column=7).value, case_table_sheet.cell(row=row, column=8).value))
            elif status_code_result == 'pass' and response_text_result == 'fail':
                case_table_sheet.cell(row=row, column=14, value='fail')
                case_table_sheet.cell(row=row, column=15, value='%s--->失败原因：返回内容对比失败' %
                                                                (case_table_sheet.cell(row=row, column=2).value))
            elif status_code_result == 'fail' and response_text_result == 'fail':
                case_table_sheet.cell(row=row, column=14, value='fail')
                case_table_sheet.cell(row=row, column=15, value='%s--->失败原因：status code和返回文本对比均失败，请查看附件<api_cases.xlsx>确认具体失败原因'
                                                                % (case_table_sheet.cell(row=row, column=2).value))
            else:
                print('请确认status code或response.text对比结果')
        case_table.save(ab_dir('api_cases.xlsx'))



# 调试
# 执行用例
# deal_request_method()
# # # # 对比用例结果
# g = CheckResult()
# g.deal_result()

