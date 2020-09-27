# coding:utf-8
import json
import os
import re
import time
from util import get_host
from openpyxl import load_workbook
import requests
from util.encrypt import encrypt_rf
from util.format_res import dict_res
from basic_info.setting import Dw_MySQL_CONFIG
from util.Open_DB import MYSQL
from basic_info.get_auth_token import get_headers, get_headers_root
from new_api_cases.dw_deal_parameters import deal_parameters
import unittest
from new_api_cases.dw_prepare_datas import pull_data, update_business_data, \
    add_subject_data, update_subject_data, query_subject_data, add_projects_data, update_projects_data, update_tag_data, \
    add_taggroup_data, update_taggroup_data, update_namerule_data

ms = MYSQL(Dw_MySQL_CONFIG["HOST"], Dw_MySQL_CONFIG["USER"], Dw_MySQL_CONFIG["PASSWORD"], Dw_MySQL_CONFIG["DB"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))
case_table = load_workbook(ab_dir("api_cases.xlsx"))
case_table_sheet = case_table.get_sheet_by_name('dw')
all_rows = case_table_sheet.max_row
jar_dir = ab_dir('woven-common-3.0.jar')


# 判断请求方法，并根据不同的请求方法调用不同的处理方式
def deal_request_method():
    for i in range(2, all_rows+1):
        request_method = case_table_sheet.cell(row=i, column=4).value
        old_request_url = case_table_sheet.cell(row=i, column=5).value
        request_url = deal_parameters(old_request_url)
        host = get_host.get_host(request_url)
        old_data = case_table_sheet.cell(row=i, column=6).value
        request_data = deal_parameters(old_data)
        #request_data = old_data.encode('utf-8')
        print("request data:", request_data)
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
    if case_detail == '根据id查询主题域':
        print('开始执行：', case_detail)
        new_data, business_id = query_subject_data(data)
        new_url = url.format(business_id)
        new_data = json.dumps(new_data, separators=(',', ':'))
        response = requests.post(url=new_url, headers=headers, data=new_data)
        print("response data:", response.status_code, response.text)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '修改业务模块':
        print('开始执行：', case_detail)
        print("request   url:", url)
        new_data,business = update_business_data(data)
        new_url = url.format(business)
        new_data = json.dumps(new_data, separators=(',', ':'))
        response = requests.post(url=new_url, headers=headers, data=new_data)
        print("response data:", response.status_code, response.text)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '修改主题域':
        print('开始执行：', case_detail)
        new_data, subject_id = update_subject_data(data)
        new_url = url.format(subject_id)
        print("request   url:", new_url)
        new_data = json.dumps(new_data, separators=(',', ':'))
        response = requests.post(url=new_url, headers=headers, data=new_data)
        print("response data:", response.status_code, response.text)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '修改项目':
        print('开始执行：', case_detail)
        new_data, protect_id = update_projects_data(data)
        new_url = url.format(protect_id)
        print("request   url:", new_url)
        new_data = json.dumps(new_data, separators=(',', ':'))
        response = requests.post(url=new_url, headers=headers, data=new_data)
        print("response data:", response.status_code, response.text)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '修改标签':
        print('开始执行：', case_detail)
        new_data, tag_id = update_tag_data(data)
        new_url = url.format(tag_id)
        print("request   url:", new_url)
        new_data = json.dumps(new_data, separators=(',', ':'))
        response = requests.post(url=new_url, headers=headers, data=new_data)
        print("response data:", response.status_code, response.text)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '修改标签组':
        print('开始执行：', case_detail)
        new_data, taggroup_id = update_taggroup_data(data)
        new_url = url.format(taggroup_id)
        print("request   url:", new_url)
        new_data = json.dumps(new_data, separators=(',', ':'))
        response = requests.post(url=new_url, headers=headers, data=new_data)
        print("response data:", response.status_code, response.text)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '修改已绑定标签组的标签':
        print('开始执行：', case_detail)
        new_data, tag_id = update_tag_data(data)
        new_url = url.format(tag_id)
        print("request   url:", new_url)
        new_data = json.dumps(new_data, separators=(',', ':'))
        response = requests.post(url=new_url, headers=headers, data=new_data)
        print("response data:", response.status_code, response.text)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif case_detail == '修改命名规则':
        print('开始执行：', case_detail)
        new_data, namerule_id = update_namerule_data(data)
        new_url = url.format(namerule_id)
        print("request   url:", new_url)
        new_data = json.dumps(new_data, separators=(',', ':'))
        response = requests.post(url=new_url, headers=headers, data=new_data)
        print("response data:", response.status_code, response.text)
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    elif '登录' in case_detail:
        new_headers = {'Content-Type': 'application/x-www-form-urlencoded', "Authorization": 'Basic ZHNwOjEyMzQ1Ng==', "Accept": "application/json"}
        if case_detail == '管理员登录':
            data = {'username': 'admin', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default','grant_type': 'manager_password'}
            #print("request   url:", url)
            response = requests.post(url=url, headers= new_headers, data=data)
            #print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '消费者登录':
            data = {'username': 'customer3', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default','grant_type':'customer_password'}
            #print("request   url:", url)
            response = requests.post(url=url, headers=new_headers, data=data)
            #print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '密码错误的账户登录':
            data = {'username': 'customer3', 'password': '1234567', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default','grant_type':'customer_password'}
            #print("request   url:", url)
            response = requests.post(url=url, headers=new_headers, data=data)
            #print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '不存在的账户登录':
            data = {'username': 'adminer', 'password': '123456', 'version': 'Baymax-3.0.0.23-20180606', 'tenant': 'default','grant_type':'manager_password'}
            #print("request   url:", url)
            response = requests.post(url=url, headers=new_headers, data=data)
            #print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '没有权限的账户登录':
            data = {'name': encrypt_rf('user_without_pression'), 'password': encrypt_rf('123456'),
                    'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
            #print("request   url:", url)
            response = requests.post(url=url, headers=new_headers, data=data)
            #print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '有权限，密码过期的账户登录':
            data = {'name': encrypt_rf('user_pwd_expired'), 'password': encrypt_rf('123456'),
                    'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
            #print("request   url:", url)
            response = requests.post(url=url, headers=new_headers, data=data)
            #print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '有权限，用户有效期过期的账户登录':
            data = {'name': encrypt_rf('user_time_expired'), 'password': encrypt_rf('123456'),
                    'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
            #print("request   url:", url)
            response = requests.post(url=url, headers=new_headers, data=data)
            #print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '有权限，密码和用户有效期均过期的账户登录':
            data = {'name': encrypt_rf('user_expired'), 'password': encrypt_rf('123456'),
                    'version': 'Europa-3.0.0.19 - 20180428', 'tenant': encrypt_rf('default')}
            #print("request   url:", url)
            response = requests.post(url=url, headers=new_headers, data=data)
            #print("response data:", response.status_code, response.text)
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
                #print("request   url:", url)
                response = requests.post(url=url, headers=headers, json=data_dict)
                #print("response data:", response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            # 列表作为参数， 如["9d3639f0-02bc-44cd-ac71-9a6d0f572632"]
            elif data.startswith('[') and data.endswith(']'):
                if "'" in data:
                    data = data.replace("'", '"')
                    #print(data)
                    #print("request   url:", url)
                    response = requests.post(url=url, headers=headers, data=data)
                    #print("response data:", response.status_code, response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:
                    #print(data)
                    #print("request   url:", url)
                    response = requests.post(url=url, headers=headers, data=data)
                    #print("response data:", response.status_code, response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            else:  # 处理参数是一个字符串id的情况,按照接口格式，放入list中处理
                new_data = []
                new_data.append(data)
                new_data = str(new_data)
                if "'" in new_data:
                    new_data = new_data.replace("'", '"')
                    #print("request   url:", url)
                    response = requests.post(url=url, headers=headers, data=new_data)
                    #print("response data:", response.status_code, response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:
                    print("request   url:", url)
                    response = requests.post(url=url, headers=headers, data=new_data)
                    print("response data:", response.status_code, response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        else:
            print("request   url:", url)
            response = requests.post(url=url, headers=headers, data=data)
            print("response data:", response.status_code, response.text)
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
        if '根据id查询项目' in case_detail:
            print('开始执行：', case_detail)
            new_url = url.format(data)
            print("request   url:", new_url)
            response = requests.get(url=new_url, headers=headers)
            print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '查询标签组关联标签' in case_detail:
            print('开始执行：', case_detail)
            new_url = url.format(data)
            print("request   url:", new_url)
            response = requests.get(url=new_url, headers=headers)
            print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '审批申请记录' in case_detail:
            print('开始执行：', case_detail)
            new_url = url.format(data)
            #print("request   url:", new_url)
            response = requests.get(url=new_url, headers=headers)
            #print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == ('管理员查询订阅服务'):  # 取消SQL analyse接口
            print('开始执行：', case_detail)
            #cancel_statement_id = get_sql_analyse_statement_id(host, data)
            new_url = url.format(data)
            #print("request   url:", new_url)
            response = requests.get(url=new_url, headers=headers)
            #print("response data:", response.status_code, response.text)
            count_num = 0
            while '"executedTimes":0'in response.text:
                print('再次查询前', response.status_code, response.text)
                response = requests.get(url=new_url, headers=headers)
                time.sleep(5)
                count_num += 1
                if count_num == 80:
                    return
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == '服务删除':  # 取消SQL analyse接口
            print('开始执行：', case_detail)
            new_url = url.format(data)
            #print("request   url:", new_url)
            response = requests.get(url=new_url, headers=headers)
            #print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == ('删除启用状态用户'):  # 取消SQL analyse接口
            print('开始执行：', case_detail)
            new_url = url.format(data)
            #print("request   url:", new_url)
            response = requests.get(url=new_url, headers=headers)
            #print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif '删除接入配置' in case_detail:  # 取消SQL analyse接口
            print('开始执行：', case_detail)
            new_url = url.format(data)
            #print("request   url:", new_url)
            response = requests.get(url=new_url, headers=headers)
            #print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == ('消费者查询订阅服务'):  # 取消SQL analyse接口
            print('开始执行：', case_detail)
            #cancel_statement_id = get_sql_analyse_statement_id(host, data)
            new_url = url.format(data)
            #print("request   url:", new_url)
            response = requests.get(url=new_url, headers=headers)
            #print("response data:", response.status_code, response.text)
            count_num = 0
            while '"executedTimes":0' in response.text:
                #print('再次查询前', response.status_code, response.text)
                response = requests.get(url=new_url, headers=headers)
                time.sleep(5)
                count_num += 1
                if count_num == 80:
                    return
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == ('管理员查询申请服务'):  # 取消SQL analyse接口
            print('开始执行：', case_detail)
            #cancel_statement_id = get_sql_analyse_statement_id(host, data)
            new_url = url.format(data)
            #print("request   url:", new_url)
            response = requests.get(url=new_url, headers=headers)
            #print("response data:", response.status_code, response.text)
            count_num = 0
            while'"isRunning":0'in response.text or '"isRunning":1' in response.text:
                print('再次查询前', response.status_code, response.text)
                response = requests.get(url=new_url, headers=headers)
                time.sleep(5)
                count_num += 1
                if count_num == 180:
                    return
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        elif case_detail == ('消费者查询申请服务'):  # 取消SQL analyse接口
            print('开始执行：', case_detail)
            #cancel_statement_id = get_sql_analyse_statement_id(host, data)
            new_url = url.format(data)
            #print("request   url:", new_url)
            response = requests.get(url=new_url, headers=headers)
            #print("response data:", response.status_code, response.text)
            count_num = 0
            while'"isRunning":0'in response.text or '"isRunning":1' in response.text:
                print('再次查询前', response.status_code, response.text)
                response = requests.get(url=new_url, headers=headers)
                time.sleep(5)
                count_num += 1
                if count_num == 180:
                    return
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        else:
            print('开始执行：', case_detail)
            #print(data)
            if '&' in str(data):  # 包含多个参数并以&分割
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
                        #print("request   url:", url_new)
                        response = requests.get(url=url_new, headers=headers)
                        #print("response data:", response.status_code, response.text)
                    except Exception:
                        return
                    #print(response.url, response.status_code,response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif len(parameters) == 2:
                    url_new = url.format(parameters[0], parameters[1])
                    #print("request   url:", url_new)
                    response = requests.get(url=url_new, headers=headers)
                    #print("response data:", response.status_code, response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                elif len(parameters) == 3:
                    url_new = url.format(parameters[0], parameters[1], parameters[2])
                    #print("request url:", url_new)
                    response = requests.get(url=url_new, headers=headers)
                    #print("response data:", response.status_code, response.text)
                    clean_vaule(table_sheet_name, row, column)
                    write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                    write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
                else:
                    print('请确认第%d行parameters' % row)
            else:  # 参数中不包含&，只有一个参数
                url_new = url.format(data)
                print("request   url:", url_new)
                response = requests.get(url=url_new, headers=headers)
                print("response data:", response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    # GET 请求参数写在URL中，直接发送请求
    else:
        if case_detail == '刷新令牌':
            res = requests.post(url=Dw_MySQL_CONFIG["URL"], headers=Dw_MySQL_CONFIG["HEADERS"],
                                data=Dw_MySQL_CONFIG["DATA"])
            login_info = dict_res(res.text)
            token = login_info["content"]["accessToken"]
            new_url = url.format(token)
            #print("request   url:", new_url)
            response = requests.get(url=new_url,headers=headers)
            #print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        else:
            print('开始执行：', case_detail)
            print("request   url:", url)
            response = requests.get(url=url, headers=headers)
            print("response data:", response.status_code, response.text)
            clean_vaule(table_sheet_name, row, column)
            write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
            write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)


# PUT请求
def put_request_result_check(url, host, row, data, table_sheet_name, column, headers):
    case_detail = case_table_sheet.cell(row=row, column=2).value
    if data and isinstance(data, str):
    #if data:
        if '&' in str(data):
            # 分隔参数
            parameters = data.split('&')
            # 拼接URL
            new_url = url.format(parameters[0])
            #print("request   url:", url)
            # print(parameters)
            # 发送的参数体
            parameters_data = parameters[-1]
            if parameters_data.startswith('{'):
                response = requests.put(url=new_url, headers=headers, json=dict_res(parameters_data))
                #print("response data:", response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column+4, response.text)
            else:
                print('请确认第%d行parameters中需要update的值格式，应为id&{data}' % row)
        else:
            if data.startswith('select id'):
                result = ms.ExecuQuery(data)
                new_data = result[0]["id"]
                #print(new_data, type(new_data))
                new_url = url.format(new_data)
                #print("request   url:",  new_url)
                response = requests.put(url=new_url, headers=headers)
                #print("response data:", response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
            elif data.startswith('{') and data.endswith('}'):
                print('开始执行：', case_detail)
                print("request   url:", url)
                response = requests.put(url=url, headers=headers, data=data.encode('utf-8'))
                print("response data:", response.status_code, response.text)
                #print(response.url, response.content)
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
            elif data.startswith('[') and data.endswith(']'):
                pass
            elif case_detail == '新增主题域':
                print('开始执行：', case_detail)
                new_data, business_id = add_subject_data(data)
                new_url = url.format(business_id)
                print("request   url:", new_url)
                new_data = json.dumps(new_data, separators=(',', ':'))
                response = requests.put(url=new_url, headers=headers, data=new_data)
                print("response data:", response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '新增项目':
                print('开始执行：', case_detail)
                new_data, business_id = add_projects_data(data)
                new_url = url.format(business_id)
                print("request   url:", new_url)
                new_data = json.dumps(new_data, separators=(',', ':'))
                response = requests.put(url=new_url, headers=headers, data=new_data)
                print("response data:", response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            elif case_detail == '新增标签组':
                print('开始执行：', case_detail)
                new_data = add_taggroup_data(data)
                new_data = json.dumps(new_data, separators=(',', ':'))
                response = requests.put(url=url, headers=headers, data=new_data)
                print("response data:", response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
            else:
                new_url = url.format(data)
                print("request   url:",  new_url)
                response = requests.put(url=new_url, headers=headers)
                print("response data:", response.status_code, response.text)
                clean_vaule(table_sheet_name, row, column)
                write_result(table_sheet_name, row, column, response.status_code)
                write_result(table_sheet_name, row, column + 4, response.text)
    else:
        print('第%s行的参数为空或格式异常' % row)

def delete_request_result_check(url, host, data, table_sheet_name, row, column, headers):
    case_detail = case_table_sheet.cell(row=row, column=2).value
    if isinstance(data, str):
        print('开始执行：', case_detail)
        if case_detail == '':
            pass
        else:
            if data.startswith('select id'):  # sql语句的查询结果当做参数
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                #print(data_select_result)
                #print(type(data_select_result))
                datas = []
                if data_select_result:
                    try:
                        for i in range(len(data_select_result)):
                            datas.append(data_select_result[i]["id"])
                    except:
                        print('请确认第%d行SQL语句' % row)
                    else:
                        if len(datas) == 1:
                            #print(datas)
                            new_url = url.format(datas[0])
                            #print("request   url:", new_url)
                            response = requests.delete(url=new_url, headers=headers)
                            #print("response data:", response.status_code, response.text)
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
                #print("request   url:", new_url)
                response = requests.delete(url=new_url, headers=headers)
                #print("response data:", response.status_code, response.text)
                # 将返回的status_code和response.text分别写入第10列和第14列
                clean_vaule(table_sheet_name, row, column)
                write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
                write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
    else:
        print('开始执行：', case_detail)
        new_url = url.format(data)
        print("request   url:", new_url)
        response = requests.delete(url=new_url, headers=headers)
        print("response data:", response.status_code, response.text)
        # 将返回的status_code和response.text分别写入第10列和第14列
        clean_vaule(table_sheet_name, row, column)
        write_result(sheet=table_sheet_name, row=row, column=column, value=response.status_code)
        write_result(sheet=table_sheet_name, row=row, column=column + 4, value=response.text)
        # print(data)
        # print(type(data))


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

