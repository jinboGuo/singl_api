# coding:utf-8
import os
import time
from urllib import parse
import requests
from basic_info.get_auth_token import get_headers, get_headers_admin, get_headers_customer
from util.format_res import dict_res
from basic_info.setting import MySQL_CONFIG
from util.Open_DB import MYSQL
from basic_info.setting import host
from selenium import webdriver
import random

from util.timestamp_13 import get_now, get_tomorrow

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])

ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))


def get_job_tasks_id(job_id):
    url = '%s/api/woven/collectors/%s/tasks' % (host, job_id)
    data = {"fieldList": [], "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
    response = requests.post(url=url, headers=get_headers(host), json=data)

    all_task_id = []
    try:
        tasks = dict_res(response.text)['content']
        for item in tasks:
            task_id = item['id']
            all_task_id.append(task_id)
    except Exception as e:
        print(e)
        return
    else:
        return all_task_id


def create_new_user(data):
    url = '%s/api/woven/users' % host
    response = requests.post(url=url, headers=get_headers(host), json=data)
    user_id = dict_res(response.text)["id"]
    #print(user_id)
    return user_id

def collector_schema_sync(data):
    """获取采集器元数据同步后返回的task id"""
    collector = 'c9'
    url = '%s/api/woven/collectors/%s/schema/fetch' % (host, collector)
    response = requests.post(url=url, headers=get_headers(host), data=data)
    time.sleep(3)
    #print(response.text)
    return response.text


def get_flow_id():
    name = "gbj_for_project_removeList" + str(random.randint(0,999999999999))
    data = {"name": name, "flowType": "dataflow",
            "projectEntity": {"id": "e47fe6f4-6086-49ed-81d1-68704aa82f2d"}, "steps": [], "links": []}
    url = '%s/api/flows/create' % host
    response = requests.post(url=url, headers=get_headers(), json=data)
    flow_id = dict_res(response.text)['id']
    #print(flow_id)
    return flow_id


def admin_flow_id(data):
    try:
        url = '%s/api/dsp/platform/service/infoById?id=%s' % (host, data)
        response = requests.get(url=url, headers=get_headers_admin(host))
        flow_id = dict_res(response.text)["content"]["jobInfo"]['flowId']
        print(flow_id)
        return flow_id
    except:
        return 1

def customer_flow_id(data):
    try:
        url = '%s/api/dsp/consumer/service/infoById?id=%s' % (host, data)
        response = requests.get(url=url, headers=get_headers_customer(host))
        flow_id = dict_res(response.text)["content"]["jobInfo"]['flowId']
        print(flow_id)
        return flow_id
    except:
        return 1


def get_applicationId():
    """进入yarn页面，获取状态为finished的application id"""
    # 进入yarn页面，获取状态为finished的application id
    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.implicitly_wait(10)
    # 进入ambari页面，然后进入yarn页面
    driver.get('http://192.168.1.81:8080/#/main/services/YARN/heatmaps')
    driver.find_element_by_xpath('.//div[@class="well login span4"]/input[1]').send_keys('admin')
    driver.find_element_by_xpath('.//div[@class="well login span4"]/input[2]').send_keys('admin')
    driver.find_element_by_xpath('.//div[@class="well login span4"]/button').click()
    driver.get('http://info2:8088/cluster')
    driver.get('http://info2:8088/cluster/apps/FINISHED')
    # 获取所有finished状态的application id
    all_applications = driver.find_elements_by_xpath('.//*[@id="apps"]/tbody/tr/td[1]/a')
    # 返回第一个application id，提供给case进行查询该applicationId的log
    application_id = all_applications[0].text
    time.sleep(3)
    # print(application_id)
    # print(type(application_id))
    return application_id


def get_woven_qaoutput_dataset_path():
    """查找woven/qaoutput下的所有数据集name，并组装成woven/qaoutput/datasetname的格式"""
    url = '%s/api/datasets/query' % host
    data = {"fieldList":[{"fieldName":"parentId","fieldValue":"4f4d687c-12b3-4e09-9ba9-bcf881249ea0","comparatorOperator":"EQUAL","logicalOperator":"AND"},{"fieldName":"owner","fieldValue":"2059750c-a300-4b64-84a6-e8b086dbfd42","comparatorOperator":"EQUAL","logicalOperator":"AND"}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
    response = requests.post(url=url,headers=get_headers(), json=data)
    contents = dict_res(response.text)["content"]
    path = []
    for content in contents:
        content_path = 'woven/qaoutput/' + content["name"]
        content_path.replace('/', '%252F')   # 应该使用parse.quote() 进行URL编码进行处理。稍后解决
        path.append(content_path.replace('/', '%252F'))
    # print(path)
    return path

dir1 = ab_dir('woven-common-3.0.jar')


def upload_jar_file_filter():
    url = "%s/api/processconfigs/uploadjar/filter class" % host
    # files = {"file": open('./new_api_cases/woven-common-3.0.jar', 'rb')}
    files = {"file": open(dir1, 'rb')}
    headers = get_headers(host)
    headers.pop('Content-Type')
    try:
        response = requests.post(url, files=files, headers=headers)
    # print(response.text)
        filter_fileName = dict_res(response.text)["fileName"]
    except:
        return
    else:
        return filter_fileName


def upload_jar_file_workflow():
    url = "%s/api/processconfigs/uploadjar/workflow selector" % host
    print(url)
    # files = {"file": open('./new_api_cases/woven-common-3.0.jar', 'rb')}
    files = {"file": open(dir1, 'rb')}
    headers = get_headers(host)
    headers.pop('Content-Type')
    try:
        response = requests.post(url, files=files, headers=headers)
        print(response.text)
        workflow_fileName = dict_res(response.text)["fileName"]
        print(workflow_fileName)
    except:
        return
    else:
        return workflow_fileName


def upload_jar_file_dataflow():
    url = "%s/api/processconfigs/uploadjar/dataflow selector" % host
    unquote_url = parse.unquote(url)
    # files = {"file": open('./new_api_cases/woven-common-3.0.jar', 'rb')}
    files = {"file": open(dir1, 'rb')}
    headers = get_headers(host)
    headers.pop('Content-Type')
    try:
        response = requests.post(url, files=files, headers=headers)
    # print(response.text)
        data_fileName = dict_res(response.text)["fileName"]
        print(data_fileName)
    except:
        return
    else:
        return data_fileName


def upload_file_standard(host,file,url):
    dir2 = ab_dir(file)
    # url = "%s/api/woven/upload/read/excel?maxSheet=1&maxRow=10000&maxColumn=3" % host
    unquote_url = parse.unquote(url)
    files = {"file": open(dir2, 'rb')}
    headers = get_headers(host)
    headers.pop('Content-Type')
    try:
        response = requests.post(url, files=files, headers=headers)
    except:
        return
    else:
        return response.status_code, response.text