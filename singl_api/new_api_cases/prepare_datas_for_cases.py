# coding:utf-8
import os
import time
from urllib import parse
import requests
from basic_info.get_auth_token import get_headers, get_headers_admin, get_headers_customer
from util.format_res import dict_res
from basic_info.setting import Dsp_MySQL_CONFIG, MySQL_CONFIG
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

def resource_data_push_event_hdfs_txt(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select name,id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        #print(sql)
        print('resource_id-name:', flow_info[0]["id"], flow_info[0]["name"])
    except:
        return "725053770861379584"
    new_data = {"name":"test_hdfs_txt_event","description":"","dataResId":flow_info[0]["id"],"dataResName":flow_info[0]["name"],"serviceMode":0,"transferType":1,"custTableName":"","custDataSourceId":"715969033517662208","custDataSourceName":"test_hdfs_text0529","otherConfiguration":{"scheduleType":"event","cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
    #print(new_data)
    return new_data

def resource_data_push_once_mysql(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select name,id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        #print(sql)
        print('resource_id-name:', flow_info[0]["id"], flow_info[0]["name"])
    except:
        return "725053770861379584"
    new_data = {"name":"test_once_mysql","description":"","dataResId":flow_info[0]["id"],"dataResName":flow_info[0]["name"],"serviceMode":1,"transferType":1,"custTableName":"student_2020","custDataSourceId":"715961791531712512","custDataSourceName":"test_mysql","otherConfiguration":{"scheduleType":"once","cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
    #print(new_data)
    return new_data

def resource_data_push_once_hdfs_csv(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select name,id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('resource_id-name:', flow_info[0]["id"], flow_info[0]["name"])
    except:
        return "725053770861379584"
    new_data = {"name":"test_once_hdfs_csv","description":"","dataResId":flow_info[0]["id"],"dataResName":flow_info[0]["name"],"serviceMode":1,"transferType":0,"custTableName":"","custDataSourceId":"715880057666535424","custDataSourceName":"test_hdfs_csv","otherConfiguration":{"scheduleType":"once","cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
    #print(new_data)
    return new_data

def push_resource_data_open(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select name,id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('resource_id-name:', flow_info[0]["id"], flow_info[0]["name"])
    except :
        return "725053770861379584"
    new_data = {"name": flow_info[0]["name"], "id": flow_info[0]["id"], "isPull":0,"isPush":1,"pullServiceMode":[],"pushServiceMode":["1","0"],"expiredTime":"","openStatus":1,"description":""}
    #print(new_data)
    return new_data

def pull_resource_data_open(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select name,id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('resource_id-name:', flow_info[0]["id"], flow_info[0]["name"])
    except :
        return "722830072351817728"
    if 'sink_es' in data:
        new_data = {"name": flow_info[0]["name"], "id": flow_info[0]["id"],"isPull": 1, "isPush": 0, "pullServiceMode": ["2"], "pushServiceMode": [], "expiredTime":"","openStatus":1,"description":""}
        #print(new_data)
        return new_data
    elif 'snow_dataset_dsp' in data:
        new_data = {"name":flow_info[0]["name"], "id": flow_info[0]["id"], "isPull": 1, "isPush": 0, "pullServiceMode": ["2"], "pushServiceMode": [], "expiredTime":"","openStatus":1,"description":""}
        #print(new_data)
        return new_data
    else:
        return
def resource_data_pull_es(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select name,id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('resource_id-name:', flow_info[0]["id"], flow_info[0]["name"])
    except:
        return "725053770861379584"
    if 'sink_es' in data:
        new_data = {"custAppId":"715879478366044160","custAppName":"192.168.2.142","name":"sink_es","description":"","serviceMode":2,"transferType":0,"dataResId": flow_info[0]["id"],"dataResName":flow_info[0]["name"],"fieldMappings":[{"index":0,"sourceField":"SALARY","sourceType":"bigint","targetField":"SALARY","targetType":"bigint","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"GENDER","sourceType":"string","targetField":"GENDER","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"TIME","sourceType":"date","targetField":"TIME","targetType":"date","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"JOB","sourceType":"string","targetField":"JOB","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"NAME","sourceType":"string","targetField":"NAME","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"AGE","sourceType":"bigint","targetField":"AGE","targetType":"bigint","encrypt":"","transformRule":{"type":"","expression":""}}]}
        return new_data
    elif 'snow_dataset_dsp' in data:
        new_data = {"custAppId":"715879478366044160","custAppName":"192.168.2.142","name":"snow_dataset_dsp_","description":"","serviceMode":2,"transferType":0,"dataResId":flow_info[0]["id"],"dataResName":flow_info[0]["name"],"fieldMappings":[{"index":0,"sourceField":"Div5AirportID","sourceType":"int","targetField":"Div5AirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginAirportSeqID","sourceType":"int","targetField":"OriginAirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1AirportID","sourceType":"int","targetField":"Div1AirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"CRSElapsedTime","sourceType":"int","targetField":"CRSElapsedTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestCityMarketID","sourceType":"int","targetField":"DestCityMarketID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4TailNum","sourceType":"string","targetField":"Div4TailNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3WheelsOff","sourceType":"string","targetField":"Div3WheelsOff","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"TotalAddGTime","sourceType":"string","targetField":"TotalAddGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestState","sourceType":"string","targetField":"DestState","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginCityName","sourceType":"string","targetField":"OriginCityName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestStateFips","sourceType":"string","targetField":"DestStateFips","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5TailNum","sourceType":"string","targetField":"Div5TailNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"WheelsOn","sourceType":"int","targetField":"WheelsOn","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"FirstDepTime","sourceType":"string","targetField":"FirstDepTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4WheelsOn","sourceType":"string","targetField":"Div4WheelsOn","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3WheelsOn","sourceType":"string","targetField":"Div3WheelsOn","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Diverted","sourceType":"int","targetField":"Diverted","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ArrDelay","sourceType":"int","targetField":"ArrDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginAirportID","sourceType":"int","targetField":"OriginAirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"TailNum","sourceType":"string","targetField":"TailNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"CRSDepTime","sourceType":"int","targetField":"CRSDepTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"FlightDate","sourceType":"date","targetField":"FlightDate","targetType":"date","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"LateAircraftDelay","sourceType":"int","targetField":"LateAircraftDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1LongestGTime","sourceType":"string","targetField":"Div1LongestGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2AirportID","sourceType":"int","targetField":"Div2AirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DayOfWeek","sourceType":"int","targetField":"DayOfWeek","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"NASDelay","sourceType":"int","targetField":"NASDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4AirportID","sourceType":"int","targetField":"Div4AirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2WheelsOff","sourceType":"string","targetField":"Div2WheelsOff","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestCityName","sourceType":"string","targetField":"DestCityName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginStateFips","sourceType":"string","targetField":"OriginStateFips","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3AirportSeqID","sourceType":"int","targetField":"Div3AirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4WheelsOff","sourceType":"string","targetField":"Div4WheelsOff","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"LongestAddGTime","sourceType":"string","targetField":"LongestAddGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"TaxiOut","sourceType":"int","targetField":"TaxiOut","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ActualElapsedTime","sourceType":"int","targetField":"ActualElapsedTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DepDelay","sourceType":"int","targetField":"DepDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4AirportSeqID","sourceType":"int","targetField":"Div4AirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Month","sourceType":"int","targetField":"Month","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DepartureDelayGroups","sourceType":"string","targetField":"DepartureDelayGroups","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5TotalGTime","sourceType":"string","targetField":"Div5TotalGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4Airport","sourceType":"string","targetField":"Div4Airport","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginWac","sourceType":"int","targetField":"OriginWac","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DistanceGroup","sourceType":"int","targetField":"DistanceGroup","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginCityMarketID","sourceType":"int","targetField":"OriginCityMarketID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ArrTimeBlk","sourceType":"string","targetField":"ArrTimeBlk","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"CancellationCode","sourceType":"string","targetField":"CancellationCode","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2TailNum","sourceType":"string","targetField":"Div2TailNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DepTime","sourceType":"int","targetField":"DepTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ArrTime","sourceType":"int","targetField":"ArrTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"WeatherDelay","sourceType":"int","targetField":"WeatherDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5LongestGTime","sourceType":"string","targetField":"Div5LongestGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DepDel15","sourceType":"int","targetField":"DepDel15","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2Airport","sourceType":"string","targetField":"Div2Airport","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginState","sourceType":"string","targetField":"OriginState","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1WheelsOff","sourceType":"string","targetField":"Div1WheelsOff","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3AirportID","sourceType":"int","targetField":"Div3AirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1TotalGTime","sourceType":"string","targetField":"Div1TotalGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DivActualElapsedTime","sourceType":"string","targetField":"DivActualElapsedTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ArrDelayMinutes","sourceType":"int","targetField":"ArrDelayMinutes","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4TotalGTime","sourceType":"string","targetField":"Div4TotalGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DivArrDelay","sourceType":"string","targetField":"DivArrDelay","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DivReachedDest","sourceType":"string","targetField":"DivReachedDest","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1Airport","sourceType":"string","targetField":"Div1Airport","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"CarrierDelay","sourceType":"int","targetField":"CarrierDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Carrier","sourceType":"string","targetField":"Carrier","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DepTimeBlk","sourceType":"string","targetField":"DepTimeBlk","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Dest","sourceType":"string","targetField":"Dest","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2AirportSeqID","sourceType":"int","targetField":"Div2AirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"TaxiIn","sourceType":"int","targetField":"TaxiIn","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DivAirportLandings","sourceType":"string","targetField":"DivAirportLandings","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DivDistance","sourceType":"string","targetField":"DivDistance","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestAirportSeqID","sourceType":"int","targetField":"DestAirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginStateName","sourceType":"string","targetField":"OriginStateName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Distance","sourceType":"int","targetField":"Distance","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Year","sourceType":"int","targetField":"Year","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5WheelsOff","sourceType":"string","targetField":"Div5WheelsOff","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"CRSArrTime","sourceType":"int","targetField":"CRSArrTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DepDelayMinutes","sourceType":"int","targetField":"DepDelayMinutes","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2WheelsOn","sourceType":"string","targetField":"Div2WheelsOn","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestWac","sourceType":"int","targetField":"DestWac","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1WheelsOn","sourceType":"string","targetField":"Div1WheelsOn","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4LongestGTime","sourceType":"string","targetField":"Div4LongestGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ArrivalDelayGroups","sourceType":"int","targetField":"ArrivalDelayGroups","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3TotalGTime","sourceType":"string","targetField":"Div3TotalGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2LongestGTime","sourceType":"string","targetField":"Div2LongestGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"UniqueCarrier","sourceType":"string","targetField":"UniqueCarrier","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"FlightNum","sourceType":"string","targetField":"FlightNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Flights","sourceType":"int","targetField":"Flights","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DayOfMonth","sourceType":"int","targetField":"DayOfMonth","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ArrDel15","sourceType":"int","targetField":"ArrDel15","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5AirportSeqID","sourceType":"int","targetField":"Div5AirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1TailNum","sourceType":"string","targetField":"Div1TailNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5Airport","sourceType":"string","targetField":"Div5Airport","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5WheelsOn","sourceType":"string","targetField":"Div5WheelsOn","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"AirlineID","sourceType":"int","targetField":"AirlineID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"AirTime","sourceType":"int","targetField":"AirTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestAirportID","sourceType":"int","targetField":"DestAirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"WheelsOff","sourceType":"int","targetField":"WheelsOff","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Cancelled","sourceType":"int","targetField":"Cancelled","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2TotalGTime","sourceType":"string","targetField":"Div2TotalGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestStateName","sourceType":"string","targetField":"DestStateName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Origin","sourceType":"string","targetField":"Origin","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3Airport","sourceType":"string","targetField":"Div3Airport","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3LongestGTime","sourceType":"string","targetField":"Div3LongestGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Quarter","sourceType":"int","targetField":"Quarter","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1AirportSeqID","sourceType":"int","targetField":"Div1AirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"SecurityDelay","sourceType":"int","targetField":"SecurityDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3TailNum","sourceType":"string","targetField":"Div3TailNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}]}
        return new_data
    else:
        return

def resource_data(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        #print(sql)
        print('resource_id:', flow_info[0]["id"])
    except:
        return
    new_data = {"name": "test_hdfs_student2020","datasetName":"test_hdfs_student2020","storage":"HDFS","encoder":"UTF-8","incrementField":"age","openStatus":1,"categoryId":"0","datasetId":"82e50c27-8b4a-440d-8807-eb970e6a7571","expiredTime":0,"type":0,"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"id":flow_info[0]["id"]}
    #print(new_data)
    return new_data

def appconfig_data(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('dsp_dc_appconfig:', flow_info[0]["id"])
    except:
        return
    new_data = {"accessIp":["192.168.2.142"],"name":"autotest_appconfig_随机数","enabled":1,"tenantId":"e5188f23-d472-4b2d-9cfa-97a0d65994cf","owner":"b398ff8e-8a90-436d-adc6-08ee08b42958","creator":"customer3","createTime":"2020-06-28 14:11:07","lastModifier":"customer3","lastModifiedTime":"2020-06-28 14:22:00","description":"","id":flow_info[0]["id"],"custId":"b398ff8e-8a90-436d-adc6-08ee08b42958","custName":"customer3","accessKey":"015a2147-34c4-4456-ad6d-a94b513ad6e0","publicKey":"MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAKvJ3JxEUIYtnPDK3Jcn+naxiDgEzCUWeUnM56dInCVTduhBuBPbkmi7Oor+4dZ/eF5+q/h7Ay/o1WHuFbwf6NUCAwEAAQ=="}
    #print(new_data)
    return new_data
def cust_data_source(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_cust_data_source where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('dsp_cust_data_source:', flow_info[0]["id"])
    except:
        return
    new_data = {"id":flow_info[0]["id"],"name":"autotest_hdfs_csv_随机数","type":"HDFS","description":"autotest_hdfs_csv_随机数","attributes":{"quoteChar":"\"","escapeChar":"\\","path":"/auto_test/out89","format":"csv","chineseName":"autotest_hdfs_csv","header":"false","separator":",","properties":[{"name":"","value":""}],"ignoreRow":0},"owner":"b398ff8e-8a90-436d-adc6-08ee08b42958","enabled":1,"tenantId":"e5188f23-d472-4b2d-9cfa-97a0d65994cf","creator":"customer3","createTime":"2020-06-24 19:25:05","lastModifier":"customer3","lastModifiedTime":"2020-06-24 19:25:05"}
    #print(new_data)
    return new_data

def corn_application_oracle(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select name,id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('resource_id-name:', flow_info[0]["id"], flow_info[0]["name"])
    except:
        return "725053770861379584"
    new_data = {"name":"test_cron_oracle","description":"","dataResId":flow_info[0]["id"],"dataResName":flow_info[0]["name"],"serviceMode":1,"transferType":1,"custTableName":"autotest_student","custDataSourceId":"717026599119093760","custDataSourceName":"test_oracle","otherConfiguration":{"scheduleType":"cron","cron":"0 0/5 * * * ? ","startTime": get_now(), "endTime": get_tomorrow()},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
    print(new_data)
    return new_data


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
def pull_data(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        #print(sql)
        print('service_id:', flow_info[0]["id"])
    except:
        return
    if 'snow_dataset_dsp' in data:
        new_data = {
            "accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1",
            "dataServiceId": flow_info[0]["id"],
            "timestamp": "1594026624140",
            "encrypted": "false",
            "query": {
                "fieldGroup": {
                    "index": 0,
                    "group": "false",
                    "andOr": "AND",
                    "fields": [{
                        "index": 1,
                        "group": "false",
                        "andOr": "AND",
                        "name": "DestCityName",
                        "oper": "LIKE",
                        "value": [
                            '%CA%'
                        ]
                    },
                        {
                            "index": 2,
                            "group": "false",
                            "andOr": "OR",
                            "name": "FlightNum",
                            "oper": "NOT_EQUAL",
                            "value": [
                                '108'
                            ]
                        }],
                    "fieldGroups": []
                },
                "ordSort": [
                    {
                        "name": "FlightNum",
                        "order": "DESC"
                    }],
                "pageable": {
                    "pageable": "true",
                    "pageNum": 1,
                    "pageSize": 91,
                    "offset": "0"
                },
                "selectProperties": [
                    "FlightNum",
                    "TailNum",
                    "OriginState",
                    "DestCityName"
                ]
            }
        }
        return new_data
    elif 'sink_es' in data:
        new_data = {
            "accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1",
            "dataServiceId": flow_info[0]["id"],
            "timestamp": "1594026624140",
            "encrypted": "false",
            "query": {
                "fieldGroup": {
                    "index": 0,
                    "group": "false",
                    "andOr": "AND",
                    "fields": [

                        {
                            "index": 1,
                            "group": "false",
                            "andOr": "AND",
                            "name": "AGE",
                            "oper": "IN",
                            "value": [
                                2
                            ]
                        }],
                    "fieldGroups": []
                },
                "pageable": {
                    "pageable": "true",
                    "pageNum": 1,
                    "pageSize": 91,
                    "offset": "0"
                },
                "selectProperties": [
                    "TIME",
                    "NAME",
                    "AGE",
                    "GENDER"
                ]
            }
        }
        return new_data
    else:
        return

def pull_Aggs(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        #print(sql)
        print('service_id:', flow_info[0]["id"])
    except:
        return
    if 'sink_es' in data:
        new_data = {
            "accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1",
            "dataServiceId": flow_info[0]["id"],
            "encrypted": "false",
            "query": {
                "aggFields": [{
                    "aggType": "AVG",
                    "alias": "AVG_DestWac",
                    "name": "AGE"
                },
                    {
                        "aggType": "SUM",
                        "alias": "SUM_DestWac",
                        "name": "AGE"
                    },
                    {
                        "aggType": "MIN",
                        "alias": "MIN_DestWac",
                        "name": "AGE"
                    },
                    {
                        "aggType": "MAX",
                        "alias": "MAX_DestWac",
                        "name": "AGE"
                    },
                    {
                        "aggType": "COUNT",
                        "alias": "COUNT_DestWac",
                        "name": "AGE"
                    }
                ],
                "fieldGroup": {
                    "index": 0,
                    "group": "false",
                    "andOr": "AND",
                    "fields": [{
                        "index": 1,
                        "andOr": "AND",
                        "name": "GENDER",
                        "oper": "EQUAL",
                        "value": [
                            "GENDER"
                        ]
                    }]
                },
                "groupFields": [
                    "NAME"
                ],
                "havingFieldGroup": {
                    "index": 0,
                    "andOr": "AND",
                    "fields": [{
                        "index": 0,
                        "andOr": "AND",
                        "name": "MAX_DestWac",
                        "oper": "BETWEEN",
                        "value": [
                            0, 1000000
                        ]
                    }]
                },
                "ordSort": [{
                    "name": "MIN_DestWac"
                }],
                "pageable": {
                    "pageNum": 1,
                    "pageSize": 500,
                    "pageable": "true"
                }
            },
            "timestamp": 1
        }
        return new_data
    elif 'snow_dataset_dsp' in data:
        new_data = {
            "accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1",
            "dataServiceId": flow_info[0]["id"],
            "encrypted": "false",
            "query": {
                "aggFields": [{
                    "aggType": "AVG",
                    "alias": "AVG_DestWac",
                    "distinct": "false",
                    "name": "DestWac"
                },
                    {
                        "aggType": "SUM",
                        "alias": "SUM_DestWac",
                        "name": "DestWac"
                    },
                    {
                        "aggType": "MIN",
                        "alias": "MIN_DestWac",
                        "distinct": "false",
                        "name": "DestWac"
                    },
                    {
                        "aggType": "MAX",
                        "alias": "MAX_DestWac",
                        "name": "DestWac"
                    },
                    {
                        "aggType": "COUNT",
                        "alias": "COUNT_DestWac",
                        "distinct": "false",
                        "name": "DestWac"
                    }
                ],
                "fieldGroup": {
                    "index": 0,
                    "group": "false",
                    "andOr": "AND",
                    "fields": [{
                        "index": 0,
                        "andOr": "AND",
                        "name": "OriginCityName",
                        "oper": "NOT_LIKE",
                        "value": [
                            "%CA%"
                        ]
                    },
                        {
                            "index": 1,
                            "andOr": "AND",
                            "name": "OriginCityName",
                            "oper": "NOT_LIKE",
                            "value": [
                                "%CA%"
                            ]
                        }]
                },
                "groupFields": [
                    "FlightNum",
                    "Year",
                    "Month",
                    "FlightDate"
                ],
                "havingFieldGroup": {
                    "index": 0,
                    "andOr": "AND",
                    "fields": [{
                        "index": 0,
                        "andOr": "AND",
                        "name": "MAX_DestWac",
                        "oper": "BETWEEN",
                        "value": [
                            0, 10000
                        ]
                    }]
                },
                "ordSort": [{
                    "name": "MIN_DestWac"
                },
                    {
                        "name": "COUNT_DestWac",
                        "order": "DESC"
                    },
                    {
                        "name": "FlightNum",
                        "order": "ASC"
                    },
                    {
                        "name": "FlightDate",
                        "order": "ASC"
                    }],
                "pageable": {
                    "pageNum": 1,
                    "pageSize": 500,
                    "pageable": "true"
                }
            },
            "timestamp": 1
        }
        return new_data
    else:
        return

def application_pull_approval(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        #print(sql)
        print('dsp_data_application:', flow_info[0]["id"])

        if 'sink_es' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name": "sink_es", "status": 0, "type": 0, "auditMind": "yyy"}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name":"snow_dataset_dsp_","status": 0, "type": 0, "auditMind": "yyy"}
            return new_data
        else:
            return
    except:
        return "722842814173413376"
def application_push_approval(data):
    ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
    try:
        sql = "select id from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        #print(sql)
        print('dsp_data_application:', flow_info[0]["id"])
        if 'test_once_hdfs_csv' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name":"test_once_hdfs_csv","status": 0, "type": 1, "scheduleType": "once", "serviceConfiguration":{"cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
            return new_data
        elif 'test_once_mysql' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description":"", "enabled":1, "expiredTime": "0", "name":"test_once_mysql","status": 0, "type": 1, "scheduleType": "once", "serviceConfiguration":{"cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
            return new_data
        elif 'test_hdfs_txt_event' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name":"test_hdfs_txt_event", "status": 0, "type": 1, "scheduleType": "event", "serviceConfiguration":{"cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
            return new_data
        elif 'test_cron_oracle' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name":"test_cron_oracle", "status": 0, "type": 1, "scheduleType": "cron", "serviceConfiguration":{"cron":"0 0/5 * * * ? ","startTime": get_now(), "endTime": get_tomorrow()}, "fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"auditMind":"yyy"}
            return new_data
        else:
            return
    except:
       return "723150172099444736"
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