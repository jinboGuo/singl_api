# coding:utf-8
import os
import requests
from basic_info.get_auth_token import get_headers_admin, get_headers_customer
from new_api_cases.dw_deal_parameters import deal_random
from util.format_res import dict_res
from basic_info.setting import Dsp_MySQL_CONFIG
from util.Open_DB import MYSQL
from basic_info.setting import host

from util.timestamp_13 import get_now, get_tomorrow, data_now

ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))

def resource_data_push(data):
    try:
        data = data.split("&")
        sql = "select name,id from dsp_data_resource where name like '%%%%%s%%%%' ORDER BY create_time desc limit 1" % data[0]
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('resource_id-name:', resource_info[0]["id"], resource_info[0]["name"])
        dss_sql = "select name,id from dsp_cust_data_source where name like '%%%%%s%%%%' ORDER BY create_time desc limit 1" % data[1]
        dss_info = ms.ExecuQuery(dss_sql.encode('utf-8'))
        print('cust_data_source_id-name:', dss_info[0]["id"], dss_info[0]["name"])
        if 'test_hdfs_csv' in data:
            new_data = {"name": "test_once_hdfs_csv","description":"","dataResId": resource_info[0]["id"],"dataResName":resource_info[0]["name"], "serviceMode":1,"transferType":0,"custTableName":"", "custDataSourceId": dss_info[0]["id"],"custDataSourceName":dss_info[0]["name"], "otherConfiguration": {"scheduleType": "once","cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
            return new_data
        elif 'test_mysql' in data:
            new_data = {"name": "test_once_mysql", "description":"", "dataResId": resource_info[0]["id"], "dataResName": resource_info[0]["name"], "serviceMode":1,"transferType":1,"custTableName": "student_2020", "custDataSourceId": dss_info[0]["id"],"custDataSourceName": dss_info[0]["name"], "otherConfiguration": {"scheduleType":"once","cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
            return new_data
        elif 'test_hdfs_txt' in data:
            new_data = {"name": "test_hdfs_txt_event", "description":"", "dataResId": resource_info[0]["id"], "dataResName":resource_info[0]["name"], "serviceMode":0,"transferType":1,"custTableName": "", "custDataSourceId": dss_info[0]["id"], "custDataSourceName": dss_info[0]["name"], "otherConfiguration": {"scheduleType": "event","cron":"0 * * * * ? ","startTime":"","endTime":""},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
            return new_data
        elif 'test_oracle' in data:
            new_data = {"name": "test_cron_oracle", "description":"", "dataResId": resource_info[0]["id"], "dataResName": resource_info[0]["name"], "serviceMode":1,"transferType":1,"custTableName": "autotest_student", "custDataSourceId": dss_info[0]["id"], "custDataSourceName": dss_info[0]["name"], "otherConfiguration": {"scheduleType":"cron","cron":"0 0/5 * * * ? ","startTime": get_now(), "endTime": get_tomorrow()},"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}]}
            return new_data
        else:
            return
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def push_resource_data_open(data):
    try:
        sql = "select name,id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('resource_id-name:', resource_info[0]["id"], resource_info[0]["name"])
        new_data = {"name": resource_info[0]["name"], "id": resource_info[0]["id"], "isPull":0,"isPush":1,"pullServiceMode":[],"pushServiceMode":["1","0"],"expiredTime":"","openStatus":1,"description":""}
        return new_data
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def pull_resource_data_open(data):
    try:
        sql = "select name,id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('resource_id-name:', resource_info[0]["id"], resource_info[0]["name"])
        if 'gjb_sink_es' in data:
            new_data = {"name": resource_info[0]["name"], "id": resource_info[0]["id"],"isPull": 1, "isPush": 0, "pullServiceMode": ["2"], "pushServiceMode": [], "expiredTime":"","openStatus":1,"description":""}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"name": resource_info[0]["name"], "id": resource_info[0]["id"], "isPull": 1, "isPush": 0, "pullServiceMode": ["2"], "pushServiceMode": [], "expiredTime":"","openStatus":1,"description":""}
            return new_data
        elif 'api_datasource' in data:
            new_data = {"name": resource_info[0]["name"], "id": resource_info[0]["id"], "isPull": 1, "isPush": 0, "pullServiceMode": ["2"], "pushServiceMode": [], "expiredTime":"","openStatus":1,"description":""}
            return new_data
        elif 'api_mysqldataset' in data:
            new_data = {"name": resource_info[0]["name"], "id": resource_info[0]["id"], "isPull": 1, "isPush": 0, "pullServiceMode": ["2"], "pushServiceMode": [], "expiredTime":"","openStatus":1,"description":""}
            return new_data
        else:
            return
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def resource_data_pull_es(data):
    try:
        data = data.split("&")
        sql = "select name,id from dsp_data_resource where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('resource_id-name:', resource_info[0]["id"], resource_info[0]["name"])
        app_sql = "select name,id from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[1]
        appconfig_info = ms.ExecuQuery(app_sql.encode('utf-8'))
        print('appconfig_info_id-name:', appconfig_info[0]["id"], appconfig_info[0]["name"])
        if 'gjb_sink_es' in data:
            new_data = {"custAppId": appconfig_info[0]["id"], "custAppName":appconfig_info[0]["name"],"name":"gjb_sink_es", "description": "", "serviceMode": 2, "transferType": 0, "dataResId": resource_info[0]["id"], "dataResName":resource_info[0]["name"], "fieldMappings": [{"index": 0, "sourceField":"SALARY","sourceType":"bigint","targetField":"SALARY","targetType":"bigint","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"GENDER","sourceType":"string","targetField":"GENDER","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"TIME","sourceType":"date","targetField":"TIME","targetType":"date","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"JOB","sourceType":"string","targetField":"JOB","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"NAME","sourceType":"string","targetField":"NAME","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"AGE","sourceType":"bigint","targetField":"AGE","targetType":"bigint","encrypt":"","transformRule":{"type":"","expression":""}}]}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"custAppId": appconfig_info[0]["id"], "custAppName": appconfig_info[0]["name"], "name": "snow_dataset_dsp_", "description":"", "serviceMode": 2, "transferType": 0, "dataResId": resource_info[0]["id"],"dataResName":resource_info[0]["name"],"fieldMappings": [{"index": 0, "sourceField":"Div5AirportID","sourceType":"int","targetField":"Div5AirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginAirportSeqID","sourceType":"int","targetField":"OriginAirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1AirportID","sourceType":"int","targetField":"Div1AirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"CRSElapsedTime","sourceType":"int","targetField":"CRSElapsedTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestCityMarketID","sourceType":"int","targetField":"DestCityMarketID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4TailNum","sourceType":"string","targetField":"Div4TailNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3WheelsOff","sourceType":"string","targetField":"Div3WheelsOff","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"TotalAddGTime","sourceType":"string","targetField":"TotalAddGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestState","sourceType":"string","targetField":"DestState","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginCityName","sourceType":"string","targetField":"OriginCityName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestStateFips","sourceType":"string","targetField":"DestStateFips","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5TailNum","sourceType":"string","targetField":"Div5TailNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"WheelsOn","sourceType":"int","targetField":"WheelsOn","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"FirstDepTime","sourceType":"string","targetField":"FirstDepTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4WheelsOn","sourceType":"string","targetField":"Div4WheelsOn","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3WheelsOn","sourceType":"string","targetField":"Div3WheelsOn","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Diverted","sourceType":"int","targetField":"Diverted","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ArrDelay","sourceType":"int","targetField":"ArrDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginAirportID","sourceType":"int","targetField":"OriginAirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"TailNum","sourceType":"string","targetField":"TailNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"CRSDepTime","sourceType":"int","targetField":"CRSDepTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"FlightDate","sourceType":"date","targetField":"FlightDate","targetType":"date","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"LateAircraftDelay","sourceType":"int","targetField":"LateAircraftDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1LongestGTime","sourceType":"string","targetField":"Div1LongestGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2AirportID","sourceType":"int","targetField":"Div2AirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DayOfWeek","sourceType":"int","targetField":"DayOfWeek","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"NASDelay","sourceType":"int","targetField":"NASDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4AirportID","sourceType":"int","targetField":"Div4AirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2WheelsOff","sourceType":"string","targetField":"Div2WheelsOff","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestCityName","sourceType":"string","targetField":"DestCityName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginStateFips","sourceType":"string","targetField":"OriginStateFips","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3AirportSeqID","sourceType":"int","targetField":"Div3AirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4WheelsOff","sourceType":"string","targetField":"Div4WheelsOff","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"LongestAddGTime","sourceType":"string","targetField":"LongestAddGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"TaxiOut","sourceType":"int","targetField":"TaxiOut","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ActualElapsedTime","sourceType":"int","targetField":"ActualElapsedTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DepDelay","sourceType":"int","targetField":"DepDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4AirportSeqID","sourceType":"int","targetField":"Div4AirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Month","sourceType":"int","targetField":"Month","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DepartureDelayGroups","sourceType":"string","targetField":"DepartureDelayGroups","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5TotalGTime","sourceType":"string","targetField":"Div5TotalGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4Airport","sourceType":"string","targetField":"Div4Airport","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginWac","sourceType":"int","targetField":"OriginWac","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DistanceGroup","sourceType":"int","targetField":"DistanceGroup","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginCityMarketID","sourceType":"int","targetField":"OriginCityMarketID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ArrTimeBlk","sourceType":"string","targetField":"ArrTimeBlk","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"CancellationCode","sourceType":"string","targetField":"CancellationCode","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2TailNum","sourceType":"string","targetField":"Div2TailNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DepTime","sourceType":"int","targetField":"DepTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ArrTime","sourceType":"int","targetField":"ArrTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"WeatherDelay","sourceType":"int","targetField":"WeatherDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5LongestGTime","sourceType":"string","targetField":"Div5LongestGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DepDel15","sourceType":"int","targetField":"DepDel15","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2Airport","sourceType":"string","targetField":"Div2Airport","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginState","sourceType":"string","targetField":"OriginState","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1WheelsOff","sourceType":"string","targetField":"Div1WheelsOff","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3AirportID","sourceType":"int","targetField":"Div3AirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1TotalGTime","sourceType":"string","targetField":"Div1TotalGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DivActualElapsedTime","sourceType":"string","targetField":"DivActualElapsedTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ArrDelayMinutes","sourceType":"int","targetField":"ArrDelayMinutes","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4TotalGTime","sourceType":"string","targetField":"Div4TotalGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DivArrDelay","sourceType":"string","targetField":"DivArrDelay","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DivReachedDest","sourceType":"string","targetField":"DivReachedDest","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1Airport","sourceType":"string","targetField":"Div1Airport","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"CarrierDelay","sourceType":"int","targetField":"CarrierDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Carrier","sourceType":"string","targetField":"Carrier","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DepTimeBlk","sourceType":"string","targetField":"DepTimeBlk","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Dest","sourceType":"string","targetField":"Dest","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2AirportSeqID","sourceType":"int","targetField":"Div2AirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"TaxiIn","sourceType":"int","targetField":"TaxiIn","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DivAirportLandings","sourceType":"string","targetField":"DivAirportLandings","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DivDistance","sourceType":"string","targetField":"DivDistance","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestAirportSeqID","sourceType":"int","targetField":"DestAirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"OriginStateName","sourceType":"string","targetField":"OriginStateName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Distance","sourceType":"int","targetField":"Distance","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Year","sourceType":"int","targetField":"Year","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5WheelsOff","sourceType":"string","targetField":"Div5WheelsOff","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"CRSArrTime","sourceType":"int","targetField":"CRSArrTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DepDelayMinutes","sourceType":"int","targetField":"DepDelayMinutes","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2WheelsOn","sourceType":"string","targetField":"Div2WheelsOn","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestWac","sourceType":"int","targetField":"DestWac","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1WheelsOn","sourceType":"string","targetField":"Div1WheelsOn","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div4LongestGTime","sourceType":"string","targetField":"Div4LongestGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ArrivalDelayGroups","sourceType":"int","targetField":"ArrivalDelayGroups","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3TotalGTime","sourceType":"string","targetField":"Div3TotalGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2LongestGTime","sourceType":"string","targetField":"Div2LongestGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"UniqueCarrier","sourceType":"string","targetField":"UniqueCarrier","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"FlightNum","sourceType":"string","targetField":"FlightNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Flights","sourceType":"int","targetField":"Flights","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DayOfMonth","sourceType":"int","targetField":"DayOfMonth","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"ArrDel15","sourceType":"int","targetField":"ArrDel15","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5AirportSeqID","sourceType":"int","targetField":"Div5AirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1TailNum","sourceType":"string","targetField":"Div1TailNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5Airport","sourceType":"string","targetField":"Div5Airport","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div5WheelsOn","sourceType":"string","targetField":"Div5WheelsOn","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"AirlineID","sourceType":"int","targetField":"AirlineID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"AirTime","sourceType":"int","targetField":"AirTime","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestAirportID","sourceType":"int","targetField":"DestAirportID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"WheelsOff","sourceType":"int","targetField":"WheelsOff","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Cancelled","sourceType":"int","targetField":"Cancelled","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div2TotalGTime","sourceType":"string","targetField":"Div2TotalGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"DestStateName","sourceType":"string","targetField":"DestStateName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Origin","sourceType":"string","targetField":"Origin","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3Airport","sourceType":"string","targetField":"Div3Airport","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3LongestGTime","sourceType":"string","targetField":"Div3LongestGTime","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Quarter","sourceType":"int","targetField":"Quarter","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div1AirportSeqID","sourceType":"int","targetField":"Div1AirportSeqID","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"SecurityDelay","sourceType":"int","targetField":"SecurityDelay","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"Div3TailNum","sourceType":"string","targetField":"Div3TailNum","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}]}
            return new_data
        elif 'api_datasource' in data:
            new_data = {"custAppId": appconfig_info[0]["id"], "custAppName": appconfig_info[0]["name"], "name": "api_datasource", "description": "", "serviceMode": 2, "transferType": 0, "dataResId": resource_info[0]["id"], "dataResName": resource_info[0]["name"], "sourceType": "DATASOURCE", "fieldMappings": []}
            return new_data
        elif 'api_mysqldataset' in data:
            new_data = {"custAppId": appconfig_info[0]["id"], "custAppName": appconfig_info[0]["name"], "name": "api_mysqldataset", "description":"", "serviceMode": 2, "transferType": 0, "dataResId": resource_info[0]["id"], "dataResName": resource_info[0]["name"], "sourceType": "DATASET", "fieldMappings": [{"index":0,"sourceField":"id","sourceType":"int","targetField":"id","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"user_id","sourceType":"int","targetField":"user_id","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"number","sourceType":"string","targetField":"number","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"createtime","sourceType":"timestamp","targetField":"createtime","targetType":"timestamp","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"note","sourceType":"string","targetField":"note","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]},{"index":0,"sourceField":"dt","sourceType":"string","targetField":"dt","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""},"supportAggs":[]}]}
            return new_data
        else:
            return
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def resource_data(data):
    try:
        sql = "select id ,dataset_id, dataset_name from dsp_data_resource where name like '%s%%%%' ORDER BY create_time limit 1" % data
        resource_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('resource_id:', resource_info[0]["id"], resource_info[0]["dataset_id"], resource_info[0]["dataset_name"])
        new_data = {"name": "gjb_test_hdfs_student2020随机数", "datasetName": resource_info[0]["dataset_name"],"storage":"HDFS","encoder":"UTF-8","incrementField":"age","openStatus":1,"categoryId":"0","datasetId": resource_info[0]["dataset_id"], "expiredTime":0,"type":0,"fieldMappings":[{"index":0,"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","transformRule":{"type":"","expression":""}},{"index":0,"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","transformRule":{"type":"","expression":""}}],"id":resource_info[0]["id"]}
        from new_api_cases.dw_deal_parameters import deal_random
        deal_random(new_data)
        return new_data
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def resource_data_save(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id,name from merce_dataset where name like '%s%%%%' ORDER BY create_time limit 1" % data
        dataset_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('dataset_id-name:', dataset_info[0]["id"], dataset_info[0]["name"])
        if 'gjb_ttest_hdfs_student2020' in data:
            new_data = {"name": "gjb_ttest_hdfs_student_随机数", "sourceType": "DATASET", "datasetName": dataset_info[0]["name"],"datasetId":dataset_info[0]["id"], "categoryId": 0, "expiredTime": 0, "fieldMappings":[{"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}}],"openStatus":0,"type":0,"isIncrementField":"true","incrementField":"age","encoder":"UTF-8","timeStamp":"","storage":"HDFS","dataset":"","query":{"parameters":[],"sqlTemplate":""}}
            deal_random(new_data)
            return new_data
        elif 'gjb_sink_es' in data:
            new_data = {"name": "gjb_sink_es_随机数", "sourceType": "DATASET", "datasetName": dataset_info[0]["name"], "datasetId":dataset_info[0]["id"], "categoryId": 0, "expiredTime": 0, "fieldMappings": [{"sourceField":"sId","sourceType":"string","targetField":"sId","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sName","sourceType":"string","targetField":"sName","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"sex","sourceType":"string","targetField":"sex","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"age","sourceType":"int","targetField":"age","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"class","sourceType":"string","targetField":"class","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}}],"openStatus":0,"type":0,"isIncrementField":"false","incrementField":"","encoder":"UTF-8","timeStamp":"","storage":"ElasticSearch","dataset":"","query":{"parameters":[],"sqlTemplate":""}}
            deal_random(new_data)
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"name": "snow_dataset_dsp_随机数", "sourceType": "DATASET", "datasetName": dataset_info[0]["name"], "datasetId":dataset_info[0]["id"], "categoryId": 0, "expiredTime": 0, "fieldMappings": [{"sourceField":"Div5AirportID","sourceType":"int","targetField":"Div5AirportID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"OriginAirportSeqID","sourceType":"int","targetField":"OriginAirportSeqID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div1AirportID","sourceType":"int","targetField":"Div1AirportID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"CRSElapsedTime","sourceType":"int","targetField":"CRSElapsedTime","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DestCityMarketID","sourceType":"int","targetField":"DestCityMarketID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div4TailNum","sourceType":"string","targetField":"Div4TailNum","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div3WheelsOff","sourceType":"string","targetField":"Div3WheelsOff","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"TotalAddGTime","sourceType":"string","targetField":"TotalAddGTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DestState","sourceType":"string","targetField":"DestState","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"OriginCityName","sourceType":"string","targetField":"OriginCityName","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DestStateFips","sourceType":"string","targetField":"DestStateFips","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div5TailNum","sourceType":"string","targetField":"Div5TailNum","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"WheelsOn","sourceType":"int","targetField":"WheelsOn","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"FirstDepTime","sourceType":"string","targetField":"FirstDepTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div4WheelsOn","sourceType":"string","targetField":"Div4WheelsOn","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div3WheelsOn","sourceType":"string","targetField":"Div3WheelsOn","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Diverted","sourceType":"int","targetField":"Diverted","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"ArrDelay","sourceType":"int","targetField":"ArrDelay","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"OriginAirportID","sourceType":"int","targetField":"OriginAirportID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"TailNum","sourceType":"string","targetField":"TailNum","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"CRSDepTime","sourceType":"int","targetField":"CRSDepTime","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"FlightDate","sourceType":"date","targetField":"FlightDate","targetType":"date","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"LateAircraftDelay","sourceType":"int","targetField":"LateAircraftDelay","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div1LongestGTime","sourceType":"string","targetField":"Div1LongestGTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div2AirportID","sourceType":"int","targetField":"Div2AirportID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DayOfWeek","sourceType":"int","targetField":"DayOfWeek","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"NASDelay","sourceType":"int","targetField":"NASDelay","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div4AirportID","sourceType":"int","targetField":"Div4AirportID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div2WheelsOff","sourceType":"string","targetField":"Div2WheelsOff","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DestCityName","sourceType":"string","targetField":"DestCityName","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"OriginStateFips","sourceType":"string","targetField":"OriginStateFips","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div3AirportSeqID","sourceType":"int","targetField":"Div3AirportSeqID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div4WheelsOff","sourceType":"string","targetField":"Div4WheelsOff","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"LongestAddGTime","sourceType":"string","targetField":"LongestAddGTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"TaxiOut","sourceType":"int","targetField":"TaxiOut","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"ActualElapsedTime","sourceType":"int","targetField":"ActualElapsedTime","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DepDelay","sourceType":"int","targetField":"DepDelay","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div4AirportSeqID","sourceType":"int","targetField":"Div4AirportSeqID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Month","sourceType":"int","targetField":"Month","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DepartureDelayGroups","sourceType":"string","targetField":"DepartureDelayGroups","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div5TotalGTime","sourceType":"string","targetField":"Div5TotalGTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div4Airport","sourceType":"string","targetField":"Div4Airport","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"OriginWac","sourceType":"int","targetField":"OriginWac","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DistanceGroup","sourceType":"int","targetField":"DistanceGroup","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"OriginCityMarketID","sourceType":"int","targetField":"OriginCityMarketID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"ArrTimeBlk","sourceType":"string","targetField":"ArrTimeBlk","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"CancellationCode","sourceType":"string","targetField":"CancellationCode","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div2TailNum","sourceType":"string","targetField":"Div2TailNum","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DepTime","sourceType":"int","targetField":"DepTime","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"ArrTime","sourceType":"int","targetField":"ArrTime","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"WeatherDelay","sourceType":"int","targetField":"WeatherDelay","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div5LongestGTime","sourceType":"string","targetField":"Div5LongestGTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DepDel15","sourceType":"int","targetField":"DepDel15","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div2Airport","sourceType":"string","targetField":"Div2Airport","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"OriginState","sourceType":"string","targetField":"OriginState","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div1WheelsOff","sourceType":"string","targetField":"Div1WheelsOff","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div3AirportID","sourceType":"int","targetField":"Div3AirportID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div1TotalGTime","sourceType":"string","targetField":"Div1TotalGTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DivActualElapsedTime","sourceType":"string","targetField":"DivActualElapsedTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"ArrDelayMinutes","sourceType":"int","targetField":"ArrDelayMinutes","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div4TotalGTime","sourceType":"string","targetField":"Div4TotalGTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DivArrDelay","sourceType":"string","targetField":"DivArrDelay","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DivReachedDest","sourceType":"string","targetField":"DivReachedDest","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div1Airport","sourceType":"string","targetField":"Div1Airport","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"CarrierDelay","sourceType":"int","targetField":"CarrierDelay","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Carrier","sourceType":"string","targetField":"Carrier","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DepTimeBlk","sourceType":"string","targetField":"DepTimeBlk","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Dest","sourceType":"string","targetField":"Dest","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div2AirportSeqID","sourceType":"int","targetField":"Div2AirportSeqID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"TaxiIn","sourceType":"int","targetField":"TaxiIn","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DivAirportLandings","sourceType":"string","targetField":"DivAirportLandings","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DivDistance","sourceType":"string","targetField":"DivDistance","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DestAirportSeqID","sourceType":"int","targetField":"DestAirportSeqID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"OriginStateName","sourceType":"string","targetField":"OriginStateName","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Distance","sourceType":"int","targetField":"Distance","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Year","sourceType":"int","targetField":"Year","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div5WheelsOff","sourceType":"string","targetField":"Div5WheelsOff","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"CRSArrTime","sourceType":"int","targetField":"CRSArrTime","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DepDelayMinutes","sourceType":"int","targetField":"DepDelayMinutes","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div2WheelsOn","sourceType":"string","targetField":"Div2WheelsOn","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DestWac","sourceType":"int","targetField":"DestWac","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div1WheelsOn","sourceType":"string","targetField":"Div1WheelsOn","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div4LongestGTime","sourceType":"string","targetField":"Div4LongestGTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"ArrivalDelayGroups","sourceType":"int","targetField":"ArrivalDelayGroups","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div3TotalGTime","sourceType":"string","targetField":"Div3TotalGTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div2LongestGTime","sourceType":"string","targetField":"Div2LongestGTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"UniqueCarrier","sourceType":"string","targetField":"UniqueCarrier","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"FlightNum","sourceType":"string","targetField":"FlightNum","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Flights","sourceType":"int","targetField":"Flights","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DayOfMonth","sourceType":"int","targetField":"DayOfMonth","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"ArrDel15","sourceType":"int","targetField":"ArrDel15","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div5AirportSeqID","sourceType":"int","targetField":"Div5AirportSeqID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div1TailNum","sourceType":"string","targetField":"Div1TailNum","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div5Airport","sourceType":"string","targetField":"Div5Airport","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div5WheelsOn","sourceType":"string","targetField":"Div5WheelsOn","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"AirlineID","sourceType":"int","targetField":"AirlineID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"AirTime","sourceType":"int","targetField":"AirTime","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DestAirportID","sourceType":"int","targetField":"DestAirportID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"WheelsOff","sourceType":"int","targetField":"WheelsOff","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Cancelled","sourceType":"int","targetField":"Cancelled","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div2TotalGTime","sourceType":"string","targetField":"Div2TotalGTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"DestStateName","sourceType":"string","targetField":"DestStateName","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Origin","sourceType":"string","targetField":"Origin","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div3Airport","sourceType":"string","targetField":"Div3Airport","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div3LongestGTime","sourceType":"string","targetField":"Div3LongestGTime","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Quarter","sourceType":"int","targetField":"Quarter","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div1AirportSeqID","sourceType":"int","targetField":"Div1AirportSeqID","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"SecurityDelay","sourceType":"int","targetField":"SecurityDelay","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"Div3TailNum","sourceType":"string","targetField":"Div3TailNum","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}}],"openStatus":0,"type":0,"isIncrementField":"false","incrementField":"","encoder":"UTF-8","timeStamp":"","storage":"JDBC","dataset":"","query":{"parameters":[],"sqlTemplate":""}}
            deal_random(new_data)
            return new_data
        elif 'dw-订单表' in data:
            new_data = {"name": "api_mysqldataset_随机数", "sourceType": "DATASET", "datasetName": dataset_info[0]["name"],"datasetId":dataset_info[0]["id"], "categoryId": 0, "expiredTime": 0, "fieldMappings": [{"sourceField":"id","sourceType":"int","targetField":"id","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"user_id","sourceType":"int","targetField":"user_id","targetType":"int","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"number","sourceType":"string","targetField":"number","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"createtime","sourceType":"timestamp","targetField":"createtime","targetType":"timestamp","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"note","sourceType":"string","targetField":"note","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}},{"sourceField":"dt","sourceType":"string","targetField":"dt","targetType":"string","encrypt":"","index":0,"transformRule":{"type":"","expression":""}}],"openStatus":0,"type":0,"isIncrementField":"false","incrementField":"","encoder":"UTF-8","timeStamp":"","storage":"JDBC","dataset":"","query":{"parameters":[],"sqlTemplate":""}}
            deal_random(new_data)
            return new_data
        else:
           return
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def resource_data_dss(data):
    from new_api_cases.dw_deal_parameters import deal_random
    try:
        sql = "select id,name from merce_dss where name like '%s%%%%' ORDER BY create_time limit 1" % data
        dss_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('dss_id-name:', dss_info[0]["id"], dss_info[0]["name"])
        if 'autotest_mysql' in data:
            new_data = {"name": "api_datasource随机数", "sourceType": "DATASOURCE", "datasetName": dss_info[0]["name"], "datasetId":dss_info[0]["id"], "categoryId": 0, "expiredTime": 0, "fieldMappings": [], "openStatus":0,"type":0,"isIncrementField":"false","incrementField":"","encoder":"UTF-8","timeStamp":"","storage":"DB","dataset":"","query":{"parameters":[{"content":"","value":"18","name":"age"}],"sqlTemplate":"select\n  *\nfrom\n  student_2020\nwhere\n  age > #{age}"}}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def appconfig_data(data):
    try:
        sql = "select id ,tenant_id, owner, access_key ,cust_id ,cust_name,public_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        config_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('dsp_dc_appconfig: ', config_info[0]["id"], config_info[0]["tenant_id"], config_info[0]["owner"],
              config_info[0]["access_key"], config_info[0]["cust_id"], config_info[0]["cust_name"] ,config_info[0]["public_key"])
        new_data = {"accessIp":["192.168.2.142"], "name": "autotest_appconfig_随机数", "enabled":1,"tenantId": config_info[0]["tenant_id"], "owner": config_info[0]["owner"],"creator":"customer3","createTime": data_now(),"lastModifier":"customer3","lastModifiedTime": data_now(),"description":"","id": config_info[0]["id"],"custId":config_info[0]["cust_id"],"custName":config_info[0]["cust_name"],"accessKey": config_info[0]["access_key"],"publicKey": config_info[0]["public_key"]}
        deal_random(new_data)
        return new_data
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def cust_data_source(data):
    try:
        sql = "select id,owner,tenant_id from dsp_cust_data_source where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        source_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('dsp_cust_data_source:', source_info[0]["id"], source_info[0]["owner"], source_info[0]["tenant_id"])
        new_data = {"id":source_info[0]["id"],"name":"autotest_hdfs_csv_随机数","type":"HDFS","description":"autotest_hdfs_csv_随机数","attributes":{"quoteChar":"\"","escapeChar":"\\","path":"/auto_test/out89","format":"csv","chineseName":"autotest_hdfs_csv","header":"false","separator":",","properties":[{"name":"","value":""}],"ignoreRow":0},"owner":source_info[0]["owner"],"enabled":1,"tenantId":source_info[0]["tenant_id"],"creator":"customer3","createTime": data_now(),"lastModifier":"customer3","lastModifiedTime": data_now()}
        deal_random(new_data)
        return new_data
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def admin_flow_id(data):
    try:
        url = '%s/api/dsp/platform/service/infoById?id=%s' % (host, data)
        response = requests.get(url=url, headers=get_headers_admin(host))
        flow_id = dict_res(response.text)["content"]["jobInfo"]['flowId']
        print(flow_id)
        return flow_id
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def customer_flow_id(data):
    try:
        url = '%s/api/dsp/consumer/service/infoById?id=%s' % (host, data)
        response = requests.get(url=url, headers=get_headers_customer(host))
        flow_id = dict_res(response.text)["content"]["jobInfo"]['flowId']
        print(flow_id)
        return flow_id
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def pull_data(data):
    try:
        data = data.split("&")
        sql = "select id ,name from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
        service_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('service_id-name: ', service_info[0]["id"], service_info[0]["name"])
        config_sql = "select access_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[1]
        config_info = ms.ExecuQuery(config_sql.encode('utf-8'))
        print('config_access_key: ', config_info[0]["access_key"])
        if 'snow_dataset_dsp' in data:
            new_data = {
                "accessKey": config_info[0]["access_key"],
                "dataServiceId": service_info[0]["id"],
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
        elif 'gjb_sink_es' in data:
            new_data = {
                "accessKey": config_info[0]["access_key"],
                "dataServiceId": service_info[0]["id"],
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
        elif 'api_datasource' in data:
            new_data = {"sourceType": "DATASOURCE","accessKey": config_info[0]["access_key"], "severName": service_info[0]["name"], "dataServiceId": service_info[0]["id"], "encrypted":"false","query":{"pageable":{"pageNum":1,"pageSize":10,"pageable":"true"},"parameters":[{"name":"age","value":"18","defaultValue":"","content":""}],"sqlTemplate":""},"timestamp":1601014752}
            return new_data
        elif 'api_mysqldataset' in data:
            new_data = {"sourceType":"DATASET","accessKey":config_info[0]["access_key"],"severName": service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"fieldGroup":{"fields":[],"fieldGroups":[]},"pageable":{"pageSize":10,"pageNum":1},"selectProperties":["id","user_id","number","createtime","note","dt"]},"timestamp":1613705505}
            return new_data
        else:
            return
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def pull_data_sql(data):
    try:
        data = data.split("&")
        sql = "select id ,name from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
        service_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('service_id-name:', service_info[0]["id"], service_info[0]["name"])
        config_sql = "select access_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[1]
        config_info = ms.ExecuQuery(config_sql.encode('utf-8'))
        print('config_access_key: ', config_info[0]["access_key"])
        if 'api_mysqldataset' in data:
            new_data = {"sourceType": "DATASET", "accessKey": config_info[0]["access_key"], "severName": service_info[0]["name"], "dataServiceId": service_info[0]["id"], "encrypted": "false", "query": {"fieldGroup": {"fields": [{"andOr":"AND","name":"id","oper":"NOT_EQUAL","value":["11"]}],"fieldGroups":[]},"ordSort":[{"name":"id","order":"ASC"}],"pageable":{"pageSize":10,"pageNum":1},"selectProperties":["id","user_id","number","createtime","note","dt"]},"timestamp":1602677876}
            return new_data
        else:
            return
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def pull_Aggs_sql(data):
    try:
        data = data.split("&")
        sql = "select id ,name from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
        service_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('service_id-name:', service_info[0]["id"], service_info[0]["name"])
        config_sql = "select access_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[1]
        config_info = ms.ExecuQuery(config_sql.encode('utf-8'))
        print('config_access_key: ', config_info[0]["access_key"])
        if 'api_mysqldataset' in data:
            new_data = {"sourceType":"DATASET","accessKey": config_info[0]["access_key"],"severName":service_info[0]["name"],"dataServiceId":service_info[0]["id"],"encrypted":"false","query":{"groupFields":["createtime"],"aggFields":[{"name":"number","alias":"number_sum","aggType":"SUM","distinct":"false"},{"name":"number","alias":"number_max","aggType":"MAX","distinct":"false"},{"name":"number","alias":"number_min","aggType":"MIN","distinct":"false"},{"name":"number","alias":"number_avg","aggType":"AVG","distinct":"false"}],"havingFieldGroup":{"fields":[{"andOr":"AND","name":"number_min","oper":"NOT_EQUAL","value":["33"]}],"fieldGroups":[]},"fieldGroup":{"fields":[{"andOr":"AND","name":"id","oper":"NOT_EQUAL","value":["22"]}],"fieldGroups":[]},"ordSort":[{"name":"number_sum","order":"ASC"}],"pageable":{"pageSize":10,"pageNum":1}},"timestamp":1602677876}
            return new_data
        else:
            return
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def pull_Aggs(data):
    try:
        data = data.split("&")
        sql = "select id ,name from dsp_data_service where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[0]
        service_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('service_id-name:', service_info[0]["id"], service_info[0]["name"])
        config_sql = "select access_key from dsp_dc_appconfig where name like '%s%%%%' ORDER BY create_time desc limit 1" % data[1]
        config_info = ms.ExecuQuery(config_sql.encode('utf-8'))
        print('config_access_key: ', config_info[0]["access_key"])
        if 'gjb_sink_es' in data:
            new_data = {
                "accessKey": config_info[0]["access_key"],
                "dataServiceId": service_info[0]["id"],
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
                "accessKey": config_info[0]["access_key"],
                "dataServiceId": service_info[0]["id"],
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
        elif 'api_mysqldataset' in data:
            new_data = {"sourceType": "DATASET", "accessKey": config_info[0]["access_key"], "severName": service_info[0]["name"], "dataServiceId": service_info[0]["id"], "encrypted": "false", "query": {"groupFields":["createtime"],"aggFields":[{"name":"number","alias":"number_sum","aggType":"SUM","distinct":"false"},{"name":"number","alias":"number_max","aggType":"MAX","distinct":"false"},{"name":"number","alias":"number_min","aggType":"MIN","distinct":"false"},{"name":"number","alias":"number_avg","aggType":"AVG","distinct":"false"}],"havingFieldGroup":{"fields":[{"andOr":"AND","name":"number_min","oper":"NOT_EQUAL","value":["33"]}],"fieldGroups":[]},"fieldGroup":{"fields":[{"andOr":"AND","name":"id","oper":"NOT_EQUAL","value":["22"]}],"fieldGroups":[]},"ordSort":[{"name":"number_sum","order":"ASC"}],"pageable":{"pageSize":10,"pageNum":1}},"timestamp":1602677876}
            return new_data
        else:
            return
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def application_pull_approval(data):
    try:
        sql = "select id from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print('dsp_data_application:', flow_info[0]["id"])

        if 'gjb_sink_es' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name": "gjb_sink_es", "status": 0, "type": 0, "auditMind": "yyy"}
            return new_data
        elif 'snow_dataset_dsp' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name": "snow_dataset_dsp_", "status": 0, "type": 0, "auditMind": "yyy"}
            return new_data
        elif 'api_datasource' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name":"api_datasource", "status": 0, "type": 0, "sourceType": "DATASOURCE", "auditMind": "yyy"}
            return new_data
        elif 'api_mysqldataset' in data:
            new_data = {"applicationId": flow_info[0]["id"], "description": "", "enabled": 1, "expiredTime": "0", "name":"api_mysqldataset", "status": 0, "type": 0, "sourceType": "DATASET", "auditMind": "yyy"}
            return new_data
        else:
            return
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def application_push_approval(data):
    try:
        sql = "select id from dsp_data_application where name like '%s%%%%' ORDER BY create_time desc limit 1" % data
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
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
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def update_customer(data):
    try:
        sql = "select id,owner,tenant_id,username from dsp_customer where enabled=0 and name like '%s%%%%' order by create_time desc limit 1" % data
        customer_info = ms.ExecuQuery(sql.encode('utf-8'))
        #print('customer_id-name:', customer_info[0]["id"], customer_info[0]["name"])
        if 'gjb_test009_' in data:
            new_data = {"username":customer_info[0]["username"],"name":"gjb_test009_随机数","password":"e10adc3949ba59abbe56e057f20f883e","checkPassword":"e10adc3949ba59abbe56e057f20f883e","enabled":0,"tenantId":customer_info[0]["tenant_id"],"owner":customer_info[0]["owner"],"creator":"admin","createTime":"2020-11-03 16:18:04","lastModifier":"admin","lastModifiedTime":"2021-02-20 18:16:37","id":customer_info[0]["id"],"expiredPeriod":"0"}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def update_user(data):
    try:
        sql = "select id,owner,tenant_id,login_id from merce_user where enabled=0 and name like '%s%%%%' order by create_time desc limit 1" % data
        user_info = ms.ExecuQuery(sql.encode('utf-8'))
        #print('user_info-name:', user_info[0]["id"], user_info[0]["name"])
        if 'autotest_dsp_' in data:
            new_data = {"confirmPassword":"","email":"11@11.com","loginId":user_info[0]["login_id"],"name":"autotest_dsp_随机数","password":"AES(aa920aacdab0d8f75bc3d04b3d84586d9825e2b2b2842d7a480a3e06c888c2d848d1144f4813e55d5c0807dae20acd80)","phone":"13111111111","resourceQueues":["default"],"enabled":0,"tenantId":user_info[0]["tenant_id"],"owner":user_info[0]["owner"],"creator":"admin","createTime":"2021-02-15 00:20:49","lastModifier":"admin","lastModifiedTime":"2021-02-20 19:03:42","id":user_info[0]["id"],"pwdExpiredTime":"2022-05-15","accountExpiredTime":"2022-08-15","hdfsSpaceQuota":"0","admin":0,"clientIds":"dsp","roles":[],"expiredPeriod":"0"}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        print("\033[31m异常：\033[0m",e)