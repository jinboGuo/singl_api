import json
import random

from basic_info.mylogging import myLog
from new_api_cases.prepare_datas_for_cases import get_collector_datasourceId
from util.timestamp_13 import *
from basic_info.setting import MySQL_CONFIG
import os
from util.Open_DB import MYSQL

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"],MySQL_CONFIG["PORT"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))
log=myLog().getLog().logger

def deal_parameters(data):
    if data:
        if '随机数' in data:
            # print(data)
            data = data.replace('随机数', str(random.randint(0, 999999999999999)))
            return deal_parameters(data)
        if '6天前时间戳' in data:
            data = data.replace('6天前时间戳', get_timestamp(6))
            return deal_parameters(data)
        if '当前时间戳' in data:
            data = data.replace('当前时间戳', get_timestamp(0))
            return deal_parameters(data)
        if '监控开始时间' in data:
            data = data.replace('监控开始时间', get_now_time()[0])
            return deal_parameters(data)
        if '监控结束时间' in data:
            data = data.replace('监控结束时间', get_now_time()[1])
            # print(data)
            return deal_parameters(data)
        if "采集器数据源id" in data:
            datasourceId = get_collector_datasourceId()
            data = data.replace("采集器数据源id", datasourceId)
            return data
        if 'select id from' in data:
            log.info("开始执行语句{}".format(data))
            #count=10
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            # while count>0:
            #     if data_select_result==():
            #         time.sleep(5)
            #         data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            #     else:
            #         continue
            #     count-=1
            # # print(data_select_result)
            log.info("{}查询结果为{}".format(data,data_select_result))
            new_data = []
            if data_select_result:
                if len(data_select_result) > 1:
                    for i in range(len(data_select_result)):
                        new_data.append(data_select_result[i]["id"])
                    if "select id from merce_dss" in data:
                        return new_data
                    elif "select id from merce_schema" in data:
                        return new_data
                    elif "select id from merce_tag" in data:
                        return new_data
                    elif "select id from merce_dataset where name like 'gjb_test%'" in data:
                        print("[str-list]: ", new_data)
                        return new_data
                    else:
                        list_data = ','.join([str(i) for i in new_data])
                        print("[str]: ", list_data)
                        return list_data
                elif "select id from merce_schema where name like 'gtest_mysql_0428_training_%'" in data:
                    new_data.append(data_select_result[0]["id"])
                    return new_data
                elif "select id from  merce_flow_execution where flow_name like 'auto_api%'"in data:
                    new_data.append(data_select_result[0]["id"])
                    return new_data
                elif "'select id from merce_resource_dir where name like 'api_test%' ORDER BY create_time desc limit 1;'" in data:
                    new_data.append(data_select_result[0]["id"])
                    return new_data
                elif "select id from merce_user" in data:
                    new_data.append(data_select_result[0]["id"])
                    print(new_data)
                    return new_data
                elif "select id from merce_fileset order by create_time limit 1" in data:
                    new_data.append(data_select_result[0]["id"])
                    print(new_data)
                    return new_data
                elif "select id from sync_job where name like 'collector_ftp%' order by create_time desc limit 1" in data:
                    new_data = data_select_result[0]["id"]
                    return new_data
                elif "select id from sync_job where collecter_id = 'c1' ORDER BY last_modified_time desc limit 1" in data:
                    new_data = data_select_result[0]["id"]
                    print(new_data)
                    return new_data
                elif "select id from sync_job" in data:
                    new_data.append(data_select_result[0]["id"])
                    print(new_data)
                    return new_data
                else:
                    try:
                        data = data_select_result[0]["id"]
                        return data
                    except:
                        log.error("请确认第%d行SQL语句")
        if 'select output_data_id' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["output_data_id"]
                    return deal_parameters(data)
                except:
                    log.error("请确认第%d行SQL语句")
        if 'SELECT enabled, id FROM' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if len(data_select_result) > 1:
                for i in range(len(data_select_result)):
                    try:
                        if data_select_result[i]["enabled"] == 1:
                            data_select_result[i]["enabled"] = 0
                        else:
                            data_select_result[i]["enabled"] = 1
                    except:
                        log.error("请确认第%d行SQL语句")
                print(data_select_result)
                return data_select_result
            else:
                try:
                    if data_select_result[0]["enabled"] == 1:
                        data_select_result[0]["enabled"] = 0
                        print(data_select_result)
                    else:
                        data_select_result[0]["enabled"] = 1
                        print(data_select_result)
                    return data_select_result
                except:
                    # print('请确认第%d行SQL语句')
                    log.error("请确认第%d行SQL语句")

        if 'select name' in data:
            # print(data)
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["name"]
                    return deal_parameters(data)
                except:
                    # print('请确认第%d行SQL语句')
                    log.error("请确认第%d行SQL语句")
        if 'select execution_id' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["execution_id"]
                    return deal_parameters(data)
                except:
                    log.error("请确认第%d行SQL语句")
            else:
                return
        else:
            return data
    else:
        return