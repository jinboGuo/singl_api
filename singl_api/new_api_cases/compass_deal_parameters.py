import random

from util.format_res import dict_res
from util.timestamp_13 import *
from basic_info.setting import Compass_MySQL_CONFIG
import os
from util.Open_DB import MYSQL

ms = MYSQL(Compass_MySQL_CONFIG["HOST"], Compass_MySQL_CONFIG["USER"], Compass_MySQL_CONFIG["PASSWORD"], Compass_MySQL_CONFIG["DB"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))

def deal_parameters(data):
    if data:
        if '随机数' in data:
            # print(data)
            data = data.replace('随机数', str(random.randint(0, 999999999999999)))
            return deal_parameters(data)
        if '监控开始时间' in data:
            data = data.replace('监控开始时间', get_now_time()[0])
            return deal_parameters(data)
        if '监控结束时间' in data:
            data = data.replace('监控结束时间', get_now_time()[1])
            return deal_parameters(data)
        if 'select job_pool_oid' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            new_data = []
            if data_select_result:
                if len(data_select_result) > 1:
                    for i in range(len(data_select_result)):
                        new_data.append(data_select_result[i]["job_pool_oid"])
                    else:
                        dat = ','.join([str(i) for i in new_data])
                        return dat
                else:
                    try:
                        new_data.append(str(data_select_result[0]['job_pool_oid']))
                        return new_data
                    except:
                        print('请确认第%d行SQL语句')
        if 'select cluster_name' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["cluster_name"]
                    return deal_parameters(data)
                except:
                    print('请确认第%d行SQL语句')
            else:
                return
        if 'select job_oid' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            new_data = []
            if data_select_result:
                if len(data_select_result) > 1:
                    for i in range(len(data_select_result)):
                        new_data.append(str(data_select_result[i]["job_oid"]))
                    print("new_data ", new_data)
                    return new_data
                else:
                    new_data.append(str(data_select_result[0]['job_oid']))
                    print("new_data:",new_data)
                    return new_data
            else:
                return
        if 'select dataflow_oid' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["dataflow_oid"]
                    return deal_parameters(str(data))
                except:
                    print('请确认第%d行SQL语句')
            else:
                return
        else:
            return data
    else:
        return

def deal_random(new_data):
    dict_res(new_data)
    for key, value in new_data.items():
        if '随机数' in str(value):
            i = value.replace('随机数', str(random.randint(0, 999)))
            new_data[key] = str(i)
    print(new_data)
    return new_data
