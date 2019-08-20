import random
from util.timestamp_13 import *
from basic_info.setting import MySQL_CONFIG
import os
from util.Open_DB import MYSQL

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))

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
            # print(deal_parameters(new_data))
            return deal_parameters(data)
        if '监控开始时间' in data:
            data = data.replace('监控开始时间', get_now_time()[0])
            print(data)
            return deal_parameters(data)
        if '监控结束时间' in data:
            data = data.replace('监控结束时间', get_now_time()[1])
            # print(data)
            return deal_parameters(data)
        if 'select id' in data:
            print(data)
            data_select_result = ms.ExecuQuery(data)
            print(data_select_result)
            if data_select_result:
                try:
                    data = data_select_result[0]["id"]
                    print(data)
                    return deal_parameters(data)
                except:
                    print('请确认第%d行SQL语句')
        else:
            return data
    else:
        return

# data = "select id from merce_dataset where name = 'API_dataset_HDFS_csv_随机数' and creator = 'admin'"
# print(deal_parameters(data))