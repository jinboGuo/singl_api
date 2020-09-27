import random

from util.format_res import dict_res
from util.timestamp_13 import *
from basic_info.setting import Dw_MySQL_CONFIG
import os
from util.Open_DB import MYSQL

ms = MYSQL(Dw_MySQL_CONFIG["HOST"], Dw_MySQL_CONFIG["USER"], Dw_MySQL_CONFIG["PASSWORD"], Dw_MySQL_CONFIG["DB"])
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
            return deal_parameters(data)
        if '监控开始时间' in data:
            data = data.replace('监控开始时间', get_now_time()[0])
            return deal_parameters(data)
        if '监控结束时间' in data:
            data = data.replace('监控结束时间', get_now_time()[1])
            # print(data)
            return deal_parameters(data)
        if 'select id from' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            #print("999999", data_select_result)
            new_data = []
            if data_select_result:
                if len(data_select_result) > 1:
                    for i in range(len(data_select_result)):
                        new_data.append(data_select_result[i]["id"])
                    if "select id from merce_dss" in data:
                        return new_data
                    elif "select id from merce_schema" in data:
                        return new_data
                    else:
                        dat = ','.join([str(i) for i in new_data])
                        return dat
                else:
                    try:
                        if "select id from dsp_data_resource where name like 'test_hdfs_student%' order by create_time limit 1" in data:
                            new_data.append(str(data_select_result[0]['id']))
                            return new_data
                        elif "select id from merce_schema where name = 'mysql_upsert_dataset_training' ORDER BY create_time desc limit 1" in data:
                            data = data_select_result[0]["id"]
                            return data
                        elif "select id from merce_schema where name like 'mysql%' ORDER BY create_time desc limit 1" == data:
                            data = data_select_result[0]["id"]
                            return data
                        elif "select id from merce_schema where name like" in data or "select id from merce_schema where id =" in data:
                            new_data.append(str(data_select_result[0]['id']))
                            return new_data
                        else:
                            data = data_select_result[0]["id"]
                            return data
                    except:
                        print('请确认第%d行SQL语句')
        if 'select enabled,id' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if len(data_select_result):
                try:
                    if data_select_result[0]["enabled"] == 1:
                        data_select_result[0]["enabled"] = 0
                    else:
                        data_select_result[0]["enabled"] = 1
                    return data_select_result
                except:
                    print('请确认第%d行SQL语句')
        if 'select status,id from' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            #print("data_select_result1:", data_select_result)
            if data_select_result:
                try:
                    if data_select_result[0]["status"] == 1 and 'is_running=1' in data:  # 正在运行服务，停止
                        status = "2"
                        id = str(data_select_result[0]["id"])
                        new_data = {'status': status, 'id': id}
                        return new_data
                    elif data_select_result[0]["status"] == 0 and 'is_running=0' in data:  # 待部署服务，启用
                        status = "1"
                        id = str(data_select_result[0]["id"])
                        new_data = {'status': status, 'id': id}
                        return new_data
                    elif 'is_running=2' in data:  # 失败服务，停用
                        status = "2"
                        id = str(data_select_result[0]["id"])
                        new_data = {'status': status, 'id': id}
                        return new_data
                    else:   # 停止服务 ，启用
                        status = "1"
                        id = str(data_select_result[0]["id"])
                        new_data = {'status': status, 'id': id}
                        return new_data
                except:
                    return {'status': '3', 'id': '725070733486587904'}
            else:
                return {'status': '2', 'id': '725070733486587904'}
        if 'select output_data_id' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["output_data_id"]
                    return deal_parameters(data)
                except:
                    print('请确认第%d行SQL语句')
        if 'select access_key' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["access_key"]
                    print(data)
                    return data
                except:
                    print('请确认第%d行SQL语句')
        if 'select name' in data:
            # print(data)
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["name"]
                    return deal_parameters(data)
                except:
                    print('请确认第%d行SQL语句')
        if 'select execution_id' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["execution_id"]
                    return deal_parameters(data)
                except:
                    print('请确认第%d行SQL语句')
            else:
                return
        else:
            return data
    else:
        return


#new_data = { "creator": "admin", "createTime": datatime_now(), "lastModifier": "admin", "lastModifiedTime": datatime_now(), "name": "api_auto_business随机数", "alias": "api_business随机数","abbr":"api_auto_business随机数","description":"api_auto_business","dt":"dt","bizDate":"yyyyMMddHH","flowId":"","flowName":"","schedulerId":"","physicalStatus":"READY","deployStatus":"offline"}

def deal_random(new_data):
    dict_res(new_data)
    for key, value in new_data.items():
        if '随机数' in str(value):
            i = value.replace('随机数', str(random.randint(0, 999)))
            new_data[key] = str(i)
    print(new_data)
    return new_data

#deal_random(new_data)