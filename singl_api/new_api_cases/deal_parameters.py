import random
from util.timestamp_13 import *
from basic_info.setting import MySQL_CONFIG, Dsp_MySQL_CONFIG
import os
from util.Open_DB import MYSQL

#ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))

def deal_parameters(data):
    if data:
    # print(data)
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
            # print(data)
            return deal_parameters(data)
        if '监控结束时间' in data:
            data = data.replace('监控结束时间', get_now_time()[1])
            # print(data)
            return deal_parameters(data)
        if 'select id from' in data:
            print("00000", data)
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            print("999999", data_select_result)
            new_data = []
            if data_select_result:
                if len(data_select_result) > 1:
                    for i in range(len(data_select_result)):
                        new_data.append(data_select_result[i]["id"])
                    l = ','.join([str(i) for i in new_data])
                    print("L=", l)
                    return l
                else:
                    try:
                        data = data_select_result[0]["id"]
                        print("7777", data, type(data))
                        return data
                    except:
                        print('请确认第%d行SQL语句')
        if 'select enabled,id' in data:
            #new_data = []
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            print("22333333333", data_select_result)
            if len(data_select_result):
                # if len(data_select_result) > 1:
                #     for i in range(len(data_select_result)):
                #        if data_select_result[i]["enabled"] == 1:
                #            data_select_result[i]["enabled"] = 0
                #        else:
                #            data_select_result[i]["enabled"] = 1
                #     print(data_select_result, type(data_select_result))
                #     return deal_parameters(data_select_result)
                # else:
                try:
                    if data_select_result[0]["enabled"] == 1:
                        data_select_result[0]["enabled"] = 0
                    else:
                        data_select_result[0]["enabled"] = 1
                    print(data_select_result, type(data_select_result))
                    return data_select_result
                except:
                    print('请确认第%d行SQL语句')
        if 'select status,id from' in data:
            new_data = {'status': '', 'id': ""}
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            print("data_select_result1:", data_select_result)
            if data_select_result:
                try:
                    if data_select_result[0]["status"] == 1 and 'is_running=1' in data:  # 正在运行服务，停止
                            status = "2"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id}
                            print("new_data1:", new_data)
                            return new_data
                    elif data_select_result[0]["status"] == 0 and 'is_running=0' in data:  # 待部署服务，启用
                            status = "1"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id}
                            print("new_data2:", new_data)
                            return new_data
                    elif 'is_running=2' in data:  # 失败服务，停用
                        status = "2"
                        id = str(data_select_result[0]["id"])
                        new_data = {'status': status, 'id': id}
                        print("new_data3:", new_data)
                        return new_data
                    else:   # 停止服务 ，启用
                        status = "1"
                        id = str(data_select_result[0]["id"])
                        new_data = {'status': status, 'id': id}
                        print("new_data4:", new_data)
                        return new_data
                except:
                      return {'status': '3', 'id': '725070733486587904'}
            else:
                 return {'status': '2', 'id': '725070733486587904'}
        if 'select output_data_id' in data:
            # print(data)
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["output_data_id"]
                    print(data)
                    return deal_parameters(data)
                except:
                    print('请确认第%d行SQL语句')
        if 'select name' in data:
            # print(data)
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["name"]
                    print(data)
                    return deal_parameters(data)
                except:
                    print('请确认第%d行SQL语句')
        if 'select execution_id' in data:
            # print(data)
            # data = data.encode('utf-8')
            data_select_result = ms.ExecuQuery(data)
            # print(data_select_result)
            if data_select_result:
                try:
                    data = data_select_result[0]["execution_id"]
                    print(data)
                    return deal_parameters(data)
                except:
                    print('请确认第%d行SQL语句')
            else:
                return
        else:
            return data
    else:
        return

#data = "select id from merce_zrule where `name` like 'API_create_%' and name not like '%测试用%'  ORDER BY create_time desc limit 3"
#print(deal_parameters(data))