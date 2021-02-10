import random
from basic_info.setting import Dsp_MySQL_CONFIG
from util.Open_DB import MYSQL

ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"])

def deal_parameters(data):
    try:
        if data:
            if '随机数' in data:
                # print(data)
                data = data.replace('随机数', str(random.randint(0, 999999999999999)))
                return deal_parameters(data)
            if 'select id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
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
                            else:
                                data = data_select_result[0]["id"]
                                return data
                        except:
                            print('请确认第%d行SQL语句')
            if 'select enabled,id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                print(data_select_result)
                if len(data_select_result):
                    try:
                        if data_select_result[0]["enabled"] == 1:
                            data_select_result[0]["enabled"] = 0
                        else:
                            data_select_result[0]["enabled"] = 1
                            print(data_select_result)
                        return data_select_result
                    except:
                        print('请确认第%d行SQL语句')
            if 'select status,id,is_running from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                print("data_select_result1:", data_select_result)
                if data_select_result:
                    try:
                        if data_select_result[0]["status"] == 1 and data_select_result[0]["is_running"] == 1:  # 正在运行服务，停止
                            status = "2"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                        if data_select_result[0]["status"] == 1 and data_select_result[0]["is_running"] == 0:  # 正在运行服务，停止
                            status = "2"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                        elif data_select_result[0]["status"] == 0 and data_select_result[0]["is_running"] == 0:  # 待部署服务，启用
                            status = "1"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                        elif data_select_result[0]["is_running"] == 2:  # 失败服务，停用
                            status = "2"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                        elif data_select_result[0]["status"] == 1 and data_select_result[0]["is_running"] == 3:  # 已成功服务，停用
                            status = "2"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                        elif data_select_result[0]["status"] == 2 and data_select_result[0]["is_running"] == 5:  # 已停止服务，启用
                            status = "1"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                        else:   # 停止服务 ，启用
                            status = "1"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                    except:
                        return {'status': '3', 'id': '725070733486587904',"expiredTime":""}
                else:
                    return {'status': '2', 'id': '725070733486587904',"expiredTime":""}
            if 'select access_key' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["access_key"]
                        print(data)
                        return data
                    except Exception as e:
                        print('\033[31m请确认第%d行SQL语句\033[0m',e)
            else:
                return data
        else:
            return data
    except Exception as e:
        print("\033[31m异常：\033[0m",e)