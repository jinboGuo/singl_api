import random
from util.format_res import dict_res
from basic_info.setting import Dw_MySQL_CONFIG
from util.Open_DB import MYSQL
from util.logs import Logger
ms = MYSQL(Dw_MySQL_CONFIG["HOST"], Dw_MySQL_CONFIG["USER"], Dw_MySQL_CONFIG["PASSWORD"], Dw_MySQL_CONFIG["DB"])

def deal_parameters(data):
    try:
        if data:
            if '随机数' in data:
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
                                data = data_select_result[0]["id"]
                                return data
                        except Exception as e:
                            Logger().get_log().info("异常信息：%s " % e)
            if 'select name' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["name"]
                        return deal_parameters(data)
                    except Exception as e:
                        Logger().get_log().info("异常信息：%s " % e)
            if 'select execution_id' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["execution_id"]
                        return deal_parameters(data)
                    except Exception as e:
                        Logger().get_log().info("异常信息：%s " % e)
                else:
                    return
            else:
                return data
        else:
            return data
    except Exception as e:
        Logger().get_log().info("异常信息：%s " % e)

def deal_random(new_data):
    try:
        dict_res(new_data)
        for key, value in new_data.items():
            if '随机数' in str(value):
                i = value.replace('随机数', str(random.randint(0, 999)))
                new_data[key] = str(i)
        return new_data
    except Exception as e:
        Logger().get_log().info("异常信息：%s " % e)