import random
from util.format_res import dict_res
from basic_info.setting import Dw_MySQL_CONFIG
from util.Open_DB import MYSQL

ms = MYSQL(Dw_MySQL_CONFIG["HOST"], Dw_MySQL_CONFIG["USER"], Dw_MySQL_CONFIG["PASSWORD"], Dw_MySQL_CONFIG["DB"])

def deal_parameters(data):
    try:
        if data:
            if '随机数' in data:
                # print(data)
                data = data.replace('随机数', str(random.randint(0, 999999999999999)))
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
                                data = data_select_result[0]["id"]
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
            return data
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def deal_random(new_data):
    try:
        dict_res(new_data)
        get_targe_value(new_data)
        print(new_data)
        return new_data
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def get_targe_value(new_data):
    # 循环字典，获取键、值
    for key, values in new_data.items():
        # 判断值的type类型，如果是list,调用get_list() 函数，
        if type(values) == list:
            get_list(values)
        # 如果是字典，调用自身
        elif type(values) == dict:
            get_targe_value(values)
        # 如果值不是list且是需要被替换的，就替换掉
        elif type(values) != list and type(values) == str and '随机数' in values:
            i = values.replace('随机数', str(random.randint(0, 999)))
            new_data[key] = str(i)
        else:
            pass

def get_list(values):
    rustle = values[0]
    if type(rustle) == list:
        get_list(values)
    else:
        get_targe_value(rustle)