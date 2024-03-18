import json
import random
import requests
from basic_info.get_auth_token import get_headers
from util.get_deal_parameter import get_resourceid, get_datasource, get_schema, get_tags, get_dataset
from util.logs import Logger
from basic_info.setting import MySQL_CONFIG, resource_type, host, data_source, tag_type
from util.Open_DB import MYSQL

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"],MySQL_CONFIG["PORT"])
log = Logger().get_log()

def deal_parameters(data,request_method,request_url):
    if data:
        if '随机数' in data:
            data = data.replace('随机数', str(random.randint(0, 9999999999999)))
            return deal_parameters(data,request_method,request_url)
        if '数据源目录' in data:
            data = data.replace('数据源目录', str(get_resourceid(resource_type[0])))
            return deal_parameters(data,request_method,request_url)
        if '数据集目录' in data:
            data = data.replace('数据集目录', str(get_resourceid(resource_type[1])))
            return deal_parameters(data,request_method,request_url)
        if '元数据目录' in data:
            data = data.replace('元数据目录',  str(get_resourceid(resource_type[2])))
            return deal_parameters(data,request_method,request_url)
        if '数据计算目录' in data:
            data = data.replace('数据计算目录',  str(get_resourceid(resource_type[3])))
            return deal_parameters(data,request_method,request_url)
        if '采集机目录' in data:
            data = data.replace('采集机目录',  str(get_resourceid(resource_type[4])))
            return deal_parameters(data,request_method,request_url)
        if '数据采集目录' in data:
            data = data.replace('数据采集目录',  str(get_resourceid(resource_type[5])))
            return deal_parameters(data,request_method,request_url)
        if '数据存储目录' in data:
            data = data.replace('数据存储目录',  str(get_resourceid(resource_type[6])))
            return deal_parameters(data,request_method,request_url)
        if '任务视图目录' in data:
            data = data.replace('任务视图目录',  str(get_resourceid(resource_type[7])))
            return deal_parameters(data,request_method,request_url)
        if '数据资产目录' in data:
            data = data.replace('数据资产目录',  str(get_resourceid(resource_type[8])))
            return deal_parameters(data,request_method,request_url)
        if '数据共享目录' in data:
            data = data.replace('数据共享目录',  str(get_resourceid(resource_type[9])))
            return deal_parameters(data,request_method,request_url)
        if '数据安全目录' in data:
            data = data.replace('数据安全目录',  str(get_resourceid(resource_type[10])))
            return deal_parameters(data,request_method,request_url)
        if '文件编目目录' in data:
            data = data.replace('文件编目目录',  str(get_resourceid(resource_type[11])))
            return deal_parameters(data,request_method,request_url)
        if '数据标准目录' in data:
            data = data.replace('数据标准目录',  str(get_resourceid(resource_type[12])))
            return deal_parameters(data,request_method,request_url)
        if '&&' in data:
            new_data = str(data).split('&&')
            if request_method == "PUT":
                if len(new_data)>2:
                    if '数据源主键' in data:
                        data = data.replace('数据源主键', str(get_datasource(data_source[0],new_data[2])))
                        return deal_parameters(data,request_method,request_url)
                    elif '标签主键' in data:
                        data = data.replace('标签主键', str(get_tags(tag_type[1],new_data[1])))
                        return deal_parameters(data,request_method,request_url)
                    elif '数据集主键' in data:
                        data = data.replace('数据集主键', str(get_dataset(data_source[4],new_data[2])))
                        return deal_parameters(data,request_method,request_url)
                    else:
                        return new_data[0]
                else:
                    try:
                        response = requests.post(url=host+new_data[1], headers=get_headers(), data=new_data[0])
                        new_data = json.loads(response.text)["content"]["list"][0]
                        log.info("查询接口响应数据:{}".format(new_data))
                        return new_data
                    except Exception as e:
                        log.error("执行过程中出错{}".format(e))
            else:
                if len(new_data)>2:
                    if '数据源主键' in data:
                        data = data.replace('数据源主键', str(get_schema(data_source[0],new_data[2])))
                        return deal_parameters(data,request_method,request_url)
                    if '数据源名称' in data:
                        data = data.replace('数据源名称', str(get_schema(data_source[1],new_data[2])))
                        return deal_parameters(data,request_method,request_url)
                    if '元数据主键' in data:
                        data = data.replace('元数据主键', str(get_schema(data_source[2],new_data[2])))
                        return deal_parameters(data,request_method,request_url)
                    if '元数据名称' in data:
                        data = data.replace('元数据名称', str(get_schema(data_source[3],new_data[2])))
                        return deal_parameters(data,request_method,request_url)
                    if '数据集主键' in data:
                        data = data.replace('数据集主键', str(get_schema(data_source[4],new_data[2])))
                        return deal_parameters(data,request_method,request_url)
                    if '数据集名称' in data:
                        data = data.replace('数据集名称', str(get_schema(data_source[5],new_data[2])))
                        return deal_parameters(data,request_method,request_url)
                    if '租户主键' in data:
                        data = data.replace('租户主键', str(get_schema(data_source[6],new_data[2])))
                        return deal_parameters(data,request_method,request_url)
                    if '管理员主键' in data:
                        data = data.replace('管理员主键', str(get_schema(data_source[7],new_data[2])))
                        return deal_parameters(data,request_method,request_url)
                else:
                    if '数据源主键' in data:
                        data = data.replace('数据源主键', str(get_datasource(data_source[0],new_data[1])))
                        return deal_parameters(data,request_method,request_url)
                    if '数据源名称' in data:
                        data = data.replace('数据源名称', str(get_datasource(data_source[1],new_data[1])))
                        return deal_parameters(data,request_method,request_url)
                    if '标签主键' in data:
                        data = data.replace('标签主键', str(get_tags(tag_type[0],new_data[1])))
                        return deal_parameters(data,request_method,request_url)
        if 'select id from' in data:
            log.info("开始执行语句:{}".format(data))
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            log.info("sql查询结果为:{}".format(data_select_result))
            new_data = []
            if data_select_result:
                if len(data_select_result) > 1:
                    for i in range(len(data_select_result)):
                        new_data.append(data_select_result[i]["id"])
                    return new_data
                else:
                    try:
                        if "{}" in request_url:
                          data = data_select_result[0]["id"]
                          return data
                        else:
                            new_data.append(data_select_result[0]["id"])
                            return new_data
                    except Exception as e:
                            log.error("执行过程中出错{}".format(e))
            else:
                log.error("sql查询结果为空！")
        if 'select output_data_id' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["output_data_id"]
                    return deal_parameters(data,request_method,request_url)
                except Exception as e:
                    log.error("执行过程中出错{}".format(e))
            else:
                log.error("sql查询结果为空！")
        if 'SELECT enabled, id FROM' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if len(data_select_result) > 1:
                for i in range(len(data_select_result)):
                    try:
                        if data_select_result[i]["enabled"] == 1:
                            data_select_result[i]["enabled"] = 0
                        else:
                            data_select_result[i]["enabled"] = 1
                    except Exception as e:
                            log.error("执行过程中出错{}".format(e))
                return data_select_result
            else:
                try:
                    if data_select_result[0]["enabled"] == 1:
                        data_select_result[0]["enabled"] = 0
                    else:
                        data_select_result[0]["enabled"] = 1
                    return data_select_result
                except Exception as e:
                    log.error("执行过程中出错{}".format(e))
        if 'select id,enabled from' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            ids = []
            new_data = {}
            if len(data_select_result) > 1:
                for i in range(len(data_select_result)):
                    try:
                        if data_select_result[i]["enabled"] == 1:
                            ids.append(data_select_result[i]["id"])
                            new_data['ids'] = ids
                            new_data['enabled'] = 0
                        else:
                            ids.append(data_select_result[i]["id"])
                            new_data['ids'] = ids
                            new_data['enabled'] = 1
                    except Exception as e:
                            log.error("执行过程中出错{}".format(e))
                return new_data
            else:
                try:
                    if data_select_result[0]["enabled"] == 1:
                        ids.append(data_select_result[0]["id"])
                        new_data['ids'] = ids
                        new_data['enabled'] = 0
                    else:
                        ids.append(data_select_result[0]["id"])
                        new_data['ids'] = ids
                        new_data['enabled'] = 1
                    return new_data
                except Exception as e:
                    log.error("执行过程中出错{}".format(e))
        if 'select name' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["name"]
                    return deal_parameters(data,request_method,request_url)
                except Exception as e:
                    log.error("执行过程中出错{}".format(e))
            else:
                log.error("sql查询结果为空！")
        if 'select execution_id' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["execution_id"]
                    return deal_parameters(data,request_method,request_url)
                except Exception as e:
                    log.error("执行过程中出错{}".format(e))
            else:
                log.error("sql查询结果为空！")
        else:
            if '&&' in data:
                new_data = data.split('&&')[0]
                return new_data
            else:
                return data
    else:
        return data