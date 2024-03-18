import json
import random
import requests
from basic_info.get_auth_token import get_headers
from util.format_res import dict_res
from basic_info.setting import Dw_MySQL_CONFIG, resource_type, data_source, tag_type, dw_host
from util.Open_DB import MYSQL
from util.get_deal_parameter import get_resourceid, get_schema, get_tags, get_datasource, get_dataset
from util.logs import Logger

ms = MYSQL(Dw_MySQL_CONFIG["HOST"], Dw_MySQL_CONFIG["USER"], Dw_MySQL_CONFIG["PASSWORD"], Dw_MySQL_CONFIG["DB"], Dw_MySQL_CONFIG["PORT"])
log = Logger().get_log()

def deal_parameters(data,request_method,request_url):
        if data:
            if '随机数' in data:
                data = data.replace('随机数', str(random.randint(0, 9999999999999)))
                return deal_parameters(data, request_method, request_url)
            if '数据源目录' in data:
                data = data.replace('数据源目录', str(get_resourceid(resource_type[0])))
                return deal_parameters(data, request_method, request_url)
            if '数据集目录' in data:
                data = data.replace('数据集目录', str(get_resourceid(resource_type[1])))
                return deal_parameters(data, request_method, request_url)
            if '元数据目录' in data:
                data = data.replace('元数据目录', str(get_resourceid(resource_type[2])))
                return deal_parameters(data, request_method, request_url)
            if '数据计算目录' in data:
                data = data.replace('数据计算目录', str(get_resourceid(resource_type[3])))
                return deal_parameters(data, request_method, request_url)
            if '采集机目录' in data:
                data = data.replace('采集机目录', str(get_resourceid(resource_type[4])))
                return deal_parameters(data, request_method, request_url)
            if '数据采集目录' in data:
                data = data.replace('数据采集目录', str(get_resourceid(resource_type[5])))
                return deal_parameters(data, request_method, request_url)
            if '数据存储目录' in data:
                data = data.replace('数据存储目录', str(get_resourceid(resource_type[6])))
                return deal_parameters(data, request_method, request_url)
            if '任务视图目录' in data:
                data = data.replace('任务视图目录', str(get_resourceid(resource_type[7])))
                return deal_parameters(data, request_method, request_url)
            if '数据资产目录' in data:
                data = data.replace('数据资产目录', str(get_resourceid(resource_type[8])))
                return deal_parameters(data, request_method, request_url)
            if '数据共享目录' in data:
                data = data.replace('数据共享目录', str(get_resourceid(resource_type[9])))
                return deal_parameters(data, request_method, request_url)
            if '数据安全目录' in data:
                data = data.replace('数据安全目录', str(get_resourceid(resource_type[10])))
                return deal_parameters(data, request_method, request_url)
            if '文件编目目录' in data:
                data = data.replace('文件编目目录', str(get_resourceid(resource_type[11])))
                return deal_parameters(data, request_method, request_url)
            if '数据标准目录' in data:
                data = data.replace('数据标准目录', str(get_resourceid(resource_type[12])))
                return deal_parameters(data, request_method, request_url)
            if '&&' in data:
                new_data = str(data).split('&&')
                if request_method == "PUT":
                    if len(new_data) > 2:
                        if '数据源主键' in data:
                            data = data.replace('数据源主键', str(get_datasource(data_source[0], new_data[2])))
                            return deal_parameters(data, request_method, request_url)
                        elif '标签主键' in data:
                            data = data.replace('标签主键', str(get_tags(tag_type[1], new_data[1])))
                            return deal_parameters(data, request_method, request_url)
                        elif '数据集主键' in data:
                            data = data.replace('数据集主键', str(get_dataset(data_source[4], new_data[2])))
                            return deal_parameters(data, request_method, request_url)
                        else:
                            return new_data[0]
                    else:
                        try:
                            response = requests.post(url=dw_host + new_data[1], headers=get_headers(), data=new_data[0])
                            new_data = json.loads(response.text)["content"]["list"][0]
                            log.info("查询接口响应数据:{}".format(new_data))
                            return new_data
                        except Exception as e:
                            log.error("执行过程中出错{}".format(e))
                else:
                    if len(new_data) > 2:
                        if '数据源主键' in data:
                            data = data.replace('数据源主键', str(get_schema(data_source[0], new_data[2])))
                            return deal_parameters(data, request_method, request_url)
                        if '数据源名称' in data:
                            data = data.replace('数据源名称', str(get_schema(data_source[1], new_data[2])))
                            return deal_parameters(data, request_method, request_url)
                        if '元数据主键' in data:
                            data = data.replace('元数据主键', str(get_schema(data_source[2], new_data[2])))
                            return deal_parameters(data, request_method, request_url)
                        if '元数据名称' in data:
                            data = data.replace('元数据名称', str(get_schema(data_source[3], new_data[2])))
                            return deal_parameters(data, request_method, request_url)
                        if '数据集主键' in data:
                            data = data.replace('数据集主键', str(get_schema(data_source[4], new_data[2])))
                            return deal_parameters(data, request_method, request_url)
                        if '数据集名称' in data:
                            data = data.replace('数据集名称', str(get_schema(data_source[5], new_data[2])))
                            return deal_parameters(data, request_method, request_url)
                        if '租户主键' in data:
                            data = data.replace('租户主键', str(get_schema(data_source[6], new_data[2])))
                            return deal_parameters(data, request_method, request_url)
                        if '管理员主键' in data:
                            data = data.replace('管理员主键', str(get_schema(data_source[7], new_data[2])))
                            return deal_parameters(data, request_method, request_url)
                    else:
                        if '数据源主键' in data:
                            data = data.replace('数据源主键', str(get_datasource(data_source[0], new_data[1])))
                            return deal_parameters(data, request_method, request_url)
                        if '数据源名称' in data:
                            data = data.replace('数据源名称', str(get_datasource(data_source[1], new_data[1])))
                            return deal_parameters(data, request_method, request_url)
                        if '标签主键' in data:
                            data = data.replace('标签主键', str(get_tags(tag_type[0], new_data[1])))
                            return deal_parameters(data, request_method, request_url)
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
            if 'select name from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["name"]
                        return deal_parameters(data, request_method, request_url)
                    except Exception as e:
                        log.error("异常信息：%s" %e)
                else:
                    log.info("查询结果为空！")
            if 'select current_info_id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["current_info_id"]
                        return str(data)
                    except Exception as e:
                        log.error("异常信息：%s" %e)
                else:
                    log.info("查询结果为空！")
            if 'select business_id,id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        business_id,dw_id = data_select_result[0]["business_id"],data_select_result[0]["id"]
                        new_data.append(business_id)
                        new_data.append(str(dw_id))
                        return new_data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            if 'select dataset_id,id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        dataset_id,dw_id = data_select_result[0]["dataset_id"],data_select_result[0]["id"]
                        new_data.append(dataset_id)
                        new_data.append(str(dw_id))
                        return new_data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            if 'select project_id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["project_id"]
                        return str(data)
                    except Exception as e:
                        log.error("异常信息：%s" %e)
                else:
                    log.info("查询结果为空！")
            if 'select metadata_id,id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        metadata_id,dw_id = data_select_result[0]["metadata_id"],data_select_result[0]["id"]
                        new_data.append(str(metadata_id))
                        new_data.append(str(dw_id))
                        return new_data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            if 'select id,current_info_id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        metadata_id,current_info_id = data_select_result[0]["id"],data_select_result[0]["current_info_id"]
                        new_data.append(str(metadata_id))
                        new_data.append(str(current_info_id))
                        return new_data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            if 'select name,project_id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        name,project_id = data_select_result[0]["name"],data_select_result[0]["project_id"]
                        new_data.append(name)
                        new_data.append(str(project_id))
                        return new_data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            if 'select project_id,id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        project_id,dw_id = data_select_result[0]["project_id"],data_select_result[0]["id"]
                        new_data.append(str(project_id))
                        new_data.append(str(dw_id))
                        return new_data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            else:
                if '&&' in data:
                    new_data = data.split('&&')[0]
                    return new_data
                else:
                    return data
        else:
            return data
        
def deal_random(new_data):
    try:
        dict_res(new_data)
        for key, value in new_data.items():
            if '随机数' in str(value):
                i = value.replace('随机数', str(random.randint(0, 999)))
                new_data[key] = str(i)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)