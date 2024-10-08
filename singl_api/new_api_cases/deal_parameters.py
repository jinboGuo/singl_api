import json
import random
import requests
from basic_info.get_auth_token import get_headers
from util.get_deal_parameter import get_resourceid, get_datasource, get_schema, get_tags, get_dataset, get_dataflow_id, \
    get_tenant_id, get_owner, get_organization
from basic_info.setting import resource_type, host, data_source, tag_type, ms, log, scheduler_name
from util.timestamp_13 import data_now
from new_api_cases.prepare_datas_for_cases import get_scheduler_id, get_execution_id, get_rtcflow_id, \
    get_rtc_execution_id, get_workflow_id, get_safety_level, get_role_id, get_user_id, get_menu_id, get_explore_id


def deal_parameters(data,request_method,request_url):
    if data:
        if '随机数' in data:
            data = data.replace('随机数', str(random.randint(0, 9999999999999)))
            return deal_parameters(data,request_method,request_url)
        if '创建时间' in data:
            data = data.replace('创建时间', str(data_now()))
            return deal_parameters(data, request_method, request_url)
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
        if 'dataflow主键' in data:
            data = data.replace('dataflow主键',  str(get_dataflow_id()))
            return deal_parameters(data,request_method,request_url)
        if 'rtcflow主键' in data:
            data = data.replace('rtcflow主键',  str(get_rtcflow_id()))
            return deal_parameters(data,request_method,request_url)
        if 'workflow主键' in data:
            data = data.replace('workflow主键',  str(get_workflow_id()))
            return deal_parameters(data,request_method,request_url)
        if '租户主键' in data:
            data = data.replace('租户主键', str(get_tenant_id()))
            return deal_parameters(data, request_method, request_url)
        if '管理员主键' in data:
            data = data.replace('管理员主键', str(get_owner()))
            return deal_parameters(data, request_method, request_url)
        if '离线作业主键' in data:
            data = data.replace('离线作业主键',  str(get_scheduler_id(scheduler_name[0])))
            return deal_parameters(data,request_method,request_url)
        if '离线作业记录主键' in data:
            data = data.replace('离线作业记录主键',  str(get_execution_id(scheduler_name[0])))
            return deal_parameters(data,request_method,request_url)
        if '实时作业主键' in data:
            data = data.replace('实时作业主键',  str(get_scheduler_id(scheduler_name[1])))
            return deal_parameters(data,request_method,request_url)
        if '实时作业记录主键' in data:
            data = data.replace('实时作业记录主键',  str(get_rtc_execution_id(scheduler_name[1])))
            return deal_parameters(data,request_method,request_url)
        if '工作流作业主键' in data:
            data = data.replace('工作流作业主键',  str(get_scheduler_id(scheduler_name[2])))
            return deal_parameters(data,request_method,request_url)
        if '工作流作业记录主键' in data:
            data = data.replace('工作流作业记录主键',  str(get_execution_id(scheduler_name[2])))
            return deal_parameters(data,request_method,request_url)
        if '组织机构目录' in data:
            data = data.replace('组织机构目录',  str(get_organization()))
            return deal_parameters(data,request_method,request_url)
        if '安全等级主键' in data:
            data = data.replace('安全等级主键',  str(get_safety_level()))
            return deal_parameters(data,request_method,request_url)
        if '角色主键' in data:
            data = data.replace('角色主键',  str(get_role_id()))
            return deal_parameters(data,request_method,request_url)
        if '用户主键' in data:
            data = data.replace('用户主键',  str(get_user_id()))
            return deal_parameters(data,request_method,request_url)
        if '菜单主键' in data:
            data = data.replace('菜单主键',  str(get_menu_id()))
            return deal_parameters(data,request_method,request_url)
        if '数据探索能力配置主键' in data:
            data = data.replace('数据探索能力配置主键',  str(get_explore_id()))
            return deal_parameters(data,request_method,request_url)
        if '/api/flowComment/detail' in data and 'dataflow' in data:
            try:
                data=str(data).split("##")[0].format(str(get_dataflow_id()))
                response = requests.get(url=host + data, headers=get_headers())
                new_data = response.json()["content"][0]
                log.info("QUERY查询接口响应数据:{}".format(new_data))
                return new_data
            except Exception as e:
                log.error("执行过程中出错{}".format(e))
        if '/api/flowComment/detail' in data and 'rtcflow' in data:
            try:
                data=str(data).split("##")[0].format(str(get_rtcflow_id()))
                response = requests.get(url=host + data, headers=get_headers())
                new_data = response.json()["content"][0]
                log.info("QUERY查询接口响应数据:{}".format(new_data))
                return new_data
            except Exception as e:
                log.error("执行过程中出错{}".format(e))
        if '/api/flowComment/detail' in data and 'workflow' in data:
            try:
                data=str(data).split("##")[0].format(str(get_workflow_id()))
                response = requests.get(url=host + data, headers=get_headers())
                new_data = response.json()["content"][0]
                log.info("QUERY查询接口响应数据:{}".format(new_data))
                return new_data
            except Exception as e:
                log.error("执行过程中出错{}".format(e))
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
                    if   '/api/sys/meta/datasets/previewinit?rows=50' == new_data[2]:
                        try:
                            response = requests.post(url=host+new_data[1], headers=get_headers(), data=new_data[0])
                            new_data1 = response.json()["content"]["list"][0]
                            response1 = requests.post(url=host+new_data[2], headers=get_headers(), data=json.dumps(new_data1))
                            statement_id = response1.json()["content"]["statementId"]
                            cluster_id = response1.json()["content"]["clusterId"]
                            session_id = response1.json()["content"]["sessionId"]
                            log.info("preview_ init接口响应数据:{}".format(response1.text))
                            new_data2 = [new_data1, statement_id, cluster_id, session_id]
                            return new_data2
                        except Exception as e:
                            log.error("执行过程中出错{}".format(e))
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
                    if   '/api/sys/meta/datasets/query' == new_data[1]:
                      try:
                        response = requests.post(url=host+new_data[1], headers=get_headers(), data=new_data[0])
                        new_data = response.json()["content"]["list"][0]
                        log.info("QUERY查询接口响应数据:{}".format(new_data))
                        return new_data
                      except Exception as e:
                        log.error("执行过程中出错{}".format(e))
                    if   '/api/sys/meta/schemas/query' == new_data[1]:
                      try:
                        response = requests.post(url=host+new_data[1], headers=get_headers(), data=new_data[0])
                        new_data = response.json()["content"]["list"][0]
                        log.info("QUERY查询接口响应数据:{}".format(new_data))
                        return new_data
                      except Exception as e:
                        log.error("执行过程中出错{}".format(e))
                    if   '/api/flowComment/detail?id={}' == new_data[1]:
                      try:
                        data_select_result = ms.ExecuQuery(new_data[0].encode('utf-8'))
                        new_data[1].format(data_select_result[0]["id"])
                        response = requests.get(url=host+new_data[1], headers=get_headers())
                        new_data = response.json()["content"]["list"][0]
                        log.info("QUERY查询接口响应数据:{}".format(new_data))
                        return new_data
                      except Exception as e:
                        log.error("执行过程中出错{}".format(e))
                    if   '/api/sys/meta/explore/sql/executeinit' == new_data[1]:
                        try:
                            response1 = requests.post(url=host+new_data[1], headers=get_headers(), data=new_data[0])
                            statement_id = response1.json()["content"]["statementId"]
                            cluster_id = response1.json()["content"]["clusterId"]
                            session_id = response1.json()["content"]["sessionId"]
                            log_id = response1.json()["content"]["logId"]
                            log.info("preview_ init接口响应数据:{}".format(response1.text))
                            new_data2 = [log_id, statement_id, cluster_id, session_id]
                            return new_data2
                        except Exception as e:
                            log.error("执行过程中出错{}".format(e))
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
        if 'select application_id' in data:
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            if data_select_result:
                try:
                    data = data_select_result[0]["application_id"]
                    return data
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