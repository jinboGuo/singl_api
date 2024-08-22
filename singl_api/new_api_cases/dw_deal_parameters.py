import json
import random
import requests
from basic_info.get_auth_token import get_headers
from util.format_res import dict_res
from basic_info.setting import resource_type, data_source, tag_type, dw_host, log, approval_type, dw_name, dw_type
from util.get_deal_parameter import get_resourceid, get_schema, get_tags, get_datasource, get_dataset, ms, \
    get_sys_approval_target, get_organization, get_asset_id, get_owner, get_tenant_id, get_approval_record, \
    get_dw_data_tier_id, get_dw_subject_domain_id, get_dw_subject_domain, get_dw_dic_group_id, get_dw_dic_id, \
    get_dw_metadata_id
from util.timestamp_13 import data_now


def deal_parameters(data,request_method,request_url):
        if data:
            if '随机数' in data:
                data = data.replace('随机数', str(random.randint(0, 9999999999999)))
                return deal_parameters(data, request_method, request_url)
            if '创建时间' in data:
                data = data.replace('创建时间', str(data_now()))
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
            if '主题域目录' in data:
                data = data.replace('主题域目录', str(get_dw_subject_domain()))
                return deal_parameters(data, request_method, request_url)
            if '资产管理审批主键' in data:
                data = data.replace('资产管理审批主键', str(get_sys_approval_target(approval_type[0])))
                return deal_parameters(data, request_method, request_url)
            if '数仓分层审批主键' in data:
                data = data.replace('数仓分层审批主键', str(get_sys_approval_target(approval_type[9])))
                return deal_parameters(data, request_method, request_url)
            if '数仓主题域审批主键' in data:
                data = data.replace('数仓主题域审批主键', str(get_sys_approval_target(approval_type[10])))
                return deal_parameters(data, request_method, request_url)
            if '元模型审批主键' in data:
                data = data.replace('元模型审批主键', str(get_sys_approval_target(approval_type[11])))
                return deal_parameters(data, request_method, request_url)
            if '组织机构主键' in data:
                data = data.replace('组织机构主键', str(get_organization()))
                return deal_parameters(data, request_method, request_url)
            if '数据资产主键' in data:
                data = data.replace('数据资产主键', str(get_asset_id()))
                return deal_parameters(data, request_method, request_url)
            if '资产管理审批工单主键' in data:
                data = data.replace('资产管理审批工单主键', str(get_approval_record(dw_name[0])))
                return deal_parameters(data, request_method, request_url)
            if '数仓分层主键' in data:
                data = data.replace('数仓分层主键', str(get_dw_data_tier_id(dw_type[2])))
                return deal_parameters(data, request_method, request_url)
            if '数仓分层名称' in data:
                data = data.replace('数仓分层名称', str(get_dw_data_tier_id(dw_type[3])))
                return deal_parameters(data, request_method, request_url)
            if '数仓分层审批工单主键' in data:
                data = data.replace('数仓分层审批工单主键', str(get_approval_record(dw_name[1])))
                return deal_parameters(data, request_method, request_url)
            if '数仓主题域主键' in data:
                data = data.replace('数仓主题域主键', str(get_dw_subject_domain_id(dw_type[4])))
                return deal_parameters(data, request_method, request_url)
            if '数仓主题域名称' in data:
                data = data.replace('数仓主题域名称', str(get_dw_subject_domain_id(dw_type[5])))
                return deal_parameters(data, request_method, request_url)
            if '数仓主题域审批工单主键' in data:
                data = data.replace('数仓主题域审批工单主键', str(get_approval_record(dw_name[2])))
                return deal_parameters(data, request_method, request_url)
            if '数仓字典集主键' in data:
                data = data.replace('数仓字典集主键', str(get_dw_dic_group_id(dw_type[0])))
                return deal_parameters(data, request_method, request_url)
            if '数仓字典集名称' in data:
                data = data.replace('数仓字典集名称', str(get_dw_dic_group_id(dw_type[1])))
                return deal_parameters(data, request_method, request_url)
            if '数仓字典主键' in data:
                data = data.replace('数仓字典名称', str(get_dw_dic_id(dw_type[6])))
                return deal_parameters(data, request_method, request_url)
            if '数仓字典名称' in data:
                data = data.replace('数仓字典名称', str(get_dw_dic_id(dw_type[7])))
                return deal_parameters(data, request_method, request_url)
            if '数仓元模型主键' in data:
                data = data.replace('数仓元模型主键', str(get_dw_metadata_id()))
                return deal_parameters(data, request_method, request_url)
            if '数仓元模型审批工单主键' in data:
                data = data.replace('数仓元模型审批工单主键', str(get_approval_record(dw_name[1])))
                return deal_parameters(data, request_method, request_url)
            if '租户主键' in data:
                data = data.replace('租户主键', str(get_tenant_id()))
                return deal_parameters(data, request_method, request_url)
            if '管理员主键' in data:
                data = data.replace('管理员主键', str(get_owner()))
                return deal_parameters(data, request_method, request_url)
            if '/api/dw/dic/' in data:
                try:
                    data = data.format(str(get_dw_dic_id(dw_type[6])))
                    response = requests.get(url=dw_host + data, headers=get_headers())
                    new_data = response.json()["content"]
                    log.info("QUERY查询接口响应数据:{}".format(new_data))
                    return new_data
                except Exception as e:
                    log.error("执行过程中出错{}".format(e))
            if '/api/dw/metadata/' in data:
                try:
                    data = data.format(str(get_dw_metadata_id()))
                    response = requests.get(url=dw_host + data, headers=get_headers())
                    new_data = response.json()["content"]
                    log.info("QUERY查询接口响应数据:{}".format(new_data))
                    return new_data
                except Exception as e:
                    log.error("执行过程中出错{}".format(e))
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
                            response = requests.post(url=dw_host + new_data[1], headers=get_headers(),
                                                     data=new_data[0])
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
                        if '数据集主键' in data:
                            data = data.replace('数据集主键', str(get_dataset(data_source[4], new_data[1])))
                            return deal_parameters(data, request_method, request_url)
                        if '数据集名称' in data:
                            data = data.replace('数据集名称', str(get_dataset(data_source[5], new_data[1])))
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