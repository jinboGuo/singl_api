import json
import random

from basic_info.setting import resource_type, data_source, dsp_host, tag_type, ms, log
from util.get_deal_parameter import get_resourceid, get_schema, get_tags, get_dataset, get_datasource, \
    get_source_dss_id, get_source_dss_name, \
    get_draft_id, get_source_schema_id, get_source_schema_name, get_sink_schema_name, get_sink_schema_id, \
    get_source_node_id, get_sink_node_id, get_collect_task_id, get_sink_dss_id, get_sink_dss_name, \
    get_sink_dataset_name, \
    get_source_dataset_name, get_collector_group_id, get_collector_group_name, get_tenant_id, get_owner, get_user_id, \
    get_sink_schema_name_and_random, get_sink_dataset_name_and_random, get_source_dataset_id, get_dss_mysql_id, \
    get_rule_name, get_dss_mysql_name, get_rule_id, get_collect_schema_task_id, get_schema_collect_task_name,get_current_time


def deal_parameters(data, request_method, request_url):
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
        if '元数据名称规则目录' in data:
            data = data.replace('元数据名称规则目录', str(get_resourceid(resource_type[13])))
            return deal_parameters(data, request_method, request_url)
        if '元数据采集任务目录' in data:
            data = data.replace('元数据采集任务目录', str(get_resourceid(resource_type[14])))
            return deal_parameters(data, request_method, request_url)
        if '&&' in data:
            new_data = str(data).split('&&')
            if request_method == "PUT":
                if len(new_data) > 2:
                    request_data = new_data[1]
                    if '租户主键' in request_data or '管理员主键' in request_data:
                        request_data = request_data.replace('租户主键', str(get_tenant_id()))
                        request_data = request_data.replace('管理员主键', str(get_owner()))
                    select_data = new_data[0]
                    data_select_result = ms.ExecuQuery(select_data.encode('utf-8'))
                    data_select_result = data_select_result[0]['id']
                    request_data = str(request_data).replace('输入', str(data_select_result))
                    return request_data
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
        if '&&' in data and '输入' in data:
            select_data = data.split('&&')[0]
            log.info("开始执行语句:{}".format(select_data))
            data_select_result = ms.ExecuQuery(select_data.encode('utf-8'))
            log.info("sql查询结果为:{}".format(data_select_result))
            data_select_result = data_select_result[0]['id']
            request_data = data.split('&&')[1]
            request_data = str(request_data).replace('输入', str(data_select_result))
            return request_data
        if '输入输入数据源id' in data or '输入输出元数据名称' in data or '输入画布id' in data or '输入采集任务id' in data or '输入元数据命名规则名称' in data or '输入元数据采集任务id' in data:
            request_data = data.replace('输入输入数据源id', str(get_source_dss_id()))
            request_data = request_data.replace('输入输入数据源名称', get_source_dss_name())
            request_data = request_data.replace('输入画布id', str(get_draft_id()))
            request_data = request_data.replace('输入输入元数据id', str(get_source_schema_id()))
            request_data = request_data.replace('输入输入元数据名称', get_source_schema_name())
            request_data = request_data.replace('输入输出元数据名称', get_sink_schema_name())
            request_data = request_data.replace('输入输出数据源id', str(get_sink_dss_id()))
            request_data = request_data.replace('输入输出数据源名称', get_sink_dss_name())
            request_data = request_data.replace('输入采集任务id', str(get_collect_task_id()))
            request_data = request_data.replace('输入输入端uuid', str(get_source_node_id()))
            request_data = request_data.replace('输入输出端uuid', str(get_sink_node_id()))
            request_data = request_data.replace('输入输出数据集名称', get_sink_dataset_name())
            request_data = request_data.replace('输入输出数据集id', str(get_source_dataset_id()))
            request_data = request_data.replace('输入输出元数据id', str(get_sink_schema_id()))
            request_data = request_data.replace('输入采集组id', str(get_collector_group_id()))
            request_data = request_data.replace('输入采集组名称', get_collector_group_name())
            request_data = request_data.replace('租户主键', str(get_tenant_id()))
            request_data = request_data.replace('管理员主键', str(get_owner()))
            request_data = request_data.replace('用户id', str(get_user_id()))
            request_data = request_data.replace('输入输出端数据集名称', str(get_sink_dataset_name()))
            request_data = request_data.replace('输出元数据名称+随机的', str(get_sink_schema_name_and_random()))
            request_data = request_data.replace('输出数据集名称+随机的', str(get_sink_dataset_name_and_random()))
            request_data = request_data.replace('输入输入数据集名称', str(get_source_dataset_name()))
            request_data = request_data.replace('输入输入数据集id', str(get_source_dataset_id()))
            request_data = request_data.replace('输入mysql数据源id', str(get_dss_mysql_id()))
            request_data = request_data.replace('输入mysql数据源名称', str(get_dss_mysql_name()))
            request_data = request_data.replace('输入元数据命名规则名称', str(get_rule_name()))
            request_data = request_data.replace('输入元数据命名规则id', str(get_rule_id()))
            request_data = request_data.replace('输入元数据采集任务id', str(get_collect_schema_task_id()))
            request_data = request_data.replace('输入元数据采集任务名称', str(get_schema_collect_task_name()))
            request_data = request_data.replace('获取当前时间', str(get_current_time()))
            return request_data
        if 'SELECT name,id from' in data and '输入名称' in data and '输入id' in data:
            select_data = data.split('&&')[0]
            log.info("开始执行语句:{}".format(select_data))
            data_select_result = ms.ExecuQuery(select_data.encode('utf-8'))
            log.info("sql查询结果为:{}".format(data_select_result))
            data_select_result_id = data_select_result[0]['id']
            data_select_result_name = data_select_result[0]['name']
            request_data = data.split('&&')[1]
            request_data = str(request_data).replace('输入名称', data_select_result_name)
            request_data = str(request_data).replace('输入id', data_select_result_id)
            return request_data

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
                            new_data.append(str(data_select_result[0]["id"]))
                            new_data = json.dumps(new_data)
                            return new_data
                    except Exception as e:
                        log.error("执行过程中出错{}".format(e))
            else:
                log.error("sql查询结果为空！")
        if 'select task_id from' in data:
            log.info("开始执行语句:{}".format(data))
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            log.info("sql查询结果为:{}".format(data_select_result))
            new_data = []
            if data_select_result:
                if len(data_select_result) > 1:
                    for i in range(len(data_select_result)):
                        new_data.append(data_select_result[i]["task_id"])
                    return new_data
                else:
                    try:
                        if "{}" in request_url:
                            data = data_select_result[0]["task_id"]
                            return data
                        else:
                            new_data.append(str(data_select_result[0]["task_id"]))
                            new_data = json.dumps(new_data)
                            return new_data
                    except Exception as e:
                        log.error("执行过程中出错{}".format(e))
            else:
                log.error("sql查询结果为空！")
        else:
            return data
