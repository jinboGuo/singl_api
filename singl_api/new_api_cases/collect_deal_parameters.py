import json
import random
from basic_info.setting import resource_type, data_source, tag_type, ms, log
from util.get_deal_parameter import (
    get_resourceid, get_schema, get_tags, get_datasource,
    get_dss_id, get_dss_name, get_draft_id, get_source_schema_id,
    get_source_schema_name, get_sink_schema_name, get_sink_schema_id,
    get_source_node_id, get_sink_node_id, get_collect_task_id,
    get_sink_dataset_name, get_source_dataset_name, get_collector_group_id,
    get_collector_group_name, get_tenant_id, get_owner, get_user_id,
    get_sink_schema_name_and_random, get_sink_dataset_name_and_random,
    get_source_dataset_id, get_dss_mysql_id, get_rule_name, get_dss_mysql_name,
    get_rule_id, get_collect_schema_task_id, get_schema_collect_task_name,
    get_current_time, get_table_name, get_sink_dataset_id, get_HDFS_id,
    get_HDFS_name, get_driver_name
)

DIRECTORY_MAP = {
    '数据源目录': 0,
    '数据集目录': 1,
    '元数据目录': 2,
    '数据计算目录': 3,
    '采集机目录': 4,
    '数据采集目录': 5,
    '数据存储目录': 6,
    '任务视图目录': 7,
    '数据资产目录': 8,
    '数据共享目录': 9,
    '数据安全目录': 10,
    '文件编目目录': 11,
    '数据标准目录': 12,
    '元数据名称规则目录': 15,
    '元数据采集任务目录': 13,
    '标签管理目录': 16
}

PLACEHOLDER_MAP = {
    '输入输入数据源id': lambda: str(get_dss_id(1)),
    '输入输入数据源名称': lambda: get_dss_name(1),
    '输入画布id': lambda: str(get_draft_id()),
    '输入输入元数据id': lambda: str(get_source_schema_id()),
    '输入输入元数据名称': lambda: get_source_schema_name(),
    '输入输出元数据名称': lambda: get_sink_schema_name(),
    '输入输出数据源id': lambda: str(get_dss_id()),
    '输入输出数据源名称': lambda: get_dss_name(),
    '输入采集任务id': lambda: str(get_collect_task_id()),
    '输入输入端uuid': lambda: str(get_source_node_id()),
    '输入输出端uuid': lambda: str(get_sink_node_id()),
    '输入输出数据集id': lambda: str(get_sink_dataset_id()),
    '输入输出元数据id': lambda: str(get_sink_schema_id()),
    '输入采集组id': lambda: str(get_collector_group_id()),
    '输入采集组名称': lambda: get_collector_group_name(),
    '租户主键': lambda: str(get_tenant_id()),
    '管理员主键': lambda: str(get_owner()),
    '用户id': lambda: str(get_user_id()),
    '输入输出端数据集名称': lambda: str(get_sink_dataset_name()),
    '输出元数据名称+随机的': lambda: str(get_sink_schema_name_and_random()),
    '输出数据集名称+随机的': lambda: str(get_sink_dataset_name_and_random()),
    '输入输入数据集名称': lambda: str(get_source_dataset_name()),
    '输入输入数据集id': lambda: str(get_source_dataset_id()),
    '输入mysql数据源id': lambda: str(get_dss_mysql_id()),
    '输入mysql数据源名称': lambda: str(get_dss_mysql_name()),
    '输入元数据命名规则名称': lambda: str(get_rule_name()),
    '输入元数据命名规则id': lambda: str(get_rule_id()),
    '输入元数据采集任务id': lambda: str(get_collect_schema_task_id()),
    '输入元数据采集任务名称': lambda: str(get_schema_collect_task_name()),
    '获取当前时间': lambda: str(get_current_time()),
    '输入输出数据集名称': lambda: get_sink_dataset_name(),
    '获取hdfs主键': lambda: str(get_HDFS_id()),
    '获取hdfs名称': lambda: str(get_HDFS_name())
}


def deal_parameters(data, request_method, request_url):
    if not data:
        log.info("输入数据为空，直接返回")
        return data

    while '随机数' in data:
        try:
            random_num = str(random.randint(0, 9999999999999))
            data = data.replace('随机数', random_num)
            log.debug(f"替换随机数为: {random_num}")
        except Exception as e:
            log.error(f"随机数替换失败: {str(e)}")
            break

    for dir_name, res_idx in DIRECTORY_MAP.items():
        if dir_name in data:
            try:
                res_id = get_resourceid(resource_type[res_idx])
                data = data.replace(dir_name, str(res_id))
                log.debug(f"替换目录[{dir_name}]为ID: {res_id}")
            except Exception as e:
                log.error(f"目录[{dir_name}]替换失败: {str(e)}")
                data = data.replace(dir_name, f"[{dir_name}_替换失败]")

    if '&&' in data and '输入' in data:
        try:
            select_sql, target_data = data.split('&&', 1)
            log.info(f"执行SQL查询: {select_sql}")
            query_result = ms.ExecuQuery(select_sql.encode('utf-8'))
            log.info(f"SQL查询结果: {query_result}")

            if not query_result:
                log.warning("SQL查询结果为空，无法替换'输入'")
                data = target_data
            else:
                replace_id = query_result[0]['id']
                data = target_data.replace('输入', str(replace_id))
                log.debug(f"SQL替换'输入'后的数据: {data}")
        except Exception as e:
            log.error(f"带'输入'的&&替换失败: {str(e)}")

    if '&&' in data:
        try:
            parts = data.split('&&')
            if request_method == "PUT" and len(parts) > 2:
                # PUT方法：处理租户主键、管理员主键及SQL替换
                target_data = parts[1]
                target_data = target_data.replace('租户主键', str(get_tenant_id()))
                target_data = target_data.replace('管理员主键', str(get_owner()))
                select_sql = parts[0]
                query_result = ms.ExecuQuery(select_sql.encode('utf-8'))
                replace_id = query_result[0]['id'] if query_result else "无结果"
                data = target_data.replace('输入', str(replace_id))
                log.debug(f"PUT方法&&替换后的数据: {data}")

            elif len(parts) > 2:
                for key, src_idx in [
                    ('数据源主键', 0), ('数据源名称', 1),
                    ('元数据主键', 2), ('元数据名称', 3),
                    ('数据集主键', 4), ('数据集名称', 5),
                    ('租户主键', 6), ('管理员主键', 7)
                ]:
                    if key in data:
                        replace_val = get_schema(data_source[src_idx], parts[2])
                        data = data.replace(key, str(replace_val))
                        log.debug(f"多部分&&替换[{key}]为: {replace_val}")
            else:
                for key, handler in [
                    ('数据源主键', lambda: get_datasource(data_source[0], parts[1])),
                    ('数据源名称', lambda: get_datasource(data_source[1], parts[1])),
                    ('标签主键', lambda: get_tags(tag_type[0], parts[1]))
                ]:
                    if key in data:
                        replace_val = handler()
                        data = data.replace(key, str(replace_val))
                        log.debug(f"两部分&&替换[{key}]为: {replace_val}")
        except Exception as e:
            log.error(f"通用&&分割逻辑处理失败: {str(e)}")

    if '输入驱动名称' in data:
        try:
            driver_name = get_driver_name()
            data = data.replace('输入驱动名称', str(driver_name))
            log.debug(f"替换'输入驱动名称'为: {driver_name}")
        except Exception as e:
            log.error(f"驱动名称替换失败: {str(e)}")
            data = data.replace('输入驱动名称', "[驱动名称替换失败]")

    if any(placeholder in data for placeholder in PLACEHOLDER_MAP):
        for placeholder, get_func in PLACEHOLDER_MAP.items():
            if placeholder in data:
                try:
                    replace_val = get_func()
                    data = data.replace(placeholder, str(replace_val))
                    log.debug(f"替换占位符[{placeholder}]为: {replace_val}")
                except Exception as e:
                    log.error(f"占位符[{placeholder}]替换失败: {str(e)}")
                    data = data.replace(placeholder, f"[{placeholder}_替换失败]")

    if 'SELECT name,id from' in data and '输入名称' in data and '输入id' in data and '&&' in data:
        try:
            select_sql, target_data = data.split('&&', 1)
            log.info(f"执行名称+ID查询SQL: {select_sql}")
            query_result = ms.ExecuQuery(select_sql.encode('utf-8'))
            log.info(f"名称+ID查询结果: {query_result}")

            if not query_result:
                log.warning("名称+ID查询结果为空，无法替换")
                data = target_data
            else:
                replace_id = query_result[0]['id']
                replace_name = query_result[0]['name']
                data = target_data.replace('输入id', str(replace_id)).replace('输入名称', str(replace_name))
                log.debug(f"名称+ID替换后的数据: {data}")
        except Exception as e:
            log.error(f"SELECT name,id替换失败: {str(e)}")

    if 'select id from' in data.lower():
        try:
            if '&&' in data:
                select_sql, target_data = data.split('&&', 1)
                log.info(f"执行ID查询SQL(带&&): {select_sql}")
                query_result = ms.ExecuQuery(select_sql.encode('utf-8'))
            else:
                select_sql = data
                log.info(f"执行ID查询SQL: {select_sql}")
                query_result = ms.ExecuQuery(select_sql.encode('utf-8'))
            log.info(f"ID查询结果: {query_result}")

            if not query_result:
                log.warning("ID查询结果为空")
                data = data
            else:
                ids = [str(item['id']) for item in query_result]
                if '/task/submitApproval' in request_url:
                    result_data = {
                        "status": "OFFLINE", "approverId": "", "approverName": "",
                        "ids": ids, "publishStatus": "OFFLINE"
                    }
                    data = json.dumps(result_data)
                elif "{}" in request_url and '/meta/poseidon/metrics/summary/exec/' in request_url:
                    data = ids[0] if ids else "[无ID结果]"
                else:
                    data = json.dumps(ids)
                log.debug(f"ID查询替换后的数据: {data}")
        except Exception as e:
            log.error(f"select id替换失败: {str(e)}")

    if 'select task_id from' in data.lower():
        try:
            log.info(f"执行task_id查询SQL: {data}")
            query_result = ms.ExecuQuery(data.encode('utf-8'))
            log.info(f"task_id查询结果: {query_result}")

            if not query_result:
                log.warning("task_id查询结果为空")
                return data

            task_ids = [item['task_id'] for item in query_result]
            if "{}" in request_url:
                data = task_ids[0] if task_ids else "[无task_id结果]"
            else:
                data = json.dumps(task_ids)
            log.debug(f"task_id查询替换后的数据: {data}")
        except Exception as e:
            log.error(f"select task_id替换失败: {str(e)}")

    return data


def deal_storage_parameters(data):
    """处理存储相关参数替换"""
    if not data:
        log.info("存储参数数据为空，直接返回")
        return data

    storage_map = {
        '获取表名': lambda: str(get_table_name()),
        '获取hdfs主键': lambda: str(get_HDFS_id()),
        '获取hdfs名称': lambda: str(get_HDFS_name())
    }

    for placeholder, get_func in storage_map.items():
        if placeholder in data:
            try:
                replace_val = get_func()
                data = data.replace(placeholder, replace_val)
                log.debug(f"存储参数替换[{placeholder}]为: {replace_val}")
            except Exception as e:
                log.error(f"存储参数[{placeholder}]替换失败: {str(e)}")
                data = data.replace(placeholder, f"[{placeholder}_替换失败]")

    return data