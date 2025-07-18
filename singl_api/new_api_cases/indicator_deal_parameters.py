import json
import random
from basic_info.setting import resource_type, data_source, tag_type, ms, log
from util.get_deal_parameter import (
    get_resourceid, get_schema, get_tags, get_datasource,
    get_dss_id, get_dss_name, get_source_schema_id, get_source_schema_name,
    get_source_dataset_name, get_tenant_id, get_owner, get_user_id,
    get_source_dataset_id, get_indicator_id, get_current_time, get_driver_name,
    get_indicator_subtask_id, get_indicator_dir_id, get_indicator_dimdir_id,
    get_indicator_dim_name, get_indicator_dim_id, get_indicator_name, get_indicator_dir_name
)


def replace_placeholders(data):
    """替换数据中所有预定义的占位符（如'输入输入数据源id'等）"""
    replacements = {
        '输入输入数据源id': lambda: str(get_dss_id()),
        '输入输入数据源名称': get_dss_name,
        '输入输入元数据id': lambda: str(get_source_schema_id()),
        '输入输入元数据名称': get_source_schema_name,
        '输入输入数据集名称': lambda: str(get_source_dataset_name()),
        '输入输入数据集id': lambda: str(get_source_dataset_id()),
        '租户主键': lambda: str(get_tenant_id()),
        '管理员主键': lambda: str(get_owner()),
        '用户id': lambda: str(get_user_id()),
        '输入第一个数据集名称': lambda: str(get_source_dataset_name(1)),
        '输入第一个数据集id': lambda: str(get_source_dataset_id(1)),
        '输入指标任务id': lambda: str(get_indicator_id()),
        '输入指标任务名称': lambda: str(get_indicator_name()),
        '输入指标任务第二个id': lambda: str(get_indicator_id(1)),
        '输入指标任务第二个名称': lambda: str(get_indicator_name(1)),
        '输入指标子任务id': lambda: str(get_indicator_subtask_id()),
        '输入指标目录id': lambda: str(get_indicator_dir_id()),
        '输入指标目录名称': lambda: str(get_indicator_dir_name()),
        '输入维度目录id': lambda: str(get_indicator_dimdir_id()),
        '输入指标维度id': lambda: str(get_indicator_dim_id()),
        '输入指标维度名称': lambda: str(get_indicator_dim_name()),
        '获取当前时间': lambda: str(get_current_time())
    }

    request_data = data
    for placeholder, func in replacements.items():
        if placeholder in request_data:
            try:
                # 执行替换函数（注意函数是否需要调用）
                replacement_value = func() if callable(func) else func
                request_data = request_data.replace(placeholder, replacement_value)
            except Exception as e:
                log.error(f"替换占位符失败：{placeholder}，错误：{str(e)}")
                request_data = request_data.replace(placeholder, "REPLACEMENT_FAILED")  # 标记替换失败
    return request_data


# 目录替换映射表：统一管理"XX目录"与资源类型的对应关系
DIRECTORY_REPLACEMENTS = {
    '数据源目录': resource_type[0],
    '数据集目录': resource_type[1],
    '元数据目录': resource_type[2],
    '数据计算目录': resource_type[3],
    '采集机目录': resource_type[4],
    '数据采集目录': resource_type[5],
    '数据存储目录': resource_type[6],
    '任务视图目录': resource_type[7],
    '数据资产目录': resource_type[8],
    '数据共享目录': resource_type[9],
    '数据安全目录': resource_type[10],
    '文件编目目录': resource_type[11],
    '数据标准目录': resource_type[12],
    '元数据名称规则目录': resource_type[15],
    '元数据采集任务目录': resource_type[13],
    '标签管理目录': resource_type[16],
    '指标管理目录': resource_type[17],
    '指标维度目录': resource_type[18]
}


def deal_parameters(data, request_method, request_url):
    if not data:
        log.info("输入数据为空，直接返回")
        return data

    # 1. 处理随机数替换（循环处理直到无随机数，避免递归）
    while '随机数' in data:
        data = data.replace('随机数', str(random.randint(0, 9999999999999)))
        log.debug(f"替换随机数后的数据：{data}")

    # 2. 处理目录替换（通过映射表批量处理，消除重复if）
    for dir_name, res_type in DIRECTORY_REPLACEMENTS.items():
        if dir_name in data:
            try:
                resource_id = get_resourceid(res_type)
                data = data.replace(dir_name, str(resource_id))
                log.debug(f"替换目录[{dir_name}]为ID：{resource_id}")
            except Exception as e:
                log.error(f"获取目录[{dir_name}]的资源ID失败：{str(e)}")
                data = data.replace(dir_name, "DIRECTORY_ID_FAILED")

    # 3. 处理带"输入"的SQL查询替换（&&分割的特殊逻辑）
    if '&&' in data and '输入' in data:
        try:
            select_data, request_data = data.split('&&', 1)  # 只分割一次，避免多&&场景错误
            log.info(f"执行SQL查询：{select_data}")
            data_select_result = ms.ExecuQuery(select_data.encode('utf-8'))
            log.info(f"SQL查询结果：{data_select_result}")
            if not data_select_result:
                log.warning("SQL查询结果为空，无法替换'输入'")
                return request_data
            # 取第一条结果的id替换
            replace_value = data_select_result[0]['id']
            data = request_data.replace('输入', str(replace_value))
            log.debug(f"SQL替换'输入'后的数据：{data}")
        except Exception as e:
            log.error(f"处理带'输入'的SQL替换失败：{str(e)}")
            return data  # 保留原始数据，避免中断

    # 4. 处理通用&&分割逻辑（按请求方法和参数类型处理）
    if '&&' in data:
        try:
            parts = data.split('&&')
            if request_method == "PUT" and len(parts) > 2:
                # PUT方法特殊处理：替换租户主键、管理员主键
                request_data = parts[1]
                request_data = request_data.replace('租户主键', str(get_tenant_id()))
                request_data = request_data.replace('管理员主键', str(get_owner()))
                # SQL查询结果替换
                select_data = parts[0]
                data_select_result = ms.ExecuQuery(select_data.encode('utf-8'))
                replace_value = data_select_result[0]['id'] if data_select_result else "NO_RESULT"
                data = request_data.replace('输入', str(replace_value))
                log.debug(f"PUT方法&&替换后的数据：{data}")
            elif len(parts) > 2:
                # 多部分&&处理：数据源/元数据/数据集等主键/名称替换
                for key, src_type in [
                    ('数据源主键', data_source[0]), ('数据源名称', data_source[1]),
                    ('元数据主键', data_source[2]), ('元数据名称', data_source[3]),
                    ('数据集主键', data_source[4]), ('数据集名称', data_source[5]),
                    ('租户主键', data_source[6]), ('管理员主键', data_source[7])
                ]:
                    if key in data:
                        replace_value = get_schema(src_type, parts[2])
                        data = data.replace(key, str(replace_value))
                        log.debug(f"替换[{key}]为：{replace_value}")
            else:
                # 两部分&&处理：数据源/标签主键替换
                for key, handler in [
                    ('数据源主键', lambda: get_datasource(data_source[0], parts[1])),
                    ('数据源名称', lambda: get_datasource(data_source[1], parts[1])),
                    ('标签主键', lambda: get_tags(tag_type[0], parts[1]))
                ]:
                    if key in data:
                        try:
                            replace_value = handler()
                            data = data.replace(key, str(replace_value))
                            log.debug(f"替换[{key}]为：{replace_value}")
                        except Exception as e:
                            log.error(f"替换[{key}]失败：{str(e)}")
                            data = data.replace(key, f"{key}_FAILED")
        except Exception as e:
            log.error(f"处理通用&&分割逻辑失败：{str(e)}")

    # 5. 处理驱动名称替换
    if '输入驱动名称' in data:
        try:
            data = data.replace('输入驱动名称', str(get_driver_name()))
            log.debug("替换'输入驱动名称'成功")
        except Exception as e:
            log.error(f"替换'输入驱动名称'失败：{str(e)}")
            data = data.replace('输入驱动名称', "DRIVER_NAME_FAILED")

    # 6. 处理带"输入名称"和"输入id"的SQL查询替换
    if 'SELECT name,id from' in data and '输入名称' in data and '输入id' in data and '&&' in data:
        try:
            select_data, request_data = data.split('&&', 1)
            log.info(f"执行名称+ID查询SQL：{select_data}")
            data_select_result = ms.ExecuQuery(select_data.encode('utf-8'))
            if not data_select_result:
                log.warning("名称+ID查询结果为空")
                return request_data
            # 替换名称和ID
            replace_id = data_select_result[0]['id']
            replace_name = data_select_result[0]['name']
            data = request_data.replace('输入id', str(replace_id)).replace('输入名称', str(replace_name))
            log.debug(f"替换名称和ID后的数据：{data}")
        except Exception as e:
            log.error(f"处理名称+ID的SQL替换失败：{str(e)}")

    # 7. 处理select id from的SQL查询替换
    if 'select id from' in data.lower():  # 忽略大小写
        try:
            if '&&' in data:
                select_data, request_data = data.split('&&', 1)
                log.info(f"执行ID查询SQL（带&&）：{select_data}")
                data_select_result = ms.ExecuQuery(select_data.encode('utf-8'))
            else:
                select_data = data
                log.info(f"执行ID查询SQL：{select_data}")
                data_select_result = ms.ExecuQuery(select_data.encode('utf-8'))

            log.info(f"ID查询结果：{data_select_result}")
            if not data_select_result:
                log.warning("ID查询结果为空")
                return data

            # 提取所有id
            ids = [str(item['id']) for item in data_select_result]
            # 特殊URL处理
            if '/meta/poseidon/task/submitApproval' in request_url:
                request_data = {
                    "status": "OFFLINE", "approverId": "", "approverName": "",
                    "ids": ids, "publishStatus": "OFFLINE"
                }
                data = json.dumps(request_data)
            elif "{}" in request_url and '/api/ind/indicator/execute/' in request_url:
                # 取第一个id作为路径参数
                data = ids[0] if ids else "NO_ID"
            else:
                # 普通场景：返回id列表的JSON
                data = json.dumps(ids)
            log.debug(f"ID查询替换后的数据：{data}")
        except Exception as e:
            log.error(f"处理select id from的SQL替换失败：{str(e)}")

    # 8. 处理select task_id from的SQL查询替换
    if 'select task_id from' in data.lower():  # 忽略大小写
        try:
            log.info(f"执行task_id查询SQL：{data}")
            data_select_result = ms.ExecuQuery(data.encode('utf-8'))
            log.info(f"task_id查询结果：{data_select_result}")
            if not data_select_result:
                log.warning("task_id查询结果为空")
                return data

            # 提取所有task_id
            task_ids = [item['task_id'] for item in data_select_result]
            # 特殊URL处理
            if "{}" in request_url:
                data = task_ids[0] if task_ids else "NO_TASK_ID"
            else:
                data = json.dumps(task_ids)
            log.debug(f"task_id查询替换后的数据：{data}")
        except Exception as e:
            log.error(f"处理select task_id from的SQL替换失败：{str(e)}")

    # 9. 执行通用占位符替换（最后执行，避免被其他逻辑覆盖）
    data = replace_placeholders(data)

    return data