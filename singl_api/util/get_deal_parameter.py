import random

import time

from basic_info.setting import tenant_name, log, ms, job_view_name, schema_collect_name, collect_task_name, \
    qa_task_name, dsp_data_source_name, asset_name


def get_tenant_id():
    """
    :return: tenant_id
    """
    try:
        sql = "select id from merce_tenant where name='%s' order by create_time desc limit 1" % tenant_name
        tenant_id = ms.ExecuQuery(sql)
        tenant_id = tenant_id[0]["id"]
        return str(tenant_id)
    except Exception as e:
        return log.error("没有查询到租户id：%s" % e)


def get_owner():
    """
    :return: 管理员的id
    """
    tenant_id = get_tenant_id()
    try:
      sql = "select id from merce_user where tenant_id='%s' and login_id='admin'" % tenant_id
      user = ms.ExecuQuery(sql)
      owner = user[0]["id"]
      return str(owner)
    except Exception as e:
        return log.error("没有查询到管理员id：%s" % e)


def get_resourceid(resource_type):
    """
    获取数据源目录、数据集目录、元数据目录、flow目录、采集机、数据采集、数据存储、任务视图、数据资产、数据共享、数据安全、文件编目、数据标准根目录id、schema_collect_task_dir
    若确少resource_type,可以在setting配置文件里resource_type = ["datasource_dir","dataset_dir","schema_dir","flow_dir",
    "poseidon_collect_dir","poseidon_task_dir"....]添加。
    """
    tenant_id = get_tenant_id()
    try:
        if resource_type == 'schema_name_rule_dir' or resource_type == 'schema_collect_task_dir':
            sql = "select id from merce_resource_dir where  tenant_id='%s'  and res_type='%s' and parent_id is NULL" % (
                tenant_id, resource_type)
            resource_dir = ms.ExecuQuery(sql.encode('utf-8'))
            resource_id = resource_dir[0]["id"]
            return resource_id
        else:
            sql = "select id from merce_resource_dir where  tenant_id='%s' and creator='admin' and res_type='%s' and parent_id is NULL" % (
                tenant_id, resource_type)
            resource_dir = ms.ExecuQuery(sql.encode('utf-8'))
            resource_id = resource_dir[0]["id"]
            return resource_id
    except Exception as e:
        log.error("没有获取到目录id：%s" % e)


def get_datasource(data_source, data):
    """
    获取数据源id、name
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id,name from merce_dss where  tenant_id='%s' and name like '%s' ORDER BY create_time desc" % (
            tenant_id, data)
        datasource = ms.ExecuQuery(sql.encode('utf-8'))
        dss_id = datasource[0]["id"]
        dss_name = datasource[0]["name"]
        if data_source == "datasource_id":
            return dss_id
        elif data_source == "datasource_name":
            return dss_name
        else:
            log.error("数据源没有数据！")
    except Exception as e:
        log.error("没有获取到数据源id和名称：%s" % e)


def get_schema(data_source, data):
    """
    获取数据源id、数据源name、元数据id、元数据name、数据集id、数据集name、租户id、owner
    """
    tenant_id = get_tenant_id()
    try:
        sql = "SELECT ms.id as schema_id, ms.name as schema_name, ms.data_source_ids,ms.data_source_names ,ms.tenant_id,ms.owner,md.id as dataset_id,md.name as dataset_name FROM merce_schema ms left join merce_dataset md on ms.id = md.schema_id where  ms.tenant_id ='%s' and ms.name ='%s' order by md.create_time limit 1" % (
            tenant_id, data)
        schema = ms.ExecuQuery(sql.encode('utf-8'))
        schema_id = schema[0]["schema_id"]
        schema_name = schema[0]["schema_name"]
        dss_id = schema[0]["data_source_ids"]
        dss_name = schema[0]["data_source_names"]
        tenant_id = schema[0]["tenant_id"]
        owner = schema[0]["owner"]
        dataset_id = schema[0]["dataset_id"]
        dataset_name = schema[0]["dataset_name"]
        if data_source == "datasource_id":
            if ',' in dss_id:
                dss_id = str(dss_id).replace(',', '')
                return dss_id
            else:
                return dss_id
        elif data_source == "datasource_name":
            return dss_name
        if data_source == "schema_id":
            return schema_id
        elif data_source == "schema_name":
            return schema_name
        if data_source == "dataset_id":
            return dataset_id
        elif data_source == "dataset_name":
            return dataset_name
        if data_source == "tenant_id":
            return tenant_id
        elif data_source == "owner":
            return owner
        else:
            log.error("没有获取到相关数据！")
    except Exception as e:
        log.error("没有获取到数据源相关id和名称：%s" % e)


def get_dataset(data_source, data):
    """
    获取数据集id、name
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id,name from merce_dataset where  tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" % (
            tenant_id, data)
        dataset = ms.ExecuQuery(sql.encode('utf-8'))
        time.sleep(5)
        dataset_id = dataset[0]["id"]
        dataset_name = dataset[0]["name"]
        if data_source == "dataset_id":
            return dataset_id
        elif data_source == "dataset_name":
            return dataset_name
        elif data_source == "datasource_id":
            return dataset[0]["datasource_id"]
        elif data_source == "datasource_name":
            return dataset[0]["datasource_name"]
        else:
            log.error("数据集没有数据！")
    except Exception as e:
        log.error("没有获取到数据集id和名称：%s" % e)


def get_tags(tag_type, data):
    """
    获取标签
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from merce_tag where  tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" % (
            tenant_id, data)
        tag = ms.ExecuQuery(sql.encode('utf-8'))
        if tag:
            if tag_type == "EQUAL":
                tag_id = tag[0]["id"]
                return tag_id
            if tag_type == "like":
                tag_id = '%' + tag[0]["id"] + '%'
                return tag_id
            else:
                log.error("标签类型错误")
        else:
            log.error("标签没有数据！")
    except Exception as e:
        log.error("没有获取到标签id：%s" % e)

def get_job_view_id():
    """
    获取视图任务id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from s_v_job_view where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,job_view_name)
        job_view = ms.ExecuQuery(sql.encode('utf-8'))
        job_view_id = job_view[0]["id"]
        return str(job_view_id)
    except Exception as e:
        log.error("没有获取到视图任务id：%s" % e)

def get_schema_collect_id():
    """
    获取元数据采集任务id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from schema_collect_task where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,schema_collect_name)
        schema_collect = ms.ExecuQuery(sql.encode('utf-8'))
        schema_collect_id = schema_collect[0]["id"]
        return str(schema_collect_id)
    except Exception as e:
        log.error("没有获取到元数据采集任务id：%s" % e)

def get_offline_collect_task_id():
    """
    获取数据采集任务id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from poseidon_task where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,collect_task_name)
        poseidon_task = ms.ExecuQuery(sql.encode('utf-8'))
        collect_id = poseidon_task[0]["id"]
        return str(collect_id)
    except Exception as e:
        log.error("没有获取到数据采集任务id：%s" % e)

"""获取采集的各种参数"""


def get_source_dss_id():
    tenant_id = get_tenant_id()
    try:
        sql = "select id,name from merce_dss where  tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc" % (
            tenant_id, 'test_api_wjp')
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        dss_id = query_data[1]["id"]
        return dss_id
    except Exception as e:
        log.error("没有获取到输入端数据源id：%s" % e)


def get_source_dss_name():
    tenant_id = get_tenant_id()
    try:
        sql = "select id,name from merce_dss where  tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc" % (
            tenant_id, 'test_api_wjp')
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        dss_name = query_data[1]["name"]
        return dss_name
    except Exception as e:
        log.error("没有获取到输入端数据源名称：%s" % e)


def get_sink_dss_id():
    tenant_id = get_tenant_id()
    try:
        sql = "select id,name from merce_dss where  tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" % (
            tenant_id, 'test_api_wjp')
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        dss_id = query_data[0]["id"]
        return dss_id
    except Exception as e:
        log.error("没有获取到输出端数据源id：%s" % e)


def get_sink_dss_name():
    tenant_id = get_tenant_id()
    try:
        sql = "select id,name from merce_dss where  tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" % (
            tenant_id, 'test_api_wjp')
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        dss_name = query_data[0]["name"]
        return dss_name
    except Exception as e:
        log.error("没有获取到输出端数据源id：%s" % e)


def get_draft_id():
    try:
        sql = "select id from poseidon_task_draft where name like '%s%%%%' ORDER BY create_time desc LIMIT 1" % (
            'test_api_wjp_collect')
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        draft_id = query_data[0]["id"]
        return draft_id
    except Exception as e:
        log.error("没有获取到画布id：%s" % e)


def get_source_schema_id():
    tenant_id = get_tenant_id()
    try:
        sql = "select id,name from merce_schema where  tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" % (
            tenant_id, 'test_api_wjp_schema')
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        schema_id = query_data[0]["id"]
        return schema_id
    except Exception as e:
        log.error("没有获取到输入端元数据id：%s" % e)


def get_source_schema_name():
    tenant_id = get_tenant_id()
    try:
        sql = "select id,name from merce_schema where  tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" % (
            tenant_id, 'test_api_wjp_schema')
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        schema_name = query_data[0]["name"]
        return schema_name
    except Exception as e:
        log.error("没有获取到输入端元数据名称：%s" % e)


def get_sink_schema_name():
    tenant_id = get_tenant_id()
    try:
        sql = "select name from merce_schema ms where tenant_id={} and name like 'test_api_wjp_schema%-sink%' ORDER BY create_time desc LIMIT 1".format(
            tenant_id)
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        schema_name = query_data[0]["name"]
        return schema_name
    except Exception as e:
        log.error("没有获取到输出端元数据名称：%s" % e)


def get_sink_schema_name_and_random():
    try:
        schema_name = 'test_api_wjp_schema-sink-'
        random_data = str(random.randint(0, 9999999999999))
        schema_name_and_random = schema_name + random_data
        return schema_name_and_random
    except Exception as e:
        log.error("没有获取到输出端元数据名称：%s" % e)


def get_sink_dataset_name_and_random():
    try:
        dataset_name = 'test_api_wjp_dataset-output-'
        random_data = str(random.randint(0, 9999999999999))
        dataset_name_and_random = dataset_name + random_data
        return dataset_name_and_random
    except Exception as e:
        log.error("没有获取到输出端元数据名称：%s" % e)


def get_sink_schema_id():
    tenant_id = get_tenant_id()
    try:
        sql = "select id from merce_schema ms where tenant_id={} and name like 'test_api_wjp_schema%-sink%' ORDER BY create_time desc LIMIT 1".format(
            tenant_id)
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        schema_id = query_data[0]["id"]
        return schema_id
    except Exception as e:
        log.error("没有获取到输出端元数据id：%s" % e)


def get_source_node_id():
    """获取输入端数据源uuid‘"""
    try:
        sql = "select id from poseidon_draft_node_field where task_id = (select id from poseidon_task where name like '%s%%%%' ORDER BY create_time desc LIMIT 1) and type = 0" % (
            'test_api_wjp_collect')
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        source_node_id = query_data[0]["id"]
        return source_node_id
    except Exception as e:
        log.error("没有获取到输入端数据源uuid：%s" % e)


def get_sink_node_id():
    """获取输出端数据源uuid‘"""
    try:
        sql = "select id from poseidon_draft_node_field where task_id = (select id from poseidon_task where name like '%s%%%%' ORDER BY create_time desc LIMIT 1) and type = 1" % (
            'test_api_wjp_collect')
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        sink_node_id = query_data[0]["id"]
        return sink_node_id
    except Exception as e:
        log.error("没有获取到输出端数据源uuid：%s" % e)


def get_collect_task_id():
    """获取采集任务id‘"""
    tenant_id = get_tenant_id()
    try:
        sql = "SELECT task_id FROM poseidon_task_draft WHERE tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc LIMIT 1" % (
            tenant_id, 'test_api_wjp_collect')
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        task_id = query_data[0]["task_id"]
        return task_id
    except Exception as e:
        log.error("没有获取到采集任务id：%s" % e)


def get_collect_schema_task_id():
    """获取元数据采集任务id‘"""
    tenant_id = get_tenant_id()
    try:
        sql = "select id from schema_collect_task  where tenant_id='%s' and name like '%s%%%%' order by create_time desc limit 1" % (
            tenant_id, 'api_wjp_schema-collect')
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        task_id = query_data[0]["id"]
        return task_id
    except Exception as e:
        log.error("没有获取到采集任务id：%s" % e)


def get_sink_dataset_name():
    """获取采集输出端数据集名称"""
    tenant_id = get_tenant_id()
    try:
        sql = "SELECT name from merce_dataset  where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc" % (
            tenant_id, 'test_api_wjp_dataset%output%')
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        if len(query_data) == 0:
            dataset_name = ''
            return dataset_name
        else:
            dataset_name = query_data[0]["name"]
        return dataset_name

    except Exception as e:
        log.error("没有获取到输出端数据集名称：%s" % e)


def get_sink_dataset_id():
    """获取采集输出端数据集id"""
    tenant_id = get_tenant_id()
    try:
        sql = "SELECT id from merce_dataset  where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc" % (
            tenant_id, 'test_api_wjp_dataset%output%')
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        dataset_id = query_data[0]["id"]
        return dataset_id
    except Exception as e:
        log.error("没有获取到输出端数据集名称：%s" % e)


def get_source_dataset_name():
    """获取采集输入端数据集名称"""
    tenant_id = get_tenant_id()
    try:
        sql = "SELECT name from merce_dataset  where tenant_id= {} and name like 'test_wjp_dataset%' ORDER BY create_time desc limit 1".format(
            tenant_id)
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        dataset_name = query_data[0]["name"]
        return dataset_name
    except Exception as e:
        log.error("没有获取到输入端数据集名称：%s" % e)


def get_source_dataset_id():
    """获取采集输入端数据集id"""
    tenant_id = get_tenant_id()
    try:
        sql = "SELECT id from merce_dataset  where tenant_id= {} and name like 'test_wjp_dataset%' ORDER BY create_time desc limit 1".format(
            tenant_id)
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        dataset_id = query_data[0]["id"]
        return dataset_id
    except Exception as e:
        log.error("没有获取到输入端数据集名称：%s" % e)


def get_collector_group_id():
    """获取采集组id"""
    tenant_id = get_tenant_id()
    try:
        sql = "SELECT id from merce_resource_dir where tenant_id={} and name like 'test_api_wjp_collector_group%' GROUP BY create_time desc limit 1".format(
            tenant_id)
        query_data = ms.ExecuQuery(sql.encode('utf-8'))
        collector_group_id = query_data[0]["id"]
        return collector_group_id
    except Exception as e:
        log.error("没有获取到采集组id：%s" % e)


def get_collector_group_name():
    """获取采集组名称"""
    tenant_id = get_tenant_id()
    try:
        sql = "SELECT name from merce_resource_dir where tenant_id={} and name like 'test_api_wjp_collector_group%' GROUP BY create_time desc limit 1".format(
            tenant_id)
        get_collector_group_name = ms.ExecuQuery(sql.encode('utf-8'))
        collector_group_name = get_collector_group_name[0]["name"]
        return collector_group_name
    except Exception as e:
        log.error("没有获取到采集组名称：%s" % e)


def get_collector_id():
    """获取采集机id"""
    try:
        sql = "SELECT id from poseidon_collecter  where  name like 'test_api_wjp_collecter%'GROUP BY create_time desc limit 1"
        get_collector_id = ms.ExecuQuery(sql.encode('utf-8'))
        collector_id = get_collector_id[0]["id"]
        return collector_id
    except Exception as e:
        log.error("没有获取到采集机id：%s" % e)


def get_user_id():
    """获取user_id"""
    tenant_id = get_tenant_id()
    try:
        sql = "select id from merce_user where tenant_id={} and name = 'admin'".format(tenant_id)
        get_user_id = ms.ExecuQuery(sql.encode('utf-8'))
        user_id = get_user_id[0]["id"]
        return user_id
    except Exception as e:
        log.error("没有获取到采集机id：%s" % e)


def get_global_variable():
    """获取变量id"""
    tenant_id = get_tenant_id()
    try:
        sql = "select id from poseidon_global_variable where tenant_id='%s' and name like '%s%%%%'GROUP BY create_time desc limit 1" % (
            tenant_id, 'test_collect_time_variable')
        get_collector_id = ms.ExecuQuery(sql.encode('utf-8'))
        collector_id = get_collector_id[0]["id"]
        return collector_id
    except Exception as e:
        log.error("没有获取到获取变量id：%s" % e)


def get_dss_mysql_id():
    """获取mysql数据源id"""
    tenant_id = get_tenant_id()
    try:
        sql = "select id from merce_dss where tenant_id='%s' and name like '%s%%%%' order by create_time desc limit 1" % (
            tenant_id, 'test_api_wjp_mysql')
        get_dss_mysql_id = ms.ExecuQuery(sql.encode('utf-8'))
        dss_mysql_id = get_dss_mysql_id[0]["id"]
        return dss_mysql_id
    except Exception as e:
        log.error("没有获取到获取变量id：%s" % e)


def get_dss_mysql_name():
    """获取mysql数据源名称"""
    tenant_id = get_tenant_id()
    try:
        sql = "select name from merce_dss where tenant_id='%s' and  name like '%s%%%%' order by create_time desc limit 1" % (
            tenant_id, 'test_api_wjp_mysql')
        get_dss_mysql_name = ms.ExecuQuery(sql.encode('utf-8'))
        dss_mysql_name = get_dss_mysql_name[0]["name"]
        return dss_mysql_name
    except Exception as e:
        log.error("没有获取到获取变量id：%s" % e)


def get_rule_name():
    """获取元数据采集命名规则名称"""
    tenant_id = get_tenant_id()
    try:
        sql = "select name from schema_collect_name_rule where tenant_id='%s' and name like '%s%%%%' order by create_time desc limit 1" % (
            tenant_id, 'api_wjp_schema-collect')
        get_rule_name = ms.ExecuQuery(sql.encode('utf-8'))
        rule_name = get_rule_name[0]["name"]
        return rule_name
    except Exception as e:
        log.error("没有获取到获取变量id：%s" % e)


def get_rule_id():
    """获取元数据采集命名规则id"""
    tenant_id = get_tenant_id()
    try:
        sql = "select id from schema_collect_name_rule where tenant_id='%s' and name like '%s%%%%' order by create_time desc limit 1" % (
            tenant_id, 'api_wjp_schema-collect')
        get_rule_id = ms.ExecuQuery(sql.encode('utf-8'))
        rule_id = get_rule_id[0]["id"]
        return rule_id
    except Exception as e:
        log.error("没有获取到获取变量id：%s" % e)


def get_schema_collect_task_name():
    """获取元数据采集任务名称"""
    tenant_id = get_tenant_id()
    try:
        sql = "select name from schema_collect_task  where tenant_id='%s' and name like '%s%%%%' order by create_time desc limit 1" % (
            tenant_id, 'api_wjp_schema-collect')
        get_schema_collect_task_name = ms.ExecuQuery(sql.encode('utf-8'))
        schema_collect_task_name = get_schema_collect_task_name[0]["name"]
        return schema_collect_task_name
    except Exception as e:
        log.error("没有获取到获取变量id：%s" % e)


def get_current_time():
    current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    return current_time


import json


def get_table_name():
    """获取采集输出端表名"""
    sql = "select properties from poseidon_draft_node_prop where  task_id =(select task_id FROM poseidon_task_draft WHERE  name like '%s%%%%' ORDER BY create_time desc LIMIT 1) and draft_id ='0' and type = 1" % (
        'test_api_wjp_collect')
    properties = ms.ExecuQuery(sql.encode('utf-8'))
    table_name = json.loads(properties[0]['properties'])["tableName"]
    return table_name


def get_HDFS_id():
    """获取最新创建的HDFS数据源id"""
    tenant_id = get_tenant_id()
    sql = "SELECT id from merce_dss where tenant_id='%s' and name like '%s%%%%' GROUP BY create_time desc limit 1" % (
        tenant_id, 'test_api_wjp_hdfs')
    hdfs_ids = ms.ExecuQuery(sql.encode('utf-8'))
    hdfs_id = hdfs_ids[0]["id"]
    return hdfs_id


def get_HDFS_name():
    """获取最新创建的HDFS数据源名称"""
    tenant_id = get_tenant_id()
    sql = "SELECT name from merce_dss where tenant_id='%s' and name like '%s%%%%' GROUP BY create_time desc limit 1" % (
        tenant_id, 'test_api_wjp_hdfs')
    hdfs_names = ms.ExecuQuery(sql.encode('utf-8'))
    hdfs_name = hdfs_names[0]["name"]
    return hdfs_name
def get_qa_task_id():
    """
    获取质检任务id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from qa_job where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,qa_task_name)
        qa_task = ms.ExecuQuery(sql.encode('utf-8'))
        qa_task_id = qa_task[0]["id"]
        return str(qa_task_id)
    except Exception as e:
        log.error("没有获取到数据质量任务id：%s" % e)


def get_dsp_data_resource(dsp_data_source):
    """
    获取数据资源id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id,name from dsp_data_resource where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,dsp_data_source_name)
        dsp_data_resource = ms.ExecuQuery(sql.encode('utf-8'))
        dsp_data_resource_id = dsp_data_resource[0]["id"]
        dsp_data_resource_name = dsp_data_resource[0]["name"]
        if dsp_data_source=="data_source_id":
           return str(dsp_data_resource_id)
        if dsp_data_source=="data_source_name":
           return str(dsp_data_resource_name)
    except Exception as e:
        log.error("没有获取到数据资源id：%s" % e)


def get_dsp_data_application():
    """
    获取数据资源申请工单id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from dsp_data_application where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,dsp_data_source_name)
        dsp_data_application = ms.ExecuQuery(sql.encode('utf-8'))
        dsp_data_application_id = dsp_data_application[0]["id"]
        return str(dsp_data_application_id)
    except Exception as e:
        log.error("没有获取到数据资源申请工单id：%s" % e)

def get_sys_approval_target(approval_type):
    """
    获取审批模块id
    若确少code"....]添加。
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from sys_approval_target where tenant_id='%s' and code='%s' and type ='INTERNAL'" % (tenant_id, approval_type)
        approval_type = ms.ExecuQuery(sql.encode('utf-8'))
        approval_type_id = approval_type[0]["id"]
        return str(approval_type_id)
    except Exception as e:
        log.error("没有获取到审批模块id：%s" % e)


def get_organization():
    """
    :return: 组织机构的id
    """
    tenant_id = get_tenant_id()
    try:
      sql = "select id from sys_organization where tenant_id='%s' and code='ROOT'" % tenant_id
      sys_organization = ms.ExecuQuery(sql)
      sys_organization_id = sys_organization[0]["id"]
      return str(sys_organization_id)
    except Exception as e:
        return log.error("没有查询组织机构id：%s" % e)

def get_asset_id():
    """
    获取资产id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from assets_info where tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,asset_name)
        assets_info = ms.ExecuQuery(sql.encode('utf-8'))
        assets_info_id = assets_info[0]["id"]
        return str(assets_info_id)
    except Exception as e:
        log.error("没有获取到资产id：%s" % e)


def get_approval_record():
    """
    获取资产审批id
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from sys_approval_record where tenant_id='%s' and approval_status='PENDING' and target_name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,asset_name)
        assets_info = ms.ExecuQuery(sql.encode('utf-8'))
        assets_info_id = assets_info[0]["id"]
        return str(assets_info_id)
    except Exception as e:
        log.error("没有获取到资产审核id：%s" % e)