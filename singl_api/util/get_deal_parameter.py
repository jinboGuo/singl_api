from basic_info.setting import MySQL_CONFIG, tenant_name, log, ms
from util.Open_DB import MYSQL



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
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"], MySQL_CONFIG["PORT"])
    try:
      sql = "select id from merce_user where tenant_id='%s' order by create_time desc" % tenant_id
      user = ms.ExecuQuery(sql)
      owner = user[0]["id"]
      return str(owner)
    except Exception as e:
        return log.error("没有查询到管理员id：%s" % e)

def get_resourceid(resource_type):
    """
    获取数据源目录、数据集目录、元数据目录、flow目录、采集机、数据采集、数据存储、任务视图、数据资产、数据共享、数据安全、文件编目、数据标准根目录id
    若确少resource_type,可以在setting配置文件里resource_type = ["datasource_dir","dataset_dir","schema_dir","flow_dir",
    "poseidon_collect_dir","poseidon_task_dir"....]添加。
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from merce_resource_dir where  tenant_id='%s' and creator='admin' and res_type='%s' and parent_id is NULL"%(tenant_id,resource_type)
        resource_dir = ms.ExecuQuery(sql.encode('utf-8'))
        resource_id = resource_dir[0]["id"]
        return resource_id
    except Exception as e:
        log.error("没有获取到目录id：%s" % e)

def get_datasource(data_source,data):
    """
    获取数据源id、name
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id,name from merce_dss where  tenant_id='%s' and name ='%s' ORDER BY create_time desc" %(tenant_id,data)
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


def get_schema(data_source,data):
    """
    获取数据源id、数据源name、元数据id、元数据name、数据集id、数据集name、租户id、owner
    """
    tenant_id = get_tenant_id()
    try:
        sql = "SELECT ms.id as schema_id, ms.name as schema_name, ms.data_source_ids,ms.data_source_names ,ms.tenant_id,ms.owner,md.id as dataset_id,md.name as dataset_name FROM merce_schema ms left join merce_dataset md on ms.id = md.schema_id where  ms.tenant_id ='%s' and ms.name ='%s' order by md.create_time limit 1" %(tenant_id,data)
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


def get_dataset(data_source,data):
    """
    获取数据源id、name
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id,name from merce_dataset where  tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,data)
        dataset = ms.ExecuQuery(sql.encode('utf-8'))
        dataset_id = dataset[0]["id"]
        dataset_name = dataset[0]["name"]
        if data_source == "dataset_id":
            return dataset_id
        elif data_source == "dataset_name":
            return dataset_name
        else:
            log.error("数据集没有数据！")
    except Exception as e:
        log.error("没有获取到数据集id和名称：%s" % e)


def get_tags(tag_type,data):
    """
    获取数据源id、name
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from merce_tag where  tenant_id='%s' and name like '%s%%%%' ORDER BY create_time desc limit 1" %(tenant_id,data)
        tag = ms.ExecuQuery(sql.encode('utf-8'))
        if tag:
           if tag_type == "EQUAL":
               tag_id=tag[0]["id"]
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


