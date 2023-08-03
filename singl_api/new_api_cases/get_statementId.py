import requests, json
from basic_info.get_auth_token import get_headers
from util.Open_DB import MYSQL
from util.logs import Logger
from basic_info.setting import tenant_id_145, tenant_id_62, tenant_id_65, MySQL_CONFIG, \
    tenant_id_61, tenant_id_95, tenant_id_220
from new_api_cases.dw_prepare_datas import sql_analyse_data
from new_api_cases.prepare_datas_for_cases import dataset_data
from util.format_res import dict_res

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"], MySQL_CONFIG["PORT"])

log = Logger().get_log()
# 根据host信息返回tenant信息
def get_tenant(host):
    global tenant_id
    if '62' in host:
        tenant_id = tenant_id_62
        return tenant_id
    elif '95' in host:
        tenant_id = tenant_id_95
        return tenant_id
    elif '220' in host:
        tenant_id = tenant_id_220
        return tenant_id
    elif '61' in host:
        tenant_id = tenant_id_61
        return tenant_id
    elif '65' in host:
        tenant_id = tenant_id_65
        return tenant_id
    elif '145' in host:
        tenant_id = tenant_id_145
        return tenant_id
    else:
        log.info('使用的host不在预期中，请确认host信息')
        return


# datasetId存在时
def statementId(host, param):
    from new_api_cases.prepare_datas_for_cases import dataset_data
    try:
        new_data = dataset_data(param)
        url = '%s/api/datasets/previewinit?rows=50'%(host)
        new_data = json.dumps(new_data, separators=(',', ':'))
        res = requests.post(url=url, headers=get_headers(host), data=new_data)
        res_statementId = json.loads(res.text)
        statementId = res_statementId['statementId']
        return statementId, new_data
    except:
        return


def statementId_flow_use(host, datasetId):
    url = '%s/api/datasets/%s/previewinit?rows=50' % (host,datasetId)
    log.info("statementId_flow_use请求url=%s" % url)
    res = requests.get(url=url, headers=get_headers(host))
    log.info("response data：%s %s" % (res.status_code, res.text))
    try:
        res_statementId = dict_res(res.text)
        log.info('%s数据集获取的statementID信息：%s' % (datasetId, res_statementId))
        statementId = res_statementId['statementId']
    except:
        log.info('数据集%s的statementID返回空' % datasetId)
        return 1
    else:
        return statementId

def statementId_flow_output_use(host, datasetId):
    url = '%s/api/datasets/%s/previewinit?rows=50' % (host, datasetId)
    res = requests.get(url=url, headers=get_headers(host))
    try:
        res_statementId = dict_res(res.text)
        statementId = res_statementId['statementId']
        log.info('%s数据集获取的statementID：%s' % (datasetId, statementId))
    except:
        log.info('数据集%s的statementID返回空' % datasetId)
        return 0
    else:
        return statementId

def preview_result_flow_use(host, datasetId, statementID):
    if isinstance(statementID, int):
        url = "%s/api/datasets/%s/previewresult?statementId=%d&clusterId=cluster1" % (host, datasetId, statementID)
        res = requests.get(url=url, headers=get_headers(host))
        log.info('%s数据集preview_result:%s' % (datasetId, res.text))
        while 'waiting' in res.text or 'running' in res.text:
            res = requests.get(url=url, headers=get_headers(host))
        try:
            dataset_result = dict_res(res.text)['content']
        except:
            return 0
        else:
            log.info('%s数据集dataset_result: %s ' % (datasetId, dataset_result))
            return dataset_result
    else:
        log.info('数据集返回的statementID为空')


# datasetId不存在时
def statementId_no_dataset(host, param):
    new_data = dataset_data(param)
    new_data = json.dumps(new_data, separators=(',', ':'))
    url = '%s/api/datasets/new/previewinit?rows=50' % host
    res = requests.get(url=url, headers=get_headers(host))
    try:
        res_statementId = json.loads(res.text)
        statementId = res_statementId['statementId']
        return statementId, new_data
    except KeyError as e:
        log.error("datasetId不存在{}".format(e))
        return



def get_sql_analyse_statement_id(host, param):
    """
    初始化Sql Analyze(解析数据集输出字段)，返回statement id，获取数据集字段给分析任务使用
    :param host:
    :param param:
    :return:
    """
    url = ' %s/api/datasets/sql/analyzeinit' % host
    param = sql_analyse_data(param)
    new_data = json.dumps(param, separators=(',', ':'))
    res = requests.post(url=url, headers=get_headers(host), data=new_data)
    try:
        res_statementId = json.loads(res.text)
        sql_analyse_statement_id = res_statementId['statementId']
        return sql_analyse_statement_id
    except KeyError:
        return



def get_sql_analyse_dataset_info(host, params):
    """
    根据初始化SQL Analyze返回的statement id,获取数据集字段(获取输出字段)
    :param host:
    :param params:
    :return:
    """
    sql_analyse_statement_id = get_sql_analyse_statement_id(host, params)
    url = ' %s/api/datasets/sql/analyzeresult?statementId=%s&clusterId=cluster1' % (host, sql_analyse_statement_id)
    res = requests.get(url=url, headers=get_headers(host))
    count_num = 0
    while "waiting" in res.text or "running" in res.text:
        log.info("再次查询前：%s %s" % (res.status_code, res.text))
        res = requests.get(url=url, headers=get_headers(host))
        count_num += 1
        if count_num == 100:
            return
    # 返回的是str类型
    if '"statement":"available"' in res.text:
        text_dict = json.loads(res.text)
        text_dict_content = text_dict["content"]
        return text_dict_content
    else:
        log.info('获取数据集输出字段失败')
        return


# 解析SQL字段后，初始化Sql任务，返回statement id，执行SQL语句使用
def get_sql_execte_statement_id(HOST,param):
    url = '%s/api/datasets/sql/executeinit' % HOST
    param = sql_analyse_data(param)
    new_data = json.dumps(param, separators=(',', ':'))
    res = requests.post(url=url, headers=get_headers(HOST), data=new_data)
    try:
        res_statementId = json.loads(res.text)
        sql_analyse_statement_id = res_statementId['statementId']
        return sql_analyse_statement_id
    except KeyError:
        return


def step_sql_analyse_data(data):
    """
    获取step的输出字段分析任务的请求体
    :param data:
    :return:
    """
    try:
        dataset = "select id,schema_id from merce_dataset where name = '%s' order by create_time desc limit 1" % data
        dataset_info = ms.ExecuQuery(dataset.encode('utf-8'))
        new_data = {"id":"source_1","name":"source","type":"source","otherConfigurations":{"schema":"training_training","dataset-dateTo":"","dataset-dateFrom":"","dataset-paths":"","dataset-sql":"","schemaId":dataset_info[0]["schema_id"],"interceptor":"","dataset":[{"rule":"set_1","dataset":"training","ignoreMissingPath":False,"datasetId":dataset_info[0]["id"],"storage":"JDBC"}]},"inputConfigurations":None,"outputConfigurations":{"output":[{"name":"id","type":"int","alias":"","description":None,"fieldCategory":None,"specId":None},{"name":"ts","type":"timestamp","alias":"","description":None,"fieldCategory":None,"specId":None},{"name":"code","type":"string","alias":"","description":None,"fieldCategory":None,"specId":None},{"name":"total","type":"float","alias":"","description":None,"fieldCategory":None,"specId":None},{"name":"forward_total","type":"float","alias":"","description":None,"fieldCategory":None,"specId":None},{"name":"reverse_total","type":"float","alias":"","description":None,"fieldCategory":None,"specId":None},{"name":"sum_flow","type":"float","alias":"","description":None,"fieldCategory":None,"specId":None}]},"libs":None,"flowId":None,"x":400,"y":250,"implementation":None,"uiConfigurations":{"output":["output"]}}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)

def get_step_output_init_statementId(host, params):
    """
    获取step的输出字段分析任务的statementID
    :param host:
    :param params:
    :return:
    """
    param = step_sql_analyse_data(params)
    new_data = json.dumps(param, separators=(',', ':'))
    url = '%s/api/steps/output/fields/init?branch=output' % host
    res = requests.post(url=url, headers=get_headers(host), data=new_data)
    try:
        res_statementId = json.loads(res.text)
        sql_analyse_statement_id = res_statementId['statementId']
        return sql_analyse_statement_id
    except KeyError:
        return

def get_step_output_ensure_statementId(host, params):
    """
    获取初始化确认step任务的statementID
    :param host:
    :param params:
    :return:
    """
    param = step_sql_analyse_data(params)
    new_data = json.dumps(param, separators=(',', ':'))
    url = '%s/api/steps/validateinit/dataflow' % host
    try:
        res = requests.post(url=url, headers=get_headers(host), data=new_data)
        res_statementId = json.loads(res.text)
        output_stattementid=res_statementId['statementId']
        return output_stattementid
    except:
        return


def step_sql_analyse_flow(params):
    """
    获取sqlsource step的输出字段分析任务的请求体
    :param :params
    :return:
    """
    try:
        dataset = "select id,name from merce_dataset where name = '%s' order by create_time desc limit 1" % params
        dataset_info = ms.ExecuQuery(dataset.encode('utf-8'))
        new_data = {"id":"sqlsource_1","name":"sqlsource","type":"sqlsource","otherConfigurations":{"interceptor":"","dataset":[{"dataset":dataset_info[0]["name"],"datasetId":dataset_info[0]["id"]}],"sql":"select * from stu"},"inputConfigurations":{},"outputConfigurations":{"output":[{"name":"sId","type":"int","alias":"","description":None,"fieldCategory":None,"specId":None},{"name":"sName","type":"string","alias":"","description":None,"fieldCategory":None,"specId":None},{"name":"age","type":"int","alias":"","description":None,"fieldCategory":None,"specId":None},{"name":"sex","type":"string","alias":"","description":None,"fieldCategory":None,"specId":None},{"name":"class","type":"string","alias":"","description":None,"fieldCategory":None,"specId":None}]},"libs":None,"flowId":None,"x":10,"y":140,"implementation":None,"uiConfigurations":{"output":["output"]}}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)


def steps_sql_parseinit_statemenId(host, params):
    """
    根据Sql语句解析表名,初始化ParseSql任务,返回statementID
    :param host:
    :param params:
    :return:
    """
    param = step_sql_analyse_flow(params)
    new_data = json.dumps(param, separators=(',', ':'))
    url = '%s/api/steps/sql/parseinit/dataflow' % host
    res = requests.post(url=url, headers=get_headers(host), data=new_data)
    try:
        res_statementId = json.loads(res.text)
        steps_sql_parseinit_statemenId = res_statementId['statementId']
        return steps_sql_parseinit_statemenId
    except KeyError:
        return



def steps_sql_analyzeinit_statementId(host,params):
    """
    初始化Sql Analyze,返回任务的statementID
    :param host:
    :param params:
    :return:
    """
    param = step_sql_analyse_flow(params)
    new_data = json.dumps(param, separators=(',', ':'))
    url = '%s/api/steps/sql/analyzeinit/dataflow' % host
    try:
        res = requests.post(url=url, headers=get_headers(host), data=new_data)
        res_statementId = json.loads(res.text)
        steps_sql_analyzeinit_statementId = res_statementId['statementId']
        return steps_sql_analyzeinit_statementId
    except KeyError:
        return