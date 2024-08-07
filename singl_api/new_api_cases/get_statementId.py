import time
import requests, json
from basic_info.get_auth_token import get_headers
from basic_info.setting import log, ms
from new_api_cases.dw_prepare_datas import sql_analyse_data
from util.format_res import dict_res

def statementId_flow_use(host, dataset_id):
    url = '%s/api/datasets/%s/previewinit?rows=50' % (host,dataset_id)
    log.info("statementId_flow_use请求url=%s" % url)
    res = requests.get(url=url, headers=get_headers())
    log.info("response data：%s %s" % (res.status_code, res.text))
    try:
        res_statement_id = dict_res(res.text)
        log.info('%s数据集获取的statementID信息：%s' % (dataset_id, res_statement_id))
        statement_id = res_statement_id['statementId']
    except Exception as e:
        log.info('数据集%s的statementID返回空%s' % (dataset_id,e))
    else:
        return statement_id

def statementId_flow_output_use(host, dataset_id):
    url = '%s/api/datasets/%s/previewinit?rows=50' % (host, dataset_id)
    res = requests.get(url=url, headers=get_headers())
    try:
        res_statement_id = dict_res(res.text)
        statement_id = res_statement_id['statementId']
        log.info('%s数据集获取的statementID：%s' % (dataset_id, statement_id))
    except Exception as e:
        log.info('数据集%s的statementID返回空%s' % (dataset_id, e))
    else:
        return statement_id

def preview_result_flow_use(host, dataset_id, statement_id):
    if isinstance(statement_id, int):
        url = "%s/api/datasets/%s/previewresult?statementId=%d&clusterId=cluster1" % (host, dataset_id, statement_id)
        res = requests.get(url=url, headers=get_headers())
        log.info('%s数据集preview_result:%s' % (dataset_id, res.text))
        while 'waiting' in res.text or 'running' in res.text:
            res = requests.get(url=url, headers=get_headers())
        try:
            dataset_result = dict_res(res.text)['content']
        except KeyError as e:
            log.error("datasetId不存在{}".format(e))
        else:
            log.info('%s数据集dataset_result: %s ' % (dataset_id, dataset_result))
            return dataset_result
    else:
        log.info('数据集返回的statementID为空')


def get_sql_analyse_statement_id(host, param):
    """
    初始化Sql Analyze(解析数据集输出字段)，返回statement id，获取数据集字段给分析任务使用
    :param host:
    :param param:
    :return:
    """
    url = ' %s/api/sys/meta/datasets/sql/analyzeinit' % host
    param = sql_analyse_data(param)
    new_data = json.dumps(param, separators=(',', ':'))
    res = requests.post(url=url, headers=get_headers(), data=new_data)
    try:
        time.sleep(25)
        res_statement_id = res.json()["content"]
        statement_id = res_statement_id['statementId']
        session_id = res_statement_id['sessionId']
        cluster_id = res_statement_id['clusterId']
        return statement_id,session_id,cluster_id
    except KeyError as e:
        log.error("statementId不存在{}".format(e))



def get_sql_analyse_dataset_info(host, params):
    """
    根据初始化SQL Analyze返回的statement id,获取数据集字段(获取输出字段)
    :param host:
    :param params:
    :return:
    """
    try:
        statement_id,session_id,cluster_id = get_sql_analyse_statement_id(host, params)
        url = ' %s/api/sys/meta/datasets/sql/analyzeresult?statementId=%s&sessionId=%s&clusterId=%s&retryTimes=0' % (host, statement_id,session_id,cluster_id)
        res = requests.get(url=url, headers=get_headers())
        text_dict = res.json()["content"]["content"]
        return text_dict
    except Exception as e:
        log.error("异常信息：%s" % e)


# 解析SQL字段后，初始化Sql任务，返回statement id，执行SQL语句使用
def get_sql_execte_statement_id(host,param):
    url = '%s/api/sys/meta/datasets/sql/executeinit' % host
    param = sql_analyse_data(param)
    new_data = json.dumps(param, separators=(',', ':'))
    res = requests.post(url=url, headers=get_headers(), data=new_data)
    try:
        time.sleep(2)
        res_statement_id = res.json()["content"]
        statement_id = res_statement_id['statementId']
        session_id = res_statement_id['sessionId']
        cluster_id = res_statement_id['clusterId']
        return statement_id,session_id,cluster_id
    except Exception as e:
        log.error("异常信息：%s" % e)


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
    res = requests.post(url=url, headers=get_headers(), data=new_data)
    try:
        res_statement_id = json.loads(res.text)
        sql_analyse_statement_id = res_statement_id['statementId']
        return sql_analyse_statement_id
    except Exception as e:
        log.error("异常信息：%s" % e)

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
        res = requests.post(url=url, headers=get_headers(), data=new_data)
        res_statement_id = json.loads(res.text)
        output_statement_id=res_statement_id['statementId']
        return output_statement_id
    except Exception as e:
        log.error("异常信息：%s" % e)


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
    res = requests.post(url=url, headers=get_headers(), data=new_data)
    try:
        res_statement_id = json.loads(res.text)
        steps_sql_parseinit_statement_id = res_statement_id['statementId']
        return steps_sql_parseinit_statement_id
    except Exception as e:
        log.error("异常信息：%s" % e)



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
        res = requests.post(url=url, headers=get_headers(), data=new_data)
        res_statement_id = json.loads(res.text)
        steps_sql_analyse_init_statement_id = res_statement_id['statementId']
        return steps_sql_analyse_init_statement_id
    except Exception as e:
        log.error("异常信息：%s" % e)