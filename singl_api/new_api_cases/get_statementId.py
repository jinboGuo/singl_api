import requests,json
from basic_info.get_auth_token import get_headers
from basic_info.setting import host, tenant_id_189, tenant_id_81,tenant_id_83,tenant_id_82
from util.format_res import dict_res


# 根据host信息返回tenant信息
def get_tenant(host):
    global tenant_id
    if '81' in host:
        tenant_id = tenant_id_81
        return tenant_id
    elif '189' in host:
        tenant_id = tenant_id_189
        return tenant_id
    elif '83' in host:
        tenant_id = tenant_id_83
        return tenant_id
#     elif '84' in host:
#         tenant_id = tenant_id_84
#         return tenant_id
    elif '82' in host:
        tenant_id = tenant_id_82
        return tenant_id
    else:
        #print('使用的host不在预期中，请确认host信息')
        return


# datasetId存在时
def statementId(host , param):

    data = param.split('&')
    url = '%s/api/datasets/%s/previewinit?tenant=%s' % (host, data[0], get_tenant(host))
    res = requests.post(url=url, headers=get_headers(host), json=dict_res(data[1]))
    try:
        res_statementId = json.loads(res.text)
        statementId = res_statementId['statementId']
        return data[0], statementId, data[1]
    except KeyError:
        return


def statementId_flow_use(host, datasetId, tenant):
    url = '%s/api/datasets/%s/previewinit?tenant=%s&rows=50' % (host, datasetId, get_tenant(host))
    res = requests.post(url=url, headers=get_headers(host))
    print(res.status_code, res.text)
    try:
        res_statementId = dict_res(res.text)
        # print('%s数据集获取的statementID信息：%s' %(datasetId, res_statementId))
        statementId = res_statementId['statementId']
        # print('%s数据集获取的statementID：%s' % (datasetId, statementId))
    except:
        print('数据集%s的statementID返回空' % datasetId)
        return
    else:
        return statementId

def statementId_flow_output_use(host, datasetId):
    url = '%s/api/datasets/%s/previewinit??tenant=db09f359-1e4d-4b3c-872e-7775bd8eed8b&rows=50' % (host, datasetId)
    res = requests.get(url=url, headers=get_headers(host))
    print("555555555",res.status_code, res.text)
    try:
        res_statementId = dict_res(res.text)
        # print('%s数据集获取的statementID信息：%s' %(datasetId, res_statementId))
        statementId = res_statementId['statementId']
        print('%s数据集获取的statementID：%s' % (datasetId, statementId))
    except:
        print('数据集%s的statementID返回空' % datasetId)
        return
    else:
        return statementId

def preview_result_flow_use(host, datasetId, tenant, statementID):
    if isinstance(statementID, int):
        url = "%s/api/datasets/%s/previewresult?tenant=%s&statementId=%d" % (host, datasetId, get_tenant(host), statementID)
        res = requests.post(url=url, headers=get_headers(host))
        print(res.url)
        print('%s数据集preview_result:%s' % (datasetId, res.text))
        count_num = 0
        while 'waiting' in res.text or 'running' in res.text:
            res = requests.post(url=url, headers=get_headers(host))
        try:
            dataset_result = dict_res(res.text)['content']
        except KeyError:
            return
        else:
            print('%s数据集dataset_result: %s ' % (datasetId, dataset_result))
            return dataset_result
    else:
        print('%s数据集返回的statementID为空')


# datasetId不存在时
def statementId_no_dataset(host, param):
    url = '%s/api/datasets/new/previewinit?tenant=%s' % (host, get_tenant(host))
    res = requests.post(url=url, headers=get_headers(host), json=param)
    try:
        res_statementId = json.loads(res.text)
        statementId = res_statementId['statementId']
        print('stateid',statementId)
        return statementId
    except KeyError:
        return


# 初始化Sql Analyze(解析数据集输出字段)，返回statement id，获取数据集字段给分析任务使用
def get_sql_analyse_statement_id(host, param):
    url = ' %s/api/datasets/sql/analyzeinit' % host
    res = requests.post(url=url, headers=get_headers(host), data=param)
    # print(res.text)
    try:
        res_statementId = json.loads(res.text)
        sql_analyse_statement_id = res_statementId['statementId']
        # print(sql_analyse_statement_id)
        return sql_analyse_statement_id
    except KeyError:
        return


# 根据初始化SQL Analyze返回的statement id,获取数据集字段(获取输出字段)
def get_sql_analyse_dataset_info(host, params):
    sql_analyse_statement_id = get_sql_analyse_statement_id(host, params)
    # print(sql_analyse_statement_id)
    url = ' %s/api/datasets/sql/analyzeresult?statementId=%s' % (host, sql_analyse_statement_id)
    res = requests.get(url=url, headers=get_headers(host))
    #print(res.text)
    count_num = 0
    while ("waiting") in res.text or ("running") in res.text:
        print('再次查询前', res.text)
        res = requests.get(url=url, headers=get_headers(host))
        count_num += 1
        if count_num == 100:
            return
        print('再次查询后', res.text)
    # 返回的是str类型
    print(res.text)
    if '"statement":"available"' in res.text:
        text_dict = json.loads(res.text)
        text_dict_content = text_dict["content"]
        # print(res.text)
        # print(text_dict_content)
        return text_dict_content
    else:
        print('获取数据集输出字段失败')
        return


# 解析SQL字段后，初始化Sql任务，返回statement id，执行SQL语句使用
def get_sql_execte_statement_id(HOST,param):
    url = '%s/api/datasets/sql/executeinit' % HOST
    res = requests.post(url=url, headers=get_headers(HOST), data=param)
    #print(res.text)
    try:
        res_statementId = json.loads(res.text)
        sql_analyse_statement_id = res_statementId['statementId']
        #print(sql_analyse_statement_id)
        return sql_analyse_statement_id
    except KeyError:
        return


# 根据Sql语句解析表名,初始化ParseSql任务,返回statementID
def steps_sql_parseinit_statemenId(HOST,params):
    url = '%s/api/steps/sql/parseinit/dataflow' % HOST
    res = requests.post(url=url, headers=get_headers(HOST), data=params)
    print(res.text)
    try:
        res_statementId = json.loads(res.text)
        steps_sql_parseinit_statemenId = res_statementId['statementId']
        print(steps_sql_parseinit_statemenId)
        return steps_sql_parseinit_statemenId
    except KeyError:
        return


# 初始化Sql Analyze,返回任务的statementID
def steps_sql_analyzeinit_statementId(HOST, params):
    url = '%s/api/steps/sql/analyzeinit/dataflow' % HOST
    params = params.encode('utf-8')
    try:
        res = requests.post(url=url, headers=get_headers(HOST), data=params)
        print(res.text)
        res_statementId = json.loads(res.text)
        steps_sql_analyzeinit_statementId = res_statementId['statementId']
        print(steps_sql_analyzeinit_statementId)
        return steps_sql_analyzeinit_statementId
    except KeyError:
        return

def get_step_output_init_statementId(HOST,params):
    url = '%s/api/steps/output/fields/init' % HOST
    try:
        res = requests.post(url=url, headers=get_headers(HOST), data=params)
        print(res.status_code, res.text)
        print(dict_res(res.text)["statementId"])
    except:
        return
    else:
        return dict_res(res.text)["statementId"]

def get_step_output_ensure_statementId(HOST,params):
    url = '%s/api/steps/validateinit/dataflow' % HOST
    try:
        res = requests.post(url=url, headers=get_headers(HOST), data=params)
        print(dict_res(res.text)["statementId"])
        res_statementId = json.loads(res.text)
        output_stattementid=res_statementId['statementId']
        return output_stattementid
    except:
        return

# params = '{"id":"source_9","name":"source_9","type":"source","x":168,"y":239,"otherConfigurations":{"schema":"schema_for_students_startjoin_step","schemaId":"31caabd3-ed37-415d-bc51-5c039f5b7689","sessionCache":"","datasetId":"5ebd5da6-793d-4cf9-bb4a-f84301eb0c4e","interceptor":"","dataset":"gbj_use_students_short_84","ignoreMissingPath":false},"outputConfigurations":[{"id":"output","fields":[{"column":"sId","alias":""},{"column":"sName","alias":""},{"column":"sex","alias":""},{"column":"age","alias":""},{"column":"class","alias":""}]}]}'
# get_step_output_ensure_statementId(params)

# params = '{"id":"source_9","name":"source_9","type":"source","x":168,"y":239,"otherConfigurations":{"schema":"schema_for_students_startjoin_step","schemaId":"31caabd3-ed37-415d-bc51-5c039f5b7689","sessionCache":"","datasetId":"5ebd5da6-793d-4cf9-bb4a-f84301eb0c4e","interceptor":"","dataset":"gbj_use_students_short_84","ignoreMissingPath":false},"outputConfigurations":[{"id":"output","fields":[{"column":"sId","alias":""},{"column":"sName","alias":""},{"column":"sex","alias":""},{"column":"age","alias":""},{"column":"class","alias":""}]}]}'

# get_step_output_init_statementId(params)

