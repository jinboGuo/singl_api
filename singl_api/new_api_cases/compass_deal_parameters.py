import json
import random
import requests
from basic_info.get_auth_token import get_headers
from util.format_res import dict_res
from util.get_deal_parameter import get_resourceid, get_datasource, get_tags, get_dataset, get_schema, ms, \
    get_job_view_id, get_schema_collect_id, get_collect_task_id, get_qa_task_id, get_dsp_data_application, \
    get_dsp_data_resource
from basic_info.setting import resource_type, tag_type, data_source, compass_host, log, dsp_data_source
import os

from util.timestamp_13 import data_now

ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))

def deal_parameters(data,request_method,request_url):
    if data:
        if '随机数' in data:
            data = data.replace('随机数', str(random.randint(0, 9999999999999)))
            return deal_parameters(data,request_method,request_url)
        if '创建时间' in data:
            data = data.replace('创建时间', data_now())
            return deal_parameters(data,request_method,request_url)
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
        if '元数据任务采集目录' in data:
            data = data.replace('元数据任务采集目录',  str(get_resourceid(resource_type[13])))
            return deal_parameters(data,request_method,request_url)
        if '元数据采集任务主键' in data:
            data = data.replace('元数据采集任务主键',  str(get_schema_collect_id()))
            return deal_parameters(data,request_method,request_url)
        if '离线采集任务主键' in data:
            data = data.replace('离线采集任务主键',  str(get_collect_task_id()))
            return deal_parameters(data,request_method,request_url)
        if '任务视图主键' in data:
            data = data.replace('任务视图主键',  str(get_job_view_id()))
            return deal_parameters(data,request_method,request_url)
        if '质检任务目录' in data:
            data = data.replace('质检任务目录',  str(get_resourceid(resource_type[14])))
            return deal_parameters(data,request_method,request_url)
        if '质检任务主键' in data:
            data = data.replace('质检任务主键',  str(get_qa_task_id()))
            return deal_parameters(data,request_method,request_url)
        if '数据资源主键' in data:
            data = data.replace('数据资源主键',  str(get_dsp_data_resource(dsp_data_source[0])))
            return deal_parameters(data,request_method,request_url)
        if '数据资源名称' in data:
            data = data.replace('数据资源名称',  str(get_dsp_data_resource(dsp_data_source[1])))
            return deal_parameters(data,request_method,request_url)
        if '数据资源申请工单主键' in data:
            data = data.replace('数据资源申请工单主键',  str(get_dsp_data_application()))
            return deal_parameters(data,request_method,request_url)
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
                        response = requests.post(url=compass_host+new_data[1], headers=get_headers(), data=new_data[0])
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
                else:
                    if '数据源主键' in data:
                        data = data.replace('数据源主键', str(get_datasource(data_source[0],new_data[1])))
                        return deal_parameters(data,request_method,request_url)
                    if '数据源名称' in data:
                        data = data.replace('数据源名称', str(get_datasource(data_source[1],new_data[1])))
                        return deal_parameters(data,request_method,request_url)
                    if '数据集主键' in data:
                        data = data.replace('数据集主键', str(get_dataset(data_source[4],new_data[1])))
                        return deal_parameters(data,request_method,request_url)
                    if '数据集名称' in data:
                        data = data.replace('数据集名称', str(get_dataset(data_source[5],new_data[1])))
                        return deal_parameters(data,request_method,request_url)
                    if '标签主键' in data:
                        data = data.replace('标签主键', str(get_tags(tag_type[0],new_data[1])))
                        return deal_parameters(data,request_method,request_url)
                    if   '/api/schema-collect/schema/task/query' == new_data[1]:
                      try:
                        response = requests.post(url=compass_host+new_data[1], headers=get_headers(), data=new_data[0])
                        new_data = response.json()["content"]["list"][0]
                        table_names= ["NewTable_time_1"]
                        new_data["tableNames"]=table_names
                        #print("type: ",type(new_data))
                        log.info("QUERY查询接口响应数据:{}".format(new_data))
                        return new_data
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
        elif 'select job_pool_oid' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data = []
                if data_select_result:
                    if len(data_select_result) > 1:
                        for i in range(len(data_select_result)):
                            new_data.append(str(data_select_result[i]["job_pool_oid"]))
                        return new_data
                    else:
                        if data == "select job_pool_oid from s_c_job_pool where pool_name like 'autotest%' limit 1":
                            job_pool_oid = str(data_select_result[0]["job_pool_oid"])
                            return deal_parameters(job_pool_oid,request_method,request_url)
                        else:
                            new_data.append(str(data_select_result[0]["job_pool_oid"]))
                            return new_data
                else:
                    log.error("sql查询结果为空！")
        elif 'select cluster_name' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["cluster_name"]
                        return deal_parameters(data,request_method,request_url)
                    except Exception as e:
                        log.error("异常信息：%s" %e)
                else:
                    log.error("sql查询结果为空！")
        elif 'select oid' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                oid = []
                if data_select_result:
                    if len(data_select_result) > 1:
                        for i in range(len(data_select_result)):
                            oid.append(str(data_select_result[i]["oid"]))
                        return oid
                    else:
                        oid.append(str(data_select_result[0]['oid']))
                        return oid
                else:
                    log.error("sql查询结果为空！")
        elif 'select job_oid' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                job_oid = []
                if data_select_result:
                    if len(data_select_result) > 1:
                        for i in range(len(data_select_result)):
                            job_oid.append(str(data_select_result[i]["job_oid"]))
                        return job_oid
                    else:
                        job_oid.append(str(data_select_result[0]['job_oid']))
                        return job_oid
                else:
                    log.error("sql查询结果为空！")
        elif 'select job_name' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                job_oid = []
                job_name = []
                if data_select_result:
                    if len(data_select_result) > 1:
                        for i in range(len(data_select_result)):
                            job_oid.append(data_select_result[i]["job_oid"])
                            job_name.append(data_select_result[i]["job_name"])
                        job_oids = ','.join([str(i) for i in job_oid])
                        job_names = ','.join([str(i) for i in job_name])
                        datas = str(job_oids)+'&'+str(job_names)
                        return datas
                    else:
                        job_oid.append(str(data_select_result[0]['job_oid']))
                        job_name.append(data_select_result[0]['job_name'])
                        job_oid = ','.join([str(i) for i in job_oid])
                        job_name = ','.join([str(i) for i in job_name])
                        datas = str(job_oid)+'&'+str(job_name)
                        return datas
                else:
                    log.error("sql查询结果为空！")
        elif 'select dataflow_oid' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["dataflow_oid"]
                        return deal_parameters(str(data),request_method,request_url)
                    except Exception as e:
                        log.error("异常信息：%s" %e)
                else:
                    log.error("sql查询结果为空！")
        elif 'select single_oid' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data = []
                if data_select_result:
                    if len(data_select_result) > 1:
                        for i in range(len(data_select_result)):
                            new_data.append(str(data_select_result[i]["single_oid"]))
                        return new_data
                    else:
                        if data == "select single_oid from s_r_job_single where task_name like 'test_cover%' order by create_time desc limit 1":
                            single_oid = str(data_select_result[0]["single_oid"])
                            return deal_parameters(single_oid,request_method,request_url)
                        else:
                            new_data.append(str(data_select_result[0]["single_oid"]))
                            return new_data
                else:
                    log.error("sql查询结果为空！")
        elif 'select job_map_oid' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data = []
                if data_select_result:
                    if len(data_select_result) > 1:
                        for i in range(len(data_select_result)):
                            new_data.append(str(data_select_result[i]["job_map_oid"]))
                        return new_data
                    else:
                        if data == "select job_map_oid from s_c_job_map where job_name like 'autotest%' limit 1":
                            job_map_oid = str(data_select_result[0]["job_map_oid"])
                            return deal_parameters(job_map_oid,request_method,request_url)
                        else:
                            new_data.append(str(data_select_result[0]["job_map_oid"]))
                            return new_data
                else:
                    log.error("sql查询结果为空！")
        elif 'select re_th_ext_oid' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data = []
                if data_select_result:
                    if len(data_select_result) > 1:
                        for i in range(len(data_select_result)):
                            new_data.append(str(data_select_result[i]["re_th_ext_oid"]))
                        return new_data
                    else:
                        if data == "select re_th_ext_oid from s_c_re_th_ext where re_th_oid in(select t.re_th_oid from s_c_re_th as t where re_oid in(select s.re_oid from (select * from s_c_re where re_name like 'autotest%' limit 1) as s)) limit 1":
                            re_th_ext_oid = str(data_select_result[0]["re_th_ext_oid"])
                            return deal_parameters(re_th_ext_oid,request_method,request_url)
                        else:
                            new_data.append(str(data_select_result[0]["re_th_ext_oid"]))
                            return new_data
                else:
                    log.error("sql查询结果为空！")
        elif 'select re_th_oid' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data = []
                if data_select_result:
                    if len(data_select_result) > 1:
                        for i in range(len(data_select_result)):
                            new_data.append(str(data_select_result[i]["re_th_oid"]))
                        return new_data
                    else:
                        if data == "select re_th_oid from s_c_re_th where re_oid in(select t.re_oid from (select * from s_c_re where re_name like 'autotest%' limit 1) as t) limit 1":
                            re_th_oid = str(data_select_result[0]["re_th_oid"])
                            return deal_parameters(re_th_oid,request_method,request_url)
                        else:
                            new_data.append(str(data_select_result[0]["re_th_oid"]))
                            return new_data
                else:
                    log.error("sql查询结果为空！")
        elif "select re_oid " in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data = []
                if data_select_result:
                    if len(data_select_result) > 1:
                        for i in range(len(data_select_result)):
                            new_data.append(str(data_select_result[i]["re_oid"]))
                        return new_data
                    else:
                        if data == "select re_oid from s_c_re where re_name like 'autotest%' limit 1":
                            re_oid = str(data_select_result[0]["re_oid"])
                            return deal_parameters(str(re_oid),request_method,request_url)
                        else:
                            new_data.append(str(data_select_result[0]["re_oid"]))
                            return new_data
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

def deal_random(new_data):
    try:
        dict_res(new_data)
        for key, value in new_data.items():
            if '随机数' in str(value):
                i = value.replace('随机数', str(random.randint(0, 99999)))
                new_data[key] = str(i)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)