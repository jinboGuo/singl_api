import random

from util.format_res import dict_res
from util.timestamp_13 import *
from basic_info.setting import Compass_MySQL_CONFIG
import os
from util.Open_DB import MYSQL

ms = MYSQL(Compass_MySQL_CONFIG["HOST"], Compass_MySQL_CONFIG["USER"], Compass_MySQL_CONFIG["PASSWORD"], Compass_MySQL_CONFIG["DB"])
ab_dir = lambda n: os.path.abspath(os.path.join(os.path.dirname(__file__), n))

def deal_parameters(data):
    try:
        if data:
            if '随机数' in data:
                # print(data)
                data = data.replace('随机数', str(random.randint(0, 999999999999999)))
                return deal_parameters(data)
            elif '监控开始时间' in data:
                data = data.replace('监控开始时间', get_now_time()[0])
                return deal_parameters(data)
            elif '监控结束时间' in data:
                data = data.replace('监控结束时间', get_now_time()[1])
                return deal_parameters(data)
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
                            return deal_parameters(job_pool_oid)
                        else:
                            new_data.append(str(data_select_result[0]["job_pool_oid"]))
                            return new_data
                else:
                    return
            elif 'select cluster_name' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["cluster_name"]
                        return deal_parameters(data)
                    except:
                        print('请确认第%d行SQL语句')
                else:
                    return
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
                    return
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
                    return
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
                        print("new_data: ", datas)
                        return datas
                    else:
                        job_oid.append(str(data_select_result[0]['job_oid']))
                        job_name.append(data_select_result[0]['job_name'])
                        job_oid = ','.join([str(i) for i in job_oid])
                        job_name = ','.join([str(i) for i in job_name])
                        datas = str(job_oid)+'&'+str(job_name)
                        print("new_data: ", datas)
                        return datas

            elif 'select dataflow_oid' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["dataflow_oid"]
                        return deal_parameters(str(data))
                    except:
                        print('请确认第%d行SQL语句')
                else:
                    return
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
                            return deal_parameters(single_oid)
                        else:
                            new_data.append(str(data_select_result[0]["single_oid"]))
                            return new_data
                else:
                    return
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
                            return deal_parameters(job_map_oid)
                        else:
                            new_data.append(str(data_select_result[0]["job_map_oid"]))
                            return new_data
                else:
                    return
            elif 'select re_th_ext_oid' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data = []
                if data_select_result:
                    if len(data_select_result) > 1:
                        for i in range(len(data_select_result)):
                            new_data.append(str(data_select_result[i]["re_th_ext_oid"]))
                        return new_data
                    else:
                        if data == "select re_th_ext_oid from s_c_re_th_ext where re_th_oid in(select t.re_th_oid from s_c_re_th as t where re_oid in(select s.re_oid from (select * from s_c_re where re_name like 'autotest%' limit 1) as s))":
                            re_th_ext_oid = str(data_select_result[0]["re_th_ext_oid"])
                            print("re_th_ext_oid ", re_th_ext_oid)
                            return deal_parameters(re_th_ext_oid)
                        else:
                            new_data.append(str(data_select_result[0]["re_th_ext_oid"]))
                            print("new_data ", new_data)
                            return new_data
                else:
                    return 1
            elif 'select re_th_oid' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data = []
                if data_select_result:
                    if len(data_select_result) > 1:
                        for i in range(len(data_select_result)):
                            new_data.append(str(data_select_result[i]["re_th_oid"]))
                        return new_data
                    else:
                        if data == "select re_th_oid from s_c_re_th where re_oid in(select t.re_oid from (select * from s_c_re where re_name like 'autotest%' limit 1) as t)":
                            re_th_oid = str(data_select_result[0]["re_th_oid"])
                            return deal_parameters(re_th_oid)
                        else:
                            new_data.append(str(data_select_result[0]["re_th_oid"]))
                            return new_data
                else:
                    return
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
                            return deal_parameters(str(re_oid))
                        else:
                            new_data.append(str(data_select_result[0]["re_oid"]))
                            return new_data
            else:
                return data
        else:
            return data
    except Exception as e:
        print("\033[31m异常：\033[0m",e)

def deal_random(new_data):
    try:
        dict_res(new_data)
        for key, value in new_data.items():
            if '随机数' in str(value):
                i = value.replace('随机数', str(random.randint(0, 999)))
                new_data[key] = str(i)
        print(new_data)
        return new_data
    except Exception as e:
        print("\033[31m异常：\033[0m",e)