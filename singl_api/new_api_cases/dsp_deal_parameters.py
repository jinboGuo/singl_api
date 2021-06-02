import random
from basic_info.setting import Dsp_MySQL_CONFIG
from util.Open_DB import MYSQL
from util.logs import Logger

ms = MYSQL(Dsp_MySQL_CONFIG["HOST"], Dsp_MySQL_CONFIG["USER"], Dsp_MySQL_CONFIG["PASSWORD"], Dsp_MySQL_CONFIG["DB"],Dsp_MySQL_CONFIG["PORT"])
log = Logger().get_log()

def deal_parameters(data):
    try:
        if data:
            if '随机数' in data:
                data = data.replace('随机数', str(random.randint(0, 999)))
                return deal_parameters(data)
            if 'select id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data = []
                if data_select_result:
                    if len(data_select_result) > 1:
                        for i in range(len(data_select_result)):
                            new_data.append(data_select_result[i]["id"])
                        if "select id from merce_dss" in data:
                            return new_data
                        elif "select id from merce_schema" in data:
                            return new_data
                        else:
                            dat = ','.join([str(i) for i in new_data])
                            return dat
                    else:
                        try:
                            if "select id from dsp_data_resource where open_status=1 and name like 'gjb_ttest_hdfs_student%' order by create_time limit 1" in data:
                                new_data.append(str(data_select_result[0]['id']))
                                return new_data
                            else:
                                data = data_select_result[0]["id"]
                                return data
                        except Exception as e:
                            log.error("异常信息：%s" %e)
            if 'select enabled,id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if len(data_select_result):
                    try:
                        if data_select_result[0]["enabled"] == 1:
                            new_data = [{'enabled': 0, 'id': data_select_result[0]["id"]}]
                            return new_data
                        else:
                            new_data = [{'enabled': 1, 'id': data_select_result[0]["id"]}]
                            return new_data
                    except Exception as e:
                        log.error("异常信息：%s" %e)
            if 'select status,id,is_running from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        if data_select_result[0]["status"] == 1 and data_select_result[0]["is_running"] == 1:  # 正在运行服务，停止
                            status = "2"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                        if data_select_result[0]["status"] == 1 and data_select_result[0]["is_running"] == 0:  # 正在运行服务，停止
                            status = "2"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                        elif data_select_result[0]["status"] == 0 and data_select_result[0]["is_running"] == 0:  # 待部署服务，启用
                            status = "1"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                        elif data_select_result[0]["is_running"] == 2:  # 失败服务，停用
                            status = "2"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                        elif data_select_result[0]["status"] == 1 and data_select_result[0]["is_running"] == 3:  # 已成功服务，停用
                            status = "2"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                        elif data_select_result[0]["status"] == 2 and data_select_result[0]["is_running"] == 5:  # 已停止服务，启用
                            status = "1"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                        else:   # 停止服务 ，启用
                            status = "1"
                            id = str(data_select_result[0]["id"])
                            new_data = {'status': status, 'id': id,"expiredTime":""}
                            return new_data
                    except Exception as e:
                        log.error("异常信息：%s" %e)
                else:
                    return {'status': '1', 'id': '725070733486587904',"expiredTime":""}
            if 'select access_key' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["access_key"]
                        return data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            if 'select cust_id,id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        cust_id,id = data_select_result[0]["cust_id"],data_select_result[0]["id"]
                        new_data.append(cust_id)
                        new_data.append(str(id))
                        return new_data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            if 'select b.id as cust_id,a.id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        cust_id,id = data_select_result[0]["cust_id"],data_select_result[0]["id"]
                        new_data.append(cust_id)
                        new_data.append(str(id))
                        return new_data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            else:
                return data
        else:
            return data
    except Exception as e:
       log.error("异常信息：%s" %e)