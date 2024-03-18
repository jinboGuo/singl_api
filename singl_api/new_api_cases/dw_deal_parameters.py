import random
from util.format_res import dict_res
from basic_info.setting import Dw_MySQL_CONFIG
from util.Open_DB import MYSQL
from util.logs import Logger

ms = MYSQL(Dw_MySQL_CONFIG["HOST"], Dw_MySQL_CONFIG["USER"], Dw_MySQL_CONFIG["PASSWORD"], Dw_MySQL_CONFIG["DB"], Dw_MySQL_CONFIG["PORT"])
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
                        elif "select id from assets_info ai" in data:
                            return new_data
                        else:
                            dat = ','.join([str(i) for i in new_data])
                            return dat
                    else:
                        try:
                            if "select id from assets_info ai" in data:
                                new_data.append(data_select_result[0]["id"])
                                return new_data
                            elif "select id from merce_dataset md" in data:
                                new_data.append(data_select_result[0]["id"])
                                return new_data
                            elif "select id from merce_schema ms" in data:
                                new_data.append(data_select_result[0]["id"])
                                return new_data
                            elif "select id from assets_info ao" in data:
                                data = data_select_result[0]["id"]
                                return data
                            elif "select id from merce_dataset ao" in data:
                                data = data_select_result[0]["id"]
                                return data
                            else:
                                data = data_select_result[0]["id"]
                                return str(data)
                        except Exception as e:
                            log.error("异常信息：%s" %e)
                else:
                    log.info("查询结果为空！")
            if 'select name from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["name"]
                        return deal_parameters(data)
                    except Exception as e:
                        log.error("异常信息：%s" %e)
                else:
                    log.info("查询结果为空！")
            if 'select current_info_id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["current_info_id"]
                        return str(data)
                    except Exception as e:
                        log.error("异常信息：%s" %e)
                else:
                    log.info("查询结果为空！")
            if 'select business_id,id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        business_id,dw_id = data_select_result[0]["business_id"],data_select_result[0]["id"]
                        new_data.append(business_id)
                        new_data.append(str(dw_id))
                        return new_data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            if 'select dataset_id,id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        dataset_id,dw_id = data_select_result[0]["dataset_id"],data_select_result[0]["id"]
                        new_data.append(dataset_id)
                        new_data.append(str(dw_id))
                        return new_data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            if 'select project_id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                if data_select_result:
                    try:
                        data = data_select_result[0]["project_id"]
                        return str(data)
                    except Exception as e:
                        log.error("异常信息：%s" %e)
                else:
                    log.info("查询结果为空！")
            if 'select metadata_id,id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        metadata_id,dw_id = data_select_result[0]["metadata_id"],data_select_result[0]["id"]
                        new_data.append(str(metadata_id))
                        new_data.append(str(dw_id))
                        return new_data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            if 'select id,current_info_id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        metadata_id,current_info_id = data_select_result[0]["id"],data_select_result[0]["current_info_id"]
                        new_data.append(str(metadata_id))
                        new_data.append(str(current_info_id))
                        return new_data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            if 'select name,project_id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        name,project_id = data_select_result[0]["name"],data_select_result[0]["project_id"]
                        new_data.append(name)
                        new_data.append(str(project_id))
                        return new_data
                    except Exception as e:
                        log.info("请确认SQL语句,异常信息：%s " %e)
                else:
                    log.info("查询结果为空！")
            if 'select project_id,id from' in data:
                data_select_result = ms.ExecuQuery(data.encode('utf-8'))
                new_data=[]
                if data_select_result:
                    try:
                        project_id,dw_id = data_select_result[0]["project_id"],data_select_result[0]["id"]
                        new_data.append(str(project_id))
                        new_data.append(str(dw_id))
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

def deal_random(new_data):
    try:
        dict_res(new_data)
        for key, value in new_data.items():
            if '随机数' in str(value):
                i = value.replace('随机数', str(random.randint(0, 999)))
                new_data[key] = str(i)
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)