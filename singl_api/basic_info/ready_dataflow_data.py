# coding:utf-8
from util.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG, MySQL_CONFIG1
from util.format_res import get_time
import random
import json

def get_dataflow_data(flow_name):
    print("开始执行get_dataflow_data(flow_name)")
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
    try:
        sql = "select id, name,flow_type from merce_flow where name like '%s%%%%' order by create_time desc limit 1" % flow_name
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
    except Exception as e:
        raise e
    else:
        try:
            flow_id = flow_info[0]["id"]
            flow_type = flow_info[0]["flow_type"]
            flow_name = flow_info[0]["name"]
        except KeyError as e:
            raise e

    data = {
        "configurations": {
            "arguments": [],
            "properties": [
                {
                    "name": "all.debug",
                    "value": "false"
                },
                {
                    "name": "all.dataset-nullable",
                    "value": "false"
                },
                {
                    "name": "all.lineage.enable",
                    "value": "true"
                },
                {
                    "name": "all.notify-output",
                    "value": "false"
                },
                {
                    "name": "all.debug-rows",
                    "value": "20"
                },
                {
                    "name": "dataflow.master",
                    "value": "yarn"
                },
                {
                    "name": "dataflow.deploy-mode",
                    "value": "client"
                },
                {
                    "name": "dataflow.queue",
                    "value": "merce.normal"
                },
                {
                    "name": "dataflow.num-executors",
                    "value": "2"
                },
                {
                    "name": "dataflow.driver-memory",
                    "value": "512M"
                },
                {
                    "name": "dataflow.executor-memory",
                    "value": "1G"
                },
                {
                    "name": "dataflow.executor-cores",
                    "value": "2"
                },
                {
                    "name": "dataflow.verbose",
                    "value": "true"
                },
                {
                    "name": "dataflow.local-dirs",
                    "value": ""
                },
                {
                    "name": "dataflow.sink.concat-files",
                    "value": "true"
                }
            ],
            "startTime": get_time()
        },
        "flowId": flow_id,
        "flowName": flow_name,
        "flowType": flow_type,
        "name": flow_name + str(random.randint(0, 99999)),
        "schedulerId": "once",
        "source": "rhinos"
    }
    return data

def get_executions_data(flow_name):
    print("开始执行get_executions_data(flow_name)")
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
    try:
        sql = "select id from merce_flow_execution where flow_name = '%s' order by create_time desc limit 1" % flow_name
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('execution_Id:', flow_info[0]["id"])
        execution_Id = flow_info[0]["id"]
    except KeyError:
            return

    new_data = {"fieldList": [{"fieldName": "executionId", "fieldValue": execution_Id, "comparatorOperator": "EQUAL","logicalOperator":"AND"}], "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}

    return new_data

def set_upsert_data():
    print("开始执行set_upsert_data")
    ms = MYSQL(MySQL_CONFIG1["HOST"], MySQL_CONFIG1["USER"], MySQL_CONFIG1["PASSWORD"], MySQL_CONFIG1["DB"])
    try:
      sql = "INSERT INTO `test_flow`.`training`(`ts`, `code`, `total`, `forward_total`, `reverse_total`, `sum_flow`, `sum_inst`, `inst_num`, `max_inst`, `max_inst_ts`, `min_inst`, `min_inst_ts`) VALUES ( CURRENT_TIMESTAMP, 'code1', 310001, 50, 5, 48, 2222, 42, 55, '2020-05-01 00:09:00', 23, '2020-01-01 00:09:00')"
      ms.ExecuNoQuery(sql.encode('utf-8'))
      sql ="UPDATE `test_flow`.`training`  set ts=CURRENT_TIMESTAMP "
      ms.ExecuNoQuery(sql.encode('utf-8'))
    except Exception:
        return

#set_upsert_data()

def set_upsert_datas():
    print("开始执行set_upsert_data")
    ms = MYSQL(MySQL_CONFIG1["HOST"], MySQL_CONFIG1["USER"], MySQL_CONFIG1["PASSWORD"], MySQL_CONFIG1["DB"])
    try:
        count=1
        sql = "INSERT INTO `test_flow`.`training`(`ts`, `code`, `total`, `forward_total`, `reverse_total`, `sum_flow`, `sum_inst`, `inst_num`, `max_inst`, `max_inst_ts`, `min_inst`, `min_inst_ts`) VALUES ( CURRENT_TIMESTAMP, 'code1', 310001, 50, 5, 48, 2222, 42, 55, '2020-05-01 00:09:00', 23, '2020-01-01 00:09:00')"
        while 1:
            print("插入count：",count)
            ms.ExecuNoQuery(sql.encode('utf-8'))
            count+=1
            if count==150000:
                break
    except Exception:
        return

#删除测试数据
def delete_autotest_datas():
    print("------开始删除数据-------")
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
    try:
        flow_sql = "delete from merce_flow where name like 'test%' or name like 'gjb%' or  name like 'auto_api_test_%'"
        dataset_sql = "delete from merce_dataset where name like 'test%' or  name like 'merce%' or  name like 'sink%' or  name like 'gjb_test%'  "
        schema_sql = "delete from merce_schema where name like 'test%' or  name like 'apitest%' or  name like  'gtest%'"
        print("删除flow表测试数据 ", flow_sql)
        ms.ExecuNoQuery(flow_sql.encode('utf-8'))
        print("删除dataset表测试数据 ",dataset_sql)
        ms.ExecuNoQuery(dataset_sql.encode('utf-8'))
        print("删除schema表测试数据 ",schema_sql)
        ms.ExecuNoQuery(schema_sql.encode('utf-8'))
    except:
       return

#delete_autotest_datas()