# coding:utf-8
from util.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG
from util.format_res import get_time
import random

def get_dataflow_data(flow_name):
    print("开始执行get_dataflow_data(flow_name)")
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
    try:
        sql = 'select id, flow_type from merce_flow where name = "%s"' % flow_name
        flow_info = ms.ExecuQuery(sql)
        print(sql)
        print('flow_info:', flow_info)
    except Exception as e:
        raise e
    else:
        try:
            flow_id = flow_info[0]["id"]
            flow_type = flow_info[0]["flow_type"]
            # print(flow_name, flow_type)
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

