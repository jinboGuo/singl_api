# coding:utf-8
from basic_info.setting import ms, log
from util.timestamp_13 import get_now, data_now


def get_dataflow_data(flow_name):

    try:
        sql = "select id,version,name,flow_type from merce_flow where name ='%s' order by create_time desc limit 1" % flow_name
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        if "auto_api_test" == flow_name:
            data = {"configurations":{"startTime":get_now(),"arguments":[],"dependencies":[],"extraConfigurations":{},"properties":[{"name":"all.debug","value":"false","input":["false","true"]},{"name":"all.debug-rows","value":20,"input":20},{"name":"all.dataset-nullable","value":"false","input":["false","true"]},{"name":"all.optimized-enable","value":"true","input":["true","false"]},{"name":"all.lineage-enable","value":"true","input":["true","false"]},{"name":"all.output-notify","value":"true","input":["true","false"]},{"name":"all.cluster-id","value":"cluster1","input":["cluster1","cluster215","cluster81"]},{"name":"dataflow.queue","value":"root.default","input":["root.default","root.baymax2","root.baymax1.queue1","root.baymax1.queue2"]},{"name":"dataflow.verbose","value":"true","input":["true","false"]},{"name":"dataflow.master","value":"yarn","input":"yarn"},{"name":"dataflow.deploy-mode","value":"cluster","input":["cluster","client"]},{"name":"dataflow.num-executors","value":2,"input":2},{"name":"dataflow.driver-memory","value":"1G","input":"1G"},{"name":"dataflow.executor-memory","value":"2G","input":"2G"},{"name":"dataflow.executor-cores","value":2,"input":2},{"name":"dataflow.local-dirs","value":"","input":""},{"name":"dataflow.temp-dir","value":"/tmp/dataflow/spark","input":"/tmp/dataflow/spark"},{"name":"dataflow.sink-concat-files","value":"true","input":["true","false"]}],"retry":{"enable":False,"limit":1,"timeInterval":1,"intervalUnit":"MINUTES"}},"schedulerId":"once","ource":"rhinos","version":flow_info[0]["version"],"flowId":flow_info[0]["id"],"flowType":flow_info[0]["flow_type"],"name":flow_info[0]["name"]+data_now(),"flowVersion":flow_info[0]["version"],"creator":"admin","oldName":flow_info[0]["name"]}
            return data
        elif "ES-upsert" == flow_name:
            data = {"configurations":{"startTime":get_now(),"arguments":[],"dependencies":[],"extraConfigurations":{},"properties":[{"name":"all.debug","value":"false","input":["false","true"]},{"name":"all.debug-rows","value":20,"input":20},{"name":"all.dataset-nullable","value":"false","input":["false","true"]},{"name":"all.optimized-enable","value":"true","input":["true","false"]},{"name":"all.lineage-enable","value":"true","input":["true","false"]},{"name":"all.output-notify","value":"true","input":["true","false"]},{"name":"all.cluster-id","value":"cluster1","input":["cluster1","cluster215","cluster81"]},{"name":"dataflow.queue","value":"root.default","input":["root.default","root.baymax2","root.baymax1.queue1","root.baymax1.queue2"]},{"name":"dataflow.verbose","value":"true","input":["true","false"]},{"name":"dataflow.master","value":"yarn","input":"yarn"},{"name":"dataflow.deploy-mode","value":"cluster","input":["cluster","client"]},{"name":"dataflow.num-executors","value":2,"input":2},{"name":"dataflow.driver-memory","value":"1G","input":"1G"},{"name":"dataflow.executor-memory","value":"2G","input":"2G"},{"name":"dataflow.executor-cores","value":2,"input":2},{"name":"dataflow.local-dirs","value":"","input":""},{"name":"dataflow.temp-dir","value":"/tmp/dataflow/spark","input":"/tmp/dataflow/spark"},{"name":"dataflow.sink-concat-files","value":"true","input":["true","false"]}],"retry":{"enable":False,"limit":1,"timeInterval":1,"intervalUnit":"MINUTES"}},"schedulerId":"once","ource":"rhinos","version":flow_info[0]["version"],"flowId":flow_info[0]["id"],"flowType":flow_info[0]["flow_type"],"name":flow_info[0]["name"]+data_now(),"flowVersion":flow_info[0]["version"],"creator":"admin","oldName":flow_info[0]["name"]}
            return data
        elif "auto_apitest_df" == flow_name:
            data = {"configurations":{"startTime":get_now(),"arguments":[],"dependencies":[],"extraConfigurations":{},"properties":[{"name":"all.debug","value":"false","input":["false","true"]},{"name":"all.debug-rows","value":20,"input":20},{"name":"all.dataset-nullable","value":"false","input":["false","true"]},{"name":"all.optimized-enable","value":"true","input":["true","false"]},{"name":"all.lineage-enable","value":"true","input":["true","false"]},{"name":"all.output-notify","value":"true","input":["true","false"]},{"name":"all.cluster-id","value":"cluster1","input":["cluster1","cluster215","cluster81"]},{"name":"dataflow.queue","value":"root.default","input":["root.default","root.baymax2","root.baymax1.queue1","root.baymax1.queue2"]},{"name":"dataflow.verbose","value":"true","input":["true","false"]},{"name":"dataflow.master","value":"yarn","input":"yarn"},{"name":"dataflow.deploy-mode","value":"cluster","input":["cluster","client"]},{"name":"dataflow.num-executors","value":2,"input":2},{"name":"dataflow.driver-memory","value":"1G","input":"1G"},{"name":"dataflow.executor-memory","value":"2G","input":"2G"},{"name":"dataflow.executor-cores","value":2,"input":2},{"name":"dataflow.local-dirs","value":"","input":""},{"name":"dataflow.temp-dir","value":"/tmp/dataflow/spark","input":"/tmp/dataflow/spark"},{"name":"dataflow.sink-concat-files","value":"true","input":["true","false"]}],"retry":{"enable":False,"limit":1,"timeInterval":1,"intervalUnit":"MINUTES"}},"schedulerId":"once","ource":"rhinos","version":flow_info[0]["version"],"flowId":flow_info[0]["id"],"flowType":flow_info[0]["flow_type"],"name":flow_info[0]["name"]+data_now(),"flowVersion":flow_info[0]["version"],"creator":"admin","oldName":flow_info[0]["name"]}
            return data
        elif "mutil_sink_storage" == flow_name:
            data = {"configurations":{"startTime":get_now(),"arguments":[],"dependencies":[],"extraConfigurations":{},"properties":[{"name":"all.debug","value":"false","input":["false","true"]},{"name":"all.debug-rows","value":20,"input":20},{"name":"all.dataset-nullable","value":"false","input":["false","true"]},{"name":"all.optimized-enable","value":"true","input":["true","false"]},{"name":"all.lineage-enable","value":"true","input":["true","false"]},{"name":"all.output-notify","value":"true","input":["true","false"]},{"name":"all.cluster-id","value":"cluster1","input":["cluster1","cluster215","cluster81"]},{"name":"dataflow.queue","value":"root.default","input":["root.default","root.baymax2","root.baymax1.queue1","root.baymax1.queue2"]},{"name":"dataflow.verbose","value":"true","input":["true","false"]},{"name":"dataflow.master","value":"yarn","input":"yarn"},{"name":"dataflow.deploy-mode","value":"cluster","input":["cluster","client"]},{"name":"dataflow.num-executors","value":2,"input":2},{"name":"dataflow.driver-memory","value":"1G","input":"1G"},{"name":"dataflow.executor-memory","value":"2G","input":"2G"},{"name":"dataflow.executor-cores","value":2,"input":2},{"name":"dataflow.local-dirs","value":"","input":""},{"name":"dataflow.temp-dir","value":"/tmp/dataflow/spark","input":"/tmp/dataflow/spark"},{"name":"dataflow.sink-concat-files","value":"true","input":["true","false"]}],"retry":{"enable":False,"limit":1,"timeInterval":1,"intervalUnit":"MINUTES"}},"schedulerId":"once","ource":"rhinos","version":flow_info[0]["version"],"flowId":flow_info[0]["id"],"flowType":flow_info[0]["flow_type"],"name":flow_info[0]["name"]+data_now(),"flowVersion":flow_info[0]["version"],"creator":"admin","oldName":flow_info[0]["name"]}
            return data
        elif "scheduer" == flow_name:
            data = {"configurations":{"startTime":get_now(),"arguments":[],"dependencies":[],"extraConfigurations":{},"properties":[{"name":"all.debug","value":"false","input":["false","true"]},{"name":"all.debug-rows","value":20,"input":20},{"name":"all.dataset-nullable","value":"false","input":["false","true"]},{"name":"all.optimized-enable","value":"true","input":["true","false"]},{"name":"all.lineage-enable","value":"true","input":["true","false"]},{"name":"all.output-notify","value":"true","input":["true","false"]},{"name":"all.cluster-id","value":"cluster1","input":["cluster1","cluster215","cluster81"]},{"name":"dataflow.queue","value":"root.default","input":["root.default","root.baymax2","root.baymax1.queue1","root.baymax1.queue2"]},{"name":"dataflow.verbose","value":"true","input":["true","false"]},{"name":"dataflow.master","value":"yarn","input":"yarn"},{"name":"dataflow.deploy-mode","value":"cluster","input":["cluster","client"]},{"name":"dataflow.num-executors","value":2,"input":2},{"name":"dataflow.driver-memory","value":"1G","input":"1G"},{"name":"dataflow.executor-memory","value":"2G","input":"2G"},{"name":"dataflow.executor-cores","value":2,"input":2},{"name":"dataflow.local-dirs","value":"","input":""},{"name":"dataflow.temp-dir","value":"/tmp/dataflow/spark","input":"/tmp/dataflow/spark"},{"name":"dataflow.sink-concat-files","value":"true","input":["true","false"]}],"retry":{"enable":False,"limit":1,"timeInterval":1,"intervalUnit":"MINUTES"}},"schedulerId":"once","ource":"rhinos","version":flow_info[0]["version"],"flowId":flow_info[0]["id"],"flowType":flow_info[0]["flow_type"],"name":flow_info[0]["name"]+data_now(),"flowVersion":flow_info[0]["version"],"creator":"admin","oldName":flow_info[0]["name"]}
            return data
        elif "multi_rtc_steps" == flow_name:
            data = {"creator":"admin","flowId":flow_info[0]["id"],"flowVersion":flow_info[0]["version"],"flowName":flow_info[0]["name"],"flowType":flow_info[0]["flow_type"],"name":flow_info[0]["name"]+data_now(),"properties":{"flowVersion":18,"settingsId":"","lastRunConfig":False,"engine":"flink","debug":False,"runtimeSettings":{"clusterId":"cluster1","master":"yarn","queue":"root.default","nodeLabel":"","driverMemory":1024,"executorMemory":1024,"executorCores":1,"parallelism":1,"useLatestState":False,"allowNonRestoredState":False,"lineageEnable":True,"savepointDir":"","proxyUser":"","kerberosEnable":False,"kerberosPrincipal":"","kerberosKeytab":"","kerberosJaasConf":"","flinkOpts":[],"flinkTableOpts":[],"javaOpts":""},"checkpointSettings":{"checkpointEnable":True,"checkpointMode":"exactly_once","checkpointDir":"hdfs:///tmp/flink/checkpoints","checkpointAsync":True,"checkpointStateBackend":"filesystem","checkpointIncremental":False,"checkpointInterval":10000,"checkpointMinpause":5000,"checkpointTimeout":600000,"checkpointExternalSave":True,"checkpointUnaligned":False},"restartStrategySettings":{"restartStrategy":"FixedDelayRestart","restartMaxAttempts":3,"restartInterval":60,"restartDelayInterval":10},"latencyTrackingSettings":{"latencyTrackingEnable":False,"latencyTrackingInterval":60000}}}
            return data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def query_dataflow_data(flow_name):
    try:
        sql = "select id from merce_flow where name ='%s' order by create_time desc limit 1" % flow_name
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        flow_id=[]
        flow_id.append(flow_info[0]["id"])
        data = {"fieldGroup":{"type":"FieldGroup","group":True,"andOr":"AND","fields":[{"type":"Field","group":False,"andOr":"AND","name":"name","oper":"LIKE","value":["%auto_apitest_df%"]},{"type":"Field","group":False,"andOr":"AND","name":"flowType","oper":"EQUAL","value":["dataflow"]},{"type":"Field","group":False,"andOr":"AND","name":"flowId","oper":"EQUAL","value":flow_id}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":20,"pageable":True}}
        return data
    except Exception as e:
        log.error("异常信息：%s" % e)


def get_executions_data(flow_name):
    try:
        data = flow_name.split("&")
        sql = "select flow_id,id from merce_flow_execution where flow_name like '%s%%%%' order by create_time desc limit 1" % data[0]
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        if "fieldName" in flow_name:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"name","comparatorOperator":"LIKE","fieldValue":"%auto_apitest%"},{"logicalOperator":"AND","fieldName":"flowType","comparatorOperator":"EQUAL","fieldValue":"dataflow"},{"logicalOperator":"AND","fieldName":"flowId","comparatorOperator":"EQUAL","fieldValue":flow_info[0]["flow_id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif "lastModifiedTime" in flow_name:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"flowType","comparatorOperator":"EQUAL","fieldValue":"dataflow"},{"logicalOperator":"AND","fieldName":"flowId","comparatorOperator":"EQUAL","fieldValue":flow_info[0]["flow_id"]},{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"GREATER_THAN","fieldValue":1664553600000},{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"LESS_THAN","fieldValue":1701359999000}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif "non" in flow_name:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"flowType","comparatorOperator":"EQUAL","fieldValue":"dataflow"},{"logicalOperator":"AND","fieldName":"flowId","comparatorOperator":"EQUAL","fieldValue":flow_info[0]["flow_id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif "execution_id" in flow_name:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"executionId","comparatorOperator":"EQUAL","fieldValue":flow_info[0]["id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def get_schedulers_data(flow_name):
    try:
        data =flow_name.split("&")
        sql = "select flow_id from merce_flow_schedule where flow_name like '%s%%%%' order by create_time desc limit 1" % data[0]
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        if "fieldName" in flow_name:
            new_data = {"fieldList": [{"logicalOperator": "AND", "fieldName": "name", "comparatorOperator": "LIKE",
                                       "fieldValue": "%auto_apitest%"},
                                      {"logicalOperator": "AND", "fieldName": "flowType", "comparatorOperator": "EQUAL",
                                       "fieldValue": "dataflow"},
                                      {"logicalOperator": "AND", "fieldName": "flowId", "comparatorOperator": "EQUAL",
                                       "fieldValue": flow_info[0]["flow_id"]}],
                        "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
            return new_data
        elif "lastModifiedTime" in flow_name:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"flowType","comparatorOperator":"EQUAL","fieldValue":"dataflow"},{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"GREATER_THAN","fieldValue":1664553600000},{"logicalOperator":"AND","fieldName":"lastModifiedTime","comparatorOperator":"LESS_THAN","fieldValue":1701446399000},{"logicalOperator":"AND","fieldName":"flowId","comparatorOperator":"EQUAL","fieldValue":flow_info[0]["flow_id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif "non" in flow_name:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"flowType","comparatorOperator":"EQUAL","fieldValue":"dataflow"},{"logicalOperator":"AND","fieldName":"flowId","comparatorOperator":"EQUAL","fieldValue":flow_info[0]["flow_id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif "enabled0" in flow_name:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"flowType","comparatorOperator":"EQUAL","fieldValue":"dataflow"},{"logicalOperator":"AND","fieldName":"enabled","comparatorOperator":"EQUAL","fieldValue":0},{"logicalOperator":"AND","fieldName":"flowId","comparatorOperator":"EQUAL","fieldValue":flow_info[0]["flow_id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif "enabled1" in flow_name:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"flowType","comparatorOperator":"EQUAL","fieldValue":"dataflow"},{"logicalOperator":"AND","fieldName":"enabled","comparatorOperator":"EQUAL","fieldValue":1},{"logicalOperator":"AND","fieldName":"flowId","comparatorOperator":"EQUAL","fieldValue":flow_info[0]["flow_id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif "cron" in flow_name:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"flowType","comparatorOperator":"EQUAL","fieldValue":"dataflow"},{"logicalOperator":"AND","fieldName":"schedulerId","comparatorOperator":"EQUAL","fieldValue":"cron"},{"logicalOperator":"AND","fieldName":"flowId","comparatorOperator":"EQUAL","fieldValue":flow_info[0]["flow_id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif "once" in flow_name:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"flowType","comparatorOperator":"EQUAL","fieldValue":"dataflow"},{"logicalOperator":"AND","fieldName":"schedulerId","comparatorOperator":"EQUAL","fieldValue":"once"},{"logicalOperator":"AND","fieldName":"flowId","comparatorOperator":"EQUAL","fieldValue":flow_info[0]["flow_id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        elif "event" in flow_name:
            new_data = {"fieldList":[{"logicalOperator":"AND","fieldName":"flowType","comparatorOperator":"EQUAL","fieldValue":"dataflow"},{"logicalOperator":"AND","fieldName":"schedulerId","comparatorOperator":"EQUAL","fieldValue":"event"},{"logicalOperator":"AND","fieldName":"flowId","comparatorOperator":"EQUAL","fieldValue":flow_info[0]["flow_id"]}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
            return new_data
        else:
            return
    except Exception as e:
            log.error("异常信息：%s" % e)
