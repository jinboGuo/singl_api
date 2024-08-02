# coding:utf-8
from basic_info.setting import ms, log
from util.get_deal_parameter import get_tenant_id
from util.timestamp_13 import get_now, data_now


def get_dataflow_data(flow_name):
    tenant_id = get_tenant_id()
    try:
        sql = "select id,version,name,flow_type from merce_flow where tenant_id='%s' and name = '%s' ORDER BY create_time desc limit 1" %(tenant_id,flow_name)
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        if "auto_api_test" == flow_name:
            data = {"needApproval":1,"name":flow_info[0]["name"]+data_now(),"flowId":flow_info[0]["id"],"flowName":flow_info[0]["name"],"flowType":flow_info[0]["flow_type"],"flowVersion":flow_info[0]["version"],"schedulerId":"once","source":"rhinos","configurations":{"event":{"sourceId":"","type":"","datasetId":""},"mqConfig":{"nameServer":"","topic":"","groupId":"","subExpression":"*"},"startTime":get_now(),"endTime":None,"arguments":[],"extraConfigurations":{},"extraConfigurationsArray":[],"dependencies":[],"cron":"","schedulingMode":"TimingSchedulingMode","properties":[{"name":"all.debug","value":"false"},{"name":"all.debug-rows","value":20},{"name":"all.dataset-nullable","value":"false"},{"name":"all.optimized-enable","value":"true"},{"name":"all.output-notify","value":"true"},{"name":"all.cluster-id","value":"cluster1"},{"name":"dataflow.queue","value":"root.default"},{"name":"dataflow.verbose","value":"true"},{"name":"dataflow.master","value":"yarn"},{"name":"dataflow.deploy-mode","value":"cluster"},{"name":"dataflow.num-executors","value":1},{"name":"dataflow.driver-memory","value":"1G"},{"name":"dataflow.executor-memory","value":"2G"},{"name":"dataflow.executor-cores","value":1},{"name":"dataflow.local-dirs","value":""},{"name":"dataflow.temp-dir","value":"/tmp/dataflow/spark"},{"name":"dataflow.sink-concat-files","value":"true"}],"retry":{"enable":False,"limit":1,"timeInterval":1,"intervalUnit":"MINUTES"}}}
            return data
        elif "ES-upsert" == flow_name:
            data = {"needApproval":1,"name":flow_info[0]["name"]+data_now(),"flowId":flow_info[0]["id"],"flowName":flow_info[0]["name"],"flowType":flow_info[0]["flow_type"],"flowVersion":flow_info[0]["version"],"schedulerId":"once","source":"rhinos","configurations":{"event":{"sourceId":"","type":"","datasetId":""},"mqConfig":{"nameServer":"","topic":"","groupId":"","subExpression":"*"},"startTime":get_now(),"endTime":None,"arguments":[],"extraConfigurations":{},"extraConfigurationsArray":[],"dependencies":[],"cron":"","schedulingMode":"TimingSchedulingMode","properties":[{"name":"all.debug","value":"false"},{"name":"all.debug-rows","value":20},{"name":"all.dataset-nullable","value":"false"},{"name":"all.optimized-enable","value":"true"},{"name":"all.output-notify","value":"true"},{"name":"all.cluster-id","value":"cluster1"},{"name":"dataflow.queue","value":"root.default"},{"name":"dataflow.verbose","value":"true"},{"name":"dataflow.master","value":"yarn"},{"name":"dataflow.deploy-mode","value":"cluster"},{"name":"dataflow.num-executors","value":1},{"name":"dataflow.driver-memory","value":"1G"},{"name":"dataflow.executor-memory","value":"2G"},{"name":"dataflow.executor-cores","value":1},{"name":"dataflow.local-dirs","value":""},{"name":"dataflow.temp-dir","value":"/tmp/dataflow/spark"},{"name":"dataflow.sink-concat-files","value":"true"}],"retry":{"enable":False,"limit":1,"timeInterval":1,"intervalUnit":"MINUTES"}}}
            return data
        elif "auto_apitest_df" == flow_name:
            data = {"needApproval":1,"name":flow_info[0]["name"]+data_now(),"flowId":flow_info[0]["id"],"flowName":flow_info[0]["name"],"flowType":flow_info[0]["flow_type"],"flowVersion":flow_info[0]["version"],"schedulerId":"once","source":"rhinos","configurations":{"event":{"sourceId":"","type":"","datasetId":""},"mqConfig":{"nameServer":"","topic":"","groupId":"","subExpression":"*"},"startTime":get_now(),"endTime":None,"arguments":[],"extraConfigurations":{},"extraConfigurationsArray":[],"dependencies":[],"cron":"","schedulingMode":"TimingSchedulingMode","properties":[{"name":"all.debug","value":"false"},{"name":"all.debug-rows","value":20},{"name":"all.dataset-nullable","value":"false"},{"name":"all.optimized-enable","value":"true"},{"name":"all.output-notify","value":"true"},{"name":"all.cluster-id","value":"cluster1"},{"name":"dataflow.queue","value":"root.default"},{"name":"dataflow.verbose","value":"true"},{"name":"dataflow.master","value":"yarn"},{"name":"dataflow.deploy-mode","value":"cluster"},{"name":"dataflow.num-executors","value":1},{"name":"dataflow.driver-memory","value":"1G"},{"name":"dataflow.executor-memory","value":"2G"},{"name":"dataflow.executor-cores","value":1},{"name":"dataflow.local-dirs","value":""},{"name":"dataflow.temp-dir","value":"/tmp/dataflow/spark"},{"name":"dataflow.sink-concat-files","value":"true"}],"retry":{"enable":False,"limit":1,"timeInterval":1,"intervalUnit":"MINUTES"}}}
            return data
        elif "mutil_sink_storage" == flow_name:
            data = {"needApproval":1,"name":flow_info[0]["name"]+data_now(),"flowId":flow_info[0]["id"],"flowName":flow_info[0]["name"],"flowType":flow_info[0]["flow_type"],"flowVersion":flow_info[0]["version"],"schedulerId":"once","source":"rhinos","configurations":{"event":{"sourceId":"","type":"","datasetId":""},"mqConfig":{"nameServer":"","topic":"","groupId":"","subExpression":"*"},"startTime":get_now(),"endTime":None,"arguments":[],"extraConfigurations":{},"extraConfigurationsArray":[],"dependencies":[],"cron":"","schedulingMode":"TimingSchedulingMode","properties":[{"name":"all.debug","value":"false"},{"name":"all.debug-rows","value":20},{"name":"all.dataset-nullable","value":"false"},{"name":"all.optimized-enable","value":"true"},{"name":"all.output-notify","value":"true"},{"name":"all.cluster-id","value":"cluster1"},{"name":"dataflow.queue","value":"root.default"},{"name":"dataflow.verbose","value":"true"},{"name":"dataflow.master","value":"yarn"},{"name":"dataflow.deploy-mode","value":"cluster"},{"name":"dataflow.num-executors","value":1},{"name":"dataflow.driver-memory","value":"1G"},{"name":"dataflow.executor-memory","value":"2G"},{"name":"dataflow.executor-cores","value":1},{"name":"dataflow.local-dirs","value":""},{"name":"dataflow.temp-dir","value":"/tmp/dataflow/spark"},{"name":"dataflow.sink-concat-files","value":"true"}],"retry":{"enable":False,"limit":1,"timeInterval":1,"intervalUnit":"MINUTES"}}}
            return data
        elif "scheduer" == flow_name:
            data = {"needApproval":1,"name":flow_info[0]["name"]+data_now(),"flowId":flow_info[0]["id"],"flowName":flow_info[0]["name"],"flowType":flow_info[0]["flow_type"],"flowVersion":flow_info[0]["version"],"schedulerId":"once","source":"rhinos","configurations":{"event":{"sourceId":"","type":"","datasetId":""},"mqConfig":{"nameServer":"","topic":"","groupId":"","subExpression":"*"},"startTime":get_now(),"endTime":None,"arguments":[],"extraConfigurations":{},"extraConfigurationsArray":[],"dependencies":[],"cron":"","schedulingMode":"TimingSchedulingMode","properties":[{"name":"all.debug","value":"false"},{"name":"all.debug-rows","value":20},{"name":"all.dataset-nullable","value":"false"},{"name":"all.optimized-enable","value":"true"},{"name":"all.output-notify","value":"true"},{"name":"all.cluster-id","value":"cluster1"},{"name":"dataflow.queue","value":"root.default"},{"name":"dataflow.verbose","value":"true"},{"name":"dataflow.master","value":"yarn"},{"name":"dataflow.deploy-mode","value":"cluster"},{"name":"dataflow.num-executors","value":1},{"name":"dataflow.driver-memory","value":"1G"},{"name":"dataflow.executor-memory","value":"2G"},{"name":"dataflow.executor-cores","value":1},{"name":"dataflow.local-dirs","value":""},{"name":"dataflow.temp-dir","value":"/tmp/dataflow/spark"},{"name":"dataflow.sink-concat-files","value":"true"}],"retry":{"enable":False,"limit":1,"timeInterval":1,"intervalUnit":"MINUTES"}}}
            return data
        elif "multi_rtc_steps" == flow_name:
            data = {"creator":"admin","flowId":flow_info[0]["id"],"flowVersion":flow_info[0]["version"],"flowName":flow_info[0]["name"],"flowType":flow_info[0]["flow_type"],"id":"","needApproval":1,"name":flow_info[0]["name"]+data_now(),"schedulerId":"once","configurations":{"properties":{"name":flow_info[0]["name"]+data_now(),"flowName":flow_info[0]["name"],"flowVersion":flow_info[0]["version"],"settingsId":"","lastRunConfig":False,"engine":"flink","debug":False,"retry":{"enable":False,"limit":0,"timeInterval":0,"intervalUnit":"SECONDS"},"runtimeSettings":{"clusterId":"cluster1","master":"yarn","queue":"root.default","nodeLabel":"","driverMemory":1024,"executorMemory":1024,"executorCores":1,"parallelism":1,"pipelineChaining":True,"useLatestState":False,"allowNonRestoredState":False,"savepointDir":"","proxyUser":"","kerberosEnable":False,"kerberosPrincipal":"","kerberosKeytab":"","kerberosJaasConf":"","flinkOpts":[],"flinkTableOpts":[],"javaOpts":""},"checkpointSettings":{"checkpointEnable":True,"checkpointMode":"exactly_once","checkpointDir":"hdfs:///tmp/flink/checkpoints","checkpointAsync":True,"checkpointStateBackend":"filesystem","checkpointIncremental":False,"checkpointInterval":10000,"checkpointMinpause":5000,"checkpointTimeout":600000,"checkpointExternalSave":True,"checkpointUnaligned":False},"restartStrategySettings":{"restartStrategy":"FixedDelayRestart","restartMaxAttempts":3,"restartInterval":60,"restartDelayInterval":10},"latencyTrackingSettings":{"latencyTrackingEnable":False,"latencyTrackingInterval":60000}},"startTime":get_now(),"needApproval":1}}
            return data
        else:
            return
    except Exception as e:
        log.error("异常信息：%s" % e)


def query_dataflow_data(flow_name):
    try:
        tenant_id = get_tenant_id()
        sql = "select flow_scheduler_id from merce_flow_execution where tenant_id='%s' and flow_name = '%s' ORDER BY create_time desc limit 1" %(tenant_id,flow_name)
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        flow_scheduler_id= [flow_info[0]["flow_scheduler_id"]]
        data = {"fieldGroup":{"type":"FieldGroup","group":True,"andOr":"AND","fields":[{"type":"Field","group":False,"andOr":"AND","name":"flowSchedulerId","oper":"EQUAL","value":flow_scheduler_id,"label":""}]},"ordSort":[{"name":"lastModifiedTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":20,"pageable":True}}
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
