# coding:utf-8
from basic_info.setting import ms, log
from util.timestamp_13 import get_now, data_now, timestamp_utc


def get_dataflow_data(flow_name):

    try:
        sql = "select id,version,name,flow_type from merce_flow where name ='%s' order by create_time desc limit 1" % flow_name
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        if "auto_api_test" == flow_name:
            data = {"configurations": {"startTime": get_now(), "arguments": [], "dependencies": [],
                                       "extraConfigurations": {},
                                       "properties": [{"name": "all.debug", "value": "false", "input": "false"},
                                                      {"name": "all.dataset-nullable", "value": "false",
                                                       "input": "false"},
                                                      {"name": "all.optimized.enable", "value": "true",
                                                       "input": "true"},
                                                      {"name": "all.lineage.enable", "value": "false",
                                                       "input": "false"},
                                                      {"name": "all.debug-rows", "value": "20", "input": "20"},
                                                      {"name": "all.runtime.cluster-id", "value": "cluster1",
                                                       "input": ["cluster1"]},
                                                      {"name": "dataflow.master", "value": "yarn", "input": "yarn"},
                                                      {"name": "dataflow.deploy-mode", "value": "client",
                                                       "input": ["client", "cluster"]},
                                                      {"name": "dataflow.queue", "value": "default",
                                                       "input": ["default"]},
                                                      {"name": "dataflow.num-executors", "value": "2", "input": "2"},
                                                      {"name": "dataflow.driver-memory", "value": "512M",
                                                       "input": "512M"},
                                                      {"name": "dataflow.executor-memory", "value": "1G",
                                                       "input": "1G"},
                                                      {"name": "dataflow.executor-cores", "value": "2", "input": "2"},
                                                      {"name": "dataflow.verbose", "value": "true", "input": "true"},
                                                      {"name": "dataflow.local-dirs", "value": "", "input": ""},
                                                      {"name": "dataflow.sink.concat-files", "value": "true",
                                                       "input": "true"},
                                                      {"name": "dataflow.tempDirectory", "value": "/tmp/dataflow/spark",
                                                       "input": "/tmp/dataflow/spark"}],
                                       "retry": {"enable": "false", "limit": 1, "timeInterval": 1,
                                                 "intervalUnit": "MINUTES"}}, "schedulerId": "once", "ource": "rhinos",
                    "version": flow_info[0]["version"], "flowId": flow_info[0]["id"], "flowType": flow_info[0]["flow_type"],
                    "name": flow_info[0]["name"]+data_now(), "creator": "admin", "oldName": flow_info[0]["name"]}
            return data
        elif "ES-upsert" == flow_name:
            data = {"configurations": {"startTime": get_now(), "arguments": [], "dependencies": [],
                                       "extraConfigurations": {},
                                       "properties": [{"name": "all.debug", "value": "false", "input": "false"},
                                                      {"name": "all.dataset-nullable", "value": "false",
                                                       "input": "false"},
                                                      {"name": "all.optimized.enable", "value": "true",
                                                       "input": "true"},
                                                      {"name": "all.lineage.enable", "value": "false",
                                                       "input": "false"},
                                                      {"name": "all.debug-rows", "value": "20", "input": "20"},
                                                      {"name": "all.runtime.cluster-id", "value": "cluster1",
                                                       "input": ["cluster1"]},
                                                      {"name": "dataflow.master", "value": "yarn", "input": "yarn"},
                                                      {"name": "dataflow.deploy-mode", "value": "client",
                                                       "input": ["client", "cluster"]},
                                                      {"name": "dataflow.queue", "value": "default",
                                                       "input": ["default"]},
                                                      {"name": "dataflow.num-executors", "value": "2", "input": "2"},
                                                      {"name": "dataflow.driver-memory", "value": "512M",
                                                       "input": "512M"},
                                                      {"name": "dataflow.executor-memory", "value": "1G",
                                                       "input": "1G"},
                                                      {"name": "dataflow.executor-cores", "value": "2", "input": "2"},
                                                      {"name": "dataflow.verbose", "value": "true", "input": "true"},
                                                      {"name": "dataflow.local-dirs", "value": "", "input": ""},
                                                      {"name": "dataflow.sink.concat-files", "value": "true",
                                                       "input": "true"},
                                                      {"name": "dataflow.tempDirectory", "value": "/tmp/dataflow/spark",
                                                       "input": "/tmp/dataflow/spark"}],
                                       "retry": {"enable": "false", "limit": 1, "timeInterval": 1,
                                                 "intervalUnit": "MINUTES"}}, "schedulerId": "once", "ource": "rhinos",
                    "version": flow_info[0]["version"], "flowId": flow_info[0]["id"], "flowType": flow_info[0]["flow_type"],
                    "name": flow_info[0]["name"]+data_now(), "creator": "admin", "oldName": flow_info[0]["name"]}
            return data
        elif "auto_apitest_df" == flow_name:
            data = {"configurations": {"startTime": get_now(), "arguments": [], "dependencies": [],
                                       "extraConfigurations": {},
                                       "properties": [{"name": "all.debug", "value": "false", "input": "false"},
                                                      {"name": "all.dataset-nullable", "value": "false",
                                                       "input": "false"},
                                                      {"name": "all.optimized.enable", "value": "true",
                                                       "input": "true"},
                                                      {"name": "all.lineage.enable", "value": "false",
                                                       "input": "false"},
                                                      {"name": "all.debug-rows", "value": "20", "input": "20"},
                                                      {"name": "all.runtime.cluster-id", "value": "cluster1",
                                                       "input": ["cluster1"]},
                                                      {"name": "dataflow.master", "value": "yarn", "input": "yarn"},
                                                      {"name": "dataflow.deploy-mode", "value": "client",
                                                       "input": ["client", "cluster"]},
                                                      {"name": "dataflow.queue", "value": "default",
                                                       "input": ["default"]},
                                                      {"name": "dataflow.num-executors", "value": "2", "input": "2"},
                                                      {"name": "dataflow.driver-memory", "value": "512M",
                                                       "input": "512M"},
                                                      {"name": "dataflow.executor-memory", "value": "1G",
                                                       "input": "1G"},
                                                      {"name": "dataflow.executor-cores", "value": "2", "input": "2"},
                                                      {"name": "dataflow.verbose", "value": "true", "input": "true"},
                                                      {"name": "dataflow.local-dirs", "value": "", "input": ""},
                                                      {"name": "dataflow.sink.concat-files", "value": "true",
                                                       "input": "true"},
                                                      {"name": "dataflow.tempDirectory", "value": "/tmp/dataflow/spark",
                                                       "input": "/tmp/dataflow/spark"}],
                                       "retry": {"enable": "false", "limit": 1, "timeInterval": 1,
                                                 "intervalUnit": "MINUTES"}}, "schedulerId": "once", "ource": "rhinos",
                    "version": flow_info[0]["version"], "flowId": flow_info[0]["id"], "flowType": flow_info[0]["flow_type"],
                    "name": flow_info[0]["name"]+data_now(), "creator": "admin", "oldName": flow_info[0]["name"]}
            return data
        elif "mutil_sink_storage" == flow_name:
            data = {"configurations": {"startTime": get_now(), "arguments": [], "dependencies": [],
                                       "extraConfigurations": {},
                                       "properties": [{"name": "all.debug", "value": "false", "input": "false"},
                                                      {"name": "all.dataset-nullable", "value": "false",
                                                       "input": "false"},
                                                      {"name": "all.optimized.enable", "value": "true",
                                                       "input": "true"},
                                                      {"name": "all.lineage.enable", "value": "false",
                                                       "input": "false"},
                                                      {"name": "all.debug-rows", "value": "20", "input": "20"},
                                                      {"name": "all.runtime.cluster-id", "value": "cluster1",
                                                       "input": ["cluster1"]},
                                                      {"name": "dataflow.master", "value": "yarn", "input": "yarn"},
                                                      {"name": "dataflow.deploy-mode", "value": "client",
                                                       "input": ["client", "cluster"]},
                                                      {"name": "dataflow.queue", "value": "default",
                                                       "input": ["default"]},
                                                      {"name": "dataflow.num-executors", "value": "2", "input": "2"},
                                                      {"name": "dataflow.driver-memory", "value": "512M",
                                                       "input": "512M"},
                                                      {"name": "dataflow.executor-memory", "value": "1G",
                                                       "input": "1G"},
                                                      {"name": "dataflow.executor-cores", "value": "2", "input": "2"},
                                                      {"name": "dataflow.verbose", "value": "true", "input": "true"},
                                                      {"name": "dataflow.local-dirs", "value": "", "input": ""},
                                                      {"name": "dataflow.sink.concat-files", "value": "true",
                                                       "input": "true"},
                                                      {"name": "dataflow.tempDirectory", "value": "/tmp/dataflow/spark",
                                                       "input": "/tmp/dataflow/spark"}],
                                       "retry": {"enable": "false", "limit": 1, "timeInterval": 1,
                                                 "intervalUnit": "MINUTES"}}, "schedulerId": "once", "ource": "rhinos",
                    "version": flow_info[0]["version"], "flowId": flow_info[0]["id"], "flowType": flow_info[0]["flow_type"],
                    "name": flow_info[0]["name"]+data_now(), "creator": "admin", "oldName": flow_info[0]["name"]}
            return data
        elif "scheduer" == flow_name:
            data = {"configurations": {"startTime": get_now(), "arguments": [], "dependencies": [],
                                       "extraConfigurations": {},
                                       "properties": [{"name": "all.debug", "value": "false", "input": "false"},
                                                      {"name": "all.dataset-nullable", "value": "false",
                                                       "input": "false"},
                                                      {"name": "all.optimized.enable", "value": "true",
                                                       "input": "true"},
                                                      {"name": "all.lineage.enable", "value": "false",
                                                       "input": "false"},
                                                      {"name": "all.debug-rows", "value": "20", "input": "20"},
                                                      {"name": "all.runtime.cluster-id", "value": "cluster1",
                                                       "input": ["cluster1"]},
                                                      {"name": "dataflow.master", "value": "yarn", "input": "yarn"},
                                                      {"name": "dataflow.deploy-mode", "value": "client",
                                                       "input": ["client", "cluster"]},
                                                      {"name": "dataflow.queue", "value": "default",
                                                       "input": ["default"]},
                                                      {"name": "dataflow.num-executors", "value": "2", "input": "2"},
                                                      {"name": "dataflow.driver-memory", "value": "512M",
                                                       "input": "512M"},
                                                      {"name": "dataflow.executor-memory", "value": "1G",
                                                       "input": "1G"},
                                                      {"name": "dataflow.executor-cores", "value": "2", "input": "2"},
                                                      {"name": "dataflow.verbose", "value": "true", "input": "true"},
                                                      {"name": "dataflow.local-dirs", "value": "", "input": ""},
                                                      {"name": "dataflow.sink.concat-files", "value": "true",
                                                       "input": "true"},
                                                      {"name": "dataflow.tempDirectory", "value": "/tmp/dataflow/spark",
                                                       "input": "/tmp/dataflow/spark"}],
                                       "retry": {"enable": "false", "limit": 1, "timeInterval": 1,
                                                 "intervalUnit": "MINUTES"}}, "schedulerId": "once", "ource": "rhinos",
                    "version": flow_info[0]["version"], "flowId": flow_info[0]["id"], "flowType": flow_info[0]["flow_type"],
                    "name": flow_info[0]["name"]+data_now(), "creator": "admin", "oldName": flow_info[0]["name"]}
            return data
        elif "multi_rtc_steps" == flow_name:
            data = {"creator":"admin","flowId":flow_info[0]["id"],"flowVersion":flow_info[0]["version"],"flowName":flow_info[0]["name"],"flowType":flow_info[0]["flow_type"],"name":flow_info[0]["name"]+"#"+timestamp_utc(),"properties":{"clusterId":"cluster1","settingsId":"","lastRunConfig":False,"engine":"flink","debug":False,"runtimeSettings":{"master":"yarn","queue":"default","nodeLabel":"","driverMemory":1024,"executorMemory":1024,"executorCores":1,"parallelism":1,"useLatestState":False,"allowNonRestoredState":False,"lineageEnable":False,"savepointDir":"","kerberosEnable":False,"kerberosPrincipal":"","kerberosKeytab":"","kerberosJaasConf":"","flinkOpts":[],"javaOpts":""},"checkpointSettings":{"checkpointEnable":True,"checkpointMode":"exactly_once","checkpointDir":"hdfs:///tmp/flink/checkpoints","checkpointAsync":True,"checkpointStateBackend":"rocksdb","checkpointIncremental":False,"checkpointInterval":10000,"checkpointMinpause":5000,"checkpointTimeout":600000,"checkpointExternalSave":True,"checkpointUnaligned":False},"restartStrategySettings":{"restartStrategy":"FixedDelayRestart","restartMaxAttempts":3,"restartInterval":60,"restartDelayInterval":10},"latencyTrackingSettings":{"latencyTrackingEnable":False,"latencyTrackingInterval":60000}}}
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


#删除测试数据
def delete_autotest_datas():
    log.info("------开始删除测试数据-------")
    try:
        flow_sql = "delete from merce_flow where name like 'test%' or name like 'gjb%' or  name like 'auto_api_test_%' or name like 'lq%' or name like 'partition%'"
        flow_execution_sql = "delete from merce_flow_execution where name like 'lq%'"
        dataset_sql = "delete from merce_dataset where name like 'test%' or  name like 'merce%' or  name like 'sink%' or  name like 'gjb_test_%' or name like 'lq%'"
        schema_sql = "delete from merce_schema where name like 'test%' or  name like 'apitest%' or  name like  'gtest%' or name like 'lq%'"
        tenant_sql = "delete from merce_tenant where name like 'api_tenants%' order by create_time desc limit 1"
        filesets_sql="DELETE from merce_fileset"
        filesets_dir ="DELETE FROM merce_resource_dir where name like 'lq_fileset%'"
        dss_sql="DELETE FROM merce_dss where name like 'lq%'"
        log.info("删除flow表测试数据%s: " % flow_sql)
        ms.ExecuNoQuery(flow_sql.encode('utf-8'))
        log.info("删除flow_execution表测试数据%s: " % flow_execution_sql)
        ms.ExecuNoQuery(flow_execution_sql.encode('utf-8'))
        log.info("删除dataset表测试数据%s: " % dataset_sql)
        ms.ExecuNoQuery(dataset_sql.encode('utf-8'))
        log.info("删除schema表测试数据%s: " % schema_sql)
        ms.ExecuNoQuery(schema_sql.encode('utf-8'))
        log.info("删除tenant表测试数据%s: " % tenant_sql)
        ms.ExecuNoQuery(tenant_sql.encode('utf-8'))
        log.info("删除filesets表测试数据%s: " % filesets_sql)
        ms.ExecuNoQuery(filesets_sql.encode('utf-8'))
        log.info("删除filesets_dir表测试数据%s: " % filesets_dir)
        ms.ExecuNoQuery(filesets_dir.encode('utf-8'))
        ms.ExecuQuery(dss_sql.encode('utf-8'))
        log.info("删除dss表测试数据%s: " % dss_sql)
    except Exception as e:
        log.error("异常信息：%s" % e)

def delete_autotest_dw():
    try:
        dw_field_defined = "delete from dw_field_defined where name like 'id%' or name like 'dt%' or name like 'item%' or name like 'order%' or name like 'avgi%' or name like 'maxi%' or name like 'count%'"
        dw_ref_dataset = "delete from dw_ref_dataset where name like 'api_order%'"
        dw_metadata = "delete from dw_metadata where name like 'api_order%' or name like 'api_aggr%'"
        dw_model = "delete from dw_model where name like 'api_aggr%' or name like 'api_order%'"
        dw_category = "delete from dw_category where name like 'api_model%' or name like 'api_standard%'"
        dw_name_rules = "delete from dw_name_rules where alias like 'api_auto_namerule%'"
        dw_tagdef = "delete from dw_tagdef where name like 'api_auto_tag%'"
        dw_taggroup = "delete from dw_taggroup where name like 'api_auto_taggroup%'"
        dw_business = "delete from dw_business where name like 'api_auto_business%'"
        dw_project = "delete from dw_project where name like 'api_auto_projects%'"
        dw_subject = "delete from dw_subject where name like 'api_auto_subject%'"
        ms.ExecuNoQuery(dw_field_defined.encode('utf-8'))
        ms.ExecuNoQuery(dw_ref_dataset.encode('utf-8'))
        ms.ExecuNoQuery(dw_metadata.encode('utf-8'))
        ms.ExecuNoQuery(dw_model.encode('utf-8'))
        ms.ExecuNoQuery(dw_category.encode('utf-8'))
        ms.ExecuNoQuery(dw_name_rules.encode('utf-8'))
        ms.ExecuNoQuery(dw_tagdef.encode('utf-8'))
        ms.ExecuNoQuery(dw_taggroup.encode('utf-8'))
        ms.ExecuNoQuery(dw_business.encode('utf-8'))
        ms.ExecuNoQuery(dw_project.encode('utf-8'))
        ms.ExecuNoQuery(dw_subject.encode('utf-8'))
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
