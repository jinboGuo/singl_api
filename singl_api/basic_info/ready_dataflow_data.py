# coding:utf-8
from util.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG, MySQL_CONFIG1, Dw_MySQL_CONFIG


def get_dataflow_data(flow_name):
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
    try:
        sql = "select id, name,flow_type from merce_flow where name like '%s%%%%' order by create_time desc limit 1" % flow_name
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print("flow_id,flow_name,flow_type", flow_info[0]["id"], flow_info[0]["name"], flow_info[0]["flow_type"])
    except:
        return

    if "auto_api_test" in flow_name:
        data = {"configurations":{"arguments":[],"properties":[{"name":"all.debug","value":"false","input":"false"},{"name":"all.dataset-nullable","value":"false","input":"false"},{"name":"all.optimized.enable","value":"true","input":"true"},{"name":"all.lineage.enable","value":"false","input":"false"},{"name":"all.debug-rows","value":"20","input":"20"},{"name":"all.runtime.cluster-id","value":"cluster1","input":["cluster1"]},{"name":"dataflow.master","value":"yarn","input":"yarn"},{"name":"dataflow.deploy-mode","value":"client","input":["client","cluster"]},{"name":"dataflow.queue","value":"default","input":["default"]},{"name":"dataflow.num-executors","value":"2","input":"2"},{"name":"dataflow.driver-memory","value":"512M","input":"512M"},{"name":"dataflow.executor-memory","value":"1G","input":"1G"},{"name":"dataflow.executor-cores","value":"2","input":"2"},{"name":"dataflow.verbose","value":"true","input":"true"},{"name":"dataflow.local-dirs","value":"","input":""},{"name":"dataflow.sink.concat-files","value":"true","input":"true"},{"name":"dataflow.tempDirectory","value":"/tmp/dataflow/spark","input":"/tmp/dataflow/spark"}],"dependencies":"","extraConfigurations":{},"startTime":1593964800000},"flowId": flow_info[0]["id"],"flowName":flow_info[0]["name"],"flowType":flow_info[0]["flow_type"],"name":"auto_api_test_随机数","schedulerId":"once","source":"rhinos"}
        return data
    elif "ES-upsert" in flow_name:
        data = {"configurations":{"startTime":1600617600000,"arguments":[],"dependencies":[],"extraConfigurations":{},"properties":[{"name":"all.debug","value":"false","input":"false"},{"name":"all.dataset-nullable","value":"false","input":"false"},{"name":"all.optimized.enable","value":"true","input":"true"},{"name":"all.lineage.enable","value":"false","input":"false"},{"name":"all.debug-rows","value":"20","input":"20"},{"name":"all.runtime.cluster-id","value":"cluster1","input":["cluster1"]},{"name":"dataflow.master","value":"yarn","input":"yarn"},{"name":"dataflow.deploy-mode","value":"client","input":["client","cluster"]},{"name":"dataflow.queue","value":"default","input":["default"]},{"name":"dataflow.num-executors","value":"2","input":"2"},{"name":"dataflow.driver-memory","value":"512M","input":"512M"},{"name":"dataflow.executor-memory","value":"1G","input":"1G"},{"name":"dataflow.executor-cores","value":"2","input":"2"},{"name":"dataflow.verbose","value":"true","input":"true"},{"name":"dataflow.local-dirs","value":"","input":""},{"name":"dataflow.sink.concat-files","value":"true","input":"true"},{"name":"dataflow.tempDirectory","value":"/tmp/dataflow/spark","input":"/tmp/dataflow/spark"}],"retry":{"enable":"false","limit":1,"timeInterval":1,"intervalUnit":"MINUTES"}},"schedulerId":"once","ource":"rhinos","version":1,"flowId":flow_info[0]["id"],"flowType":flow_info[0]["flow_type"],"name":"ES-upsert_随机数","creator":"admin","oldName":flow_info[0]["name"]}
        return data
    else:
        return

def query_dataflow_data(flow_name):
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
    try:
        sql = "select id from merce_flow where name like '%s%%%%' order by create_time desc limit 1" % flow_name
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print("flow_id", flow_info[0]["id"])
    except:
        return
    data = {"fieldList":[{"fieldName":"flowId","fieldValue": flow_info[0]["id"],"comparatorOperator":"EQUAL","logicalOperator":"AND"}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
    return data

def get_executions_data(flow_name):
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
    try:
        sql = "select id from merce_flow_execution where flow_name like '%s%%%%' order by create_time desc limit 1" % flow_name
        flow_info = ms.ExecuQuery(sql.encode('utf-8'))
        print(sql)
        print('execution_Id:', flow_info[0]["id"])
    except:
        return
    new_data = {"fieldList": [{"fieldName": "executionId", "fieldValue": flow_info[0]["id"], "comparatorOperator": "EQUAL","logicalOperator":"AND"}], "sortObject": {"field": "lastModifiedTime", "orderDirection": "DESC"}, "offset": 0, "limit": 8}
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
    print("------开始删除测试数据-------")
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
    try:
        flow_sql = "delete from merce_flow where name like 'test%' or name like 'gjb%' or  name like 'auto_api_test_%' or name like 'lq%'"
        flow_execution_sql = "delete from merce_flow_execution where name like 'lq%'"
        dataset_sql = "delete from merce_dataset where name like 'test%' or  name like 'merce%' or  name like 'sink%' or  name like 'gjb_test_%' or name like 'lq%'"
        schema_sql = "delete from merce_schema where name like 'test%' or  name like 'apitest%' or  name like  'gtest%' or name like 'lq%'"
        tenant_sql = "delete from merce_tenant where name like 'api_tenants%' order by create_time desc limit 1"
        filesets_sql="DELETE from merce_fileset"
        filesets_dir ="DELETE FROM merce_resource_dir where name like 'lq_fileset%'"
        print("删除flow表测试数据 ", flow_sql)
        ms.ExecuNoQuery(flow_sql.encode('utf-8'))
        print("删除flow_execution表测试数据 ", flow_execution_sql)
        ms.ExecuNoQuery(flow_execution_sql.encode('utf-8'))
        print("删除dataset表测试数据 ", dataset_sql)
        ms.ExecuNoQuery(dataset_sql.encode('utf-8'))
        print("删除schema表测试数据 ", schema_sql)
        ms.ExecuNoQuery(schema_sql.encode('utf-8'))
        print("删除tenant表测试数据 ", tenant_sql)
        ms.ExecuNoQuery(tenant_sql.encode('utf-8'))
        print("删除filesets表测试数据 ", filesets_sql)
        ms.ExecuNoQuery(filesets_sql.encode('utf-8'))
        print("删除filesets_dir表测试数据 ", filesets_dir)
        ms.ExecuNoQuery(filesets_dir.encode('utf-8'))
    except:
       return

def delete_autotest_dw():
    print("------开始删除测试数据-------")
    ms = MYSQL(Dw_MySQL_CONFIG["HOST"], Dw_MySQL_CONFIG["USER"], Dw_MySQL_CONFIG["PASSWORD"], Dw_MySQL_CONFIG["DB"])
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
        print("删除dw_field_defined表测试数据 ", dw_field_defined)
        ms.ExecuNoQuery(dw_field_defined.encode('utf-8'))
        print("删除dw_ref_dataset表测试数据 ", dw_ref_dataset)
        ms.ExecuNoQuery(dw_ref_dataset.encode('utf-8'))
        print("删除dw_metadata表测试数据 ", dw_metadata)
        ms.ExecuNoQuery(dw_metadata.encode('utf-8'))
        print("删除dw_model表测试数据 ", dw_model)
        ms.ExecuNoQuery(dw_model.encode('utf-8'))
        print("删除dw_category表测试数据 ", dw_category)
        ms.ExecuNoQuery(dw_category.encode('utf-8'))
        print("删除dw_name_rules表测试数据 ", dw_name_rules)
        ms.ExecuNoQuery(dw_name_rules.encode('utf-8'))
        print("删除dw_tagdef表测试数据 ", dw_tagdef)
        ms.ExecuNoQuery(dw_tagdef.encode('utf-8'))
        print("删除dw_taggroup表测试数据 ", dw_taggroup)
        ms.ExecuNoQuery(dw_taggroup.encode('utf-8'))
        print("删除dw_business表测试数据 ", dw_business)
        ms.ExecuNoQuery(dw_business.encode('utf-8'))
        print("删除dw_project表测试数据 ", dw_project)
        ms.ExecuNoQuery(dw_project.encode('utf-8'))
        print("删除dw_subject表测试数据 ", dw_subject)
        ms.ExecuNoQuery(dw_subject.encode('utf-8'))
    except:
        return