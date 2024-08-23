# coding:utf-8
import json
import random
from basic_info.setting import log, ms
from util.get_deal_parameter import get_tenant_id, get_owner


def alarm_job():
    tenant_id = get_tenant_id()
    
    try:
        sql="select id,name from merce_flow where  total_executed >1  and flow_type='dataflow' and is_hide =0  and tenant_id='{}' order by  last_modified_time desc limit 1".format(tenant_id)
        ids = ms.ExecuQuery(sql.encode('utf-8'))
        data=json.dumps({"monitoredTasks": [{"model": "数据计算","taskName": "%s","type": "dataflow","taskId": "%s"}],"name": "alarmmage_test%s","description ":""})
        data=data % (ids[0]['name'],ids[0]['id'],str(random.randint(111,999)))
        return data
    except Exception as e:
        log.error("没有获取到目录id：%s" % e)
    

def alarm_config(num):
    tenant_id = get_tenant_id()
    user_id=get_owner()
    tmpdata=[{
            "sourceName": "%s",
            "alarmRuleName": "任务成功",
            "judgmentLogic": "=",
            "value": 1,
            "monitorId": "%s",
            "alarmLevel": "通知",
            "noteRule": "inner",
            "userNames": ["admin"],
            "model": "数据计算",
            "type": "dataflow",
            "sourceId": "%s",
            "sourceType": "数据计算",
            "alarmType": "批计算",
            "alarmRuleId": "%s",
            "alarmEvent": "任务执行成功",
            "userIds": ["%s"],
            "sort": 0
        }, {
            "sourceName": "%s",
            "alarmRuleName": "任务结束",
            "judgmentLogic": "=",
            "value": 1,
            "monitorId": "%s",
            "alarmLevel": "通知",
            "noteRule": "inner",
            "userNames": ["admin"],
            "model": "数据计算",
            "type": "dataflow",
            "sourceId": "%s",
            "sourceType": "数据计算",
            "alarmType": "批计算",
            "alarmRuleId": "%s",
            "alarmEvent": "任务执行结束",
            "userIds": ["%s"],
            "sort": 1
        }, {
            "sourceName": "%s",
            "alarmRuleName": "任务失败",
            "judgmentLogic": "=",
            "value": 1,
            "monitorId": "%s",
            "alarmLevel": "通知",
            "noteRule": "inner",
            "userNames": ["admin"],
            "model": "数据计算",
            "type": "dataflow",
            "sourceId": "%s",
            "sourceType": "数据计算",
            "alarmType": "批计算",
            "alarmRuleId": "%s",
            "alarmEvent": "任务执行失败",
            "userIds": ["%s"],
            "sort": 2
        }, {
            "sourceName": "%s",
            "alarmRuleName": "任务执行时长",
            "judgmentLogic": "=",
            "value": 1,
            "monitorId": "%s",
            "alarmLevel": "通知",
            "noteRule": "inner",
            "userNames": ["admin"],
            "model": "数据计算",
            "type": "dataflow",
            "sourceId": "%s",
            "sourceType": "数据计算",
            "alarmType": "批计算",
            "alarmRuleId": "%s",
            "alarmEvent": "任务执行时长${判断逻辑}${判断值}",
            "userIds": ["%s"],
            "sort": 3
        }]
    tmpdata1={
        "tenantId": "%s",
        "owner": "%s",
        "name": "%s",
        "enabled": 1,
        "creator": "admin",
        "createTime": "2024-08-13 11:33:04",
        "lastModifier": "admin",
        "lastModifiedTime": "2024-08-13 11:43:20",
        "id": "%s",
        "monitoredTasks": [{
            "model": "数据计算",
            "type": "dataflow",
            "taskId": "%s",
            "taskName": "%s"
        }, {
            "model": "数据采集",
            "taskName": "%s",
            "type": "collectLocal",
            "taskId": "%s"
        }, {
            "model": "指标管理",
            "taskName": "%s",
            "type": "indicator",
            "taskId": "%s"
        }, {
            "model": "元数据采集",
            "taskName": "%s",
            "type": "schemaCollect",
            "taskId": "%s"
        }],
        "description": "修改",
        "configNum": 4,
        "enable": True
    }
    try:
        sql = "select id from alarm_rule_desc where job_type='批计算'  and tenant_id ='{}' order by sort ".format(tenant_id)
        ids = ms.ExecuQuery(sql.encode('utf-8'))
        
        sql1="select id,name From alarm_monitor where name like 'alarmmage_test%'  and tenant_id ='{}'  order by create_time desc limit 1".format(tenant_id)
        mids= ms.ExecuQuery(sql1.encode('utf-8'))
        mid=str(mids[0]['id'])
        midname=mids[0]['name']
        
        sql2="select id,name from merce_flow where  total_executed >1  and flow_type='dataflow' and is_hide =0  and tenant_id='{}' order by  last_modified_time desc limit 1".format(tenant_id)
        jobid = ms.ExecuQuery(sql2.encode('utf-8'))
        
        sql3="select id,name from poseidon_task where exec_number >=1 and status ='ONLINE' and tenant_id='{}'  order by last_modified_time desc limit 1".format(tenant_id)
        poseid= ms.ExecuQuery(sql3.encode('utf-8'))
        
        sql4="select id,name from schema_collect_task where exec_number >=1 and status ='ONLINE'  and tenant_id='{}' order by last_modified_time desc limit 1".format(tenant_id)
        scheid= ms.ExecuQuery(sql4.encode('utf-8'))
        
        sql5="select id, name from ind_indicator where total_executed >=1 and status ='ONLINE'   and tenant_id='{}'  order by last_modified_time  desc limit 1".format(tenant_id)
        inid= ms.ExecuQuery(sql5.encode('utf-8'))
        
        
        data=str(tmpdata) % (jobid[0]['name'],mid,jobid[0]['id'],ids[0]['id'],user_id,jobid[0]['name'],mid,jobid[0]['id'],ids[1]['id'],user_id,jobid[0]['name'],mid,jobid[0]['id'],ids[2]['id'],user_id,jobid[0]['name'],mid,jobid[0]['id'],ids[3]['id'],user_id)
        data1=str(tmpdata1)% (tenant_id,user_id,midname,mid,jobid[0]['id'],jobid[0]['name'],poseid[0]['name'],poseid[0]['id'],inid[0]['name'],inid[0]['id'],scheid[0]['name'],scheid[0]['id'])

        if num==0:
            return mid,data
        elif num==1:
            return data1.replace('True','true')
    except Exception as e:
        log.error("没有获取到目录id：%s" % e)


def alarm_jap(data,num):
    tenant_id = get_tenant_id()
    try:
        sql1="select id From alarm_record where source_type ='数据计算'  and tenant_id='{}'  order by last_modified_time  desc limit 1".format(tenant_id)
        sql2="select id From alarm_record where source_type ='数据采集' and tenant_id='{}'  order by last_modified_time  desc limit 1".format(tenant_id)
        sql3="select id From alarm_record where source_type ='元数据采集' and tenant_id='{}'  order by last_modified_time  desc limit 1".format(tenant_id)
        
        

        cal= ms.ExecuQuery(sql1.encode('utf-8'))
        col = ms.ExecuQuery(sql2.encode('utf-8'))
        sche = ms.ExecuQuery(sql3.encode('utf-8'))

        if num==0:
            url=data.format(sche[0]['id'])
        elif num==1:
            url=data.format(col[0]['id'])
        elif num==2:
            url=data.format(cal[0]['id'])

        return url
    except Exception as e:
            log.error("没有获取到目录id：%s" % e)
    
def alarm_handle(num=0):
    tenant_id = get_tenant_id()
    try:
        if num==0:
            sql="select *  From alarm_record where  tenant_id='{}'  order by last_modified_time  desc limit 1".format(tenant_id)
            res = ms.ExecuQuery(sql.encode('utf-8'))
            res=res[0]
            
            data='[{"tenantId":"{}","owner":"","name":"","enabled":null,"creator":"","createTime":"2023-12-14 10:24:53","lastModifier":"","lastModifiedTime":"2023-12-14 10:24:53","id":"{}","monitorId":"{}","monitorName":"{}","alarmConfigId":"{}","alarmType":"{}","sourceId":"{}","sourceName":"{}","sourceType":"{}","executionId":"{}","executionName":"{}","alarmRuleId":"{}","alarmRuleName":"{}","alarmLevel":"{}","noteRule":"inner","noteConfig":null,"userIds":[null],"userNames":["admin"],"departmentIds":[],"departmentNames":[],"alarmStatus":0,"alarmTime":"2023-12-14 10:24:53","handleTime":null,"handleUser":"admin","handleRecord":"test","alarmContent":"执行结束","enable":false}]'
            data=data.format(str(tenant_id),res['id'],res['monitor_id'],res['monitor_name'],res['alarm_config_id'],res['alarm_type'],res['source_id'],res['source_name'],res['source_type'],res['execution_id'],res['execution_name'],res['alarm_rule_id'],res['alarm_rule_name'],res['alarm_level'])
            return data
        else:
            sql="select *  From alarm_record where  tenant_id='{}'  order by last_modified_time  desc limit {}".format(tenant_id,num)
            res = ms.ExecuQuery(sql.encode('utf-8'))
            res=res[0]
            data=[]
            for i in range(num):
                datas='{"tenantId":"{}","owner":"","name":"","enabled":null,"creator":"","createTime":"2023-12-14 10:24:53","lastModifier":"","lastModifiedTime":"2023-12-14 10:24:53","id":"{}","monitorId":"{}","monitorName":"{}","alarmConfigId":"{}","alarmType":"{}","sourceId":"{}","sourceName":"{}","sourceType":"{}","executionId":"{}","executionName":"{}","alarmRuleId":"{}","alarmRuleName":"{}","alarmLevel":"{}","noteRule":"inner","noteConfig":null,"userIds":[null],"userNames":["admin"],"departmentIds":[],"departmentNames":[],"alarmStatus":0,"alarmTime":"2023-12-14 10:24:53","handleTime":null,"handleUser":"admin","handleRecord":"test","alarmContent":"执行结束","enable":false}'
                data.append(datas.format(str(tenant_id),res['id'],res['monitor_id'],res['monitor_name'],res['alarm_config_id'],res['alarm_type'],res['source_id'],res['source_name'],res['source_type'],res['execution_id'],res['execution_name'],res['alarm_rule_id'],res['alarm_rule_name'],res['alarm_level']))
            return data
    except Exception as e:
        log.error("没有获取到目录id：%s" % e)
        
