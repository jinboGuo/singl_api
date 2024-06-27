# coding:utf-8
import json
import os
import requests
from basic_info.get_auth_token import get_headers
from basic_info.setting import log, ms, host
from util.get_deal_parameter import get_tenant_id,get_dataset,get_resourceid,get_datasource
#from requests_toolbelt.multipart.encoder import MultipartEncoder

def get_qaresourceid(resource_type):
    """
    获取数据源目录、数据集目录、元数据目录、flow目录、采集机、数据采集、数据存储、任务视图、数据资产、数据共享、数据安全、文件编目、数据标准根目录id
    若确少resource_type,可以在setting配置文件里resource_type = ["datasource_dir","dataset_dir","schema_dir","flow_dir",
    "poseidon_collect_dir","poseidon_task_dir"....]添加。
    """
    tenant_id = get_tenant_id()
    try:
        sql = "select id from qa_rule_dir where  tenant_id='{}' and  build_in =0 and path like '{}%'".format(tenant_id,resource_type)
        resource_dir = ms.ExecuQuery(sql.encode('utf-8'))
        resource_id = resource_dir[0]["id"]
        return resource_id
    except Exception as e:
        log.error("没有获取到目录id：%s" % e)
        
def get_selfqaruleid(name,build=1):

    tenant_id = get_tenant_id()
    try:
        if build==1:
            sql = "select id from  qa_rule where tenant_id ='{}' and name='{}'".format(tenant_id,name)
        else:
            sql = "select id from  qa_rule where name='{}'".format(name)
            
        resource_dir = ms.ExecuQuery(sql.encode('utf-8'))
        resource_id = resource_dir[0]["id"]
        return resource_id
    except Exception as e:
        log.error("没有获取到目录id：%s" % e)
        
def get_qajobid(name):

    tenant_id = get_tenant_id()
    try:
        sql = "select id from qa_job where  tenant_id='{}' and  name ='{}'".format(tenant_id,name)
        resource_dir = ms.ExecuQuery(sql.encode('utf-8'))
        resource_id = resource_dir[0]["id"]
        return resource_id
    except Exception as e:
        log.error("没有获取到目录id：%s" % e)

def create_datasetjob_data(data):
    try:
        new_data = {"jobType":"BATCH_DATASET","name":"api_dana_dataset_job","responsibleId":"1228346912479621120","responsibleName":"admin","description":"api_dana_dataset_job描述","schedulerType":"SINGLE","schedulerCycle":"","schedulerDelay":{"delay":1,"unit":"MINUTE"},"epStorageType":"FTP","iterativeType":"FULL","ruleConf":{"datasetRuless":[{"dataset":{"datasetId":get_dataset('dataset_id','dana_api_mysql_dataset'),"datasetName":"dana_api_mysql_dataset"},"rules":[{"ruleId":get_selfqaruleid('字段联合空值校验',0),"ruleName":"字段联合空值校验","version":1,"weights":1,"buildIn":1,"parameters":{"templateType":"T11","operator":"and","value":["change","deal_time"],"alarmRanges":[0,25,50],"yellowValue":0,"orangeValue":25,"redValue":50}},{"ruleId":get_selfqaruleid('api_dana_dataset_testrule'),"ruleName":"api_dana_dataset_testrule","version":1,"weights":1,"buildIn":0,"parameters":{"templateType":"T22","alarmRanges":[0,25,50],"yellowValue":0,"orangeValue":25,"redValue":50,"refValCycle":0,"sqlExpr":"sum( case when #{FIELD0}  in  ('男','女') then 0 else 1 end) ","badSqlExpr":" #{SEL_FIELD0}  in  ('男','女')","mainTable":"","paramPair":{"FIELD0":"deal_type","SEL_FIELD0":"deal_type"}}}],"increment":{"incrType":"FIELD","incrUnit":"DAY","incrField":"deal_time","incrFormat":"yyyyMMddHH","fieldType":"timestamp"}}],"datasetFieldRules":{"fieldRuless":[],"conditionSql":"","dimensions":[],"increment":None}},"notice":{"noticeTypes":[],"noticeLevels":[],"noticeMembers":[],"noticeMembersName":[]},"isExceptionOut":1,"tempEntity":{"noticeMembers":[],"datasets":"dana_api_mysql_dataset"},"exceptionOutConfig":{"normalOut":False,"retainCycle":0.5,"outPath":"/home/europa/ftp_test","datasourceName":"dana_api_ftp_test","datasourceId":get_datasource('datasource_id','dana_api_ftp_test'),"epDatasetResourceName":"数据集","epDatasetResourceId":get_resourceid('dataset_dir'),"schemaResourceName":"元数据","schemaResourceId":get_resourceid('schema_dir')},"resourceId":get_resourceid('qa_job_dir')}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)

def create_datacoljob_data(data):
    try:
        new_data = {"jobType":"BATCH_COLUMN","name":"api_dana_datacol_job","responsibleId":"","responsibleName":"","description":"","schedulerType":"SINGLE","schedulerCycle":"","schedulerDelay":{"delay":1,"unit":"MINUTE"},"epStorageType":"FTP","iterativeType":"FULL","ruleConf":{"datasetRuless":[],"datasetFieldRules":{"fieldRuless":[{"field":{"fieldName":"deal_type","fieldType":"string"},"rules":[{"ruleId":get_selfqaruleid('api_dana_col_testrule'),"ruleName":"api_dana_col_testrule","version":1,"weights":1,"buildIn":0,"parameters":{"templateType":"T22","alarmRanges":[0,25,50],"yellowValue":0,"orangeValue":25,"redValue":50,"refValCycle":0,"sqlExpr":"coalesce(sum(case when !(( #{FIELD0}  regexp '北+' ))  then 1 else 0 end),0)","badSqlExpr":"!( #{SEL_FIELD0}   regexp '北+' )","mainTable":"","paramPair":{"FIELD0":"deal_type","SEL_FIELD0":"deal_type"}}}]},{"field":{"fieldName":"change","fieldType":"float"},"rules":[{"ruleId":get_selfqaruleid('数据空值规则','0'),"ruleName":"数据空值规则","version":1,"weights":1,"buildIn":1,"parameters":{"templateType":"T2","alarmRanges":[0,25,50],"yellowValue":0,"orangeValue":25,"redValue":50}}]}],"conditionSql":"pay_id is not null","dimensions":[],"increment":{"incrType":"PARTITION","incrUnit":"DAY","incrField":"","incrFormat":""},"dataset":{"datasetId":get_dataset('dataset_id', 'dana_api_mysql_dataset'),"datasetName":"dana_api_mysql_dataset"}}},"notice":{"noticeTypes":[],"noticeLevels":[],"noticeMembers":[],"noticeMembersName":[]},"isExceptionOut":1,"tempEntity":{"noticeMembers":[],"datasets":"dana_api_mysql_dataset"},"exceptionOutConfig":{"normalOut":False,"retainCycle":0.5,"outPath":"/home/europa/ftp_test","datasourceName":"dana_api_ftp_test","datasourceId":get_datasource('datasource_id', 'dana_api_ftp_test'),"epDatasetResourceName":"数据集","epDatasetResourceId":get_resourceid('dataset_dir'),"schemaResourceName":"元数据","schemaResourceId":get_resourceid('schema_dir')},"resourceId":get_resourceid('qa_job_dir')}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" %e)
        
def create_rule_import(data):
    ruledir=os.path.join(os.path.abspath('.'),'attachment\\'+data).replace("\\","/")
    fs = {"file": open(ruledir, 'rb')}    
    
    head={'Accept':'*/*',
          'Cookie':'AMBARISESSIONID=node01kgsdeg3ykyeu5nwg7pyy8bmd90.node0'
          }
    head['Authorization']=get_headers()['Authorization']
    return head,fs

def get_success_exehistoryjobid(data):
    if data['成功']=='数据集':
        tn={"fieldGroup":{"type":"FieldGroup","group":True,"andOr":"AND","fields":[{"type":"Field","group":False,"andOr":"AND","name":"resourceId","oper":"EQUAL","value":[str(get_resourceid('qa_job_dir'))],"label":""},{"type":"Field","group":False,"andOr":"AND","name":"lastExecutedStatus","oper":"IN","value":["SUCCEEDED"],"label":""},{"type":"Field","group":False,"andOr":"AND","name":"jobType","oper":"IN","value":["BATCH_DATASET"],"label":""}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":20,"pageable":True}}
    else:
        tn={"fieldGroup":{"type":"FieldGroup","group":True,"andOr":"AND","fields":[{"type":"Field","group":False,"andOr":"AND","name":"resourceId","oper":"EQUAL","value":[str(get_resourceid('qa_job_dir'))],"label":""},{"type":"Field","group":False,"andOr":"AND","name":"lastExecutedStatus","oper":"IN","value":["SUCCEEDED"],"label":""},{"type":"Field","group":False,"andOr":"AND","name":"jobType","oper":"IN","value":["BATCH_COLUMN"],"label":""}]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":20,"pageable":True}}

    try:
        res=requests.post(url=host+'/api/dw/qa/job/page',headers=get_headers(),data=json.dumps(tn))
        jobid=res.json()['content']['list'][0]['id']
        
        tm={"fieldGroup":{"type":"FieldGroup","group":True,"andOr":"AND","fields":[{"type":"Field","group":False,"andOr":"AND","name":"jobId","oper":"EQUAL","value":[str(jobid)],"label":""}]},"ordSort":[{"name":"bizdate","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":20,"pageable":True}}
        res0=requests.post(url=host+'/api/dw/qa/jobexecution/page',headers=get_headers(),data=json.dumps(tm))
        exeid=res0.json()['content']['list'][0]['id']
        
        sql="select id from qa_job_execution where job_id='"+jobid+"' and alarm_level is not null limit 1"
        ressql = ms.ExecuQuery(sql.encode('utf-8'))
        ressql_id = ressql[0]["id"]
         
        rules={"fieldGroup":{"type":"FieldGroup","group":True,"andOr":"AND","fields":[]},"ordSort":[{"name":"createTime","order":"DESC"}],"pageable":{"pageNum":1,"pageSize":20,"pageable":True}}
        rule_res=requests.post(url=host+'/api/dw/qa/analyze/detail/'+str(ressql_id),headers=get_headers(),data=json.dumps(rules))
        rule_res=rule_res.json()['content']['list'][0]
  
        if  data['exe'] =='是':
            return exeid
        elif data['exe'] =='url':
            return ressql_id
        elif data['exe'] =='all':
            return rule_res
        else:
            return jobid
    except Exception as e:
        print(e)
    
    
    
