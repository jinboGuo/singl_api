# coding:utf-8
from basic_info.setting import *

from basic_info.setting import scheduler_id, zmod_id, zmod_exectuion_id

# -------------------------schedulers---------------------------------------------------
# 创建scheduler的接口
create_scheduler_url = "%s/api/schedulers" % host
# 查询scheduler的接口
query_scheduler_url = "%s/api/schedulers/query" % host
select_by_schedulerId_url = "%s/api/schedulers/%s" % (host, scheduler_id)
# 启用scheduler接口

enable_scheduler_url = "%s/api/schedulers/enable" % host
# 停用scheduler接口
disable_scheduler_url = "%s/api/schedulers/disable" % host
# 批量删除schedulers
remove_list_url = "%s/api/schedulers/removeList" % host
# 更新schedulers, scheduler_id给定
update_scheduler_url = "%s/api/schedulers/%s" % (host, scheduler_id)

# -------------------------executions----------------------------------------------------
# 查询execution
query_exectution_url = "%s/api/executions/query" % host
# 批量查询execution
gQuery_execution_url = "%s/api/executions/groupQuery" % host
# 批量删除schedulers
delete_schedulers_url = "%s/api/schedulers/removeList" % host


# ----------------login-------------------------
login_url = "%s/api/auth/login" % host
# ----------------dataset---------------------------
priview_url = "%s/api/datasets/%s/preview?rows=50&tenant=2d7ad891-41c5-4fba-9ff2-03aef3c729e5" % (host,dataset_id)
create_dataset_url = '%s/api/datasets' % host
# ----------------flow-------------------------------
create_flow_url = "%s/api/flows/create" % host
# ----------------schema-----------------------
create_schema_url = '%s/api/schemas' % host

# ----------------collector-------------------------
collector_table_url = '%s/api/woven/collectors/c1/resource/2b5ff16f-ca1b-465e-8a6d-69b8b39f8d61/tables?' % host

# ----------------schema接口---------
create_schema_url = '%s/api/schemas' % host

# --------------status 接口---------
query_component_status_url = '%s/api/component_status' % MY_LOGIN_INFO["HOST"]






# 查询flow接口
query_flows_url = "%s/api/flows/query" % HOST_189
# 创建flow接口
create_flows_url = "%s/api/flows/create" % HOST_189
# 根据名称查询流程接口
query_flowname_url = "%s/api/flows/flowname/%s" % (HOST_189, query_flow_name)
# 根据名称和版本查询历史流程接口
query_flowname_version_url = "%s/api/flows/name/%s/%s" % (HOST_189, query_flow_name, query_flow_version)
# 查询简化版流程
query_flow_all_url = '%s/api/flows/all' % HOST_189
# 更新流程
flow_update_url = '%s/api/flows/%s' % (HOST_189, flow_update_id)
# 查询需要更新的流程的flowid的url added by pengyuan 1120
flow_update_flowid_url = '%s/api/flows/%s/findFlow' % (HOST_189, flow_update_id)
# 根据老的版本查询历史流程
query_flow_history_version_url = '%s/api/flows/history/%s/%s' % (HOST_189, flow_update_id, query_flow_version)
# 根据老的id查询历史流程
query_flow_history_id_url = '%s/api/flows/history/list/%s' % (HOST_189, flow_update_id)
# 根据流程id和计划id查询执行历史
query_flow_flowAscheduler_id_url = '%s/api/flows/%s/schedulers/%s/executions' % (HOST_189, flow_update_id, flow_scheduler_id)
# 根据老的版本查询流程
query_flow_version_url = '%s/api/flows/%s/%s' % (HOST_189, flow_update_id, query_flow_version)


# ------质量分析接口------
# 创建分析模板
create_analysis_model = "%s/api/woven/zmod" % HOST_189
# query_zmod_rule = "%s/api/woven/zmodrules/%s/detailslist" % (MY_LOGIN_INFO["HOST"], zmod_id)
# 删除分析模板
zmod_removeList_url = "%s/api/woven/zmod/removeList" % HOST_189
# 创建rule
create_rule_url = "%s/api/woven/rule" % HOST_189
# 查询规则
rule_query_url = "%s/api/woven/rule/query" % HOST_189
# 查询任务
zdaf_query_url = "%s/api/zdaf/query" % HOST_189

# 查询规则详情页
# query_rule_detail_url = "%s/api/woven/rule/%s" % (HOST_189, sql_rule_id)
# 批量删除rule
rule_removeList_url = "%s/api/woven/rule/removeList" % HOST_189
# 创建zmod flow，分析任务创建scheduler使用
create_zmod_flow_url = "%s/api/woven/zmod/createFlow" % HOST_189
# 查询分析任务
query_zdaf = "%s/api/woven/zdaf/query" % HOST_189
# 查看任务关联模板详情
query_zmod_model_detail_url = "%s/api/woven/zmod/%s" % (HOST_189, zmod_id[0])
# 查看任务执行信息
query_zmod_exectuion_url = "%s/api/woven/zdaf/%s/%s" % (HOST_189, zmod_id[0], zmod_exectuion_id)
# 查看执行结果
query_zmod_execution_dataset = "%s/api/datasets/334ebaae-b7a0-415d-b149-2bf3e16846a1/preview?rows=100" % HOST_189
# 查看统计结果:质量评级，坏数据占比, 统计方式为总计
query_zdaf_result_url = "%s/api/woven/zdaf/stats/qualityRank,badRatio/Total" % HOST_189