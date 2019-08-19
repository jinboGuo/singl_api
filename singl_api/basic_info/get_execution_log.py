from util.Open_DB import MYSQL
from basic_info.get_auth_token import get_headers
from basic_info.setting import MySQL_CONFIG
from util.format_res import dict_res
import requests


class GetLog(object):
    def __init__(self, execution_id, host):
        self.ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
        self.execution_id = execution_id
        self.host = host

    def get_job_id(self):
        sql = "select flow_id ,job_id from merce_flow_execution where id = '%s' " % self.execution_id
        try:
            flow_job_id = self.ms.ExecuQuery(sql)
            flow_id = flow_job_id[0]["flow_id"]
            job_id = flow_job_id[0]["job_id"]
            # print(flow_id, job_id)
            return flow_id, job_id
        except Exception:
            return

    def get_log_url(self):
        """
        根据flow id和job id，查询任务日志
        1. 若任务生成application ID，则进入yarn直接查看日志
        2. 若任务没有生成application ID,则进入系统页面查看是否有日志生成
        """
        flow_id = self.get_job_id()[0]
        job_id = self.get_job_id()[1]
        detail_url = "%s/api/executions/query" % self.host
        data = {"fieldList": [{"fieldName":"flowId","fieldValue":flow_id,"comparatorOperator":"EQUAL","logicalOperator":"AND"},{"fieldName":"jobId","fieldValue":job_id,"comparatorOperator":"EQUAL"}],"sortObject":{"field":"lastModifiedTime","orderDirection":"DESC"},"offset":0,"limit":8}
        response = requests.post(url=detail_url,headers=get_headers(self.host),json=data)
        # print(response.text)
        try:
            content_info = dict_res(response.text)["content"][0]
            # print(content_info)
            content_info_list = list(content_info.keys())
            # 优先判断返回的detail信息中是否包含APPId,存在就到yarn上查看对应的日志
            if 'appId' in content_info_list:
                appId = content_info["appId"]
                yarn_url = "http://info2:8088/cluster/app/%s" % appId
                # print(yarn_url)
                return yarn_url
            # 如果没有生成application id，就进入日志页面直接查看
            else:
                print('没有生成applicationId,需要进入详情页查看日志信息')
                loginfo_url = "%s/#/design/executingDetail/%s/exection/%s/logInfo" % (self.host, flow_id, job_id)
                # print(loginfo_url)
                return loginfo_url
        except Exception:
            return




#
# g = GetLog('6bac071d-5cc7-42bc-b64e-12b368ff7099', 'http://192.168.1.76:8515').get_log_url()
# print(g)