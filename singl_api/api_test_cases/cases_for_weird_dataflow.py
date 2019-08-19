# coding:utf-8
import unittest
import requests
import time
from util.Open_DB import MYSQL
from basic_info.setting import MySQL_CONFIG, HOST_189
from util.format_res import dict_res
from basic_info.ready_dataflow_data import get_dataflow_data
from basic_info.url_info import create_scheduler_url
from basic_info.get_auth_token import get_headers


class ExecuteWeirdDataflow(unittest.TestCase):

    # def setUp(self):
    #     self.ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
    #     self.expected_result = ['[{"name":"james","id":"6","age":"50"}]', '[{"name":"xiaowang","id":"3","age":"30"}]', '[{"name":"xiaoming","id":"1","age":"18"}]', '[{"name":"tyest","id":"4","age":"12"}]', '[{"name":"xiaohong","id":"2","age":"20"}]', '[{"name":"空","id":"5","age":"空"}]']
    #
    # def tearDown(self):
    #     pass
    ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
    expected_result = ['[{"name":"james","id":"6","age":"50"}]', '[{"name":"xiaowang","id":"3","age":"30"}]', '[{"name":"xiaoming","id":"1","age":"18"}]', '[{"name":"tyest","id":"4","age":"12"}]', '[{"name":"xiaohong","id":"2","age":"20"}]', '[{"name":"空","id":"5","age":"空"}]']
    #
    def test_create_scheduler(self):
        print("开始执行test_create_scheduler(self)")
        data = get_dataflow_data('tc_auto_df_sink_hdfs_path使用$进行分区、使用sliceTimeColumn1545633382888')
        res = requests.post(url=create_scheduler_url, headers=get_headers(HOST_189), json=data)
        print(res.url)
        print(res.status_code)
        self.assertEqual(201, res.status_code, '创建scheduler失败，失败原因%s' % res.text)
        self.assertNotEqual(res.json().get('id', 'scheduler创建可能失败了'), 'scheduler创建可能失败了')
        # scheduler_id = res.json()['id']
        # print('---------scheduler_id-------', scheduler_id)
        # print(res.json()["id"])
        return res.json()['id']

    def test_get_execution_info(self):
        print("开始执行get_execution_info(self)")
        scheduler_id = self.test_create_scheduler()
        e_status_format = {'type': 'READY'}
        while e_status_format["type"] in ("READY", "RUNNING"):
            time.sleep(5)
            execution_sql = 'select id, status_type, flow_id, flow_scheduler_id from merce_flow_execution where flow_scheduler_id = "%s"' % scheduler_id
            time.sleep(20)
            print(execution_sql)
            select_result = self.ms.ExecuQuery(execution_sql)
            print(select_result)
            e_status_format["type"] = select_result[0]["status_type"]
            # e_status_format = dict_res(e_status)
        if e_status_format['type'] == 'SUCCEEDED':
            self.assertEqual('SUCCEEDED', e_status_format['type'])
            print('select_result: \n', select_result)
            return select_result
        else:
            return None
    def test_get_dataset_id(self):
        """获取execution的id和状态, 最终返回execution执行成功后的dataset id """

        e_info = self.test_get_execution_info()
        if e_info:
            data_json_sql = 'select b.dataset_json from merce_flow_execution as a  LEFT JOIN merce_flow_execution_output as b on a.id = b.execution_id where a.id ="%s"' % e_info[0]["id"]
            data_json = self.ms.ExecuQuery(data_json_sql)
            sink_dataset_list = []
            for n in range(len(data_json)):
                sink_dataset = data_json[n]["dataset_json"]  # 返回结果为元祖
                sink_dataset_id = dict_res(sink_dataset)["id"]  # 取出json串中的dataset id
                sink_dataset_list.append(sink_dataset_id)
            print('----------sink_dataset_list----------', sink_dataset_list)
            return sink_dataset_list
        else:
            return None
    def test_test_check_result(self):
        ''' 返回多dataset且ID会变，对该flow的校验 '''
        sink_dataset_list = self.test_get_dataset_id()
        if sink_dataset_list:
            L = []
            for dataset_id in sink_dataset_list:
                priview_url = "%s/api/datasets/%s/preview?rows=5000&tenant=2d7ad891-41c5-4fba-9ff2-03aef3c729e5" % (HOST_189, dataset_id)
                result = requests.get(url=priview_url, headers=get_headers(HOST_189))
                L.append(result.text)
            different_result = [i for i in self.expected_result if i not in L]
            self.assertEqual(len(self.expected_result), len(L))
            self.assertEqual(different_result, [])
        else:
            return None

# g = ExecuteWeirdDataflow()
# g.test_test_check_result()





