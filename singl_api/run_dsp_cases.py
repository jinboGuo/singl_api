# coding:utf-8
from util.send_mail import dsp_main3
import datetime
from new_api_cases.execute_dsp_cases import deal_request_method
from new_api_cases.execute_dsp_cases import CheckResult
from basic_info.setting import dsp_host
from basic_info.setting import receivers_list

# from newSuite import NewSuite

# 添加用例集的API用例。暂停执行该用例集
# testcase = unittest.TestSuite()
# discover = unittest.defaultTestLoader.discover(start_dir='./api_test_cases', pattern='cases_for_*.py')
# for test_suite in discover:
#     for test_case in test_suite:
#         # print(test_case)
#         testcase.addTest(test_case)
# filename = time.strftime("%Y%m%d%H", time.localtime()) + '_report.html'
# # report_path = 'E:\Reports\\' + filename
# # report_path = '/root/gbj/Reports/' + filename  # 192.168.1.87环境Jenkins使用
# report_path = './Reports/' + filename  # 192.168.1.87环境Jenkins使用
# fp = open(report_path, 'wb')
# runner = HTMLTestRunner.HTMLTestRunner(stream=fp, title='API自动化测试报告', description='覆盖dataset,schema,schedulers,execution等测试场景')
# print('开始执行用例集用例')
# runner.run(testcase)
# fp.close()
print('--------开始执行用例-------')
start_time = datetime.datetime.now()
print('开始时间：', start_time)
print('--------开始执行api case-------')

# 执行API用例并对比结果
deal_request_method()
CheckResult().deal_result()
stop_time = datetime.datetime.now()
print('结束时间：', stop_time)
print('耗时:', stop_time-start_time)
# 发送邮件
dsp_main3(dsp_host, receivers_list)













