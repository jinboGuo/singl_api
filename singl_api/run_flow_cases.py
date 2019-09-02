# coding:utf-8
from util.send_mail import mail_for_flow
from api_test_cases.get_execution_output_json import GetCheckoutDataSet
from basic_info.setting import host
import datetime
from basic_info.setting import receivers_list,receivers_test


# 添加用例集的API用例。暂停
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
print('------开始执行用例-------')
start_time = datetime.datetime.now()
print('用例执行环境：', host)
print('开始时间', start_time)
print('------开始执行flow用例------')
# 执行flow用例
obj = GetCheckoutDataSet()
obj.get_json()
print('------开始执行api case------')
# 执行API用例
# deal_request_method()
# # 对比API用例结果
# CheckResult().deal_result()
# 发送邮件
print('用例执行完毕，开始发送邮件')
stop_time = datetime.datetime.now()
print('结束时间：', stop_time)
print('耗时:', stop_time-start_time)
# 发送邮件
mail_for_flow(host)












