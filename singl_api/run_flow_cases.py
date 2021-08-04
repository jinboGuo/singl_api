# coding:utf-8
from util.send_mail import mail_for_flow
from api_test_cases.get_execution_output_json import GetCheckoutDataSet
from basic_info.setting import host
import datetime
from basic_info.setting import receivers_list,receivers_test


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
# mail_for_flow(host,receivers_list)











