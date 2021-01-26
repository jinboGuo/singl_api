# coding:utf-8
from util.send_mail import dw_main3
import datetime
from new_api_cases.execute_compass_cases import deal_request_method
from new_api_cases.execute_compass_cases import CheckResult

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
#dw_main3(dw_host, receivers_list)













