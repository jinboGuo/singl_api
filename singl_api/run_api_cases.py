# coding:utf-8
from util.send_mail import baymax_main3
import datetime
from new_api_cases.execute_cases import deal_request_method
from new_api_cases.execute_cases import CheckResult
from basic_info.setting import host, baymax_master
from basic_info.setting import receivers_list
from util.logs import Logger

log = Logger().get_log()

log.info("--------开始执行用例-------")
start_time = datetime.datetime.now()
log.info("开始时间：%s" % start_time)
log.info("--------开始执行api case-------")

# 执行API用例并对比结果
deal_request_method()
CheckResult().deal_result()
stop_time = datetime.datetime.now()
log.info("结束时间：%s" % stop_time)
log.info("总耗时：%s" % (stop_time-start_time))
# 发送邮件
baymax_main3(host, receivers_list, baymax_master)













