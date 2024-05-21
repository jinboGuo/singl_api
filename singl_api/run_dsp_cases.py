# coding:utf-8
from util.api_result_report import get_result_report
from util.send_mail import baymax_main
from new_api_cases.execute_dsp_cases import deal_request_method, cases_dir
from new_api_cases.execute_dsp_cases import CheckResult
from basic_info.setting import dsp_host, dsp_sheet, receivers_test, receivers_list, log, begin_times, pattern

if __name__ == '__main__':
    log.info("--------开始执行用例-------")
    log.info("开始时间：%s" % begin_times)

    """执行API用例并对比结果"""
    deal_request_method()
    CheckResult().deal_result()
    total_time = get_result_report(dsp_host,pattern[1])

    """发送邮件"""
    baymax_main(cases_dir,dsp_host, receivers_test, dsp_sheet, begin_times,total_time)