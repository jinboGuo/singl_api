# coding:utf-8
import smtplib
import os.path as pth
import time, os
from email import encoders
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import sys
from smtplib import SMTP_SSL
from openpyxl import load_workbook
from api_test_cases.get_execution_output_json import abs_dir, GetCheckoutDataSet
from new_api_cases.execute_cases import ab_dir
from basic_info.setting import receivers_list, host

current_path = os.path.abspath(os.path.dirname(__file__))
root_path = os.path.split(current_path)[0]
sys.path.append(root_path)


def send_email(content, title, from_name, from_address, to_address, serverport, serverip, username, password):
    # 邮件对象:
    msg = MIMEMultipart()
    msg['Subject'] = Header(title, 'utf-8')
    # 这里的to_address只用于显示，必须是一个string
    msg['To'] = ','.join(to_address)
    msg['From'] = from_name
    # 邮件正文是MIMEText:
    msg.attach(MIMEText(content, 'html', 'utf-8'))
    # 添加附件就是加上一个MIMEBase，从本地读取一个文件:
    with open('E:\Reports\Test_report.html', 'rb') as f:
        # 设置附件的MIME和文件名，这里是png类型:
        mime = MIMEBase('report', 'html', filename='Test_Report.html')
        # mime = MIMEBase('report', 'html', filename=filename)
        # 加上必要的头信息:
        mime.add_header('Content-Disposition', 'attachment', filename='Test_Report.html')
        mime.add_header('Content-ID', '<0>')
        mime.add_header('X-Attachment-Id', '0')
        # 把附件的内容读进来:
        mime.set_payload(f.read())
        # 用Base64编码:
        encoders.encode_base64(mime)
        # 添加到MIMEMultipart:
        msg.attach(mime)

    try:
        s = smtplib.SMTP_SSL(serverip, serverport)
        s.login(username, password)
        # 这里的to_address是真正需要发送的到的mail邮箱地址需要的是一个list
        s.sendmail(from_address, to_address, msg.as_string())
        print('%s----发送邮件成功' % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    except Exception as err:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print(err)


# 发送邮件
def main3(host,receivers):
    # 163邮箱smtp服务器
    host_server = "smtp.163.com"
    # 发件人
    sender_163 = "ruifan_test@163.com"
    # pwd为发件人163邮箱的授权码
    pwd = "ruifantest2018"
    # 发件人的邮箱
    sender_163_mail = "ruifan_test@163.com"
    # 收件人邮箱
    receivers = receivers_list  # 定时任务使用
    #receivers = receivers_test  # 调试使用
    msg = MIMEMultipart()

    # 邮件的正文内容----API执行结果
    # 统计api执行结果，加入到邮件正文中，失败的用例name：失败的原因
    api_cases_table = load_workbook(ab_dir('api_cases.xlsx'))
    cases_sheet = api_cases_table.get_sheet_by_name('199')
    sheet_rows = cases_sheet.max_row
    cases_num = sheet_rows - 1
    pass_cases = 0
    failed_cases = 0
    failed_cases_detail = {}
    failed_cases_name = []
    for row in range(2, sheet_rows+1):
        if cases_sheet.cell(row=row, column=14).value == 'pass':
            pass_cases += 1
        else:
            failed_cases += 1
            case_name = cases_sheet.cell(row=row, column=2).value
            failed_cases_detail[case_name] = cases_sheet.cell(row=row, column=15).value   # 将用例name和失败原因列出
            failed_cases_name.append(cases_sheet.cell(row=row, column=2).value)
    # 邮件的正文内容
    if failed_cases != 0:
        mail_content = """各位好:
        本次用例执行环境：%s
        API用例共执行：%d 条
        执行成功: %d 条
        执行失败: %d 条
        用例执行详情请查看附件《api_cases.xlsx》
        失败用例名称如下：
        %s
        """ % (host, cases_num, pass_cases, failed_cases, failed_cases_name)
    else:
        mail_content = """各位好:
        本次用例执行环境：%s
        API用例共执行：%d 条
        执行成功: %d 条
        用例执行详情请查看附件《api_cases.xlsx》""" % (host, cases_num, pass_cases)
    # print(mail_content)
    # 邮件标题
    mail_title = time.strftime("%Y-%m-%d", time.localtime()) + ' BayMax系统API用例自动化执行日报'

    # 添加邮件正文，格式 MIMEText:
    msg.attach(MIMEText(mail_content, "plain", 'utf-8'))

    # 添加api用例 excel表格
    # 添加附件，就是加上一个MIMEBase，从本地读取一个文件:
    # 添加API用例集执行报告
    apicases_filepath = ab_dir('api_cases.xlsx')
    with open(apicases_filepath, 'rb') as a:
        # 设置附件的MIME和文件名:
        mime = MIMEBase('report', 'xlsx', filename='api_cases.xlsx')
        # 加上必要的头信息:
        mime.add_header('Content-Disposition', 'attachment', filename='api_casex.xlsx')
        mime.add_header('Content-ID', '<0>')
        mime.add_header('X-Attachment-Id', '0')
        # 把附件的内容读进来:
        mime.set_payload(a.read())
        # # 用Base64编码:
        encoders.encode_base64(mime)
        # 添加到MIMEMultipart:
        msg.attach(mime)

    # ssl登录
    smtp = SMTP_SSL(host_server)
    # set_debuglevel()是用来调试的。参数值为1表示开启调试模式，参数值为0关闭调试模式
    smtp.set_debuglevel(0)
    smtp.ehlo(host_server)
    smtp.login(sender_163_mail, pwd)
    msg["Subject"] = Header(mail_title, 'utf-8')
    msg["From"] = sender_163
    msg["To"] = Header("gjb,lq,fq", 'utf-8')  # 接收者的别名
    smtp.sendmail(sender_163_mail, receivers, msg.as_string())
    print('%s----发送邮件成功' % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    smtp.quit()


def mail_for_flow(host,receivers):
    # 163邮箱smtp服务器
    host_server = "smtp.163.com"
    # 发件人
    sender_163 = "ruifan_test@163.com"
    # pwd为发件人163邮箱的授权码
    pwd = "ruifantest2018"
    # 发件人的邮箱
    sender_163_mail = "ruifan_test@163.com"
    # 收件人邮箱
    #receivers = receivers_test  # 调试使用
    receivers = receivers_list  # 定时任务使用
    msg = MIMEMultipart()
    # 邮件的正文内容----flow执行结果
    f = load_workbook(abs_dir("flow_dataset_info.xlsx"))
    f_sheet = f.get_sheet_by_name("flow_info")
    rows = f_sheet.max_row
    succeed_flow = []
    failed_flow = []
    flow_id_list = GetCheckoutDataSet().get_flow_id()
    total = len(list(set(flow_id_list)))
    # print('flow总数total:', total)
    detail_msg = ''' '''
    for row in range(2, rows+1):
        if f_sheet.cell(row=row, column=9).value == "fail":
            detail_msg += f_sheet.cell(row=row, column=10).value + '\n'
            if f_sheet.cell(row=row, column=3).value:
                failed_flow.append(f_sheet.cell(row=row, column=3).value)
            else:
                for i in range(row, 2, -1):
                    if f_sheet.cell(row=i-1, column=3).value:
                        failed_flow.append(f_sheet.cell(row=i-1, column=3).value)
                        break
        elif f_sheet.cell(row=row, column=9).value == "pass":
            if f_sheet.cell(row=row, column=3).value:
                succeed_flow.append(f_sheet.cell(row=row, column=3).value)
                # print(succeed_flow)
            else:
                for i in range(row, 2, -1):
                    if f_sheet.cell(row=i-1, column=3).value:
                        succeed_flow.append(f_sheet.cell(row=i-1, column=3).value)
                        break
    failed_flow_s = list(set(failed_flow))
    succeed_flow_s = list(set(succeed_flow))
    for disct_id in (disct_ids for disct_ids in failed_flow_s if disct_ids in succeed_flow_s):
        succeed_flow_s.remove(disct_id)

    # 邮件的正文内容
    filename = time.strftime("%Y%m%d%H", time.localtime()) + '_report.html'
    if len(failed_flow_s) != 0:
        mail_content = """各位好:
        flow用例执行信息如下:
        用例执行环境：%s
        本次Flow用例共执行：%d 个
        执行成功: %d 个
        执行失败: %d 个
        失败的flow名称为:
        %s
        失败原因请查看附件《flow_info.xlsx》中的log信息""" % (host, total, len(succeed_flow_s), len(failed_flow_s), failed_flow_s)
    else:
        mail_content = """各位好:
        flow用例执行信息如下:
        用例执行环境：%s
        本次Flow用例共执行: %d 个
        成功: %d 个
        任务详情请查看附件《flow_info.xlsx》中的log""" % (host, total, len(succeed_flow_s))
    # print(mail_content)
    # 邮件标题
    mail_title = time.strftime("%Y-%m-%d", time.localtime()) + ' Flow用例自动化执行日报'

    # 添加邮件正文，格式 MIMEText:
    msg.attach(MIMEText(mail_content, "plain", 'utf-8'))

    # 添加附件，就是加上一个MIMEBase，从本地读取一个文件:
    flow_filepath = abs_dir('flow_dataset_info.xlsx')
    with open(flow_filepath, 'rb') as ff:
        # 设置附件的MIME和文件名，这里是html类型:
        flow_mime = MIMEBase('report', 'xlsx', filename='flow_info.xlsx')
        # 加上必要的头信息:
        flow_mime.add_header('Content-Disposition', 'attachment', filename='flow_info.xlsx')
        flow_mime.add_header('Content-ID', '<0>')
        flow_mime.add_header('X-Attachment-Id', '0')
        # 把附件的内容读进来:
        flow_mime.set_payload(ff.read())
        # # 用Base64编码:
        encoders.encode_base64(flow_mime)
        # 添加到MIMEMultipart:
        msg.attach(flow_mime)

    # ssl登录
    smtp = SMTP_SSL(host_server)
    # set_debuglevel()是用来调试的。参数值为1表示开启调试模式，参数值为0关闭调试模式
    smtp.set_debuglevel(0)
    smtp.ehlo(host_server)
    smtp.login(sender_163_mail, pwd)
    msg["Subject"] = Header(mail_title, 'utf-8')
    msg["From"] = sender_163
    msg["To"] = Header("gjb,lq,fq", 'utf-8')  # 接收者的别名
    smtp.sendmail(sender_163_mail, receivers, msg.as_string())
    print('%s----发送邮件成功' % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    smtp.quit()

#host = '192.168.1.82:8515'
#mail_for_flow(host, receivers_test)
#main3(host, receivers_list)

def dsp_main3(host,receivers):
    # 163邮箱smtp服务器
    host_server = "smtp.163.com"
    # 发件人
    sender_163 = "ruifan_test@163.com"
    # pwd为发件人163邮箱的授权码
    pwd = "ruifantest2018"
    # 发件人的邮箱
    sender_163_mail = "ruifan_test@163.com"
    # 收件人邮箱
    receivers = receivers_list  # 定时任务使用
    #receivers = receivers_test  # 调试使用
    msg = MIMEMultipart()

    # 邮件的正文内容----API执行结果
    # 统计api执行结果，加入到邮件正文中，失败的用例name：失败的原因
    api_cases_table = load_workbook(ab_dir('api_cases.xlsx'))
    cases_sheet = api_cases_table.get_sheet_by_name('dsp')
    sheet_rows = cases_sheet.max_row
    cases_num = sheet_rows - 1
    pass_cases = 0
    failed_cases = 0
    failed_cases_detail = {}
    failed_cases_name = []
    for row in range(2, sheet_rows+1):
        if cases_sheet.cell(row=row, column=14).value == 'pass':
            pass_cases += 1
        else:
            failed_cases += 1
            case_name = cases_sheet.cell(row=row, column=2).value
            failed_cases_detail[case_name] = cases_sheet.cell(row=row, column=15).value   # 将用例name和失败原因列出
            failed_cases_name.append(cases_sheet.cell(row=row, column=2).value)
    # 邮件的正文内容
    if failed_cases != 0:
        mail_content = """各位好:
        本次用例执行环境：%s
        API用例共执行：%d 条
        执行成功: %d 条
        执行失败: %d 条
        用例执行详情请查看附件《api_cases.xlsx》
        失败用例名称如下：
        %s
        """ % (host, cases_num, pass_cases, failed_cases, failed_cases_name)
    else:
        mail_content = """各位好:
        本次用例执行环境：%s
        API用例共执行：%d 条
        执行成功: %d 条
        用例执行详情请查看附件《api_cases.xlsx》""" % (host, cases_num, pass_cases)
    # print(mail_content)
    # 邮件标题
    mail_title = time.strftime("%Y-%m-%d", time.localtime()) + ' DSP系统API用例自动化执行日报'

    # 添加邮件正文，格式 MIMEText:
    msg.attach(MIMEText(mail_content, "plain", 'utf-8'))

    # 添加api用例 excel表格
    # 添加附件，就是加上一个MIMEBase，从本地读取一个文件:
    # 添加API用例集执行报告
    apicases_filepath = ab_dir('api_cases.xlsx')
    with open(apicases_filepath, 'rb') as a:
        # 设置附件的MIME和文件名:
        mime = MIMEBase('report', 'xlsx', filename='api_cases.xlsx')
        # 加上必要的头信息:
        mime.add_header('Content-Disposition', 'attachment', filename='api_casex.xlsx')
        mime.add_header('Content-ID', '<0>')
        mime.add_header('X-Attachment-Id', '0')
        # 把附件的内容读进来:
        mime.set_payload(a.read())
        # # 用Base64编码:
        encoders.encode_base64(mime)
        # 添加到MIMEMultipart:
        msg.attach(mime)

    # ssl登录
    smtp = SMTP_SSL(host_server)
    # set_debuglevel()是用来调试的。参数值为1表示开启调试模式，参数值为0关闭调试模式
    smtp.set_debuglevel(0)
    smtp.ehlo(host_server)
    smtp.login(sender_163_mail, pwd)
    msg["Subject"] = Header(mail_title, 'utf-8')
    msg["From"] = sender_163
    msg["To"] = Header("gjb,lq,fq", 'utf-8')  # 接收者的别名
    smtp.sendmail(sender_163_mail, receivers, msg.as_string())
    print('%s----发送邮件成功' % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    smtp.quit()

def dw_main3(host,receivers):
    # 163邮箱smtp服务器
    host_server = "smtp.163.com"
    # 发件人
    sender_163 = "ruifan_test@163.com"
    # pwd为发件人163邮箱的授权码
    pwd = "ruifantest2018"
    # 发件人的邮箱
    sender_163_mail = "ruifan_test@163.com"
    # 收件人邮箱
    receivers = receivers_list  # 定时任务使用
    #receivers = receivers_test  # 调试使用
    msg = MIMEMultipart()

    # 邮件的正文内容----API执行结果
    # 统计api执行结果，加入到邮件正文中，失败的用例name：失败的原因
    api_cases_table = load_workbook(ab_dir('api_cases.xlsx'))
    cases_sheet = api_cases_table.get_sheet_by_name('dw')
    sheet_rows = cases_sheet.max_row
    cases_num = sheet_rows - 1
    pass_cases = 0
    failed_cases = 0
    failed_cases_detail = {}
    failed_cases_name = []
    for row in range(2, sheet_rows+1):
        if cases_sheet.cell(row=row, column=14).value == 'pass':
            pass_cases += 1
        else:
            failed_cases += 1
            case_name = cases_sheet.cell(row=row, column=2).value
            failed_cases_detail[case_name] = cases_sheet.cell(row=row, column=15).value   # 将用例name和失败原因列出
            failed_cases_name.append(cases_sheet.cell(row=row, column=2).value)
    # 邮件的正文内容
    if failed_cases != 0:
        mail_content = """各位好:
        本次用例执行环境：%s
        API用例共执行：%d 条
        执行成功: %d 条
        执行失败: %d 条
        用例执行详情请查看附件《api_cases.xlsx》
        失败用例名称如下：
        %s
        """ % (host, cases_num, pass_cases, failed_cases, failed_cases_name)
    else:
        mail_content = """各位好:
        本次用例执行环境：%s
        API用例共执行：%d 条
        执行成功: %d 条
        用例执行详情请查看附件《api_cases.xlsx》""" % (host, cases_num, pass_cases)
    # print(mail_content)
    # 邮件标题
    mail_title = time.strftime("%Y-%m-%d", time.localtime()) + ' DSP系统API用例自动化执行日报'

    # 添加邮件正文，格式 MIMEText:
    msg.attach(MIMEText(mail_content, "plain", 'utf-8'))

    # 添加api用例 excel表格
    # 添加附件，就是加上一个MIMEBase，从本地读取一个文件:
    # 添加API用例集执行报告
    apicases_filepath = ab_dir('api_cases.xlsx')
    with open(apicases_filepath, 'rb') as a:
        # 设置附件的MIME和文件名:
        mime = MIMEBase('report', 'xlsx', filename='api_cases.xlsx')
        # 加上必要的头信息:
        mime.add_header('Content-Disposition', 'attachment', filename='api_casex.xlsx')
        mime.add_header('Content-ID', '<0>')
        mime.add_header('X-Attachment-Id', '0')
        # 把附件的内容读进来:
        mime.set_payload(a.read())
        # # 用Base64编码:
        encoders.encode_base64(mime)
        # 添加到MIMEMultipart:
        msg.attach(mime)

    # ssl登录
    smtp = SMTP_SSL(host_server)
    # set_debuglevel()是用来调试的。参数值为1表示开启调试模式，参数值为0关闭调试模式
    smtp.set_debuglevel(0)
    smtp.ehlo(host_server)
    smtp.login(sender_163_mail, pwd)
    msg["Subject"] = Header(mail_title, 'utf-8')
    msg["From"] = sender_163
    msg["To"] = Header("gjb,lq,fq", 'utf-8')  # 接收者的别名
    smtp.sendmail(sender_163_mail, receivers, msg.as_string())
    print('%s----发送邮件成功' % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    smtp.quit()
