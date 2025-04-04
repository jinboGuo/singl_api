import os
import socket
import time
from kafka import KafkaProducer
from basic_info.setting import log, ms_conn, MySQL_CONFIG1
import base64
import xmltodict
from pykafka import KafkaClient
import json
import requests
from faker import Faker
from util.timestamp_13 import data_now, hour_stamp, hour_slice, timestamp_now, timestamp_utc, day_now

fake = Faker("zh_CN")  # 初始化，可生成中文数据


def xmlToJson(xml):
    """
    function:xml转换为json
    """
    try:
        converteJson = xmltodict.parse(xml,encoding='utf-8')
        jsonStr = json.dumps(converteJson,indent=4)
        return jsonStr
    except Exception as e:
        print("异常信息：%s", e)


def jsonToXml(js):
    """
    function:json转换为xml
    """
    convertXml = ''
    jsDict =json.loads(js)
    try:
        convertXml = xmltodict.unparse(jsDict,encoding='utf-8')
    except:
        convertXml = xmltodict.unparse({'msg':jsDict},encoding='utf-8')
    finally:
        return convertXml


def test_secret_key(url, body, header):
    """
    function:返回密钥
    header = {'Content-Type': 'application/json', "Accept": "application/json"}
    body = {"accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1"}
    url = 'http://192.168.1.82:8008/api/dsp/dataapi/data/secertkey'
    """
    result = requests.get(url=url, params=body, headers=header)
    log.info("返回密钥：%s: " % result.text)
    return result.text


def test_pull(url, body, header):
    """
    function:查询数据记录接口
    header = {'hosts': '192.168.2.142', 'Content-Type': 'application/json', "Accept": "application/json"}
    body = {"dataServiceId": "722844377071747072", "accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1", "encrypted":"false","offset": 0, "size": 100, "timestamp": 1}
    url = 'http://192.168.1.82:8008/api/dsp/dataapi/data/pull'
    """
    result = requests.post(url=url, json=body, headers=header)
    log.info("查询数据记录：%s: " % result.text)
    return result.text


# def test_inset():
#  #f =open("C:/Users/Administrator/Downloads/Baymax支持backendservice step.docx","rb")
#     inset_sql = 'insert into blob_test0431 VALUES(%s,%s);'
#     f= open("F:/AUtoApi/For_API/singl_api/new_api_cases/pic1.png", "rb")
#     blob1=f.read()
#     f.close()
#     print(blob1)
#     f = open("F:/AUtoApi/For_API/singl_api/new_api_cases/sex.xls", "rb")
#     xls = f.read()
#     f.close()
#     print(xls)
#     conn = ms._Getconnect()
#     info = {1, blob1, doc, xls, txt}
#     info1 = ('1', blob1)
#     conn.execute(inset_sql,info1)
#     ms.ExecuNoQuery(inset_sql)


def test_blob():

    header = {'Content-Type': 'application/json', "X-AUTH-TOKEN": "Bearer eyJhbGciOiJIUzUxMiJ9.eyJzY29wZSI6ImFjY2Vzc190b2tlbiIsImNsaWVudElkIjoiZGVmYXVsdCIsInZlcnNpb24iOiJCYXltYXgtMy4wLjAuMjMtMjAxODA2MDYiLCJzdWIiOiJhZG1pbiIsImlzcyI6ImRlZmF1bHQiLCJpYXQiOjE1ODgyMjg3OTEsImV4cCI6MTU4ODIzNDc5MX0.7Q79TEQqYceEb8YI_TXB7o8c4dbfRUA7DV5jS1Q_gOIocl8yMsUajzSMleeSaBz8B5P4FDtqRitHQI31Wt_7dQ"}
    body = {"namespace": "default", "column": "doc,blob", "rowkey": "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b", "tablename": "blob_test43017"}
    url = 'http://192.168.1.23:8515/api/woven/synchronizations/objectSyncJobs/data'
    result = requests.post(url=url, json=body, headers=header)
    return result.text


def decod_blob():

    result = test_blob()
    img = base64.b64decode(json.loads(result)['vf']['blob'])
    with open("pic1.png", "wb") as f:
        f.write(img)
    doc = base64.b64decode(json.loads(result)['vf']['doc'])
    with open("3.doc", "wb") as f:
        f.write(doc)



def socket_tcp():
  # 1初始化套接字
    tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

      # 2建立链接  要传入链接的服务器ip和port
    tcp_socket.connect(('192.168.1.83', 8881))
    for i in range(10000):
        time.sleep(1)
        data={"id":i,"name":"zhangsan"+str(i),"sex":"M","age":30,"dates":data_now()}
        # 3发数据
        log.info("往tcp协议socket输入的data：%s",data)
        tcp_socket.send(str(data).encode('utf-8'))
        tcp_socket.send(str('\n').encode('utf-8'))



def batch_create_table():
    """
    function:批量创建多字段表
    """
    import datetime
    # 记录生成时间
    record_start_time = datetime.datetime.now()
    log.info("创建表开始时间：%s" % record_start_time)
    for name in range(20250101, 20250122):
        sql = '''CREATE TABLE `cutomer{}`(
  `user_id33` bigint(20) DEFAULT NULL,
  `name33` text,
  `ID_card33` text,
  `gender33` text,
  `age33` bigint(20) DEFAULT NULL,
  `job33` text,
  `salary33` bigint(20) DEFAULT NULL,
  `product_id33` text,
  `currency_code33` text,
  `credit_card_number33` bigint(20) DEFAULT NULL,
  `credit_card_provider33` text,
  `credit_card_security_code33` bigint(20) DEFAULT NULL,
  `product33` text,
  `channel33` text,
  `prt_dt33` text,
  `time33` text,
  `profile33` text,
  `postcode33` bigint(20) DEFAULT NULL,
  `province33` text,
  `city33` text,
  `user_id10` bigint(20) DEFAULT NULL,
  `name10` text,
  `ID_card10` text,
  `gender10` text,
  `age10` bigint(20) DEFAULT NULL,
  `job10` text,
  `salary10` bigint(20) DEFAULT NULL,
  `product_id10` text,
  `currency_code10` text,
  `credit_card_number10` bigint(20) DEFAULT NULL,
  `credit_card_provider10` text,
  `credit_card_security_code10` bigint(20) DEFAULT NULL,
  `product10` text,
  `channel10` text,
  `prt_dt10` text,
  `time10` text,
  `profile10` text,
  `postcode10` bigint(20) DEFAULT NULL,
  `province10` text,
  `city10` text,
  `name29` text,
  `ID_card29` text,
  `gender29` text,
  `age29` bigint(20) DEFAULT NULL,
  `job29` text,
  `salary29` bigint(20) DEFAULT NULL,
  `product_id29` text,
  `currency_code29` text,
  `credit_card_number29` bigint(20) DEFAULT NULL,
  `credit_card_provider29` text,
  `credit_card_security_code29` bigint(20) DEFAULT NULL,
  `product29` text,
  `channel29` text,
  `prt_dt29` text,
  `time29` text,
  `profile29` text,
  `postcode29` bigint(20) DEFAULT NULL,
  `province29` text,
  `city29` text,
  `street29` text,
  `street9` text,
  `name9` text,
  `ID_card9` text,
  `gender9` text,
  `age9` bigint(20) DEFAULT NULL,
  `job9` text,
  `salary9` bigint(20) DEFAULT NULL,
  `product_id9` text,
  `currency_code9` text,
  `credit_card_number9` bigint(20) DEFAULT NULL,
  `credit_card_provider9` text,
  `credit_card_security_code9` bigint(20) DEFAULT NULL,
  `product9` text,
  `channel9` text,
  `prt_dt9` text,
  `time9` text,
  `profile9` text,
  `postcode9` bigint(20) DEFAULT NULL,
  `province9` text,
  `city9` text,
  `user_id18` bigint(20) DEFAULT NULL,
  `name18` text,
  `ID_card18` text,
  `gender18` text,
  `age18` bigint(20) DEFAULT NULL,
  `job18` text,
  `salary18` bigint(20) DEFAULT NULL,
  `product_id18` text,
  `currency_code18` text,
  `credit_card_number18` bigint(20) DEFAULT NULL,
  `credit_card_provider18` text,
  `credit_card_security_code18` bigint(20) DEFAULT NULL,
  `product18` text,
  `channel18` text,
  `prt_dt18` text,
  `time18` text,
  `profile18` text,
  `postcode18` bigint(20) DEFAULT NULL,
  `province18` text,
  `city18` text,
  `name27` text,
  `ID_card27` text,
  `gender27` text,
  `age27` bigint(20) DEFAULT NULL,
  `job27` text,
  `salary27` bigint(20) DEFAULT NULL,
  `product_id27` text,
  `currency_code27` text,
  `credit_card_number27` bigint(20) DEFAULT NULL,
  `credit_card_provider27` text,
  `credit_card_security_code27` bigint(20) DEFAULT NULL,
  `product27` text,
  `channel27` text,
  `prt_dt27` text,
  `time27` text,
  `profile27` text,
  `postcode27` bigint(20) DEFAULT NULL,
  `province27` text,
  `city27` text,
  `street27` text,
  `name7` text,
  `ID_card7` text,
  `gender7` text,
  `age7` bigint(20) DEFAULT NULL,
  `job7` text,
  `salary7` bigint(20) DEFAULT NULL,
  `product_id7` text,
  `currency_code7` text,
  `credit_card_number7` bigint(20) DEFAULT NULL,
  `credit_card_provider7` text,
  `credit_card_security_code7` bigint(20) DEFAULT NULL,
  `product7` text,
  `channel7` text,
  `prt_dt7` text,
  `time7` text,
  `profile7` text,
  `postcode7` bigint(20) DEFAULT NULL,
  `province7` text,
  `city7` text,
  `user_id16` bigint(20) DEFAULT NULL,
  `name16` text,
  `ID_card16` text,
  `gender16` text,
  `age16` bigint(20) DEFAULT NULL,
  `job16` text,
  `salary16` bigint(20) DEFAULT NULL,
  `product_id16` text,
  `currency_code16` text,
  `credit_card_number16` bigint(20) DEFAULT NULL,
  `credit_card_provider16` text,
  `credit_card_security_code16` bigint(20) DEFAULT NULL,
  `product16` text,
  `channel16` text,
  `prt_dt16` text,
  `time16` text,
  `profile16` text,
  `postcode16` bigint(20) DEFAULT NULL,
  `province16` text,
  `city16` text,
  `name26` text,
  `ID_card26` text,
  `gender26` text,
  `age26` bigint(20) DEFAULT NULL,
  `job26` text,
  `salary26` bigint(20) DEFAULT NULL,
  `product_id26` text,
  `currency_code26` text,
  `credit_card_number26` bigint(20) DEFAULT NULL,
  `credit_card_provider26` text,
  `credit_card_security_code26` bigint(20) DEFAULT NULL,
  `product26` text,
  `channel26` text,
  `prt_dt26` text,
  `time26` text,
  `profile26` text,
  `postcode26` bigint(20) DEFAULT NULL,
  `province26` text,
  `city26` text,
  `street26` text,
  `name5` text,
  `ID_card5` text,
  `gender5` text,
  `age5` bigint(20) DEFAULT NULL,
  `job5` text,
  `salary5` bigint(20) DEFAULT NULL,
  `product_id5` text,
  `currency_code5` text,
  `credit_card_number5` bigint(20) DEFAULT NULL,
  `credit_card_provider5` text,
  `credit_card_security_code5` bigint(20) DEFAULT NULL,
  `product5` text,
  `channel5` text,
  `prt_dt5` text,
  `time5` text,
  `profile5` text,
  `postcode5` bigint(20) DEFAULT NULL,
  `province5` text,
  `city5` text,
  `user_id15` bigint(20) DEFAULT NULL,
  `name15` text,
  `ID_card15` text,
  `gender15` text,
  `age15` bigint(20) DEFAULT NULL,
  `job15` text,
  `salary15` bigint(20) DEFAULT NULL,
  `product_id15` text,
  `currency_code15` text,
  `credit_card_number15` bigint(20) DEFAULT NULL,
  `credit_card_provider15` text,
  `credit_card_security_code15` bigint(20) DEFAULT NULL,
  `product15` text,
  `channel15` text,
  `prt_dt15` text,
  `time15` text,
  `profile15` text,
  `postcode15` bigint(20) DEFAULT NULL,
  `province15` text,
  `city15` text,
  `name25` text,
  `ID_card25` text,
  `gender25` text,
  `age25` bigint(20) DEFAULT NULL,
  `job25` text,
  `salary25` bigint(20) DEFAULT NULL,
  `product_id25` text,
  `currency_code25` text,
  `credit_card_number25` bigint(20) DEFAULT NULL,
  `credit_card_provider25` text,
  `credit_card_security_code25` bigint(20) DEFAULT NULL,
  `product25` text,
  `channel25` text,
  `prt_dt25` text,
  `time25` text,
  `profile25` text,
  `postcode25` bigint(20) DEFAULT NULL,
  `province25` text,
  `city25` text,
  `street25` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8'''.format(name)
        ms_conn.ExecuNoQuery(sql)
    record_end_time = datetime.datetime.now()
    # 表记录生成时间
    log.info("表生成结束时间：%s" % record_end_time)
    log.info("创建表总耗时：%s" % (record_end_time - record_start_time))
#batch_create_table()

def batch_insert_table():
    """
    :return: 大批量数据插入mysql表
     sql = "INSERT INTO `supp_1` VALUES (%s,%s,%s,%s)"
    """
    import datetime,random
    values_list = []
    sql = "INSERT INTO `supp_1` VALUES (%s,%s,%s,%s)"
    # 记录生成时间
    record_start_time = datetime.datetime.now()
    log.info("生成数据开始时间：%s" % record_start_time)
    for i in range(5000000):
        name = fake.name()
        age = random.randint(20, 39)
        sex = random.choice('男女')
        times = data_now()
        values_list.append((name, age, sex, times))
    record_end_time = datetime.datetime.now()
    # 记录生成时间
    log.info("生成数据结束时间：%s" % record_end_time)
    log.info("生成数据总耗时：%s" % (record_end_time - record_start_time))
    # 记录执行前时间
    start_time = datetime.datetime.now()
    log.info("数据插入开始时间：%s" % start_time)
    log.info("批量数据插入中.....")
    ms_conn.ExecutManyInsert(sql, values_list)
    # 记录执行完成时间
    end_time = datetime.datetime.now()
    log.info("数据插入结束时间：%s" % end_time)
    # 计算时间差
    log.info("批量插入完成")
    log.info("插入数据耗时：%s" % (end_time - start_time))
    log.info("总耗时：%s" % (end_time - record_start_time))

#batch_insert_table()


from faker import Faker
import random
import datetime
import pandas as pd
def random_data():
    """
    :return: 生成csv文件
    """
    fake = Faker("zh_CN") # 初始化，可生成中文数据
    #设置字段
    #index = []
    # for i in range(1,12):
    #     exec('x'+str(i)+'=[]')
    start_time = datetime.datetime.now()
    log.info('开始时间：%s',start_time)
    x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21,x22,x23,x24,x25,x26,x27,x28,x29,x30,x31,x32,x33,x34,x35,x36,x37,x38,x39,x40,x41,x42,x43,x44,x45,x46,x47,x48,x49,x50,x51,x52,x53,x54,x55,x56,x57,x58,x59,x60=[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]
    #设置样本
    prod_cd = ['W00028','W00021','W00022']
    prod_nm = ['微信支付','银联扫码支付','转账']
    channel = ['APP','网银','短信']
    year = ['2020','2021','2022']

    #循环生成数据20行，具体多少行可以根据需求修改
    for i in range(10):
        date = random.choice(year)+fake.date()[4:]
        time = random.choice(year)+fake.date()[4:]+' '+fake.time()
        x1.append('1'+str(fake.random_number(digits=8))) # 随机数字，参数digits设置生成的数字位数
        x2.append(fake.name())
        x3.append(fake.ssn()) # 身份证
        x4.append(random.choice('男女'))
        x5.append(random.randint(22,35))
        x6.append(fake.job())
        x7.append(random.randint(10000,1000000))
        x8.append(random.choice(prod_cd))
        x17.append(fake.currency_code())
        x18.append(fake.credit_card_number())
        x19.append(fake.credit_card_provider())
        x20.append(fake.credit_card_security_code())
        x9.append(random.choice(prod_nm))
        x10.append(random.choice(channel))
        x11.append(date)
        x21.append(time)
        x12.append(fake.postcode())
        x13.append(fake.postcode())
        x14.append(fake.province())
        x15.append(fake.city_suffix())
        x16.append(fake.street_address())
        x22.append(fake.name())
        x23.append(fake.ssn()) # 身份证
        x24.append(random.choice('男女'))
        x25.append(random.randint(22,35))
        x26.append(fake.job())
        x27.append(random.randint(10000,1000000))
        x28.append(random.choice(prod_cd))
        x29.append(fake.currency_code())
        x30.append(fake.credit_card_number())
        x31.append(fake.credit_card_provider())
        x32.append(fake.credit_card_security_code())
        x33.append(random.choice(prod_nm))
        x34.append(random.choice(channel))
        x35.append(date)
        x36.append(time)
        x37.append(fake.postcode())
        x38.append(fake.postcode())
        x39.append(fake.province())
        x40.append(fake.city_suffix())
        x41.append(fake.street_address())
        x42.append(fake.name())
        x43.append(fake.ssn()) # 身份证
        x44.append(random.choice('男女'))
        x45.append(random.randint(22,35))
        x46.append(fake.job())
        x47.append(random.randint(10000,1000000))
        x48.append(random.choice(prod_cd))
        x49.append(fake.currency_code())
        x50.append(fake.credit_card_number())
        x51.append(fake.credit_card_provider())
        x52.append(fake.credit_card_security_code())
        x53.append(random.choice(prod_nm))
        x54.append(random.choice(channel))
        x55.append(date)
        x56.append(time)
        x57.append(fake.postcode())
        x58.append(fake.postcode())
        x59.append(fake.province())
        x60.append(fake.city_suffix())
    #创建数据表
    datas = pd.DataFrame({
        'user_id':x1,
        'name':x2,
        'ID_card':x3,
        'gender':x4,
        'age':x5,
        'job':x6,
        'salary':x7,
        'product_id':x8,
        'currency_code':x17,
        'credit_card_number':x18,
        'credit_card_provider':x19,
        'credit_card_security_code':x20,
        'product':x9,
        'channel':x10,
        'prt_dt':x11,
        'time':x21,
        'profile':x12,
        'postcode':x13,
        'province':x14,
        'city':x15,
        'street':x16,
        'user_id1': x22,
        'name1': x23,
        'ID_card1': x24,
        'gender1': x25,
        'age1': x26,
        'job1': x27,
        'salary1': x28,
        'product_id1': x29,
        'currency_code1': x30,
        'credit_card_number1': x31,
        'credit_card_provider1': x32,
        'credit_card_security_code1': x33,
        'product1': x34,
        'channel1': x35,
        'prt_dt1': x36,
        'time1': x37,
        'profile1': x38,
        'postcode1': x39,
        'province1': x40,
        'city1': x41,
        'street1': x42,
        'user_id2': x43,
        'name2': x44,
        'ID_card2': x45,
        'gender2': x46,
        'age2': x47,
        'job2': x48,
        'salary2': x49,
        'product_id2': x50,
        'currency_code2': x51,
        'credit_card_number2': x52,
        'credit_card_provider2': x53,
        'credit_card_security_code2': x54,
        'product2': x55,
        'channel2': x56,
        'prt_dt2': x57,
        'time2': x58,
        'profile2': x59,
        'postcode2': x60
    })

    #DataFrame类的to_csv()方法输出数据内容，不保存行索引和列名
    datas.to_csv(r'F:\baymax-1.2.3\customer1.csv',encoding='utf-8',index=False,header=True)
    stop_time = datetime.datetime.now()
    log.info("结束时间：%s",stop_time)
    log.info("耗时：%s",stop_time-start_time)

#random_data()



# -*- coding:UTF-8 -*-
import pandas as pd
from sqlalchemy import create_engine

def csv_import_table():
    """
    :return: 批量创建表，如果不存在表，则自动创建，csv文件批量导入表
    """
    # 文件路径
    path = r'F:\baymax-1.2.3\customer1.csv'
    data = pd.read_csv(path,encoding='utf8')
    log.info("data: " % data)
    start_time = datetime.datetime.now()
    log.info("csv文件导入表-开始时间：%s" % start_time)
    # 表名
    # 如果不存在表，则自动创建
    table_name = []
    for i in range(2024010101,2024010201):
        name ="customer"+ str(i)
        table_name.append(name)
    for table in table_name:
        engine = create_engine("mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}".format(**MySQL_CONFIG1), max_overflow=5)
        data.to_sql(table,engine,index=False,if_exists='append',)
        end_time = datetime.datetime.now()
        log.info("csv文件导入%s表-结束时间：%s" % (table, end_time))
    # 记录执行完成时间
    end_time = datetime.datetime.now()
    log.info("csv文件导入表-结束时间：%s" % end_time)
    # 计算时间差
    log.info("csv文件导入完成")
    log.info("导入数据总耗时：%s" % (end_time - start_time))

#csv_import_table()




class operateKafka:
    """
    :return: 操作kafka，往kafka发送字符串和json数据
    """
    def __init__(self):
        hosts = ["192.168.1.82:9094","192.168.1.82:9094"]
        client = KafkaClient(hosts=hosts[0])
        clients = KafkaClient(hosts=hosts[1])
        self.bstrap_servers=['192.168.1.82:9094']   #192.168.1.82:9094 192.168.1.67:9092
        self.topic = client.topics['commander.scheduler.xdr.120.123']  #commander.scheduler.poseidon.flow COMMANDER_FLOW  commander.scheduler.xdr_compass_16x
        self.str_topic = clients.topics['test_kafka0210'] #往topic发送字符串
        self.json_topic = "test_kafka042712" #往topic发送json


    """
    function:send 调度message to kafka
    """
    def sendMessage(self, cluster,data):
        with self.topic.get_sync_producer() as producer:
                 js = xmlToJson(data)
                 js=json.loads(js)
                 js["root"]["sliceTime"]=str(hour_slice())
                 js["root"]["createTime"]=str(data_now())
                 js["root"]["fullName"]=cluster+"///tmp/gjt////demo-"+str(hour_stamp())+".csv"
                 print(js)
                 xm = jsonToXml(json.dumps(js))
                 print(xm)
                 mydict = xm.replace('<?xml version="1.0" encoding="utf-8"?>','')
                 print( mydict)
                 producer.produce(str(mydict).encode())

    """
    function:send json message to kafka
    """
    def send_string_kafka(self):
        with self.str_topic.get_sync_producer() as producer:
            new_data=[]
            for i in range(10):
                data={"test_int":i,"test_bigint":i+100000000,"test_byte":100+random.randint(1,5),"test_short":random.randint(32000,35000),"test_binary":None,"test_float":100.123456+i,"test_double":i+0.23456789123001,"test_decimal":123456789123456789.001+i,"test_boolean":"true","test_string1":"我是string类型1","test_string2":'{"test_json":"我json1"}',"test_string3":"<book><name>Izzy</name><tel>17302280361</tel><age>6</age></book>","test_timestamp":data_now(),"test_date":day_now()}
                new_data.append(data)
            log.info("往kafka输入的data：%s", new_data)
            for data in new_data:
                dat = ','.join([str(i) for i in list(data.values())])
                producer.produce(str(dat).encode())

    """
    function:send str message to kafka
    """
    def send_str_kafka(self):
        with self.str_topic.get_sync_producer() as producer:
            new_data=[]
            for i in range(10000):
                data={"id":i,"name":fake.name(),"sex":random.choice('男女'),"age":random.randint(22,35),"dates":timestamp_now()}
                new_data.append(data)
            log.info("往kafka输入的data：%s", new_data)
            for data in new_data:
                dat = ','.join([str(i) for i in list(data.values())])
                producer.produce(str(dat).encode())



    """
    function:send json message to kafka
    """
    def send_manayjson_kafka(self):
        new_data=[]
        producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                 bootstrap_servers=self.bstrap_servers)
        for i in range(10000):
            time.sleep(1)
            data = {"id": i, "name": fake.name(), "sex": random.choice('男女'), "age": random.randint(21, 35),
                    "stime": timestamp_utc(), "time": timestamp_utc(), "create_time": timestamp_utc(),"update_time": timestamp_utc(),"ssn":fake.ssn(),
            "randoms":random.randint(22,35),"job":fake.job(),"rand_int":random.randint(10000,1000000), "currency_code":fake.currency_code(),
            "credit_card_number":fake.credit_card_number(), "credit_card_provider": fake.credit_card_provider(), "credit_card_security_code": fake.credit_card_security_code(),
            "postcode":fake.postcode(), "province": fake.province(), "city_suffix": fake.city_suffix(), "street_address":fake.street_address()}
            new_data.append(data)
        #log.info("往kafka输入的data：%s", new_data)
        #time.sleep(1)
        for message in new_data:
            producer.send(self.json_topic, message)
        producer.close()

# if __name__ == '__main__':
#
#     while True:
#      operateKafka().send_str_kafka()



# from pdf2docx import Converter
#
# def pdfTodoc():
#     """
#     function:pdf转换doc**
#     """
#     fileset_dir=os.path.join(os.path.abspath('.'),'InfoMover安装配置手册V1.6.pdf')
#     pdf_file ='sTempoAI帮助手册_V6.6 - 副本.pdf'
#     docx_file ='InfoMover安装配置手册V1.6.docx'
#     # convert pdf to docx
#     cv = Converter(fileset_dir)
#     cv.convert(docx_file, start=0, end=None)
#     cv.close()

#pdfTodoc()