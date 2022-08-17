import os
import socket
import time
import random
from kafka import KafkaProducer
from util.logs import Logger
from basic_info.setting import MySQL_CONFIG1
from util.Open_DB import MYSQL
import base64
import xmltodict
from pykafka import KafkaClient
import json
import requests
from faker import Faker
from elasticsearch import Elasticsearch
from util.timestamp_13 import data_now, hour_stamp, hour_slice, second_stamp, min_stamp, day_now, timestamp_now, timestamp_utc

log = Logger().get_log()
ms = MYSQL(MySQL_CONFIG1["HOST"], MySQL_CONFIG1["USER"], MySQL_CONFIG1["PASSWORD"], MySQL_CONFIG1["DB"], MySQL_CONFIG1["PORT"])
''''''
sql = "select links from merce_flow where  name = 'minus_0628_3_no_element_test'"

fake = Faker("zh_CN") # 初始化，可生成中文数据

def xmlToJson(xml):
    try:
        converteJson = xmltodict.parse(xml,encoding='utf-8')
        jsonStr = json.dumps(converteJson,indent=4)
        return jsonStr
    except Exception as e:
        print("异常信息：%s",e)

def jsonToXml(js):
    convertXml =''
    jsDict =json.loads(js)
    try:
        convertXml = xmltodict.unparse(jsDict,encoding='utf-8')
    except:
        convertXml = xmltodict.unparse({'msg':jsDict},encoding='utf-8')
    finally:
        return convertXml

def test_secret_key():
    header = {'Content-Type': 'application/json', "Accept": "application/json"}
    body = {"accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1"}
    url = 'http://192.168.1.82:8008/api/dsp/dataapi/data/secertkey'
    result = requests.get(url=url, params=body, headers=header)
    print("--====----", result.text)


def test_pull():

    header = {'hosts': '192.168.2.142', 'Content-Type': 'application/json', "Accept": "application/json"}
    body = {"dataServiceId": "722844377071747072", "accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1", "encrypted":"false","offset": 0, "size": 100, "timestamp": 1}
    url = 'http://192.168.1.82:8008/api/dsp/dataapi/data/pull'
    result = requests.post(url=url, json=body, headers=header)
    print("------", result.text)


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
    print("------", result)
    return result.text

def decod_blob():

    result = test_blob()
    print(result)
    print(type(result))
    img = base64.b64decode(json.loads(result)['vf']['blob'])
    with open("pic1.png", "wb") as f:
        f.write(img)
    doc = base64.b64decode(json.loads(result)['vf']['doc'])
    with open("3.doc", "wb") as f:
        f.write(doc)


def es_create():
    try:
        es = Elasticsearch(hosts="192.168.1.82", port=9204,http_auth=('admin', 'admin')) #http_auth开启用户名和密码认证http_auth=('admin', 'admin')
        #es = Elasticsearch(hosts="192.168.1.82", port=9206) #http_auth开启用户名和密码认证http_auth=('admin', 'admin')
        es.indices.create(index="sink_es6", ignore=400)
        for i in range(10000):
            time.sleep(1)
            data = {"name": "小明", "age": "28", "gender": "男","date":"2021-12-16","ad":"","calss":3,"kind":"23","book":"语文","count":"","price":228,"love":"reading"}
            # 3发数据
            log.info("往es输入的data：%s",data)
            res = es.index(index="sink_es6", doc_type="doc", body=data)
            if i==10000:
                break
    except Exception as e:
        log.error("es查询异常",e)
        return


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


def insert_table():
    for i in range(10000000):
          insertsql = "insert  into supp values ('%s','%s','%s','%s')"%(fake.name(),random.randint(20,35),random.choice('男女'),data_now())
          ms.ExecuNoQuery(insertsql)

# -*- coding: utf-8 -*-
import pandas as pd
from faker import Faker
import random
import datetime
def random_data():
    fake = Faker("zh_CN") # 初始化，可生成中文数据

    #设置字段
    #index = []
    # for i in range(1,12):
    #     exec('x'+str(i)+'=[]')
    start_time = datetime.datetime.now()
    log.info('开始时间：%s',start_time)
    x1,x2,x3,x4,x5,x6,x7,x8,x9,x10,x11,x12,x13,x14,x15,x16,x17,x18,x19,x20,x21=[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]
    #设置样本
    prod_cd = ['W00028','W00021','W00022']
    prod_nm = ['微信支付','银联扫码支付','转账']
    channel = ['APP','网银','短信']
    year = ['2020','2021','2022']

    #循环生成数据20行，具体多少行可以根据需求修改
    for i in range(5000000):
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
        x12.append(fake.profile())
        x13.append(fake.postcode())
        x14.append(fake.province())
        x15.append(fake.city_suffix())
        x16.append(fake.street_address())

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
        'street':x16
    })

    #DataFrame类的to_csv()方法输出数据内容，不保存行索引和列名
    datas.to_csv(r'F:\baymax-1.2.3\customer.csv',encoding='utf-8',index=False,header=True)
    stop_time = datetime.datetime.now()
    log.info("结束时间：%s",stop_time)
    log.info("耗时：%s",stop_time-start_time)
#random_data()

class operateKafka:


    def __init__(self):
        hosts = ["192.168.1.55:9092","192.168.1.82:9094"]
        client = KafkaClient(hosts=hosts[0])
        clients = KafkaClient(hosts=hosts[1])
        self.bstrap_servers=['192.168.1.82:9094']
        self.topic = client.topics['commander.scheduler.poseidon.flow']  #CARPO_FLOW1 CARPO_XDR commander.scheduler COMMANDER_FLOW
        self.str_topic = clients.topics['test_kafka0209'] #往topic发送字符串
        self.json_topic = "test_kafka0210" #往topic发送json
    global stu_nm
    stu_nm = ['张三','李四','王五','赵六','黄七','陈八']


    """
    function:send message to kafka
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
    def send_json_kafka(self):
     producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),bootstrap_servers=self.bstrap_servers)
     for i in range(100000):
        time.sleep(1)
        data={"id":i,"name":fake.name(),"sex":random.choice('男女'),"age":random.randint(21,35),"stime":timestamp_utc()}
        log.info("往kafka输入的data：%s",data)
        producer.send(self.json_topic, data)
     producer.close()

    """
    function:send str message to kafka
    """
    def send_str_kafka(self):
        with self.str_topic.get_sync_producer() as producer:
            for i in range(100000):
                time.sleep(1)
                data={"id":i,"name":fake.name(),"sex":random.choice('男女'),"age":random.randint(1,1),"dates":timestamp_now()}
                dat = ','.join([str(i) for i in list(data.values())])
                log.info("往kafka输入的data：%s",dat)
                producer.produce(str(dat).encode())

if __name__ == '__main__':

    while True:
     operateKafka().send_str_kafka()

#insert_table()

from pdf2docx import Converter

def pdfTodoc():

  ## pdf转换doc**
    fileset_dir=os.path.join(os.path.abspath('.'),'TempoAI帮助手册_V6.6.pdf')
    pdf_file ='sTempoAI帮助手册_V6.6 - 副本.pdf'
    docx_file ='TempoAI帮助手册_V6.6.docx'

    # convert pdf to docx
    cv = Converter(fileset_dir)
    cv.convert(docx_file, start=0, end=None)
    cv.close()

#pdfTodoc()