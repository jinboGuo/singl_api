import socket
import time

from kafka import KafkaProducer
from util.logs import Logger
from basic_info.setting import MySQL_CONFIG1
from util.Open_DB import MYSQL
import base64
import xmltodict
from pykafka import KafkaClient
import json
import requests
from elasticsearch import Elasticsearch
from util.timestamp_13 import data_now, hour_stamp, hour_slice, second_stamp, min_stamp, day_now, timestamp_now

log = Logger().get_log()
ms = MYSQL(MySQL_CONFIG1["HOST"], MySQL_CONFIG1["USER"], MySQL_CONFIG1["PASSWORD"], MySQL_CONFIG1["DB"], MySQL_CONFIG1["PORT"])
''''''
sql = "select links from merce_flow where  name = 'minus_0628_3_no_element_test'"



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
    es = Elasticsearch(hosts="192.168.2.131", port=9200,http_auth=('admin', 'admin')) #http_auth开启用户名和密码认证http_auth=('admin', 'admin')
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
    for i in range(100000):
      if i%2==0:
        insertsql = "insert  into supp values ('%s','%s','%s','%s')"%("wangwu"+str(i),31,'F',data_now())
        ms.ExecuNoQuery(insertsql)
      else:
          insertsql = "insert  into supp values ('%s','%s','%s','%s')"%("lisi"+str(i),29,'M',data_now())
          ms.ExecuNoQuery(insertsql)

class operateKafka:



    def __init__(self):
        hosts = ["192.168.1.55:9092","192.168.1.82:9094"]
        client = KafkaClient(hosts=hosts[0])
        clients = KafkaClient(hosts=hosts[1])
        self.bstrap_servers=['192.168.1.82:9094']
        self.topic = client.topics['commander.scheduler']  #CARPO_FLOW1 CARPO_XDR commander.scheduler COMMANDER_FLOW
        self.str_topic = clients.topics['test_kafka1207'] #往topic发送字符串
        self.json_topic = "into_003_kafka_012" #往topic发送json



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
     for i in range(10000):
        time.sleep(1)
        data={"id":i,"name":"zhangsan"+str(i),"sex":"m","age":30,"dates":timestamp_now()}
        log.info("往kafka输入的data：%s",data)
        producer.send(self.json_topic, data)
     producer.close()

    """
    function:send str message to kafka
    """
    def send_str_kafka(self):
        with self.str_topic.get_sync_producer() as producer:
            for i in range(10000):
                time.sleep(1)
                if i%2==0:
                    data={"id":i,"name":"zhangsan"+str(i),"sex":"m","age":30,"dates":timestamp_now()}
                    dat = ','.join([str(i) for i in list(data.values())])
                    log.info("往kafka输入的data：%s",dat)
                    producer.produce(str(dat).encode())
                else:
                    data={"id":i,"name":"ouyanfei"+str(i),"sex":"F","age":31,"dates":timestamp_now()}
                    dat = ','.join([str(i) for i in list(data.values())])
                    log.info("往kafka输入的data：%s",dat)
                    producer.produce(str(dat).encode())

if __name__ == '__main__':

    while True:
     operateKafka().send_str_kafka()