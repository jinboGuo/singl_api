from basic_info.setting import MySQL_CONFIG
from util.Open_DB import MYSQL
import base64
import xmltodict
from pykafka import KafkaClient
import json
import requests
from elasticsearch import Elasticsearch
from util.timestamp_13 import data_now, hour_stamp, hour_slice

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
''''''
sql = "select links from merce_flow where  name = 'minus_0628_3_no_element_test'"
insetsql = "insert  into blob_test0430"


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
#     f= open("F:/AUtoApi/For_API/singl_api/new_api_cases/3.doc", "rb")
#     doc=f.read()
#     f.close()
#     print(doc)
#     f=open("F:/AUtoApi/For_API/singl_api/new_api_cases/test1.txt", "rb")
#     txt=f.read()
#     f.close()
#     print(txt)
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
    es = Elasticsearch(hosts="192.168.2.142", port=9200, http_auth=('admin', 'admin')) #http_auth开启用户名和密码认证
    es.indices.create(index="sink_es3", ignore=400)

    data = {"name": "小明", "age": "8", "gender": "男"}
    res = es.index(index="sink_es3", doc_type="doc", body=data)
    print(res)


class operateKafka:

    def __init__(self):
        hosts = "192.168.1.55:9092"
        client = KafkaClient(hosts=hosts)
        self.topic = client.topics['CARPO_XDR']

    """
    function:send message to kafka
    """
    def sendMessage(self, data):
        with self.topic.get_sync_producer() as producer:
                 js = xmlToJson(data)
                 js=json.loads(js)
                 js["root"]["sliceTime"]=str(hour_slice())
                 js["root"]["createTime"]=str(data_now())
                 js["root"]["fullName"]="hdfs://info1:8020///tmp/gjt////demo-"+str(hour_stamp())+".csv"
                 print(js)
                 xm = jsonToXml(json.dumps(js))
                 print(xm)
                 mydict = xm.replace('<?xml version="1.0" encoding="utf-8"?>','')
                 print( mydict)
                 producer.produce(str(mydict).encode())