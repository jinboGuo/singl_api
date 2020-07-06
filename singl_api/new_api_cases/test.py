from util.encrypt import parameter_ungzip
from basic_info.setting import MySQL_CONFIG
from util.Open_DB import MYSQL
import base64
import json
import time

import requests
# 配置数据库连接
from util.timestamp_13 import timestamp_now

ms = MYSQL(MySQL_CONFIG["HOST"], MySQL_CONFIG["USER"], MySQL_CONFIG["PASSWORD"], MySQL_CONFIG["DB"])
''''''
sql = "select links from merce_flow where  name = 'minus_0628_3_no_element_test'"
insetsql = "insert  into blob_test0430"
#result = ms.ExecuQuery(sql)
#end_result = result[0]["links"]
#print(result[0]["links"])
#print(parameter_ungzip(end_result))

def test_secret_key():

    #header = {'hosts': '127.0.0.1','Content-Type': 'application/json', "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX25hbWUiOiJ0ZXN0MDA3Iiwic2NvcGUiOlsicmVhZCIsIndyaXRlIiwidHJ1c3QiXSwiZXhwIjoxNTg3MTIwMjI4LCJhdXRob3JpdGllcyI6WyJST0xFX0NVU1RPTUVSIl0sImp0aSI6ImV5SmhiR2NpT2lKU1V6STFOaUlzSW5SNWNDSTZJa3BYVkNKOS5leUoxYzJWeVgyNWhiV1VpT2lKMFpYTjBNREEzSWl3aWMyTnZjR1VpT2xzaWNtVmhaQ0lzSW5keWFYUmxJaXdpZEhKMWMzUWlYU3dpWlhod0lqb3hOVGczTVRJd01qSTRMQ0poZFhSb2IzSnBkR2xsY3lJNld5SlNUMHhGWDBOVlUxUlBUVVZTSWwwc0ltcDBhU0k2SW1OaVpEZzNOREptTFRrM01HUXRORE00WVMwNE1UQXlMV1kwTUdOak56TTNZMkV4WmlJc0luVnpaWEpwYm1adklqcDdJbWh2YzNRaU9tNTFiR3dzSW1Oc2FXVnVkRWxrSWpvaVkzVnpkRzl0WlhJaUxDSjBaVzVoYm5SSlpDSTZJakl3Wm1VMFpqY3hMVEZoTXpjdE5HRmtaaTFpTWpJMExXUmlaamszTlRJek9UTTFPU0lzSW5SbGJtRnVkRTVoYldVaU9pSmtaV1poZFd4MElpd2lkWE5sY2tsa0lqb2lZV1kyTVRVeVl6WXRaR1poWmkwMFlXWTFMV0UwT0dJdE1Ua3dOVEpsTW1Nell6UXpJaXdpZFhObGNtNWhiV1VpT2lKMFpYTjBNREEzSWl3aVlXUnRhVzRpT21aaGJITmxMQ0oxYzJWeVZIbHdaU0k2SWtOMWMzUnZiV1Z5SW4wc0ltTnNhV1Z1ZEY5cFpDSTZJbU4xYzNSdmJXVnlJbjAuYjBnYks1dVBSeHB3c3hFR0ZEUkRzRkV2MlBJLXc2MkpPeEdobnllQk9hZXEyNHN0aldDbXkzZ1VJejFteUxPZ2FYZ0Y1b2FZU0Qzb0tyRDZUTWRvT0dIYXF0MnM5THJKV3dTNmI0bU5MQlgwcmVTNnRpSjBfb1lOeU55ZzY0eEJidlpJWHBSelR0QUNBZjF2UENXLW5GSHY4NGFGVmJ4ZEl4bzh2U2tQYzlaeVNVd0lQOG15YmxneDZiVTZiVzlHM1BxUDMyT3M1XzM2V0s5RW44b3c0MkI2cDNMTGNNMnZzb2Ffa3lBOFJzZUdqbklraHB4WEdUSTlhSVh4NU9JcVZ3TjBEWTlld0p2VXVjLUJhSWZxTS00QTNKaDlEcTFwY25UbmF3RTZabTl2VGJzRjRaVUJjeFBDRWFoYWN0V01fc2IyRWdnUndJamY2QWhVSUZmLXVnIiwidXNlcmluZm8iOnsiaG9zdCI6bnVsbCwiY2xpZW50SWQiOiJjdXN0b21lciIsInRlbmFudElkIjoiMjBmZTRmNzEtMWEzNy00YWRmLWIyMjQtZGJmOTc1MjM5MzU5IiwidGVuYW50TmFtZSI6ImRlZmF1bHQiLCJ1c2VySWQiOiJhZjYxNTJjNi1kZmFmLTRhZjUtYTQ4Yi0xOTA1MmUyYzNjNDMiLCJ1c2VybmFtZSI6InRlc3QwMDciLCJhZG1pbiI6ZmFsc2UsInVzZXJUeXBlIjoiQ3VzdG9tZXIifSwiY2xpZW50X2lkIjoiY3VzdG9tZXIifQ.BC1KkBcKBIyiBU2zReVUQaYmowMeMu1MRrhQTsyWySofVa4r3qATPOqGUAP3xCFJpuw6WKl3vkYMiEHOlsIHY3c1CQAzyq-ne1x-ZsMrQCYMBu9rhL5WdMUVWq-2UFwpYVWlBpX-gLUr6auMdqQCSd0YpH87TdjykKjLcW73mT908tmwipIaP742jKYxhOOyaZeuq42AtI_F8BpOIbmL6BXY9aeB9eMCo1biGxBxja2YXeR3qC3j6DPyfjfPfBCfYcHBSduzAPCcRVtwQL0KGfVHYG75kDeyR_7OoiG0jYTXRiVBGXtiXD6ZvY1yMOaPObbJEBKkXo_eH0Dp4yaZcA", "Accept": "application/json"}
    #body = {"dataServiceId": "700735577841991680", "accessKey": "1bb2f62d-25e2-470c-b802-4b289b847e1a", "offset": 0, "size": 10000}
    header = {'Content-Type': 'application/json', "Accept": "application/json"}
    body = {"accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1"} #15f84879-071f-422d-b9a6-4294ae625769  eeaad4eb-3f07-426f-9afb-f89ff7d6893b
    url = 'http://192.168.1.82:8008/api/dsp/dataapi/data/secertkey'
    result = requests.get(url=url, params=body, headers=header)
    print("--====----", result.text)


def test_pull():

    #header = {'hosts': '127.0.0.1','Content-Type': 'application/json', "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX25hbWUiOiJ0ZXN0MDA3Iiwic2NvcGUiOlsicmVhZCIsIndyaXRlIiwidHJ1c3QiXSwiZXhwIjoxNTg3MTIwMjI4LCJhdXRob3JpdGllcyI6WyJST0xFX0NVU1RPTUVSIl0sImp0aSI6ImV5SmhiR2NpT2lKU1V6STFOaUlzSW5SNWNDSTZJa3BYVkNKOS5leUoxYzJWeVgyNWhiV1VpT2lKMFpYTjBNREEzSWl3aWMyTnZjR1VpT2xzaWNtVmhaQ0lzSW5keWFYUmxJaXdpZEhKMWMzUWlYU3dpWlhod0lqb3hOVGczTVRJd01qSTRMQ0poZFhSb2IzSnBkR2xsY3lJNld5SlNUMHhGWDBOVlUxUlBUVVZTSWwwc0ltcDBhU0k2SW1OaVpEZzNOREptTFRrM01HUXRORE00WVMwNE1UQXlMV1kwTUdOak56TTNZMkV4WmlJc0luVnpaWEpwYm1adklqcDdJbWh2YzNRaU9tNTFiR3dzSW1Oc2FXVnVkRWxrSWpvaVkzVnpkRzl0WlhJaUxDSjBaVzVoYm5SSlpDSTZJakl3Wm1VMFpqY3hMVEZoTXpjdE5HRmtaaTFpTWpJMExXUmlaamszTlRJek9UTTFPU0lzSW5SbGJtRnVkRTVoYldVaU9pSmtaV1poZFd4MElpd2lkWE5sY2tsa0lqb2lZV1kyTVRVeVl6WXRaR1poWmkwMFlXWTFMV0UwT0dJdE1Ua3dOVEpsTW1Nell6UXpJaXdpZFhObGNtNWhiV1VpT2lKMFpYTjBNREEzSWl3aVlXUnRhVzRpT21aaGJITmxMQ0oxYzJWeVZIbHdaU0k2SWtOMWMzUnZiV1Z5SW4wc0ltTnNhV1Z1ZEY5cFpDSTZJbU4xYzNSdmJXVnlJbjAuYjBnYks1dVBSeHB3c3hFR0ZEUkRzRkV2MlBJLXc2MkpPeEdobnllQk9hZXEyNHN0aldDbXkzZ1VJejFteUxPZ2FYZ0Y1b2FZU0Qzb0tyRDZUTWRvT0dIYXF0MnM5THJKV3dTNmI0bU5MQlgwcmVTNnRpSjBfb1lOeU55ZzY0eEJidlpJWHBSelR0QUNBZjF2UENXLW5GSHY4NGFGVmJ4ZEl4bzh2U2tQYzlaeVNVd0lQOG15YmxneDZiVTZiVzlHM1BxUDMyT3M1XzM2V0s5RW44b3c0MkI2cDNMTGNNMnZzb2Ffa3lBOFJzZUdqbklraHB4WEdUSTlhSVh4NU9JcVZ3TjBEWTlld0p2VXVjLUJhSWZxTS00QTNKaDlEcTFwY25UbmF3RTZabTl2VGJzRjRaVUJjeFBDRWFoYWN0V01fc2IyRWdnUndJamY2QWhVSUZmLXVnIiwidXNlcmluZm8iOnsiaG9zdCI6bnVsbCwiY2xpZW50SWQiOiJjdXN0b21lciIsInRlbmFudElkIjoiMjBmZTRmNzEtMWEzNy00YWRmLWIyMjQtZGJmOTc1MjM5MzU5IiwidGVuYW50TmFtZSI6ImRlZmF1bHQiLCJ1c2VySWQiOiJhZjYxNTJjNi1kZmFmLTRhZjUtYTQ4Yi0xOTA1MmUyYzNjNDMiLCJ1c2VybmFtZSI6InRlc3QwMDciLCJhZG1pbiI6ZmFsc2UsInVzZXJUeXBlIjoiQ3VzdG9tZXIifSwiY2xpZW50X2lkIjoiY3VzdG9tZXIifQ.BC1KkBcKBIyiBU2zReVUQaYmowMeMu1MRrhQTsyWySofVa4r3qATPOqGUAP3xCFJpuw6WKl3vkYMiEHOlsIHY3c1CQAzyq-ne1x-ZsMrQCYMBu9rhL5WdMUVWq-2UFwpYVWlBpX-gLUr6auMdqQCSd0YpH87TdjykKjLcW73mT908tmwipIaP742jKYxhOOyaZeuq42AtI_F8BpOIbmL6BXY9aeB9eMCo1biGxBxja2YXeR3qC3j6DPyfjfPfBCfYcHBSduzAPCcRVtwQL0KGfVHYG75kDeyR_7OoiG0jYTXRiVBGXtiXD6ZvY1yMOaPObbJEBKkXo_eH0Dp4yaZcA", "Accept": "application/json"}
    #body = {"dataServiceId": "700735577841991680", "accessKey": "1bb2f62d-25e2-470c-b802-4b289b847e1a", "offset": 0, "size": 10000}
    header = {'hosts': '192.168.2.142', 'Content-Type': 'application/json', "Accept": "application/json"}
    body = {"dataServiceId": "722844377071747072", "accessKey": "5d577396-2f67-483c-90ab-a4e94932ecd1", "encrypted":"false","offset": 0, "size": 100, "timestamp": 1}
    url = 'http://192.168.1.82:8008/api/dsp/dataapi/data/pull'
    result = requests.post(url=url, json=body, headers=header)
    print("------", result.text)


#test_secret_key()
#test_pull()

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

    #header = {'hosts': '127.0.0.1','Content-Type': 'application/json', "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX25hbWUiOiJ0ZXN0MDA3Iiwic2NvcGUiOlsicmVhZCIsIndyaXRlIiwidHJ1c3QiXSwiZXhwIjoxNTg3MTIwMjI4LCJhdXRob3JpdGllcyI6WyJST0xFX0NVU1RPTUVSIl0sImp0aSI6ImV5SmhiR2NpT2lKU1V6STFOaUlzSW5SNWNDSTZJa3BYVkNKOS5leUoxYzJWeVgyNWhiV1VpT2lKMFpYTjBNREEzSWl3aWMyTnZjR1VpT2xzaWNtVmhaQ0lzSW5keWFYUmxJaXdpZEhKMWMzUWlYU3dpWlhod0lqb3hOVGczTVRJd01qSTRMQ0poZFhSb2IzSnBkR2xsY3lJNld5SlNUMHhGWDBOVlUxUlBUVVZTSWwwc0ltcDBhU0k2SW1OaVpEZzNOREptTFRrM01HUXRORE00WVMwNE1UQXlMV1kwTUdOak56TTNZMkV4WmlJc0luVnpaWEpwYm1adklqcDdJbWh2YzNRaU9tNTFiR3dzSW1Oc2FXVnVkRWxrSWpvaVkzVnpkRzl0WlhJaUxDSjBaVzVoYm5SSlpDSTZJakl3Wm1VMFpqY3hMVEZoTXpjdE5HRmtaaTFpTWpJMExXUmlaamszTlRJek9UTTFPU0lzSW5SbGJtRnVkRTVoYldVaU9pSmtaV1poZFd4MElpd2lkWE5sY2tsa0lqb2lZV1kyTVRVeVl6WXRaR1poWmkwMFlXWTFMV0UwT0dJdE1Ua3dOVEpsTW1Nell6UXpJaXdpZFhObGNtNWhiV1VpT2lKMFpYTjBNREEzSWl3aVlXUnRhVzRpT21aaGJITmxMQ0oxYzJWeVZIbHdaU0k2SWtOMWMzUnZiV1Z5SW4wc0ltTnNhV1Z1ZEY5cFpDSTZJbU4xYzNSdmJXVnlJbjAuYjBnYks1dVBSeHB3c3hFR0ZEUkRzRkV2MlBJLXc2MkpPeEdobnllQk9hZXEyNHN0aldDbXkzZ1VJejFteUxPZ2FYZ0Y1b2FZU0Qzb0tyRDZUTWRvT0dIYXF0MnM5THJKV3dTNmI0bU5MQlgwcmVTNnRpSjBfb1lOeU55ZzY0eEJidlpJWHBSelR0QUNBZjF2UENXLW5GSHY4NGFGVmJ4ZEl4bzh2U2tQYzlaeVNVd0lQOG15YmxneDZiVTZiVzlHM1BxUDMyT3M1XzM2V0s5RW44b3c0MkI2cDNMTGNNMnZzb2Ffa3lBOFJzZUdqbklraHB4WEdUSTlhSVh4NU9JcVZ3TjBEWTlld0p2VXVjLUJhSWZxTS00QTNKaDlEcTFwY25UbmF3RTZabTl2VGJzRjRaVUJjeFBDRWFoYWN0V01fc2IyRWdnUndJamY2QWhVSUZmLXVnIiwidXNlcmluZm8iOnsiaG9zdCI6bnVsbCwiY2xpZW50SWQiOiJjdXN0b21lciIsInRlbmFudElkIjoiMjBmZTRmNzEtMWEzNy00YWRmLWIyMjQtZGJmOTc1MjM5MzU5IiwidGVuYW50TmFtZSI6ImRlZmF1bHQiLCJ1c2VySWQiOiJhZjYxNTJjNi1kZmFmLTRhZjUtYTQ4Yi0xOTA1MmUyYzNjNDMiLCJ1c2VybmFtZSI6InRlc3QwMDciLCJhZG1pbiI6ZmFsc2UsInVzZXJUeXBlIjoiQ3VzdG9tZXIifSwiY2xpZW50X2lkIjoiY3VzdG9tZXIifQ.BC1KkBcKBIyiBU2zReVUQaYmowMeMu1MRrhQTsyWySofVa4r3qATPOqGUAP3xCFJpuw6WKl3vkYMiEHOlsIHY3c1CQAzyq-ne1x-ZsMrQCYMBu9rhL5WdMUVWq-2UFwpYVWlBpX-gLUr6auMdqQCSd0YpH87TdjykKjLcW73mT908tmwipIaP742jKYxhOOyaZeuq42AtI_F8BpOIbmL6BXY9aeB9eMCo1biGxBxja2YXeR3qC3j6DPyfjfPfBCfYcHBSduzAPCcRVtwQL0KGfVHYG75kDeyR_7OoiG0jYTXRiVBGXtiXD6ZvY1yMOaPObbJEBKkXo_eH0Dp4yaZcA", "Accept": "application/json"}
    #body = {"dataServiceId": "700735577841991680", "accessKey": "1bb2f62d-25e2-470c-b802-4b289b847e1a", "offset": 0, "size": 10000}
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

#test_inset()
#test_blob()
#test_blob_decode64()

from pykafka import KafkaClient
import json

class operateKafka:
    def __init__(self):
        myhosts = "192.168.1.82:9094"
        client = KafkaClient(hosts=myhosts)                         # 可接受多个Client这是重点
        self.topic = client.topics['test_kafka06291']    # 选择一个topic

    """
    function:send message to kafka
    dataList:数据格式是list，每个元素是tuple格式
    author:01376233
    """
    def sendMessage(self, dataList):
        with self.topic.get_sync_producer() as producer:
            for data in dataList:
                # transform tuple to dic
                #mydict = {'t1': data[0], 't2': data[1], 't3': data[2], 't4': data[3], 't5': data[4]}
                #python_to_json = json.dumps(data, ensure_ascii=False)
                for i in range(len(data)):
                #print( data[i], end="")
                 producer.produce((str(data[i])).encode())


# myopKafka = operateKafka()
#
# #print(t1)
# while 1:
#     time.sleep(10)
#     t1 = timestamp_now()
#     df = [(t1 , 1, 'li', 'a3', 58), (t1 , 2, 'huang', 'b3', 68), (t1 , 3, 'liu', 'c3', 98), (t1 , 4, 'zhao', 'a4', 81), (t1 , 5, 'bai', 'b5', 78), (t1, 6, 'qin', 'c6', 89), (t1, 7, 'zhang', 'a7', 87), (t1, 8, 'guo', 'b8', 86), (t1, 9, 'xiang', 'c9', 88)]
#     #myopKafka.sendMessage(df)