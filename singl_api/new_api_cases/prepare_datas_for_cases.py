# coding:utf-8
import json
import os
import time
import requests
from basic_info.get_auth_token import get_headers
from new_api_cases.compass_deal_parameters import deal_random
from util.format_res import dict_res
from basic_info.setting import ms, log
from basic_info.setting import host
from selenium import webdriver
from util.timestamp_13 import data_now

woven_dataflow = os.path.join(os.path.abspath('.'),'attachment\\import_dataflow_steps.woven').replace('\\','/')
multi_sink_steps = os.path.join(os.path.abspath('.'),'attachment\\mutil_sink_storage.woven').replace('\\','/')
multi_rtc_steps = os.path.join(os.path.abspath('.'),'attachment\\multi_rtc_steps.woven').replace('\\','/')


def filesets_data(data):
    try:
        sql="select id from merce_resource_dir where creator='admin' and name='Filesets' and parent_id is NULL and path='Filesets;'"
        fileset_info = ms.ExecuQuery(sql)
        fileset_id=fileset_info[0]["id"]
        cluster_id=cluster_data()
        if "lq_fileset_hdfs_directory" in data:
            new_data = {"name": "lq_fileset_hdfs_directory_随机数", "storage": "HDFS",
                        "storageConfigurations": {"fileType": "DIRECTORY", "path": "/tmp/lisatest/collector_sink",
                                                  "clusterId": cluster_id, "cluster": "cluster1", "host": "",
                                                  "port": "", "username": "", "password": ""},
                        "resource": {"id": fileset_id}, "isShowButton": 'false'}
            deal_random(new_data)
            return new_data
        if "lq_fileset_hdfs_file" in data:
            new_data = {"name": "lq_fileset_hdfs_file_随机数", "storage": "HDFS",
                        "storageConfigurations": {"fileType": "FILE",
                                                  "path": "/tmp/lisatest/hdfs_click/hdfs_source.txt",
                                                  "clusterId": cluster_id, "cluster": "cluster1", "host": "",
                                                  "port": 22, "username": "admin", "password": "123456"},
                        "resource": {"id": fileset_id}, "isShowButton": 'false'}
            deal_random(new_data)
            return new_data
        if "lq_fileset_hdfs_recursive_dir" in data:
            new_data = {"name": "lq_fileset_hdfs_recursive_dir_随机数", "storage": "HDFS",
                        "storageConfigurations": {"fileType": "RECURSIVE_DIR", "path": "/tmp/lisatest/filesets",
                                                  "clusterId": cluster_id, "cluster": "cluster1", "host": "",
                                                  "port": 22, "username": "admin", "password": "123456"},
                        "resource": {"id": fileset_id}, "isShowButton": 'false'}
            deal_random(new_data)
            return new_data
        if "lq_fileset_sftp_directory" in data:
            new_data = {"name": "lq_fileset_sftp_directory_随机数", "storage": "SFTP",
                        "storageConfigurations": {"fileType": "DIRECTORY", "path": "/home/europa/lq_sftp/sftp_sub",
                                                  "clusterId": "", "cluster": "", "host": "192.168.1.84", "port": 22,
                                                  "username": "europa", "password": "europa"},
                        "resource": {"id": fileset_id}, "isShowButton": 'false'}
            deal_random(new_data)
            return new_data
        if "lq_fileset_sftp_recursive_dir" in data:
            new_data = {"name": "lq_fileset_sftp_recursive_dir_随机数", "storage": "SFTP",
                        "storageConfigurations": {"fileType": "RECURSIVE_DIR",
                                                  "path": "/home/europa/lq_sftp/sftp_sub1/", "clusterId": "",
                                                  "cluster": "", "host": "192.168.1.84", "port": 22,
                                                  "username": "europa", "password": "europa"},
                        "resource": {"id": fileset_id}, "isShowButton": 'false'}
            deal_random(new_data)
            return new_data
        if "lq_fileset_ftp_file" in data:
            new_data = {"name": "lq_fileset_ftp_file_随机数", "storage": "FTP",
                        "storageConfigurations": {"fileType": "FILE", "path": "/app/fq_bak/file/txt/lq.txt",
                                                  "clusterId": "7aed23c1-0d17-4613-b317-341df52def48",
                                                  "cluster": "cluster1", "host": "192.168.1.82", "port": "21",
                                                  "username": "merce", "password": "merce@82"},
                        "resource": {"id": fileset_id}, "isShowButton": False}
            deal_random(new_data)
            return new_data
        if "lq_fileset_local_file" in data:
            new_data = {"name": "lq_fileset_local_file_随机数", "storage": "LOCAL",
                        "storageConfigurations": {"fileType": "FILE", "path": "/root/baymax/test/filesearch.txt",
                                                  "clusterId": "", "cluster": "", "host": "192.168.1.149", "port": 22,
                                                  "username": "root", "password": "Inf0refiner"},
                        "resource": {"id": fileset_id}, "isShowButton": 'false'}
            deal_random(new_data)
            return new_data
        elif "lq_fileset_ozone_recursive_dir" in data:
            new_data = {"name": "lq_fileset_ozone_recursive_dir_随机数", "storage": "OZONE",
                        "storageConfigurations": {"fileType": "RECURSIVE_DIR", "path": "/info5/file", "clusterId": "",
                                                  "cluster": "", "host": "", "port": 22, "username": "",
                                                  "password": ""}, "resource": {"id": fileset_id},
                        "isShowButton": False}
            deal_random(new_data)
            return new_data
        elif "lq_fileset_minio_recursive_dir" in data:
            new_data = {"name": "lq_fileset_minio_recursive_dir_随机数", "storage": "MINIO",
                        "storageConfigurations": {"fileType": "RECURSIVE_DIR", "path": "test", "clusterId": "",
                                                  "cluster": "", "host": "192.168.1.81", "port": 9000,
                                                  "username": "minio", "password": "inforefiner"},
                        "resource": {"id": fileset_id}, "isShowButton": False}
            deal_random(new_data)
            return new_data
        else:
            return
    except Exception as e:
        log.error("filesets_data出错{}".format(e))

def filesets_id(data):
    try:
        sql="select id from merce_fileset where name like '%s%%' ORDER BY create_time desc limit 1 "% data
        fileset_info = ms.ExecuQuery(sql)
        return fileset_info[0]["id"]
    except Exception as e:
        log.error("filesets_id出错{}".format(e))


def cluster_data():
    try:
        sql="select id from merce_cluster_info where name='cluster1'"
        cluster_id=ms.ExecuQuery(sql)[0]["id"]
        return cluster_id
    except Exception as e:
        log.error("cluster_data出错{}".format(e))



def get_fs(flag):
    """
    返回导入文件，根据flag判断
    :param flag:
    :return:
    """
    try:
        if flag == 'flag1':
            fs = {"file": open(woven_dataflow, 'rb')}
            return fs
        elif flag == 'flag2':
            fs = {"file": open(multi_sink_steps, 'rb')}
            return fs
        elif flag == 'flag3':
            fs = {"file": open(multi_rtc_steps, 'rb')}
            return fs
        else:
            return log.warn("请输入正确的flag1或者flag2")
    except Exception as e:
        log.error("异常信息：%s" % e)


def get_improt_dataflow(headers, HOST, flag):
    """
    返回导入dataflow文件的请求体参数
    :param headers:
    :param HOST:
    :return:
    """
    url = '%s/api/mis/upload' % HOST
    fs = get_fs(flag)
    headers.pop('Content-Type')
    res = requests.post(url=url, headers=headers, files=fs)
    try:
        cdf_list, cds_list, csm_list = [], [], []
        res = json.loads(res.text)
        for cds_id in res['cds']:
         cds_list.append(cds_id["id"])
        for csm_id in res['csm']:
         csm_list.append(csm_id["id"])
        cdf_list.append(res['cfd'][0]['id'])
        new_data = {"cfd": cdf_list, "cds": cds_list, "csm": csm_list, "tag":[], "uploadDirectory": res["uploadDir"],"overWrite":True,"flowResourceId":"","datasetResourceId":"","schemaResourceId":""}
        return new_data
    except Exception as e:
        log.error("异常信息：%s" % e)