# -*- coding: utf-8 -*-
from time import *
import paramiko
import os
from basic_info.setting import log

class Linux(object):
    def __init__(self, ip, username, password, timeout=30):
        self.ip = ip
        self.username = username
        self.password = password
        self.timeout = timeout
        self.t = ''
        self.chan = ''
        self.try_times = 3

    def connect(self):
        while True:
            try:
                log.info("----开始连接服务器----")
                self.t = paramiko.Transport(sock=(self.ip, 22))
                self.t.connect(username=self.username, password=self.password)
                self.chan = self.t.open_session()
                self.chan.settimeout(self.timeout)
                self.chan.get_pty()
                self.chan.invoke_shell()
                log.info("连接%s成功" % self.ip)
                log.info("网络数据解码:%s "% self.chan.recv(65535).decode('utf-8'))
                return
            except Exception as e1:
                if self.try_times != 0:
                    log.error("连接%s失败，进行重试%s" % (self.ip,e1))
                    self.try_times -= 1
                else:
                    log.error("重试3次失败，结束程序")
                    exit(1)


    def close(self):
        log.info("----关闭服务器连接----")
        self.chan.close()
        self.t.close()


    def send(self, cmd):
        cmd += '\r'
        result = ''
        log.info("开始执行命令: %s" %cmd)
        self.chan.send(cmd)
        while True:
            sleep(1)
            ret = self.chan.recv(65535)
            ret = ret.decode('utf-8')
            result += ret
            return result

    def get_size(self,path):
        file_list = os.listdir(path)
        for filename in file_list:
            pathTmp = os.path.join(path,filename)
            if os.path.isdir(pathTmp):
                self.get_size(pathTmp)
            elif os.path.isfile(pathTmp):
               list =[]
               filesize = os.path.getsize(pathTmp)
               print(pathTmp)
               list.append(filesize)
               count =len(open(pathTmp,"r+",encoding='gbk').readlines())
               list.append(count)
               list.append(filename)
               return list



    '''
    发送文件
    @:param upload_files上传文件路径 例如：/tmp/test.py
    @:param upload_path 上传到目标路径 例如：/tmp/test_new.py
    '''
    def upload_file(self,upload_files,upload_path):
        try:
            tran=paramiko.Transport(sock=(self.ip, self.port))
            tran.connect(username=self.username, password=self.password)
            sftp = paramiko.SFTPClient.from_transport(tran)
            result=sftp.put(upload_files, upload_path)
            return True if result else False
        except Exception as ex:
            log.error("异常信息：%s " %ex)
            tran.close()
        finally:
            tran.close()


# 连接正常的情况
# if __name__ == '__main__':
#     host = Linux('192.168.1.55', 'root', 'Inf0refiner') # 传入Ip，用户名，密码
#     host.connect()
#     host.send('cd /data/input/demo/') # 发送一个查看ip的命令
#     host.send('sh createdata2.sh')
#     sleep(5)
#     host.send('cd /data/input/demo/')
#     sleep(2)
#     host.send('mv [demo]* gdemo/')
    #os.system('mv [demo]* gdemo/')
    # def input_cmd(str):
    #     return input(str)
    # tishi_msg="输入命令："
    # while True:
    #     msg=input(tishi_msg)
    #     if msg=="exit":
    #         host.close()
    #         break
    #     else:
    #         res=host.send(msg)
    #         data=res.replace(res.split("\n")[-1],"")
    #         tishi_msg=res.split("\n")[-1]
    #         print(res.split("\n")[-1] + data.strip("\n"))