# -*- coding: utf-8 -*-
from time import *
import paramiko
import os
from util.logs import Logger
# 定义一个类，表示一台远端linux主机
class Linux(object):
    # 通过IP, 用户名，密码，超时时间初始化一个远程Linux主机
    def __init__(self, ip, username, password, timeout=30):
        self.ip = ip
        self.username = username
        self.password = password
        self.timeout = timeout
        # transport和chanel
        self.t = ''
        self.chan = ''
        # 链接失败的重试次数
        self.try_times = 3

    # 调用该方法连接远程主机
    def connect(self):
        while True:
            # 连接过程中可能会抛出异常，比如网络不通、链接超时
            try:
                Logger().get_log().info("----开始连接服务器----")
                self.t = paramiko.Transport(sock=(self.ip, 22))
                self.t.connect(username=self.username, password=self.password)
                self.chan = self.t.open_session()
                self.chan.settimeout(self.timeout)
                self.chan.get_pty()
                self.chan.invoke_shell()
                # 如果没有抛出异常说明连接成功，直接返回
                Logger().get_log().info("连接%s成功" % self.ip)
                # 接收到的网络数据解码为str
                Logger().get_log().info("网络数据解码:%s "% self.chan.recv(65535).decode('utf-8'))
                return
            # 这里不对可能的异常如socket.error, socket.timeout细化，直接一网打尽
            except Exception as e1:
                if self.try_times != 0:
                    Logger().get_log().error("连接%s失败，进行重试" % self.ip)
                    self.try_times -= 1
                else:
                    Logger().get_log().error("重试3次失败，结束程序")
                    exit(1)

    # 断开连接
    def close(self):
        Logger().get_log().info("----关闭服务器连接----")
        self.chan.close()
        self.t.close()

    # 发送要执行的命令
    def send(self, cmd):
        cmd += '\r'
        result = ''
        # 发送要执行的命令
        Logger().get_log().info("开始执行命令: %s" %cmd)
        self.chan.send(cmd)
        # 回显很长的命令可能执行较久，通过循环分批次取回回显,执行成功返回true,失败返回false
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
            Logger().get_log().error("异常信息：%s " %ex)
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