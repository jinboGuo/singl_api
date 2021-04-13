import logging
from logging import handlers
import time
import os

PATH = lambda p:  os.path.abspath(
    os.path.join(os.path.dirname(__file__), p))

class Logger(object):

    #日志级别关系映射
    level_relation = {
        'debug':logging.DEBUG,
        'info':logging.INFO,
        'warning':logging.WARNING,
        'error':logging.ERROR,
        'crit':logging.CRITICAL
    }

    def __init__(self):
        global resultPath, log
        log= 'AutoTest' + '_' + time.strftime('%Y%m%d', time.localtime()) + '.log'
        resultPath = PATH("../Reports/Log")
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)


    def get_log(self):
        self.delDir(resultPath)
        fmt='%(asctime)s - %(funcName)s[line:%(lineno)d] - %(levelname)s: %(message)s'
        filename = os.path.join(resultPath, log)
        logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)#设置日志格式
        logger.setLevel(self.level_relation.get('info'))#设置日志级别
        if not logger.handlers:
            sh = logging.StreamHandler()#往屏幕上输出
            sh.setFormatter(format_str) #设置屏幕上显示的格式
            th = handlers.TimedRotatingFileHandler(filename=filename,when='D',interval=1, backupCount=1,encoding='utf-8')#往文件里写入#指定间隔时间自动生成文件的处理器
            #实例化TimedRotatingFileHandler
            #interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
            # S 秒
            # M 分
            # H 小时、
            # D 天、
            # W 每星期（interval==0时代表星期一）
            # midnight 每天凌晨
            th.setFormatter(format_str)#设置文件里写入的格式
            logger.addHandler(sh) #把对象加到logger里
            logger.addHandler(th)
        return  logger

    @staticmethod
    def delDir(resultPath,t=24*60*60*2):
    #获取文件夹下所有文件和文件夹
        files = os.listdir(resultPath)
        for file in files:
            filePath = resultPath + "/" + file
            #判断是否是文件
            if os.path.isfile(filePath):
                #最后一次修改的时间
                last = int(os.stat(filePath).st_mtime)
                #上一次访问的时间
                #last = int(os.stat(filePath).st_atime)
                #当前时间
                now = int(time.time())
                #删除过期文件
                if (now - last >= t):
                    os.remove(filePath)
                    Logger().get_log().info(" %s was removed!" % filePath)
            elif os.path.isdir(filePath):
                #如果是文件夹，继续遍历删除
                resultPath.delDir(filePath, t)
                #如果是空文件夹，删除空文件夹
                if not os.listdir(filePath):
                    os.rmdir(filePath)
                    Logger().get_log().info(" %s was removed!" % filePath)