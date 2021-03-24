import logging
import logging.handlers
import os
import threading
import time


def logger_info(msg):
    LEVELS = {'NOTSET': logging.NOTSET,
              'DEBUG': logging.DEBUG,
              'INFO': logging.INFO,
              'WARNING': logging.WARNING,
              'ERROR': logging.ERROR,
              'CRITICAL': logging.CRITICAL}


    # create logs file folder
    logs_dir = os.path.join(os.path.curdir, "logs")
    if os.path.exists(logs_dir) and os.path.isdir(logs_dir):
        pass
    else:
        os.mkdir(logs_dir)

    # define a rotating file handler
    rotatingFileHandler = logging.handlers.RotatingFileHandler(filename="logs/outlog.txt",

                                                               maxBytes=1024 * 1024 * 50,

                                                               backupCount=5)

    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")

    rotatingFileHandler.setFormatter(formatter)

    logging.getLogger("").addHandler(rotatingFileHandler)

    # define a handler whitch writes messages to sys

    console = logging.StreamHandler()

    console.setLevel(logging.NOTSET)

    # set a format which is simple for console use

    formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")

    # tell the handler to use this format

    console.setFormatter(formatter)

    # add the handler to the root logger

    logging.getLogger("").addHandler(console)

    # set initial log level
    logger = logging.getLogger("")
    logger.setLevel(logging.NOTSET)

PATH = lambda p:  os.path.abspath(
    os.path.join(os.path.dirname(__file__), p))

class Log:
    def __init__(self):
        global resultPath, log_path
        print(PATH)
        resultPath = PATH("logs")
        # create result file if it doesn't exist
        if not os.path.exists(resultPath):
            os.mkdir(resultPath)
        # defined test result file name by localtime
        log_path = resultPath + '\\'+ time.strftime('%Y%m%d_%H%M%S', time.localtime())
        # create test result file if it doesn't exist
        if not os.path.exists(log_path):
            os.mkdir(log_path)
        # defined logger
        self.logger = logging.getLogger()
        # defined log level
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler(log_path+"/output.log", encoding='utf-8')
        # defined formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # defined formatter
        handler.setFormatter(formatter)
        # add handler
        self.logger.addHandler(handler)



class myLog:
    """
    This class is used to get log
    """

    log = None
    mutex = threading.Lock()

    def __init__(self):
        pass

    @staticmethod
    def getLog():
        if myLog.log is None:
            myLog.mutex.acquire()
            myLog.log = Log()
            myLog.mutex.release()

        return myLog.log


#
# import logging
# logger = logging.getLogger(__name__)

if __name__ == "__main__":
    msg = "this is just a test"
    myLog().getLog().logger.info(msg)