import json
from websocket import create_connection
from basic_info.setting import ws_url, exec_data
from util.logs import Logger

def execute_shell(url, data):
    ws = create_connection(url)
    ws.send(json.dumps(data))
    # 获取连接状态
    Logger().get_log().info( "获取连接状态：%s"% ws.getstatus())
    # 获取返回结果
    result = ws.recv()
    print("接收结果：", result)
    # 关闭连接
    #ws.close()


execute_shell(ws_url, exec_data)