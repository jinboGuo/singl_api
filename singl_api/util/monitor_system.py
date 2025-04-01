import socket
import psutil
import time
import pandas as pd
import matplotlib.pyplot as plt
import schedule

# 获取公网 IP 地址
def get_public_ip():
    # 创建一个套接字
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # 连接到一个远程地址（这里我们使用 Google 的公共 DNS）
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
    except Exception as e:
        print(f"Error: {e}")
        local_ip = None
    finally:
        s.close()
    return local_ip
date = time.strftime("%Y/%m/%d", time.localtime())
hour = time.strftime("%Y%m%d%H", time.localtime())
# 初始化数据列表
data = {
    'Timestamp': [],
    'CPU Usage (%)': [],
    'Memory Usage (%)': [],
    'Disk Read (bytes)': [],
    'Disk Write (bytes)': [],
    'Network Sent (bytes)': [],
    'Network Received (bytes)': []
}

# 监控函数
def monitor_resources():
    # 获取 CPU 使用率
    cpu_usage = psutil.cpu_percent(interval=1)

    # 获取内存使用情况
    memory_info = psutil.virtual_memory()
    memory_usage = memory_info.percent

    # 获取磁盘 I/O 信息
    disk_io = psutil.disk_io_counters()
    disk_read = disk_io.read_bytes
    disk_write = disk_io.write_bytes

    # 获取网络 I/O 信息
    net_io = psutil.net_io_counters()
    net_sent = net_io.bytes_sent
    net_recv = net_io.bytes_recv

    # 收集数据
    timestamp = time.strftime("%H:%M:%S", time.localtime())
    data['Timestamp'].append(timestamp)
    data['CPU Usage (%)'].append(cpu_usage)
    data['Memory Usage (%)'].append(memory_usage)
    data['Disk Read (bytes)'].append(disk_read)
    data['Disk Write (bytes)'].append(disk_write)
    data['Network Sent (bytes)'].append(net_sent)
    data['Network Received (bytes)'].append(net_recv)

    print(f"Data recorded at {timestamp}")

# 生成可视化图表
def generate_report():
    if len(data['Timestamp']) == 0:
        print("No data to generate report.")
        return

    public_ip = get_public_ip()

    # 转换为 DataFrame
    df = pd.DataFrame(data)

    # 生成可视化图表
    plt.figure(figsize=(19, 11))

    # 子图 1: CPU 使用率
    plt.subplot(2, 2, 1)
    plt.plot(df['Timestamp'], df['CPU Usage (%)'], marker='o', color='blue')
    plt.title(f'CPU Usage Over Time (IP: {public_ip})-{date}')
    plt.xlabel('Timestamp')
    plt.ylabel('CPU Usage (%)')
    plt.xticks(rotation=90)
    plt.grid()

    # 子图 2: 内存使用率
    plt.subplot(2, 2, 2)
    plt.plot(df['Timestamp'], df['Memory Usage (%)'], marker='o', color='green')
    plt.title(f'Memory Usage Over Time (IP: {public_ip})-{date}')
    plt.xlabel('Timestamp')
    plt.ylabel('Memory Usage (%)')
    plt.xticks(rotation=90)
    plt.grid()

    # 子图 3: 磁盘 I/O
    plt.subplot(2, 2, 3)
    plt.plot(df['Timestamp'], df['Disk Read (bytes)'], label='Disk Read', color='orange')
    plt.plot(df['Timestamp'], df['Disk Write (bytes)'], label='Disk Write', color='red')
    plt.title(f'Disk I/O (B/s) (IP: {public_ip})-{date}')
    plt.xlabel('Timestamp')
    plt.ylabel('Bytes')
    plt.xticks(rotation=90)
    plt.legend()
    plt.grid()

    # 子图 4: 网络 I/O
    plt.subplot(2, 2, 4)
    plt.plot(df['Timestamp'], df['Network Sent (bytes)'], label='Network Sent', color='purple')
    plt.plot(df['Timestamp'], df['Network Received (bytes)'], label='Network Received', color='brown')
    plt.title(f'Network I/O (B/s) (IP: {public_ip})-{date}')
    plt.xlabel('Timestamp')
    plt.ylabel('Bytes')
    plt.xticks(rotation=90)
    plt.legend()
    plt.grid()

    # 调整布局并保存图表
    plt.tight_layout()
    plt.savefig('resource_monitor_report'+hour+'.png')
    #plt.show()

# 设置定时任务
schedule.every(60).seconds.do(monitor_resources)  # 每60秒监控一次
schedule.every(60).seconds.do(generate_report)     # 每600秒生成一次报告

print("Monitoring resources... Press Ctrl+C to stop.")

# 主循环
try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print("Monitoring stopped.")