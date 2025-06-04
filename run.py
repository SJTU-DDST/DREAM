# -*- coding: utf-8 -*-

import threading
from fabric import Connection
import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument("num_servers", type=int, help="Number of servers to run the command on", default=1)
parser.add_argument("command_type", choices=["server", "client", "ser_cli"], help="Command type to run", default="server")
parser.add_argument('cli_num', type=int, help='client数量', default=8)
parser.add_argument('coro_num', type=int, help='coro数量', default=1)

args = parser.parse_args()

# 然后将 num_servers 用作运行命令的服务器数量
num_servers = args.num_servers
command_type = args.command_type
cli_num = args.cli_num
coro_num = args.coro_num

# Define the connection information for each server
servers = [
    # {'host': '192.168.98.70', 'user': 'congyong', 'password': '1111'}, # server
    {'host': '192.168.98.73', 'user': 'congyong', 'password': '1111'},
    {'host': '192.168.98.72', 'user': 'congyong', 'password': '1111'},
    {'host': '192.168.98.71', 'user': 'congyong', 'password': '1111'},
    {'host': '192.168.98.74', 'user': 'congyong', 'password': '1111'},
    # {'host': '192.168.98.75', 'user': 'congyong', 'password': '1111'},
    # {'host': '192.168.1.52', 'user': 'xxx', 'password': 'xxx'},
    # {'host': '192.168.1.53', 'user': 'xxx', 'password': 'xxx'},
    # {'host': '192.168.1.11', 'user': 'xxx', 'password': 'xxx'},
    # {'host': '192.168.1.12', 'user': 'xxx', 'password': 'xxx'},
    # {'host': '192.168.1.13', 'user': 'xxx', 'password': 'xxx'},
    # {'host': '192.168.1.14', 'user': 'xxx', 'password': 'xxx'},
    # {'host': '192.168.1.10', 'user': 'xxx', 'password': 'xxx'},
    # Add more servers if needed
]

# Create a list of Connection objects for each server
connections = [Connection(host=server['host'], user=server['user'], connect_kwargs={"password": server['password']}) for server in servers]

# Define a task to run on all servers
def server_command(i):
    conn = connections[i]
    print(f"server: {conn.host}")
    # conn.run('killall multi_rdma', warn=True)

    # print (f'RUN ./ser_cli.sh server {i} on {conn.host}')
    # result = conn.run(f'./ser_cli.sh server {i}')
    print (f'RUN cd /home/congyong/ && ./ser_cli_var_kv --server --gid_idx 1 --max_coro 256 --cq_size 64 --mem_size 61268055040 on {conn.host}') 
    result = conn.run(f'cd /home/congyong/ && screen -d -m timeout 30m ./ser_cli_var_kv --server --gid_idx 1 --max_coro 256 --cq_size 64 --mem_size 61268055040', hide=True, warn=True)

def client_command(i):
    conn = connections[i]
    print(f"client: {conn.host}")
    # conn.run('killall ser_cli_var_kv', warn=True)
    # conn.run('free -h', warn=True)
    # print(f'RUN rm -f insert*.txt search*.txt out.txt core || true on {conn.host}')
    result = conn.run(f'rm -f insert*.txt search*.txt out.txt core || true')
    
    # Wait for i seconds before running the script
    # print(f'Waiting for {i} seconds before running on {conn.host}...')
    time.sleep(i)
    
    print(f'RUN timeout 30m ./run.sh {i} {cli_num} {coro_num} {num_servers} on {conn.host}')
    result = conn.run(f'timeout 30m ./run.sh {i} {cli_num} {coro_num} {num_servers}')
    
    if result.return_code == 124:
        print(f'Command ./run.sh {i} {cli_num} {coro_num} {num_servers} on {conn.host} timed out.')
    else:
        print(f'Command ./run.sh {i} {cli_num} {coro_num} {num_servers} on {conn.host} completed with return code {result.return_code}.')

def client_command_on_70():
    conn = Connection(host='192.168.98.70', user='congyong', connect_kwargs={"password": "1111"})
    print(f"client: 192.168.98.70")
    conn.run(f'rm -f insert*.txt search*.txt out.txt core || true')
    time.sleep(0)
    print(f'RUN timeout 30m ./run.sh 0 {cli_num} {coro_num} {num_servers} on 192.168.98.70')
    result = conn.run(f'timeout 30m ./run.sh 0 {cli_num} {coro_num} {num_servers}')
    if result.return_code == 124:
        print(f'Command ./run.sh 0 {cli_num} {coro_num} {num_servers} on 192.168.98.70 timed out.')
    else:
        print(f'Command ./run.sh 0 {cli_num} {coro_num} {num_servers} on 192.168.98.70 completed with return code {result.return_code}.')

def ser_cli_mode():
    # 1. 启动所有server（后台），跳过i=0，因为我们在i=0的服务器上运行client
    threads = []
    for i in range(1, len(servers)):
        def start_server(i):
            conn = connections[i]
            print(f"[ser_cli] start server: {conn.host}")
            conn.run(f'cd /home/congyong/ && screen -d -m bash -c "timeout 5m ./ser_cli_var_kv --server --gid_idx 1 --max_coro 256 --cq_size 64 --mem_size 61268055040 > /home/congyong/ser_cli.log 2>&1"', hide=True, warn=True)
        t = threading.Thread(target=start_server, args=(i,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    print("[ser_cli] All servers started.")
    time.sleep(30)  # 等待服务器启动
    # 2. 启动所有client（阻塞，等待全部返回）
    threads = []
    for i in range(num_servers):
        t = threading.Thread(target=client_command, args=(i,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    print("[ser_cli] All clients finished.")
    # # 2. 只在192.168.98.70上运行client
    # print(f"[ser_cli] run client on: 192.168.98.70")
    # client_command_on_70()
    # print("[ser_cli] Client finished.")
    # 3. kill所有server
    for i in range(len(servers)):
        conn = connections[i]
        print(f"[ser_cli] kill server: {conn.host}")
        conn.run('killall ser_cli_var_kv', warn=True)
    print("[ser_cli] All servers killed.")

# Execute the task on all servers
if command_type == "ser_cli":
    ser_cli_mode()
else:
    threads = []
    for i in range(num_servers):
        if command_type == "server":
            thread = threading.Thread(target=server_command, args=(i,))
        elif command_type == "client":
            thread = threading.Thread(target=client_command, args=(i,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
