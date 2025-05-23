# -*- coding: utf-8 -*-

import threading
from fabric import Connection
import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument("num_servers", type=int, help="Number of servers to run the command on", default=1)
parser.add_argument("command_type", choices=["server", "client"], help="Command type to run", default="server")
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
    {'host': '192.168.98.72', 'user': 'congyong', 'password': '1111'},
    {'host': '192.168.98.74', 'user': 'congyong', 'password': '1111'},
    {'host': '192.168.98.73', 'user': 'congyong', 'password': '1111'},
    {'host': '192.168.98.71', 'user': 'congyong', 'password': '1111'},
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

    print (f'RUN ./ser_cli.sh server {i} on {conn.host}')
    result = conn.run(f'./ser_cli.sh server {i}') 

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

# Execute the task on all servers
threads = []
for i in range(num_servers):
    if command_type == "server":
        thread = threading.Thread(target=server_command, args=(i,))
    elif command_type == "client":
        thread = threading.Thread(target=client_command, args=(i,))
    thread.start()
    threads.append(thread)

# Wait for all threads to complete
for thread in threads:
    thread.join()
