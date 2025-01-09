import os
import subprocess
import shutil

def set_hash_type(hash_type, old_hash_type = "SEPHASH"):
    # 修改 ser_cli.cc 文件中的 hash 类型
    if hash_type != "MYHASH":
        ser_cli_cc_path = "../test/ser_cli.cc"
        with open(ser_cli_cc_path, "r") as file:
            data = file.read()
            data = data.replace(f"using ClientType = {old_hash_type}::Client;", f"using ClientType = {hash_type}::Client;")
            data = data.replace(f"using ServerType = {old_hash_type}::Server;", f"using ServerType = {hash_type}::Server;")
            data = data.replace(f"using Slice = {old_hash_type}::Slice;", f"using Slice = {hash_type}::Slice;")
        with open(ser_cli_cc_path, "w") as file:
            file.write(data)

    common_h_path = "../include/common.h"
    with open(common_h_path, "r") as file:
        data = file.read()
        if hash_type == "MYHASH":
            data = data.replace("#define MODIFIED 0", "#define MODIFIED 1")
        else:
            data = data.replace("#define MODIFIED 1", "#define MODIFIED 0")

    with open(common_h_path, "w") as file:
        file.write(data)

def reset_hash_type(old_hash_type):
    if old_hash_type != "MYHASH":
        set_hash_type("SEPHASH", old_hash_type)
    set_hash_type("MYHASH")

# 定义 num_cli 列表
num_cli_list = [1, 2, 4, 8, 16, 32]
hash_types = ["SEPHASH", "MYHASH", "Plush"]
# hash_types = ["RACE"]
# hash_types = ["CLEVEL"]

for hash_type in hash_types:
    base_dir = f"data/{hash_type}"
    set_hash_type(hash_type)

    # 确保目标目录存在
    os.makedirs(base_dir, exist_ok=True)

    for num_cli in num_cli_list:
        # 构建命令
        command = f"../ser_cli.sh server {num_cli} 1 1"
        
        # 执行命令
        print(f"Running command: {command}")
        result = subprocess.run(command, shell=True)
        
        # 检查命令是否成功执行
        if result.returncode != 0:
            print(f"Command failed with return code {result.returncode}")
            continue
        
        # 定义 num_cli 目录
        num_cli_dir = os.path.join(base_dir, str(num_cli))
        
        # 确保 num_cli 目录存在
        os.makedirs(num_cli_dir, exist_ok=True)
        
        # 移动生成的 out*.txt 文件到 num_cli 目录
        for filename in os.listdir("."):
            if filename.startswith("out") and filename.endswith(".txt"):
                shutil.move(filename, os.path.join(num_cli_dir, filename))

        print(f"Processed num_cli={num_cli}")

    print(f"run {hash_type} done")
    # TODO: 提取数据，修复RACE
    reset_hash_type(hash_type)

