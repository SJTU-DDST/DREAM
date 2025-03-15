import os
import subprocess
import shutil
import re
import sys

def set_hash_type(hash_type):
    # 修改 ser_cli.cc 文件中的 hash 类型
    common_h_path = "../include/common.h"
    with open(common_h_path, "r") as file:
        data = file.read()
        data = re.sub(r"#define HASH_TYPE \w+", f"#define HASH_TYPE {hash_type}", data)
        if hash_type == "MYHASH":
            data = data.replace("#define MODIFIED 0", "#define MODIFIED 1")
        else:
            data = data.replace("#define MODIFIED 1", "#define MODIFIED 0")
    with open(common_h_path, "w") as file:
        file.write(data)

def reset_hash_type():
    set_hash_type("MYHASH")

# Should be run in the build directory
# 获取实验类型参数，如果未提供则默认为"insert"
experiment_type = sys.argv[1] if len(sys.argv) > 1 else "insert"

# 定义 num_cli 列表
# num_cli_list = [1, 2, 4, 8, 16, 24, 32, 40, 48]
# hash_types = ["MYHASH", "SEPHASH", "Plush", "RACE"]
num_cli_list = [16]
hash_types = ["MYHASH"]

# 设置脚本路径
experiment_script = f"../scripts/ser_cli_{experiment_type}.sh"
default_script = "../scripts/ser_cli_insert.sh"

for hash_type in hash_types:
    base_dir = f"data_{experiment_type}/{hash_type}"
    set_hash_type(hash_type)

    # 确保目标目录存在
    os.makedirs(base_dir, exist_ok=True)
    
    # 切换到对应实验脚本 - 使用复制而不是符号链接
    if os.path.exists("../ser_cli.sh"):
        os.remove("../ser_cli.sh")
    shutil.copy2(experiment_script, "../ser_cli.sh")

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
    reset_hash_type()

# 实验结束后恢复默认脚本 - 使用复制而不是符号链接
if os.path.exists("../ser_cli.sh"):
    os.remove("../ser_cli.sh")
shutil.copy2(default_script, "../ser_cli.sh")

