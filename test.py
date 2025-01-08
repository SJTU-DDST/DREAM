import os
import subprocess
import shutil

# 定义 num_cli 列表
num_cli_list = [1, 8, 16, 32]

# 定义目标目录
base_dir = "data/MyHash"

# 确保目标目录存在
os.makedirs(base_dir, exist_ok=True)

for num_cli in num_cli_list:
    # 构建命令
    command = f"../ser_cli.sh server {num_cli} 1 1"
    
    # 执行命令
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

print("All done.")

# TODO: 提取数据，支持切换hash类型