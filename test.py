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

# 如果是insert实验，和RACE保持一致，设置 READ_FULL_KEY_ON_FP_COLLISION 为 0，即默认插入的键不重复，不读取完整键。
# 后续可以在Slot中加入是否为insert条目的标记，以便在合并时跳过完整键的读取。
# 也可以在插入时检查是否有重复键，如果有则转换为更新操作，但这样需要对RACE的设计进行修改，目前还是默认插入的键不重复。
def set_fp_collision_mode(enable):
    # 修改 common.h 文件中的 READ_FULL_KEY_ON_FP_COLLISION 设置
    common_h_path = "../include/common.h"
    value = "1" if enable else "0"
    with open(common_h_path, "r") as file:
        data = file.read()
        data = re.sub(r"#define READ_FULL_KEY_ON_FP_COLLISION \d", f"#define READ_FULL_KEY_ON_FP_COLLISION {value}", data)
    with open(common_h_path, "w") as file:
        file.write(data)
    print(f"设置 READ_FULL_KEY_ON_FP_COLLISION 为 {value}")

# Should be run in the build directory
# 获取实验类型参数，如果未提供则默认为"insert"
experiment_type = sys.argv[1] if len(sys.argv) > 1 else "insert"

# 检查是否为重新运行模式或只检查模式
rerun_mode = len(sys.argv) > 2 and sys.argv[2] == "rerun"
check_only_mode = len(sys.argv) > 2 and sys.argv[2] == "check"

# 如果实验类型含有"insert"，设置 READ_FULL_KEY_ON_FP_COLLISION 为 0
if "insert" in experiment_type and not check_only_mode:
    set_fp_collision_mode(False)

# 定义 num_cli 列表
num_cli_list = [1, 2, 4, 8, 16, 24, 32, 40, 48]
hash_types = ["MYHASH", "SEPHASH", "Plush", "RACE"]
# num_cli_list = [64]
# hash_types = ["MYHASH"]

# 设置脚本路径
experiment_script = f"../scripts/ser_cli_{experiment_type}.sh"
default_script = "../scripts/ser_cli_insert.sh"

if rerun_mode or check_only_mode:
    print(f"{'Checking' if check_only_mode else 'Running in rerun mode'}, checking for incomplete experiments...")
    need_rerun_count = 0
    for hash_type in hash_types:
        set_hash_type(hash_type)
        for num_cli in num_cli_list:
            base_dir = f"data_{experiment_type}/{hash_type}/{num_cli}"
            
            # 检查目录是否存在
            if not os.path.exists(base_dir):
                print(f"目录不存在: {base_dir}")
                continue
            
            # 检查该目录下的所有输出文件
            need_rerun = False
            for filename in os.listdir(base_dir):
                print(f"Checking {base_dir}/{filename}")
                if filename.startswith("out") and filename.endswith(".txt"):
                    file_path = os.path.join(base_dir, filename)
                    with open(file_path, "r") as f:
                        content = f.read()
                        if "Run IOPS:" not in content:
                            need_rerun = True
                            break
            
            if need_rerun:
                need_rerun_count += 1
                print(f"需要重新运行: {hash_type}, num_cli={num_cli}")
                
                # 如果是check模式，只打印需要重新运行的实验，不执行
                if check_only_mode:
                    continue
                
                # 确保实验脚本正确
                if os.path.exists("../ser_cli.sh"):
                    os.remove("../ser_cli.sh")
                shutil.copy2(experiment_script, "../ser_cli.sh")
                
                # 运行实验
                # Calculate number of machines needed (max 56 clients per machine)
                num_machines = (num_cli + 55) // 56  # Ceiling division
                clients_per_machine = (num_cli + num_machines - 1) // num_machines  # Distribute evenly
                
                # Build command with appropriate distribution
                command = f"../ser_cli.sh server {clients_per_machine} 1 {num_machines}"
                print(f"Experiment: {experiment_type}, Hash: {hash_type}, Threads: {num_cli}, Command: {command}")
                result = subprocess.run(command, shell=True)
                # ../ser_cli.sh server 36 1 2 可以但是test.py不行
                # 处理结果
                if result.returncode == 0:
                    for filename in os.listdir("."):
                        if filename.startswith("out") and filename.endswith(".txt"):
                            # 删除旧文件并移动新文件
                            old_file = os.path.join(base_dir, filename)
                            if os.path.exists(old_file):
                                os.remove(old_file)
                            shutil.move(filename, base_dir)
                    print(f"Successfully rerun {hash_type}, num_cli={num_cli}")
                else:
                    print(f"Rerun failed for {hash_type}, num_cli={num_cli}")
        
        print(f"Rerun check for {hash_type} done")
        reset_hash_type()
    
    if check_only_mode:
        print(f"检查完成：总共有 {need_rerun_count} 个实验需要重新运行")
        sys.exit(0)
    
    # 实验结束后恢复默认脚本
    if os.path.exists("../ser_cli.sh"):
        os.remove("../ser_cli.sh")
    shutil.copy2(default_script, "../ser_cli.sh")
    
    # 如果实验类型含有"insert"，恢复 READ_FULL_KEY_ON_FP_COLLISION 为 1
    if "insert" in experiment_type:
        set_fp_collision_mode(True)
    
    sys.exit(0)

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
        # Calculate number of machines needed (max 56 clients per machine)
        num_machines = (num_cli + 55) // 56  # Ceiling division
        clients_per_machine = (num_cli + num_machines - 1) // num_machines  # Distribute evenly
        
        # Build command with appropriate distribution
        command = f"../ser_cli.sh server {clients_per_machine} 1 {num_machines}"
        
        # 执行命令
        print(f"Experiment: {experiment_type}, Hash: {hash_type}, Threads: {num_cli}, Command: {command}")
        result = subprocess.run(command, shell=True)
        
        # 检查命令是否成功执行
        if result.returncode != 0:
            print(f"Command failed with return code {result.returncode}")
            # continue
        
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

# 如果实验类型含有"insert"，恢复 READ_FULL_KEY_ON_FP_COLLISION 为 1
if "insert" in experiment_type:
    set_fp_collision_mode(True)

