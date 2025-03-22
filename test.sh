#!/bin/bash
# filepath: /home/congyong/SepHash/run_experiment.sh
# Should be run in the build directory

# 默认参数
experiment_type=${1:-"insert"}
mode=${2:-"run"}  # 可以是 "run", "rerun" 或 "check"

# 配置
num_cli_list=(112)
# num_cli_list=(1 4 8 16 32 56 112 168 224)
# hash_types=("RACE")
# hash_types=("Plush" "SEPHASH" "MYHASH")
hash_types=("MYHASH")

# num_cli_list=(224)
# num_cli_list=(168 112)
# hash_types=("Plush")

experiment_script="../scripts/ser_cli_${experiment_type}.sh"
default_script="../scripts/ser_cli_insert.sh"

# 函数：设置哈希类型
set_hash_type() {
    local hash_type=$1
    local common_h_path="../include/common.h"
    
    # 备份原文件
    # cp "$common_h_path" "${common_h_path}.bak"
    
    # 完全替换HASH_TYPE的整行定义，避免部分匹配问题
    sed -i "s/^#define HASH_TYPE .*/#define HASH_TYPE ${hash_type}/" "$common_h_path"
    
    # 根据哈希类型设置MODIFIED
    if [ "$hash_type" == "MYHASH" ]; then
        sed -i "s/^#define MODIFIED 0/#define MODIFIED 1/" "$common_h_path"
    else
        sed -i "s/^#define MODIFIED 1/#define MODIFIED 0/" "$common_h_path"
    fi
    
    # 验证替换成功
    echo "已设置哈希类型为: ${hash_type}"
    grep "^#define HASH_TYPE" "$common_h_path"
}

# 函数：重置哈希类型为MYHASH
reset_hash_type() {
    set_hash_type "MYHASH"
}

# 函数：设置fp碰撞模式
# 如果是insert实验，和RACE保持一致，设置 READ_FULL_KEY_ON_FP_COLLISION 为 0，即默认插入的键不重复，不读取完整键。
# 后续可以在Slot中加入是否为insert条目的标记，以便在合并时跳过完整键的读取。
# 也可以在插入时检查是否有重复键，如果有则转换为更新操作，但这样需要对RACE的设计进行修改，目前还是默认插入的键不重复。
set_fp_collision_mode() {
    local enable=$1
    local common_h_path="../include/common.h"
    local value=1
    
    if [ "$enable" == "false" ]; then
        value=0
    fi
    
    # 修改设置
    sed -i "s/#define READ_FULL_KEY_ON_FP_COLLISION [0-1]/#define READ_FULL_KEY_ON_FP_COLLISION ${value}/" "$common_h_path"
    echo "设置 READ_FULL_KEY_ON_FP_COLLISION 为 ${value}"
}

# 如果实验类型包含"insert"且不是check模式，设置fp碰撞模式为false
if [[ "$experiment_type" == *"insert"* ]] && [ "$mode" != "check" ]; then
    set_fp_collision_mode false
else
    set_fp_collision_mode true
fi

# 重新运行模式或仅检查模式
if [ "$mode" == "rerun" ] || [ "$mode" == "check" ]; then
    echo "运行模式: ${mode}，检查未完成的实验..."
    need_rerun_count=0
    
    for hash_type in "${hash_types[@]}"; do
        set_hash_type "$hash_type"
        
        for num_cli in "${num_cli_list[@]}"; do
            base_dir="data_${experiment_type}/${hash_type}/${num_cli}"
            
            # 检查目录是否存在
            if [ ! -d "$base_dir" ]; then
                echo "目录不存在: ${base_dir}"
                continue
            fi
            
            # 检查该目录下的所有输出文件
            need_rerun=false
            for filename in "$base_dir"/out*.txt; do
                echo "检查 ${filename}"
                if [ -f "$filename" ] && ! grep -q "Run IOPS:" "$filename"; then
                    need_rerun=true
                    break
                fi
            done
            
            if [ "$need_rerun" == "true" ]; then
                ((need_rerun_count++))
                echo "需要重新运行: ${hash_type}, num_cli=${num_cli}"
                
                # 如果是check模式，只打印需要重新运行的实验，不执行
                if [ "$mode" == "check" ]; then
                    continue
                fi
                
                # 确保实验脚本正确
                if [ -f "../ser_cli.sh" ]; then
                    rm "../ser_cli.sh"
                fi
                cp "$experiment_script" "../ser_cli.sh"
                
                # 计算需要的机器数量和每台机器的客户端数量
                num_machines=$(( (num_cli + 55) / 56 ))
                clients_per_machine=$(( (num_cli + num_machines - 1) / num_machines ))
                
                # 构建命令
                command="../ser_cli.sh server ${clients_per_machine} 1 ${num_machines}"
                echo "实验: ${experiment_type}, 哈希: ${hash_type}, 线程: ${num_cli}, 命令: ${command}"
                
                # 确保使用正确的库路径
                if [ -d "/usr/local/gcc-14.2.0/lib64" ]; then
                    export LD_LIBRARY_PATH="/usr/local/gcc-14.2.0/lib64:$LD_LIBRARY_PATH"
                elif [ -d "/usr/local/lib64" ]; then
                    export LD_LIBRARY_PATH="/usr/local/lib64:$LD_LIBRARY_PATH"
                fi
                
                # 执行命令
                eval "$command"
                result=$?
                
                # 处理结果
                if [ $result -eq 0 ]; then
                    for out_file in out*.txt; do
                        if [ -f "$out_file" ]; then
                            # 删除旧文件并移动新文件
                            old_file="${base_dir}/${out_file}"
                            if [ -f "$old_file" ]; then
                                rm "$old_file"
                            fi
                            mv "$out_file" "$base_dir/"
                        fi
                    done
                    echo "成功重新运行 ${hash_type}, num_cli=${num_cli}"
                else
                    echo "重新运行失败: ${hash_type}, num_cli=${num_cli}"
                fi
            fi
        done
        
        echo "重新运行检查 ${hash_type} 完成"
        reset_hash_type
    done
    
    if [ "$mode" == "check" ]; then
        echo "检查完成：总共有 ${need_rerun_count} 个实验需要重新运行"
        
        # 恢复默认脚本
        if [ -f "../ser_cli.sh" ]; then
            rm "../ser_cli.sh"
        fi
        cp "$default_script" "../ser_cli.sh"
        
        # 如果实验类型包含"insert"，恢复fp碰撞模式为true
        if [[ "$experiment_type" == *"insert"* ]]; then
            set_fp_collision_mode true
        fi
        
        exit 0
    fi
fi

# 正常运行模式
for hash_type in "${hash_types[@]}"; do
    base_dir="data_${experiment_type}/${hash_type}"
    set_hash_type "$hash_type"
    
    # 确保目标目录存在
    mkdir -p "$base_dir"
    
    # 切换到对应实验脚本
    if [ -f "../ser_cli.sh" ]; then
        rm "../ser_cli.sh"
    fi
    cp "$experiment_script" "../ser_cli.sh"
    
    for num_cli in "${num_cli_list[@]}"; do
        # 计算需要的机器数量和每台机器的客户端数量
        num_machines=$(( (num_cli + 55) / 56 ))
        clients_per_machine=$(( (num_cli + num_machines - 1) / num_machines ))
        
        # 构建命令
        command="../ser_cli.sh server ${clients_per_machine} 1 ${num_machines}"
        
        # 确保使用正确的库路径
        if [ -d "/usr/local/gcc-14.2.0/lib64" ]; then
            export LD_LIBRARY_PATH="/usr/local/gcc-14.2.0/lib64:$LD_LIBRARY_PATH"
        elif [ -d "/usr/local/lib64" ]; then
            export LD_LIBRARY_PATH="/usr/local/lib64:$LD_LIBRARY_PATH"
        fi
        
        # 执行命令
        echo "实验: ${experiment_type}, 哈希: ${hash_type}, 线程: ${num_cli}, 命令: ${command}"
        eval "$command"
        result=$?
        
        # 检查命令是否成功执行
        if [ $result -ne 0 ]; then
            echo "命令执行失败，返回码: $result"
            # continue
        fi
        
        # 定义num_cli目录
        num_cli_dir="${base_dir}/${num_cli}"
        
        # 确保num_cli目录存在
        mkdir -p "$num_cli_dir"
        
        # 移动生成的out*.txt文件到num_cli目录
        for filename in out*.txt; do
            if [ -f "$filename" ]; then
                mv "$filename" "${num_cli_dir}/"
            fi
        done
        
        echo "处理完成 num_cli=${num_cli}"
    done
    
    echo "运行 ${hash_type} 完成"
    reset_hash_type
done

# 实验结束后恢复默认脚本
if [ -f "../ser_cli.sh" ]; then
    rm "../ser_cli.sh"
fi
cp "$default_script" "../ser_cli.sh"

# 如果实验类型包含"insert"，恢复fp碰撞模式为true
if [[ "$experiment_type" == *"insert"* ]]; then
    set_fp_collision_mode true
fi