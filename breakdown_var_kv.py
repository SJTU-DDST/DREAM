import os
import re
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

hat = ['//', '\\\\', 'xx', '||', '--', '++']
markers = ['H', '^', '>', 'D', 'o', 's', 'p', 'x']
c = np.array([[102, 194, 165], [252, 141, 98], [141, 160, 203], 
        [231, 138, 195], [166,216,84], [255, 217, 47],
        [229, 196, 148], [179, 179, 179]])
c  = c/255

colors = {
    'RACE': c[0],
    'RACE-Partitioned': c[1],
    'Plush': c[2],
    'SEPHASH': c[3],
    'MYHASH': c[4],
    'MYHASH-NoOpt': c[5],
    'insert': c[0],
    'update': c[1],
    'read': c[2],
}

hatches = {
    'RACE': hat[0],
    'RACE-Partitioned': hat[1],
    'Plush': hat[2],
    'SEPHASH': hat[3],
    'MYHASH': hat[4],
    'MYHASH-NoOpt': hat[5],
}

markers = {
    'RACE': markers[0],
    'RACE-Partitioned': markers[1],
    'Plush': markers[2],
    'SEPHASH': markers[3],
    'MYHASH': markers[4],
    'MYHASH-NoOpt': markers[5],
    'insert': markers[0],
    'update': markers[1],
    'read': markers[2],
}

def hash_type_to_label(hash_type):
    if hash_type == 'MYHASH':
        return 'DREAM'
    elif hash_type == 'SEPHASH':
        return 'SepHash'
    else:
        return hash_type

def hash_type_to_label_breakdown(hash_type):
    if hash_type == 'MYHASH':
        return '+Split (DREAM)'
    elif hash_type == 'MYHASH-NoOpt':
        return '+CC&Compact'
    elif hash_type == 'SEPHASH':
        return 'Base (SepHash)'
    elif hash_type == 'RACE-Partitioned':
        return 'RACE-Partitioned'
    else:
        return hash_type 

# 定义哈希类型的顺序
def sort_hash_types(hash_types):
    # 指定的顺序: RACE, RACE-Partitioned, Plush, SepHash, MYHASH
    order = {"RACE": 1, "RACE-Partitioned": 2, "Plush": 3, "SEPHASH": 4, "MYHASH-NoOpt": 5, "MYHASH": 6}
    return sorted(hash_types, key=lambda x: order.get(x, 999))  # 未指定的类型放在最后

# 定义数据目录
DATA_DIR = "../mixed_data"

# 定义提取IOPS的函数
def extract_iops(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
        match = re.search(r'Run IOPS:([\d.]+)Kops', content)
        if match:
            return float(match.group(1))
    return None

# 定义提取延迟的函数
def extract_latency(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
        match = re.search(r'avg latency: ([\d.]+) us', content)
        if match:
            return float(match.group(1))
    return None

# 定义获取所有比例目录的函数
def get_ratio_dirs():
    return [d for d in os.listdir(DATA_DIR) if d.startswith('data_insert')]

# 定义获取哈希类型的函数
def get_hash_types(ratio_dir):
    path = os.path.join(DATA_DIR, ratio_dir)
    return [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]

# 定义获取IOPS数据的函数
def get_iops_data(ratio_dir, hash_type):
    thread_dir = os.path.join(DATA_DIR, ratio_dir, hash_type, "224")
    if not os.path.exists(thread_dir):
        return []
    
    iops_values = []
    for file_name in os.listdir(thread_dir):
        if file_name.startswith("out"):
            file_path = os.path.join(thread_dir, file_name)
            iops = extract_iops(file_path)
            if iops:
                iops_values.append(iops)
    
    return iops_values

def plot_variable_kv(ax):
    # 定义操作和KV大小
    operations = ['insert', 'update', 'read']
    sizes = [64, 128, 512, 2048, 8192]
    
    # 收集数据
    data = {}
    for operation in operations:
        data[operation] = {}
        for size in sizes:
            data_dir = f"../data/data_{operation}_size{size}"
            if not os.path.exists(data_dir):
                print(f"Directory {data_dir} does not exist.")
                continue
            
            latency_values = []
            for hash_type in colors.keys():
                thread_dir = os.path.join(data_dir, hash_type, "1")
                if not os.path.exists(thread_dir):
                    continue
                
                for file_name in os.listdir(thread_dir):
                    if file_name.startswith("out"):
                        file_path = os.path.join(thread_dir, file_name)
                        latency = extract_latency(file_path)
                        if latency:
                            latency_values.append(latency)
            if latency_values:
                avg_latency = np.mean(latency_values)
                data[operation][size] = avg_latency
    
    # 计算并输出每个KV大小相对于所有较小KV大小的延迟变化百分比
    for operation in operations:
        if not data[operation]:
            continue
        
        print(f"\n--- {operation.capitalize()} Operation Latency Changes ---")
        sorted_sizes = sorted(data[operation].keys())
        
        for i, current_size in enumerate(sorted_sizes):
            if i == 0:  # 最小的KV大小没有比它更小的
                continue
                
            current_latency = data[operation][current_size]
            
            # 对于当前大小，计算与所有较小大小的延迟变化
            for j in range(i):
                smaller_size = sorted_sizes[j]
                smaller_latency = data[operation][smaller_size]
                
                percentage_change = ((current_latency - smaller_latency) / smaller_latency) * 100
                
                size_label_current = f"{current_size}" if current_size < 1000 else f"{current_size//1024}K"
                size_label_smaller = f"{smaller_size}" if smaller_size < 1000 else f"{smaller_size//1024}K"
                
                print(f"{operation.capitalize()}: {size_label_current} vs {size_label_smaller}: +{percentage_change:.2f}%")
    
    # 绘制折线图
    for operation in operations:
        if not data[operation]:
            continue
        
        sizes_available = sorted(data[operation].keys())
        latencies = [data[operation][size] for size in sizes_available]
        
        # Use positions 0, 1, 2, etc. for x-axis instead of actual size values
        x_positions = list(range(len(sizes_available)))
        
        ax.plot(x_positions, latencies, 
                marker=markers.get(operation, 'o'),
                color=colors.get(operation, 'black'),
                label=operation.capitalize(),
                lw=3, mec='black', markersize=8, alpha=1)
    
    # 设置图表属性
    ax.set_xlabel('KV Size (bytes)')
    ax.set_ylabel('Average Latency (μs)')
    ax.set_title('(b) Variable KV Sizes')
    
    # Set x-tick positions and labels manually
    ax.set_xticks(list(range(len(sizes))))
    ax.set_xticklabels([str(size) if size < 1000 else f"{size//1024}K" for size in sizes])
    
    ax.grid(axis='y', linestyle='-.')
    # ax.legend()

def plot_breakdown(ax):
    # 定义数据目录
    data_dir = "../data/data_insert_breakdown"
    
    # 获取所有哈希类型
    hash_types = []
    if os.path.exists(data_dir):
        hash_types = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
    
    # 使用自定义顺序排序哈希类型
    hash_types = sort_hash_types(hash_types)
    
    # 动态获取所有可用的线程数
    all_threads = set()
    for hash_type in hash_types:
        hash_type_dir = os.path.join(data_dir, hash_type)
        if os.path.exists(hash_type_dir):
            threads = [int(d) for d in os.listdir(hash_type_dir) if os.path.isdir(os.path.join(hash_type_dir, d)) and d.isdigit()]
            all_threads.update(threads)
    
    threads = sorted(all_threads)  # 排序线程数
    
    # 收集数据
    data = {}
    for hash_type in hash_types:
        data[hash_type] = {}
        for thread in threads:
            thread_dir = os.path.join(data_dir, hash_type, str(thread))
            if not os.path.exists(thread_dir):
                continue
                
            iops_values = []
            for file_name in os.listdir(thread_dir):
                if file_name.startswith("out"):
                    file_path = os.path.join(thread_dir, file_name)
                    iops = extract_iops(file_path)
                    if iops:
                        iops_values.append(iops)
            
            if iops_values:
                avg_iops = np.mean(iops_values)
                data[hash_type][thread] = avg_iops
    
    # 计算并输出每个哈希类型相比前一个哈希类型的性能提升百分比
    print("\n--- Breakdown Analysis Performance Improvements ---")
    for i, hash_type in enumerate(hash_types):
        if i == 0:  # 第一个哈希类型没有前一个类型可比较
            continue
        
        prev_hash_type = hash_types[i-1]
        
        # 检查两个哈希类型是否都有数据
        common_threads = set(data[hash_type].keys()) & set(data[prev_hash_type].keys())
        if not common_threads:
            continue
        
        improvements = []
        for thread in common_threads:
            current_iops = data[hash_type][thread]
            prev_iops = data[prev_hash_type][thread]
            
            if prev_iops > 0:
                improvement = ((current_iops - prev_iops) / prev_iops) * 100
                improvements.append((thread, improvement))
        
        if improvements:
            min_improvement = min(improvements, key=lambda x: x[1])
            max_improvement = max(improvements, key=lambda x: x[1])
            avg_improvement = np.mean([imp[1] for imp in improvements])
            
            current_label = hash_type_to_label_breakdown(hash_type)
            prev_label = hash_type_to_label_breakdown(prev_hash_type)
            
            print(f"{current_label} vs {prev_label}:")
            print(f"  Min improvement: {min_improvement[1]:.2f}% (at {min_improvement[0]} threads)")
            print(f"  Max improvement: {max_improvement[1]:.2f}% (at {max_improvement[0]} threads)")
            print(f"  Avg improvement: {avg_improvement:.2f}%")
    
    # 使用均匀间隔的x轴位置
    x_positions = list(range(len(threads)))
    
    # 绘制折线图
    for hash_type in hash_types:
        if not data[hash_type]:
            continue
            
        threads_available = sorted(data[hash_type].keys())
        iops_values = [data[hash_type][t] for t in threads_available]
        
        # 将实际线程数映射到均匀间隔的位置
        positions = [x_positions[threads.index(t)] for t in threads_available]
        
        ax.plot(positions, iops_values,
                marker=markers.get(hash_type, 'o'),
                color=colors.get(hash_type, 'black'),
                label=hash_type_to_label_breakdown(hash_type),
                lw=2.5, mec='black', markersize=7)
    
    # 设置图表属性
    ax.set_xlabel('Number of Threads')
    ax.set_ylabel('Throughput (Kops)')
    ax.set_title('(a) Breakdown Analysis')
    
    # 设置x轴刻度为均匀间隔
    ax.set_xticks(x_positions)
    ax.set_xticklabels([str(t) for t in threads], rotation=45)
    
    # 添加网格线
    ax.grid(axis='y', linestyle='-.')

if __name__ == "__main__":
    # 创建 1x4 子图布局
    fig, axs = plt.subplots(1, 2, figsize=(5, 2.5))
    
    # Fig 3: 分解（原本是变长KV，现交换顺序）
    plot_breakdown(axs[0])
    
    # Fig 4: 变长KV（原本是分解，现交换顺序）
    plot_variable_kv(axs[1])
    
    # 为第三张子图（现在是分解）创建单独的图例
    handles3, labels3 = axs[0].get_legend_handles_labels()
    fig.legend(handles3, labels3, loc='upper center',
            bbox_to_anchor=(0.325, 1.27), ncol=1, frameon=True)
    
    # 为第四张子图（现在是变长KV）创建单独的图例
    handles4, labels4 = axs[1].get_legend_handles_labels()
    fig.legend(handles4, labels4, loc='upper center',
            bbox_to_anchor=(0.8, 1.27), ncol=1, frameon=True)
    
    # 调整整体布局
    plt.tight_layout()
    
    # 保存图片
    plt.savefig('../out_png/breakdown_var_kv.png', bbox_inches='tight')
    plt.savefig('../out_pdf/breakdown_var_kv.pdf', bbox_inches='tight')
    
    # 显示图表
    plt.show()