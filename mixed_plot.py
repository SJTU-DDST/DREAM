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
def hash_type_to_label(hash_type):
    if hash_type == 'MYHASH':
        return 'DREAM'
    elif hash_type == 'SEPHASH':
        return 'SepHash'
    else:
        return hash_type

# 定义哈希类型的顺序
def sort_hash_types(hash_types):
    # 指定的顺序: RACE, RACE-Partitioned, Plush, SepHash, MYHASH
    order = {"RACE": 1, "RACE-Partitioned": 2, "Plush": 3, "SEPHASH": 4, "MYHASH": 5}
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

def plot_mixed(ax):
    # 收集数据
    data = {}
    ratio_dirs = sorted(get_ratio_dirs())
    
    all_hash_types = set()
    for ratio_dir in ratio_dirs:
        hash_types = get_hash_types(ratio_dir)
        all_hash_types.update(hash_types)
    
    # 使用自定义顺序排序哈希类型，而不是字母顺序
    all_hash_types = sort_hash_types(list(all_hash_types))
    
    for ratio_dir in ratio_dirs:
        ratio_match = re.match(r'data_insert(\d+)_read(\d+)', ratio_dir)
        if ratio_match:
            insert_ratio = int(ratio_match.group(1)) // 10
            read_ratio = int(ratio_match.group(2)) // 10
            ratio_label = f"{insert_ratio}:{read_ratio}"
            
            data[ratio_label] = {}
            
            for hash_type in all_hash_types:
                iops_values = get_iops_data(ratio_dir, hash_type)
                if iops_values:
                    avg_iops = np.mean(iops_values)
                    data[ratio_label][hash_type] = avg_iops
    
    # 设置柱状图参数
    n_hash_types = len(all_hash_types)
    bar_width = 0.2  # Make bars narrower
    opacity = 1.0
    index = np.arange(len(data.keys()))
    
    # 设置网格线在柱子下方
    ax.set_axisbelow(True)
    
    # 计算每组柱状图的偏移量，使它们并排显示而不重叠
    offsets = np.linspace(-(n_hash_types-1)*bar_width/2, (n_hash_types-1)*bar_width/2, n_hash_types)
    
    # 绘制柱状图
    for i, hash_type in enumerate(all_hash_types):
        values = []
        for ratio_label in data.keys():
            values.append(data[ratio_label].get(hash_type, 0))
        
        position = index + offsets[i]  # Apply offset for each hash type
        ax.bar(position, values, bar_width,
               color=c[i % len(c)],
               edgecolor='black',
               lw=1.2,
               hatch=hat[i % len(hat)],
               label=hash_type_to_label(hash_type))
    
    # 设置图表属性
    ax.set_xlabel('Insert/Search Ratio')
    ax.set_ylabel('Throughput (Kops)')
    ax.set_title('Hybrid Workloads')
    ax.set_xticks(index)
    ax.set_xticklabels(data.keys(), rotation=0)
    
    # 优化Y轴刻度显示
    def thousands_formatter(x, pos):
        return f'{int(x):,}'
    
    ax.yaxis.set_major_formatter(FuncFormatter(thousands_formatter))
    
    # 添加网格线以便于读取数据
    ax.grid(axis='y', linestyle='-.')

def plot_ycsb(ax):
    ax.text(0.5, 0.5, 'YCSB Benchmarks\n(WIP)', 
            horizontalalignment='center', verticalalignment='center', 
            transform=ax.transAxes)
    ax.set_title('YCSB Benchmarks')
    # 添加占位符，后续实现具体绘图逻辑

def plot_variable_kv(ax):
    ax.text(0.5, 0.5, 'Variable Length KV\n(WIP)', 
            horizontalalignment='center', verticalalignment='center', 
            transform=ax.transAxes)
    ax.set_title('Variable KV Size')
    # 添加占位符，后续实现具体绘图逻辑

def plot_decomposition(ax):
    ax.text(0.5, 0.5, 'Decomposition\n(WIP)', 
            horizontalalignment='center', verticalalignment='center', 
            transform=ax.transAxes)
    ax.set_title('Performance Decomposition')
    # 添加占位符，后续实现具体绘图逻辑

if __name__ == "__main__":
    # 创建 1x4 子图布局
    fig, axs = plt.subplots(1, 4, figsize=(10, 2.5))
    
    # Fig 1: Mixed Workload
    plot_mixed(axs[0])
    
    # Fig 2: YCSB
    plot_ycsb(axs[1])
    
    # Fig 3: 变长KV
    plot_variable_kv(axs[2])
    
    # Fig 4: 分解
    plot_decomposition(axs[3])

    handles, labels = axs[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 1.05), ncol=4, frameon=False)
    
    # 调整整体布局
    plt.tight_layout()
    
    # 保存图片
    plt.savefig('../out_png/misc.png', bbox_inches='tight')
    plt.savefig('../out_pdf/misc.pdf', bbox_inches='tight')
    
    # 显示图表
    plt.show()