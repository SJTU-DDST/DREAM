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
        return '+Split'
    elif hash_type == 'MYHASH-NoOpt':
        return '+CC&Compact'
    elif hash_type == 'SEPHASH':
        return 'Base'
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
               color=colors[hash_type],
               edgecolor='black',
               lw=1.2,
               hatch=hatches[hash_type],
               label=hash_type_to_label(hash_type))
    
    # 计算DREAM相对于其他哈希的性能提升百分比
    for i, ratio_label in enumerate(data.keys()):
        if 'MYHASH' in data[ratio_label]:  # MYHASH是DREAM
            dream_value = data[ratio_label]['MYHASH']
            improvements = []
            
            for hash_type in all_hash_types:
                if hash_type != 'MYHASH' and hash_type in data[ratio_label] and data[ratio_label][hash_type] > 0:
                    improvement = (dream_value / data[ratio_label][hash_type] - 1) * 100
                    improvements.append(improvement)
            
            if improvements:
                min_improvement = min(improvements)
                max_improvement = max(improvements)
                # improvement_text = f"{min_improvement:.1f}%\n{max_improvement:.1f}%"
                
                # # 在柱状图上方显示提升范围
                # ax.text(i, dream_value * 1.05, improvement_text, ha='center', va='bottom', fontsize=8)
                print(f"Insert/Search Ratio: {ratio_label}, DREAM Improvement: {min_improvement:.1f}% ~ {max_improvement:.1f}%")
    
    # 设置图表属性
    ax.set_xlabel('Insert/Search Ratio')
    ax.set_ylabel('Throughput (Kops)')
    ax.set_title('(a) Hybrid Workloads')
    ax.set_xticks(index)
    ax.set_xticklabels(data.keys(), rotation=0)
    
    # 优化Y轴刻度显示
    # def thousands_formatter(x, pos):
    #     return f'{int(x):,}'
    
    # ax.yaxis.set_major_formatter(FuncFormatter(thousands_formatter))
    
    # 添加网格线以便于读取数据
    ax.grid(axis='y', linestyle='-.')

if __name__ == "__main__":
    # 创建 1x4 子图布局
    fig, ax = plt.subplots(1, 1, figsize=(5, 2.5))
    
    # Fig 1: Mixed Workload
    plot_mixed(ax)

    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center',
            bbox_to_anchor=(0.5, 1.15), ncol=2, frameon=False)
    
    # 调整整体布局
    plt.tight_layout()
    
    # 保存图片
    plt.savefig('../out_png/insert_search.png', bbox_inches='tight')
    plt.savefig('../out_pdf/insert_search.pdf', bbox_inches='tight')
    
    # 显示图表
    plt.show()