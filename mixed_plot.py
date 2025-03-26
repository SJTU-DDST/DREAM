import os
import re
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

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

# 主函数
def main():
    # 收集数据
    data = {}
    ratio_dirs = sorted(get_ratio_dirs())
    
    all_hash_types = set()
    for ratio_dir in ratio_dirs:
        hash_types = get_hash_types(ratio_dir)
        all_hash_types.update(hash_types)
    
    all_hash_types = sorted(list(all_hash_types))
    
    for ratio_dir in ratio_dirs:
        ratio_match = re.match(r'data_insert(\d+)_read(\d+)', ratio_dir)
        if ratio_match:
            insert_ratio = int(ratio_match.group(1))
            read_ratio = int(ratio_match.group(2))
            ratio_label = f"{insert_ratio}% Insert\n{read_ratio}% Read"
            
            data[ratio_label] = {}
            
            for hash_type in all_hash_types:
                iops_values = get_iops_data(ratio_dir, hash_type)
                if iops_values:
                    avg_iops = np.mean(iops_values)
                    data[ratio_label][hash_type] = avg_iops
    
    # 配置图表
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # 设置柱状图参数
    bar_width = 0.3
    opacity = 1.0
    index = np.arange(len(data.keys()))
    
    # 定义色彩、纹理和边框样式
    hat = ['//', '\\\\', 'xx', '||', '--', '++']
    c = np.array([
        [102, 194, 165], [252, 141, 98], [141, 160, 203], 
        [231, 138, 195], [166, 216, 84], [255, 217, 47]
    ]) / 255
    
    # 设置网格线在柱子下方
    ax.set_axisbelow(True)
    
    # 绘制柱状图
    for i, hash_type in enumerate(all_hash_types):
        values = []
        for ratio_label in data.keys():
            values.append(data[ratio_label].get(hash_type, 0))
        
        position = index + i * bar_width
        plt.bar(position, values, bar_width,
                color=c[i % len(c)],
                edgecolor='black',
                lw=1.2,
                hatch=hat[i % len(hat)],
                label=hash_type)
    
    # 设置图表属性
    plt.xlabel('Insert/Read Ratio', fontsize=14)
    plt.ylabel('IOPS (K ops/sec)', fontsize=14)
    plt.title('IOPS Performance Comparison: SepHash vs MyHash (224 Threads)', fontsize=16)
    plt.xticks(index + bar_width * (len(all_hash_types) - 1) / 2, data.keys(), rotation=0)
    plt.legend(title='Hash Type', loc='upper center', bbox_to_anchor=(0.5, -0.05), ncol=len(all_hash_types), frameon=False)
    
    # 优化Y轴刻度显示
    def thousands_formatter(x, pos):
        return f'{int(x):,}'
    
    ax.yaxis.set_major_formatter(FuncFormatter(thousands_formatter))
    
    # 添加网格线以便于读取数据
    plt.grid(axis='y', linestyle='-.', alpha=0.7)
    
    # 调整布局
    plt.tight_layout(rect=[0, 0.05, 1, 0.95])
    
    # 保存图片
    plt.savefig('../out/mixed.png', dpi=300, bbox_inches='tight')
    plt.savefig('../out/mixed.eps', bbox_inches='tight')  # 同时保存EPS格式
    
    # 显示图表
    plt.show()

if __name__ == "__main__":
    main()
