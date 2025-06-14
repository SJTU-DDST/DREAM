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
    # 新增Skewed颜色
    'RACE-Skewed': c[2],
    'SEPHASH-Skewed': c[4],
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
    # 新增Skewed marker
    'RACE-Skewed': markers[2],
    'SEPHASH-Skewed': markers[4],
}

def hash_type_to_label(hash_type):
    if hash_type == 'MYHASH':
        return 'DREAM'
    elif hash_type == 'SEPHASH':
        return 'SepHash'
    else:
        return hash_type

def hash_type_to_label_breakdown(hash_type, skew_type):
    # 支持skew类型的label
    if hash_type == 'RACE':
        return 'RACE-' + skew_type
    elif hash_type == 'SEPHASH':
        return 'SepHash-' + skew_type
    elif hash_type == 'MYHASH':
        return 'DREAM-' + skew_type
    elif hash_type == 'MYHASH-NoOpt':
        return '+CC&Compact-' + skew_type
    elif hash_type == 'RACE-Partitioned':
        return 'RACE-Partitioned-' + skew_type
    else:
        return hash_type + '-' + skew_type

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

def extract_avg_retry(file_path):
    # 提取所有 avg_retry 行的数值并返回均值
    retries = []
    with open(file_path, 'r') as f:
        for line in f:
            match = re.search(r'avg_retry:([\d.]+)', line)
            if match:
                retries.append(float(match.group(1)))
    if retries:
        return np.mean(retries)
    return None

def plot_retry_and_bytes(ax):
    configs = [
        ("../data/data_update", "Uniform"),
        ("../data/data_update_zipf99", "Skewed"),
    ]
    hash_types = ['RACE', 'SEPHASH']

    all_threads = set()
    for data_dir, _ in configs:
        for hash_type in hash_types:
            hash_type_dir = os.path.join(data_dir, hash_type)
            if os.path.exists(hash_type_dir):
                threads = [int(d) for d in os.listdir(hash_type_dir) if os.path.isdir(os.path.join(hash_type_dir, d)) and d.isdigit()]
                all_threads.update(threads)
    threads = sorted(all_threads)

    data = {}
    for data_dir, skew_type in configs:
        for hash_type in hash_types:
            key = (hash_type, skew_type)
            data[key] = {}
            for thread in threads:
                thread_dir = os.path.join(data_dir, hash_type, str(thread))
                if not os.path.exists(thread_dir):
                    continue
                retry_values = []
                for file_name in os.listdir(thread_dir):
                    if file_name.startswith("out"):
                        file_path = os.path.join(thread_dir, file_name)
                        avg_retry = extract_avg_retry(file_path)
                        if avg_retry is not None:
                            retry_values.append(avg_retry)
                if retry_values:
                    avg_retry_all = np.mean(retry_values)
                    data[key][thread] = avg_retry_all

    def calc_bytes(avg_retry):
        # RACE和SepHash都用288bytes
        return avg_retry * 288

    plot_order = [
        ('RACE', 'Uniform'),
        ('RACE', 'Skewed'),
        ('SEPHASH', 'Uniform'),
        ('SEPHASH', 'Skewed'),
    ]
    x_positions = list(range(len(threads)))
    ax2 = ax.twinx()

    lines = []
    labels = []
    lines2 = []
    labels2 = []

    for (hash_type, skew_type) in plot_order:
        if (hash_type, skew_type) not in data or not data[(hash_type, skew_type)]:
            continue
        threads_available = sorted(data[(hash_type, skew_type)].keys())
        retry_values = [data[(hash_type, skew_type)][t] for t in threads_available]
        avg_read_bytes = [calc_bytes(v) for v in retry_values]
        positions = [x_positions[threads.index(t)] for t in threads_available]
        if skew_type == "Uniform":
            color = colors.get(hash_type, 'black')
            marker_style = markers.get(hash_type, 'o')
        else:
            color = colors.get(f"{hash_type}-Skewed", 'black')
            marker_style = markers.get(f"{hash_type}-Skewed", 'x')
        # 只画重试次数，副y轴画bytes但不加图例
        l1, = ax.plot(
            positions,
            retry_values,
            marker=marker_style,
            color=color,
            label=hash_type_to_label_breakdown(hash_type, skew_type),
            lw=2.5, mec='black', markersize=7, linestyle='-'
        )
        lines.append(l1)
        labels.append(hash_type_to_label_breakdown(hash_type, skew_type))
        # 副y轴也用实线
        ax2.plot(
            positions,
            avg_read_bytes,
            marker=marker_style,
            color=color,
            lw=2.5, mec='black', markersize=7, linestyle='-'
        )

    ax.set_xlabel('Threads')
    ax.set_ylabel('Average Retry', labelpad=8)
    ax2.set_ylabel('Average Read Bytes', labelpad=8)
    ax.set_xticks(x_positions)
    ax.set_xticklabels([str(t) for t in threads], rotation=45)
    ax.grid(axis='y', linestyle='-.')
    # ax.set_title('Avg Retry & Read Bytes', fontsize=11, pad=8)

    # 只用主y轴的legend
    # ax.legend(lines, labels, loc='upper center', bbox_to_anchor=(0.5, 1.35), ncol=2, frameon=False)

if __name__ == "__main__":
    fig, ax = plt.subplots(1, 1, figsize=(5, 2.5))
    plot_retry_and_bytes(ax)
    handles, labels = ax.get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=2, frameon=False)
    plt.tight_layout()
    plt.savefig('../out_png/retry.png', bbox_inches='tight')
    plt.savefig('../out_pdf/retry.pdf', bbox_inches='tight')
    plt.show()

# 改成两张子图，子图1是RACE在16/224线程下的重试次数（双坐标，右侧坐标读取量），重试次数内部不同颜色划分三种原因的重试
# RACE纯更新没resize，再加个插入时的重试次数
# 子图2是SepHash的