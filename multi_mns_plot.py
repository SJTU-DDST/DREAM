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


# 支持的哈希类型
hash_types = ['RACE-Partitioned', 'Plush', 'SEPHASH', 'MYHASH']

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

def hash_type_to_label(hash_type):
    if hash_type == 'MYHASH':
        return 'DREAM'
    elif hash_type == 'SEPHASH':
        return 'SepHash'
    else:
        return hash_type

def extract_iops(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
        match = re.search(r'Run IOPS:([\d.]+)Kops', content)
        if match:
            return float(match.group(1))
    return None

# 服务器数量 1和4
server_nums = [1, 4]
x_labels = [str(n) for n in server_nums]

# 采集16线程数据
data_dict_16 = {h: [] for h in hash_types}
for h in hash_types:
    for n in server_nums:
        if n == 1:
            file_path = f"../data/data_insert/{h}/16/out192.168.98.74.txt"
        else:
            file_path = f"../data/data_insert_{n}_servers/{h}/16/out192.168.98.74.txt"
        if os.path.exists(file_path):
            iops = extract_iops(file_path)
            if iops:
                data_dict_16[h].append(iops)
            else:
                data_dict_16[h].append(0)
        else:
            data_dict_16[h].append(0)

# 采集32线程数据
data_dict_32 = {h: [] for h in hash_types}
for h in hash_types:
    for n in server_nums:
        if n == 1:
            file_path = f"../data/data_insert/{h}/32/out192.168.98.74.txt"
        else:
            file_path = f"../data/data_insert_{n}_servers/{h}/32/out192.168.98.74.txt"
        if os.path.exists(file_path):
            iops = extract_iops(file_path)
            if iops:
                data_dict_32[h].append(iops)
            else:
                data_dict_32[h].append(0)
        else:
            data_dict_32[h].append(0)

# 画左右子图
fig, axs = plt.subplots(1, 2, figsize=(5, 2.5), sharey=True)
index = np.arange(len(server_nums))
bar_width = 0.2

handles = []
labels = []

def thousands_formatter(x, pos):
    return f'{int(x):,}'

# 16线程子图（左）
for i, h in enumerate(hash_types):
    bar = axs[0].bar(index + (i - 1.5) * bar_width, data_dict_16[h], width=bar_width, color=colors[h], label=hash_type_to_label(h), hatch=hatches[h], edgecolor='black', alpha=0.95)
    if i == 0:
        handles = [bar]
        labels = [h]
    else:
        handles.append(bar)
        labels.append(h)
axs[0].set_xlabel('Number of Servers')
axs[0].set_ylabel('Throughput (Kops)')
axs[0].set_title('16 Threads')
axs[0].set_xticks(index)
axs[0].set_xticklabels(x_labels)
axs[0].grid(axis='y', linestyle='-.')
axs[0].yaxis.set_major_formatter(FuncFormatter(thousands_formatter))

# 32线程子图（右）
for i, h in enumerate(hash_types):
    axs[1].bar(index + (i - 1.5) * bar_width, data_dict_32[h], width=bar_width, color=colors[h], label=hash_type_to_label(h), hatch=hatches[h], edgecolor='black', alpha=0.95)
axs[1].set_xlabel('Number of Servers')
axs[1].set_title('32 Threads')
axs[1].set_xticks(index)
axs[1].set_xticklabels(x_labels)
axs[1].grid(axis='y', linestyle='-.')
axs[1].yaxis.set_major_formatter(FuncFormatter(thousands_formatter))

# 共享图例，放到上方
handles, labels = axs[0].get_legend_handles_labels()
fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=len(labels) / 2, frameon=False)
    
plt.tight_layout()
plt.savefig('../out_png/multi_mns.png', bbox_inches='tight')
plt.savefig('../out_pdf/multi_mns.pdf', bbox_inches='tight')
plt.show()

# 打印4 server相比1 server降低百分比
print("==== 4 server vs 1 server IOPS 降低百分比 ====")
for th, data_dict in [("16线程", data_dict_16), ("32线程", data_dict_32)]:
    print(f"--- {th} ---")
    for h in hash_types:
        v1 = data_dict[h][0]
        v4 = data_dict[h][1]
        if v1 > 0:
            percent = (v1 - v4) / v1 * 100
            print(f"{h}: {percent:.1f}%")
        else:
            print(f"{h}: 无法计算（1server数据为0）")