import os
import re
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# 颜色和样式
c = np.array([[102, 194, 165], [252, 141, 98], [141, 160, 203], [231, 138, 195]])
c = c / 255

# 支持的哈希类型
hash_types = ['MYHASH', 'SEPHASH', 'RACE-Partitioned', 'Plush']

# 颜色和样式扩展
colors = {
    'MYHASH': c[0],
    'SEPHASH': c[1],
    'RACE-Partitioned': c[2],
    'Plush': c[3],
}
markers = {
    'MYHASH': 'o',
    'SEPHASH': 's',
    'RACE-Partitioned': 'D',
    'Plush': '^',
}
hatches = {
    'MYHASH': '//',
    'SEPHASH': 'xx',
    'RACE-Partitioned': '++',
    'Plush': '..',
}

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
fig, axs = plt.subplots(1, 2, figsize=(8, 3.5), sharey=True)
index = np.arange(len(server_nums))
bar_width = 0.18

handles = []
labels = []

def thousands_formatter(x, pos):
    return f'{int(x):,}'

# 16线程子图（左）
for i, h in enumerate(hash_types):
    bar = axs[0].bar(index + (i - 1.5) * bar_width, data_dict_16[h], width=bar_width, color=colors[h], label=h, hatch=hatches[h], edgecolor='black', alpha=0.95)
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
    axs[1].bar(index + (i - 1.5) * bar_width, data_dict_32[h], width=bar_width, color=colors[h], label=h, hatch=hatches[h], edgecolor='black', alpha=0.95)
axs[1].set_xlabel('Number of Servers')
axs[1].set_title('32 Threads')
axs[1].set_xticks(index)
axs[1].set_xticklabels(x_labels)
axs[1].grid(axis='y', linestyle='-.')
axs[1].yaxis.set_major_formatter(FuncFormatter(thousands_formatter))

# 共享图例，放到上方
fig.legend(hash_types, loc='upper center', ncol=len(hash_types), bbox_to_anchor=(0.5, 1.04), frameon=False)

plt.tight_layout(rect=[0, 0, 1, 0.97])
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