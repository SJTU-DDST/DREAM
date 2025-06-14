import numpy as np
import matplotlib.pyplot as plt

# 颜色、hatch、marker 定义
hat = ['//', '\\\\', 'xx', '||', '--', '++']
markers_list = ['H', '^', '>', 'D', 'o', 's', 'p', 'x']
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

def sort_hash_types(hash_types):
    order = {"RACE": 1, "RACE-Partitioned": 2, "Plush": 3, "SEPHASH": 4, "MYHASH-NoOpt": 5, "MYHASH": 6}
    return sorted(hash_types, key=lambda x: order.get(x, 999))

# 数据
data = {
    "RACE": {
        "global_dep": 16,
        "space_consumption": 121827856,
        "buc_meta_consumption": 13361664,
        "dir_entry_consumption": 1572880,
        "total_meta_consumption": 14934544,
        "segment_cnt": 8699,
        "entry_total": 13361664,
        "entry_cnt": 10000000,
        "entry_utilization": 0.748410,
        "space_utilization": 0.656664
    },
    "MYHASH": {
        "global_dep": 11,
        "sizeof_CurSeg": 3776,
        "sizeof_Slot": 24,
        "space_consumption": 243709648,
        "buc_meta_consumption": 327680,
        "dir_entry_consumption": 589832,
        "total_meta_consumption": 917512,
        "segment_cnt": 2048,
        "entry_total": 10116339,
        "entry_cnt": 10000000,
        "entry_utilization": 0.988500,
        "space_utilization": 0.984778
    },
    "SEPHASH": {
        "global_dep": 11,
        "sizeof_CurSeg": 2872,
        "sizeof_Slot": 16,
        "space_consumption": 162791256,
        "buc_meta_consumption": 311296,
        "dir_entry_consumption": 589832,
        "total_meta_consumption": 901128,
        "segment_cnt": 2048,
        "entry_total": 10118133,
        "entry_cnt": 10004842,
        "entry_utilization": 0.988803,
        "space_utilization": 0.983330
    },
    "Plush": {
        "cur_level": 2,
        "space_consumption": 24660616,
        "buc_meta_consumption": 0,
        "dir_entry_consumption": 14469120,
        "total_meta_consumption": 0,
        "entry_total": 1306704,
        "entry_cnt": 620630,
        "entry_utilization": 0.474958,
        "space_utilization": 0.201335
    }
}

# 打印latex表格
def print_latex_table(data):
    # 按表格顺序排列
    order = ["MYHASH", "SEPHASH", "RACE", "Plush"]
    name_map = {"MYHASH": "DREAM", "SEPHASH": "SepHash", "RACE": "RACE", "Plush": "Plush"}
    print("\\begin{table}[t]")
    print("\\caption{Space Overhead}")
    print("\\begin{center}")
    print("\\begin{tabular}{|c|c|c|c|c|c|}")
    print("\\hline")
    print("\\textbf{Index} & \\textbf{Depth} & \\textbf{BucMeta} & \\textbf{DirMeta} & \\textbf{Total} & \\textbf{\\#Entry} \\\\")
    print("           & \\textbf{/Level} & (MB)    & (MB)    & (MB)  & (million) \\\\")
    print("\\hline")
    for k in order:
        d = data[k]
        if k == "Plush":
            depth = d.get("cur_level", "")
        else:
            depth = d.get("global_dep", "")
        bucmeta = d.get("buc_meta_consumption", 0) / 1024 / 1024
        dirmeta = d.get("dir_entry_consumption", 0) / 1024 / 1024
        total = d.get("space_consumption", 0) / 1024 / 1024
        entry_total = d.get("entry_total", 0) / 1e6
        print(f"{name_map[k]}  & {depth} & {bucmeta:.2f}  & {dirmeta:.2f} & {total:.2f} & {entry_total:.2f} \\\\")
    print("\\hline")
    print("\\end{tabular}")
    print("\\label{tab:hash_meta_compare}")
    print("\\end{center}")
    print("\\end{table}")

print_latex_table(data)

# RACE
# ERROR [/home/congyong/SepHash/src/race.cc:257] cal_utilization: global_dep:16
# ERROR [/home/congyong/SepHash/src/race.cc:291] cal_utilization: space_consumption:121827856 buc_meta_consumption:13361664 dir_entry_consumption:1572880 total_meta_consumption:14934544 segment_cnt:8699 entry_total:13361664 entry_cnt:10000000 entry_utilization:0.748410 space_utilization:0.656664
# MYHASH
# ERROR [/home/congyong/SepHash/src/sephash.cc:223] cal_utilization: global_dep:11
# ERROR [/home/congyong/SepHash/src/sephash.cc:224] cal_utilization: sizeof(CurSeg):3776 sizeof(Slot):24
# ERROR [/home/congyong/SepHash/src/sephash.cc:280] cal_utilization: space_consumption:243709648 buc_meta_consumption:327680 dir_entry_consumption:589832 total_meta_consumption:917512 segment_cnt:2048 entry_total:10116339 entry_cnt:10000000 entry_utilization:0.988500 space_utilization:0.984778
# SEPHASH
# ERROR [/home/congyong/SepHash/src/sephash.cc:223] cal_utilization: global_dep:11
# ERROR [/home/congyong/SepHash/src/sephash.cc:224] cal_utilization: sizeof(CurSeg):2872 sizeof(Slot):16
# ERROR [/home/congyong/SepHash/src/sephash.cc:264] cal_utilization: space_consumption:162791256 buc_meta_consumption:311296 dir_entry_consumption:589832 total_meta_consumption:901128 segment_cnt:2048 entry_total:10118133 entry_cnt:10004842 entry_utilization:0.988803 space_utilization:0.983330
# Plush
# ERROR [/home/congyong/SepHash/src/plush.cc:489] cal_utilization: cur_level:2
# ERROR [/home/congyong/SepHash/src/plush.cc:490] cal_utilization: space_consumption:24660616 buc_meta_consumption:0 dir_entry_consumption:14469120 total_meta_consumption:0 entry_total:1306704 entry_cnt:620630 entry_utilization:0.474958 space_utilization:0.201335

# 按指定顺序排序
labels = sort_hash_types(list(data.keys()))
entry_util = [data[k]["entry_utilization"] for k in labels]
space_util = [data[k]["space_utilization"] for k in labels]

x = np.arange(len(labels))
width = 0.35

fig, ax = plt.subplots(figsize=(5, 2.5))

# Entry Utilization
rects1 = ax.bar(x - width/2, entry_util, width,
        label='Entry Utilization',
        color=c[0],
        edgecolor='black',
        lw=1.2,
        hatch=hat[0], zorder=2)

# Space Utilization
rects2 = ax.bar(x + width/2, space_util, width,
        label='Space Utilization',
        color=c[1],
        edgecolor='black',
        lw=1.2,
        hatch=hat[1], zorder=2)

# Y轴
# ax.set_ylabel('Utilization')
ax.set_ylim(0, 1.1)
ax.set_xticks(x)
ax.set_xticklabels([hash_type_to_label(k) for k in labels])
# ax.set_title('Entry & Space Utilization of Hash Indexes')
ax.grid(axis='y', linestyle='-.', zorder=0)

# 柱子上标数值
# for rect in rects1:
#     height = rect.get_height()
#     ax.annotate(f'{height:.2f}',
#         xy=(rect.get_x() + rect.get_width() / 2, height),
#         xytext=(0, 3),
#         textcoords="offset points",
#         ha='center', va='bottom', fontsize=8)
# for rect in rects2:
#     height = rect.get_height()
#     ax.annotate(f'{height:.2f}',
#         xy=(rect.get_x() + rect.get_width() / 2, height),
#         xytext=(0, 3),
#         textcoords="offset points",
#         ha='center', va='bottom', fontsize=8)

# legend
handles, legend_labels = ax.get_legend_handles_labels()
fig.legend(handles, legend_labels, loc='upper center',
       bbox_to_anchor=(0.5, 1.1), ncol=2, frameon=False)

plt.tight_layout()
plt.savefig('../out_png/utilization.png', bbox_inches='tight')
plt.savefig('../out_pdf/utilization.pdf', bbox_inches='tight')
plt.show()