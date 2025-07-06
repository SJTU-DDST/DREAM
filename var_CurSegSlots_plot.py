import matplotlib.pyplot as plt
import numpy as np

hat = ['//', '\\\\', 'xx', '||', '--', '++']
markers_list = ['H', '^', '>', 'D', 'o', 's', 'p', 'x']
c = np.array([[102, 194, 165], [252, 141, 98], [141, 160, 203], 
    [231, 138, 195], [166,216,84], [255, 217, 47],
    [229, 196, 148], [179, 179, 179]])
c  = c/255

# 手动整理的数据
slots = [16, 32, 64, 128, 256, 512, 1024, 2048, 4096]
insert_iops = [
    np.mean([6721.45, 6722.59, 6722.22, 6721.99]),
    np.mean([5973.99, 5976.27, 5974.57, 5974.12]),
    np.mean([6077.45, 6079.80, 6074.87, 6076.45]),
    np.mean([6079.97, 6083.15, 6079.94, 6080.24]),
    np.mean([6257.71, 6261.37, 6258.22, 6257.71]),
    np.mean([6188.97, 6191.36, 6189.25, 6188.99]),
    np.mean([6055.10, 6059.02, 6057.45, 6055.60]),
    np.mean([5892.50, 5893.75, 5892.80, 5892.59]),
    np.mean([6174.17, 6178.30, 6175.69, 6176.35]),
]
read_iops = [
    np.mean([11373.92, 11381.26, 11369.04, 11365.04]),
    np.mean([10196.55, 10206.03, 10195.41, 10196.43]),
    np.mean([9914.20, 9930.57, 9916.49, 9915.26]),
    np.mean([9441.71, 9450.01, 9449.54, 9445.53]),
    np.mean([8970.32, 8980.07, 8969.46, 8969.39]),
    np.mean([5713.47, 5714.67, 5712.44, 5714.02]),
    np.mean([5653.30, 5656.41, 5653.19, 5653.24]),
    np.mean([453.83, 453.85, 453.82, 453.83]),
    np.mean([453.55, 453.57, 453.55, 453.56]),
]

x = np.arange(len(slots))
plt.figure(figsize=(5, 2.5))
plt.plot(x, insert_iops, marker=markers_list[0], label='Insert', color=c[0], lw=2.5, mec='black', markersize=7)
plt.plot(x, read_iops, marker='s', label='Read', color=c[1], lw=2.5, mec='black', markersize=7)
# plt.xlabel('CurSegSlots (SLOT_PER_SEG)')
plt.xlabel('Number of WriteBuffer Slots')
plt.ylabel('Throughput (Kops)')
# plt.title('CurSegSlots size impact on Read/Insert IOPS')
plt.xticks(x, slots)
plt.legend()
plt.grid(axis='y', linestyle='-.')
plt.tight_layout()
# plt.show()
plt.savefig('../out_png/var_CurSegSlots_plot.png', bbox_inches='tight')
plt.savefig('../out_pdf/var_CurSegSlots_plot.pdf', bbox_inches='tight')