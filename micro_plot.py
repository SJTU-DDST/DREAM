import os
import re
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

hat = ['//', '\\\\', 'xx', '||', '--', '++']
markers = ['H', '^', '>', 'D', 'o', 's', 'p', 'x']

palette = sns.color_palette("Set2", n_colors=8)
# c = np.array([[102, 194, 165], [252, 141, 98], [141, 160, 203], 
#         [231, 138, 195], [166,216,84], [255, 217, 47],
#         [229, 196, 148], [179, 179, 179]])
# c  = c/255
c = np.array(palette)
print("Using color palette:", c)
def hash_type_to_label(hash_type):
    if hash_type == 'MYHASH':
        return 'DREAM'
    elif hash_type == 'SEPHASH':
        return 'SepHash'
    else:
        return hash_type

def parse_txt_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    
    iops_match = re.search(r'Run IOPS:(\d+\.\d+)Kops', content)
    iops = float(iops_match.group(1)) if iops_match else None
    
    # Check if this is a read operation
    is_read_workload = "data_read" in file_path
    
    # For read workload, only look for search latency
    if is_read_workload:
        avg_latency_matches = re.findall(r'op: search.*?avg latency: (\d+\.\d+) us', content)
        p99_latency_matches = re.findall(r'op: search.*?99% latency: (\d+\.\d+) us', content)
    # For insert/default workload, only look for insert latency
    else:
        avg_latency_matches = re.findall(r'op: insert.*?avg latency: (\d+\.\d+) us', content)
        p99_latency_matches = re.findall(r'op: insert.*?99% latency: (\d+\.\d+) us', content)
    
    avg_latencies = [float(lat) for lat in avg_latency_matches]
    p99_latencies = [float(lat) for lat in p99_latency_matches]
    
    avg_latency = sum(avg_latencies) / len(avg_latencies) if avg_latencies else None
    p99_latency = sum(p99_latencies) / len(p99_latencies) if p99_latencies else None
    
    return iops, avg_latency, p99_latency

def get_ordered_hash_types(hash_dict):
    """
    将哈希类型按照指定顺序排序
    指定顺序: RACE, RACE-Partitioned, Plush, SepHash, MYHASH
    """
    # 指定的哈希类型顺序
    order_priority = {
        'RACE': 0, 
        'RACE-Partitioned': 1, 
        'Plush': 2, 
        'SEPHASH': 3, 
        'MYHASH': 4
    }
    
    # 对哈希类型按照指定优先级排序
    sorted_types = sorted(hash_dict.keys(), key=lambda x: order_priority.get(x, 999))
    return sorted_types

def plot_combined_data(data_sets):
    """
    Plot data from multiple data_dirs in a single figure.
    data_sets is a dictionary where keys are data_dir names and values are tuples of (iops_data, avg_latency_data, p99_latency_data)
    """
    # Create a 3x4 subplot grid (3 rows for metrics, 4 columns for data sets)
    fig, axs = plt.subplots(3, 4, figsize=(10, 6))
    
    # Column titles for each dataset
    dataset_titles = {
        'data_insert': 'Insert',
        'data_update': 'Update (Uniform)',
        'data_update_zipf99': 'Update (Zipf θ=0.99)',
        'data_read': 'Read'
    }
    
    # Row titles
    row_titles = ['Throughput (Kops)', 'Average Latency (us)', '99% Latency (us)']
    
    # 确保列按照指定顺序排列
    ordered_data_dirs = ['data_insert', 'data_update', 'data_update_zipf99', 'data_read']
    
    # 记录哈希类型对应的颜色和标记映射，确保一致性
    hash_type_colors = {}
    hash_type_markers = {}
    
    for col_idx, data_dir in enumerate(ordered_data_dirs):
        if data_dir not in data_sets:
            print(f"Warning: {data_dir} not found in data sets. Skipping.")
            continue
            
        iops_data, avg_latency_data, p99_latency_data = data_sets[data_dir]
        
        # 获取所有线程数，以便统一横坐标
        all_threads = set()
        for hash_type in iops_data:
            all_threads.update(iops_data[hash_type].keys())
        all_threads = sorted(list(all_threads))
        
        # 创建非线性映射，将线程数映射到均匀的位置
        position_mapping = {thread: i for i, thread in enumerate(all_threads)}
        
        # 按照指定顺序排序哈希类型
        ordered_hash_types = get_ordered_hash_types(iops_data)
        
        # Plot IOPS (first row)
        for i, hash_type in enumerate(ordered_hash_types):
            values = iops_data[hash_type]
            threads = sorted(values.keys())
            x_positions = [position_mapping[thread] for thread in threads]
            iops_metrics = [values[thread]['iops'] for thread in threads]
            
            # 如果这个哈希类型还没有分配颜色和标记，就分配一个
            if hash_type not in hash_type_colors:
                hash_type_colors[hash_type] = c[i % len(c)]
                hash_type_markers[hash_type] = markers[i % len(markers)]
            
            axs[0, col_idx].plot(x_positions, iops_metrics, 
                      marker=hash_type_markers[hash_type], 
                      label=hash_type_to_label(hash_type),
                      color=hash_type_colors[hash_type], 
                      lw=3, mec='black', markersize=8, alpha=1)
            
        # Plot Average Latency (second row)
        for hash_type in ordered_hash_types:
            if hash_type not in avg_latency_data:
                continue
            values = avg_latency_data[hash_type]
            threads = sorted(values.keys())
            x_positions = [position_mapping[thread] for thread in threads]
            avg_latency_metrics = [values[thread]['avg_latency'] for thread in threads]
            
            axs[1, col_idx].plot(x_positions, avg_latency_metrics, 
                      marker=hash_type_markers[hash_type],
                      label=hash_type_to_label(hash_type), 
                      color=hash_type_colors[hash_type], 
                      lw=3, mec='black', markersize=8, alpha=1)
            
        # Plot P99 Latency (third row)
        for hash_type in ordered_hash_types:
            if hash_type not in p99_latency_data:
                continue
            values = p99_latency_data[hash_type]
            threads = sorted(values.keys())
            x_positions = [position_mapping[thread] for thread in threads]
            p99_latency_metrics = [values[thread]['p99_latency'] for thread in threads]
            
            axs[2, col_idx].plot(x_positions, p99_latency_metrics, 
                      marker=hash_type_markers[hash_type],
                      label=hash_type_to_label(hash_type), 
                      color=hash_type_colors[hash_type], 
                      lw=3, mec='black', markersize=8, alpha=1)
        
        # 设置所有子图的X轴刻度和标签
        for row_idx in range(3):
            ax = axs[row_idx, col_idx]
            ax.set_xticks(list(position_mapping.values()))
            ax.set_xticklabels([str(thread) for thread in all_threads], rotation=45)
            ax.grid(axis='y', linestyle='-.')
            
            # Add column title to top row only
            if row_idx == 0:
                ax.set_title(dataset_titles.get(data_dir, data_dir))
            
            # Add x-label only to bottom row
            if row_idx == 2:
                ax.set_xlabel('Number of Threads')
            
            # Set y-scale for latency plots
            if row_idx > 0:  # For latency plots
                ax.set_yscale('log')
            
            # Add y-label only to first column
            if col_idx == 0:
                ax.set_ylabel(row_titles[row_idx])
    
    # Add a single legend for the entire figure
    handles, labels = axs[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 1.05), ncol=len(labels), frameon=False)
    
    plt.tight_layout()
    plt.savefig(f'../out_png/micro.png', bbox_inches='tight')
    plt.savefig(f'../out_pdf/micro.pdf', bbox_inches='tight')

def calculate_relative_performance(data_sets):
    """
    计算并打印MYHASH的性能相对于其他哈希类型的比值
    """
    print("\n=== DREAM (MYHASH) 相对性能比较 ===")
    
    ordered_data_dirs = ['data_insert', 'data_update', 'data_update_zipf99', 'data_read']
    metric_names = {
        'iops': "吞吐量(IOPS)",
        'avg_latency': "平均延迟", 
        'p99_latency': "P99延迟"
    }
    
    for data_dir in ordered_data_dirs:
        if data_dir not in data_sets:
            continue
            
        print(f"\n== {data_dir} 工作负载 ==")
        
        iops_data, avg_latency_data, p99_latency_data = data_sets[data_dir]
        
        # 检查是否有MYHASH数据
        if 'MYHASH' not in iops_data:
            print(f"  没有找到MYHASH数据")
            continue
        
        # 为每种类型的数据进行比较
        for metric_key, metric_label in metric_names.items():
            print(f"\n-- {metric_label} --")
            
            # 选择对应的数据集
            if metric_key == 'iops':
                data_dict = iops_data
                metric_field = 'iops'
                better_is_higher = True  # IOPS越高越好
            elif metric_key == 'avg_latency':
                data_dict = avg_latency_data
                metric_field = 'avg_latency'
                better_is_higher = False  # 延迟越低越好
            else:  # p99_latency
                data_dict = p99_latency_data
                metric_field = 'p99_latency'
                better_is_higher = False  # 延迟越低越好
            
            # 获取其他哈希类型
            other_hash_types = [ht for ht in data_dict.keys() if ht != 'MYHASH']
            
            if not other_hash_types:
                print(f"  没有找到其他哈希类型进行比较")
                continue
            
            # 对每种哈希类型计算相对性能
            for other_hash in other_hash_types:
                # 获取所有共有的线程数
                common_threads = set(data_dict['MYHASH'].keys()) & set(data_dict[other_hash].keys())
                
                if not common_threads:
                    print(f"  DREAM与{hash_type_to_label(other_hash)}没有共同的线程数")
                    continue
                
                # 计算每个线程数上的性能比值
                ratios = []
                single_thread_ratio = None
                
                for thread in sorted(common_threads):
                    myhash_value = data_dict['MYHASH'][thread][metric_field]
                    other_value = data_dict[other_hash][thread][metric_field]
                    
                    if myhash_value is None or other_value is None or other_value == 0:
                        continue
                    
                    # 计算比值，考虑指标的方向（越高越好还是越低越好）
                    ratio = (myhash_value / other_value - 1) * 100  # 转换为百分比
                    
                    ratios.append(ratio)
                    
                    # 记录1线程的性能比值
                    if thread == 1:
                        single_thread_ratio = ratio
                
                if ratios:
                    min_ratio = min(ratios)
                    max_ratio = max(ratios)
                    avg_ratio = sum(ratios) / len(ratios)
                    
                    comparison = "高于" if avg_ratio > 0 else "低于"
                    print(f"  与{hash_type_to_label(other_hash)}相比: {min_ratio:.1f}% 到 {max_ratio:.1f}% {comparison}，平均 {avg_ratio:.1f}%", end="")
                    
                    # 如果有1线程的数据，额外输出
                    if single_thread_ratio is not None:
                        single_thread_comparison = "高于" if single_thread_ratio > 0 else "低于"
                        print(f"，单线程时 {single_thread_ratio:.1f}% {single_thread_comparison}", end="")
                    
                    # 如果有224线程的数据，额外输出
                    if 224 in common_threads:
                        idx_224 = sorted(common_threads).index(224)
                        ratio_224 = ratios[idx_224]
                        comparison_224 = "高于" if ratio_224 > 0 else "低于"
                        print(f"，224线程时 {ratio_224:.1f}% {comparison_224}")
                    else:
                        print()  # 换行

def main(data_root_dir):
    # 我们只关心这四个数据集
    target_data_dirs = ['data_insert', 'data_update', 'data_update_zipf99', 'data_read']
    all_data_sets = {}
    
    data_dirs = [d for d in os.listdir(data_root_dir) if os.path.isdir(os.path.join(data_root_dir, d)) and d in target_data_dirs]
    
    for data_dir in data_dirs:
        hash_types = [d for d in os.listdir(os.path.join(data_root_dir, data_dir)) if os.path.isdir(os.path.join(data_root_dir, data_dir, d))]
        
        iops_data = {}
        avg_latency_data = {}
        p99_latency_data = {}
        
        for hash_type in hash_types:
            iops_data[hash_type] = {}
            avg_latency_data[hash_type] = {}
            p99_latency_data[hash_type] = {}
            
            hash_dir = os.path.join(data_root_dir, data_dir, hash_type)
            thread_dirs = [d for d in os.listdir(hash_dir) if os.path.isdir(os.path.join(hash_dir, d))]
            
            for thread_dir in thread_dirs:
                thread_count = int(thread_dir)
                file_paths = [os.path.join(hash_dir, thread_dir, f) for f in os.listdir(os.path.join(hash_dir, thread_dir)) if f.startswith('out')]
                
                iops_list = []
                avg_latency_list = []
                p99_latency_list = []
                
                for file_path in file_paths:
                    iops, avg_latency, p99_latency = parse_txt_file(file_path)
                    if iops is not None:
                        iops_list.append(iops)
                    if avg_latency is not None:
                        avg_latency_list.append(avg_latency)
                    if p99_latency is not None:
                        p99_latency_list.append(p99_latency)
                
                # Check for significant IOPS variations, maybe old out.txt files are not cleaned up
                if len(iops_list) > 1:
                    avg_iops = sum(iops_list) / len(iops_list)
                    max_deviation = max([abs(iops - avg_iops) / avg_iops for iops in iops_list])
                    if max_deviation > 0.1:  # If any value deviates more than 10% from the average
                        print(f"WARNING: Significant IOPS variation detected in {data_dir}/{hash_type}, threads={thread_count}")
                        print(f"  IOPS values: {iops_list}")
                        print(f"  Average: {avg_iops:.2f}, Max deviation: {max_deviation*100:.2f}%")
                
                avg_iops = sum(iops_list) / len(iops_list) if iops_list else None
                avg_avg_latency = sum(avg_latency_list) / len(avg_latency_list) if avg_latency_list else None
                avg_p99_latency = sum(p99_latency_list) / len(p99_latency_list) if p99_latency_list else None
                
                iops_data[hash_type][thread_count] = {'iops': avg_iops}
                avg_latency_data[hash_type][thread_count] = {'avg_latency': avg_avg_latency}
                p99_latency_data[hash_type][thread_count] = {'p99_latency': avg_p99_latency}
        
        # 存储每个数据集的分析结果
        all_data_sets[data_dir] = (iops_data, avg_latency_data, p99_latency_data)
    
    # 绘制组合图
    plot_combined_data(all_data_sets)
    
    # 计算并打印MYHASH的相对性能
    calculate_relative_performance(all_data_sets)

if __name__ == '__main__':
    main('../data')