import os
import re
import matplotlib.pyplot as plt

def parse_txt_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    
    iops_match = re.search(r'Run IOPS:(\d+\.\d+)Kops', content)
    iops = float(iops_match.group(1)) if iops_match else None
    
    avg_latency_matches = re.findall(r'avg latency: (\d+\.\d+) us', content)
    avg_latencies = [float(lat) for lat in avg_latency_matches]
    
    p99_latency_matches = re.findall(r'99% latency: (\d+\.\d+) us', content)
    p99_latencies = [float(lat) for lat in p99_latency_matches]
    
    avg_latency = sum(avg_latencies) / len(avg_latencies) if avg_latencies else None
    p99_latency = sum(p99_latencies) / len(p99_latencies) if p99_latencies else None
    
    return iops, avg_latency, p99_latency

def plot_data_subplots(iops_data, avg_latency_data, p99_latency_data, data_dir):
    fig, axs = plt.subplots(1, 3, figsize=(10, 3))
    
    # 获取所有线程数，以便统一横坐标
    all_threads = set()
    for hash_type in iops_data:
        all_threads.update(iops_data[hash_type].keys())
    all_threads = sorted(list(all_threads))
    
    # 创建非线性映射，将线程数映射到均匀的位置
    position_mapping = {thread: i for i, thread in enumerate(all_threads)}
    
    for hash_type, values in iops_data.items():
        threads = sorted(values.keys())
        # 使用映射后的位置作为X轴位置
        x_positions = [position_mapping[thread] for thread in threads]
        iops_metrics = [values[thread]['iops'] for thread in threads]
        axs[0].plot(x_positions, iops_metrics, marker='o', label=hash_type)
        
    for hash_type, values in avg_latency_data.items():
        threads = sorted(values.keys())
        x_positions = [position_mapping[thread] for thread in threads]
        avg_latency_metrics = [values[thread]['avg_latency'] for thread in threads]
        axs[1].plot(x_positions, avg_latency_metrics, marker='o', label=hash_type)
        
    for hash_type, values in p99_latency_data.items():
        threads = sorted(values.keys())
        x_positions = [position_mapping[thread] for thread in threads]
        p99_latency_metrics = [values[thread]['p99_latency'] for thread in threads]
        axs[2].plot(x_positions, p99_latency_metrics, marker='o', label=hash_type)
    
    # 设置所有子图的X轴刻度和标签
    for ax in axs:
        ax.set_xticks(list(position_mapping.values()))
        ax.set_xticklabels([str(thread) for thread in all_threads], rotation=45)
    
    axs[0].set_xlabel('Number of Threads')
    axs[0].set_ylabel('IOPS (Kops)')
    axs[0].set_title('IOPS')
    axs[0].legend()
    axs[0].grid(True)
    
    axs[1].set_xlabel('Number of Threads')
    # axs[1].set_ylabel('Latency (us)')
    axs[1].set_ylabel('Latency (us, log scale)')
    axs[1].set_title('Average Latency')
    axs[1].set_yscale('log')  # 使用对数坐标轴
    # axs[1].set_ylim(0, 500)
    axs[1].legend()
    axs[1].grid(True)
    
    axs[2].set_xlabel('Number of Threads')
    axs[2].set_ylabel('Latency (us, log scale)')
    axs[2].set_title('99% Latency')
    axs[2].set_yscale('log')  # 使用对数坐标轴
    axs[2].legend()
    axs[2].grid(True)
    
    plt.tight_layout()
    plt.savefig(f'../out/{data_dir}.png')

def main(data_root_dir):
    data_dirs = [d for d in os.listdir(data_root_dir) if os.path.isdir(os.path.join(data_root_dir, d))]
    
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
                
                avg_iops = sum(iops_list) / len(iops_list) if iops_list else None
                avg_avg_latency = sum(avg_latency_list) / len(avg_latency_list) if avg_latency_list else None
                avg_p99_latency = sum(p99_latency_list) / len(p99_latency_list) if p99_latency_list else None
                
                iops_data[hash_type][thread_count] = {'iops': avg_iops}
                avg_latency_data[hash_type][thread_count] = {'avg_latency': avg_avg_latency}
                p99_latency_data[hash_type][thread_count] = {'p99_latency': avg_p99_latency}
        
        plot_data_subplots(iops_data, avg_latency_data, p99_latency_data, data_dir)

if __name__ == '__main__':
    main('../data')