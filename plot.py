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

def plot_data_subplots(iops_data, avg_latency_data, p99_latency_data):
    fig, axs = plt.subplots(1, 3, figsize=(10, 3))
    
    for hash_type, values in iops_data.items():
        threads = sorted(values.keys())
        iops_metrics = [values[thread]['iops'] for thread in threads]
        axs[0].plot(threads, iops_metrics, marker='o', label=hash_type)
        axs[0].set_xticks(threads)
    
    for hash_type, values in avg_latency_data.items():
        threads = sorted(values.keys())
        avg_latency_metrics = [values[thread]['avg_latency'] for thread in threads]
        axs[1].plot(threads, avg_latency_metrics, marker='o', label=hash_type)
        axs[1].set_xticks(threads)
    
    for hash_type, values in p99_latency_data.items():
        threads = sorted(values.keys())
        p99_latency_metrics = [values[thread]['p99_latency'] for thread in threads]
        axs[2].plot(threads, p99_latency_metrics, marker='o', label=hash_type)
        axs[2].set_xticks(threads)
    
    axs[0].set_xlabel('Number of Threads')
    axs[0].set_ylabel('IOPS (Kops)')
    axs[0].set_title('IOPS')
    axs[0].legend()
    axs[0].grid(True)
    
    axs[1].set_xlabel('Number of Threads')
    axs[1].set_ylabel('Latency (us, log scale)')
    axs[1].set_title('Average Latency')
    axs[1].set_yscale('log')  # 使用对数坐标轴
    axs[1].legend()
    axs[1].grid(True)
    
    axs[2].set_xlabel('Number of Threads')
    axs[2].set_ylabel('Latency (us, log scale)')
    axs[2].set_title('99% Latency')
    axs[2].set_yscale('log')  # 使用对数坐标轴
    axs[2].legend()
    axs[2].grid(True)
    
    plt.tight_layout()
    plt.savefig('combined_metrics.png')

def main(data_dir):
    hash_types = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
    
    iops_data = {}
    avg_latency_data = {}
    p99_latency_data = {}
    
    for hash_type in hash_types:
        iops_data[hash_type] = {}
        avg_latency_data[hash_type] = {}
        p99_latency_data[hash_type] = {}
        
        hash_dir = os.path.join(data_dir, hash_type)
        thread_dirs = [d for d in os.listdir(hash_dir) if os.path.isdir(os.path.join(hash_dir, d))]
        
        for thread_dir in thread_dirs:
            thread_count = int(thread_dir)
            file_path = os.path.join(hash_dir, thread_dir, 'out192.168.98.71.txt')
            iops, avg_latency, p99_latency = parse_txt_file(file_path)
            iops_data[hash_type][thread_count] = {'iops': iops}
            avg_latency_data[hash_type][thread_count] = {'avg_latency': avg_latency}
            p99_latency_data[hash_type][thread_count] = {'p99_latency': p99_latency}
    
    plot_data_subplots(iops_data, avg_latency_data, p99_latency_data)

if __name__ == '__main__':
    main('data')