import re
import os
import sys
import pandas as pd
import subprocess

# data_dir="/mnt/nvme1n1/xq/mlsm/database_comparison/titan_3.16_ycsb_a_with_param"
data_dir = sys.argv[1]

if len(sys.argv) >= 3:
    benchmark_log_name = sys.argv[2]
else:
    benchmark_log_name = "benchmark_ycsb_a.t1.s1.log"

# Create a dictionary to store the performance metrics
performance_metrics = {
    'flush_write': [],
    'write_rate': [],
    'blob_file_count': [],
    'blob_size': [],
    'garbage_size': [],
    'space_amp': [],
    'compaction_write_GB': [],
    'compaction_write_rate': [],
    'compaction_read_GB': [],
    'compaction_read_rate': [],
    'compaction_time': [],
    'lsm_size': [],
    'read_gb': [],
    'write_gb': [],
    'write_blob': [],
    'write_blob_overwrite': [],
    'write_amp': [],
    'write_microsecond_median': [],
    'write_microsecond_average': [],
    'read_microsecond_median': [],
    'read_microsecond_average': []
}

def read_performance(benchmark_log_path):
    # print("Current benchmark_log_path:", benchmark_log_path)
    global performance_metrics
    with open(benchmark_log_path, 'r') as file:
        benchmark_log = file.readlines()
        
    # Reverse the benchmark_log
    benchmark_log.reverse()

    ### part 1

    pattern = r"^Cumulative writes: .*"

    # Find the first occurrence of a line starting with "Cumulative writes" from the end
    text = next((line for line in benchmark_log if re.match(pattern, line)), None)
    # text = "Cumulative writes: 25M writes, 25M keys, 25M commit groups, 1.0 writes per commit group, ingest: 29.64 GB, 9.97 MB/s"

    # 使用正则表达式匹配所有的数字，包括小数
    numbers = re.findall(r'\d+\.?\d*', text)
    # Flush/Compaction  read/ write ：FlushCP GroupFlushInPool
    flush_write = numbers[4]

    # Write rate
    write_rate=numbers[5]

  
    ### part 2

    # pattern = r"^Cumulative compaction: .*"
    pattern = r"^Cumulative compaction:(?!.*write-lsm).*"
    # Find the first occurrence of a line starting with "Cumulative writes" from the end
    text = next((line for line in benchmark_log if re.match(pattern, line)), None)
    # text = "Cumulative compaction: 111.44 GB write, 37.50 MB/s write, 89.54 GB read, 30.13 MB/s read, 316.1 seconds"

    # 使用正则表达式匹配所有的数字，包括小数
    numbers = re.findall(r'\d+\.?\d*', text)

    # Compaction write GB 
    compaction_write_GB = numbers[0]
    # Compaction write rate
    compaction_write_rate = numbers[1]
    # Compaction read GB 
    compaction_read_GB = numbers[2]
    # Compaction read rate
    compaction_read_rate = numbers[3]
    # Compaction time 
    compaction_time = numbers[4]


    ### part 3

    pattern = r"^ Sum .*"

    # Find the first occurrence of a line starting with "Cumulative writes" from the end
    text = next((line for line in benchmark_log if re.match(pattern, line)), None)
    # text = " Sum    128/0   14.39 GB        14.39 GB   0.0     75.5    11.6     63.9      56.5      33.4     -7.4       0.0   2.5    299.5    356.6       258       387    0.667    233M    18M"

    # 使用正则表达式匹配所有的数字，包括小数
    numbers = re.findall(r'\d+\.?\d*', text)

    # lsm-tree size
    lsm_size = numbers[2]
    # Read GB: rocksdb/titan:4 terarkdb: 5
    read_gb = numbers[4]
    # Write GB: rocksdb/titan:7 terarkdb: 8
    write_gb = numbers[7]
    # Write amp: rocksdb/titan:10 terarkdb: 12
    write_amp = numbers[10]
    
    
    ### part 4
    
    output_path = os.path.join(os.path.dirname(benchmark_log_path), "output.txt")
    
    with open(output_path, 'r') as file:
        lines = file.readlines()
        
        # Blob file count
        blob_file_count = lines[0].strip()

        # Blob size
        blob_size = lines[1].strip()
        
        
    ### part 5
    
    pattern = r"^Microseconds per write:"
    # Find the first occurrence of a line starting with "Microseconds per write" from the end
    text = next((line for line in benchmark_log if re.match(pattern, line)), None)
    if text:
        # Get the index of the line
        index = benchmark_log.index(text)

        # Get the next two lines
        # Since I have reversed the order, here it should be index-2 and index
        lines = benchmark_log[index-2:index]

        # write microsecond median
        numbers = re.findall(r'\d+\.?\d*', lines[0])
        write_microsecond_median = numbers[1]
        
        # write microsecond average               
        numbers = re.findall(r'\d+\.?\d*', lines[1])
        write_microsecond_average = numbers[1]
    else:
        print("Microseconds per write not found")
    
    
    ### part 6
    
    pattern = r"^Microseconds per read:"
    # Find the first occurrence of a line starting with "Microseconds per read" from the end
    text = next((line for line in benchmark_log if re.match(pattern, line)), None)
    if text:
        # Get the index of the line
        index = benchmark_log.index(text)
        
        # Get the next two lines
        # Since I have reversed the order, here it should be index-2 and index
        lines = benchmark_log[index-2:index]

        # read microsecond median
        numbers = re.findall(r'\d+\.?\d*', lines[0])
        read_microsecond_median = numbers[1]
        
        # read microsecond average
        numbers = re.findall(r'\d+\.?\d*', lines[1])
        read_microsecond_average = numbers[1]
        
        performance_metrics['read_microsecond_median'].append(read_microsecond_median)
        performance_metrics['read_microsecond_average'].append(read_microsecond_average)
    else:
        if 'read_microsecond_median' in performance_metrics:
            del performance_metrics['read_microsecond_median']
            del performance_metrics['read_microsecond_average']
        print("Microseconds per read not found")
        
    ### part 7
    # caclulate the user space and space amp
    
    # workload_path = "/mnt/nvme1n1/zt/YCSB-C/data/workloada-load-10000000-50000000.log_run.formated"
    # keys = set()
    # with open(workload_path, 'r') as file:
    #     lines = file.readlines()
    #     for line in lines:
    #         key = line.split()[1]
    #         keys.add(key)
    
    # 单位：字节
    # user_space = len(keys) * 1000 + sum([len(key) for key in keys])
    
    user_space = 5628895880 # /mnt/nvme1n1/zt/YCSB-C/data/workloada-load-10000000-100000000.log_run.formated
    # user_space = 5604959453 # /mnt/nvme1n1/zt/YCSB-C/data/workloada-load-10000000-50000000.log_run.formated
    
    # Convert blob_size to numeric value
    blob_size_numeric = float(blob_size[:-1])

    # Convert blob_size to actual value based on unit
    unit = blob_size[-1].upper()
    if unit == 'K':
        blob_size_numeric = blob_size_numeric * 1000
    elif unit == 'M':
        blob_size_numeric = blob_size_numeric * 1000000
    elif unit == 'G':
        blob_size_numeric = blob_size_numeric * 1000000000
    elif unit == 'T':
        blob_size_numeric = blob_size_numeric * 1000000000000
    else:
        print("Invalid unit for blob_size")
        
    
    # Space amp
    space_amp = round(blob_size_numeric / user_space, 2)
    
    # garbage size
    garbage_size = blob_size_numeric - user_space
    
    # Convert garbage_size to actual value based on unit
    garbage_size_numeric = float(garbage_size)
    garbage_size_unit = 'B'
    if garbage_size_numeric >= 1024:
        garbage_size_numeric /= 1024
        garbage_size_unit = 'K'
    if garbage_size_numeric >= 1024:
        garbage_size_numeric /= 1024
        garbage_size_unit = 'M'
    if garbage_size_numeric >= 1024:
        garbage_size_numeric /= 1024
        garbage_size_unit = 'G'
    if garbage_size_numeric >= 1024:
        garbage_size_numeric /= 1024
        garbage_size_unit = 'T'

    if garbage_size_unit == 'G':
        garbage_size = f"{garbage_size_numeric:.2f}"
    else:
        garbage_size = f"{garbage_size_numeric:.2f}{garbage_size_unit}"
    
    if blob_size[-1].upper() == 'G':
        blob_size = blob_size[:-1]
        
    ### part 8
    write_blob = 0
    write_blob_overwrite = 0
    LOG_PATH = os.path.join(os.path.dirname(benchmark_log_path), "LOG")
    with open(LOG_PATH, 'r') as file:
        lines = file.readlines()
        for line in lines:
            if "Titan GC job completed" in line:
                # Find the number after ", written:"
                match_written = re.search(r", written:(\d+\.?\d*)", line)
                if match_written:
                    write_blob += int(match_written.group(1))
                # Find the number after ", bytes overwritten:"
                match_overwritten = re.search(r", bytes overwritten:(\d+\.?\d*)", line)
                if match_overwritten:
                    write_blob_overwrite += int(match_overwritten.group(1))
                    
        command = f"grep 'OnFlushCompleted.*output blob' {LOG_PATH} | awk '{{print $12}}' | awk -F':' '{{print $1}}' | tr -d '.' | awk '{{s+=$1}} END {{print s}}'"
        flush_blob_write = subprocess.check_output(command, shell=True)
        write_blob += int(flush_blob_write.decode())
        write_blob = float(write_blob) / 1000000000
        wrie_blob_overwrite = float(write_blob_overwrite) / 1000000000
                
    performance_metrics['flush_write'].append(flush_write)
    performance_metrics['write_rate'].append(write_rate)
    performance_metrics['blob_file_count'].append(blob_file_count)
    performance_metrics['blob_size'].append(blob_size)
    performance_metrics['garbage_size'].append(garbage_size)
    performance_metrics['space_amp'].append(space_amp)
    performance_metrics['compaction_write_GB'].append(compaction_write_GB)
    performance_metrics['compaction_write_rate'].append(compaction_write_rate)
    performance_metrics['compaction_read_GB'].append(compaction_read_GB)
    performance_metrics['compaction_read_rate'].append(compaction_read_rate)
    performance_metrics['compaction_time'].append(compaction_time)
    performance_metrics['lsm_size'].append(lsm_size)
    performance_metrics['read_gb'].append(read_gb)
    performance_metrics['write_gb'].append(write_gb)
    performance_metrics['write_blob'].append(write_blob)
    performance_metrics['write_blob_overwrite'].append(write_blob_overwrite)
    performance_metrics['write_amp'].append(write_amp)
    performance_metrics['write_microsecond_median'].append(write_microsecond_median)
    performance_metrics['write_microsecond_average'].append(write_microsecond_average)


dirs=os.listdir(data_dir)

# delete the dirs that not start with "with_gc"
dirs = [d for d in dirs if os.path.isdir(os.path.join(data_dir, d))]
value_size = [name.split('_')[2] for name in dirs]
blob_file_discardable_ratio = [name.split('_')[7] for name in dirs]

for data_with_param_dir in dirs:
    print("Current data_with_param_dir:", data_with_param_dir)
    benchmark_log_path = os.path.join(data_dir, data_with_param_dir, benchmark_log_name)
    read_performance(benchmark_log_path)
    
# Create a DataFrame from the performance metrics dictionary
df = pd.DataFrame(performance_metrics, index=blob_file_discardable_ratio)
df.insert(0, 'blob_file_discardable_ratio', blob_file_discardable_ratio)
df.insert(1, 'value_size', value_size)
df = df.sort_values(['blob_file_discardable_ratio', 'value_size'])
print(df)

# Output to data_dir
output_file = os.path.join(data_dir, "performance_metrics.csv")
df.to_csv(output_file)
print("Output to", output_file)

# draw=False
draw=False
if not draw:
    exit()
    
# draw
import matplotlib.pyplot as plt

# Plot the line chart

df['blob_file_discardable_ratio'] = df['blob_file_discardable_ratio'].astype(float)
df['value_size'] = df['value_size'].astype(float)
df['write_amp'] = df['write_amp'].astype(float)
df['blob_size'] = df['blob_size'].astype(float)

plt.figure()
df_filtered = df[df['value_size'] == 1024]
plt.plot(df_filtered['blob_file_discardable_ratio'], df_filtered['write_amp'], marker='o')
plt.xlabel('blob_file_discardable_ratio')
plt.ylabel('write_amp')
plt.title('Write Amp vs blob_file_discardable_ratio (Value Size: 1024)')
plt.savefig(os.path.join(data_dir, "write_amp_vs_blob_file_discardable_ratio_value_size_1024.png"))
plt.close()
print("Output to", os.path.join(data_dir, "write_amp_vs_blob_file_discardable_ratio_value_size_1024.png"))

plt.figure()
df_filtered = df[df['value_size'] == 4096]
plt.plot(df_filtered['blob_file_discardable_ratio'], df_filtered['write_amp'], marker='o')
plt.xlabel('blob_file_discardable_ratio')
plt.ylabel('write_amp')
plt.title('Write Amp vs blob_file_discardable_ratio (Value Size: 4096)')
plt.savefig(os.path.join(data_dir, "write_amp_vs_blob_file_discardable_ratio_value_size_4096.png"))
plt.close()
print("Output to", os.path.join(data_dir, "write_amp_vs_blob_file_discardable_ratio_value_size_4096.png"))

plt.figure()
df_filtered = df[df['value_size'] == 1024]
plt.plot(df_filtered['blob_file_discardable_ratio'], df_filtered['blob_size'], marker='o')
plt.xlabel('blob_file_discardable_ratio')
plt.ylabel('blob_size')
plt.title('blob_size vs blob_file_discardable_ratio (Value Size: 1024)')
plt.savefig(os.path.join(data_dir, "blob_size_vs_blob_file_discardable_ratio_value_size_1024.png"))
plt.close()
print("Output to", os.path.join(data_dir, "blob_size_vs_blob_file_discardable_ratio_value_size_1024.png"))

plt.figure()
df_filtered = df[df['value_size'] == 4096]
plt.plot(df_filtered['blob_file_discardable_ratio'], df_filtered['blob_size'], marker='o')
plt.xlabel('blob_file_discardable_ratio')
plt.ylabel('blob_size')
plt.title('blob_size vs blob_file_discardable_ratio (Value Size: 4096)')
plt.savefig(os.path.join(data_dir, "blob_size_vs_blob_file_discardable_ratio_value_size_4096.png"))
plt.close()
print("Output to", os.path.join(data_dir, "blob_size_vs_blob_file_discardable_ratio_value_size_4096.png"))
