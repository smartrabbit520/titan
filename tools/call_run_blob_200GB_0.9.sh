#!/bin/bash

function call_run_blob() {
  local wal_dir num_keys db_dir output_dir enable_blob_file enable_blob_gc
  wal_dir=$1
  num_keys=$2
  db_dir=$3
  output_dir=$4
  enable_blob_file=${5:1}
  enable_blob_gc=${6:-1}
  local value_size=${7:-1}
  local blob_file_discardable_ratio=${8:-0.3}
  local read_ycsb_file=${9:-"error"}
  local write_buffer_size=$((100 * 1024 * 1024))
  local blob_file_target_size=$((256 * 1024 * 1024))
  local titan_max_background_gc=1
  echo "db_dir: $db_dir"

  COMPRESSION_TYPE=none BLOB_COMPRESSION_TYPE=none WAL_DIR=$wal_dir \
   NUM_KEYS=$num_keys DB_DIR=$db_dir \
   OUTPUT_DIR=$output_dir ENABLE_BLOB_FILES=$enable_blob_file \
   ENABLE_BLOB_GC=$enable_blob_gc  \
   VALUE_SIZE=$value_size \
   READ_YCSB_FILE=$read_ycsb_file \
   TITAN_MAX_BACKGROUND_GC=$titan_max_background_gc \
   BLOB_FILE_TARGET_SIZE=$blob_file_target_size \
   WRITE_BUFFER_SIZE=$write_buffer_size NUM_THREADS=1 \
   BLOB_FILE_DISCARDABLE_RATIO=$blob_file_discardable_ratio \
   ./run_blob_bench.sh
}

now_time=$(date +"%Y-%m-%d-%H:%M:%S")
# now_time=2024-04-26-09:59:02

num_keys=5000000
enable_blob_file=1
enable_blob_gc=true
read_ycsb_files=(
  /mnt/nvme0n1/YCSB-C/data/workloada_200GB_0.99_65536_zipfian.log_run.formated
  /mnt/nvme0n1/YCSB-C/data/workloada_200GB_0.99_16384_zipfian.log_run.formated
  /mnt/nvme0n1/YCSB-C/data/workloada_200GB_0.99_4096_zipfian.log_run.formated
  /mnt/nvme0n1/YCSB-C/data/workloada_200GB_0.99_1024_zipfian.log_run.formated
)
value_sizes=(65536 16384 4096 1024)
# value_sizes=(1024)

blob_file_discardable_ratios=(0.2)

for ((i=0; i<${#read_ycsb_files[@]}; i++)); do {
# for read_ycsb_file in "${read_ycsb_files[@]}" ; do {
  read_ycsb_file=${read_ycsb_files[$i]}
  value_size=${value_sizes[$i]}
    echo "read_ycsb_file: $read_ycsb_file"
    workload_info="ycsb_a_100GB_0.9_zipfian_adaptive_sst_file_size"
    db_info=titan_${now_time}_${workload_info}
    db_dir=/mnt/nvme0n1/xq/mlsm/database_comparison/${db_info}
    git_result_dir=/mnt/nvme0n1/xq/git_result/rocksdb_kv_sep/result/${db_info}
    
    # if db_dir not exist, create it
    if [ ! -d "$db_dir" ]; then
      mkdir -p "$db_dir"
    fi

    # for value_size in ${value_sizes[@]} ; do {
      echo "value_size: $value_size"

      for blob_file_discardable_ratio in "${blob_file_discardable_ratios[@]}" ; do {
        with_gc_dir=${db_dir}/value_size_${value_size}_blob_file_discardable_ratio_${blob_file_discardable_ratio}
        log_path=${git_result_dir}/value_size_${value_size}_blob_file_discardable_ratio_${blob_file_discardable_ratio}
        log_file_name=${log_path}/log.txt

        # if log_path not exist, create it
        if [ ! -d "$log_path" ]; then
          mkdir -p "$log_path"
        fi

        call_run_blob  $with_gc_dir $num_keys $with_gc_dir $with_gc_dir $enable_blob_file \
          $enable_blob_gc $value_size $blob_file_discardable_ratio $read_ycsb_file | tee -a $log_file_name

        output_text=${with_gc_dir}/output.txt

        # Clear the output file
        > $output_text

        # Count the number of .blob files
        num_files=$(find $with_gc_dir/titandb -type f -name "*.blob" | wc -l)

        # Calculate the total size of .blob files
        total_size=$(du -cs $with_gc_dir/titandb/*.blob | grep total$ | awk '{print $1 / 1024 / 1024 "G"}')

        # Output the results to the output file
        echo $num_files >> $output_text
        echo $total_size >> $output_text

        echo cp ${with_gc_dir}/benchmark_ycsb_a.t1.s1.log $log_path
        cp ${with_gc_dir}/benchmark_ycsb_a.t1.s1.log $log_path
        echo cp ${with_gc_dir}/LOG $log_path
        cp ${with_gc_dir}/LOG $log_path
        echo cp $output_text $log_path
        cp $output_text $log_path

        find $with_gc_dir -type f -name "*.blob" -delete
        find $with_gc_dir -type f -name "*.sst" -delete

      } & done
      wait
    # } done
python3 ./get_performance.py $db_dir "benchmark_ycsb_a.t1.s1.log"
cp ${db_dir}/performance_metrics.csv $git_result_dir
} done

