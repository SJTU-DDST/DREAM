#!/bin/bash

#define TEST_SEND 1 // 启用SEND
#define SEND_TO_CURSEG 1 // SEND写入CurSeg而非TempSeg
#define REMOVE_CAS 1 // 不再用CAS写入CurSeg。配合SEND_TO_CURSEG使用，后续去掉。
#define RDMA_SIGNAL 1 // 创建专用于SEND合并完成信号的QP（未实现完）。
#define WRITE_WITH_IMM_SIGNAL 1 // 客户端使用RDMA WRITE WITH IMM发送合并完成信号，服务端用poll opcode接收。而非轮询等待CurSeg的sign被反转。
#define CORO_DEBUG 1 // 协程调试
#define FAA_DETECT_FULL 1 // 使用fetch_add检测CurSeg是否满
#define NO_BEFORE_READ 1 // 不再在SEND前读取CurSegMeta->slot_cnt，而是在SEND后读取。
#define LARGER_FP_FILTER_GRANULARITY 1 // 使用更大的FP过滤粒度，避免写入FP过滤器前需要先读取。现在先用每个FP占用64bit粒度，可能可以改成8bit。TODO: 读取filter的地方还没改。

#define SEND_MULTI_SEG 1 // 启用多个TempSeg，用于测试多个TempSeg的性能。

#define LARGE_MAIN_SEG 0 // 使用大的main_seg，相当于禁用分裂

# 定义要使用的宏
DEFINES="-DMODIFIED=1 -DTEST_SEND=1 -DSEND_TO_CURSEG=1 -DREMOVE_CAS=1 -DRDMA_SIGNAL=1 -DWRITE_WITH_IMM_SIGNAL=1 -DCORO_DEBUG=1 -DFAA_DETECT_FULL=1 -DNO_BEFORE_READ=1 -DLARGER_FP_FILTER_GRANULARITY=1 -DSEND_MULTI_SEG=1 -DLARGE_MAIN_SEG=0"

# 创建temp文件夹
mkdir -p temp/include
mkdir -p temp/src

# 处理include文件夹中的.h文件
for file in include/*.h; do
    filename=$(basename "$file")
    unifdef $DEFINES "$file" > "temp/include/$filename"
done

# 处理src文件夹中的.cc文件
for file in src/*.cc; do
    filename=$(basename "$file")
    unifdef $DEFINES "$file" > "temp/src/$filename"
done

echo "处理完成，结果已保存到temp文件夹中。"