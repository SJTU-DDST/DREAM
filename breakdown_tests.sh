# 测insert
# Base (SepHash, RACE-Partition) 已经有了
# 委托并发控制&RTT-less客户端合并 (NoOpt)
../test.sh insert run "" "MYHASH-NoOpt"
# 乐观分裂 (MYHASH)