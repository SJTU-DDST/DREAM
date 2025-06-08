# 测insert
# Base (SepHash, RACE-Partition) 已经有了
# 委托并发控制&RTT-less客户端合并 (NoOpt)
../test.sh insert run "1,4,8,16,32,56,112,168" "MYHASH-NoOpt"
# 乐观分裂 (MYHASH)