# ../test.sh insert run "1,4,8,16,32,56,112,168" MYHASH
# ../test.sh read run "1,4,8,16,32,56,112,168" MYHASH,RACE
# ../test.sh update run "1,4,8,16,32,56,112,168" MYHASH,RACE
# ../test.sh update_zipf99 run "1,4,8,16,32,56,112,168" MYHASH,RACE

# FIXME: 目前74被占用，224先不跑

../test.sh insert run "224" MYHASH
../test.sh read run "224" MYHASH,RACE
../test.sh update run "224" MYHASH,RACE
../test.sh update_zipf99 run "224" MYHASH,RACE

# 我想知道是不是一个MN超过112线程会0xc，然后彻底解决0xc
# 每个CN 28，共112，0xc
# 3cn 共84，OK 第二次0xc
# 4cn 16 64, OK
# 4cn 24 96, 0xc
# 4cn 20 80, 0xc (可能也和cn数量有关)
# 4cn 18 72, OK 第二次0xc
# 或者上本地锁，直到返回，可能降低扩展
# 或者就3cn，也不行

# 可以考虑缩小mainseg，加速合并
# 调大重试间隔？这是可以的，但降低IOPS。看下能不出错的最小间隔

# 感觉完全可以深度一开始就16，合并时间长有可能是分裂导致的
# 不是分裂？还没分裂就挂了
# 先调小mainseg，也不行
# 可能是只能有固定数量的client在等合并？试一下每个CN1线程？
# 改为信号量？

# 可能不是时间的原因，而是MN的并发客户端太多。
# 是时间原因。rnr_retry改成0就不行了。但7好像比6长挺多的。
# 不是！是retry_cnt?但retry_cnt=0也可以运行，而rnr_retry=0就不行。
# send确实是可以无限期重试的。如果设置成不无限重试，是返回0xd IBV_WC_RNR_RETRY_EXC_ERR
# 0xc IBV_WC_RETRY_EXC_ERR 可能是因为线程太多？或者是远端无响应（连NACK也没返回）

# https://www.rdmamojo.com/2013/01/26/ibv_post_send/
# Hi, Dotan.  嗨，Dotan。
# I have a question about the parallel RDMA READ. Since RDMA is a async model, before we finished a RDMA READ, we can launch another, so there is a lot of unfinished RDMA READ at a time, the number of this RDMA READ operation may exceed the initiator_depth and responder resource. What will happen when exceed? does the NIC will launch the RDMA READ as common, or it will wait until the number of unfinished RDMA READ do not exceed?
# 我有一个关于并行 RDMA READ 的问题。由于 RDMA 是一个异步模型，在我们完成一个 RDMA READ 之前，我们可以启动另一个，所以一次有很多未完成的 RDMA READ，这个 RDMA READ 作的数量可能会超过 initiator_depth 和响应方资源。超过时会发生什么情况？网卡是将 RDMA READ 作为通用启动，还是会等到未完成的 RDMA READ 数量不超过？

# I keep the parallel RDMA READ model in a cluster, when I do not limit the parallel number, I failed with IBV_WC_RETRY_EXC_ERR, but when I limit the number of parallel RDMA READ, I can success.
# 我将并行 RDMA READ 模型保存在一个集群中，当我不限制并行数量时，我失败了 IBV_WC_RETRY_EXC_ERR，但是当我限制并行 RDMA READ 的数量时，我可以成功。

# Is there any limit for parallel RDMA READ? or we should avoid this. Thanks!
# 并行 RDMA READ 有限制吗？或者我们应该避免这种情况。谢谢！

# max_qp_rd_atom, initiator_depth,
# https://www.rdmamojo.com/2012/07/13/ibv_query_device/
# max_qp_rd_atom: 16      // 此设备启动 RDMA 读取和原子作的每个 QP 的最大深度，如果支持的话
#  限制同时send不能超过16？论文里说下.未完成的send即使suspend仍然占用，否则0xc

    #    responder_resources
    #           The maximum number of outstanding RDMA read and atomic
    #           operations that the local side will accept from the remote
    #           side.  Applies only to RDMA_PS_TCP.  This value must be
    #           less than or equal to the local RDMA device attribute
    #           max_qp_rd_atom and remote RDMA device attribute
    #           max_qp_init_rd_atom.  The remote endpoint can adjust this
    #           value when accepting the connection.

    #    initiator_depth
    #           The maximum number of outstanding RDMA read and atomic
    #           operations that the local side will have to the remote
    #           side.  Applies only to RDMA_PS_TCP.  This value must be
    #           less than or equal to the local RDMA device attribute
    #           max_qp_init_rd_atom and remote RDMA device attribute
    #           max_qp_rd_atom.  The remote endpoint can adjust this value
    #           when accepting the connection.

# 每个线程有1QP，对应接收方的1QP，因此这个QP最多只能有16 进行中请求

# IMPORTANT: 整个设备只能有16个进行中请求，同时利用两个网卡获得更高的并发数