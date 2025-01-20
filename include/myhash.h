#pragma once
#include <shared_mutex>
#include "sephash.h"

namespace MYHASH
{
    using namespace SEPHASH;  // 引入整个SEPHASH命名空间
    using SEPHASH::SLOT_PER_SEG;
    class Client : public SEPHASH::Client
    {
    public:
#if RDMA_SIGNAL
        Client(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, rdma_conn *_xrc_conn, rdma_conn *_wowait_conn,
               uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id) : SEPHASH::Client(config, _lmr, _cli, _conn, _wowait_conn, _machine_id, _cli_id, _coro_id), xrc_conn(_xrc_conn)
        {
        }
        // 将旧构造函数声明为delete，禁止使用
        Client(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn,
               rdma_conn *_wowait_conn, uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id) = delete;
#endif
        task<> insert(Slice *key, Slice *value);

    protected:
        rdma_conn *xrc_conn;
#if !LARGER_FP_FILTER_GRANULARITY // 只是为了通过编译，MYHASH只会在LARGER_FP_FILTER_GRANULARITY=1时使用
        std::unordered_map<size_t, CurSegMeta> seg_meta; // 本地缓存CurSegMeta
#endif
#if SPLIT_LOCAL_LOCK
        static std::vector<std::shared_mutex> segloc_locks;
#endif
        task<> Split(uint64_t seg_loc, uintptr_t seg_ptr, CurSegMeta *old_seg_meta);
        uint64_t merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg, uint64_t local_depth);
    };

    class Server : public SEPHASH::Server
    {
    public:
        Server(Config &config) : SEPHASH::Server(config) {}
    };
} // namespace MYHASH