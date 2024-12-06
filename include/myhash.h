#pragma once
#include "sephash.h"

namespace MYHASH
{
    using namespace SEPHASH;  // 引入整个SEPHASH命名空间
    using SEPHASH::SLOT_PER_SEG;
    class Client : public SEPHASH::Client
    {
    public:
#if RDMA_SIGNAL
        Client(Config &config, ibv_mr *_lmr, std::vector<rdma_client *> _clis, std::vector<rdma_conn *> _conns, rdma_conn *_wowait_conn, rdma_conn *_signal_conn,
               uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id) : SEPHASH::Client(config, _lmr, _clis[0], _conns[0], _wowait_conn, _signal_conn, _machine_id, _cli_id, _coro_id)
        {
            clis = _clis;
            conns = _conns;
        }
        // 将旧构造函数声明为delete，禁止使用
        Client(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn,
               rdma_conn *_wowait_conn, rdma_conn *_signal_conn,
               uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id) = delete;

        // : SEPHASH::Client(config, _lmr, _cli, _conns[0], _wowait_conn, _signal_conn, _machine_id, _cli_id, _coro_id) {}
        #else
        //         Client(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, rdma_conn *_wowait_conn,
        //                uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id) : SEPHASH::Client(config, _lmr, _cli, _conn, _wowait_conn, _machine_id, _cli_id, _coro_id) {}
        #endif
        task<> insert(Slice *key, Slice *value);

    protected:
#if !LARGER_FP_FILTER_GRANULARITY // 只是为了通过编译，MYHASH只会在LARGER_FP_FILTER_GRANULARITY=1时使用
        CurSegMeta *seg_meta[DIR_SIZE]; // 本地缓存CurSegMeta
#endif
    };

    class Server : public SEPHASH::Server
    {
    public:
        Server(Config &config) : SEPHASH::Server(config) {}
    };
} // namespace MYHASH