#pragma once
#include <shared_mutex>
#include "sephash.h"
enum
{
    rdma_exchange_proto_invaild,
    rdma_exchange_proto_setup,
    rdma_exchange_proto_ready,
    rdma_exchange_proto_reconnect,
};

namespace MYHASH
{
    using namespace SEPHASH;  // 引入整个SEPHASH命名空间
    class Client : public SEPHASH::Client
    {
    public:
#if RDMA_SIGNAL
        Client(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, rdma_conn *_wowait_conn,
               uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id, uint64_t _server_id) : SEPHASH::Client(config, _lmr, _cli, _conn, _wowait_conn, _machine_id, _cli_id, _coro_id, _server_id)
        {
            xrc_conn = _cli->connect(config.server_ips[_server_id].c_str(), rdma_default_port, {ConnType::XRC_SEND, 0});
        }
#else
        Client(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, rdma_conn *_wowait_conn,
               uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id, uint64_t _server_id)
            : SEPHASH::Client(config, _lmr, _cli, _conn, _wowait_conn, _machine_id, _cli_id, _coro_id, _server_id)
        {
            // TicketHash直接复用SEPHASH的初始化，无需额外处理
        }
#endif
        ~Client()
        {
            delete xrc_conn;
        }
        
        // 销毁并重新创建xrc_conn
        void recreate_xrc_conn()
        {
            static std::mutex recreate_mutex;
            std::lock_guard<std::mutex> lock(recreate_mutex);

            int sock = xrc_conn->sock;
            // log_err("销毁并重新创建xrc_conn, sock: %d", xrc_conn->sock);
            xrc_conn->send_exchange(rdma_exchange_proto_reconnect, {ConnType::XRC_SEND, 0});
            xrc_conn->sock = -1; // 这样不会销毁sock
            delete xrc_conn;
            xrc_conn = nullptr;
#if RDMA_SIGNAL
            // xrc_conn = cli->connect(config.server_ip.c_str(), rdma_default_port, {ConnType::XRC_SEND, 0});
            // log_err("cli->reconnect重新创建xrc_conn开始, sock: %d", sock);
            xrc_conn = cli->reconnect(sock, {ConnType::XRC_SEND, 0});
            // log_err("cli->reconnect重新创建xrc_conn成功, sock: %d", sock);
#endif
        }

        task<> insert(Slice *key, Slice *value);
        task<> update(Slice *key, Slice *value);
        task<> remove(Slice *key);

    protected:
        rdma_conn *xrc_conn;
#if !LARGER_FP_FILTER_GRANULARITY // 只是为了通过编译，MYHASH只会在LARGER_FP_FILTER_GRANULARITY=1时使用
        std::unordered_map<size_t, CurSegMeta> seg_meta; // 本地缓存CurSegMeta
#endif
#if SPLIT_LOCAL_LOCK
        static std::vector<std::shared_mutex> segloc_locks;
#endif
        task<> Split(uint64_t seg_loc, uintptr_t seg_ptr, CurSegMeta *old_seg_meta);
#if READ_FULL_KEY_ON_FP_COLLISION
        task<uint64_t> merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg, uint64_t local_depth, bool dedup = false);
#else
        uint64_t merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg, uint64_t local_depth);
#endif
        task<> check_slots(Slot *slots, uint64_t len, std::string desc = "", std::source_location loc = std::source_location::current());
        task<> check_slot(uint64_t offset = 0x3380, std::string desc = "", std::source_location loc = std::source_location::current());
    };

    class Server : public SEPHASH::Server
    {
    public:
        Server(Config &config) : SEPHASH::Server(config) {}
    };
} // namespace MYHASH