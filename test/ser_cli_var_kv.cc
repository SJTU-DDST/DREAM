#include "clevel.h"
#include "clevel_filter.h"
#include "clevel_single_filter.h"
#include "generator.h"
#include "plush.h"
#include "plush_single_filter.h"
#include "race.h"
#include "sephash.h"
#include "sephash_zip.h"
#include "myhash.h"
#include "split_batch.h"
#include "split_hash.h"
#include "split_hash_idle.h"
#include "split_inline_dep.h"
#include "split_search_base.h"
#include "split_search_fptable.h"
#include "split_search_fptable_wocache.h"
#include <set>
#include <stdint.h>
#include <barrier>
#include <csignal>
#include <functional>
#define ORDERED_INSERT
#define ALLOW_KEY_OVERLAP // 不同客户端之间允许key重叠，默认打开，只有RACE-Partitioned才关闭
#define FIXED_LOAD_SETUP // 最多16个线程执行load，缓解load线程太多导致争用太多的问题
#define ONLY_FIRST_CORO_START // 每台机器只有第一个线程的第一个协程调用cli->start和cli->stop，机器内部使用本地的barrier协调，减少网络开销
// #define SPLIT_DEV_NUMA // 打开此宏启用多网卡+NUMA亲和
#ifdef SPLIT_DEV_NUMA
#include <numa.h>
#include <pthread.h>
#endif
Config config;
uint64_t load_num;

using ClientType = HASH_TYPE::Client;
using ServerType = HASH_TYPE::Server;
using Slice = HASH_TYPE::Slice;

constexpr uint64_t key_len = 2; // 2 * 8B = 16B
constexpr uint64_t value_len = 32;

void GenKey(uint64_t key, uint64_t *tmp_key)
{
    uint64_t data_key;
#ifdef ORDERED_INSERT
    data_key = key;
#else
    data_key = FNVHash64(key);
#endif
    for (int i = 0; i < key_len; i++)
        tmp_key[i] = data_key;
}

std::unique_ptr<std::barrier<>> barrier;

// hash函数用于key路由到server
inline size_t key_server_hash(uint64_t key, size_t num_server) {
    return key % num_server;
}

template <class Client>
    requires KVTrait<Client, Slice *, Slice *>
task<> load(std::vector<Client*>& clis, uint64_t cli_id, uint64_t coro_id)
{
    barrier->arrive_and_wait();
#ifdef ONLY_FIRST_CORO_START
    if (cli_id == 0 && coro_id == 0)
        co_await clis[0]->start(config.num_machine);
    barrier->arrive_and_wait();
#else
    co_await clis[0]->start(config.num_machine * config.num_cli * config.num_coro);
#endif

    uint64_t tmp_key[key_len];
    Slice key, value;
    std::string tmp_value = std::string(value_len, '1');
    value.len = tmp_value.length();
    value.data = (char *)tmp_value.data();
    key.len = key_len * sizeof(uint64_t);
    key.data = (char *)tmp_key;
    size_t server_id = key_server_hash(tmp_key[0], clis.size());

#ifdef FIXED_LOAD_SETUP
    // Fixed setup: 1 machine, 16 clients, 1 coroutine
    const uint64_t fixed_num_machine = 1;
    const uint64_t fixed_num_cli = std::min(config.num_cli, 16ul);
    const uint64_t fixed_num_coro = 1;
    uint64_t num_op = load_num / (fixed_num_machine * fixed_num_cli * fixed_num_coro);
// #ifdef ALLOW_KEY_OVERLAP
//     Generator *gen = new seq_gen(load_num);
// #else
    Generator *gen = new seq_gen(num_op);
// #endif
    xoshiro256pp key_chooser;

    if (config.machine_id == 0 && coro_id == 0 && cli_id < fixed_num_cli) {
        for (uint64_t i = 0; i < num_op; i++)
        {
            if (i % 100000 == 0 && !cli_id)
                log_err("cli_id:%lu coro_id:%lu Load Progress: %lu/%lu", cli_id, coro_id, i, num_op);
// #ifdef ALLOW_KEY_OVERLAP // Plush?
//             GenKey(gen->operator()(key_chooser()), tmp_key);
// #else
            GenKey((config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id) * num_op + gen->operator()(key_chooser()),
                   tmp_key);
// #endif
            // log_err("cli_id:%lu coro_id:%lu insert key:%lu", cli_id, coro_id, tmp_key[0]);
            co_await clis[server_id]->insert(&key, &value);
        }
    }
#else
    // Original version
    uint64_t num_op = load_num / (config.num_machine * config.num_cli * config.num_coro);
// #ifdef ALLOW_KEY_OVERLAP
//     Generator *gen = new seq_gen(load_num);
// #else
    Generator *gen = new seq_gen(num_op);
// #endif
    xoshiro256pp key_chooser;
    for (uint64_t i = 0; i < num_op; i++)
    {
        if (i % 100000 == 0)
            log_err("cli_id:%lu coro_id:%lu Load Progress: %lu/%lu", cli_id, coro_id, i, num_op);
// #ifdef ALLOW_KEY_OVERLAP
//         GenKey(gen->operator()(key_chooser()), tmp_key);
// #else
        GenKey((config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id) * num_op + gen->operator()(key_chooser()),
               tmp_key);
// #endif
        co_await clis[server_id]->insert(&key, &value);
    }
#endif

    // All threads wait at barrier before stopping
    barrier->arrive_and_wait();

#ifdef ONLY_FIRST_CORO_START
    // Only first coroutine in each machine calls stop
    if (cli_id == 0 && coro_id == 0) {
        co_await clis[0]->stop();
    }
    
    // Wait for stop to complete before any thread returns
    barrier->arrive_and_wait();
#else
    co_await clis[0]->stop();
#endif
    
    co_return;
}

template <class Client>
    requires KVTrait<Client, Slice *, Slice *>
task<> run(Generator *gen, std::vector<Client*>& clis, uint64_t cli_id, uint64_t coro_id)
{
    barrier->arrive_and_wait();
#ifdef ONLY_FIRST_CORO_START
    if (cli_id == 0 && coro_id == 0)
        co_await clis[0]->start(config.num_machine);
    barrier->arrive_and_wait();
#else
    co_await clis[0]->start(config.num_machine * config.num_cli * config.num_coro);
#endif
    uint64_t tmp_key[key_len];
    char buffer[8192];
    Slice key, value, ret_value, update_value;

    ret_value.data = buffer;

    std::string tmp_value = std::string(value_len, '1');
    value.len = tmp_value.length();
    value.data = (char *)tmp_value.data();

    std::string tmp_value_2 = std::string(value_len, '2');
    update_value.len = tmp_value_2.length();
    update_value.data = (char *)tmp_value_2.data();

    key.len = key_len * sizeof(uint64_t);
    key.data = (char *)&tmp_key;
    size_t server_id = key_server_hash(tmp_key[0], clis.size());

    double op_frac;
    double read_frac = config.insert_frac + config.read_frac;
    double update_frac = config.insert_frac + config.read_frac + config.update_frac;
    xoshiro256pp op_chooser;
    xoshiro256pp key_chooser;
    uint64_t num_op = config.num_op / (config.num_machine * config.num_cli * config.num_coro);
    // uint64_t load_avr = load_num / (config.num_machine * config.num_cli * config.num_coro); // if load_num == 0, load_avr == 0, causing key overlap, and key range is [0, op_per_coro]
    uint64_t load_avr = num_op; // this is correct, key cannot overlap, key range is [coros * num_op, (coros + 1) * num_op]
    for (uint64_t i = 0; i < num_op; i++)
    {
        if (i % 100000 == 0 && !cli_id)
            log_err("cli_id:%lu coro_id:%lu Run Progress: %lu/%lu", cli_id, coro_id, i, num_op);
        op_frac = op_chooser();
        if (op_frac < config.insert_frac)
        {
#ifdef ALLOW_KEY_OVERLAP
            GenKey(load_num + gen->operator()(key_chooser()), tmp_key);
#else
            GenKey(load_num +
                       (config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id) *
                           load_avr +
                       gen->operator()(key_chooser()),
                   tmp_key);
                //    log_err("cli_id:%lu coro_id:%lu insert key:%lu", cli_id, coro_id, tmp_key[0]);
#endif
            co_await clis[server_id]->insert(&key, &value);
        }
        else if (op_frac < read_frac)
        {
            ret_value.len = 0;
#ifdef ALLOW_KEY_OVERLAP
            GenKey(gen->operator()(key_chooser()), tmp_key);
#else
            GenKey((config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id) *
                           load_avr +
                       gen->operator()(key_chooser()),
                   tmp_key);
#endif
            co_await clis[server_id]->search(&key, &ret_value);
            // if (ret_value.len != value.len || memcmp(ret_value.data, value.data, value.len) != 0)
            // {
            //     log_err("[%lu:%lu]wrong value for key:%lu with value:%s expected:%s", cli_id, coro_id, tmp_key[0],
            //             ret_value.data, value.data);
            //     // exit(-1);
            // }
        }
        else if (op_frac < update_frac)
        {
            // update
#ifdef ALLOW_KEY_OVERLAP
            GenKey(gen->operator()(key_chooser()), tmp_key);
#else
            GenKey((config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id) *
                           load_avr +
                       gen->operator()(key_chooser()),
                   tmp_key);
#endif
            co_await clis[server_id]->update(&key, &update_value);
            // auto [slot_ptr, slot] = co_await cli->search(&key, &ret_value);
            // if (slot_ptr == 0ull)
            //     log_err("[%lu:%lu]update for key:%lu result in loss", cli_id, coro_id, tmp_key);
            // else if (ret_value.len != update_value.len || memcmp(ret_value.data, update_value.data, update_value.len)
            // != 0)
            // {
            //     log_err("[%lu:%lu]wrong value for key:%lu with value:%s expected:%s", cli_id, coro_id, tmp_key,
            //             ret_value.data, update_value.data);
            // }
        }
        else
        {
            // delete
#ifdef ALLOW_KEY_OVERLAP
            GenKey(gen->operator()(key_chooser()), tmp_key);
#else
            GenKey((config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id) *
                           load_avr +
                       gen->operator()(key_chooser()),
                   tmp_key);
#endif
            co_await clis[server_id]->remove(&key);
            // uint64_t cnt = 0;
            // while(true){
            //     auto [slot_ptr, slot] = co_await cli->search(&key, &ret_value);
            //     if (slot_ptr != 0ull)
            //         log_err("[%lu:%lu]fail to delete value for key:%lu with slot_ptr:%lx and ret_value:%s", cli_id,
            //         coro_id, tmp_key,slot_ptr,ret_value.data);
            //     else
            //         break;
            //     if(cnt++>=3){
            //         exit(-1);
            //     }
            //     co_await cli->remove(&key);
            // }
        }
    }
    // All threads wait at barrier before stopping
    barrier->arrive_and_wait();
#ifdef ONLY_FIRST_CORO_START
    // Only first coroutine in each machine calls stop
    if (cli_id == 0 && coro_id == 0)
    {
        co_await clis[0]->stop();
    }

    // Wait for stop to complete before any thread returns
    barrier->arrive_and_wait();
#else
    co_await clis[0]->stop();
#endif
    co_return;
}

int main(int argc, char *argv[])
{
    signal(SIGPIPE, SIG_IGN);
    config.ParseArg(argc, argv);
    barrier = std::make_unique<std::barrier<>>(config.num_cli * config.num_coro);
    load_num = config.load_num;
    if (config.is_server)
    {
        ServerType ser(config);
        // while (true)
        //     ;
    }
    else
    {
        uint64_t num_server = config.server_ips.size();
        uint64_t cbuf_size = (1ul << 20) * 500; // 500MB for each lmr, adjust as needed, can be smaller
        // 修正：mem_buf分配空间要乘以num_server，保证每个lmr的地址唯一
        char *mem_buf = (char *)malloc(cbuf_size * (config.num_cli * config.num_coro * num_server + 1));
#ifdef SPLIT_DEV_NUMA
        rdma_dev dev0("mlx5_0", 1, config.gid_idx);
        rdma_dev dev1("mlx5_1", 1, config.gid_idx);
#else
        rdma_dev dev(nullptr, 1, config.gid_idx);
#endif
        std::vector<std::vector<ibv_mr *>> lmrs(config.num_cli * config.num_coro, std::vector<ibv_mr *>(num_server, nullptr));
        std::vector<rdma_client *> rdma_clis(config.num_cli, nullptr);
        std::vector<std::vector<rdma_conn *>> rdma_conns(config.num_cli, std::vector<rdma_conn *>(num_server, nullptr));
        std::vector<std::vector<rdma_conn *>> rdma_wowait_conns(config.num_cli, std::vector<rdma_conn *>(num_server, nullptr));
        std::vector<std::vector<BasicDB *>> clis(config.num_cli * config.num_coro, std::vector<BasicDB *>(num_server, nullptr));
        std::thread ths[80];

        for (uint64_t i = 0; i < config.num_cli; i++)
        {
#ifdef SPLIT_DEV_NUMA
            int half = config.num_cli / 2;
            int numa_node = (i < half) ? 0 : 1;
            rdma_dev &my_dev = (i < half) ? dev0 : dev1;
#else
            rdma_dev &my_dev = dev;
#endif
            // 同一个rdma_cli可以连接rdma_conn，他们共享cq
            rdma_clis[i] = new rdma_client(my_dev, so_qp_cap, rdma_default_tempmp_size, config.max_coro, config.cq_size);
            for (uint64_t s = 0; s < num_server; s++)
            {
                rdma_conns[i][s] = rdma_clis[i]->connect(config.server_ips[s].c_str());
                assert(rdma_conns[i][s] != nullptr);
                rdma_wowait_conns[i][s] = rdma_clis[i]->connect(config.server_ips[s].c_str(), rdma_default_port, {ConnType::Signal, 0});
                assert(rdma_wowait_conns[i][s] != nullptr);
                for (uint64_t j = 0; j < config.num_coro; j++)
                {
                    // 修正：lmr的起始地址要包含server维度，保证每个lmr唯一
                    size_t lmr_idx = (i * config.num_coro + j) * num_server + s;
                    lmrs[i * config.num_coro + j][s] = my_dev.create_mr(cbuf_size, mem_buf + cbuf_size * lmr_idx);
                    BasicDB *cli = new ClientType(config, lmrs[i * config.num_coro + j][s], rdma_clis[i], rdma_conns[i][s], rdma_wowait_conns[i][s], config.machine_id, i, j, s);
                    clis[i * config.num_coro + j][s] = cli;
                }
            }
        }

        // For Rehash Thread
        std::atomic_bool exit_flag{true};
        bool rehash_flag = typeid(ClientType) == typeid(CLEVEL::Client) ||
                           typeid(ClientType) == typeid(ClevelFilter::Client) ||
                           typeid(ClientType) == typeid(ClevelSingleFilter::Client);
        ;

        if (config.machine_id == 0 && rehash_flag)
        {
            // rdma_clis[config.num_cli] =
            //     new rdma_client(dev, so_qp_cap, rdma_default_tempmp_size, config.max_coro, config.cq_size);
            // rdma_conns[config.num_cli] = rdma_clis[config.num_cli]->connect(config.server_ip.c_str());
            // rdma_wowait_conns[config.num_cli] = rdma_clis[config.num_cli]->connect(config.server_ip.c_str());

            // lmrs[config.num_cli * config.num_coro] =
            //     dev.create_mr(cbuf_size, mem_buf + cbuf_size * (config.num_cli * config.num_coro));
            // ClientType *rehash_cli = new ClientType(
            //     config, lmrs[config.num_cli * config.num_coro], rdma_clis[config.num_cli], rdma_conns[config.num_cli],
            //     rdma_wowait_conns[config.num_cli], config.machine_id, config.num_cli, config.num_coro);
            // auto th = [&](rdma_client *rdma_cli) {
            //     // rdma_cli->run(rehash_cli->rehash(exit_flag));
            // };
            // ths[config.num_cli] = std::thread(th, rdma_clis[config.num_cli]);
        }

        printf("Load start\n");
        auto start = std::chrono::steady_clock::now();
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            auto th = [&](rdma_client *rdma_cli, uint64_t cli_id) {
#ifdef SPLIT_DEV_NUMA
                int half = config.num_cli / 2;
                int numa_node = (cli_id < half) ? 0 : 1;
                numa_run_on_node(numa_node);
#endif
                std::vector<task<>> tasks;
                for (uint64_t j = 0; j < config.num_coro; j++)
                {
                    std::vector<ClientType *> &client_vec = reinterpret_cast<std::vector<ClientType *> &>(clis[cli_id * config.num_coro + j]);
                    tasks.emplace_back(load(client_vec, cli_id, j));
                }
                rdma_cli->run(gather(std::move(tasks)));
            };
            ths[i] = std::thread(th, rdma_clis[i], i);
        }
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            ths[i].join();
        }
        auto end = std::chrono::steady_clock::now();
        double op_cnt = 1.0 * load_num;
        double duration = std::chrono::duration<double, std::milli>(end - start).count();
        printf("Load duration:%.2lfms\n", duration);
        printf("Load IOPS:%.2lfKops\n", op_cnt / duration);
        fflush(stdout);
        // ths[config.num_cli].join();

        printf("Run start\n");
#ifdef ALLOW_KEY_OVERLAP
        auto op_per_coro = config.num_op; // load_num for update
#else
        auto op_per_coro = config.num_op / (config.num_machine * config.num_cli * config.num_coro);
#endif
        std::vector<Generator *> gens;
        for (uint64_t i = 0; i < config.num_cli * config.num_coro; i++)
        {
            if (config.pattern_type == 0)
            {
                gens.push_back(new seq_gen(op_per_coro));
            }
            else if (config.pattern_type == 1)
            {
                gens.push_back(new uniform(op_per_coro));
            }
            else if (config.pattern_type == 2)
            {
                gens.push_back(new zipf99(op_per_coro));
            }
            else
            {
                gens.push_back(new SkewedLatestGenerator(op_per_coro));
            }
        }
        start = std::chrono::steady_clock::now();
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            auto th = [&](rdma_client *rdma_cli, uint64_t cli_id) {
#ifdef SPLIT_DEV_NUMA
                int half = config.num_cli / 2;
                int numa_node = (cli_id < half) ? 0 : 1;
                numa_run_on_node(numa_node);
#endif
                std::vector<task<>> tasks;
                for (uint64_t j = 0; j < config.num_coro; j++)
                {
                    std::vector<ClientType *> &client_vec = reinterpret_cast<std::vector<ClientType *> &>(clis[cli_id * config.num_coro + j]);
                    tasks.emplace_back(run(gens[cli_id * config.num_coro + j], client_vec, cli_id, j));
                }
                rdma_cli->run(gather(std::move(tasks)));
            };
            ths[i] = std::thread(th, rdma_clis[i], i);
        }
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            ths[i].join();
        }
        end = std::chrono::steady_clock::now();
        op_cnt = 1.0 * config.num_op;
        duration = std::chrono::duration<double, std::milli>(end - start).count();
        printf("Run duration:%.2lfms\n", duration);
        printf("Run IOPS:%.2lfKops\n", op_cnt / duration);
        fflush(stdout);

        exit_flag.store(false);
        if (config.machine_id == 0 && rehash_flag)
        {
            // ths[config.num_cli].join();
        }
        if (config.machine_id != 0 && rehash_flag)
        {
            // rdma_clis[config.num_cli] =
            //     new rdma_client(dev, so_qp_cap, rdma_default_tempmp_size, config.max_coro, config.cq_size);
            // rdma_conns[config.num_cli] = rdma_clis[config.num_cli]->connect(config.server_ip.c_str());
            // rdma_wowait_conns[config.num_cli] = rdma_clis[config.num_cli]->connect(config.server_ip.c_str());

            // lmrs[config.num_cli * config.num_coro] =
            //     dev.create_mr(cbuf_size, mem_buf + cbuf_size * (config.num_cli * config.num_coro));
            // ClientType *check_cli = new ClientType(
            //     config, lmrs[config.num_cli * config.num_coro], rdma_clis[config.num_cli], rdma_conns[config.num_cli],
            //     rdma_wowait_conns[config.num_cli], config.machine_id, config.num_cli, config.num_coro);

            // auto th = [&](rdma_client *rdma_cli) {
            //     // while(rdma_cli->run(check_cli->check_exit())){
            //     // log_err("waiting for rehash exit");
            //     // }
            // };
            // ths[config.num_cli] = std::thread(th, rdma_clis[config.num_cli]);
            // ths[config.num_cli].join();
        }

        if (config.machine_id == 0)
        {
            // rdma_clis[0]->run(((ClientType*)clis[0])->cal_utilization());
        }

        for (auto gen : gens)
        {
            delete gen;
        }

        // Reset Ser
        if (config.machine_id == 0)
        {
            for (uint64_t s = 0; s < num_server; s++) {
                rdma_clis[0]->run(((ClientType *)clis[0][s])->reset_remote());
            }
        }

        free(mem_buf);
        for (uint64_t i = 0; i < config.num_cli; i++)
        {
            for (uint64_t s = 0; s < num_server; s++)
            {
                for (uint64_t j = 0; j < config.num_coro; j++)
                {
                    rdma_free_mr(lmrs[i * config.num_coro + j][s], false);
                    delete clis[i * config.num_coro + j][s];
                }
                delete rdma_wowait_conns[i][s];
                delete rdma_conns[i][s];
            }
            delete rdma_clis[i];
        }
    }
}