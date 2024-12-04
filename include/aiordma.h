#pragma once

#include <cstdint>
#include <mutex>
#include <thread>
#include <coroutine>
#include <unordered_map>
#include <functional>
#include <infiniband/verbs.h>

#include "common.h"

#include "config.h"

#if CORO_DEBUG
#define DEBUG_LOCATION_DECL , const std::source_location &location = std::source_location::current()
#define DEBUG_LOCATION_CALL_ARG , location
#define DEBUG_LOCATION_DEFINE , const std::source_location &location

#define DEBUG_DESC_DECL , rdma_coro_desc desc = rdma_coro_desc()
#define DEBUG_CORO_CALL_ARG , desc
#define DEBUG_CORO_DEFINE , rdma_coro_desc desc
#else
#define DEBUG_LOCATION_DECL
#define DEBUG_LOCATION_CALL_ARG
#define DEBUG_LOCATION_DEFINE

#define DEBUG_DESC_DECL
#define DEBUG_CORO_CALL_ARG
#define DEBUG_CORO_DEFINE
#endif

  constexpr uint64_t SEGMENT_SIZE = 1024;
constexpr uint64_t SLOT_PER_SEG = ((SEGMENT_SIZE) / (sizeof(uint64_t) + sizeof(uint8_t)));

struct Slot
{
    uint8_t fp : 8;
    uint8_t len : 3;
    uint8_t sign : 1; // 用来表示split delete信息
    uint8_t dep : 4;
    uint64_t offset : 48;
    uint8_t fp_2;
    // TODO: sender's local depth
    operator uint64_t()
    {
        return *(uint64_t *)this;
    }
    Slot(uint64_t u)
    {
        *this = *(Slot *)(&u);
    }
    bool operator<(const Slot &a) const
    {
        return fp < a.fp;
    }
    void print(uint64_t slot_id = -1)
    {
        if (slot_id != -1)
            printf("slot_id:%lu\t", slot_id);
        printf("fp:%02x\t", fp);
        printf("fp_2:%02x\t", fp_2);
        printf("len:%d\t", len);
        printf("sign:%d\t", sign);
        printf("dep:%02d\t", dep);
        printf("offset:%012lx\t", offset);
        printf("size:%ld\n", sizeof(Slot));
    }
    void print(const std::string &message)
    {
        printf("%s\t fp:%02x\t fp_2:%02x\t len:%d\t sign:%d\t dep:%02d\t offset:%012lx\t size:%ld\n",
               message.c_str(), fp, fp_2, len, sign, dep, offset, sizeof(Slot));
    }
} __attribute__((aligned(1)));

#if LARGER_FP_FILTER_GRANULARITY
constexpr size_t FP_BITMAP_LENGTH = 1024;
using FpBitmapType = uint64_t;
#else
constexpr size_t FP_BITMAP_LENGTH = 16;
using FpBitmapType = uint64_t;
#endif

constexpr uint64_t LOCAL_DEPTH_BITS = 55;
constexpr uint64_t SIGN_AND_SLOT_CNT_BITS = 64 - LOCAL_DEPTH_BITS;
struct CurSegMeta
{
    uint8_t sign : 1; // 实际中的split_lock可以和sign、depth合并，这里为了不降rdma驱动版本就没有合并。
    uint64_t slot_cnt : 8;                   // 将 slot_cnt 的位数减少到 8 位。指示CurSeg中slot的数量，注意FAA可能把它加到超出SLOT_PER_SEG，因此会在合并时清零。使用FAA+(1<<1)，从而避免增加sign。
    uint64_t local_depth : LOCAL_DEPTH_BITS; // 将 local_depth 的位数减少到 55 位
    uintptr_t main_seg_ptr;
    uintptr_t main_seg_len;
    FpBitmapType fp_bitmap[FP_BITMAP_LENGTH]; // 16*64 = 1024,代表10bits fp的出现情况；整个CurSeg大约会出现（1024/8=128）个FP，因此能极大的减少search对CurSeg的访问
    struct ibv_srq *srq;

    void print(std::string desc = "")
    {
        log_err("%s slot_cnt:%lu local_depth:%lu main_seg_ptr:%lx main_seg_len:%lu srq:%p", desc.c_str(), slot_cnt, local_depth, main_seg_ptr, main_seg_len, srq);
    }
} __attribute__((aligned(1)));

struct CurSeg
{
    uint64_t split_lock;
    CurSegMeta seg_meta;
    Slot slots[SLOT_PER_SEG];
} __attribute__((aligned(1)));

constexpr uint64_t MAX_FP_INFO = 256;
constexpr uint64_t MAX_DEPTH = 16;
constexpr uint64_t DIR_SIZE = (1 << MAX_DEPTH);
struct FpInfo
{
    uint8_t num; // 数量
    operator uint64_t()
    {
        return *(uint64_t *)this;
    }
} __attribute__((aligned(1)));

struct DirEntry
{
    // TODO : 实际上只需要用5 bits，为了方便ptr统一48，所以这里仍保留16bits
    uint64_t local_depth;
    uintptr_t cur_seg_ptr;
    uintptr_t main_seg_ptr;
    uint64_t main_seg_len;
    FpInfo fp[MAX_FP_INFO];
    bool operator==(const DirEntry &other) const
    {
        return cur_seg_ptr == other.cur_seg_ptr && main_seg_ptr == other.main_seg_ptr &&
               main_seg_len == other.main_seg_len;
    }

    void print(std::string desc = "")
    {
        log_err("%s local_depth:%lu cur_seg_ptr:%lx main_seg_ptr:%lx main_seg_lne:%lx", desc.c_str(), local_depth, cur_seg_ptr, main_seg_ptr, main_seg_len);
    }
} __attribute__((aligned(1)));

struct Directory
{
    uint64_t global_depth;   // number of segment
    DirEntry segs[DIR_SIZE]; // Directory use MSB and is allocated enough space in advance.
    uint64_t start_cnt;      // 为多客户端同步保留的字段，不影响原有空间布局

    void print(std::string desc = "")
    {
        log_err("%s Global_Depth:%lu", desc.c_str(), global_depth);
        for (uint64_t i = 0; i < std::max(4, (1 << global_depth)); i++)
        {
            log_err("Entry %lx : local_depth:%lu cur_seg_ptr:%lx main_seg_ptr:%lx main_seg_lne:%lx", i, segs[i].local_depth, segs[i].cur_seg_ptr, segs[i].main_seg_ptr, segs[i].main_seg_len);
        }
    }
} __attribute__((aligned(1)));

const int wr_wo_await = UINT16_MAX - 1000;

const int mr_flag_lo = IBV_ACCESS_LOCAL_WRITE;
const int mr_flag_ro = mr_flag_lo | IBV_ACCESS_REMOTE_READ;
const int mr_flag_rw = mr_flag_ro | IBV_ACCESS_REMOTE_WRITE;
const int mr_flag_all = mr_flag_rw | IBV_ACCESS_REMOTE_ATOMIC;

extern const ibv_qp_cap sr_qp_cap;
extern const ibv_qp_cap so_qp_cap;
extern const ibv_qp_cap zero_qp_cap;

const int rdma_info_mr_size = 1 << 21;
const int rdma_info_mr_probing = 256;
const int rdma_max_rd_atomic = 8;
const int rdma_default_cq_size = 1024; // 64;
const char rdma_default_host[] = "127.0.0.1";
const int rdma_default_port = 10001;
const int rdma_default_max_coros = 256;
const int rdma_default_tempmp_size = 1 << 26; // 64M
const int rdma_sock_recv_buf_size = 128;
const int rdma_max_wc_per_poll = SLOT_PER_SEG; // 8;
const int rdma_max_pending_tasks = 16;
const int dma_default_workq_size = 64;
const int dma_default_inv_buf_size = 128;
const int dma_tempmp_mmap_name = 114514;

using rdma_dmmr = std::tuple<ibv_dm *, ibv_mr *>;

struct doca_dev;
struct doca_mmap;
struct doca_buf_inventory;
struct doca_dma;
struct doca_ctx;
struct doca_workq;
struct doca_buf;

class rdma_dev
{
    ibv_context *ib_ctx{nullptr};
    ibv_device_attr device_attr;
    ibv_port_attr port_attr;
    ibv_pd *pd{nullptr};
    int ib_port{-1};
    int gid_idx{-1};

#ifdef ENABLE_DOCA_DMA
    doca_dev *dma_dev{nullptr};
    std::unordered_map<uint32_t, std::variant<ibv_mr *, doca_mmap *>> info_idx;
#else
    std::unordered_map<uint32_t, std::variant<ibv_mr *>> info_idx;
#endif
    ibv_mr *info_mr;
    std::mutex info_lock;

    cycle_queue<uint16_t> conn_ids;
    std::mutex conn_id_lock;
    size_t alloc_conn_id() { std::lock_guard lg(conn_id_lock); return conn_ids.dequeue(); }
    void free_conn_id(size_t conn_id) { std::lock_guard lg(conn_id_lock); conn_ids.enqueue(conn_id); }

    ibv_cq *create_cq(int cq_size) { return ibv_create_cq(ib_ctx, cq_size, nullptr, nullptr, 0); }
    ibv_srq *create_srq(int cq_size)
    {
        struct ibv_srq_init_attr attr = {
            .attr = {
                .max_wr = (uint32_t)cq_size,
                .max_sge = 1}};
        return ibv_create_srq(pd, &attr);
    }

public:
    rdma_dev(const char *dev_name = nullptr, int _ib_port = 1, int _gid_idx = 1);
    ~rdma_dev();
    ibv_mr *create_mr(size_t size, void *buf = nullptr, int mr_flags = mr_flag_all);
    ibv_mr *reg_mr(uint32_t mr_id, ibv_mr *mr);
    /**
     * @brief 创建并注册MR
     *
     * 注册后的MR可在远程通过rdma_conn::query_remote_mr查询
     *
     * @param mr_id MR标识，全局唯一
     * @param size MR大小
     * @param buf 绑定到已有空间或创建
     * @param mr_flags MR权限
     * @return ibv_mr* 创建的MR
     */
    ibv_mr *reg_mr(uint32_t mr_id, size_t size, void *buf = nullptr, int mr_flags = mr_flag_all);
    ibv_dm *create_dm(size_t size, uint32_t log_align = 3);
    rdma_dmmr create_dmmr(size_t size, uint32_t log_align = 3, int mr_flags = mr_flag_all);
    rdma_dmmr reg_dmmr(uint32_t mr_id, size_t size, uint32_t log_align = 3, int mr_flags = mr_flag_all);

#ifdef ENABLE_DOCA_DMA
    void enable_dma(const char *dev_name = nullptr);
    static void free_mmap_mem(void *addr, size_t len, void *opaque) { free_hugepage(addr, upper_align(len, 1 << 21)); };
    std::tuple<doca_mmap *, void *> create_mmap(uint32_t mmap_id, size_t len, void *addr = nullptr);
    std::tuple<doca_mmap *, void *> reg_mmap(uint32_t mmap_id, std::tuple<doca_mmap *, void *> &mmpaddr);
    std::tuple<doca_mmap *, void *> reg_mmap(uint32_t mmap_id, size_t len, void *addr = nullptr);
#endif

    friend class rdma_conn;
    friend class rdma_worker;
};

enum
{
    rdma_info_type_invaild,
    rdma_info_type_mr,
    rdma_info_type_mmap,
};

struct rdma_rmr
{
    uint32_t type : 2;
    uint32_t mr_id : 30;
    uint32_t rkey;
    uint64_t raddr;
    uint64_t rlen;
};

struct rdma_rmmap
{
    uint32_t type : 2;
    uint32_t mmap_id : 30;
    uint32_t len;
    uint8_t data[0];
};

struct rdma_infomr_hdr
{
    uint64_t tail;
    uint64_t cksum;
    uint8_t data[0];
};

class rdma_conn;

enum
{
    coro_state_invaild,
    coro_state_inited = 1 << 0,
    coro_state_ready = 1 << 1,
    coro_state_error = 1 << 2,
};

const uint16_t rdma_coro_none = ~0;
struct rdma_coro_desc
{
    uint64_t cli_id{0}, coro_id{0}, segloc{0};
    int send_cnt{0};
    rdma_conn *conn{nullptr};
    uint32_t rkey{0}, lkey{0};

    rdma_coro_desc(uint64_t cli_id = 0, uint64_t coro_id = 0, uint64_t segloc = 0,
                   int send_cnt = 0, rdma_conn *conn = nullptr, uint32_t rkey = 0, uint32_t lkey = 0)
        : cli_id(cli_id), coro_id(coro_id), segloc(segloc), send_cnt(send_cnt),
          conn(conn), rkey(rkey), lkey(lkey)
    {
    }
};

struct rdma_coro
{
    uint16_t id;
    uint16_t next;
    uint16_t coro_state{coro_state_invaild};
    uint16_t ctx{0}; // conn id: free coro after disconnect
    std::coroutine_handle<> resume_handler{nullptr};
#if CORO_DEBUG // 会影响SIZE
    std::source_location location;
    rdma_coro_desc desc;
    std::chrono::steady_clock::time_point start_time;
#endif
    void print(std::string desc = "")
    {
#if CORO_DEBUG
        log_err("%s: Coro ID: %u, Context ID: %u, File: %s:%d, Desc: %s", desc.c_str(), this->id, this->ctx, this->location.file_name(), this->location.line(), std::format("[{}:{}]segloc:{}第{}次SEND slot", this->desc.cli_id, this->desc.coro_id, this->desc.segloc, this->desc.send_cnt).c_str());
#else
        log_err("%s: Coro ID: %u, Context ID: %u", desc.c_str(), this->id, this->ctx);
#endif
    }
};

class rdma_worker : noncopyable
{
    using task_ring = scsp_task_ring<rdma_max_pending_tasks>;
    using handle_ring = cycle_queue<std::coroutine_handle<>>;

public:
    rdma_dev &dev;
    ibv_cq *cq{nullptr};
    std::shared_ptr<std::vector<rdma_coro>> coros; // shared rdma_coro *coros{nullptr};
    std::shared_ptr<uint32_t> free_head;
    // std::mutex free_head_mutex; // 用于保护 free_head 的互斥锁
    uint64_t _max_segloc{0};

protected:
    const ibv_qp_cap &qp_cap;
    std::vector<struct ibv_srq *> srqs{nullptr}; // record SRQ, used to destroy
#if RDMA_SIGNAL
    struct ibv_srq *signal_srq{nullptr}; // signal conn's SRQ
#endif
    int max_coros{0};
    void *worker_ctx{nullptr};

    tempmp *mp{nullptr};
    ibv_mr *mpmr{nullptr};

    Directory *dir{nullptr};

#ifdef ENABLE_DOCA_DMA
    doca_mmap *mpmmp{nullptr};
    doca_buf_inventory *buf_inv{nullptr};
    doca_dma *dma{nullptr};
    doca_ctx *dma_ctx{nullptr};
    doca_workq *dma_workq{nullptr};
    handle_ring *pending_dma_task{nullptr};
#endif

    bool loop_flag{false};
    void worker_loop();

    task_ring *pending_tasks{nullptr}; // WARNING: may overflow if too many conn arrive at same time
    handle_ring *yield_handler{nullptr};

    inline rdma_coro *alloc_coro(uint16_t conn_id DEBUG_LOCATION_DECL DEBUG_DESC_DECL);
#if CORO_DEBUG
    std::thread periodic_thread;
    std::atomic<bool> stop_flag;
    std::condition_variable cv;
    std::mutex cv_m;
    inline void print_running_coros();
    void start_periodic_task();
    void stop_periodic_task();
#endif
    inline void free_coro(rdma_coro *cor);
    task<> cancel_coros(uint16_t conn_id, std::vector<int> &&cancel_list, volatile int &finish_flag);

public:
    /**
     * @brief Construct a new rdma Worker object
     *
     * _cap.max_recv_wr为0则Worker仅能ibv_post_send
     *
     * @param _dev 绑定rdma设备
     * @param _cap 建立QP连接的参数
     * @param tempmp_size 临时内存池大小，一个Worker中所有连接共享
     * @param max_coros 一个Worker中最大并发RDMA请求数
     * @param cq_size Worker关联的CQ大小，Worker中所有连接共享CQ
     */
    rdma_worker(rdma_dev &_dev, const ibv_qp_cap &_cap = so_qp_cap, int tempmp_size = rdma_default_tempmp_size,
                int max_coros = rdma_default_max_coros, int cq_size = rdma_default_cq_size,
                Directory *_dir = nullptr, ibv_cq *existing_cq = nullptr,
                std::shared_ptr<std::vector<rdma_coro>> existing_coros = nullptr,
                std::shared_ptr<uint32_t> existing_free_head = nullptr);
    ~rdma_worker();
    inline void *alloc_buf(size_t size) { return mp->alloc(size); }
    template <is_integral... Ts>
    inline auto alloc_many(Ts... size) { return mp->alloc_many(size...); }
    inline void free_buf(void *buf) { mp->free(buf); }
    constexpr uint32_t lkey() { return mpmr->lkey; }
    template <typename T = void>
    constexpr T *get_ctx() { return (T *)worker_ctx; }
    constexpr void set_ctx(void *ctx) { worker_ctx = ctx; }

    /**
     * @brief 在Worker上启动协程
     *
     * @param input_task 要启动的协程
     * @return auto 协程结果
     */
    template <typename TaskType>
    auto run(TaskType &&input_task)
    {
        assert_require(!loop_flag);
        using value_type = typename std::decay_t<TaskType>::value_type;
        task_result<value_type> value;
        std::move(std::forward<TaskType>(input_task))
            .start([&value, this](auto &&result)
                   { value = std::move(result); loop_flag = false; });
        if (!value.has_value())
            worker_loop(); // IMPORTANT: client poll cq
        return std::move(value).result();
    }
    void push_task(task<> &&t)
    {
        assert_check(pending_tasks);
        pending_tasks->enqueue(std::forward<task<>>(t));
    }
    auto yield()
    {
        assert_check(yield_handler);
        struct yield_awaiter
        {
            rdma_worker *worker{nullptr};
            constexpr bool await_ready() { return false; }
            void await_suspend(std::coroutine_handle<> h) noexcept { worker->yield_handler->enqueue(std::move(h)); }
            constexpr void await_resume() {}
        };
        return yield_awaiter{this};
    }
    /**
     * @brief 连接到RDMA Server
     *
     * @param host Server地址
     * @param port Server端口
     * @return rdma_conn* 建立的RDMA连接
     */
    rdma_conn *connect(const char *host = rdma_default_host, int port = rdma_default_port, uint8_t is_signal_conn = 0, uint64_t segloc = 0);

#ifdef ENABLE_DOCA_DMA
    void enable_dma(uint32_t workq_size = dma_default_workq_size, size_t buf_inv_size = dma_default_inv_buf_size);
#endif

    friend class rdma_conn;
    friend class rdma_server;
    friend class rdma_future;
    friend class dma_future;
};

class rdma_server : public noncopyable
{
    rdma_dev &dev;
    int listenfd{-1};
    std::unordered_map<int, rdma_conn *> sk2conn;

    std::jthread sock_thread;
    std::vector<rdma_worker *> workers;
    std::vector<std::thread> worker_threads;

public:
    rdma_server(rdma_dev &_dev) : dev(_dev) {}
    ~rdma_server();

    /**
     * @brief 启动RDMA Server
     *
     * handler为空则Server仅处理单边请求，不建立内存池，协程池。
     * handler非空则需要设定qp_cap
     *
     * @param handler 处理新连接的协程，接受新连接后调用
     * @param worker_num Server工作线程数，Server接受新连接后分配连接到Worker
     * @param qp_cap 建立QP连接的参数
     * @param tempmp_size 临时内存池大小，一个Worker中所有连接共享
     * @param max_coros 一个Worker中最大并发RDMA请求数
     * @param cq_size Worker关联的CQ大小，Worker中所有连接共享CQ
     * @param port Server监听端口
     */
    void start_serve(std::function<task<>(rdma_conn*)> handler = nullptr, int worker_num = 1, const ibv_qp_cap &qp_cap = zero_qp_cap,
                     int tempmp_size = rdma_default_tempmp_size, int max_coros = rdma_default_max_coros, int cq_size = rdma_default_cq_size, int port = rdma_default_port, Directory *dir = nullptr);

    void stop_serve();
    rdma_worker *operator[](size_t i) { return workers.at(i); }

    friend class rdma_conn;
};

using rdma_client = rdma_worker;

class [[nodiscard]] rdma_future
{
public:
// protected:
    rdma_coro *cor{nullptr};
    rdma_conn *conn{nullptr};

    rdma_future(const rdma_future &) = delete;
    rdma_future &operator=(const rdma_future &) = delete;

public:
    rdma_future() = default;
    rdma_future(rdma_coro *cor, rdma_conn *conn) : cor(cor), conn(conn) {}
    rdma_future(rdma_future &&other) : cor(std::move(other.cor)), conn(std::move(other.conn))
    {
        other.cor = nullptr;
        other.conn = nullptr;
    }

    constexpr bool await_ready() noexcept { return !cor || (cor->coro_state & coro_state_ready); }
    void await_suspend(std::coroutine_handle<> h)
    {
        cor->resume_handler = h;
        cor->coro_state |= coro_state_inited;
    }
    int await_resume();

    friend class rdma_conn;
};

class [[nodiscard]] rdma_buffer_future : public rdma_future
{
protected:
    void *_res_buf{nullptr};

public:
    rdma_buffer_future(rdma_coro *cor, rdma_conn *conn, void *res_buf)
        : rdma_future(cor, conn), _res_buf(res_buf) {}
    rdma_buffer_future(rdma_buffer_future &&other)
        : rdma_future(std::move(other)), _res_buf(other._res_buf) { other._res_buf = nullptr; }
    void *await_resume();
};

class [[nodiscard]] rdma_cas_future : public rdma_future
{
    void *_res_buf{nullptr};
    uint64_t &_cmpval;

public:
    rdma_cas_future(rdma_coro *cor, rdma_conn *conn, void *res_buf, uint64_t &cmpval)
        : rdma_future(cor, conn), _res_buf(res_buf), _cmpval(cmpval) {}
    rdma_cas_future(rdma_cas_future &&other)
        : rdma_future(std::move(other)), _res_buf(other._res_buf), _cmpval(other._cmpval) { other._res_buf = nullptr; }
    int await_resume();
};

class [[nodiscard]] rdma_cas_n_future : public rdma_future
{
    void *_res_buf{nullptr};
    uint64_t _cmpval;

public:
    rdma_cas_n_future(rdma_coro *cor, rdma_conn *conn, void *res_buf, uint64_t &cmpval)
        : rdma_future(cor, conn), _res_buf(res_buf), _cmpval(cmpval) {}
    rdma_cas_n_future(rdma_cas_n_future &&other)
        : rdma_future(std::move(other)), _res_buf(other._res_buf), _cmpval(other._cmpval) { other._res_buf = nullptr; }
    int await_resume();
};

class [[nodiscard]] rdma_faa_future : public rdma_buffer_future
{

public:
    rdma_faa_future(rdma_coro *cor, rdma_conn *conn, void *res_buf)
        : rdma_buffer_future(cor, conn, res_buf) {}
    rdma_faa_future(rdma_faa_future &&other)
        : rdma_buffer_future(std::move(other)) { }
    uint64_t await_resume();
};

#ifdef ENABLE_DOCA_DMA
class [[nodiscard]] dma_future
{
    rdma_worker *worker{nullptr};
    doca_buf *src_buf{nullptr};
    doca_buf *dst_buf{nullptr};

public:
    dma_future(rdma_worker *w, doca_buf *src, doca_buf *dst) : worker(w), src_buf(src), dst_buf(dst){};
    constexpr bool await_ready() noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) { worker->pending_dma_task->enqueue(std::move(h)); };
    void await_resume();
};

class [[nodiscard]] dma_buf_future : public dma_future
{
    void *res_buf{nullptr};

public:
    dma_buf_future(rdma_worker *w, doca_buf *src, doca_buf *dst, void *buf) : dma_future(w, src, dst), res_buf(buf){};
    void *await_resume();
};
#endif

class rdma_conn
{
public:
    rdma_dev &dev;
    rdma_worker *worker;
    int sock;
    ibv_qp *qp{nullptr};
    ibv_mr *exchange_mr{nullptr};
    ibv_sge __exchange_sge{};
    ibv_send_wr __exchange_wr;
    ibv_send_wr *exchange_wr{&__exchange_wr};
    std::unordered_map<uint32_t, std::variant<rdma_rmr, std::string>> exchange_idx;

    void send_exchange(uint16_t proto, uint8_t is_signal_conn, uint64_t segloc);
    void handle_recv_setup(const void *buf, size_t len, uint8_t is_signal_conn, uint64_t segloc);
    int modify_qp_to_init();
    int modify_qp_to_rtr(uint32_t rqp_num, uint16_t rlid, const ibv_gid *rgid);
    int modify_qp_to_rts();
    task<> update_cache();
    void release_working_coros();

    uint16_t conn_id;

public:
    rdma_conn(rdma_worker *w, int _sock, uint8_t is_signal_conn, uint64_t segloc);
    ~rdma_conn();

    auto yield() { return worker->yield(); }

    task<rdma_rmr> query_remote_mr(uint32_t mr_id);
#ifdef ENABLE_DOCA_DMA
    task<std::tuple<doca_mmap *, uint64_t>> query_remote_mmap(uint32_t mmap_id);
    dma_buf_future dma_read(doca_mmap *rmmp, uint64_t raddr, size_t len);
    dma_future dma_read(doca_mmap *rmmp, uint64_t raddr, doca_mmap *lmmp, void *laddr, size_t len);
    dma_future dma_write(doca_mmap *rmmp, uint64_t raddr, doca_mmap *lmmp, void *laddr, size_t len);
    constexpr doca_mmap *lmmp() { return worker->mpmmp; }
#endif

    void pure_write(uint64_t raddr, uint32_t rkey, void *laddr, uint32_t len, uint32_t lkey = 0);
    void pure_write_with_imm(uint64_t raddr, uint32_t rkey, void *laddr, uint32_t len, uint32_t lkey, uint32_t imm_data);
    void pure_send(void *laddr, uint32_t len, uint32_t lkey = 0);
    void pure_read(uint64_t raddr, uint32_t rkey, void *laddr, uint32_t len, uint32_t lkey = 0);
    void pure_recv(void *laddr, uint32_t len, uint32_t lkey = 0);

    // co_return int: 1: success, 0: failure
    rdma_future do_send(ibv_send_wr *wr_begin, ibv_send_wr *wr_end DEBUG_LOCATION_DECL DEBUG_DESC_DECL);
    rdma_future do_recv(ibv_recv_wr *wr DEBUG_LOCATION_DECL);

    rdma_buffer_future read(uint64_t raddr, uint32_t rkey, uint32_t len DEBUG_LOCATION_DECL);
    rdma_buffer_future read(const rdma_rmr &remote_mr, uint32_t offset, uint32_t len DEBUG_LOCATION_DECL)
    { return read(remote_mr.raddr + offset, remote_mr.rkey, len DEBUG_LOCATION_CALL_ARG); }
    rdma_future read(uint64_t raddr, uint32_t rkey, void *laddr, uint32_t len, uint32_t lkey DEBUG_LOCATION_DECL);
    rdma_future read(const rdma_rmr &remote_mr, uint32_t offset, void *laddr, uint32_t len, uint32_t lkey DEBUG_LOCATION_DECL)
    { return read(remote_mr.raddr + offset, remote_mr.rkey, laddr, len, lkey DEBUG_LOCATION_CALL_ARG); }
    rdma_future write(uint64_t raddr, uint32_t rkey, void *laddr, uint32_t len, uint32_t lkey = 0 DEBUG_LOCATION_DECL);
    rdma_future write(const rdma_rmr &remote_mr, uint32_t offset, void *laddr, uint32_t len, uint32_t lkey = 0 DEBUG_LOCATION_DECL)
    { return write(remote_mr.raddr + offset, remote_mr.rkey, laddr, len, lkey DEBUG_LOCATION_CALL_ARG); }
    rdma_future write_with_imm(uint64_t raddr, uint32_t rkey, void *laddr, uint32_t len, uint32_t lkey = 0, uint32_t imm_data = 0 DEBUG_LOCATION_DECL);
    rdma_cas_future fetch_add(uint64_t raddr, uint32_t rkey, uint64_t &fetch, uint64_t addval DEBUG_LOCATION_DECL);
    rdma_cas_future cas(uint64_t raddr, uint32_t rkey, uint64_t &cmpval, uint64_t swapval DEBUG_LOCATION_DECL);
    rdma_cas_future cas(const rdma_rmr &remote_mr, uint32_t offset, uint64_t &cmpval, uint64_t swapval DEBUG_LOCATION_DECL)
    { return cas(remote_mr.raddr + offset, remote_mr.rkey, cmpval, swapval DEBUG_LOCATION_CALL_ARG); }
    rdma_cas_n_future cas_n(uint64_t raddr, uint32_t rkey, uint64_t cmpval, uint64_t swapval DEBUG_LOCATION_DECL);
    rdma_cas_n_future cas_n(const rdma_rmr &remote_mr, uint32_t offset, uint64_t cmpval, uint64_t swapval DEBUG_LOCATION_DECL)
    { return cas_n(remote_mr.raddr + offset, remote_mr.rkey, cmpval, swapval DEBUG_LOCATION_CALL_ARG); }
    rdma_faa_future faa(uint64_t raddr, uint32_t rkey, uint64_t addval DEBUG_LOCATION_DECL);
    rdma_faa_future faa(const rdma_rmr &remote_mr, uint32_t offset, uint64_t addval DEBUG_LOCATION_DECL)
    { return faa(remote_mr.raddr + offset, remote_mr.rkey, addval DEBUG_LOCATION_CALL_ARG); }
    rdma_future send(void *laddr, uint32_t len, uint32_t lkey = 0 DEBUG_LOCATION_DECL DEBUG_DESC_DECL);
    rdma_future send_with_imm(void *laddr, uint32_t len, uint32_t lkey, uint32_t imm_data DEBUG_LOCATION_DECL DEBUG_DESC_DECL);
    rdma_future send_then_read(void *send_laddr, uint32_t send_len, uint32_t send_lkey, uint64_t read_raddr, uint32_t read_rkey, void *read_laddr, uint32_t read_len, uint32_t read_lkey DEBUG_LOCATION_DECL);
    rdma_future send_then_fetch_add(void *send_laddr, uint32_t send_len, uint32_t send_lkey, uint64_t faa_raddr, uint32_t faa_rkey, uint64_t &faa_fetch, uint64_t faa_addval DEBUG_LOCATION_DECL);
    rdma_buffer_future recv(uint32_t len DEBUG_LOCATION_DECL);
    rdma_future recv(void *laddr, uint32_t len, uint32_t lkey DEBUG_LOCATION_DECL);
    task<uint32_t> fill(uint64_t raddr, uint32_t rkey, uint32_t rlen, void *fill_val, uint32_t fill_val_len, uint32_t work_buf_size = 1024 DEBUG_LOCATION_DECL);
    task<uint32_t> fill(const rdma_rmr &remote_mr, uint32_t offset, uint32_t rlen, void *fill_val, uint32_t fill_val_len, uint32_t work_buf_size = 1024 DEBUG_LOCATION_DECL)
    { return fill(remote_mr.raddr + offset, remote_mr.rkey, rlen, fill_val, fill_val_len, work_buf_size DEBUG_LOCATION_CALL_ARG); }

    inline void *alloc_buf(size_t size) { return worker->alloc_buf(size); }
    template <is_integral... Ts>
    inline auto alloc_many(Ts... size) { assert_require(this && worker); return worker->alloc_many(size...); }
    inline void free_buf(void *buf) { worker->free_buf(buf); }
    constexpr uint32_t lkey() { return worker->lkey(); }
    // get worker context
    template <typename T = void>
    constexpr T *get_ctx() { return worker->get_ctx<T>(); }

    friend class rdma_worker;
    friend class rdma_server;
    friend class rdma_future;
};

// =========================== utils =============================

inline void cleanup_conn(rdma_conn **c) { delete (rdma_conn *)(*c); }
#define rdma_auto_conn __attribute__((cleanup(cleanup_conn))) rdma_conn *
inline void cleanup_mr(ibv_mr** mr) { free((*mr)->addr), ibv_dereg_mr(*mr); }
#define rdma_auto_mr __attribute__((cleanup(cleanup_mr))) ibv_mr *

inline void rdma_free_mr(ibv_mr *mr, const bool free_mem = true)
{
    if (free_mem)
        free_hugepage(mr->addr, upper_align(mr->length, 1 << 21));
    if (ibv_dereg_mr(mr))
        log_warn("failed dereg mr");
}

template <typename T>
concept rdma_wr = std::same_as<T, ibv_send_wr> || std::same_as<T, ibv_recv_wr>;

inline void rdma_free_wr(rdma_wr auto *wr, bool free_mem = true)
{
    if (free_mem)
        free(wr->sg_list);
    free(wr);
}

inline void rdma_free_dmmr(rdma_dmmr &&dmmr)
{
    auto [dm, mr] = dmmr;
    if (mr && ibv_dereg_mr(mr))
        log_warn("failed dereg mr");
    if (dm && ibv_free_dm(dm))
        log_warn("failed free dm");
}

#ifdef ENABLE_DOCA_DMA
void free_mmap(doca_mmap *mmp);
std::tuple<uint64_t, uint64_t> get_addrlen_from_export(std::string &export_str);
#endif

template <ibv_wr_opcode opcode>
inline void fill_rw_wr(ibv_send_wr *wr, ibv_sge *sge, uint64_t raddr, uint32_t rkey, void *laddr, uint32_t len, uint32_t lkey)
{
    memset(wr, 0, sizeof(ibv_send_wr));
    sge->addr = (uint64_t)laddr;
    sge->length = len;
    sge->lkey = lkey;
    wr->num_sge = 1;
    wr->sg_list = sge;
    wr->opcode = opcode;
    wr->wr.rdma.remote_addr = raddr;
    wr->wr.rdma.rkey = rkey;
    // log_err("opcode:%d, raddr:%lx, rkey:%u, laddr:%p, len:%u, lkey:%u", opcode, raddr, rkey, laddr, len, lkey);
}

template <ibv_wr_opcode opcode>
inline void fill_atomic_wr(ibv_send_wr *wr, ibv_sge *sge, uint64_t raddr, uint32_t rkey, void *laddr, uint32_t lkey, uint64_t cmpadd, uint64_t swapval)
{
    memset(wr, 0, sizeof(ibv_send_wr));
    sge->addr = (uint64_t)laddr;
    sge->length = sizeof(uint64_t);
    sge->lkey = lkey;
    wr->sg_list = sge;
    wr->num_sge = 1;
    wr->opcode = opcode;
    wr->wr.atomic.remote_addr = raddr;
    wr->wr.atomic.compare_add = cmpadd;
    wr->wr.atomic.swap = swapval;
    wr->wr.atomic.rkey = rkey;
}

inline void fill_recv_wr(ibv_recv_wr *wr, ibv_sge *sge, void *laddr, uint32_t len, uint32_t lkey)
{
    memset(wr, 0, sizeof(ibv_recv_wr));
    sge->addr = (uint64_t)laddr;
    sge->length = len;
    sge->lkey = lkey;
    wr->num_sge = 1; // num_sge 通常表示单个工作请求（work request）中包含的 SGE（Scatter-Gather Element）的数量。
    wr->sg_list = sge;
}

uint64_t crc64(const void *data, size_t l);
