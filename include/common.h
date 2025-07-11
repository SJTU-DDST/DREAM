#pragma once

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <tuple>
#include <variant>
#include <coroutine>
#include <optional>
#include <mutex>
#include <vector>

#include <bitset>
#include <format>
#include <memory>
#include <print>
#include <source_location>
#include <sys/mman.h>
#include <utility>
#include <condition_variable>
#include <mutex>
#include <iomanip>

#define HASH_TYPE MYHASH
#define MODIFIED 1
#if MODIFIED
// DREAM
#define SPLIT_LOCAL_LOCK 1 // 合并/分裂时在本地上锁，参考Sherman
#define DISABLE_OPTIMISTIC_SPLIT 0 // 禁用客户端的乐观分裂检测，用于性能分解实验
#define EMBED_FULL_KEY 1 // 在CurSeg中嵌入完整key，避免合并时需要读取完整key。即使Slot大于8B，也可以通过单次SEND发送，且因为FAA slot_cnt不会读取/合并不完整的条目。
// #define CACHE_FILTER 0 // 使用缓存过滤器，减少对CurSeg的读取。但修复后CurSeg读取率5%，不太需要。目前仅实现插入还未实现读取。插入性能启用3284，禁用3854，禁用但u64 filter 3659
// TicketHash
#define USE_TICKET_HASH 1 // 使用TicketHash
// 目前吞吐量2274.55Kops比SEND的3326.04Kops低，可能受限于两次FAA、等待合并完成的READ对带宽的消耗
// Ticket双网卡：insert 3773Kops，update 2212.11Kops，update_zipf99 1447Kops，read Run IOPS: 4824.40Kops
// RACE双网卡：update Run IOPS: 1917.18Kops，read Run IOPS: 5591.35Kops
// DREAM双网卡：insert Run IOPS: 4911.60Kops，update Run IOPS: 1610.08Kops，read Run IOPS: 5006.26Kops
#if USE_TICKET_HASH
#define RDMA_SIGNAL 0
#define USE_XRC 0
#define DOUBLE_BUFFER_MERGE 0 // 启用双缓冲区合并机制，降低写操作尾部延迟.目前打开后update IOPS 4300->2800? SEGMENT_SIZE调大也不行。但merge换成原来的（merge整个WB）又变成3200
// 如果不开启，即使把SIZZE=512，也是4300Kops
// 还没实现完，现在只是轮流使用不能合并时写入
#define NEW_MERGE 0 // 另一种双缓冲区，没实现好。
// TODO: 后台线程合并
// TODO: 测试不同CurSeg/MainSeg大小对读写性能影响
#else
#define RDMA_SIGNAL 1 // 创建专用于SEND合并完成信号的QP。
#define USE_XRC 1     // 使用XRC
#endif
// #define SIMULATE_FAA_FILTER 1 // 模拟FAA增加过滤器，而非使用原来的WRITE
// TODO: 先看下多少%读取命中CurSeg/MainSeg【50%?但之前有没完全清空filter的问题，已经修复】，评估减少CurSeg读放大的必要性
// 然后在读取CurSeg时只读取8 slots，模拟减少读放大的影响【7500Kops->9000Kops!】
// 修复后的[6%命中 10985Kops -> 不读取CurSeg 10279Kops] 不需要缓存？
// ycsbb?[30% 7334Kops]
// 模拟WRITE额外写一次Slot到读取缓存的影响

// SEPHASH使用REUSE_MAIN_SEG
// 一致性可能还得修复一下
#endif

constexpr int MAX_SEND_CONCURRENCY = 8; // 每个RNIC设备的outstanding request数量有限，超出会导致IBV_WC_RETRY_EXC_ERR
// FIXME: 目前只限制了SEND的，还有小概率出错，后续可以在do_send处获取信号量并在poll_cq处释放，保证不出错
constexpr int DEDUPLICATE_INTERVAL = 4; // 对于zipf99，线程数少实际上热键更集中

// Config
#define REUSE_MAIN_SEG 1               // 允许reuse main_seg，分裂时不创建新的main_seg
#define LARGER_FP_FILTER_GRANULARITY 1 // 使用更大的FP过滤粒度，避免写入FP过滤器前需要先读取。现在每个FP占用8bit粒度。
#define READ_FULL_KEY_ON_FP_COLLISION 1 // fp相同时读取完整key比较
#define USE_DM_MR 1         // CLEVEL使用DM MR，目前设备不支持
#define LOW_MIN_RTR_TIMER 1 // 低延迟模式
#define CLOSE_SOCKET 0 // 使用后关闭socket，目前只在server关闭，使用XRC后已经不需要
#define CORO_DEBUG 0 // 协程调试
#define TEST_SEG_SIZE 1 // 测试不同的CurSeg/MainSeg大小
#define ALLOC_CORO_THREAD_SAFE 0
// #define INTEGRATED_SLOT_CNT 1 // 将 local_depth 和 slot_cnt 合并到一个 uint64_t 类型的位域中。
// #define ALLOW_KEY_OVERLAP // 允许不同thread/coroutine的key范围重叠。即使key范围不重叠，也可能映射到同一个CurSeg，导致CAS失败。
#define RETRY_CAS 1 // 更新中CAS失败后重试。
#define USE_END_CNT 0

#define LOG_LEVEL_NONE 0
#define LOG_LEVEL_ERROR 1
#define LOG_LEVEL_WARNING 2
#define LOG_LEVEL_INFO 3
#define LOG_LEVEL_DEBUG 4

#ifndef LOG_LEVEL
#define LOG_LEVEL LOG_LEVEL_ERROR
#endif

#define do_log(log_type, fmt, arg...) fprintf(stderr, #log_type " [" __FILE__ ":%d] %s: " fmt "\n", __LINE__, __FUNCTION__, ##arg)
#if LOG_LEVEL >= LOG_LEVEL_ERROR
#ifdef EXIT_AT_LOG_ERROR
#define log_err(fmt, arg...) (do_log(ERROR, fmt, ##arg), exit(0))
#else
#define log_err(fmt, arg...) do_log(ERROR, fmt, ##arg)
#define log_test(fmt, arg...) //do_log(TEST, fmt, ##arg)
#define log_merge(fmt, arg...) //do_log(MERGE, fmt, ##arg)
#endif
#else
#define log_err(fmt, arg...)
#endif
#if LOG_LEVEL >= LOG_LEVEL_WARNING
#define log_warn(fmt, arg...) do_log(WARNING, fmt, ##arg)
#else
#define log_warn(fmt, arg...)
#endif
#if LOG_LEVEL >= LOG_LEVEL_INFO
#define log_info(fmt, arg...) do_log(INFO, fmt, ##arg)
#else
#define log_info(fmt, arg...)
#endif
#if LOG_LEVEL >= LOG_LEVEL_DEBUG
#define log_debug(fmt, arg...) do_log(DEBUG, fmt, ##arg)
#else
#define log_debug(fmt, arg...)
#endif
#define ASSERT_LEVEL_NONE 0
#define ASSERT_LEVEL_REQUIRE 1
#define ASSERT_LEVEL_CHECK 2
#define ASSERT_LEVEL_WARN 3

#ifndef ASSERT_LEVEL
#define ASSERT_LEVEL ASSERT_LEVEL_CHECK
#endif

#define do_assert(expr)           \
    do                            \
    {                             \
        if (!(expr)) [[unlikely]] \
        {                         \
            log_err(#expr);       \
            std::abort();         \
        }                         \
    } while (0);

#if ASSERT_LEVEL >= ASSERT_LEVEL_REQUIRE
#define assert_require(expr) do_assert(expr)
#else
#define assert_require(expr) (void)(expr)
#endif
#if ASSERT_LEVEL >= ASSERT_LEVEL_CHECK
#define assert_check(expr) do_assert(expr)
#else
#define assert_check(expr) (void)(expr)
#endif
#if ASSERT_LEVEL >= ASSERT_LEVEL_WARN
#define assert_warn(expr) do_assert(expr)
#else
#define assert_warn(expr) (void)(expr)
#endif

const uint32_t cache_line_size = 64;

inline bool is_valid_ptr(uintptr_t ptr){
    return ptr > 0 && ptr < (1ULL << 48);
}

template <typename T>
requires std::is_integral_v<T>
constexpr bool is_power_of2(T val) noexcept { return val >= 0 && (val & (val - 1)) == 0; }

template <typename T>
requires std::is_integral_v<T>
constexpr bool is_times_ofN(T val, std::type_identity_t<T> N = cache_line_size) noexcept
{ return val >= 0 && N > 0 && (val & (N - 1)) == 0; }

template <typename T>
requires std::is_integral_v<T>
constexpr T upper_align(T val, std::type_identity_t<T> align = cache_line_size) {
    assert_check(val >= 0);
    return (val + align - 1) & (~(align - 1));
}

constexpr uint32_t align32pow2(uint32_t x)
{
    x--;
    x |= x >> 1; x |= x >> 2;
    x |= x >> 4; x |= x >> 8;
    x |= x >> 16;
    return x + 1;
}

#define tmap(expr, src_tuple) std::apply([&](auto... x) { return std::make_tuple((expr)...); }, src_tuple)

struct noncopyable
{
protected:
    noncopyable() = default;
    ~noncopyable() = default;
    noncopyable(noncopyable &&) = default;
    noncopyable &operator=(noncopyable &&) = default;
    noncopyable(const noncopyable &) = delete;
    noncopyable &operator=(const noncopyable &) = delete;
};

const int hugepage_flags = MAP_ANONYMOUS | MAP_PRIVATE | MAP_HUGETLB;
const int hugepage_2m_flags = hugepage_flags | (21 << MAP_HUGE_SHIFT);
const int hugepage_1g_flags = hugepage_flags | (30 << MAP_HUGE_SHIFT);
inline void *alloc_hugepage(size_t size, int page_flags = hugepage_2m_flags) {
    assert_require(size != 0);
    void *res;
#ifdef ENABLE_HUGEPAGE
    res = mmap(nullptr, size, PROT_READ | PROT_WRITE, page_flags, 0, 0);
    if (res == MAP_FAILED) [[unlikely]]
    {
        log_warn("failed to map hugepages: %s", strerror(errno));
        return nullptr;
    }
#else
    if (posix_memalign(&res, 1 << (page_flags >> MAP_HUGE_SHIFT), size)) [[unlikely]]
    {
        log_warn("failed to malloc: %s", strerror(errno));
        return nullptr;
    }
#endif
    return res;
}
inline void free_hugepage(void *ptr, size_t size) {
    assert_require(ptr);
#ifdef ENABLE_HUGEPAGE
    if (munmap(ptr, size)) [[unlikely]]
    { log_warn("failed to unmap hugepages: %s", strerror(errno)); }
#else
    free(ptr);
#endif
}

template <typename T>
concept is_integral = std::is_integral_v<T>;

template<uint32_t align = cache_line_size>
class tempmp_impl : noncopyable
{
    uint32_t head;
    uint32_t tail;
    uint32_t blk_num;
    uint32_t blk_cap;
    uint8_t *data{nullptr};

    enum
    { flag_freed, flag_inuse, flag_gc, flag_invaild };
    struct ctrl_block {
        uint32_t flag : 2;
        uint32_t len : 30;
    };
    ctrl_block *ctl_blks{nullptr};

    std::tuple<> alloc_many_impl(uint64_t base) { return {}; }

    template <typename T, typename... Ts>
    auto alloc_many_impl(uint64_t base, T val, Ts... size)
    { return std::tuple_cat(std::make_tuple((void *)base), alloc_many_impl(base + upper_align(val, align), size...)); }

public:
    tempmp_impl(size_t _size) : head(0), tail(0), blk_num(_size / align), blk_cap(_size / align - 1) {
        assert_require(is_power_of2(_size) && _size >= (1 << 21) && (_size / align) <= (size_t)(1u << 31));
        assert_require(data = (uint8_t *)alloc_hugepage(_size));
        assert_require(ctl_blks = (ctrl_block *)calloc(blk_num, sizeof(ctrl_block)));
    }
    ~tempmp_impl() {
        if (data)
            free_hugepage(data, get_data_len());
        if (ctl_blks)
            ::free(ctl_blks);
    }
    constexpr void *get_data_addr() noexcept { return data; }
    constexpr size_t get_data_len() noexcept { return (size_t)blk_num * align; }
    void *alloc(uint32_t size) {
        assert_check(size != 0);
        uint32_t real_blk = ((size + align - 1) / align) + 1;
        uint32_t free_blk = blk_cap + tail - head;
        if (real_blk > free_blk)
            return nullptr;
        ctrl_block *elem = &ctl_blks[head & blk_cap];
        elem->flag = flag_inuse;
        elem->len = real_blk;
        head += real_blk;
        if ((head & blk_cap) < ((head - real_blk) & blk_cap)) {
            elem->flag = flag_gc;
            return alloc(size);
        }
        size_t offset = (size_t)((head - real_blk) & blk_cap) * align;
        *(void**)(data + offset) = this;
        return data + offset + align;
    }
    void free(void *ptr) {
        assert_check(ptr != nullptr);
        ctrl_block *elem = &ctl_blks[(((uint8_t *)ptr - data) / align) - 1];
        if (elem->flag != flag_inuse) {
            log_warn("invaild ptr: head: %u  tail: %u  cap: %u", head, tail, blk_cap);
            return;
        }
        elem->flag = flag_gc;
        elem = &ctl_blks[tail & blk_cap];
        while (elem->flag == flag_gc && tail != head) {
            tail += elem->len;
            elem->flag = flag_freed;
            elem = &ctl_blks[tail & blk_cap];
        }
    }
    template <is_integral... Ts>
    auto alloc_many(Ts... size) {
        uint32_t tot{0};
        ((tot += upper_align(size, align)), ...);
        return alloc_many_impl((uint64_t)alloc(tot), size...);
    }
};

using tempmp = tempmp_impl<cache_line_size>;
inline void tempmp_free(void **ptr) { (*(tempmp **)((uint8_t *)*ptr - cache_line_size))->free(*ptr); }
#define tempmp_auto __attribute__((cleanup(tempmp_free))) void *

template <typename T>
class cycle_queue : noncopyable
{
    T *data;
    uint32_t head, tail, cap;

public:
    cycle_queue(uint32_t _size) : head(0), tail(0), cap(_size - 1) {
        assert_require(is_power_of2(_size) && _size >= 4);
        assert_require(data = (T *)alloc_hugepage(_size * sizeof(T)));
    }
    cycle_queue(cycle_queue &&oth) : data(std::move(oth.data)), head(std::move(oth.head)),
                                     tail(std::move(oth.tail)), cap(std::move(oth.cap)) {}
    ~cycle_queue() { free_hugepage(data, upper_align((cap + 1) * sizeof(T), 1 << 21)); }
    void enqueue(T &&t) {
        assert_check(cap + tail - head > 0);
        data[head++ & cap] = std::move(t);
    }
    T &&dequeue() {
        assert_check(head != tail);
        return std::move(data[tail++ & cap]);
    }
    constexpr uint32_t count() noexcept { return head - tail; }
};

template <typename ValueType, uint32_t size = 256, uint32_t search_cap = 8>
requires std::is_pointer_v<ValueType>
class unique_int_hashmap
{
    static_assert(is_power_of2(size));
    struct {
        uint32_t key;
        ValueType val;
    } data[size];

public:
    unique_int_hashmap() { memset(data, 0, sizeof(data)); }
    void lockfree_insert(uint32_t key, ValueType val) {
        uint32_t hkey = key & (size - 1);
        uint32_t __cas_placeholder;
        for (uint32_t i = 0; i < search_cap; ++i, (hkey = (hkey + 1) & (size - 1))) {
            if (data[hkey].key == 0) {
                __cas_placeholder = 0;
                if (!__atomic_compare_exchange_n(&data[hkey].key, &__cas_placeholder, -1, false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST))
                    continue;
                __atomic_store_n(&data[hkey].val, val, __ATOMIC_SEQ_CST);
                __atomic_store_n(&data[hkey].key, key, __ATOMIC_SEQ_CST);
                break;
            }
        }
    }
    void lockfree_remove(uint32_t key) {
        uint32_t hkey = key & (size - 1);
        for (uint32_t i = 0; i < search_cap; ++i, (hkey = (hkey + 1) & (size - 1)))
            if (data[hkey].key == key) {
                __atomic_store_n(&data[hkey].key, 0, __ATOMIC_SEQ_CST);
                __atomic_store_n(&data[hkey].val, nullptr, __ATOMIC_SEQ_CST);
                break;
            }
    }
    ValueType search(uint32_t key) {
        uint32_t hkey = key & (size - 1);
        for (uint32_t i = 0; i < search_cap; ++i, (hkey = (hkey + 1) & (size - 1)))
            if (data[hkey].key == key)
                return data[hkey].val;
        return nullptr;
    }
};

template <typename T>
struct task_result
{
    constexpr bool has_value() const noexcept { return std::get_if<std::monostate>(&__result) == nullptr; }
    constexpr bool has_error() const noexcept { return std::get_if<std::exception_ptr>(&__result) != nullptr; }
    template <typename R>
    constexpr void return_value(R &&value) noexcept { __result.template emplace<T>(std::forward<R>(value)); }
    void unhandled_exception() noexcept { __result = std::current_exception(); }
    constexpr T result() & {
        if (auto res = std::get_if<T>(&__result)) [[likely]]
            return *res;
        if (auto exception = std::get_if<std::exception_ptr>(&__result)) [[unlikely]]
            std::rethrow_exception(*exception);
        throw std::logic_error("result is not set");
    }
    constexpr T result() && {
        if (auto res = std::get_if<T>(&__result)) [[likely]]
            return std::move(*res);
        if (auto exception = std::get_if<std::exception_ptr>(&__result)) [[unlikely]]
            std::rethrow_exception(*exception);
        throw std::logic_error("result is not set");
    }

    std::variant<std::monostate, T, std::exception_ptr> __result;
};

template <>
struct task_result<void>
{
    constexpr bool has_value() const noexcept { return __result.has_value(); }
    constexpr bool has_error() const noexcept { return (__result.has_value() && *__result != nullptr); }
    void return_void() noexcept { __result.emplace(nullptr); }
    void unhandled_exception() noexcept { __result = std::current_exception(); }
    void result() {
        if (has_error()) [[unlikely]]
            std::rethrow_exception(*__result);
        if (!has_value()) [[unlikely]]
            throw std::logic_error("result is not set");
    }

    std::optional<std::exception_ptr> __result;
};

struct undelay_task {
    struct promise_type {
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() { std::rethrow_exception(std::current_exception()); }
        undelay_task get_return_object() noexcept { return {}; }
    };
};

template <typename R = void>
struct [[nodiscard]] task : private noncopyable
{
    struct promise_type;
    using coro_handle = std::coroutine_handle<promise_type>;
    using value_type = R;

    explicit task(coro_handle h = nullptr) noexcept : __handle(h) {}
    task(task &&t) noexcept : __handle(std::exchange(t.__handle, {})) {}
    task &operator=(task &&t) { __handle = std::exchange(t.__handle, {}); return *this; }
    ~task() { destroy(); }

    decltype(auto) get_result() & { return __handle.promise().result(); }
    decltype(auto) get_result() && { return std::move(__handle.promise()).result(); }

    struct final_awaiter {
        constexpr bool await_ready() noexcept { return false; }
        template <typename PromiseType>
        constexpr decltype(auto) await_suspend(std::coroutine_handle<PromiseType> h) noexcept
        { return h.promise().__previous ? (std::coroutine_handle<>)h.promise().__previous : std::noop_coroutine(); }
        constexpr void await_resume() noexcept {}
    };

    struct promise_type : task_result<R> {
        task get_return_object() noexcept { return task{coro_handle::from_promise(*this)}; }
        std::suspend_always initial_suspend() noexcept { return {}; }
        final_awaiter final_suspend() noexcept { return {}; }
        std::coroutine_handle<> __previous{};
    };

    struct awaiter_base {
        constexpr bool await_ready() { return __coro.done(); }
        template <typename Promise>
        auto await_suspend(std::coroutine_handle<Promise> h) const noexcept { return (__coro.promise().__previous = h), __coro; }
        std::coroutine_handle<promise_type> __coro{};
    };

    auto operator co_await() &noexcept {
        struct awaiter : awaiter_base
        { decltype(auto) await_resume() const { return awaiter_base::__coro.promise().result(); } };
        // return awaiter{std::exchange(__handle, {})};
        return awaiter{__handle};
    }

    auto operator co_await() &&noexcept {
        struct awaiter : awaiter_base
        { decltype(auto) await_resume() const { return std::move(awaiter_base::__coro.promise()).result(); } };
        // return awaiter{std::exchange(__handle, {})};
        return awaiter{__handle};
    }

    bool done() const { return !__handle || __handle.done(); }
    void destroy() {
        if (__handle) {
            __handle.destroy();
            __handle = nullptr;
        }
    }

    template <typename F>
    void start(F &&callback) {
        struct start_awaiter : awaiter_base
        { decltype(auto) await_resume() const { return std::move(awaiter_base::__coro.promise()); } };

        [](task input_task, std::decay_t<F> cb) -> undelay_task
        {
            cb(std::move(co_await start_awaiter{input_task.__handle}));
        }(std::move(*this), std::forward<F>(callback));
    }

    template <typename F>
    void start_value(F &&callback) {
        [](task input_task, std::decay_t<F> cb) -> undelay_task
        {
            cb(std::move(co_await std::move(input_task)));
        }(std::move(*this), std::forward<F>(callback));
    }

    coro_handle __handle;
};

template <uint32_t size = 16>
class alignas(cache_line_size) scsp_task_ring : noncopyable
{
    static_assert(is_power_of2(size) && size > 0);
    volatile uint32_t prod_head alignas(cache_line_size){0};
    volatile uint32_t prod_tail{0};
    volatile uint32_t cons_head alignas(cache_line_size){0};
    volatile uint32_t cons_tail{0};
    task<> data[size];

public:
    inline void enqueue(task<> &&t) {
        assert_check(size - 1 + cons_tail - prod_head > 0);
        prod_head = prod_head + 1; // scsp & aligned -> thread safe
        data[prod_tail & (size - 1)] = std::move(t);
        prod_tail = prod_tail + 1;
    }
    inline task<> &&dequeue() {
        assert_check(prod_tail != cons_head);
        cons_head = cons_head + 1;
        auto &&res = data[cons_tail & (size - 1)];
        cons_tail = cons_tail + 1;
        return std::move(res);
    }
    inline constexpr uint32_t count() noexcept { return prod_tail - cons_head; }
};

template <typename TaskType>
inline auto sync_await(TaskType &&input_task) {
    std::mutex m;
    using value_type = typename std::decay_t<TaskType>::value_type;

    task_result<value_type> value;
    m.lock();
    std::move(std::forward<TaskType>(input_task))
        .start([&m, &value](auto &&result)
               { value = std::move(result);  m.unlock(); });
    m.lock();
    return std::move(value).result();
}

template <typename T>
concept raw_awaitable = requires(T t, std::coroutine_handle<> handle)
{
    t.await_ready();
    t.await_suspend(handle);
    t.await_resume();
};

template <typename A>
struct get_awaiter : std::type_identity<A>
{ };

template <typename A>
requires requires(A &&a) { std::forward<A>(a).operator co_await(); }
struct get_awaiter<A> : std::type_identity<decltype(std::declval<A>().operator co_await())>
{ };

template <typename A>
requires requires(A &&a)
{
    operator co_await(std::forward<A>(a));
    requires !(requires { std::forward<A>(a).operator co_await(); });
}
struct get_awaiter<A> : std::type_identity<decltype(operator co_await(std::declval<A>()))>
{ };

template <typename A>
using get_awaiter_t = typename get_awaiter<A>::type;

template <typename A>
using get_result_t = task_result<decltype(std::declval<get_awaiter_t<A>>().await_resume())>;

template <typename A>
using get_value_t = decltype(std::declval<get_awaiter_t<A>>().await_resume());

struct try_resumer
{
    std::coroutine_handle<> resume_handler;
    size_t __target;
    size_t __ready{0}; // assume thread safe
    void operator()() {
        if (++__ready == __target)
            resume_handler();
    }
};

template <raw_awaitable T, typename R, typename F>
inline void gather_start(T &t, task_result<R> &r, F &f)
{
    [](T &t, task_result<R> &r, F &f) -> undelay_task {
        try {
            if constexpr (std::is_void_v<R>) {
                co_await t;
                r.__result = nullptr;
            } else r.__result.template emplace<R>(std::move(co_await t));
        } catch (...)
        { r.__result = std::current_exception(); }
        f();
    }(t, r, f);
}

template <raw_awaitable T, typename R, typename F>
inline void gather_start_value(T &t, R &r, F &f)
{
    [](T &t, R &r, F &f) -> undelay_task {
        r = co_await t;
        f();
    }(t, r, f);
}

template <typename T, typename R, typename F>
inline void gather_start(task<T> &t, task_result<R> &r, F &f)
{ t.start([&](task_result<R> &&res) { r = res; f(); }); }

template <typename T, typename R, typename F>
inline void gather_start_value(task<T> &t, R &r, F &f)
{ t.start_value([&](R &&res) { r = res; f(); }); }

template <bool is_val, typename AwaitableTypes>
struct [[nodiscard]] gather_awaiter
{
    using result_type = std::conditional_t<is_val, get_value_t<AwaitableTypes>, get_result_t<AwaitableTypes>>;
    static_assert(!std::is_same_v<result_type, bool>);
    gather_awaiter(std::vector<AwaitableTypes> &&input_tasks)
        : __tasks(std::move(input_tasks)) { __task_results.resize(__tasks.size()); }
    constexpr bool await_ready() { return __tasks.size() == 0; }
    void await_suspend(std::coroutine_handle<> h) {
        __resumer.resume_handler = h;
        __resumer.__target = __tasks.size() + 1;
        for (size_t i = 0; i < __tasks.size(); ++i)
            if constexpr (is_val) gather_start_value(__tasks[i], __task_results[i], __resumer);
            else gather_start(__tasks[i], __task_results[i], __resumer);
        __resumer();
    }
    inline decltype(auto) await_resume() { return std::move(__task_results); }
    std::vector<AwaitableTypes> __tasks;
    std::vector<result_type> __task_results;
    try_resumer __resumer;
};

template <typename AwaitableTypes>
[[nodiscard]] task<std::vector<get_result_t<AwaitableTypes>>> gather(std::vector<AwaitableTypes> &&input_awaitables)
{ co_return co_await gather_awaiter<false, AwaitableTypes>(std::forward<std::vector<AwaitableTypes>>(input_awaitables)); }

template <typename AwaitableTypes>
[[nodiscard]] task<std::vector<get_value_t<AwaitableTypes>>> gather_value(std::vector<AwaitableTypes> &&input_awaitables)
{ co_return co_await gather_awaiter<true, AwaitableTypes>(std::forward<std::vector<AwaitableTypes>>(input_awaitables)); }

template <typename... AwaitableTypes>
struct [[nodiscard]] gather_tuple_awaiter
{
    gather_tuple_awaiter(AwaitableTypes &&...input_awaitables)
        : __awaitables(std::make_tuple(std::forward<AwaitableTypes>(input_awaitables)...)) {};
    constexpr bool await_ready() { return sizeof...(AwaitableTypes) == 0; }
    void await_suspend(std::coroutine_handle<> h) {
        __resumer.resume_handler = h;
        __resumer.__target = sizeof...(AwaitableTypes) + 1;
        [&]<size_t... Is>(std::index_sequence<Is...>)
        { (gather_start(std::get<Is>(__awaitables), std::get<Is>(__awaitable_results), __resumer), ...); }
        (std::make_index_sequence<sizeof...(AwaitableTypes)>{});
        __resumer();
    }
    inline decltype(auto) await_resume() { return std::move(__awaitable_results); }
    std::tuple<AwaitableTypes...> __awaitables;
    std::tuple<get_result_t<AwaitableTypes>...> __awaitable_results;
    try_resumer __resumer;
};

template <typename... AwaitableTypes>
[[nodiscard]] task<std::tuple<get_result_t<AwaitableTypes>...>> gather(AwaitableTypes &&...input_awaitables)
{ co_return co_await gather_tuple_awaiter(std::forward<AwaitableTypes>(input_awaitables)...); }

template <typename... AwaitableTypes>
[[nodiscard]] task<std::tuple<get_value_t<AwaitableTypes>...>> gather_value(AwaitableTypes &&...input_awaitables)
{ co_return tmap(x.result(), co_await gather_tuple_awaiter(std::forward<AwaitableTypes>(input_awaitables)...)); }