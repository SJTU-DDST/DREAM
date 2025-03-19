#pragma once
#include "aiordma.h"
#include "alloc.h"
#include "config.h"
#include "hash.h"
#include "kv_trait.h"
#include "perf.h"
#include "search.h"
#include <cassert>
#include <chrono>
#include <fcntl.h>
#include <map>
#include <math.h>
#include <tuple>
#include <vector>
#include <random>

// #define TOO_LARGE_KV


namespace SEPHASH
{
constexpr uint64_t SEGMENT_SIZE = 1024;
constexpr uint64_t SLOT_PER_SEG = ((SEGMENT_SIZE) / (sizeof(uint64_t)+sizeof(uint8_t)));
constexpr uint64_t SLOT_BATCH_SIZE = 8;
constexpr uint64_t RETRY_LIMIT = (SLOT_PER_SEG/SLOT_BATCH_SIZE); // TODO : 后期试试改成其他较小的值
#if LARGE_MAIN_SEG
constexpr uint64_t MAX_MAIN_SIZE = 640000 * SLOT_PER_SEG; // IMPORTANT: 打开后相当于main seg不能分裂
#else
constexpr uint64_t MAX_MAIN_SIZE = 64 * SLOT_PER_SEG;
#endif
// constexpr uint64_t MAX_FP_INFO = 256;
constexpr uint64_t INIT_DEPTH = 8;
// constexpr uint64_t MAX_DEPTH = 16;
// constexpr uint64_t DIR_SIZE = (1 << MAX_DEPTH);
constexpr uint64_t ALIGNED_SIZE = 64;             // aligned size of len bitfield in DepSlot
constexpr uint64_t dev_mem_size = (1 << 10) * 64; // 64KB的dev mem，用作lock
constexpr uint64_t num_lock =
    (dev_mem_size - sizeof(uint64_t)) / sizeof(uint64_t); // Lock数量，client对seg_id使用hash来共享lock

inline __attribute__((always_inline)) uint64_t fp(uint64_t pattern)
{
    return ((uint64_t)((pattern) >> 32) & ((1 << 8) - 1));
}

inline __attribute__((always_inline)) uint64_t fp2(uint64_t pattern)
{
    return ((uint64_t)((pattern) >> 24) & ((1 << 8) - 1));
}

inline __attribute__((always_inline)) uint64_t get_seg_loc(uint64_t pattern, uint64_t global_depth)
{
    return ((pattern) & ((1 << global_depth) - 1));
}

#if LARGER_FP_FILTER_GRANULARITY
inline __attribute__((always_inline)) uint64_t get_fp_bit(uint8_t fp1, uint8_t fp2)
{
    uint64_t fp = fp1;
    fp = fp << 8;
    fp = fp | fp2;
    // fp = fp & ((1 << 4) - 1); // 现在先用原来的16位filter，后面再改
    fp %= FP_BITMAP_LENGTH;
    return fp;
}
#else
inline __attribute__((always_inline)) std::tuple<uint64_t, uint64_t> get_fp_bit(uint8_t fp1, uint8_t fp2)
{
    uint64_t fp = fp1;
    fp = fp << 8;
    fp = fp | fp2;
    fp = fp & ((1 << 10) - 1);
    uint64_t bit_loc = fp / 64;
    uint64_t bit_info = (fp % 64);
    bit_info = 1ll << bit_info;
    return std::make_tuple(bit_loc, bit_info);
}
#endif

void cal_fpinfo(Slot *main_seg, uint64_t main_seg_len, FpInfo *fp_info);
// struct Slot
// {
//     uint8_t fp : 8;
//     uint8_t len : 3;
//     uint8_t sign : 1; // 用来表示split delete信息
//     uint8_t dep : 4;
//     uint64_t offset : 48;
//     uint8_t fp_2;
//     operator uint64_t()
//     {
//         return *(uint64_t *)this;
//     }
//     Slot(uint64_t u)
//     {
//         *this = *(Slot *)(&u);
//     }
//     bool operator<(const Slot &a) const
//     {
//         return fp < a.fp;
//     }
//     void print(uint64_t slot_id = -1)
//     {
//         if(slot_id!=-1) printf("slot_id:%lu\t", slot_id);
//         printf("fp:%x\t", fp);
//         printf("fp_2:%x\t", fp_2);
//         printf("len:%d\t", len);
//         printf("sign:%d\t", sign);
//         printf("dep:%d\t", dep);
//         printf("offset:%lx\t", offset);
//         printf("size:%ld\n", sizeof(Slot));
//     }
// }__attribute__((aligned(1)));

struct Slice
{
    uint64_t len;
    char *data;

    void print_var_kv(){
        printf("len:%lu\n",len);
        for(uint64_t i = 0 ; i < len/8 ; i++){
            printf("key[%lu]:%lu\t",i,*(uint64_t *)(data+8*i));
        }
        printf("\n");
    }
};

struct KVBlock
{
    uint64_t k_len;
    uint64_t v_len;
    char data[0]; // 变长数组，用来保证KVBlock空间上的连续性，便于RDMA操作
    void print(const char *desc = nullptr)
    {
        if (desc != nullptr)
            log_err("%s klen:%lu key:%lu vlen:%lu value:%s", desc, k_len, *(uint64_t *)data, v_len,
                    data + sizeof(uint64_t));
        else
            log_err("klen:%lu key:%lu vlen:%lu value:%s", k_len, *(uint64_t *)data, v_len, data + sizeof(uint64_t));
    }

    void print_var_kv(){
        printf("klen:%lu vlen:%lu\n",k_len,v_len);
        for(uint64_t i = 0 ; i < k_len/8 ; i++){
            printf("key[%lu]:%lu\t",i,*(uint64_t *)(data+8*i));
        }
        printf("\n");
    }
}__attribute__((aligned(1)));

template <typename Alloc>
    requires Alloc_Trait<Alloc, uint64_t>
KVBlock *InitKVBlock(Slice *key, Slice *value, Alloc *alloc)
{
    KVBlock *kv_block = (KVBlock *)alloc->alloc(2 * sizeof(uint64_t) + key->len + value->len);
    kv_block->k_len = key->len;
    kv_block->v_len = value->len;
    memcpy(kv_block->data, key->data, key->len);
    memcpy(kv_block->data + key->len, value->data, value->len);
    return kv_block;
}

// struct CurSegMeta{
//     uint8_t sign : 1; // 实际中的split_lock可以和sign、depth合并，这里为了不降rdma驱动版本就没有合并。
//     uint64_t local_depth : 63;
//     uintptr_t main_seg_ptr;
//     uintptr_t main_seg_len;
//     uint64_t fp_bitmap[16]; // 16*64 = 1024,代表10bits fp的出现情况；整个CurSeg大约会出现（1024/8=128）个FP，因此能极大的减少search对CurSeg的访问
// }__attribute__((aligned(1)));

// struct CurSeg
// {
//     uint64_t split_lock;
//     CurSegMeta seg_meta;
//     Slot slots[SLOT_PER_SEG];
// }__attribute__((aligned(1)));

struct MainSeg
{
    Slot slots[0];
    
    void print(uint64_t length) {
        printf("MainSeg with %lu slots\n", length);
        for (uint64_t i = 0; i < length; i++) {
            printf("[%lu] ", i);
            slots[i].print();
        }
        printf("\n");
    }
    
    std::string to_string(uint64_t length) {
        std::stringstream ss;
        ss << "MainSeg with " << length << " slots" << std::endl;
        for (uint64_t i = 0; i < length; i++) {
            ss << "[" << i << "] ";
            // Assuming Slot::print() prints to stdout, we need a way to capture its output
            // Instead, we'll add the relevant slot fields directly
            ss << "fp:" << (int)slots[i].fp << "\t";
            ss << "fp_2:" << (int)slots[i].fp_2 << "\t";
            ss << "len:" << (int)slots[i].len << "\t";
            ss << "sign:" << (int)slots[i].sign << "\t";
            ss << "dep:" << (int)slots[i].dep << "\t";
            ss << "offset:" << std::hex << slots[i].offset << std::dec << std::endl;
        }
        return ss.str();
    }
}__attribute__((aligned(1)));

// struct FpInfo{ 
//     uint8_t num; // 数量 
//     operator uint64_t()
//     {
//         return *(uint64_t *)this;
//     }
// }__attribute__((aligned(1)));

// struct DirEntry
// {
//     // TODO : 实际上只需要用5 bits，为了方便ptr统一48，所以这里仍保留16bits
//     uint64_t local_depth ; 
//     uintptr_t cur_seg_ptr ;
//     uintptr_t main_seg_ptr ;
//     uint64_t main_seg_len ;
//     FpInfo fp[MAX_FP_INFO];
//     bool operator==(const DirEntry &other) const
//     {
//         return cur_seg_ptr == other.cur_seg_ptr && main_seg_ptr == other.main_seg_ptr &&
//                main_seg_len == other.main_seg_len;
//     }

//     void print(std::string desc = ""){
//         log_err("%s local_depth:%lu cur_seg_ptr:%lx main_seg_ptr:%lx main_seg_lne:%lx",desc.c_str(),local_depth,cur_seg_ptr,main_seg_ptr,main_seg_len);
//     }
// } __attribute__((aligned(1)));

// struct Directory
// {
//     uint64_t global_depth;   // number of segment
//     DirEntry segs[DIR_SIZE]; // Directory use MSB and is allocated enough space in advance.
//     uint64_t start_cnt;      // 为多客户端同步保留的字段，不影响原有空间布局

//     void print(std::string desc = ""){
//         log_err("%s Global_Depth:%lu", desc.c_str(), global_depth);
//         for(uint64_t i = 0 ; i < (1<<global_depth) ; i++){
//             log_err("Entry %lx : local_depth:%lu cur_seg_ptr:%lx main_seg_ptr:%lx main_seg_lne:%lx",i,segs[i].local_depth,segs[i].cur_seg_ptr,segs[i].main_seg_ptr,segs[i].main_seg_len);
//         }
//     }
// } __attribute__((aligned(1)));

struct SlotOffset
{
    // 记录每个CurSeg中上次insert访问到的slot offset
    bool sign;
    uint8_t offset; 
    uint64_t main_seg_ptr; 
} __attribute__((aligned(1)));


class Client : public BasicDB
{
  public:
    Client(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, rdma_conn *_wowait_conn,
           uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id);

    Client(const Client &) = delete;

    ~Client();

    // Used for sync operation and test
    task<> start(uint64_t total);
    task<> stop();
    task<> reset_remote();
    task<> cal_utilization();

    task<> insert(Slice *key, Slice *value);
    task<std::tuple<uintptr_t, uint64_t>> search(Slice *key, Slice *value);
    task<> update(Slice *key, Slice *value);
    task<> remove(Slice *key);

  protected:
    Config &config;

    task<> sync_dir();
    task<uintptr_t> check_gd(uint64_t segloc = -1, bool read_fp = false);

    task<> Split(uint64_t seg_loc, uintptr_t seg_ptr, CurSegMeta *old_seg_meta);
#if READ_FULL_KEY_ON_FP_COLLISION
    task<uint64_t> merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg);
#else
    uint64_t merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg);
#endif
  
    task<> print_main_seg(uint64_t seg_loc,uintptr_t main_seg_ptr, uint64_t main_seg_len);

    // rdma structs
    rdma_client *cli;
    rdma_conn *conn;
    rdma_conn *wo_wait_conn;
    rdma_rmr seg_rmr;
    struct ibv_mr *lmr;

    Alloc alloc;
    RAlloc ralloc;
    
    // Debug
    uint64_t machine_id;
    uint64_t cli_id;
    uint64_t coro_id;
    uint64_t key_num;
    uint64_t key_off;
    uint64_t kv_block_len;

    // Statistic
    Perf perf;
    SumCost sum_cost;
    uint64_t op_cnt;
    uint64_t miss_cnt;
    uint64_t retry_cnt; // 其实是try_cnt，=1是一次成功，=2是重试了一次，retry_cnt = try_cnt - 1

    // Data part
    SlotOffset offset[DIR_SIZE] ; // 记录当前CurSeg中的freeslot开头？仅作参考，还是每个cli进行随机read
                        // 还是随机read吧，使用一个固定的序列？保存在本地，免得需要修改远端的。
    Directory *dir;
#if LARGER_FP_FILTER_GRANULARITY
    std::unordered_map<size_t, CurSegMeta> seg_meta; // 本地缓存CurSegMeta, TODO: 换成指针并用alloc.alloc分配
#endif
};

class Server : public BasicDB
{
 public:
    Server(Config &config);
    ~Server();

  protected:
    void Init();

    rdma_dev dev;
    rdma_server ser;
    struct ibv_mr *seg_mr;
    ibv_dm *lock_dm; // Locks for Segments
    ibv_mr *lock_mr;
    char *mem_buf;

    Alloc alloc;
    Directory *dir;
};

} // namespace SPLIT_HASH