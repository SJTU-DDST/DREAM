#include "clevel.h"
#include <csignal>
#include <atomic>
#include <condition_variable>
#include <mutex>

namespace
{
    std::atomic_bool g_stop_flag{false};
    std::condition_variable g_cv;
    std::mutex g_mtx;

    void signal_handler(int)
    {
        g_stop_flag = true;
        g_cv.notify_all();
    }
}
namespace CLEVEL
{

inline __attribute__((always_inline)) uint64_t fp(uint64_t pattern)
{
    return ((uint64_t)((pattern) >> 32) & ((1 << 8) - 1));
}

inline __attribute__((always_inline)) uint64_t get_seg_loc(uint64_t pattern, uint64_t global_depth)
{
    return ((pattern) & ((1 << global_depth) - 1));
}

void KvCache::update_cache(uint64_t offset,Slice* key,Slice* value){
    auto iter = cache.find(offset);
    // 1. 在cache中
    if(iter!= cache.end()){
        memcpy(iter->second->data,key->data,key->len);
        memcpy(iter->second->data + key->len,value->data,value->len);
    }else{
        // 2. 不在cache中，判断是否有多余空间
        uint64_t cache_size = sizeof(cache);
        cache_size += cache.size()*(2*sizeof(uint64_t)+key->len + value->len); 
        if(cache_size < max_cache_size){
            KVBlock* kv_block = (KVBlock*) malloc(2*sizeof(uint64_t)+key->len + value->len);;
            kv_block->k_len = key->len;
            kv_block->v_len = value->len;
            memcpy(kv_block->data,key->data,key->len);
            memcpy(kv_block->data+key->len,value->data,value->len);
            cache[offset] = kv_block;
        }else{
            // 3.kvcache的话就不淘汰了 反正都会命中
        }
    }
}


KVBlock* KvCache::get_cache(uint64_t offset){
    auto iter = cache.find(offset);
    if(iter!= cache.end()){
        return iter->second;
    }
    return nullptr;
}

Server::Server(Config &config) : dev(nullptr, 1, config.gid_idx), ser(dev)
{
    seg_mr = dev.reg_mr(233, config.mem_size);
#if USE_DM_MR
    auto [dm, mr] = dev.reg_dmmr(234, dev_mem_size);
    lock_dm = dm;
    lock_mr = mr;
#else
    lock_mr = dev.reg_mr(234, dev_mem_size);
#endif

    alloc.Set((char *)seg_mr->addr, seg_mr->length);
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    Init();
    log_err("init");

    // Init locks
#if USE_DM_MR
    char tmp[dev_mem_size] = {}; // init locks to zero
    lock_dm->memcpy_to_dm(lock_dm, 0, tmp, dev_mem_size);
#else
    memset(lock_mr->addr, 0, dev_mem_size);
#endif
    log_err("memset");

    if (config.auto_run_client)
    {
        log_err("auto run client");
        config.print();

        ser.start_serve();
        std::string type = "client";
        if (config.server_ips.size() > 1)
            type = "ser_cli"; // 需要额外运行servers

        log_err("start clients with run.py");
        std::string command = std::format("python3 ../run.py {} {} {} {}", config.num_machine, type, config.num_cli, config.num_coro);
        log_err("Auto run client command: %s", command.c_str());
        int result = system(command.c_str());
        log_err("run.py completed with result: %d", result);
    }
    else
    {
        signal(SIGINT, signal_handler);
        signal(SIGTERM, signal_handler);
        auto wait_exit = [&]()
        {
            std::unique_lock<std::mutex> lk(g_mtx);
            g_cv.wait(lk, []
                      { return g_stop_flag.load(); });
            ser.stop_serve();
            log_err("Exiting...");
        };
        std::thread th(wait_exit);
        ser.start_serve();
        th.join();
    }
}

void Server::Init()
{
    dir->is_resizing = 0;

    dir->last_level = (uintptr_t)alloc.alloc(sizeof(LevelTable)+sizeof(Bucket)*INIT_TABLE_SIZE);
    dir->first_level = (uintptr_t)alloc.alloc(sizeof(LevelTable)+sizeof(Bucket)*INIT_TABLE_SIZE*2);

    LevelTable* cur_table = (LevelTable*)dir->last_level;
    cur_table->up = dir->first_level;
    cur_table->capacity = INIT_TABLE_SIZE;
    memset(cur_table->buckets,0,sizeof(Bucket)*cur_table->capacity);

    cur_table = (LevelTable*)dir->first_level;
    cur_table->up = 0;
    cur_table->capacity = INIT_TABLE_SIZE*2;
    memset(cur_table->buckets,0,sizeof(Bucket)*cur_table->capacity);
}

Server::~Server()
{
    rdma_free_mr(seg_mr);
#if USE_DM_MR
    rdma_free_dmmr({lock_dm, lock_mr});
#else
    rdma_free_mr(lock_mr);
#endif
}

Client::Client(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, rdma_conn *_wowait_conn,
                uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id, uint64_t _server_id)
{
    // id info
    machine_id = _machine_id;
    cli_id = _cli_id;
    coro_id = _coro_id;

    // rdma utils
    cli = _cli;
    conn = _conn;
    wo_wait_conn = _wowait_conn;
    lmr = _lmr;

    // alloc info
    alloc.Set((char *)lmr->addr, lmr->length);
    seg_rmr = cli->run(conn->query_remote_mr(233));
    lock_rmr = cli->run(conn->query_remote_mr(234));
    // uint64_t rbuf_size = (seg_rmr.rlen - (1ul << 20) * 20) /
    //                         (config.num_machine * config.num_cli * config.num_coro); // 头部保留5GB，其他的留给client
    // 对于Cluster Hash，其头部空间全部留着用来作为Table的空间，ralloc仅用来写入KV Block
    // 11000000 * ( 8*2 + 8 + 32) = 588 MB
    uint64_t rbuf_size = ((1ul << 30) * 20) / (config.num_machine * config.num_cli * config.num_coro);
    uint64_t buf_id = config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id;
    uintptr_t remote_ptr = seg_rmr.raddr + seg_rmr.rlen - rbuf_size * buf_id; // 从尾部开始分配
    ralloc.SetRemote(remote_ptr, rbuf_size, seg_rmr.raddr, seg_rmr.rlen);
    ralloc.alloc(ALIGNED_SIZE); // 提前分配ALIGNED_SIZE，免得读取的时候越界
    // log_err("ralloc start_addr:%lx offset_max:%lx", ralloc.raddr, ralloc.rsize);

    // util variable
    op_cnt = 0;
    miss_cnt = 0;
    perf.init();
    sum_cost.init();

    // sync dir
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
}

Client::~Client()
{
    // log_err("[%lu:%lu] miss_cnt:%lu cache_hit_cnt:%lu", cli_id, coro_id, miss_cnt,cache_hit_cnt);
    perf.print(cli_id,coro_id);
    sum_cost.print(cli_id,coro_id);
}

task<> Client::reset_remote()
{
    // 模拟远端分配器信息
    Alloc server_alloc;
    server_alloc.Set((char *)seg_rmr.raddr, seg_rmr.rlen);
    server_alloc.alloc(sizeof(Directory));

    // 重置远端 Lock
    alloc.ReSet(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    co_await conn->write(lock_rmr.raddr, lock_rmr.rkey, dir, dev_mem_size, lmr->lkey);

    // 重置远端segment
    dir->is_resizing = 0;
    
    dir->last_level = (uintptr_t)server_alloc.alloc(sizeof(LevelTable)+sizeof(Bucket)*INIT_TABLE_SIZE);
    dir->first_level = (uintptr_t)server_alloc.alloc(sizeof(LevelTable)+sizeof(Bucket)*INIT_TABLE_SIZE*2);

    LevelTable* cur_table = (LevelTable*)alloc.alloc(sizeof(LevelTable));
    Bucket* buc = (Bucket*)alloc.alloc(sizeof(Bucket)*zero_size);  // 不知道为啥这里0长数组失效了，只能手动分配空间
    memset(buc,0,sizeof(Bucket)*zero_size);
    cur_table->up = dir->first_level;
    cur_table->capacity = INIT_TABLE_SIZE;
    uint64_t upper = INIT_TABLE_SIZE / zero_size ;
    for(uint64_t i = 0 ; i < upper ; i++){
        co_await conn->write(dir->last_level+sizeof(LevelTable)+i*zero_size*sizeof(Bucket),seg_rmr.rkey,buc,sizeof(Bucket)*zero_size,lmr->lkey);
    }
    co_await conn->write(dir->last_level,seg_rmr.rkey,cur_table,sizeof(LevelTable),lmr->lkey);
    
    cur_table->up = 0;
    cur_table->capacity = INIT_TABLE_SIZE*2;
    upper = cur_table->capacity / zero_size ;
    for(uint64_t i = 0 ; i < upper ; i++){
        co_await conn->write(dir->first_level+sizeof(LevelTable)+i*zero_size*sizeof(Bucket),seg_rmr.rkey,buc,sizeof(Bucket)*zero_size,lmr->lkey);
    }
    co_await conn->write(dir->first_level,seg_rmr.rkey,cur_table,sizeof(LevelTable),lmr->lkey);

    // 重置远端 Directory
    co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);
}

task<> Client::start(uint64_t total)
{
    uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t), true);
    *start_cnt = 0;
    co_await conn->fetch_add(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, *start_cnt, 1);
    // log_info("Start_cnt:%lu", *start_cnt);
    while ((*start_cnt) < total)
    {
        co_await conn->read(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, start_cnt,
                            sizeof(uint64_t), lmr->lkey);
    }
}

task<> Client::stop()
{
    uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t));
    co_await conn->fetch_add(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, *start_cnt, -1);
    // log_err("Start_cnt:%lu", *start_cnt);
    while ((*start_cnt) != 0)
    {
        co_await conn->read(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, start_cnt,
                            sizeof(uint64_t), lmr->lkey);
    }
}

task<> Client::cal_utilization(){
    if(this->machine_id !=0 || this->cli_id != 0 || this->coro_id != 0) co_return;
    uint64_t space_consumption = 3*sizeof(uint64_t);
    uint64_t segment_cnt = 0 ;
    uint64_t entry_total = 0 ;
    uint64_t entry_cnt = 0 ;

    // 1. Read Header
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);    

    // 2. 遍历level
    uintptr_t cur_table_ptr = dir->last_level;
    LevelTable* cur_table = (LevelTable*) alloc.alloc(sizeof(LevelTable));
    Bucket* buc = (Bucket*)alloc.alloc(sizeof(Bucket)*zero_size);  // 不知道为啥这里0长数组失效了，只能手动分配空间
    uint64_t level_id = 0;
    while(cur_table_ptr != 0){
        // 2.1 Read LevelTable Header : capacity,up
        co_await conn->read(cur_table_ptr,seg_rmr.rkey,cur_table,sizeof(LevelTable),lmr->lkey);
        space_consumption += 2*sizeof(uint64_t) + cur_table->capacity*BUCKET_SIZE*sizeof(Entry);
        entry_total += cur_table->capacity*BUCKET_SIZE;
        log_err("level:%lu capacity:%lu",level_id,cur_table->capacity);
        for(uint64_t i = 0 ; i < cur_table->capacity ; i+=zero_size){
            co_await conn->read(cur_table_ptr + i * sizeof(Bucket),seg_rmr.rkey,buc,zero_size * sizeof(Bucket),lmr->lkey);
            for(uint64_t buc_id  = 0 ; buc_id < zero_size ; buc_id++){
                for(uint64_t slot_id = 0 ; slot_id < BUCKET_SIZE ; slot_id++){
                    if(cur_table->buckets[buc_id].entrys[slot_id].offset != 0){
                        entry_cnt++;
                    }
                }
            }
        }
        cur_table_ptr = cur_table->up;
        level_id++;
    }
    double space_utilization = (1.0*entry_cnt*sizeof(Entry))/(1.0*space_consumption);
    // space_consumption = space_consumption>>20;
    double entry_utilization = (1.0*entry_cnt)/(1.0*entry_total);
    log_err("space_consumption:%luMB segment_cnt:%lu entry_total:%lu entry_cnt:%lu entry_utilization:%lf space_utilization:%lf",space_consumption,segment_cnt,entry_total,entry_cnt,entry_utilization,space_utilization);
}


task<> Client::insert(Slice *key, Slice *value)
{
    perf.start_insert();
    sum_cost.start_insert();
    op_cnt++;
    uint64_t op_size = (1 << 20) * 1;
    // 因为存在pure_write,为上一个操作保留的空间，1MB够用了
    alloc.ReSet(sizeof(Directory));
    auto pattern = hash(key->data, key->len);
    uint64_t pattern_1 = (uint64_t)pattern;
    uint64_t pattern_2 = (uint64_t)(pattern >> 64);
    uint64_t tmp_fp = fp(pattern_1);
    KVBlock *kv_block = InitKVBlock(key, value, &alloc);
    uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 2;
    uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
    // writekv
    wo_wait_conn->pure_write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey);
    retry_cnt = 0;
    this->key_num = *(uint64_t *)key->data;
    KVBlock *tmp_block = (KVBlock *)alloc.alloc(17 * ALIGNED_SIZE);
    uint64_t level_cnt = 0;
Retry:
    retry_cnt++;
    if(retry_cnt >= 1000000){
        // log_err("[%lu:%lu:%lu]too much retry",this->cli_id,this->coro_id,this->key_num);
        // exit(-1);
    }
    alloc.ReSet(sizeof(Directory) + kvblock_len + 17 * ALIGNED_SIZE);

    // log_err("[%lu:%lu:%lu]",this->cli_id,this->coro_id,this->key_num);
    // 1. Read Header
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);    

    // 2. Find Duplicate or Free Slot
    // https://www.icyf.me/2020/09/04/paper-atc20-clevel/
    // 对于插入：由最底层开始逐层向上寻找空位插入； 对于更新：由最底层开始逐层向上查找，如果出现多次命中，只保留位于最高层的那一个。
    // 根据CLevel的源码，第一次需要从下往上查找所有匹配key并进行删除，然后插入到尽可能高位的Empty Entry
    // 对于可能出现的duplicate key，都允许存在;search时，认为最高Level的Insert为有效数据
    LevelTable* cur_table = (LevelTable*) alloc.alloc(sizeof(LevelTable));
    uintptr_t cur_table_ptr = dir->last_level;
    Bucket * buc1 = (Bucket*)alloc.alloc(sizeof(Bucket));
    Bucket * buc2 = (Bucket*)alloc.alloc(sizeof(Bucket));
    uint64_t level_id = 0;
    uintptr_t free_slot_ptr = -1;
    uintptr_t free_level_id = -1;
    uintptr_t free_buc_id = -1;
    uintptr_t free_entry_id = -1;
    uint64_t first_level_ptr = -1;

    while(cur_table_ptr != 0){
        level_cnt++;
        // 2.1 Read LevelTable Header : capacity,up
        co_await conn->read(cur_table_ptr,seg_rmr.rkey,cur_table,sizeof(LevelTable),lmr->lkey);
        uint64_t buc_idx1,buc_idx2;
        buc_idx1 = pattern_1 % cur_table->capacity;
        buc_idx2 = pattern_2 % cur_table->capacity;
        
        // 2.2 Read 2 Bucket
        uintptr_t buc_ptr1 = cur_table_ptr + sizeof(LevelTable) + buc_idx1 * sizeof(Bucket);
        uintptr_t buc_ptr2 = cur_table_ptr + sizeof(LevelTable) + buc_idx2 * sizeof(Bucket);
        auto read_buc1 = conn->read(buc_ptr1,seg_rmr.rkey,buc1,sizeof(Bucket),lmr->lkey);
        auto read_buc2 = wo_wait_conn->read(buc_ptr2,seg_rmr.rkey,buc2,sizeof(Bucket),lmr->lkey);
        
        // 2.3 Find Duplicate Key
        Bucket* buc;
        uintptr_t buc_ptr;
        for(uint64_t i = 0 ; i < 2 ; i++){
            if(i==0){
                co_await std::move(read_buc1);
            }else{
                co_await std::move(read_buc2);
            }

            buc = (i==0)? buc1:buc2;
            buc_ptr = (i==0)? buc_ptr1:buc_ptr2;
            for(uint64_t entry_id = 0 ; entry_id < BUCKET_SIZE ; entry_id++){
                uint64_t buc_id = (i==0)? buc_idx1:buc_idx2;

                if(buc->entrys[entry_id] == 0)
                {
                    free_slot_ptr = buc_ptr + sizeof(Entry) * entry_id;
                    free_level_id = level_id;
                    free_buc_id = (i==0)? buc_idx1:buc_idx2;
                    free_entry_id = entry_id;
                    continue;
                }
                if(buc->entrys[entry_id].fp == tmp_fp)
                {
                    co_await conn->read(ralloc.ptr(buc->entrys[entry_id].offset), seg_rmr.rkey, tmp_block,(buc->entrys[entry_id].len) * ALIGNED_SIZE, lmr->lkey);
                    if (memcmp(key->data, tmp_block->data, key->len) == 0)
                    {
                        // duplicate key : delete
                        // log_err("[%lu:%lu:%lu]duplicate",this->cli_id,this->coro_id,this->key_num);
                        uintptr_t slot_ptr = buc_ptr + sizeof(Entry) * entry_id;
                        co_await conn->cas_n(slot_ptr, seg_rmr.rkey,buc->entrys[entry_id], 0);
                    }
                }
            }
        }
        first_level_ptr = cur_table_ptr;
        cur_table_ptr = cur_table->up;
        level_id++;
    }

    // 3. Write KV 
    // log_err("[%lu:%lu:%lu]",this->cli_id,this->coro_id,this->key_num);
    if(free_slot_ptr != -1){
        // 3.1 check rehash
        if(dir->is_resizing == 1 && free_level_id == 0){
            if(retry_cnt >= (10000-5)){
                log_err("[%lu:%lu:%lu]too much retry",this->cli_id,this->coro_id,this->key_num);
                // exit(-1);
            }
            goto Retry;
        }
        // 3.2 cas entry
        Entry *tmp = (Entry *)alloc.alloc(sizeof(Entry));
        tmp->fp = tmp_fp;
        tmp->len = (kvblock_len + ALIGNED_SIZE - 1) / ALIGNED_SIZE;
        tmp->offset = ralloc.offset(kvblock_ptr);
        if(!co_await conn->cas_n(free_slot_ptr, seg_rmr.rkey,0,*tmp)){
            // log_err("[%lu:%lu:%lu] cas entry fail at slot_ptr:%lx",this->cli_id,this->coro_id,this->key_num,free_slot_ptr);
            if(retry_cnt >= (10000-5)){
                log_err("[%lu:%lu:%lu]too much retry",this->cli_id,this->coro_id,this->key_num);
                // exit(-1);
            }
            goto Retry;
        }
        // log_err("[%lu:%lu:%lu]write at level:%lu buc_id:%lu entry:%lu",this->cli_id,this->coro_id,this->key_num,free_level_id,free_buc_id,free_entry_id);
        kv_cache.update_cache(tmp->offset,key,value);

        // 3.3 recheck global context
        co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);
        if(dir->is_resizing == 1 && free_level_id == 0){
            if(retry_cnt >= (10000-5)){
                log_err("[%lu:%lu:%lu]too much retry",this->cli_id,this->coro_id,this->key_num);
                // exit(-1);
            }
            goto Retry;
        }
        perf.push_insert();
        sum_cost.end_insert();
        sum_cost.push_level_cnt(level_cnt);
        co_return;
    }

    // 4. Recheck Global Context
    sum_cost.start_split();
    Directory* tmp_dir = (Directory*)alloc.alloc(sizeof(Directory));
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, tmp_dir, sizeof(Directory), lmr->lkey);
    // tmp_dir->print();
    if(dir->first_level != tmp_dir->first_level){
        if(retry_cnt >= (10000-5)){
                log_err("[%lu:%lu:%lu]too much retry",this->cli_id,this->coro_id,this->key_num);
                // exit(-1);
            }
        sum_cost.end_split();
        goto Retry;
    }

    // 5. Resize
    // 5.1 GetLock
    if(!co_await conn->cas_n(lock_rmr.raddr, lock_rmr.rkey, 0, 1)){
        if(retry_cnt >= (10000-5)){
            log_err("[%lu:%lu:%lu]too much retry",this->cli_id,this->coro_id,this->key_num);
            // exit(-1); 
        }
        sum_cost.end_split();
        goto Retry;
    }
    log_err("[%lu:%lu:%lu]expand",this->cli_id,this->coro_id,this->key_num);
    // 5.1 alloc new level
    uintptr_t new_level_ptr = dir->first_level + sizeof(LevelTable) + cur_table->capacity * sizeof(Bucket);
    cur_table->capacity = cur_table->capacity * 2;
    cur_table->up = 0 ;
    co_await conn->write(new_level_ptr,seg_rmr.rkey,cur_table,sizeof(LevelTable),lmr->lkey);
    
    // 5.2 clear new level table
    Bucket* zero_table = (Bucket*)alloc.alloc(sizeof(Bucket)*zero_size);
    memset(zero_table,0,sizeof(Bucket)*zero_size);
    uint64_t upper = cur_table->capacity / zero_size ;
    for(uint64_t i = 0 ; i < upper ; i++){
        // log_err("[%lu:%lu:%lu]i:%lu upper:%lu ptr:%lx cur_table->capacity:%lu zero_size:%lu",this->cli_id,this->coro_id,this->key_num,i,upper,new_level_ptr+sizeof(LevelTable)+i*zero_size*sizeof(Bucket),cur_table->capacity,zero_size);
        co_await conn->write(new_level_ptr+sizeof(LevelTable)+i*zero_size*sizeof(Bucket),seg_rmr.rkey,zero_table,sizeof(Bucket)*zero_size,lmr->lkey);
    }

    log_err("[%lu:%lu:%lu]",this->cli_id,this->coro_id,this->key_num);
    // 5.2 list new level to first level
    cur_table->up = new_level_ptr;
    co_await conn->write(first_level_ptr,seg_rmr.rkey,&cur_table->up,sizeof(uint64_t),lmr->lkey);
    
    // 5.3 Expand Global Context
    // log_err("[%lu:%lu:%lu]dir->first_level:%lx new_level_ptr:%lx cur_table_ptr:%lx cas_ptr:%lx",this->cli_id,this->coro_id,this->key_num,dir->first_level,new_level_ptr,cur_table_ptr,seg_rmr.raddr + sizeof(uint64_t));
    if(!co_await conn->cas_n(seg_rmr.raddr + sizeof(uint64_t), seg_rmr.rkey,dir->first_level,new_level_ptr)){
        log_err("[%lu:%lu:%lu] fail to update first level",this->cli_id,this->coro_id,this->key_num);
        sum_cost.end_split();
        goto Retry;
    }
    dir->is_resizing = 1;
    co_await conn->write(seg_rmr.raddr,seg_rmr.rkey,dir,sizeof(uint64_t),lmr->lkey);
    log_err("[%lu:%lu:%lu]",this->cli_id,this->coro_id,this->key_num);
    
    // 5.4 Unlock 
    co_await conn->cas_n(lock_rmr.raddr, lock_rmr.rkey, 1, 0);
    log_err("[%lu:%lu:%lu]",this->cli_id,this->coro_id,this->key_num);

    // 6. ReHash
    // Complete By Rehash Thread

    // 7. ReInsert
    if(retry_cnt >= (10000-5)){
        log_err("[%lu:%lu:%lu]too much retry",this->cli_id,this->coro_id,this->key_num);
        // exit(-1);
    }
    sum_cost.end_split();
    goto Retry;
}

task<bool> Client::check_exit(){
    co_await conn->read(seg_rmr.raddr,seg_rmr.rkey,dir,sizeof(Directory),lmr->lkey);
    co_return dir->is_resizing;
}

task<> Client::rehash(std::atomic_bool& extern_flag){
    LevelTable * last_table = (LevelTable*)alloc.alloc(sizeof(LevelTable));
    LevelTable * first_table = (LevelTable*)alloc.alloc(sizeof(LevelTable));
    Bucket* buc1 = (Bucket*)alloc.alloc(sizeof(Bucket));
    Bucket* buc2 = (Bucket*)alloc.alloc(sizeof(Bucket));
    KVBlock *kv_block = (KVBlock *)alloc.alloc(17 * ALIGNED_SIZE);
    Bucket* buc = (Bucket*)alloc.alloc(zero_size*sizeof(Bucket));
    Bucket* zero_table = (Bucket*)alloc.alloc(zero_size*sizeof(Bucket));
    memset(zero_table,0,sizeof(Bucket)*zero_size);
    log_err("[%lu:%lu:%lu]rehash",this->cli_id,this->coro_id,this->key_num);
    bool resize_flag = false;
    bool exit_flag = false;
    uint64_t exit_cnt = 0; // 仅用于run有insert时
    sum_cost.start_merge();
    while(extern_flag.load()){
        co_await conn->read(seg_rmr.raddr,seg_rmr.rkey,dir,sizeof(Directory),lmr->lkey);
        // log_err("[%lu:%lu:%lu]read dir",this->cli_id,this->coro_id,this->key_num);
        if(dir->is_resizing){
            if(exit_flag) exit_cnt++;
            log_err("[%lu:%lu:%lu]Detect resize with dir->first_level:%lx dir->last_level:%lx",this->cli_id,this->coro_id,this->key_num,dir->first_level,dir->last_level);
            // 1. read last level header
            co_await conn->read(dir->last_level,seg_rmr.rkey,last_table,sizeof(LevelTable),lmr->lkey);

            // 2. read last level buckets
            uint64_t upper = last_table->capacity / zero_size ;
            uint64_t buc_id = 0; //记录update的位置
            uintptr_t buc_ptr = dir->last_level + sizeof(LevelTable);
            // log_err("Upper :%lu last_table->capacity:%lu up:%lx zero_size:%lu buc_ptr:%lx",upper,last_table->capacity,last_table->up,zero_size,buc_ptr);
            for(uint64_t i = 0 ; i < upper ; i++){
                co_await conn->read(buc_ptr,seg_rmr.rkey,buc,sizeof(Bucket)*zero_size,lmr->lkey);
                for(uint64_t buc_id = 0 ; buc_id < zero_size ; buc_id++){
                    for(uint64_t entry_id = 0 ; entry_id < BUCKET_SIZE ; entry_id++){
                        // 2.1 Read KvBlock
                        // log_err("Move buc:%lu entry:%lu",buc_id,entry_id);
                        // buc[buc_id].entrys[entry_id].print("Move");
                        if(buc[buc_id].entrys[entry_id].offset == 0) continue;
                        if(buc[buc_id].entrys[entry_id].len > 1){
                            log_err("Incorrect len in buc_id:%lu entry_id:%lu",buc_id+i*zero_size,entry_id);
                            buc[buc_id].entrys[entry_id].print();
                            exit(-1);
                        }
                        co_await conn->read(ralloc.ptr(buc[buc_id].entrys[entry_id].offset), seg_rmr.rkey, kv_block,(buc[buc_id].entrys[entry_id].len) * ALIGNED_SIZE, lmr->lkey);
                        // kv_block->print("Move");
                        auto pattern = hash(kv_block->data, kv_block->k_len);
                        uint64_t pattern_1 = (uint64_t)pattern;
                        uint64_t pattern_2 = (uint64_t)(pattern >> 64);
Retry:
                        // 2.2 Read First level header
                        co_await conn->read(seg_rmr.raddr,seg_rmr.rkey,dir,sizeof(Directory),lmr->lkey);
                        co_await conn->read(dir->first_level,seg_rmr.rkey,first_table,sizeof(LevelTable),lmr->lkey);
                        // log_err("Fisrt_level:%lx first_table->capacity:%lu",dir->first_level,first_table->capacity);
                        // 2.3 read 2 bucket in first level && insert
                        uint64_t buc_idx1 = pattern_1 % first_table->capacity;
                        uint64_t buc_idx2 = pattern_2 % first_table->capacity;
                        uintptr_t buc_ptr1 = dir->first_level + sizeof(LevelTable) + buc_idx1 * sizeof(Bucket);
                        uintptr_t buc_ptr2 = dir->first_level + sizeof(LevelTable) + buc_idx2 * sizeof(Bucket);
                        auto read_buc1 = conn->read(buc_ptr1,seg_rmr.rkey,buc1,sizeof(Bucket),lmr->lkey);
                        auto read_buc2 = wo_wait_conn->read(buc_ptr2,seg_rmr.rkey,buc2,sizeof(Bucket),lmr->lkey);
                        uintptr_t free_slot_ptr = -1;
                        uintptr_t free_buc_id = -1;
                        uintptr_t free_entry_id = -1;
                        co_await std::move(read_buc1);
                        co_await std::move(read_buc2);
                        for(uint64_t i = 0 ; i < 2 ; i++){
                            Bucket* choice_buc = (i==0)? buc1:buc2;
                            uintptr_t  choice_buc_ptr = (i==0)? buc_ptr1:buc_ptr2;
                            for(uint64_t entry_id = 0 ; entry_id < BUCKET_SIZE ; entry_id++){
                                if(choice_buc->entrys[entry_id].offset == 0){
                                    free_buc_id = (i==0)? buc_idx1:buc_idx2;
                                    free_entry_id = entry_id;
                                    free_slot_ptr = choice_buc_ptr + sizeof(Entry) * entry_id;
                                    break;
                                }
                            }
                            if(free_slot_ptr!=-1) break;
                        }

                        if(free_slot_ptr != -1){
                            if(!co_await conn->cas_n(free_slot_ptr, seg_rmr.rkey,0,buc[buc_id].entrys[entry_id])){
                                log_err("Move buc:%lu entry:%lu cas fail retry",i*zero_size + buc_id,entry_id);
                                goto Retry;
                            }else{
                                // log_err("Move buc:%lu entry:%lu to free_buc_id:%lu free_entry_id:%lu free_slot_ptr:%lx",i*zero_size+buc_id,entry_id,free_buc_id,free_entry_id,free_slot_ptr);
                                continue;
                            }
                        }
                        
                        // 2.4 no free slot in first level
                        // a. lock dir
                        if(!co_await conn->cas_n(lock_rmr.raddr, lock_rmr.rkey, 0, 1)){
                            goto Retry;
                        }
                        // b. expand 
                        uintptr_t new_level_ptr = dir->first_level + sizeof(LevelTable) + first_table->capacity * sizeof(Bucket);
                        uint64_t upper = (first_table->capacity * 2) / zero_size ;
                        for(uint64_t i = 0 ; i < upper ; i++){
                            co_await conn->write(new_level_ptr+sizeof(LevelTable)+i*zero_size*sizeof(Bucket),seg_rmr.rkey,zero_table,sizeof(Bucket)*zero_size,lmr->lkey);
                        }
                        first_table->up = 0;
                        first_table->capacity = first_table->capacity * 2;
                        co_await conn->write(new_level_ptr,seg_rmr.rkey,first_table,sizeof(LevelTable),lmr->lkey);

                        first_table->up = new_level_ptr ;
                        co_await conn->write(dir->first_level,seg_rmr.rkey,&(first_table->up),sizeof(uint64_t),lmr->lkey);

                        co_await conn->cas_n(seg_rmr.raddr + sizeof(uint64_t), seg_rmr.rkey,dir->first_level,new_level_ptr);
                        
                        // c. unlock
                        co_await conn->cas_n(lock_rmr.raddr, lock_rmr.rkey, 1, 0);
                        
                        // d. retry insert
                        log_err("Move buc:%lu entry:%lu expand retry",buc_id,entry_id);
                        goto Retry;

                    }
                }
                buc_ptr += zero_size * sizeof(Bucket);
            }

            // 3. write is_resizing to false
            log_err("End resize for last_table:%lx last_table",dir->last_level);
            resize_flag = true;
            dir->print();
            co_await conn->write(seg_rmr.raddr+2*sizeof(uint64_t),seg_rmr.rkey,&(last_table->up),sizeof(uint64_t),lmr->lkey);
            if((first_table->capacity / last_table->capacity) == 4){
                dir->is_resizing = 0;
                co_await conn->write(seg_rmr.raddr,seg_rmr.rkey,&(dir->is_resizing),sizeof(uint64_t),lmr->lkey);
            }else{
                log_err("Continue resize for next last_table:%lx",last_table->up);
            }
        }else{
            // 4. Check Exit
            co_await conn->read(seg_rmr.raddr,seg_rmr.rkey,dir,sizeof(Directory),lmr->lkey);
            if(dir->start_cnt==0 && resize_flag ){
                if(exit_cnt){
                    log_err("[%lu:%lu:%lu] rehash exit",this->cli_id,this->coro_id,this->key_num);
                    sum_cost.end_merge();
                    sum_cost.print(0,0);
                    co_await conn->read(dir->first_level,seg_rmr.rkey,first_table,sizeof(LevelTable),lmr->lkey);
                    log_err("Final Level :%lu",first_table->capacity/INIT_TABLE_SIZE);
                    co_return;
                }else{
                    exit_flag = true;
                }
            }
        }
    }
    sum_cost.end_merge();
    sum_cost.print(0,0);
    co_await conn->read(dir->first_level,seg_rmr.rkey,first_table,sizeof(LevelTable),lmr->lkey);
    log_err("Final Level :%lu",first_table->capacity/INIT_TABLE_SIZE);
}

task<bool> Client::search(Slice *key, Slice *value)
{
    perf.start_search();
    auto pattern = hash(key->data, key->len);
    uint64_t pattern_1 = (uint64_t)pattern;
    uint64_t pattern_2 = (uint64_t)(pattern >> 64);
    uint64_t tmp_fp = fp(pattern_1);
    this->key_num = *(uint64_t *)key->data;
    uint64_t level_id = 0;
Retry:
    alloc.ReSet(sizeof(Directory));
    KVBlock *res = nullptr;
    KVBlock *kv_block = (KVBlock *)alloc.alloc(17 * ALIGNED_SIZE);

    // 1. Read Header
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);

    // 2. Find Key
    // 由最底层开始逐层向上查找；为了解决假阴性问题，如果出现阴性结果，那么要检查全局 context 是否发生变化，如果变化了就再查一次。
    LevelTable* cur_table = (LevelTable*) alloc.alloc(sizeof(LevelTable));
    uintptr_t cur_table_ptr = dir->last_level;
    Bucket * buc1 = (Bucket*)alloc.alloc(sizeof(Bucket));
    Bucket * buc2 = (Bucket*)alloc.alloc(sizeof(Bucket));
    while(cur_table_ptr != 0){
        level_id++;
        // 2.1 Read LevelTable Header : capacity,up
        co_await conn->read(cur_table_ptr,seg_rmr.rkey,cur_table,sizeof(LevelTable),lmr->lkey);
        uint64_t buc_idx1,buc_idx2;
        buc_idx1 = pattern_1 % cur_table->capacity;
        buc_idx2 = pattern_2 % cur_table->capacity;
        
        // 2.2 Read 2 Bucket
        uintptr_t buc_ptr1 = cur_table_ptr + sizeof(LevelTable) + buc_idx1 * sizeof(Bucket);
        uintptr_t buc_ptr2 = cur_table_ptr + sizeof(LevelTable) + buc_idx2 * sizeof(Bucket);
        auto read_buc1 = conn->read(buc_ptr1,seg_rmr.rkey,buc1,sizeof(Bucket),lmr->lkey);
        auto read_buc2 = wo_wait_conn->read(buc_ptr2,seg_rmr.rkey,buc2,sizeof(Bucket),lmr->lkey);
        
        // 2.3 Find Duplicate Key
        Bucket* buc;
        uintptr_t buc_ptr;
        for(uint64_t i = 0 ; i < 2 ; i++){
            if(i==0){
                co_await std::move(read_buc1);
            }else{
                co_await std::move(read_buc2);
            }
            buc = (i==0)? buc1:buc2;
            for(uint64_t entry_id = 0 ; entry_id < BUCKET_SIZE ; entry_id++){
                if(buc->entrys[entry_id].fp == tmp_fp){
                    KVBlock* cache_block = kv_cache.get_cache(buc->entrys[entry_id].offset);
                    if(cache_block != nullptr){
                        if (memcmp(key->data, cache_block->data, key->len) == 0)
                        {
                            // log_err("cache hit");
                            cache_hit_cnt++;
                            res = cache_block;
                        }   
                    }else{
                        co_await conn->read(ralloc.ptr(buc->entrys[entry_id].offset), seg_rmr.rkey, kv_block,(buc->entrys[entry_id].len) * ALIGNED_SIZE, lmr->lkey);
                        if (memcmp(key->data, kv_block->data, key->len) == 0)
                        {
                            res = kv_block;
                        }   
                    }
                }
            }
        }
        cur_table_ptr = cur_table->up;
    }

    // 3. recheck global context
    Directory * tmp_dir = (Directory*)alloc.alloc(sizeof(Directory));
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, tmp_dir, sizeof(Directory), lmr->lkey);
    if(tmp_dir->first_level != dir->first_level || tmp_dir->last_level != dir->last_level){
        goto Retry;
    }

    if (res != nullptr && res->v_len != 0)
    {
        value->len = res->v_len;
        memcpy(value->data, res->data + res->k_len, value->len);
        perf.push_search();
        sum_cost.push_retry_cnt(level_id);
        co_return true;
    }

    log_err("[%lu:%lu:%lu]No mathc key",this->cli_id,this->coro_id,this->key_num);
    // exit(-1);
    perf.push_search();
    sum_cost.push_retry_cnt(level_id);
    co_return false;
}

task<> Client::update(Slice *key, Slice *value)
{
    co_await this->insert(key,value);
    co_return;
}
task<> Client::remove(Slice *key)
{
    co_return;
}

} // namespace SPLIT_OP