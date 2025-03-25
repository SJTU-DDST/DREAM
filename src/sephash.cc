#include "sephash.h"
namespace SEPHASH
{

// inline __attribute__((always_inline)) uint64_t fp(uint64_t pattern)
// {
//     return ((uint64_t)((pattern) >> 32) & ((1 << 8) - 1));
// }

// inline __attribute__((always_inline)) uint64_t fp2(uint64_t pattern)
// {
//     return ((uint64_t)((pattern) >> 24) & ((1 << 8) - 1));
// }

// inline __attribute__((always_inline)) uint64_t get_seg_loc(uint64_t pattern, uint64_t global_depth)
// {
//     return ((pattern) & ((1 << global_depth) - 1));
// }

void print_mainseg(Slot *main_seg, uint64_t main_seg_len);

// #if LARGER_FP_FILTER_GRANULARITY
// inline __attribute__((always_inline)) uint64_t get_fp_bit(uint8_t fp1, uint8_t fp2)
// {
//     uint64_t fp = fp1;
//     fp = fp << 8;
//     fp = fp | fp2;
//     // fp = fp & ((1 << 4) - 1); // 现在先用原来的16位filter，后面再改
//     fp %= FP_BITMAP_LENGTH;
//     return fp;
// }
// #else
// inline __attribute__((always_inline)) std::tuple<uint64_t, uint64_t> get_fp_bit(uint8_t fp1, uint8_t fp2)
// {
//     uint64_t fp = fp1;
//     fp = fp << 8;
//     fp = fp | fp2;
//     fp = fp & ((1 << 10) - 1);
//     uint64_t bit_loc = fp / 64;
//     uint64_t bit_info = (fp % 64);
//     bit_info = 1ll << bit_info;
//     return std::make_tuple(bit_loc, bit_info);
// }
// #endif

void print_bit_map(uint64_t* fp_bitmap){
    for(int i = 0 ; i < 16 ; i++){
        log_err("%16lx",fp_bitmap[i]);
    }
}

Server::Server(Config &config) : dev(nullptr, 1, config.gid_idx), ser(dev)
{
    seg_mr = dev.reg_mr(233, config.mem_size);
    dev.seg_mr = seg_mr;
    auto [dm, mr] = dev.reg_dmmr(234, dev_mem_size);
    lock_dm = dm;
    lock_mr = mr;

    alloc.Set((char *)seg_mr->addr, seg_mr->length);
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    Init();
    log_err("init");

    // Init locks
    char tmp[dev_mem_size] = {}; // init locks to zero
    lock_dm->memcpy_to_dm(lock_dm, 0, tmp, dev_mem_size);
    log_err("memset, any key to exit");

    if (config.auto_run_client)
    {
        // log_err("auto run client");
        // config.print();

        ser.start_serve(nullptr, 1, zero_qp_cap, rdma_default_tempmp_size, rdma_default_max_coros, rdma_default_cq_size, rdma_default_port, dir);

        // log_err("start clients with run.py");
        // int result = system("python3 ../run.py 1 client 8 1");
        std::string command = std::format("python3 ../run.py {} client {} {}", config.num_machine, config.num_cli, config.num_coro);
        log_err("Auto run client command: %s", command.c_str());
        int result = system(command.c_str());
        log_err("run.py completed with result: %d", result);
    }
    else
    {
        auto wait_exit = [&]()
        {
            std::cin.get(); // 等待用户输入
            ser.stop_serve();
            log_err("Exiting...");
        };
        std::thread th(wait_exit);

        // ser.start_serve();
        ser.start_serve(nullptr, 1, zero_qp_cap, rdma_default_tempmp_size, rdma_default_max_coros, rdma_default_cq_size, rdma_default_port, dir);
        th.join();
    }
}

void Server::Init()
{
    // Set MainTable to zero
    dir->global_depth = INIT_DEPTH;

    // Init CurTable for initial segments
    CurSeg *cur_seg;
    for (uint64_t i = 0; i < (1 << dir->global_depth); i++)
    {
        dir->segs[i].cur_seg_ptr = (uintptr_t)alloc.alloc(sizeof(CurSeg));
        dir->segs[i].local_depth = INIT_DEPTH;
        cur_seg = (CurSeg *)dir->segs[i].cur_seg_ptr;
        memset(cur_seg, 0, sizeof(CurSeg));
        cur_seg->seg_meta.local_depth = INIT_DEPTH;
        cur_seg->seg_meta.sign = 1;
    }
    
#if HASH_TYPE == MYHASH
    // Initialize remaining entries in the directory (up to 65536)
    // Each entry at index i+(1<<INIT_DEPTH) is initialized with values from segs[i%(1<<INIT_DEPTH)]
    const uint64_t init_entries = (1 << INIT_DEPTH);

    for (uint64_t i = init_entries; i < DIR_SIZE; i++)
    {
        uint64_t base_idx = i % init_entries;
        dir->segs[i].cur_seg_ptr = dir->segs[base_idx].cur_seg_ptr;
        dir->segs[i].local_depth = dir->segs[base_idx].local_depth;
        dir->segs[i].main_seg_ptr = dir->segs[base_idx].main_seg_ptr;
        dir->segs[i].main_seg_len = dir->segs[base_idx].main_seg_len;
        // Copy any other fields that need to be initialized
    }
#endif
}

Server::~Server()
{
    rdma_free_mr(seg_mr);
    rdma_free_dmmr({lock_dm, lock_mr});
}

Client::Client(Config &config, ibv_mr *_lmr, rdma_client *_cli, rdma_conn *_conn, rdma_conn *_wowait_conn,
                uint64_t _machine_id, uint64_t _cli_id, uint64_t _coro_id): config(config)
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

    perf.init();
    sum_cost.init();

    // alloc info
    alloc.Set((char *)lmr->addr, lmr->length);
    seg_rmr = cli->run(conn->query_remote_mr(233));
    uint64_t rbuf_size = (seg_rmr.rlen - (1ul << 20) * 200) /
                            (config.num_machine * config.num_cli * config.num_coro); // 头部保留5GB，其他的留给client
    uint64_t buf_id = config.machine_id * config.num_cli * config.num_coro + cli_id * config.num_coro + coro_id;
    uintptr_t remote_ptr = seg_rmr.raddr + seg_rmr.rlen - rbuf_size * buf_id; // 从尾部开始分配
    ralloc.SetRemote(remote_ptr, rbuf_size, seg_rmr.raddr, seg_rmr.rlen);
    ralloc.alloc(ALIGNED_SIZE); // 提前分配ALIGNED_SIZE，免得读取的时候越界
    // log_err("ralloc start_addr:%lx offset_max:%lx", ralloc.raddr, ralloc.rsize);

    // util variable
    op_cnt = 0;
    miss_cnt = 0;

    // sync dir
    dir = (Directory *)alloc.alloc(sizeof(Directory));
    memset(dir, 0, sizeof(Directory));
    memset(offset, 0, sizeof(SlotOffset) * DIR_SIZE);
    for(uint64_t i = 0 ; i < (1<<INIT_DEPTH) ; i++) this->offset[i].sign = 1;
    cli->run(sync_dir());
}

Client::~Client()
{
    // log_err("[%lu:%lu] miss_cnt:%lu", cli_id, coro_id, miss_cnt);
    perf.print(cli_id,coro_id);
    sum_cost.print(cli_id,coro_id);
}

task<> Client::cal_utilization(){
    if(this->machine_id !=0 || this->cli_id != 0 || this->coro_id != 0) co_return;
    co_await sync_dir();
    uint64_t space_consumption = sizeof(uint64_t)+(1<<dir->global_depth)*sizeof(DirEntry);
    uint64_t segment_cnt = 0 ;
    uint64_t entry_total = 0 ;
    uint64_t entry_cnt = 0 ;
    uint64_t buc_meta_consumption = 0 ;
    uint64_t dir_entry_consumption = sizeof(uint64_t)+(1<<dir->global_depth)*sizeof(DirEntry);
    uint64_t total_meta_consumption = dir_entry_consumption ;

    // 遍历Segment，统计空间开销和空间利用率
    log_err("global_dep:%lu",dir->global_depth);
    log_err("sizeof(CurSeg):%lu sizeof(Slot):%lu",sizeof(CurSeg),sizeof(Slot));
    CurSeg * cur_seg = (CurSeg*)alloc.alloc(sizeof(CurSeg));
    for(uint64_t i = 0 ; i < (1<<dir->global_depth) ; i++){
        uint64_t first_index = i & ((1<<dir->segs[i].local_depth)-1);
        first_index |= 1<<dir->segs[i].local_depth ;
        if(dir->segs[i].local_depth == dir->global_depth || i == first_index ){
            // space_consumption += sizeof(CurSeg);
            // buc_meta_consumption += sizeof(CurSegMeta);
            space_consumption += SEGMENT_SIZE+16*8+24;
            buc_meta_consumption += 16*8+24;
            entry_total += SLOT_PER_SEG;
            segment_cnt++;
            
            // add main segment
            co_await conn->read(dir->segs[i].cur_seg_ptr,seg_rmr.rkey,cur_seg,sizeof(CurSeg),lmr->lkey);
            // space_consumption += cur_seg->seg_meta.main_seg_len * sizeof(Slot);
            space_consumption += cur_seg->seg_meta.main_seg_len * 9;
            entry_total += cur_seg->seg_meta.main_seg_len;
            entry_cnt += cur_seg->seg_meta.main_seg_len;

            // cal cur segment
            for(uint64_t i = 0 ; i < SLOT_PER_SEG ; i++){
                if(cur_seg->slots[i].sign != cur_seg->seg_meta.sign ){
                    entry_cnt++;
                }
            }
        }
    }
    // double space_utilization = (1.0*entry_cnt*sizeof(Slot))/(1.0*space_consumption);
    double space_utilization = (1.0*entry_cnt*9)/(1.0*space_consumption);
    double entry_utilization = (1.0*entry_cnt)/(1.0*entry_total);

    total_meta_consumption += buc_meta_consumption;
    // space_consumption = space_consumption>>20;

    // log_err("space_consumption:%luMB segment_cnt:%lu entry_total:%lu entry_cnt:%lu entry_utilization:%lf space_utilization:%lf",space_consumption,segment_cnt,entry_total,entry_cnt,entry_utilization,space_utilization);
    log_err("space_consumption:%lu buc_meta_consumption:%lu dir_entry_consumption:%lu total_meta_consumption:%lu segment_cnt:%lu entry_total:%lu entry_cnt:%lu entry_utilization:%lf space_utilization:%lf",space_consumption,buc_meta_consumption, dir_entry_consumption, total_meta_consumption,segment_cnt,entry_total,entry_cnt,entry_utilization,space_utilization);
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

    // 重置远端segment
    dir->global_depth = INIT_DEPTH;

    CurSeg *cur_seg = (CurSeg *)alloc.alloc(sizeof(CurSeg));
    memset(cur_seg, 0, sizeof(CurSeg));
    cur_seg->seg_meta.local_depth = INIT_DEPTH;
    cur_seg->seg_meta.sign = 1;
    for (uint64_t i = 0; i < (1 << dir->global_depth); i++)
    {
        dir->segs[i].cur_seg_ptr = (uintptr_t)server_alloc.alloc(sizeof(CurSeg));
        dir->segs[i].local_depth = INIT_DEPTH;
        co_await conn->write(dir->segs[i].cur_seg_ptr, seg_rmr.rkey, cur_seg, size_t(sizeof(CurSeg)), lmr->lkey);
    }

    // 重置远端 Directory
    co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, dir, sizeof(Directory), lmr->lkey);
}

// 定义一个全局的随机数生成器
std::random_device rd;
std::mt19937 gen(rd());
std::uniform_int_distribution<> dis(5, 20);

task<> Client::start(uint64_t total)
{
    co_await sync_dir();
    uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t), true);
    *start_cnt = 0;
    // std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen)));
    co_await conn->fetch_add(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, *start_cnt, 1);
    // log_err("准备开始 Start_cnt:%lu", *start_cnt);
    while ((*start_cnt) < total)
    {
        // std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen))); // IMPORTANT: 通过FAA让所有客户端一起开始，为了避免CPU占用过高，这里加了一个sleep
        // log_err("[%lu:%lu]开始重试 Start_cnt:%lu", cli_id, coro_id, *start_cnt);
        co_await conn->read(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, start_cnt,
                            sizeof(uint64_t), lmr->lkey);
    }
}

task<> Client::stop()
{
    uint64_t *start_cnt = (uint64_t *)alloc.alloc(sizeof(uint64_t));
    // std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen)));
    co_await conn->fetch_add(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, *start_cnt, -1);
    // log_err("[%lu:%lu]准备停止 Start_cnt:%lu", cli_id, coro_id, *start_cnt);
    while ((*start_cnt) != 0)
    {
        // log_info("准备停止 start_cnt:%lu", *start_cnt);
        co_await conn->read(seg_rmr.raddr + sizeof(Directory) - sizeof(uint64_t), seg_rmr.rkey, start_cnt,
                            sizeof(uint64_t), lmr->lkey);
        // std::this_thread::sleep_for(std::chrono::milliseconds(dis(gen))); // IMPORTANT: 通过FAA让所有客户端一起结束，为了避免CPU占用过高，这里加了一个sleep
    }
}

task<> Client::sync_dir()
{
    // First read the current directory information
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, &dir->global_depth, 2 * sizeof(uint64_t), lmr->lkey);
    
    // Read all directory entries for the current directory size
    uint64_t current_dir_size = (1 << dir->global_depth);
    co_await conn->read(seg_rmr.raddr + sizeof(uint64_t), seg_rmr.rkey, dir->segs,
                        current_dir_size * sizeof(DirEntry), lmr->lkey);
    
    // Initialize the offset array with values from the actual segments
    for(uint64_t i = 0; i < current_dir_size; i++) {
        this->offset[i].main_seg_ptr = dir->segs[i].main_seg_ptr;
    }

#if HASH_TYPE == MYHASH
    // Initialize the remaining entries in the directory (up to 65536)
    // Each entry at index i+current_dir_size is initialized with values from segs[i%current_dir_size]
    for (uint64_t i = current_dir_size; i < DIR_SIZE; i++)
    {
        uint64_t base_idx = i % current_dir_size;
        dir->segs[i].cur_seg_ptr = dir->segs[base_idx].cur_seg_ptr;
        dir->segs[i].local_depth = dir->segs[base_idx].local_depth;
        dir->segs[i].main_seg_ptr = dir->segs[base_idx].main_seg_ptr;
        dir->segs[i].main_seg_len = dir->segs[base_idx].main_seg_len;
    }
#endif
}

/// @brief 读取远端的Global Depth
/// @return
task<uintptr_t> Client::check_gd(uint64_t segloc, bool read_fp, bool recursive)
{
    if (segloc != -1)
    {
        uint64_t old_local_depth = dir->segs[segloc].local_depth;
        uintptr_t dentry_ptr = seg_rmr.raddr + sizeof(uint64_t) + segloc * sizeof(DirEntry);
        uint64_t read_size = (read_fp) ? sizeof(DirEntry) : 4 * sizeof(uint64_t);

        auto read_cur_ptr = wo_wait_conn->read(dentry_ptr, seg_rmr.rkey, &dir->segs[segloc], read_size, lmr->lkey);
        co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, &dir->global_depth, sizeof(uint64_t), lmr->lkey);
        co_await read_cur_ptr;

        if (recursive && dir->segs[segloc].local_depth != old_local_depth)
        {
            // log_err("[%lu:%lu:%lu]check_gd本地segloc:%lu的local_depth:%lu->%lu",cli_id,coro_id,this->key_num,segloc,old_local_depth,dir->segs[segloc].local_depth);

            // 递归读取新增加的位图示的segment
            uint64_t new_local_depth = dir->segs[segloc].local_depth;

            // 逐位递增读取所有可能的新segment
            for (uint64_t depth = old_local_depth + 1; depth <= new_local_depth; depth++)
            {
                // 计算当前depth下，新增的bit位置
                uint64_t bit_pos = depth - 1; // 0-indexed bit position

                // 计算新的segloc，在原始segloc的基础上，设置新的bit
                uint64_t new_segloc = segloc | (1ULL << bit_pos);

                // 只有当new_segloc与当前segloc不同时才递归读取
                if (new_segloc != segloc)
                {
                    // log_err("[%lu:%lu:%lu]递归读取新的segloc:%lu",cli_id,coro_id,this->key_num,new_segloc);
                    assert_require(new_segloc < (1ULL << dir->global_depth));
                    co_await check_gd(new_segloc, read_fp, true);
                }
            }
        }
        co_return dir->segs[segloc].cur_seg_ptr;
    }
    co_await conn->read(seg_rmr.raddr, seg_rmr.rkey, &dir->global_depth, sizeof(uint64_t), lmr->lkey);
    co_return 0;
}

task<> Client::insert(Slice *key, Slice *value)
{
    // log_err("insert key:%lu value:%s", *(uint64_t *)key->data, value->data);
    perf.start_perf();
    sum_cost.start_insert();
    op_cnt++;
    uint64_t op_size = (1 << 20) * 1;
    // 因为存在pure_write,为上一个操作保留的空间，1MB够用了
    if (op_cnt % 2)
        alloc.ReSet(sizeof(Directory) + op_size);
    else
        alloc.ReSet(sizeof(Directory));
    uint64_t pattern = (uint64_t)hash(key->data, key->len);
    KVBlock *kv_block = InitKVBlock(key, value, &alloc);
    uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 2;
    this->kv_block_len = kvblock_len;
    uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
    // a. writekv
    wo_wait_conn->pure_write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey);
    retry_cnt = 0;
    this->key_num = *(uint64_t *)key->data;
    int retry_reason = 0;
Retry:
    alloc.ReSet(sizeof(Directory)+kvblock_len);
#if !MODIFIED
    if(retry_cnt++ == 1000) {
        log_err("[%lu:%lu:%lu]Fail to insert after %lu retries, last retry_reason: %d",cli_id,coro_id,this->key_num,retry_cnt, retry_reason);
        perf.push_insert();
        sum_cost.end_insert();
        sum_cost.push_retry_cnt(retry_cnt);
        co_return;
    }
#endif
    // 1. Cal Segloc according to Global Depth At local
    Slot *tmp = (Slot *)alloc.alloc(sizeof(Slot));
    uint64_t segloc = get_seg_loc(pattern, dir->global_depth);
    uintptr_t segptr = dir->segs[segloc].cur_seg_ptr;
    if(segptr == 0){
        // log_err("[%lu:%lu:%lu]本地segloc:%lu的segptr=0, 客户端第一次访问这个CurSeg？",cli_id,coro_id,this->key_num,segloc);
        segptr = co_await check_gd(segloc); // 现在只会修改main_seg_len和main_seg_ptr
        uint64_t new_seg_loc = get_seg_loc(pattern, dir->global_depth);
        if(new_seg_loc != segloc) {
            retry_reason = 1;
            goto Retry;
        }
    }

    // 2. read segment_meta && segment_slot concurrently
    // a. read meta
    CurSegMeta *remote_seg_meta = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta));
#if CORO_DEBUG
    auto read_meta = conn->read(segptr + sizeof(uint64_t), seg_rmr.rkey, remote_seg_meta, sizeof(CurSegMeta), lmr->lkey, std::source_location::current(), std::format("[{}:{}]segloc:{}读取meta地址{}", cli_id, coro_id, segloc, segptr + sizeof(uint64_t)));
#else
    auto read_meta = conn->read(segptr + sizeof(uint64_t), seg_rmr.rkey, remote_seg_meta, sizeof(CurSegMeta), lmr->lkey);
#endif
    // b. read slots
    Slot *seg_slots = (Slot *)alloc.alloc(sizeof(Slot) * 2 * SLOT_BATCH_SIZE); // 一次只读取16个slot
    uint64_t seg_offset = this->offset[segloc].offset % SLOT_PER_SEG;
    uintptr_t seg_slots_ptr = segptr + sizeof(uint64_t) + sizeof(CurSegMeta) + seg_offset * sizeof(Slot);
    uint64_t slots_len = SLOT_PER_SEG - seg_offset;
    slots_len = (slots_len < (2 * SLOT_BATCH_SIZE)) ? slots_len : SLOT_BATCH_SIZE;
#if CORO_DEBUG
    auto read_slots = wo_wait_conn->read(seg_slots_ptr, seg_rmr.rkey, seg_slots, sizeof(Slot) * slots_len, lmr->lkey, std::source_location::current(), std::format("[{}:{}]segloc:{}读取slots地址{}", cli_id, coro_id, segloc, seg_slots_ptr));
#else
    auto read_slots = wo_wait_conn->read(seg_slots_ptr, seg_rmr.rkey, seg_slots, sizeof(Slot) * slots_len, lmr->lkey);
#endif
    // c. 直接通过main_seg_ptr来判断一致性
    co_await std::move(read_meta);
    if(remote_seg_meta->local_depth > dir->global_depth){
        // log_err("远端segloc:%lx上发生了Global SPlit, 远端local_depth:%lu>本地global depth%lu", segloc, remote_seg_meta->local_depth, dir->global_depth);
        // 远端segloc上发生了Global SPlit
        // log_err("[%lu:%lu:%lu]remote local_depth:%lu at segloc:%lx exceed local global depth%lu segptr:%lx",cli_id,coro_id,this->key_num,seg_meta->local_depth,segloc,dir->global_depth,segptr);
        if(remote_seg_meta->local_depth <= MAX_DEPTH) dir->global_depth = remote_seg_meta->local_depth;
        uint64_t new_seg_loc = get_seg_loc(pattern, dir->global_depth);
        co_await check_gd(new_seg_loc);
        co_await std::move(read_slots); // 保证异步读取完成
        retry_reason = 2;
        goto Retry;
    }
    
    if (remote_seg_meta->main_seg_ptr != this->offset[segloc].main_seg_ptr || remote_seg_meta->sign != this->offset[segloc].sign)
    {
        // 检查一遍Global Depth是否一致
        // TODO:增加segloc参数，读取对应位置的cur_seg_ptr；否则split的信息无法被及时同步
        uintptr_t new_cur_ptr = co_await check_gd(segloc);
        uint64_t new_seg_loc = get_seg_loc(pattern, dir->global_depth);
        if(new_cur_ptr != segptr || segloc != new_seg_loc){
            this->offset[segloc].offset = 0;
            this->offset[segloc].main_seg_ptr = dir->segs[segloc].main_seg_ptr;
            co_await std::move(read_slots);
            retry_reason = 3;
            goto Retry;
        } 
        // 更新所有指向此Segment的DirEntry
        uint64_t new_local_depth = remote_seg_meta->local_depth;
        uint64_t stride = (1llu) << (dir->global_depth - new_local_depth);
        uint64_t cur_seg_loc;
        uint64_t first_seg_loc = segloc & ((1ull << new_local_depth) - 1);
        for (uint64_t i = 0; i < stride; i++)
        {
            cur_seg_loc = (i << new_local_depth) | first_seg_loc;
            dir->segs[segloc].local_depth = remote_seg_meta->local_depth;
            dir->segs[cur_seg_loc].main_seg_ptr = remote_seg_meta->main_seg_ptr;
            dir->segs[cur_seg_loc].main_seg_len = remote_seg_meta->main_seg_len;
            this->offset[cur_seg_loc].sign = remote_seg_meta->sign;
            this->offset[cur_seg_loc].offset = 0;
            this->offset[cur_seg_loc].main_seg_ptr = remote_seg_meta->main_seg_ptr;
        }
        co_await std::move(read_slots);
        // 怎么同步信息；同步哪些信息
        retry_reason = 4;
        goto Retry;
    }
    // 3. find free slot // 其实可能不一致，读到sign后sign被反转，这时会把刚写入的entry当成空slot。SEND没有这个问题。
    uint64_t sign = !remote_seg_meta->sign;
    uint64_t slot_id = -1;
    co_await std::move(read_slots);

    for (uint64_t i = 0; i < slots_len; i++)
    {
        if (seg_slots[i].sign == sign)
        {
            slot_id = i;
            break;
        }
    }
    if (slot_id == -1)
    {
        // log_err("[%lu:%lu:%lu] segloc:%lx edit segoffset:%lu to %lu",cli_id,coro_id,this->key_num,segloc,seg_offset,(seg_offset+slots_len)%SLOT_PER_SEG);
        this->offset[segloc].offset += slots_len;
        this->offset[segloc].offset = this->offset[segloc].offset%SLOT_PER_SEG;
        retry_reason = 5; // FIXME: !!!
        goto Retry;
    }
    else if (slot_id == slots_len - 1)
    {
        // log_err("[%lu:%lu:%lu] segloc:%lx edit segoffset:%lu to %lu",cli_id,coro_id,this->key_num,segloc,seg_offset,(seg_offset+slots_len)%SLOT_PER_SEG);
        this->offset[segloc].offset += slots_len;
        this->offset[segloc].offset = this->offset[segloc].offset%SLOT_PER_SEG;
    }

    // 4. write slot
    // a. Init Slot
    uint64_t dep = remote_seg_meta->local_depth - (remote_seg_meta->local_depth % 4); // 按4对齐
    tmp->dep = pattern >> dep;
    tmp->fp = fp(pattern);
    tmp->len = (kvblock_len + ALIGNED_SIZE - 1) / ALIGNED_SIZE;
    tmp->sign = remote_seg_meta->sign;
    tmp->offset = ralloc.offset(kvblock_ptr);
    tmp->fp_2 = fp2(pattern);

    // b. cas slot
    uintptr_t slot_ptr = seg_slots_ptr + slot_id * sizeof(Slot);
    int remote_slot_index = (slot_ptr - segptr - sizeof(uint64_t) - sizeof(CurSegMeta)) / sizeof(Slot);
    if (!co_await conn->cas_n(slot_ptr, seg_rmr.rkey,(uint64_t)(seg_slots[slot_id]), *tmp))
    {
        // log_err("[%lu:%lu:%lu] cas failed",cli_id,coro_id,this->key_num);
        retry_reason = 6;
        goto Retry;
    }
    
    // 6. write fp2 && bitmap
    // a. write fp2
    wo_wait_conn->pure_write(slot_ptr + sizeof(uint64_t), seg_rmr.rkey, &tmp->fp_2, sizeof(uint8_t), lmr->lkey);
    // check if need split
    if (seg_offset + slot_id == SLOT_PER_SEG - 1)
    {
        // Split
        co_await Split(segloc, segptr, remote_seg_meta);
        perf.push_insert();
        sum_cost.end_insert();
        sum_cost.push_retry_cnt(retry_cnt);
        co_return;
    }

    // b. write fp bitmap
#if LARGER_FP_FILTER_GRANULARITY
        auto bit_loc = get_fp_bit(tmp->fp, tmp->fp_2);
        uintptr_t fp_ptr = segptr + 4 * sizeof(uint64_t) + bit_loc * sizeof(FpBitmapType);
        // 远端的seg_meta->fp_bitmap[bit_loc]写入00000001。这里不用seg_meta，先alloc.alloc申请一个8byte buffer，写入00000001，然后写入远端。
        FpBitmapType *tmp_fp_bitmap = (FpBitmapType *)alloc.alloc(sizeof(FpBitmapType));
        if (seg_meta.find(segloc) == seg_meta.end())
        {
            seg_meta[segloc] = CurSegMeta();
            memset(&seg_meta[segloc], 0, sizeof(CurSegMeta));
        }
        seg_meta[segloc].fp_bitmap[bit_loc] = 1;
        *tmp_fp_bitmap = 1;
        co_await conn->write(fp_ptr, seg_rmr.rkey, tmp_fp_bitmap, sizeof(FpBitmapType), lmr->lkey);
#else
    auto [bit_loc, bit_info] = get_fp_bit(tmp->fp, tmp->fp_2);
    uintptr_t fp_ptr = segptr + 4 * sizeof(uint64_t) + bit_loc * sizeof(FpBitmapType);
    remote_seg_meta->fp_bitmap[bit_loc] = remote_seg_meta->fp_bitmap[bit_loc] | bit_info;

    // 目前segptr只会从0->7dbfaa480010变一次，所以可以记住。但需要seg_meta->fp_bitmap[bit_loc]的值，我们能不能只写1bit？或者浪费点内存给每种fp分配uint64。
    // static uintptr_t last_segptr = 0;
    // if (last_segptr != segptr)
    // {
    //     log_err("segptr changed: %lx->%lx", last_segptr, segptr);
    //     log_err("segptr:%lx fp_ptr:%lx bit_loc:%d bit_info:%lx", segptr, fp_ptr, bit_loc, bit_info);
    //     last_segptr = segptr;
    // }

    // Ensure strong consistency by waiting for the filter write to complete before considering
    // the write operation as finished, avoiding the scenario where a read operation cannot find
    // the just-completed write.
    co_await conn->write(fp_ptr, seg_rmr.rkey, &remote_seg_meta->fp_bitmap[bit_loc], sizeof(uint64_t), lmr->lkey);
#endif
    perf.push_insert();
    sum_cost.end_insert();
    sum_cost.push_retry_cnt(retry_cnt);
}

#if READ_FULL_KEY_ON_FP_COLLISION // correct but not efficient
task<uint64_t> Client::merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg)
{
    // 定义排序用的结构体
    struct SortItem
    {
        uint8_t fp;
        uint8_t fp_2;
        uint64_t key_hash; // 只在需要时填充
        bool from_data;    // true:来自data, false:来自old_seg
        uint64_t index;    // 在原始数组中的索引
        bool need_read;    // 是否需要读取完整key
        bool is_delete;    // 是否是删除条目(v_len=0)
        Slot *slot_ptr;    // 指向原始Slot的指针

        // 排序规则
        bool operator<(const SortItem &other) const
        {
            if (fp != other.fp)
                return fp < other.fp;
            if (fp_2 != other.fp_2)
                return fp_2 < other.fp_2;
            if (need_read)
            {
                // 如果需要读取key，则比较key_hash
                if (key_hash != other.key_hash)
                    return key_hash < other.key_hash;
                // 相同key时，优先保留较新的(data优先，索引大的优先)
                if (from_data != other.from_data)
                    return from_data;
                return index > other.index; // 对于相同数组，保留索引更大的(较新的)
            }
            // 如果不需要读取key，则按来源和索引排序
            if (from_data != other.from_data)
                return from_data;
            return index < other.index;
        }
    };

    // 1. 统计每个(fp,fp_2)组合的出现次数
    std::map<std::pair<uint8_t, uint8_t>, int> fp_counts;

    for (uint64_t i = 0; i < len; i++)
        fp_counts[{data[i].fp, data[i].fp_2}]++;

    for (uint64_t i = 0; i < old_seg_len; i++)
    {
        fp_counts[{old_seg[i].fp, old_seg[i].fp_2}]++;
    }

    // 2. 创建排序项
    std::vector<SortItem> sort_items;
    sort_items.reserve(len + old_seg_len);

    // 添加data中的项
    for (uint64_t i = 0; i < len; i++)
    {
        if (data[i].is_valid()) {
            auto fp_key = std::make_pair(data[i].fp, data[i].fp_2);
            SortItem item{
                .fp = data[i].fp,
                .fp_2 = data[i].fp_2,
                .key_hash = 0,
                .from_data = true,
                .index = i,
                .need_read = fp_counts[fp_key] > 1, // 如果有多个相同(fp,fp_2)则需要读取key
                .is_delete = false,                 // 初始时不知道是否是删除条目
                .slot_ptr = &data[i]};
            sort_items.push_back(item);
        }
    }

    // 添加old_seg中的项
    for (uint64_t i = 0; i < old_seg_len; i++)
    {
        if (old_seg[i].is_valid()) {
            auto fp_key = std::make_pair(old_seg[i].fp, old_seg[i].fp_2);
            SortItem item{
                .fp = old_seg[i].fp,
                .fp_2 = old_seg[i].fp_2,
                .key_hash = 0,
                .from_data = false,
                .index = i,
                .need_read = fp_counts[fp_key] > 1, // 如果有多个相同(fp,fp_2)则需要读取key
                .is_delete = false,                 // 初始时不知道是否是删除条目
                .slot_ptr = &old_seg[i]};
            sort_items.push_back(item);
        }
    }

    // 3. 为需要读取完整key的项读取key并计算hash
    std::unordered_map<uint64_t, KVBlock *> kv_cache; // 避免重复读取相同offset的KV

    for (auto &item : sort_items)
    {
        if (item.need_read)
        {
            Slot *slot = item.slot_ptr;
            uint64_t offset = ralloc.ptr(slot->offset);

            if (kv_cache.find(offset) == kv_cache.end())
            {
                // 未读取过，执行RDMA读取
                KVBlock *kv = (KVBlock *)alloc.alloc(slot->len * ALIGNED_SIZE);
                co_await conn->read(offset, seg_rmr.rkey, kv, slot->len * ALIGNED_SIZE, lmr->lkey);
                // 验证KVBlock合法性
                if (!kv->is_valid())
                {
                    kv->k_len = 8;
                    kv->v_len = 0; // 直接删掉
                }
                kv_cache[offset] = kv;
            }

            KVBlock *kv = kv_cache[offset];
            item.is_delete = kv->v_len == 0;

            // 计算key的哈希值
            if (kv->k_len <= sizeof(uint64_t))
            {
                // 小key直接使用值
                memcpy(&item.key_hash, kv->data, kv->k_len);
            }
            else
            {
                // 大key计算哈希
                item.key_hash = hash(kv->data, kv->k_len);
            }
        }
    }

    // std::unordered_map<uint64_t, KVBlock *> kv_cache; // 避免重复读取相同offset的KV
    // std::vector<rdma_future> read_tasks;                   // 存储未完成的读取任务

    // // 发起所有读取请求
    // for (auto &item : sort_items)
    // {
    //     if (item.need_read)
    //     {
    //         Slot *slot = item.slot_ptr;
    //         uint64_t offset = ralloc.ptr(slot->offset);

    //         if (kv_cache.find(offset) == kv_cache.end())
    //         {
    //             // 未读取过，执行RDMA读取但不等待
    //             KVBlock *kv = (KVBlock *)alloc.alloc(slot->len * ALIGNED_SIZE);
    //             kv_cache[offset] = kv;

    //             // 直接将任务放入vector中，不进行复制
    //             read_tasks.push_back(wo_wait_conn->read(offset, seg_rmr.rkey, kv, slot->len * ALIGNED_SIZE, lmr->lkey)); // FIXME: ENOMEM
    //         }
    //     }
    // }

    // // 等待所有读取完成
    // for (auto &task : read_tasks)
    // {
    //     // 使用std::move转移所有权，不进行复制
    //     co_await std::move(task);
    // }

    // // 现在所有KV块都已读取完成，可以计算hash
    // for (auto &item : sort_items)
    // {
    //     if (item.need_read)
    //     {
    //         Slot *slot = item.slot_ptr;
    //         uint64_t offset = ralloc.ptr(slot->offset);
    //         KVBlock *kv = kv_cache[offset];

    //         // 计算key的哈希值
    //         if (kv->k_len <= sizeof(uint64_t))
    //         {
    //             // 小key直接使用值
    //             memcpy(&item.key_hash, kv->data, kv->k_len);
    //         }
    //         else
    //         {
    //             // 大key计算哈希
    //             item.key_hash = hash(kv->data, kv->k_len);
    //         }
    //     }
    // }

    // 4. 根据定义的规则排序
    std::sort(sort_items.begin(), sort_items.end());

    // 5. 处理排序后的结果，相同key只保留最新的，且处理删除标记
    uint64_t new_seg_len = 0;
    uint64_t last_fp = 0xFF; // 不可能的值
    uint64_t last_fp2 = 0xFF;
    uint64_t last_key_hash = 0;

    for (size_t i = 0; i < sort_items.size(); i++)
    {
        const auto &item = sort_items[i];

        // 如果需要检查key且与上一个相同则跳过
        if (item.need_read &&
            item.fp == last_fp &&
            item.fp_2 == last_fp2 &&
            item.key_hash == last_key_hash)
        {
            // 跳过重复的key
            continue;
        }

        // 更新上一个处理的项的信息
        if (item.need_read)
        {
            last_fp = item.fp;
            last_fp2 = item.fp_2;
            last_key_hash = item.key_hash;
        }

        // 将Slot添加到结果数组
        if (!item.is_delete)
            new_seg[new_seg_len++] = *item.slot_ptr;
        // else log_err("[%lu:%lu:%lu]删除了一个条目", cli_id, coro_id, this->key_num);
    }
    // log_err("[%lu:%lu:%lu]合并后的seg长度从%lu减少到%lu", cli_id, coro_id, this->key_num, len + old_seg_len, new_seg_len);
    co_return new_seg_len;
}
#else // 去重不干净
#if READ_FULL_KEY_ON_FP_COLLISION
task<uint64_t> Client::merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg)
#else
uint64_t Client::merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg)
#endif
{
    std::sort(data, data + len);
    uint8_t sign = data[0].sign;
    int off_1 = 0, off_2 = 0;
    uint64_t new_seg_len = 0;
    if (len && old_seg_len)
    {
        for (uint64_t i = 0; i < len + old_seg_len; i++)
        {
#if READ_FULL_KEY_ON_FP_COLLISION
            if (data[off_1].fp < old_seg[off_2].fp)
#else
            if (data[off_1].fp <= old_seg[off_2].fp)
#endif
            {
                new_seg[new_seg_len++] = data[off_1];
                off_1++;
            }
#if READ_FULL_KEY_ON_FP_COLLISION
            else if (data[off_1].fp > old_seg[off_2].fp)
#else
            else
#endif
            {
                if (old_seg_len == 0)
                    old_seg[0].print(std::format("[{}:{}:{}]old_seg_len==0", cli_id, coro_id, this->key_num));
                new_seg[new_seg_len++] = old_seg[off_2];
                off_2++;
            }
#if READ_FULL_KEY_ON_FP_COLLISION
            else if (data[off_1].fp == old_seg[off_2].fp && data[off_1].fp_2 != old_seg[off_2].fp_2)
            {
                // fp相同但fp_2不同，按fp_2排序
                if (data[off_1].fp_2 < old_seg[off_2].fp_2)
                {
                    new_seg[new_seg_len++] = data[off_1];
                    off_1++;
                }
                else
                {
                    new_seg[new_seg_len++] = old_seg[off_2];
                    off_2++;
                }
            }
            else // fp和fp_2都相同
            {
                // 读取完整key进行比较
                // log_err("[%lu:%lu:%lu]发现fp=%d fp2=%d相同，读取完整key比较", cli_id, coro_id, this->key_num, data[off_1].fp, data[off_1].fp_2);

                // 分配足够空间存储KV块
                KVBlock *kv1 = (KVBlock *)alloc.alloc(data[off_1].len * ALIGNED_SIZE);
                KVBlock *kv2 = (KVBlock *)alloc.alloc(old_seg[off_2].len * ALIGNED_SIZE);

                // 读取两个KV块
#if CORO_DEBUG
                co_await conn->read(ralloc.ptr(data[off_1].offset), seg_rmr.rkey, kv1, data[off_1].len * ALIGNED_SIZE, lmr->lkey,
                                    std::source_location::current(),
                                    std::format("[{}:{}]segloc:{}读取CurSegKVBlock地址\n{}", cli_id, coro_id, this->key_num, ralloc.ptr(data[off_1].offset)));
                co_await conn->read(ralloc.ptr(old_seg[off_2].offset), seg_rmr.rkey, kv2, old_seg[off_2].len * ALIGNED_SIZE, lmr->lkey,
                                    std::source_location::current(),
                                    std::format("[{}:{}]segloc:{}读取MainSegKVBlock地址\n{}", cli_id, coro_id, this->key_num, ralloc.ptr(old_seg[off_2].offset)));
#else
                co_await conn->read(ralloc.ptr(data[off_1].offset), seg_rmr.rkey, kv1, data[off_1].len * ALIGNED_SIZE, lmr->lkey);
                co_await conn->read(ralloc.ptr(old_seg[off_2].offset), seg_rmr.rkey, kv2, old_seg[off_2].len * ALIGNED_SIZE, lmr->lkey);
#endif

                // 比较key是否相同
                bool same_key = (kv1->k_len == kv2->k_len) &&
                                (memcmp(kv1->data, kv2->data, kv1->k_len) == 0);

                if (same_key)
                {
                    // 相同key，只保留较新的数据(data[off_1])
                    // log_err("[%lu:%lu:%lu]确认为相同key，保留较新数据", cli_id, coro_id, this->key_num);
                    new_seg[new_seg_len++] = data[off_1];
                }
                else
                {
                    // 不同key，哈希冲突，两个都保留
                    // log_err("[%lu:%lu:%lu]确认为不同key(哈希冲突)，两个都保留 offset1:%lu offset2:%lu",
                    //         cli_id, coro_id, this->key_num,
                    //         data[off_1].offset, old_seg[off_2].offset);
                    new_seg[new_seg_len++] = data[off_1];
                    new_seg[new_seg_len++] = old_seg[off_2];
                }

                off_1++;
                off_2++;
            }
#endif
            if (off_1 >= len || off_2 >= old_seg_len)
                break;
        }
    }

    // 处理剩余元素
    if (off_1 < len)
    {
        // memcpy(new_seg + old_seg_len + off_1, data + off_1, (len - off_1) * sizeof(Slot));
        for (uint64_t i = off_1; i < len; i++)
            new_seg[new_seg_len++] = data[i];
    }
    else if (off_2 < old_seg_len)
    {
        for (uint64_t i = off_2; i < old_seg_len; i++)
            new_seg[new_seg_len++] = old_seg[i];
    }
#if READ_FULL_KEY_ON_FP_COLLISION
    co_return new_seg_len;
#else
    return new_seg_len;
#endif
}
#endif

void print_mainseg(Slot *main_seg, uint64_t main_seg_len)
{
    log_err("main_seg_len:%lu", main_seg_len);
    for (uint64_t i = 0; i < main_seg_len; i++)
    {
        main_seg[i].print();
    }
}

void print_fpinfo(FpInfo *fp_info)
{
    for (uint64_t i = 0; i <= UINT8_MAX; i++)
    {
        log_err("FP:%lu NUM:%d", i, fp_info[i].num);
    }
}

void cal_fpinfo(Slot *main_seg, uint64_t main_seg_len, FpInfo *fp_info)
{
    double avg = (1.0 * main_seg_len) / UINT8_MAX;
    uint64_t base_off = 0;
    uint64_t base_index = 0;
    uint64_t predict;
    uint8_t prev_fp = 0;
    uint64_t max_error = 32;
    uint64_t correct_cnt = 0;
    uint64_t err;
    for (uint64_t i = 0; i < main_seg_len; i++)
    {
        fp_info[main_seg[i].fp].num++;
    }
}

task<> Client::Split(uint64_t seg_loc, uintptr_t seg_ptr, CurSegMeta *old_seg_meta)
{
    log_merge("[%lu:%lu:%lu]开始分裂/合并，seg_loc:%lx", cli_id, coro_id, this->key_num, seg_loc);
    sum_cost.start_merge();
    sum_cost.start_split();
    uint64_t local_depth = old_seg_meta->local_depth;
    uint64_t global_depth = dir->global_depth;
    uint64_t main_seg_ptr = old_seg_meta->main_seg_ptr;
    // 1. Read CurSeg
    CurSeg *cur_seg = (CurSeg *)alloc.alloc(sizeof(CurSeg));
    co_await conn->read(seg_ptr, seg_rmr.rkey, cur_seg, sizeof(CurSeg), lmr->lkey);

    // 因为只有cas写入最后一个slot的cli才会进入到对一个segment的split，所以不用对Segment额外加锁
    if (cur_seg->seg_meta.main_seg_ptr != old_seg_meta->main_seg_ptr || cur_seg->seg_meta.local_depth != local_depth)
    {
        // 不应该发生，直接报错
        log_err("[%lu:%lu:%lu] inconsistent ptr at segloc:%lx local-lp:%lu remote-lp:%lu local-main_ptr:%lx remote-main_ptr:%lx", cli_id, coro_id, this->key_num, seg_loc, local_depth, dir->segs[seg_loc].local_depth, old_seg_meta->main_seg_ptr, dir->segs[seg_loc].main_seg_ptr);
        exit(-1);
    }

    // 2. Read MainSeg
    uint64_t main_seg_size = sizeof(Slot) * dir->segs[seg_loc].main_seg_len;
    MainSeg *main_seg = (MainSeg *)alloc.alloc(main_seg_size);
    if(main_seg_size)
        co_await conn->read(dir->segs[seg_loc].main_seg_ptr, seg_rmr.rkey, main_seg, main_seg_size, lmr->lkey);

    // 3. Sort Segment
    MainSeg *new_main_seg = (MainSeg *)alloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG);
#if READ_FULL_KEY_ON_FP_COLLISION
    uint64_t new_seg_len = co_await merge_insert(cur_seg->slots, SLOT_PER_SEG, main_seg->slots, dir->segs[seg_loc].main_seg_len, new_main_seg->slots);
#else
    uint64_t new_seg_len = merge_insert(cur_seg->slots, SLOT_PER_SEG, main_seg->slots, dir->segs[seg_loc].main_seg_len, new_main_seg->slots);
#endif
    FpInfo fp_info[MAX_FP_INFO] = {};
    cal_fpinfo(new_main_seg->slots, new_seg_len, fp_info); // SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len

    log_merge("将segloc:%lu/%lu的%d个CurSeg条目合并到%d个MainSeg条目", seg_loc, (1ull << global_depth) - 1, SLOT_PER_SEG, dir->segs[seg_loc].main_seg_len);
    // 4. Split (为了减少协程嵌套层次的开销，这里就不抽象成单独的函数了)
    if (dir->segs[seg_loc].main_seg_len >= MAX_MAIN_SIZE){
        // dir->print(std::format("[{}:{}]分裂之前", cli_id, coro_id));
        // log_err("开始分裂，seg_loc:%lx main_seg_len:%lu >= MAX_MAIN_SIZE:%lu", seg_loc, dir->segs[seg_loc].main_seg_len, MAX_MAIN_SIZE);
        // 为了避免覆盖bug，同时Merge/Local Split中都额外更新了一倍的DirEntry
        // 因此Global Split不用处理DirEntry翻倍操作，只需要更新Global Depth

        // 4.1 Split Main Segment
        MainSeg *new_seg_1 = (MainSeg *)alloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG);
        MainSeg *new_seg_2 = (MainSeg *)alloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG);
        uint64_t dep_off = (local_depth) % 4;

        // if (dep_off == 3)
        //     log_err("dep_off:%lu, 需要读取完整KV", dep_off);

        bool dep_bit = false;
        uint64_t pattern;
        uint64_t off1 = 0, off2 = 0;
        KVBlock *kv_block = (KVBlock *)alloc.alloc(130 * ALIGNED_SIZE);
        for (uint64_t i = 0; i < new_seg_len; i++) // SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len
        {
            dep_bit = (new_main_seg->slots[i].dep >> dep_off) & 1;
            if (dep_off == 3)
            {
                // if dep_off == 3 (Have consumed all info in dep bits), read && construct new dep
#ifdef TOO_LARGE_KV
                co_await conn->read(ralloc.ptr(new_main_seg->slots[i].offset), seg_rmr.rkey, kv_block,
                                    this->kv_block_len, lmr->lkey);
#else
                co_await conn->read(ralloc.ptr(new_main_seg->slots[i].offset), seg_rmr.rkey, kv_block,
                                    new_main_seg->slots[i].len * ALIGNED_SIZE, lmr->lkey); // IMPORTANT: 读取完整KV，合并可以参考。
#endif
                // 验证KVBlock合法性
                if (!kv_block->is_valid())
                    continue;

                pattern = (uint64_t)hash(kv_block->data, kv_block->k_len);
                new_main_seg->slots[i].dep = pattern >> (local_depth + 1);
                // if (kv_block->k_len != 8)
                // {
                //     new_main_seg->slots[i].print();
                //     uint64_t cros_seg_loc = get_seg_loc(pattern, local_depth+1);
                //     log_err("[%lu:%lu:%lu]kv_block k_len:%lu v_len:%lu key:%lu value:%s cros_seg_loc:%lx",cli_id,coro_id,this->key_num, kv_block->k_len, kv_block->v_len, *(uint64_t *)kv_block->data, kv_block->data + 8, cros_seg_loc);
                //     exit(-1);
                // }
            }
            if (dep_bit)
                new_seg_2->slots[off2++] = new_main_seg->slots[i];
            else
                new_seg_1->slots[off1++] = new_main_seg->slots[i];
        }
        FpInfo fp_info1[MAX_FP_INFO] = {};
        FpInfo fp_info2[MAX_FP_INFO] = {};
        cal_fpinfo(new_seg_1->slots, off1, fp_info1);
        cal_fpinfo(new_seg_2->slots, off2, fp_info2); // IMPORTANT: 此时new_seg_1和new_seg_2准备完成，可以写入到远端。

        // 4.2 Update new-main-ptr for DirEntries // IMPORTANT: 远端申请新的CurSeg和MainSeg
        uintptr_t new_cur_ptr = ralloc.alloc(sizeof(CurSeg), true);
        uintptr_t new_main_ptr1 = ralloc.alloc(sizeof(Slot) * off1);
        uintptr_t new_main_ptr2 = ralloc.alloc(sizeof(Slot) * off2);

        // a. 同步远端global depth, 确认split类型
        co_await check_gd();

        // b. update dir
        uint64_t stride = (1llu) << (dir->global_depth - local_depth);
        uint64_t cur_seg_loc;
        uint64_t first_seg_loc = seg_loc & ((1ull << local_depth) - 1);
        uintptr_t cur_seg_ptr ;
        uint64_t dir_size = (1 << dir->global_depth); //为了兼容同步发生的Global Split
        uint64_t dir_bound = 2;
        if(local_depth == dir->global_depth){
            stride = 2; // 为了global split正确触发
            dir_bound = 1;
        }

        uint64_t first_new_seg_loc = UINT64_MAX, first_original_seg_loc = UINT64_MAX;

        for(uint64_t i = 0 ; i < dir_bound ; i++){
            uint64_t offset = i * dir_size;
            for (uint64_t i = 0; i < stride; i++)
            {
                cur_seg_loc = (i << local_depth) | first_seg_loc;
                if (i & 1){
                    // Local SegOffset
                    this->offset[cur_seg_loc+offset].offset = 0;
                    this->offset[cur_seg_loc+offset].main_seg_ptr = new_main_ptr2;
                    // DirEntry
                    // log_err("更新dir->segs[%lu+%lu=%lu].cur_seg_ptr为新的seg_ptr: %lx->%lx", cur_seg_loc, offset, cur_seg_loc + offset, dir->segs[cur_seg_loc + offset].cur_seg_ptr, new_cur_ptr);
                    dir->segs[cur_seg_loc+offset].cur_seg_ptr = new_cur_ptr;
                    dir->segs[cur_seg_loc+offset].main_seg_ptr = new_main_ptr2;
                    dir->segs[cur_seg_loc+offset].main_seg_len = off2;
                    memcpy(dir->segs[cur_seg_loc+offset].fp, fp_info2, sizeof(FpInfo) * MAX_FP_INFO);
                    if (first_new_seg_loc == UINT64_MAX)
                        first_new_seg_loc = cur_seg_loc + offset;
                }else{
                    // Local SegOffset
                    this->offset[cur_seg_loc+offset].offset = 0;
                    this->offset[cur_seg_loc+offset].main_seg_ptr = new_main_ptr1;
                    // DirEntry
                    // log_err("更新dir->segs[%lu+%lu=%lu].cur_seg_ptr为旧的seg_ptr: %lx->%lx", cur_seg_loc, offset, cur_seg_loc + offset, dir->segs[cur_seg_loc + offset].cur_seg_ptr, seg_ptr);
                    dir->segs[cur_seg_loc+offset].cur_seg_ptr = seg_ptr; // 可能dir中的多个条目指向同一个seg
                    dir->segs[cur_seg_loc+offset].main_seg_ptr = new_main_ptr1;
                    dir->segs[cur_seg_loc+offset].main_seg_len = off1;
                    memcpy(dir->segs[cur_seg_loc+offset].fp, fp_info1, sizeof(FpInfo) * MAX_FP_INFO);
                    if (first_original_seg_loc == UINT64_MAX)
                        first_original_seg_loc = cur_seg_loc + offset;
                }
                dir->segs[cur_seg_loc].local_depth = local_depth + 1;
                cur_seg_ptr = seg_rmr.raddr + sizeof(uint64_t) + (cur_seg_loc+offset) * sizeof(DirEntry);
                
                // Update DirEntry
                co_await conn->write(cur_seg_ptr, seg_rmr.rkey,&dir->segs[cur_seg_loc+offset], sizeof(DirEntry), lmr->lkey);

                // if (local_depth == dir->global_depth){
                //     // global
                //     log_err("[%lu:%lu:%lu]Global SPlit At segloc:%lx depth:%lu to :%lu with new seg_ptr:%lx new_main_seg_ptr:%lx", cli_id, coro_id, this->key_num, cur_seg_loc+offset, local_depth, local_depth + 1, i & 1 ? new_cur_ptr:seg_ptr,i & 1 ?new_main_ptr2:new_main_ptr1);
                // }else{
                //     // local 
                //     log_err("[%lu:%lu:%lu]Local SPlit At segloc:%lx depth:%lu to :%lu with new seg_ptr:%lx new main_seg_ptr:%lx", cli_id, coro_id, this->key_num, cur_seg_loc+offset, local_depth, local_depth + 1, i & 1 ? new_cur_ptr:seg_ptr, i & 1 ? new_main_ptr2:new_main_ptr1);
                // }
            }
        }
        // Update Global Depth
        if(local_depth == dir->global_depth){
            // log_err("local_depth达到上限，更新global depth:%lu -> %lu, max segs:%d to %d",dir->global_depth,dir->global_depth+1, (1 << global_depth), (1 << (global_depth+1))); // at first we have 16 segs
            dir->global_depth++;
            co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, &dir->global_depth, sizeof(uint64_t), lmr->lkey);
        }

        // 4.3 Write New MainSeg to Remote
        // a. New CurSeg && New MainSeg
        CurSeg *new_cur_seg = (CurSeg *)alloc.alloc(sizeof(CurSeg));
        memset(new_cur_seg, 0, sizeof(CurSeg));
        new_cur_seg->seg_meta.local_depth = local_depth + 1; // IMPORTANT: local_depth+1，这是让客户端感知到split的关键
        new_cur_seg->seg_meta.sign = 1;
        new_cur_seg->seg_meta.main_seg_ptr = new_main_ptr2;
        new_cur_seg->seg_meta.main_seg_len = off2;
        wo_wait_conn->pure_write(new_cur_seg->seg_meta.main_seg_ptr, seg_rmr.rkey, new_seg_2, sizeof(Slot) * off2, lmr->lkey);
        co_await conn->write(new_cur_ptr, seg_rmr.rkey, new_cur_seg, sizeof(CurSeg), lmr->lkey);
        // log_err("[%lu:%lu:%lu]更新新的CurSeg->seg_meta.local_depth: %lu", cli_id, coro_id, this->key_num, new_cur_seg->seg_meta.local_depth);
        // b. new main_seg for old
        cur_seg->seg_meta.main_seg_ptr = new_main_ptr1;
        cur_seg->seg_meta.main_seg_len = off1;
        cur_seg->seg_meta.local_depth = local_depth + 1; // IMPORTANT: local_depth+1，这是让客户端感知到split的关键
        // cur_seg->seg_meta.sign = !cur_seg->seg_meta.sign; // 对old cur_seg的清空放到最后?保证同步。
        memset(cur_seg->seg_meta.fp_bitmap, 0, sizeof(uint64_t) * 16);
        wo_wait_conn->pure_write(cur_seg->seg_meta.main_seg_ptr, seg_rmr.rkey, new_seg_1, sizeof(Slot) * off1, lmr->lkey);
        co_await conn->write(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(CurSegMeta),lmr->lkey);
        // co_await conn->write(seg_ptr + 2 * sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 2, sizeof(CurSegMeta) - sizeof(uint64_t), lmr->lkey);
//         log_err("[%lu:%lu:%lu]更新旧的CurSeg->seg_meta.local_depth: %lu", cli_id, coro_id, this->key_num, new_cur_seg->seg_meta.local_depth);

        // 4.4 Change Sign (Equal to unlock this segment)
        cur_seg->seg_meta.sign = !cur_seg->seg_meta.sign;
        co_await wo_wait_conn->write(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(uint64_t), lmr->lkey);
        sum_cost.end_split();
        // dir->print(std::format("[{}:{}]分裂之后", cli_id, coro_id));
        co_return;
    }

    // 5. Merge
    // 5.1 Write New MainSeg to Remote && Update CurSegMeta
    // a. write main segment

    uintptr_t new_main_ptr = ralloc.alloc(new_seg_len * sizeof(Slot), true); // IMPORTANT: 在MN分配new main seg，注意 FIXME: ralloc没有free功能 // main_seg_size + sizeof(Slot) * SLOT_PER_SEG
    // uint64_t new_main_len = dir->segs[seg_loc].main_seg_len + SLOT_PER_SEG;
    wo_wait_conn->pure_write(new_main_ptr, seg_rmr.rkey, new_main_seg->slots,
                             sizeof(Slot) * new_seg_len, lmr->lkey);
    // co_await wo_wait_conn->write(new_main_ptr, seg_rmr.rkey, new_main_seg->slots,
    //                             sizeof(Slot) * new_seg_len, lmr->lkey);

    // b. Update MainSegPtr/Len and fp_bitmap
    cur_seg->seg_meta.main_seg_ptr = new_main_ptr;
    cur_seg->seg_meta.main_seg_len = new_seg_len; // main_seg_size / sizeof(Slot) + SLOT_PER_SEG;
    this->offset[seg_loc].offset = 0;
    memset(cur_seg->seg_meta.fp_bitmap, 0, sizeof(uint64_t) * 16);
    co_await conn->write(seg_ptr + 2 * sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 2, sizeof(CurSegMeta) - sizeof(uint64_t), lmr->lkey);

    // 5.2 Update new-main-ptr for DirEntries
    uint64_t stride = (1llu) << (dir->global_depth - local_depth);
    uint64_t cur_seg_loc;
    uint64_t first_seg_loc = seg_loc & ((1ull << local_depth) - 1);
    uintptr_t dentry_ptr ;
    uint64_t dir_size = (1 << dir->global_depth); //为了兼容同步发生的Global Split
    for(uint64_t i = 0 ; i < 2 ; i++){ // 同一个Segment可以对应多个DirEntry
        uint64_t offset = i * dir_size;
        for (uint64_t i = 0; i < stride; i++)
        {
            cur_seg_loc = (i << local_depth) | first_seg_loc;
            // Local SegOffset
            this->offset[cur_seg_loc+offset].offset = 0;
            this->offset[cur_seg_loc+offset].main_seg_ptr = new_main_ptr;
            // DirEntry
            dir->segs[cur_seg_loc+offset].local_depth = cur_seg->seg_meta.local_depth;
            dir->segs[cur_seg_loc+offset].cur_seg_ptr = seg_ptr; // 这里会在没分裂时就更新超出depth的dir
            dir->segs[cur_seg_loc+offset].main_seg_ptr = new_main_ptr;
            dir->segs[cur_seg_loc+offset].main_seg_len = new_seg_len;
            memcpy(dir->segs[cur_seg_loc+offset].fp, fp_info, sizeof(FpInfo) * MAX_FP_INFO);
            dentry_ptr = seg_rmr.raddr + sizeof(uint64_t) + (cur_seg_loc+offset) * sizeof(DirEntry) ;
            // Update
            // 暂时还是co_await吧
            co_await conn->write(dentry_ptr, seg_rmr.rkey,&dir->segs[cur_seg_loc+offset], sizeof(DirEntry) , lmr->lkey);
            // conn->pure_write(dentry_ptr, seg_rmr.rkey,&dir->segs[cur_seg_loc+offset], sizeof(DirEntry), lmr->lkey);
            // log_err("[%lu:%lu:%lu]Merge At segloc:%lu depth:%lu with old_main_ptr:%lx new_main_ptr:%lx",cli_id,coro_id,this->key_num,cur_seg_loc+offset,local_depth,main_seg_ptr,new_main_ptr);
        }
    }

    // 5.3 Change Sign (Equal to unlock this segment)
    cur_seg->seg_meta.sign = !cur_seg->seg_meta.sign;
    cur_seg->seg_meta.slot_cnt = 0;
    co_await wo_wait_conn->write(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(uint64_t),lmr->lkey);
    sum_cost.end_merge();
}

task<> Client::print_main_seg(uint64_t seg_loc,uintptr_t main_seg_ptr, uint64_t main_seg_len){
    Slot *main_seg = (Slot *)alloc.alloc(main_seg_len*sizeof(Slot));
    co_await conn->read(main_seg_ptr, seg_rmr.rkey, main_seg, main_seg_len*sizeof(Slot), lmr->lkey);
    KVBlock *kv_block = (KVBlock *)alloc.alloc(130 * ALIGNED_SIZE);
    for (uint64_t i = 0; i < main_seg_len; i++){
        main_seg[i].print(i);
        co_await wo_wait_conn->read(ralloc.ptr(main_seg[i].offset), seg_rmr.rkey, kv_block,(main_seg[i].len) * ALIGNED_SIZE, lmr->lkey);
        std::string desc = "main_seg:"+std::to_string(seg_loc)+",main_seg_len:"+std::to_string(main_seg_len)+" i:"+std::to_string(i);
        kv_block->print(desc.c_str());
    }
}

task<std::tuple<uintptr_t, uint64_t>> Client::search(Slice *key, Slice *value)
{
    // log_err("search key:%lu value:%s", *(uint64_t *)key->data, value->data);
    perf.start_perf();
    // 1. Cal Segloc && Pattern
    uint64_t pattern = (uint64_t)hash(key->data, key->len);
    uint64_t pattern_fp1 = fp(pattern);
    uint64_t pattern_fp2 = fp2(pattern);
#if LARGER_FP_FILTER_GRANULARITY
    auto bit_loc = get_fp_bit(pattern_fp1, pattern_fp2); // 目前完全没用上
#else
    auto [bit_loc, bit_info] = get_fp_bit(pattern_fp1,pattern_fp2);
#endif
    this->key_num = *(uint64_t *)key->data;
    uintptr_t slot_ptr;
    uint64_t slot_content;
    uint64_t cnt=0;
Retry:
    cnt++;
    alloc.ReSet(sizeof(Directory));
    uint64_t segloc = get_seg_loc(pattern, dir->global_depth);

    // 2. Get SegPtr, MainSegPtr and Slot Offset
    uintptr_t cur_seg_ptr = dir->segs[segloc].cur_seg_ptr;
    uintptr_t main_seg_ptr = dir->segs[segloc].main_seg_ptr;
    uint64_t main_seg_len = dir->segs[segloc].main_seg_len;
    if(cur_seg_ptr==0 || main_seg_ptr == 0){ // FIXME: stuck here, main_seg_ptr is 0 when load_num < num_op
        // log_err("seg_loc:%lu,cur_seg_ptr:%lx main_seg_ptr:%lx",segloc,cur_seg_ptr,main_seg_ptr);
        cur_seg_ptr = co_await check_gd(segloc,true);
        this->offset[segloc].offset = 0;
        this->offset[segloc].main_seg_ptr = dir->segs[segloc].main_seg_ptr;
        // dir->print();
        goto Retry;
    }

    uint64_t start_pos = 0;
    uint64_t end_pos = main_seg_len;
    for (uint64_t i = 0; i <= UINT8_MAX; i++)
    {
        if (i == UINT8_MAX || i >= pattern_fp1)
        {
            break;
        }
        start_pos += dir->segs[segloc].fp[i].num;
    }
    end_pos = start_pos + dir->segs[segloc].fp[pattern_fp1].num;
    uint64_t main_size = (end_pos - start_pos) * sizeof(Slot);
    // log_err("读取segloc:%lu的范围为[%lu,%lu)的%d个Slot",segloc,start_pos,end_pos,dir->segs[segloc].fp[pattern_fp1].num);

    // 3. Read SegMeta && MainSlots 读取CurSegMeta(不含bitmap)和FPTable过滤后的MainSeg
    CurSegMeta *my_seg_meta = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta)); // TODO: 同时更新到本地的seg_meta缓存里
    // auto read_meta = conn->read(cur_seg_ptr + sizeof(uint64_t), seg_rmr.rkey, my_seg_meta, sizeof(CurSegMeta), lmr->lkey);
    auto read_meta = conn->read(cur_seg_ptr + sizeof(uint64_t), seg_rmr.rkey, my_seg_meta, 3 * sizeof(uint64_t), lmr->lkey);
    auto read_bit_map = conn->read(cur_seg_ptr + 4 * sizeof(uint64_t) + bit_loc * sizeof(FpBitmapType), seg_rmr.rkey, &my_seg_meta->fp_bitmap[bit_loc], sizeof(FpBitmapType), lmr->lkey); // IMPORTANT: 读取CurSeg FPBitmap的对应bit
    Slot *main_seg = (Slot *)alloc.alloc(main_size);
    auto read_main_seg = wo_wait_conn->read(main_seg_ptr + start_pos * sizeof(Slot), seg_rmr.rkey, main_seg, main_size, lmr->lkey);
    
    // 4. Check Depth && MainSegPtr
    co_await std::move(read_meta);
    if (my_seg_meta->main_seg_ptr != this->offset[segloc].main_seg_ptr) { // 读取到的main_seg_ptr和本地的不一致，意味着读到的过滤MainSeg是错误的，需要重新读取
        // 检查一遍Global Depth是否一致
        // TODO:增加segloc参数，读取对应位置的cur_seg_ptr；否则split的信息无法被及时同步
        uintptr_t new_cur_ptr = co_await check_gd(segloc);
        uint64_t new_seg_loc = get_seg_loc(pattern, dir->global_depth);
        if(new_cur_ptr != cur_seg_ptr || segloc != new_seg_loc){
            // log_err("[%lu:%lu:%lu]stale cur_seg_ptr for segloc:%lx with old:%lx new:%lx",cli_id,coro_id,this->key_num,segloc,cur_seg_ptr,new_cur_ptr);
            this->offset[segloc].offset = 0;
            this->offset[segloc].main_seg_ptr = dir->segs[segloc].main_seg_ptr;
            co_await std::move(read_bit_map);
            co_await std::move(read_main_seg);
            goto Retry;
        } 
        // 更新所有指向此Segment的DirEntry
        // log_err("[%lu:%lu:%lu]stale main_seg_ptr for segloc:%lx with old:%lx new:%lx",cli_id,coro_id,this->key_num,segloc,my_seg_meta->main_seg_ptr,this->offset[segloc].main_seg_ptr);
        uint64_t new_local_depth = my_seg_meta->local_depth;
        uint64_t stride = (1llu) << (dir->global_depth - new_local_depth);
        uint64_t cur_seg_loc;
        uint64_t first_seg_loc = segloc & ((1ull << new_local_depth) - 1);
        for (uint64_t i = 0; i < stride; i++)
        {
            cur_seg_loc = (i << new_local_depth) | first_seg_loc;
            dir->segs[cur_seg_loc].main_seg_ptr = my_seg_meta->main_seg_ptr;
            dir->segs[cur_seg_loc].main_seg_len = my_seg_meta->main_seg_len;
            this->offset[cur_seg_loc].offset = 0;
            this->offset[cur_seg_loc].main_seg_ptr = my_seg_meta->main_seg_ptr;
            dir->segs[segloc].local_depth = my_seg_meta->local_depth;
        }
        co_await std::move(read_bit_map);
        co_await std::move(read_main_seg);
        // 怎么同步信息；同步哪些信息
        goto Retry;
    }
    // log_err("read at seg_loc:%lu with main_ptr:%lx",segloc,my_seg_meta->main_seg_ptr);

    // 5. Find Slot && Read KV
    uint64_t version = UINT64_MAX;
    uint64_t res_slot = UINT64_MAX;
    KVBlock *res = nullptr;
    KVBlock *kv_block = (KVBlock *)alloc.alloc(130 * ALIGNED_SIZE);
    uint64_t dep = dir->segs[segloc].local_depth - (dir->segs[segloc].local_depth % 4); // 按4对齐
    uint8_t dep_info = (pattern >> dep) & 0xf;

    // 5.1 Find In Main
    // 在Main中，相同FP的Slot，更新的Slot会被写入在更靠前的位置
    co_await std::move(read_main_seg);
    // 合并两次循环，同时查找fp匹配+fp2匹配的条目和fp匹配但fp2不匹配的条目
    for (uint64_t i = 0; i < end_pos - start_pos; i++)
    {
#if HASH_TYPE == MYHASH // MYHASH: 只有fp_2匹配的才读取并检查完整key
        if (main_seg[i] != 0 && main_seg[i].fp == pattern_fp1 && main_seg[i].dep == dep_info && main_seg[i].fp_2 == pattern_fp2 && main_seg[i].is_valid())
#else // SepHash: 不论fp_2是否匹配，都读取并检查完整key
        if (main_seg[i] != 0 && main_seg[i].fp == pattern_fp1 && main_seg[i].dep == dep_info && main_seg[i].is_valid())
#endif
        {
            uintptr_t kv_ptr = ralloc.ptr(main_seg[i].offset);
            if (kv_ptr == 0)
                continue;

#ifdef TOO_LARGE_KV
            co_await wo_wait_conn->read(kv_ptr, seg_rmr.rkey, kv_block, this->kv_block_len, lmr->lkey);
#else
            co_await wo_wait_conn->read(kv_ptr, seg_rmr.rkey, kv_block, (main_seg[i].len) * ALIGNED_SIZE, lmr->lkey);
#endif

            // 检查是否是我们要找的key
            if (memcmp(key->data, kv_block->data, key->len) == 0)
            {
                // 找到了匹配的key，更新最佳匹配
                // 在MainSeg中，更靠前的位置表示更新的版本，所以第一个匹配的是最新的
                slot_ptr = main_seg_ptr + (start_pos + i) * sizeof(Slot);
                slot_content = main_seg[i];
                res_slot = i;
                res = kv_block;
                break; // 因为MainSeg中靠前的是更新的，所以找到第一个匹配项就可以退出循环
            }
        }
    }

    // 5.2 Find In CurSeg
    // 在CurSeg中，相同FP的Slot，更新的Slot会被写入在更靠后的位置
    co_await std::move(read_bit_map);
#if LARGER_FP_FILTER_GRANULARITY
    if (my_seg_meta->fp_bitmap[bit_loc]) // 实际上应该fp_bitmap中认为有的，都要去CurSeg中找
#else
    if ((my_seg_meta->fp_bitmap[bit_loc] & bit_info) == bit_info)
#endif
    // if (res==nullptr) // 只有在Main中找不到时才去CurSeg中找
    {
        Slot* curseg_slots = (Slot *)alloc.alloc(sizeof(Slot) * SLOT_PER_SEG);
        co_await conn->read(cur_seg_ptr + sizeof(uint64_t) + sizeof(CurSegMeta), seg_rmr.rkey, curseg_slots, sizeof(Slot) * SLOT_PER_SEG, lmr->lkey);
        for (uint64_t i = SLOT_PER_SEG-1; i != -1; i--)
        {
            // curseg_slots[i].print();
            // IMPORTANT: 对于MYHASH(curseg_slots[i].local_depth != 0)，还要匹配local_depth，不匹配的是乐观写入时写错segment的条目
#if HASH_TYPE == MYHASH
            if (curseg_slots[i] != 0 && curseg_slots[i].fp == pattern_fp1 && curseg_slots[i].dep == dep_info && curseg_slots[i].fp_2 == pattern_fp2 && curseg_slots[i].local_depth == my_seg_meta->local_depth && curseg_slots[i].is_valid())
#else
            if (curseg_slots[i] != 0 && curseg_slots[i].fp == pattern_fp1 && curseg_slots[i].dep == dep_info && curseg_slots[i].fp_2 == pattern_fp2 && curseg_slots[i].is_valid())
#endif
            {
                uintptr_t kv_ptr = ralloc.ptr(curseg_slots[i].offset);
                if (kv_ptr == 0) continue;
#ifdef TOO_LARGE_KV
                co_await conn->read(kv_ptr, seg_rmr.rkey, kv_block, this->kv_block_len, lmr->lkey);
#else
                co_await conn->read(kv_ptr, seg_rmr.rkey, kv_block, (curseg_slots[i].len) * ALIGNED_SIZE, lmr->lkey);
#endif
                // log_err("[%lu:%lu:%lu]read at segloc:%lx cur_seg with: pattern_fp1:%lx pattern_fp2:%lx dep_info:%x seg slot:fp:%x fp2:%x dep:%x",cli_id,coro_id,this->key_num,segloc,pattern_fp1,pattern_fp2,dep_info,curseg_slots[i].fp,curseg_slots[i].fp_2,curseg_slots[i].dep);
                if (memcmp(key->data, kv_block->data, key->len) == 0)
                {
                    slot_ptr = cur_seg_ptr + sizeof(uint64_t) + sizeof(CurSegMeta) +  i * sizeof(Slot);
                    slot_content = curseg_slots[i];
                    res_slot = i;
                    res = kv_block;
                    break;
                }
            }
        }
    }

    if (res != nullptr && res->v_len != 0)
    {
        value->len = res->v_len;
        memcpy(value->data, res->data + res->k_len, value->len);
        perf.push_search();
        sum_cost.push_level_cnt(cnt);
        co_return std::make_tuple(slot_ptr, slot_content);;
    }

    // log_err("[%lu:%lu:%lu] No match key at segloc:%lu with local_depth:%lu and global_depth:%lu seg_ptr:%lx main_seg_ptr:%lx",
    //         cli_id, coro_id, key_num, segloc, dir->segs[segloc].local_depth, dir->global_depth, cur_seg_ptr,
    //         dir->segs[segloc].main_seg_ptr);
    // log_err("[%lu:%lu:%lu] segloc:%lx bit_loc:%lu bit_info:%16lx fp_bitmap[%lu]&bit_info = %lu",cli_id,coro_id,this->key_num,segloc,bit_loc,bit_info,bit_loc,seg_meta->fp_bitmap[bit_loc]&bit_info);
    // if(miss_cnt==0 && cli_id == 0 && coro_id == 0){
    //     co_await print_main_seg(segloc,main_seg_ptr,main_seg_len);
    // }
    miss_cnt++;
    // exit(-1);
    perf.push_search();
    sum_cost.push_level_cnt(cnt);
    co_return std::make_tuple(0ull, 0);
}

// task<> Client::update(Slice *key, Slice *value)
// {
//     co_await this->insert(key,value);
//     co_return;
// }

// 找到对应的slot，然后CAS更新slot的内容
task<> Client::update(Slice *key, Slice *value)
{
    // 日志结构顺序写入的设计中，update操作等同于insert操作
    co_await this->insert(key, value);
//     // log_err("before update key:%lu value:%s cli_id:%d", *(uint64_t *)key->data, value->data,cli_id);
//     uint64_t pattern = (uint64_t)hash(key->data, key->len);
//     KVBlock *kv_block = InitKVBlock(key, value, &alloc);
//     uint64_t kvblock_len = key->len + value->len + sizeof(uint64_t) * 2;
//     uint64_t kvblock_ptr = ralloc.alloc(kvblock_len);
//     wo_wait_conn->pure_write(kvblock_ptr, seg_rmr.rkey, kv_block, kvblock_len, lmr->lkey);

//     char data[1024];
//     Slice ret_value;
//     ret_value.data = data;
//     auto [slot_ptr, old_slot] = co_await search(key, &ret_value); // FIXME: may stuck when load_num < num_op
    
//     Slot *tmp = (Slot *)alloc.alloc(sizeof(Slot));
//     Slot old = (Slot) old_slot;
//     tmp->dep = old.dep;
//     tmp->fp = fp(pattern);
//     tmp->len = (kvblock_len + ALIGNED_SIZE - 1) / ALIGNED_SIZE;
//     tmp->sign = old.sign;
//     tmp->offset = ralloc.offset(kvblock_ptr);
//     tmp->fp_2 = fp2(pattern);

//     if (slot_ptr != 0ull)
//     {
//         // log_err("old kvblock_ptr:%lx offset:%lx",ralloc.ptr(old.offset),old.offset);
//         // log_err("new kvblock_ptr:%lx offset:%lx",kvblock_ptr,tmp->offset);
//         // log_err("[%lu:%lu]slot_ptr:%lx slot:%lx for %lu to be updated with new slot: fp:%d len:%d offset:%lx",cli_id,coro_id,slot_ptr,old_slot,*(uint64_t*)key->data,tmp->fp,tmp->len,tmp->offset);
//         // 3rd RTT: Setting the key-value block to full zero
// #if RETRY_CAS
//         int retry_count = 0;
//         while (retry_count < 1000000) {
//             if (co_await conn->cas_n(slot_ptr, seg_rmr.rkey, old_slot, *(uint64_t*)tmp)) {
//             break;
//             }
//             retry_count++;
//         }
//         if (retry_count >= 1000000) {
//             log_err("[%lu:%lu]Fail to update key : %lu after 1000000 retries", cli_id, coro_id, *(uint64_t*)key->data);
//         }
// #else
//         if (!co_await conn->cas_n(slot_ptr, seg_rmr.rkey, old_slot, *(uint64_t*)tmp)){
//             log_err("[%lu:%lu]Fail to update key : %lu",cli_id,coro_id,*(uint64_t*)key->data);
//         }
// #endif
//     }else{
//         log_err("[%lu:%lu]No match key for %lu to update",cli_id,coro_id,*(uint64_t*)key->data);
//     }
    co_return;
}

task<> Client::remove(Slice *key)
{
    Slice delete_value;
    delete_value.len = 0;
    delete_value.data = nullptr;
    co_await this->insert(key, &delete_value);
    co_return;
}

} // namespace SPLIT_OP
