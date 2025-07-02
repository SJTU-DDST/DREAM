#include "myhash.h"
#include <arpa/inet.h>

namespace MYHASH
{
#if SPLIT_LOCAL_LOCK
    std::vector<std::shared_mutex> Client::segloc_locks(DIR_SIZE);
#endif
    task<> Client::insert(Slice *key, Slice *value)
    {
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
        alloc.ReSet(sizeof(Directory) + kvblock_len);

        // 1. Cal Segloc according to Global Depth At local
        Slot *tmp = (Slot *)alloc.alloc(sizeof(Slot));
        uint64_t segloc = get_seg_loc(pattern, dir->global_depth);
        segloc %= (1 << dir->segs[segloc].local_depth);
        uintptr_t segptr = dir->segs[segloc].cur_seg_ptr;
        assert_require(segptr != 0);

#if DISABLE_OPTIMISTIC_SPLIT
        // uint64_t old_segloc = segloc;
        co_await check_gd();// segloc, false, true);
        // uint64_t new_segloc = get_seg_loc(pattern, dir->global_depth);
        // new_segloc %= (1 << dir->segs[new_segloc].local_depth);
        // if (old_segloc != new_segloc) {
        //     retry_cnt++;
        //     goto Retry;
        // }
#endif

        // Init Slot
        uint64_t dep = dir->segs[segloc].local_depth - (dir->segs[segloc].local_depth % 4);
        tmp->dep = pattern >> dep;
        tmp->len = (kvblock_len + ALIGNED_SIZE - 1) / ALIGNED_SIZE;
        tmp->sign = 0; // 目前sign的作用已经被slot_cnt替代
        tmp->offset = ralloc.offset(kvblock_ptr);
#if EMBED_FULL_KEY
        tmp->fp = *(uint64_t *)key->data;
        tmp->fp_2 = *(uint64_t *)(key->data + sizeof(uint64_t));
#else
        tmp->fp = fp(pattern);
        tmp->fp_2 = fp2(pattern);
#endif
        tmp->local_depth = dir->segs[segloc].local_depth; // 本地的local_depth，用于合并时去除过时条目
        if (seg_meta.find(segloc) == seg_meta.end())
        {
            seg_meta[segloc] = CurSegMeta();
            memset(&seg_meta[segloc], 0, sizeof(CurSegMeta));
        }

        uint64_t fetch = 0;
        FetchMeta meta;
        bool need_retry = false;
#if USE_TICKET_HASH
        // RTT1. FAA slot_ticket
        {
#if SPLIT_LOCAL_LOCK
            std::shared_lock<std::shared_mutex> read_lock(segloc_locks[segloc]);
#endif
            co_await conn->fetch_add(segptr + sizeof(uint64_t), seg_rmr.rkey, fetch, SLOT_TICKET_INC);
            // print_fetch_meta(fetch, std::format("增加segloc:{}的slot_ticket", segloc), SLOT_TICKET_INC);
        }
        meta = *reinterpret_cast<FetchMeta *>(&fetch);
        seg_meta[segloc].sign = meta.sign;
        if (meta.local_depth > dir->segs[segloc].local_depth)
        {
            co_await check_gd(segloc, false, true);
            uint64_t new_segloc = get_seg_loc(pattern, dir->global_depth);
            new_segloc %= (1 << dir->segs[new_segloc].local_depth);
            if (!need_retry && segloc != new_segloc) {
                need_retry = true;
                // log_err("[%lu:%lu:%lu]远端分裂了！remote_local_depth:%lu>local_depth:%lu，segloc:%lu->%lu，FAA地址:%lx",
                //         cli_id, coro_id, this->key_num, meta.local_depth, dir->segs[segloc].local_depth, segloc, new_segloc, segptr + sizeof(uint64_t));
            }
            if (!need_retry) tmp->local_depth = dir->segs[segloc].local_depth; // 如果不需要重试，则更新local_depth，避免在合并时去除过时条目
        }
        // bool is_write_buffer_1 = true; // (meta.slot_ticket < (SLOT_PER_SEG / 2)); 先只用一个buffer
        int target_merge_cnt = (meta.slot_ticket / SLOT_PER_SEG) % CLEAR_MERGE_CNT_PERIOD;
        if (meta.merge_cnt % CLEAR_MERGE_CNT_PERIOD != target_merge_cnt)
        {
#if SPLIT_LOCAL_LOCK
            std::unique_lock<std::shared_mutex> write_lock(segloc_locks[segloc], std::try_to_lock);
#endif
            FetchMeta *my_fetch_meta = (FetchMeta *)alloc.alloc(sizeof(FetchMeta));
            // int count = 0;
            while (meta.merge_cnt % CLEAR_MERGE_CNT_PERIOD != target_merge_cnt)
            {
                co_await conn->read(segptr + sizeof(uint64_t), seg_rmr.rkey, my_fetch_meta, sizeof(FetchMeta), lmr->lkey);
                meta.merge_cnt = my_fetch_meta->merge_cnt;
                usleep(10);
            }
            // if (count > 100) {
            //     log_err("[%lu:%lu:%lu]等到了segloc:%lu的merge_cnt变为%u，当前值为%u，retry:%d",
            //             cli_id, coro_id, this->key_num, segloc, target_merge_cnt, meta.merge_cnt, count);
            // }
            seg_meta[segloc].sign = my_fetch_meta->sign;
            if (my_fetch_meta->local_depth > dir->segs[segloc].local_depth)
            {
                co_await check_gd(segloc, false, true);
                uint64_t new_segloc = get_seg_loc(pattern, dir->global_depth);
                new_segloc %= (1 << dir->segs[new_segloc].local_depth);
                if (!need_retry && segloc != new_segloc)
                {
                    need_retry = true;
                    // log_err("[%lu:%lu:%lu]远端分裂了！remote_local_depth:%lu>local_depth:%lu，segloc:%lu->%lu，FAA地址:%lx",
                    //         cli_id, coro_id, this->key_num, meta.local_depth, dir->segs[segloc].local_depth, segloc, new_segloc, segptr + sizeof(uint64_t));
                }
                if (!need_retry) tmp->local_depth = dir->segs[segloc].local_depth; // 如果不需要重试，则更新local_depth，避免在合并时去除过时条目
            }
        }
        // 如果是远端分裂了，可能需要重试。重新计算uint64_t segloc = get_seg_loc(pattern, dir->global_depth);，如果和目前的segloc不同
        // 则需要重新发送到最新的segloc。因为已经获取了旧segloc的ticket，需要先结束本次写入：先加cnt（Slot可以不写，会因为depth不匹配被过滤），如果cnt加到了上限
        // 还需要触发合并。之后goto Retry重试写入。
#else
            // RTT1. send slot
            uint32_t *srq_num_ptr = nullptr;
        while (!seg_meta[segloc].srq_num)
        {
            if (!srq_num_ptr) srq_num_ptr = (uint32_t *)alloc.alloc(sizeof(uint32_t));
            uint64_t srq_num_offset = segptr + 4 * sizeof(uint64_t) + FP_BITMAP_LENGTH * sizeof(FpBitmapType);
#if CORO_DEBUG
            co_await conn->read(srq_num_offset, seg_rmr.rkey, srq_num_ptr, sizeof(uint32_t), lmr->lkey,
                                std::source_location::current(),
                                std::format("[{}:{}]segloc:{}读取srq_num地址{}", cli_id, coro_id, segloc, srq_num_offset));
#else
            co_await conn->read(srq_num_offset, seg_rmr.rkey, srq_num_ptr, sizeof(uint32_t), lmr->lkey);
#endif
            seg_meta[segloc].srq_num = *srq_num_ptr;
        }
        assert_require(seg_meta[segloc].srq_num > 0);
        {
#if SPLIT_LOCAL_LOCK
            std::shared_lock<std::shared_mutex> lock(segloc_locks[segloc]); // 上锁，避免线程写入一个正在合并的段
#endif
            xrc_conn->dev.send_semaphore.acquire(); // 每个RNIC设备的outstanding request数量有限，超出会导致IBV_WC_RETRY_EXC_ERR
            // 目前只限制了SEND的，其他READ/WRITE等请求没有限制，后续可以在do_send处获取信号量并在poll_cq处释放，保证不出错
            auto send_slot = xrc_conn->send(tmp, sizeof(Slot), lmr->lkey, seg_meta[segloc].srq_num);
#if SPLIT_LOCAL_LOCK
            lock.unlock(); // 解锁
#endif            
            // TODO: 如果send等待超过20us则上独占锁，因为其他CN可能正在合并这个段
            // std::this_thread::sleep_for(std::chrono::microseconds(20));
            // if (!send_slot.await_ready()) {
            //     // std::unique_lock<std::shared_mutex> lock(segloc_locks[segloc]); // 上锁，避免更多线程写入一个正在合并的段
            //     co_await std::move(send_slot);
            //     // 解锁
            // } else       
            co_await std::move(send_slot);
            xrc_conn->dev.send_semaphore.release();
        }
#endif

        // RTT2. write fp bitmap
#if LARGER_FP_FILTER_GRANULARITY
#if EMBED_FULL_KEY
        auto bit_loc = get_fp_bit(fp(pattern), fp2(pattern));
#else
        auto bit_loc = get_fp_bit(tmp->fp, tmp->fp_2);
#endif
#if CACHE_FILTER
        CacheSlot *my_cache_slot = (CacheSlot *)alloc.alloc(sizeof(CacheSlot));
        my_cache_slot->len = tmp->len;
        my_cache_slot->invalid = 0;
        my_cache_slot->dep = tmp->dep;
        my_cache_slot->offset = tmp->offset;
        my_cache_slot->fp_2 = tmp->fp_2;
        uint64_t fetch_cache_slot_num = 0;
        auto write_fp_bitmap = conn->cas(segptr + 4 * sizeof(uint64_t) + bit_loc * sizeof(FpBitmapType), seg_rmr.rkey, fetch_cache_slot_num, *(uint64_t *)my_cache_slot);
#else
        seg_meta[segloc].fp_bitmap[bit_loc] = 1;
        CurSegMeta *my_seg_meta = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta));
        my_seg_meta->fp_bitmap[bit_loc] = 1;
        auto write_fp_bitmap = conn->write(segptr + 4 * sizeof(uint64_t) + bit_loc * sizeof(FpBitmapType), seg_rmr.rkey, my_seg_meta->fp_bitmap + bit_loc, sizeof(FpBitmapType), lmr->lkey);
#endif
#endif
        // RTT2. FAA slot_cnt
        auto faa_slot_cnt = conn->fetch_add(segptr + sizeof(uint64_t), seg_rmr.rkey, fetch, SLOT_CNT_INC);

#if USE_TICKET_HASH
        // RTT2. write slot
        co_await conn->write(segptr + sizeof(uint64_t) + sizeof(CurSegMeta) + (meta.slot_ticket % SLOT_PER_SEG) * sizeof(Slot), seg_rmr.rkey, tmp, sizeof(Slot), lmr->lkey); // TODO: can delay
#endif
        co_await std::move(write_fp_bitmap); // TODO: can delay
#if CACHE_FILTER
        CacheSlot fetch_cache_slot = *reinterpret_cast<CacheSlot *>(&fetch_cache_slot_num);
        if (fetch_cache_slot.get_num() == 1)
        {
            my_cache_slot->invalid = 1; // 设置为无效
            auto write_cache_slot = conn->cas(segptr + 4 * sizeof(uint64_t) + bit_loc * sizeof(FpBitmapType), seg_rmr.rkey, fetch_cache_slot_num, *(uint64_t *)my_cache_slot);
            co_await std::move(write_cache_slot); // TODO: 可以不等待完成
        }
#endif
        co_await std::move(faa_slot_cnt); 
        // print_fetch_meta(fetch, std::format("增加segloc:{}的slot_cnt", segloc), SLOT_CNT_INC);
        meta = *reinterpret_cast<FetchMeta *>(&fetch);
        seg_meta[segloc].sign = meta.sign;
        seg_meta[segloc].slot_cnt = (meta.slot_cnt + 1) % SLOT_PER_SEG;

        if (meta.local_depth > dir->segs[segloc].local_depth)
        {
            // tmp->print(std::format("[{}:{}:{}]远端分裂了！remote_local_depth:{}>local_depth:{}，本次写入作废！需要重写！segloc:{}，FAA地址:{}", cli_id, coro_id, this->key_num, meta.local_depth, dir->segs[segloc].local_depth, segloc, segptr + sizeof(uint64_t)));
            co_await check_gd(segloc, false, true);
#if USE_TICKET_HASH
            uint64_t new_segloc = get_seg_loc(pattern, dir->global_depth);
            new_segloc %= (1 << dir->segs[new_segloc].local_depth);
            if (!need_retry && segloc != new_segloc)
            {
                need_retry = true;
                // log_err("[%lu:%lu:%lu]远端分裂了！remote_local_depth:%lu>local_depth:%lu，segloc:%lu->%lu，FAA地址:%lx",
                //         cli_id, coro_id, this->key_num, meta.local_depth, dir->segs[segloc].local_depth, segloc, new_segloc, segptr + sizeof(uint64_t));
            }
#else
            need_retry = true;
#endif
        }
        // check if need split
        if (seg_meta[segloc].slot_cnt == 0)
        {
            {
#if SPLIT_LOCAL_LOCK
                std::unique_lock<std::shared_mutex> write_lock(segloc_locks[segloc], std::try_to_lock);
#endif
                CurSegMeta *my_seg_meta = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta)); // use seg_meta?
                co_await conn->read(segptr + sizeof(uint64_t), seg_rmr.rkey, my_seg_meta, 3 * sizeof(uint64_t), lmr->lkey); // don't read bitmap
                this->offset[segloc].main_seg_ptr = my_seg_meta->main_seg_ptr;
                seg_meta[segloc].local_depth = my_seg_meta->local_depth; // 顺便同步元数据，但因为seg_meta不是用alloc.alloc分配的，不能直接读过去
                seg_meta[segloc].main_seg_ptr = my_seg_meta->main_seg_ptr;
                seg_meta[segloc].main_seg_len = my_seg_meta->main_seg_len; // IMPORTANT: 相比dir->segs[segloc]，seg_meta[segloc]多了srq_num和fp_bitmap
                dir->segs[segloc].local_depth = my_seg_meta->local_depth;
                dir->segs[segloc].main_seg_ptr = my_seg_meta->main_seg_ptr;
                #if TEST_SEG_SIZE
                dir->segs[segloc].main_seg_len = my_seg_meta->main_seg_len;
                #endif
                co_await Split(segloc, segptr, my_seg_meta);
            }
            if (need_retry) goto Retry;
            perf.push_insert();
            sum_cost.end_insert();
            sum_cost.push_retry_cnt(retry_cnt);
            co_return;
        }
        if (need_retry) goto Retry;

        perf.push_insert();
        sum_cost.end_insert();
        sum_cost.push_retry_cnt(retry_cnt);
    }

    task<> Client::update(Slice *key, Slice *value) // FIXME: 目前无法在预先插入(load_num)后更新
    {
        // 日志结构顺序写入的设计中，update操作等同于insert操作
        co_await insert(key, value);
        co_return;
    }

    task<> Client::Split(uint64_t seg_loc, uintptr_t seg_ptr, CurSegMeta *old_seg_meta)
    {
        log_merge("[%lu:%lu:%lu]开始分裂/合并，seg_loc:%lu", cli_id, coro_id, this->key_num, seg_loc);
        sum_cost.start_merge();
        sum_cost.start_split();
        uint64_t local_depth = old_seg_meta->local_depth;
        uint64_t global_depth = dir->global_depth;
        uint64_t main_seg_ptr = old_seg_meta->main_seg_ptr;
        // 1. Read CurSeg
        CurSeg *cur_seg = (CurSeg *)alloc.alloc(sizeof(CurSeg));
        co_await conn->read(seg_ptr, seg_rmr.rkey, cur_seg, sizeof(CurSeg), lmr->lkey); // TODO: 可以不读filter

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
        if (main_seg_size)
            co_await conn->read(dir->segs[seg_loc].main_seg_ptr, seg_rmr.rkey, main_seg, main_seg_size, lmr->lkey);

        // 3. Sort Segment
        MainSeg *new_main_seg = (MainSeg *)alloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG);
#if READ_FULL_KEY_ON_FP_COLLISION
#if EMBED_FULL_KEY
        uint64_t new_seg_len = co_await merge_insert(cur_seg->slots, SLOT_PER_SEG, main_seg->slots, dir->segs[seg_loc].main_seg_len, new_main_seg->slots, cur_seg->seg_meta.local_depth, true);
#else
        static thread_local int counter = 0;
        bool dedup = false;
        if (local_depth >= MAX_DEPTH - 1 || SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len >= MAX_MAIN_SIZE)
            dedup = true;
        else if (counter++ % DEDUPLICATE_INTERVAL == 0)
            dedup = true;

        uint64_t new_seg_len = co_await merge_insert(cur_seg->slots, SLOT_PER_SEG, main_seg->slots, dir->segs[seg_loc].main_seg_len, new_main_seg->slots, cur_seg->seg_meta.local_depth, dedup);
#endif
#else
        uint64_t new_seg_len = merge_insert(cur_seg->slots, SLOT_PER_SEG, main_seg->slots, dir->segs[seg_loc].main_seg_len, new_main_seg->slots, cur_seg->seg_meta.local_depth);
#endif  
        FpInfo fp_info[MAX_FP_INFO] = {};
        cal_fpinfo(new_main_seg->slots, new_seg_len, fp_info); // SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len
        bool is_update = SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len != new_seg_len;
        // if (new_seg_len != SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len)
            // log_err("将segloc:%lu/%lu的%d个CurSeg条目合并到%d个MainSeg条目，new_seg_len:%lu", seg_loc, (1ull << global_depth) - 1, SLOT_PER_SEG, dir->segs[seg_loc].main_seg_len, new_seg_len);
        // 4. Split (为了减少协程嵌套层次的开销，这里就不抽象成单独的函数了)
        if (dir->segs[seg_loc].main_seg_len >= MAX_MAIN_SIZE && local_depth < MAX_DEPTH)
        {
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
                    if (!ralloc.ptr(new_main_seg->slots[i].offset)) {
                        log_err("[%lu:%lu:%lu]new_main_seg->slots[%lu]的偏移地址为0，不合法条目?", cli_id, coro_id, this->key_num, i);
                        new_main_seg->slots[i].print("Invalid Slot");
                        // continue;
                    }
                    // if dep_off == 3 (Have consumed all info in dep bits), read && construct new dep
#if EMBED_FULL_KEY // fp是key的前8字节，fp_2是key的后8字节，由此构造kv_block并计算hash
                    kv_block->k_len = 2 * sizeof(uint64_t);
                    memcpy(kv_block->data, &new_main_seg->slots[i].fp, sizeof(uint64_t));
                    memcpy(kv_block->data + sizeof(uint64_t), &new_main_seg->slots[i].fp_2, sizeof(uint64_t));
                    pattern = (uint64_t)hash(kv_block->data, kv_block->k_len);
#else
#ifdef TOO_LARGE_KV
                    co_await conn->read(ralloc.ptr(new_main_seg->slots[i].offset), seg_rmr.rkey, kv_block, this->kv_block_len, lmr->lkey);
#else
                    co_await conn->read(ralloc.ptr(new_main_seg->slots[i].offset), seg_rmr.rkey, kv_block, new_main_seg->slots[i].len * ALIGNED_SIZE, lmr->lkey);
#endif
                    // 验证KVBlock合法性
                    if (!kv_block->is_valid()) {
                        log_err("[%lu:%lu:%lu]new_main_seg->slots[%lu]的KV条目不合法（写入溢出了？），k_len:%lu v_len:%lu", cli_id, coro_id, this->key_num, i, kv_block->k_len, kv_block->v_len);
                        new_main_seg->slots[i].print("Invalid Slot");
                        // continue;
                    }
                    pattern = (uint64_t)hash(kv_block->data, kv_block->k_len);
#endif
                    new_main_seg->slots[i].dep = pattern >> (local_depth + 1);
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
#if REUSE_MAIN_SEG
            uintptr_t new_main_ptr1 = dir->segs[seg_loc].main_seg_ptr;
            if (new_main_ptr1 == 0) {
                new_main_ptr1 = ralloc.alloc(MAX_MAIN_SEG_SIZE, true);
                dir->segs[seg_loc].main_seg_ptr = new_main_ptr1;
            }
            uintptr_t new_main_ptr2 = ralloc.alloc(MAX_MAIN_SEG_SIZE, true);
#else
            uintptr_t new_main_ptr1 = ralloc.alloc(sizeof(Slot) * off1);
            uintptr_t new_main_ptr2 = ralloc.alloc(sizeof(Slot) * off2);
#endif

            // a. 同步远端global depth, 确认split类型
            co_await check_gd();

            // b. update dir
            uint64_t stride = (1llu) << (dir->global_depth - local_depth);
            uint64_t cur_seg_loc;
            uint64_t first_seg_loc = seg_loc & ((1ull << local_depth) - 1);
            uintptr_t cur_seg_ptr;
            uint64_t dir_size = (1 << dir->global_depth); // 为了兼容同步发生的Global Split
            uint64_t dir_bound = 2;
            if (local_depth == dir->global_depth)
            {
                stride = 2; // 为了global split正确触发
                dir_bound = 1;
            }

            uint64_t first_new_seg_loc = UINT64_MAX, first_original_seg_loc = UINT64_MAX;

            for (uint64_t i = 0; i < dir_bound; i++)
            {
                uint64_t offset = i * dir_size;
                for (uint64_t i = 0; i < stride; i++)
                {
                    cur_seg_loc = (i << local_depth) | first_seg_loc;
                    if (i & 1)
                    {
                        // Local SegOffset
                        this->offset[cur_seg_loc + offset].offset = 0;
                        this->offset[cur_seg_loc + offset].main_seg_ptr = new_main_ptr2;
                        // DirEntry
                        // log_err("更新dir->segs[%lu+%lu=%lu].cur_seg_ptr为新的seg_ptr: %lx->%lx", cur_seg_loc, offset, cur_seg_loc + offset, dir->segs[cur_seg_loc + offset].cur_seg_ptr, new_cur_ptr);
                        dir->segs[cur_seg_loc + offset].cur_seg_ptr = new_cur_ptr;
                        dir->segs[cur_seg_loc + offset].main_seg_ptr = new_main_ptr2;
                        dir->segs[cur_seg_loc + offset].main_seg_len = off2;
                        memcpy(dir->segs[cur_seg_loc + offset].fp, fp_info2, sizeof(FpInfo) * MAX_FP_INFO);
                        if (first_new_seg_loc == UINT64_MAX)
                            first_new_seg_loc = cur_seg_loc + offset;
                    }
                    else
                    {
                        // Local SegOffset
                        this->offset[cur_seg_loc + offset].offset = 0;
                        this->offset[cur_seg_loc + offset].main_seg_ptr = new_main_ptr1;
                        // DirEntry
                        // log_err("更新dir->segs[%lu+%lu=%lu].cur_seg_ptr为旧的seg_ptr: %lx->%lx", cur_seg_loc, offset, cur_seg_loc + offset, dir->segs[cur_seg_loc + offset].cur_seg_ptr, seg_ptr);
                        dir->segs[cur_seg_loc + offset].cur_seg_ptr = seg_ptr; // 可能dir中的多个条目指向同一个seg
                        dir->segs[cur_seg_loc + offset].main_seg_ptr = new_main_ptr1;
                        dir->segs[cur_seg_loc + offset].main_seg_len = off1;
                        memcpy(dir->segs[cur_seg_loc + offset].fp, fp_info1, sizeof(FpInfo) * MAX_FP_INFO);
                        if (first_original_seg_loc == UINT64_MAX)
                            first_original_seg_loc = cur_seg_loc + offset;
                    }
                    dir->segs[cur_seg_loc].local_depth = local_depth + 1;
                    // log_err("[%lu:%lu:%lu]segloc:%lu分裂，local_depth:%lu->%lu", cli_id, coro_id, this->key_num, cur_seg_loc, local_depth, local_depth + 1);
                    cur_seg_ptr = seg_rmr.raddr + sizeof(uint64_t) + (cur_seg_loc + offset) * sizeof(DirEntry);

                    // Update DirEntry
                    co_await conn->write(cur_seg_ptr, seg_rmr.rkey, &dir->segs[cur_seg_loc + offset], sizeof(DirEntry), lmr->lkey);

                    // log_err("[%lu:%lu:%lu]Segment分裂, first_original_seg_loc:%lu, first_new_seg_loc:%lu, depth:%lu->%lu", cli_id, coro_id, this->key_num, first_original_seg_loc, first_new_seg_loc, local_depth, local_depth + 1);
                    if (local_depth == dir->global_depth){
                    //     // global
                        log_err("[%lu:%lu:%lu]Global SPlit At segloc:%lu depth:%lu to :%lu with new seg_ptr:%lx new_main_seg_ptr:%lx", cli_id, coro_id, this->key_num, cur_seg_loc+offset, local_depth, local_depth + 1, i & 1 ? new_cur_ptr:seg_ptr,i & 1 ?new_main_ptr2:new_main_ptr1);
                    }//else{
                    //     // local
                    //     log_err("[%lu:%lu:%lu]Local SPlit At segloc:%lu depth:%lu to :%lu with new seg_ptr:%lx new main_seg_ptr:%lx", cli_id, coro_id, this->key_num, cur_seg_loc+offset, local_depth, local_depth + 1, i & 1 ? new_cur_ptr:seg_ptr, i & 1 ? new_main_ptr2:new_main_ptr1);
                    // }
                }
            }
            // Update Global Depth
            if (local_depth == dir->global_depth)
            {
                // log_err("local_depth达到上限，更新global depth:%lu -> %lu, max segs:%d to %d",dir->global_depth,dir->global_depth+1, (1 << global_depth), (1 << (global_depth+1))); // at first we have 16 segs
                dir->global_depth++;
                co_await conn->write(seg_rmr.raddr, seg_rmr.rkey, &dir->global_depth, sizeof(uint64_t), lmr->lkey);
            }

            // 4.3 Write New MainSeg to Remote // TODO: 还需要在远端更新srqn
            // a. New CurSeg && New MainSeg
            std::bitset<32> new_segloc_bits((uint32_t)first_new_seg_loc); // new_seg_indices[0]
            new_segloc_bits.set(31);

            CurSeg *new_cur_seg = (CurSeg *)alloc.alloc(sizeof(CurSeg));
            memset(new_cur_seg, 0, sizeof(CurSeg));
            new_cur_seg->seg_meta.local_depth = local_depth + 1; // IMPORTANT: local_depth+1，这是让客户端感知到split的关键
            new_cur_seg->seg_meta.sign = 1;
            new_cur_seg->seg_meta.main_seg_ptr = new_main_ptr2;
            new_cur_seg->seg_meta.main_seg_len = off2;
#if USE_TICKET_HASH
            wo_wait_conn->pure_write(new_cur_seg->seg_meta.main_seg_ptr, seg_rmr.rkey, new_seg_2, sizeof(Slot) * off2, lmr->lkey);
#else
            wo_wait_conn->pure_write_with_imm(new_cur_seg->seg_meta.main_seg_ptr, seg_rmr.rkey, new_seg_2, sizeof(Slot) * off2, lmr->lkey, new_segloc_bits.to_ulong()); // 提醒为新的Segment创建SRQ，并更新SRQN
#endif
            co_await conn->write(new_cur_ptr, seg_rmr.rkey, new_cur_seg, sizeof(CurSeg), lmr->lkey);
            // new_cur_seg->seg_meta.print(std::format("[{}:{}]新Segment的CurSeg->seg_meta", cli_id, coro_id));
            // b. new main_seg for old
            cur_seg->seg_meta.main_seg_ptr = new_main_ptr1;
            cur_seg->seg_meta.main_seg_len = off1;
            cur_seg->seg_meta.local_depth = local_depth + 1; // IMPORTANT: local_depth+1，这是让客户端感知到split的关键
            // cur_seg->seg_meta.sign = !cur_seg->seg_meta.sign; // 对old cur_seg的清空放到最后?保证同步。
            memset(cur_seg->seg_meta.fp_bitmap, 0, sizeof(FpBitmapType) * FP_BITMAP_LENGTH);
            wo_wait_conn->pure_write(cur_seg->seg_meta.main_seg_ptr, seg_rmr.rkey, new_seg_1, sizeof(Slot) * off1, lmr->lkey);
#if USE_TICKET_HASH
            auto write_remaining_meta = conn->write(seg_ptr + 2 * sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 2, sizeof(CurSegMeta) - sizeof(uint64_t), lmr->lkey);
            // co_await conn->fetch_add(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, fetch, LOCAL_DEPTH_INC); // TODO: 还有slot_cnt，去掉下面的放到这里
            uint64_t fetch = 0;
            if (cur_seg->seg_meta.merge_cnt + 1 == CLEAR_MERGE_CNT_PERIOD)
            {
                uint64_t addval = LOCAL_DEPTH_INC - (int64_t)(CLEAR_MERGE_CNT_PERIOD - 1) * MERGE_CNT_INC - (int64_t)(CLEAR_MERGE_CNT_PERIOD * SLOT_PER_SEG) * SLOT_TICKET_INC - (int64_t)(SLOT_PER_SEG)*SLOT_CNT_INC;
                co_await conn->fetch_add(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, fetch, addval);
                // FetchMeta meta = *reinterpret_cast<FetchMeta *>(&fetch);
                // print_fetch_meta(fetch, std::format("增加segloc:{}的local_depth，清零merge_cnt，减少slot_ticket，并清零slot_cnt", seg_loc), addval);
            }
            else
            {
                co_await conn->fetch_add(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, fetch, LOCAL_DEPTH_INC + MERGE_CNT_INC - (int64_t)(SLOT_PER_SEG)*SLOT_CNT_INC);
                // FetchMeta meta = *reinterpret_cast<FetchMeta *>(&fetch);
                // print_fetch_meta(fetch, std::format("增加segloc:{}的local_depth&merge_cnt并清零slot_cnt", seg_loc), LOCAL_DEPTH_INC + MERGE_CNT_INC - (int64_t)(SLOT_PER_SEG)*SLOT_CNT_INC);
            }
            co_await std::move(write_remaining_meta);
#else
            co_await conn->write(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(CurSegMeta), lmr->lkey);
#endif
            
            // cur_seg->seg_meta.print(std::format("[{}:{}]旧Segment的CurSeg->seg_meta", cli_id, coro_id));

            // 4.4 Change Sign (Equal to unlock this segment)
            cur_seg->seg_meta.sign = !cur_seg->seg_meta.sign;
            cur_seg->seg_meta.slot_cnt = 0;
#if !USE_TICKET_HASH
#if MODIFIED
            log_merge("向远端地址%lx写入1，提醒为旧Segment发布RECV", seg_ptr + sizeof(uint64_t));
            wo_wait_conn->pure_write_with_imm(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(uint64_t), lmr->lkey, first_original_seg_loc); // 为旧Segment发布RECV，新Segment会在第一次连接时发布RECV
            log_merge("提醒为segloc:%lu htonl:%lu旧的Segment发布RECV完成, main_seg_ptr:%lx, main_seg_len:%lu", first_original_seg_loc, htonl(first_original_seg_loc), new_main_ptr1, off1);
#else
            co_await wo_wait_conn->write(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(uint64_t), lmr->lkey);
#endif
#endif
            sum_cost.end_split();
            // dir->print(std::format("[{}:{}]分裂之后", cli_id, coro_id));
            co_return;
        }

        // 5. Merge
        // 5.1 Write New MainSeg to Remote && Update CurSegMeta
        // a. write main segment

#if REUSE_MAIN_SEG
        uintptr_t new_main_ptr = dir->segs[seg_loc].main_seg_ptr;
        if (new_main_ptr == 0) {
            new_main_ptr = ralloc.alloc(MAX_MAIN_SEG_SIZE, true); // 大小是2 * MAX_MAIN_SIZE，因为还需要额外存储SLOT_PER_SEG个Slot
            dir->segs[seg_loc].main_seg_ptr = new_main_ptr; // 更新dir中的main_seg_ptr
        }
#else
        uintptr_t new_main_ptr = ralloc.alloc(new_seg_len * sizeof(Slot), true); // IMPORTANT: 在MN分配new main seg，注意 FIXME: ralloc没有free功能 // main_seg_size + sizeof(Slot) * SLOT_PER_SEG
#endif
        if (sizeof(Slot) * new_seg_len > MAX_MAIN_SEG_SIZE) {
            log_err("[%lu:%lu:%lu]new_main_ptr:%lx, new_seg_len:%lu, offset:%lx, size:%lu > MAX_MAIN_SEG_SIZE:%lu, reallocate...", cli_id, coro_id, this->key_num, new_main_ptr, new_seg_len, ralloc.offset(new_main_ptr), sizeof(Slot) * new_seg_len, MAX_MAIN_SEG_SIZE);
            assert_require(false); // 如果达到depth上限，MainSeg的大小可能超出MAX_MAIN_SEG_SIZE，可以在每次溢出时翻倍容量，目前简单退出
        }
        wo_wait_conn->pure_write(new_main_ptr, seg_rmr.rkey, new_main_seg->slots, sizeof(Slot) * new_seg_len, lmr->lkey);

        // b. Update MainSegPtr/Len and fp_bitmap
        cur_seg->seg_meta.main_seg_ptr = new_main_ptr;
        cur_seg->seg_meta.main_seg_len = new_seg_len; // main_seg_size / sizeof(Slot) + SLOT_PER_SEG;
        this->offset[seg_loc].offset = 0;
        memset(cur_seg->seg_meta.fp_bitmap, 0, sizeof(FpBitmapType) * FP_BITMAP_LENGTH);
        co_await conn->write(seg_ptr + 2 * sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 2, sizeof(CurSegMeta) - sizeof(uint64_t), lmr->lkey);

        // 5.2 Update new-main-ptr for DirEntries
        if (dir->global_depth < local_depth) co_await check_gd();
        assert_require(dir->global_depth >= local_depth);
        uint64_t stride = (1llu) << (dir->global_depth - local_depth);
        uint64_t cur_seg_loc;
        uint64_t first_seg_loc = seg_loc & ((1ull << local_depth) - 1);
        uintptr_t dentry_ptr;
        uint64_t dir_size = (1 << dir->global_depth); // 为了兼容同步发生的Global Split
        for (uint64_t i = 0; i < 2; i++)
        { // 同一个Segment可以对应多个DirEntry
            uint64_t offset = i * dir_size;
            for (uint64_t i = 0; i < stride; i++)
            {
                cur_seg_loc = (i << local_depth) | first_seg_loc;
                // Local SegOffset
                this->offset[cur_seg_loc + offset].offset = 0;
                this->offset[cur_seg_loc + offset].main_seg_ptr = new_main_ptr;
                // DirEntry
                dir->segs[cur_seg_loc + offset].local_depth = cur_seg->seg_meta.local_depth;
                dir->segs[cur_seg_loc + offset].cur_seg_ptr = seg_ptr; // 这里会在没分裂时就更新超出depth的dir
                dir->segs[cur_seg_loc + offset].main_seg_ptr = new_main_ptr;
                dir->segs[cur_seg_loc + offset].main_seg_len = new_seg_len;
                memcpy(dir->segs[cur_seg_loc + offset].fp, fp_info, sizeof(FpInfo) * MAX_FP_INFO);
                dentry_ptr = seg_rmr.raddr + sizeof(uint64_t) + (cur_seg_loc + offset) * sizeof(DirEntry);
                // Update
                // 暂时还是co_await吧
                co_await conn->write(dentry_ptr, seg_rmr.rkey, &dir->segs[cur_seg_loc + offset], sizeof(DirEntry), lmr->lkey);
                // conn->pure_write(dentry_ptr, seg_rmr.rkey,&dir->segs[cur_seg_loc+offset], sizeof(DirEntry), lmr->lkey);
                // log_err("[%lu:%lu:%lu]Merge At segloc:%lu depth:%lu with old_main_ptr:%lx new_main_ptr:%lx",cli_id,coro_id,this->key_num,cur_seg_loc+offset,local_depth,main_seg_ptr,new_main_ptr);
            }
        }

        // 5.3 Change Sign (Equal to unlock this segment)
#if USE_TICKET_HASH
        uint64_t fetch = 0;
        if (cur_seg->seg_meta.merge_cnt + 1 == CLEAR_MERGE_CNT_PERIOD)
        {
            // 如果cur_seg->seg_meta.merge_cnt在本次FAA后会上升到128，则改为将merge_cnt减少127，同时将slot_ticket减少128*SLOT_PER_SEG（SLOT_CNT_INC还是和原来一样减少SLOT_PER_SEG）
            // merge_cnt减少CLEAR_MERGE_CNT_PERIOD-1，slot_ticket减少CLEAR_MERGE_CNT_PERIOD * SLOT_PER_SEG，清零slot_cnt
            uint64_t addval = -(int64_t)(CLEAR_MERGE_CNT_PERIOD - 1) * MERGE_CNT_INC - (int64_t)(CLEAR_MERGE_CNT_PERIOD * SLOT_PER_SEG) * SLOT_TICKET_INC - (int64_t)(SLOT_PER_SEG)*SLOT_CNT_INC;
            co_await conn->fetch_add(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, fetch, addval);
            // FetchMeta meta = *reinterpret_cast<FetchMeta *>(&fetch);
            // print_fetch_meta(fetch, std::format("[{}:{}:{}]清零segloc:{}的merge_cnt，减少slot_ticket，并清零slot_cnt", cli_id, coro_id, this->key_num, seg_loc), addval);
        }
        else
        {
            co_await conn->fetch_add(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, fetch, MERGE_CNT_INC - (int64_t)(SLOT_PER_SEG)*SLOT_CNT_INC);
            // FetchMeta meta = *reinterpret_cast<FetchMeta *>(&fetch);
            // print_fetch_meta(fetch, std::format("[{}:{}:{}]增加segloc:{}的merge_cnt并清零slot_cnt", cli_id, coro_id, this->key_num, seg_loc), MERGE_CNT_INC - (int64_t)(SLOT_PER_SEG)*SLOT_CNT_INC);
        }
       
#else
        cur_seg->seg_meta.sign = !cur_seg->seg_meta.sign;
        cur_seg->seg_meta.slot_cnt = 0;
#if MODIFIED
        log_merge("segloc:%lu合并了，准备向远端地址%lx写入1，提醒为旧Segment发布RECV", seg_loc, seg_ptr + sizeof(uint64_t));
        wo_wait_conn->pure_write_with_imm(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(uint64_t), lmr->lkey, seg_loc); // 为旧Segment发布RECV，此处的WRITE和上面的相同，无实际作用，只是为了触发信号
        log_merge("合并后提醒为segloc:%lu旧Segment发布RECV完成，main_seg_ptr:%lx,main_seg_len:%lu", seg_loc, new_main_ptr, new_seg_len);
#else
        co_await wo_wait_conn->write(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(uint64_t), lmr->lkey);
#endif
#endif
        sum_cost.end_merge();
    }

#if READ_FULL_KEY_ON_FP_COLLISION // correct but not efficient
    task<uint64_t> Client::merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg, uint64_t local_depth, bool dedup)
    {
        if (dedup) {
            // 定义排序用的结构体
            struct SortItem
            {
#if EMBED_FULL_KEY
                uint64_t fp;
                uint64_t fp_2;
#else
                uint8_t fp;
                uint8_t fp_2;
                std::string full_key;
                bool need_read; // 是否需要读取完整key
#endif
                bool from_data;    // true:来自data, false:来自old_seg
                uint64_t index;    // 在原始数组中的索引
                bool is_delete;    // 是否是删除条目(v_len=0)
                Slot *slot_ptr;    // 指向原始Slot的指针

                // 排序规则
                bool operator<(const SortItem &other) const
                {
                    if (fp != other.fp)
                        return fp < other.fp;
                    if (fp_2 != other.fp_2)
                        return fp_2 < other.fp_2;
#if !EMBED_FULL_KEY
                    if (need_read)
                    {
                        // 如果需要读取key，则比较full_key
                        if (full_key != other.full_key)
                            return full_key < other.full_key;
                        // 相同key时，优先保留较新的(data优先，索引大的优先)
                        if (from_data != other.from_data)
                            return from_data;
                        return index > other.index; // 对于相同数组，保留索引更大的(较新的)
                    }
#endif
                    // 如果不需要读取key，则按来源和索引排序
                    if (from_data != other.from_data)
                        return from_data;
                    return index < other.index;
                }
            };

#if !EMBED_FULL_KEY
            // 1. 统计每个(fp,fp_2)组合的出现次数
            std::map<std::pair<uint8_t, uint8_t>, int> fp_counts;

            for (uint64_t i = 0; i < len; i++)
            {
                if (data[i].local_depth == local_depth)
                    fp_counts[{data[i].fp, data[i].fp_2}]++;
            }

            for (uint64_t i = 0; i < old_seg_len; i++)
                fp_counts[{old_seg[i].fp, old_seg[i].fp_2}]++;
#endif

            // 2. 创建排序项
            std::vector<SortItem> sort_items;
            sort_items.reserve(len + old_seg_len);

            // 添加data中的项
            for (uint64_t i = 0; i < len; i++)
            {
                if (data[i].local_depth == local_depth && data[i].is_valid())
                {
                    auto fp_key = std::make_pair(data[i].fp, data[i].fp_2);
                    SortItem item{
                        .fp = data[i].fp,
                        .fp_2 = data[i].fp_2,
                        // .full_key = 0,
#if !EMBED_FULL_KEY
                        .need_read = fp_counts[fp_key] > 1, // 如果有多个相同(fp,fp_2)则需要读取key
#endif
                        .from_data = true,
                        .index = i,
                        .is_delete = false,                 // 初始时不知道是否是删除条目
                        .slot_ptr = &data[i]};
                    sort_items.push_back(item);
                }
            }

            // 添加old_seg中的项
            for (uint64_t i = 0; i < old_seg_len; i++)
            {
                if (old_seg[i].is_valid())
                {
                    auto fp_key = std::make_pair(old_seg[i].fp, old_seg[i].fp_2);
                    SortItem item{
                        .fp = old_seg[i].fp,
                        .fp_2 = old_seg[i].fp_2,
                        // .full_key = 0,
#if !EMBED_FULL_KEY
                        .need_read = fp_counts[fp_key] > 1, // 如果有多个相同(fp,fp_2)则需要读取key
#endif
                        .from_data = false,
                        .index = i,
                        .is_delete = false, // 初始时不知道是否是删除条目
                        .slot_ptr = &old_seg[i]};
                    sort_items.push_back(item);
                }
            }

            // 3. 为需要读取完整key的项读取key并计算hash
#if !EMBED_FULL_KEY
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
                        memset(kv, 0, slot->len * ALIGNED_SIZE);
#if CORO_DEBUG
                        co_await conn->read(offset, seg_rmr.rkey, kv, slot->len * ALIGNED_SIZE, lmr->lkey, std::source_location::current(), std::format("为Slot:{}读取KVBlock, 地址:{}, 长度: {}", slot->to_string(), offset, slot->len * ALIGNED_SIZE));
#else
                        co_await conn->read(offset, seg_rmr.rkey, kv, slot->len * ALIGNED_SIZE, lmr->lkey);
#endif
                        // 验证KVBlock合法性
                        if (!kv->is_valid())
                        {
                            kv->k_len = 8; kv->v_len = 0; // 直接删掉
                        }
                        kv_cache[offset] = kv;
                    }

                    KVBlock *kv = kv_cache[offset];
                    item.is_delete = kv->v_len == 0;
                    item.full_key = std::string(kv->data, kv->k_len);
                }
            }
#endif

            // 4. 根据定义的规则排序
            std::sort(sort_items.begin(), sort_items.end());

            // 5. 处理排序后的结果，相同key只保留最新的，且处理删除标记
            uint64_t new_seg_len = 0;
            uint64_t last_fp = 0xFFFF; // 不可能的值
            uint64_t last_fp2 = 0xFFFF;
#if EMBED_FULL_KEY
            bool first = true; // 用于处理第一个元素的特殊情况
#else
            std::string last_full_key;
#endif

            for (size_t i = 0; i < sort_items.size(); i++)
            {
                const auto &item = sort_items[i];
#if EMBED_FULL_KEY // fp和fp_2匹配就跳过
                if (!first && item.fp == last_fp && item.fp_2 == last_fp2) continue;
                last_fp = item.fp;
                last_fp2 = item.fp_2;
                first = false;
#else // 如果需要检查key且与上一个相同则跳过
                if (item.need_read && item.fp == last_fp && item.fp_2 == last_fp2 && item.full_key == last_full_key) continue;
                if (item.need_read)
                {
                    last_fp = item.fp;
                    last_fp2 = item.fp_2;
                    last_full_key = item.full_key;
                }
#endif
                if (!item.is_delete) // 将Slot添加到结果数组
                    new_seg[new_seg_len++] = *item.slot_ptr;
                // else log_err("[%lu:%lu:%lu]删除了一个条目", cli_id, coro_id, this->key_num);
            }
            // log_err("[%lu:%lu:%lu]合并后的seg长度从%lu减少到%lu", cli_id, coro_id, this->key_num, len + old_seg_len, new_seg_len);
            co_return new_seg_len;
        } else {
            std::sort(data, data + len);
            uint8_t sign = data[0].sign;
            int off_1 = 0, off_2 = 0;
            uint64_t new_seg_len = 0;
            if (len && old_seg_len)
            {
                for (uint64_t i = 0; i < len + old_seg_len; i++)
                {
                    if (data[off_1].fp <= old_seg[off_2].fp)
                    {
                        if (data[off_1].local_depth == local_depth)
                            new_seg[new_seg_len++] = data[off_1];
                        off_1++;
                    }
                    else
                    {
                        if (old_seg_len == 0)
                            old_seg[0].print(std::format("[{}:{}:{}]old_seg_len==0", cli_id, coro_id, this->key_num));
                        new_seg[new_seg_len++] = old_seg[off_2];
                        off_2++;
                    }
                    if (off_1 >= len || off_2 >= old_seg_len)
                        break;
                }
            }

            // 处理剩余元素
            if (off_1 < len)
            {
                // memcpy(new_seg + old_seg_len + off_1, data + off_1, (len - off_1) * sizeof(Slot));
                for (uint64_t i = off_1; i < len; i++)
                {
                    if (data[i].local_depth == local_depth)
                        new_seg[new_seg_len++] = data[i];
                    // else data[i].print(std::format("[{}:{}:{}]发现过时条目，local_depth:{} != {}", cli_id, coro_id, this->key_num, data[i].local_depth, local_depth));
                }
            }
            else if (off_2 < old_seg_len)
            {
                for (uint64_t i = off_2; i < old_seg_len; i++)
                    new_seg[new_seg_len++] = old_seg[i];
            }
            // log_err("[%lu:%lu:%lu]合并后的seg长度从%lu减少到%lu", cli_id, coro_id, this->key_num, len + old_seg_len, new_seg_len);
            co_return new_seg_len;
        }
    }
#else
    uint64_t Client::merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg, uint64_t local_depth)
    {
        std::sort(data, data + len);
        uint8_t sign = data[0].sign;
        int off_1 = 0, off_2 = 0;
        uint64_t new_seg_len = 0;
        if (len && old_seg_len) {
            for (uint64_t i = 0; i < len + old_seg_len; i++)
            {
                if (data[off_1].fp <= old_seg[off_2].fp)
                {
                    if (data[off_1].local_depth == local_depth)
                        new_seg[new_seg_len++] = data[off_1];
                    off_1++;
                }
                else
                {
                    if (old_seg_len == 0)
                        old_seg[0].print(std::format("[{}:{}:{}]old_seg_len==0", cli_id, coro_id, this->key_num));
                    new_seg[new_seg_len++] = old_seg[off_2];
                    off_2++;
                }
                if (off_1 >= len || off_2 >= old_seg_len)
                    break;
            }
        }

        // 处理剩余元素
        if (off_1 < len)
        {
            // memcpy(new_seg + old_seg_len + off_1, data + off_1, (len - off_1) * sizeof(Slot));
            for (uint64_t i = off_1; i < len; i++)
            {
                if (data[i].local_depth == local_depth)
                    new_seg[new_seg_len++] = data[i];
                // else data[i].print(std::format("[{}:{}:{}]发现过时条目，local_depth:{} != {}", cli_id, coro_id, this->key_num, data[i].local_depth, local_depth));
            }
        }
        else if (off_2 < old_seg_len)
        {
            for (uint64_t i = off_2; i < old_seg_len; i++)
                new_seg[new_seg_len++] = old_seg[i];
        }
        // log_err("[%lu:%lu:%lu]合并后的seg长度从%lu减少到%lu", cli_id, coro_id, this->key_num, len + old_seg_len, new_seg_len);
        return new_seg_len;
    }
#endif

    task<> Client::remove(Slice *key)
    {
        Slice delete_value;
        delete_value.len = 0;
        delete_value.data = nullptr;
        co_await this->insert(key, &delete_value);
        co_return;
    }

    task<> Client::check_slots(Slot *slots, uint64_t len, std::string desc, std::source_location loc) {
        KVBlock *kv_block = (KVBlock *)alloc.alloc(130 * ALIGNED_SIZE);
        for (uint64_t i = 0; i < len; i++)
        {
            auto &slot = slots[i];
            co_await conn->read(ralloc.ptr(slot.offset), seg_rmr.rkey, kv_block, slot.len * ALIGNED_SIZE, lmr->lkey);
            if (!kv_block->is_valid())
            {
                log_err("在%s:%d的[%s]检查中发现不合法的slots[%lu], offset:%lx, ptr: %lx",
                        loc.file_name(), loc.line(), desc.c_str(),
                        i, slot.offset, ralloc.ptr(slot.offset));
                    // ", loc.file_name(), loc.line(), desc.c_str());
                slot.print(i);
                kv_block->print("不合法的KVBlock");
            }
        }
    }

    task<> Client::check_slot(uint64_t offset, std::string desc, std::source_location loc) {
        KVBlock *kv_block = (KVBlock *)alloc.alloc(130 * ALIGNED_SIZE);
        co_await conn->read(ralloc.ptr(offset), seg_rmr.rkey, kv_block, ALIGNED_SIZE * 130, lmr->lkey);
        if (!kv_block->is_valid() && kv_block->k_len != 0) {
            log_err("在%s:%d的[%s]检查中发现不合法的slot, offset:%lx, ptr: %lx",
                    loc.file_name(), loc.line(), desc.c_str(),
                    offset, ralloc.ptr(offset));
            kv_block->print("不合法的KVBlock");
        }
    }
}