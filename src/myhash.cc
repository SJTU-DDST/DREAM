#include "myhash.h"
#include <arpa/inet.h>

namespace MYHASH
{
#if SPLIT_LOCAL_LOCK
    std::vector<std::shared_mutex> Client::segloc_locks(65536);
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
        if (dir->segs[segloc].local_depth != dir->global_depth)
        {
            uint64_t ancestor_segloc = segloc;
            uint64_t current_depth = dir->global_depth;
            while (current_depth > 0)
            {
                // 向上查找一级
                current_depth--;
                uint64_t parent_segloc = get_seg_loc(pattern, current_depth);
                log_test("[%lu:%lu]向上查找一级，当前深度:%lu，当前段%lu, LD:%lu, 根段%lu, LD:%lu", cli_id, coro_id, current_depth, ancestor_segloc, dir->segs[ancestor_segloc].local_depth, parent_segloc, dir->segs[parent_segloc].local_depth);

                // 检查找到的段的LD是否为当前检查的深度
                if (dir->segs[ancestor_segloc].local_depth == 0) {
                    log_test("[%lu:%lu]段%lu的LD:%lu不存在，继续向上查找根段%lu, LD:%lu", cli_id, coro_id, ancestor_segloc, dir->segs[ancestor_segloc].local_depth, parent_segloc, dir->segs[parent_segloc].local_depth);
                    ancestor_segloc = parent_segloc;
                }
                else if (dir->segs[ancestor_segloc].local_depth == dir->segs[parent_segloc].local_depth && dir->segs[ancestor_segloc] == dir->segs[parent_segloc]) {
                    log_test("[%lu:%lu]段%lu的LD:%lu和根段%lu的LD:%lu匹配，并且是同一个段，优先使用根段", cli_id, coro_id, ancestor_segloc, dir->segs[ancestor_segloc].local_depth, parent_segloc, dir->segs[parent_segloc].local_depth);
                    ancestor_segloc = parent_segloc;
                } else {
                    log_test("[%lu:%lu]段%lu的LD:%lu和根段%lu的LD:%lu不是同一个段，使用这个段", cli_id, coro_id, ancestor_segloc, dir->segs[ancestor_segloc].local_depth, parent_segloc, dir->segs[parent_segloc].local_depth);
                    break;
                }
            }
            segloc = ancestor_segloc;
        }

        uintptr_t segptr = dir->segs[segloc].cur_seg_ptr;
        assert_require(segptr != 0);

        // 4. write slot
        // a. Init Slot
        uint64_t dep = dir->segs[segloc].local_depth - (dir->segs[segloc].local_depth % 4);
        // 去掉before read后现在还没有seg_meta，可以用缓存的dir->segs[segloc].local_depth
        // 这个会影响分裂后去到哪个segment，发送slot时需要带上本地的local_depth，分裂时如果不匹配则读取远端的local_depth
        tmp->dep = pattern >> dep;
        tmp->fp = fp(pattern);
        tmp->len = (kvblock_len + ALIGNED_SIZE - 1) / ALIGNED_SIZE;
        tmp->sign = 0; // 目前sign的作用已经被slot_cnt替代
        tmp->offset = ralloc.offset(kvblock_ptr);
        tmp->fp_2 = fp2(pattern);
        tmp->local_depth = dir->segs[segloc].local_depth; // 本地的local_depth，用于合并时去除过时条目

        // a2. send slot
        static int send_cnt = 0;
        perf.start_perf();

        if (seg_meta.find(segloc) == seg_meta.end())
        {
            seg_meta[segloc] = CurSegMeta();
            memset(&seg_meta[segloc], 0, sizeof(CurSegMeta));
        }
        while (!seg_meta[segloc].srq_num)
        {
            CurSegMeta *tmp_seg_meta = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta));
#if CORO_DEBUG
            co_await conn->read(segptr + sizeof(uint64_t), seg_rmr.rkey, tmp_seg_meta, sizeof(CurSegMeta), lmr->lkey, std::source_location::current(), std::format("[{}:{}]segloc:{}读取meta地址{}", cli_id, coro_id, segloc, segptr + sizeof(uint64_t)));
#else
            co_await conn->read(segptr + sizeof(uint64_t), seg_rmr.rkey, tmp_seg_meta, sizeof(CurSegMeta), lmr->lkey);
#endif
            memcpy(&seg_meta[segloc], tmp_seg_meta, sizeof(CurSegMeta));
            log_test("读取到segloc:%lu的meta地址%llx srq_num:%u global_depth:%lu local_depth:%lu", segloc, segptr + sizeof(uint64_t), seg_meta[segloc].srq_num, dir->global_depth, seg_meta[segloc].local_depth);
            // seg_meta[segloc].print(std::format("seg_meta[{}]在读取srq_num后:", segloc));
        }
        log_test("准备用qp_num:%u发送slot到segloc:%lu srq_num:%u", xrc_conn->qp->qp_num, segloc, seg_meta[segloc].srq_num);
        assert_require(seg_meta[segloc].srq_num > 0);
#if SPLIT_LOCAL_LOCK
        std::shared_lock<std::shared_mutex> read_lock(segloc_locks[segloc]);
#endif
#if CORO_DEBUG
        auto send_slot = xrc_conn->send(tmp, sizeof(Slot), lmr->lkey, seg_meta[segloc].srq_num, 
                                       std::source_location::current(), 
                                       std::format("[{}:{}]segloc:{}第{}次SEND slot", 
                                                  cli_id, coro_id, segloc, send_cnt + 1));
#else
        auto send_slot = xrc_conn->send(tmp, sizeof(Slot), lmr->lkey, seg_meta[segloc].srq_num);
#endif
#if SPLIT_LOCAL_LOCK
        read_lock.unlock();
#endif
        co_await std::move(send_slot);
        log_test("发送slot到segloc:%lu srq_num:%u完成", segloc, seg_meta[segloc].srq_num);
        perf.push_perf("send_slot");
        // log_test("[%lu:%lu]完成第%d次SEND slot segloc:%lu", cli_id, coro_id, ++send_cnt, segloc);

        // a3. fetch and add slot_cnt
        uint64_t fetch = 0;
        auto faa_slot_cnt = conn->fetch_add(segptr + sizeof(uint64_t), seg_rmr.rkey, fetch, 1ULL << 1);

        // b. write fp bitmap 同时进行write fp和faa slot_cnt，可能导致因为远端分裂而无效的写入被读到，但读取时会读CurSegment元数据，可以检查depth是否匹配而排除
#if LARGER_FP_FILTER_GRANULARITY
        auto bit_loc = get_fp_bit(tmp->fp, tmp->fp_2);
        uintptr_t fp_ptr = segptr + 4 * sizeof(uint64_t) + bit_loc * sizeof(FpBitmapType);
        // 远端的seg_meta->fp_bitmap[bit_loc]写入00000001。这里不用seg_meta，先alloc.alloc申请一个8byte buffer，写入00000001，然后写入远端。
        CurSegMeta *tmp_seg_meta = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta));
        seg_meta[segloc].fp_bitmap[bit_loc] = 1;
        memcpy(tmp, &seg_meta[segloc], sizeof(CurSegMeta));
        auto write_fp_bitmap = conn->write(fp_ptr, seg_rmr.rkey, &tmp_seg_meta->fp_bitmap[bit_loc], sizeof(FpBitmapType), lmr->lkey);
#endif

        co_await std::move(faa_slot_cnt);
#if LARGER_FP_FILTER_GRANULARITY
        co_await std::move(write_fp_bitmap);
#endif

        auto remote_sign = fetch & 1;
        auto remote_slot_cnt = (fetch & ((1 << SIGN_AND_SLOT_CNT_BITS) - 1)) >> 1;
        auto remote_local_depth = fetch >> SIGN_AND_SLOT_CNT_BITS;
        seg_meta[segloc].sign = remote_sign;
        seg_meta[segloc].slot_cnt = remote_slot_cnt; // TODO: 不使用fetch，直接返回到slot_cnt
        seg_meta[segloc].slot_cnt++;
        if (seg_meta[segloc].slot_cnt >= SLOT_PER_SEG) {
            log_test("[%lu:%lu]FAA segloc:%lu的meta地址%llx成功 remote_slot_cnt:%lu remote_local_depth:%lu",
                    cli_id, coro_id, segloc, segptr + sizeof(uint64_t), remote_slot_cnt, remote_local_depth);
        }
        seg_meta[segloc].slot_cnt %= SLOT_PER_SEG;

        bool remote_split = remote_local_depth > dir->segs[segloc].local_depth;
        if (remote_split)
        { // 应该大于本地的local_depth就需要更新
            // tmp->print(std::format("[{}:{}:{}]远端分裂了！remote_local_depth:{}>local_depth:{}，本次写入作废！需要重写！segloc:{}，FAA地址:{}", cli_id, coro_id, this->key_num, remote_local_depth, dir->segs[segloc].local_depth, segloc, segptr + sizeof(uint64_t)));
            co_await check_gd(segloc); // TODO: 稍后重写，如果是最后一个，至少要合并完
            // 更新本地的seg_meta
            auto new_segloc = get_seg_loc(pattern, dir->global_depth);
            co_await check_gd(new_segloc); // 现在&dir->segs[segloc]是最新的
            // segptr = dir->segs[segloc].cur_seg_ptr;
            // TODO: 更新所有指向此Segment的DirEntry
            // dir->print(std::format("[{}:{}]同步之后", cli_id, coro_id));
        }
        // check if need split
        if (seg_meta[segloc].slot_cnt == 0)
        {
            {
#if SPLIT_LOCAL_LOCK
                std::unique_lock<std::shared_mutex> write_lock(segloc_locks[segloc]);
#endif

                CurSegMeta *my_seg_meta = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta)); // use seg_meta?
                co_await conn->read(segptr + sizeof(uint64_t), seg_rmr.rkey, my_seg_meta, sizeof(CurSegMeta), lmr->lkey);

                co_await Split(segloc, segptr, my_seg_meta);
            }
            send_cnt = 0;
            if (remote_split) goto Retry;
            perf.push_insert();
            sum_cost.end_insert();
            sum_cost.push_retry_cnt(retry_cnt);
            co_return;
        }
        if (remote_split) goto Retry;

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
        if (main_seg_size)
            co_await conn->read(dir->segs[seg_loc].main_seg_ptr, seg_rmr.rkey, main_seg, main_seg_size, lmr->lkey);

        // 3. Sort Segment
        MainSeg *new_main_seg = (MainSeg *)alloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG);
#if READ_FULL_KEY_ON_FP_COLLISION
        uint64_t new_seg_len = co_await merge_insert(cur_seg->slots, SLOT_PER_SEG, main_seg->slots, dir->segs[seg_loc].main_seg_len, new_main_seg->slots, cur_seg->seg_meta.local_depth);
        // new_main_seg->print(new_seg_len);
#else
        uint64_t new_seg_len = merge_insert(cur_seg->slots, SLOT_PER_SEG, main_seg->slots, dir->segs[seg_loc].main_seg_len, new_main_seg->slots, cur_seg->seg_meta.local_depth);
#endif
        FpInfo fp_info[MAX_FP_INFO] = {};
        cal_fpinfo(new_main_seg->slots, new_seg_len, fp_info); // SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len

        log_merge("将segloc:%lu/%lu的%d个CurSeg条目合并到%d个MainSeg条目", seg_loc, (1ull << global_depth) - 1, SLOT_PER_SEG, dir->segs[seg_loc].main_seg_len);
        // 4. Split (为了减少协程嵌套层次的开销，这里就不抽象成单独的函数了)
        if (dir->segs[seg_loc].main_seg_len >= MAX_MAIN_SIZE)
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
                    // if dep_off == 3 (Have consumed all info in dep bits), read && construct new dep
#ifdef TOO_LARGE_KV
                    co_await conn->read(ralloc.ptr(new_main_seg->slots[i].offset), seg_rmr.rkey, kv_block,
                                            this->kv_block_len, lmr->lkey);
#else
                    co_await conn->read(ralloc.ptr(new_main_seg->slots[i].offset), seg_rmr.rkey, kv_block,
                                            new_main_seg->slots[i].len * ALIGNED_SIZE, lmr->lkey, std::source_location::current(), std::format("[{}:{}]segloc:{}读取KVBlock地址{}", cli_id, coro_id, seg_loc, ralloc.ptr(new_main_seg->slots[i].offset))); // IMPORTANT: 读取完整KV，合并可以参考。
#endif
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
                    cur_seg_ptr = seg_rmr.raddr + sizeof(uint64_t) + (cur_seg_loc + offset) * sizeof(DirEntry);

                    // Update DirEntry
                    co_await conn->write(cur_seg_ptr, seg_rmr.rkey, &dir->segs[cur_seg_loc + offset], sizeof(DirEntry), lmr->lkey);

                    // if (local_depth == dir->global_depth){
                    //     // global
                    //     log_err("[%lu:%lu:%lu]Global SPlit At segloc:%lu depth:%lu to :%lu with new seg_ptr:%lx new_main_seg_ptr:%lx", cli_id, coro_id, this->key_num, cur_seg_loc+offset, local_depth, local_depth + 1, i & 1 ? new_cur_ptr:seg_ptr,i & 1 ?new_main_ptr2:new_main_ptr1);
                    // }else{
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
            wo_wait_conn->pure_write_with_imm(new_cur_seg->seg_meta.main_seg_ptr, seg_rmr.rkey, new_seg_2, sizeof(Slot) * off2, lmr->lkey, new_segloc_bits.to_ulong()); // 提醒为新的Segment创建SRQ，并更新SRQN
            co_await conn->write(new_cur_ptr, seg_rmr.rkey, new_cur_seg, sizeof(CurSeg), lmr->lkey);
            // log_err("[%lu:%lu:%lu]更新新的CurSeg->seg_meta.local_depth: %lu", cli_id, coro_id, this->key_num, new_cur_seg->seg_meta.local_depth);
            // b. new main_seg for old
            cur_seg->seg_meta.main_seg_ptr = new_main_ptr1;
            cur_seg->seg_meta.main_seg_len = off1;
            cur_seg->seg_meta.local_depth = local_depth + 1; // IMPORTANT: local_depth+1，这是让客户端感知到split的关键
            // cur_seg->seg_meta.sign = !cur_seg->seg_meta.sign; // 对old cur_seg的清空放到最后?保证同步。
            memset(cur_seg->seg_meta.fp_bitmap, 0, sizeof(uint64_t) * 16);
            wo_wait_conn->pure_write(cur_seg->seg_meta.main_seg_ptr, seg_rmr.rkey, new_seg_1, sizeof(Slot) * off1, lmr->lkey);
            co_await conn->write(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(CurSegMeta), lmr->lkey);
            // co_await conn->write(seg_ptr + 2 * sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 2, sizeof(CurSegMeta) - sizeof(uint64_t), lmr->lkey);
            //         log_err("[%lu:%lu:%lu]更新旧的CurSeg->seg_meta.local_depth: %lu", cli_id, coro_id, this->key_num, new_cur_seg->seg_meta.local_depth);

            // 4.4 Change Sign (Equal to unlock this segment)
            cur_seg->seg_meta.sign = !cur_seg->seg_meta.sign;
            cur_seg->seg_meta.slot_cnt = 0;
#if MODIFIED
            log_merge("向远端地址%lx写入1，提醒为旧Segment发布RECV", seg_ptr + sizeof(uint64_t));
            wo_wait_conn->pure_write_with_imm(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(uint64_t), lmr->lkey, first_original_seg_loc); // 为旧Segment发布RECV，新Segment会在第一次连接时发布RECV
            log_merge("提醒为segloc:%lu htonl:%lu旧的Segment发布RECV完成, main_seg_ptr:%lx, main_seg_len:%lu", first_original_seg_loc, htonl(first_original_seg_loc), new_main_ptr1, off1);
#else
            co_await wo_wait_conn->write(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(uint64_t), lmr->lkey);
#endif
            sum_cost.end_split();
            // dir->print(std::format("[{}:{}]分裂之后", cli_id, coro_id));
            co_return;
        }

        // 5. Merge
        // 5.1 Write New MainSeg to Remote && Update CurSegMeta
        // a. write main segment

        uintptr_t new_main_ptr = ralloc.alloc(new_seg_len * sizeof(Slot), true); // IMPORTANT: 在MN分配new main seg，注意 FIXME: ralloc没有free功能 // main_seg_size + sizeof(Slot) * SLOT_PER_SEG
        // uint64_t new_main_len = new_seg_len; // dir->segs[seg_loc].main_seg_len + SLOT_PER_SEG;
        wo_wait_conn->pure_write(new_main_ptr, seg_rmr.rkey, new_main_seg->slots,
                                 sizeof(Slot) * new_seg_len, lmr->lkey);
        // co_await wo_wait_conn->write(new_main_ptr, seg_rmr.rkey, new_main_seg->slots,
        //                             sizeof(Slot) * new_seg_len, lmr->lkey);

        // b. Update MainSegPtr/Len and fp_bitmap
        cur_seg->seg_meta.main_seg_ptr = new_main_ptr;
        cur_seg->seg_meta.main_seg_len = new_seg_len; // main_seg_size / sizeof(Slot) + SLOT_PER_SEG;
        // if (main_seg_size / sizeof(Slot) + SLOT_PER_SEG != new_seg_len)
        //     log_err("新的main_seg_len从原来的%lu，去除掉过时条目后减少为%lu", main_seg_size / sizeof(Slot) + SLOT_PER_SEG, new_seg_len);
        this->offset[seg_loc].offset = 0;
        memset(cur_seg->seg_meta.fp_bitmap, 0, sizeof(uint64_t) * 16);
        co_await conn->write(seg_ptr + 2 * sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 2, sizeof(CurSegMeta) - sizeof(uint64_t), lmr->lkey);

        // 5.2 Update new-main-ptr for DirEntries
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
        cur_seg->seg_meta.sign = !cur_seg->seg_meta.sign;
        cur_seg->seg_meta.slot_cnt = 0;
#if MODIFIED
        log_merge("segloc:%lu合并了，准备向远端地址%lx写入1，提醒为旧Segment发布RECV", seg_loc, seg_ptr + sizeof(uint64_t));
        wo_wait_conn->pure_write_with_imm(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(uint64_t), lmr->lkey, seg_loc); // 为旧Segment发布RECV，此处的WRITE和上面的相同，无实际作用，只是为了触发信号
        log_merge("合并后提醒为segloc:%lu旧Segment发布RECV完成，main_seg_ptr:%lx,main_seg_len:%lu", seg_loc, new_main_ptr, new_seg_len);
#else
        co_await wo_wait_conn->write(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(uint64_t), lmr->lkey);
#endif
        sum_cost.end_merge();
    }

#if READ_FULL_KEY_ON_FP_COLLISION
    task<uint64_t> Client::merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg, uint64_t local_depth)
#else
    uint64_t Client::merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg, uint64_t local_depth)
#endif
    {
        std::sort(data, data + len);
        uint8_t sign = data[0].sign;
        int off_1 = 0, off_2 = 0;
        uint64_t new_seg_len = 0;
        if (len && old_seg_len) {
            for (uint64_t i = 0; i < len + old_seg_len; i++)
            {
            #if READ_FULL_KEY_ON_FP_COLLISION
                if (data[off_1].fp < old_seg[off_2].fp)
            #else
                if (data[off_1].fp <= old_seg[off_2].fp)
            #endif
                {
                    if (data[off_1].local_depth == local_depth)
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
                        if (data[off_1].local_depth == local_depth)
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
                        if (data[off_1].local_depth == local_depth)
                            new_seg[new_seg_len++] = data[off_1];
                    }
                    else
                    {
                        // 不同key，哈希冲突，两个都保留
                        // log_err("[%lu:%lu:%lu]确认为不同key(哈希冲突)，两个都保留 offset1:%lu offset2:%lu", 
                        //         cli_id, coro_id, this->key_num, 
                        //         data[off_1].offset, old_seg[off_2].offset);
                        if (data[off_1].local_depth == local_depth)
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
#if READ_FULL_KEY_ON_FP_COLLISION
        co_return new_seg_len;
#else
        return new_seg_len;
#endif
    }
}