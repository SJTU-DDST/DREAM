#include "myhash.h"

namespace MYHASH
{
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
            uint64_t root_segloc = get_seg_loc(pattern, dir->segs[segloc].local_depth);
            if (dir->segs[root_segloc].local_depth == dir->segs[segloc].local_depth)
            {
                // log_err("[%lu:%lu]本地还没分裂，segloc:%lu用root_segloc:%lu代替", cli_id, coro_id, segloc, root_segloc);
                segloc = root_segloc; // 如果local_depth<global_depth，会有多个segloc对应一个seg，此时将segloc最小的作为root_segloc用于send
            } else co_await check_gd(segloc);
        }
        uintptr_t segptr = dir->segs[segloc].cur_seg_ptr;
        if (segptr == 0)
        {
            // log_err("[%lu:%lu:%lu]本地segloc:%lu的segptr=0, 客户端第一次访问这个CurSeg？",cli_id,coro_id,this->key_num,segloc);
            segptr = co_await check_gd(segloc); // 现在只会修改main_seg_len和main_seg_ptr
            uint64_t new_seg_loc = get_seg_loc(pattern, dir->global_depth);
            if (new_seg_loc != segloc)
            {
                retry_reason = 1;
                goto Retry;
            }
        }

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
        log_test("[%lu:%lu]开始第%d次SEND slot segloc:%lu", cli_id, coro_id, send_cnt + 1, segloc);
        if (segloc >= conns.size())
        {
            conns.resize(segloc + 1, nullptr);
        }
        if (!conns[segloc])
        {
            co_await check_gd(segloc);                                                                                                                                                    // 先更新对应&dir->segs[segloc]
            conns[segloc] = cli->connect(config.server_ip.c_str(), rdma_default_port, 0, segloc);                                                                                // 一个cli只能有一个cli->conn，需要新建cli
            assert(conns[segloc] != nullptr);
        }
        perf.start_perf();
#if CORO_DEBUG
        auto start_time = std::chrono::steady_clock::now();
        co_await conns[segloc]->send(tmp, sizeof(Slot), lmr->lkey, std::source_location::current(), rdma_coro_desc(cli_id, coro_id, segloc, send_cnt + 1, conns[segloc], seg_rmr.rkey, lmr->lkey));
        auto end_time = std::chrono::steady_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);
        if (duration.count() > 1)
        {
            if (!seg_meta[segloc])
            {
                seg_meta[segloc] = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta));
                memset(seg_meta[segloc], 0, sizeof(CurSegMeta));
            }

            co_await conns[0]->read(segptr + sizeof(uint64_t), seg_rmr.rkey, seg_meta[segloc], sizeof(uint64_t), lmr->lkey);
            auto remote_sign = seg_meta[segloc]->sign;
            auto remote_slot_cnt = seg_meta[segloc]->slot_cnt;
            auto remote_local_depth = seg_meta[segloc]->local_depth;
            log_err("[%lu:%lu]超时了！读取FAA地址%llx segloc=%llu remote_sign:%lu "
                    "remote_slot_cnt:%lu remote_local_depth:%lu",
                    cli_id, coro_id, segptr + sizeof(uint64_t), segloc,
                    remote_sign, remote_slot_cnt, remote_local_depth);
        }
#else
        co_await conns[segloc]->send(tmp, sizeof(Slot), lmr->lkey);
#endif
        perf.push_perf("send_slot");
        log_test("[%lu:%lu]完成第%d次SEND slot segloc:%lu", cli_id, coro_id, ++send_cnt, segloc);

        // a3. fetch and add slot_cnt
        uint64_t fetch = 0;
        co_await conns[0]->fetch_add(segptr + sizeof(uint64_t), seg_rmr.rkey, fetch, 1ULL << 1); // segloc?

        auto remote_sign = fetch & 1;
        auto remote_slot_cnt = (fetch & ((1 << SIGN_AND_SLOT_CNT_BITS) - 1)) >> 1;
        auto remote_local_depth = fetch >> SIGN_AND_SLOT_CNT_BITS;
        if (!seg_meta[segloc])
        {
            seg_meta[segloc] = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta));
            memset(seg_meta[segloc], 0, sizeof(CurSegMeta));
        }
        seg_meta[segloc]->sign = remote_sign;
        seg_meta[segloc]->slot_cnt = remote_slot_cnt; // TODO: 不使用fetch，直接返回到slot_cnt
        seg_meta[segloc]->slot_cnt++;
        // if (seg_meta[segloc]->slot_cnt >= SLOT_PER_SEG)
        //     log_err("[%lu:%lu]FAA segloc:%lu的meta地址%llx成功 remote_slot_cnt:%lu remote_local_depth:%lu",
        //             cli_id, coro_id, segloc, segptr + sizeof(uint64_t), remote_slot_cnt, remote_local_depth);
        seg_meta[segloc]->slot_cnt %= SLOT_PER_SEG;

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
        if (seg_meta[segloc]->slot_cnt == 0)
        {
            CurSegMeta *my_seg_meta = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta)); // use seg_meta?
            co_await conns[0]->read(segptr + sizeof(uint64_t), seg_rmr.rkey, my_seg_meta, sizeof(CurSegMeta), lmr->lkey);

            co_await Split(segloc, segptr, my_seg_meta);
            send_cnt = 0;
            if (remote_split) goto Retry;
            perf.push_insert();
            sum_cost.end_insert();
            sum_cost.push_retry_cnt(retry_cnt);
            co_return;
        }
        if (remote_split) goto Retry;

        // b. write fp bitmap
#if LARGER_FP_FILTER_GRANULARITY
        auto bit_loc = get_fp_bit(tmp->fp, tmp->fp_2);
        uintptr_t fp_ptr = segptr + 4 * sizeof(uint64_t) + bit_loc * sizeof(FpBitmapType);
        // 远端的seg_meta->fp_bitmap[bit_loc]写入00000001。这里不用seg_meta，先alloc.alloc申请一个8byte buffer，写入00000001，然后写入远端。
        if (!seg_meta[segloc])
        {
            seg_meta[segloc] = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta));
            memset(seg_meta[segloc], 0, sizeof(CurSegMeta));
        }
        seg_meta[segloc]->fp_bitmap[bit_loc] = 1;
        conns[0]->pure_write(fp_ptr, seg_rmr.rkey, &seg_meta[segloc]->fp_bitmap[bit_loc], sizeof(FpBitmapType), lmr->lkey);
#endif
        perf.push_insert();
        sum_cost.end_insert();
        sum_cost.push_retry_cnt(retry_cnt);
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
        co_await conns[0]->read(seg_ptr, seg_rmr.rkey, cur_seg, sizeof(CurSeg), lmr->lkey);

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
            co_await conns[0]->read(dir->segs[seg_loc].main_seg_ptr, seg_rmr.rkey, main_seg, main_seg_size, lmr->lkey);

        // 3. Sort Segment
        MainSeg *new_main_seg = (MainSeg *)alloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG);
        uint64_t new_seg_len = merge_insert(cur_seg->slots, SLOT_PER_SEG, main_seg->slots, dir->segs[seg_loc].main_seg_len, new_main_seg->slots, cur_seg->seg_meta.local_depth);
        FpInfo fp_info[MAX_FP_INFO] = {};
        cal_fpinfo(new_main_seg->slots, SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len, fp_info);

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
            for (uint64_t i = 0; i < (SLOT_PER_SEG + dir->segs[seg_loc].main_seg_len); i++)
            {
                dep_bit = (new_main_seg->slots[i].dep >> dep_off) & 1;
                if (dep_off == 3)
                {
                    // if dep_off == 3 (Have consumed all info in dep bits), read && construct new dep
#ifdef TOO_LARGE_KV
                    co_await conns[0]->read(ralloc.ptr(new_main_seg->slots[i].offset), seg_rmr.rkey, kv_block,
                                            this->kv_block_len, lmr->lkey);
#else
                    co_await conns[0]->read(ralloc.ptr(new_main_seg->slots[i].offset), seg_rmr.rkey, kv_block,
                                            new_main_seg->slots[i].len * ALIGNED_SIZE, lmr->lkey); // IMPORTANT: 读取完整KV，合并可以参考。
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
                    co_await conns[0]->write(cur_seg_ptr, seg_rmr.rkey, &dir->segs[cur_seg_loc + offset], sizeof(DirEntry), lmr->lkey);

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
            if (local_depth == dir->global_depth)
            {
                // log_err("local_depth达到上限，更新global depth:%lu -> %lu, max segs:%d to %d",dir->global_depth,dir->global_depth+1, (1 << global_depth), (1 << (global_depth+1))); // at first we have 16 segs
                dir->global_depth++;
                co_await conns[0]->write(seg_rmr.raddr, seg_rmr.rkey, &dir->global_depth, sizeof(uint64_t), lmr->lkey);
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
            co_await conns[0]->write(new_cur_ptr, seg_rmr.rkey, new_cur_seg, sizeof(CurSeg), lmr->lkey);
            // log_err("[%lu:%lu:%lu]更新新的CurSeg->seg_meta.local_depth: %lu", cli_id, coro_id, this->key_num, new_cur_seg->seg_meta.local_depth);
            // b. new main_seg for old
            cur_seg->seg_meta.main_seg_ptr = new_main_ptr1;
            cur_seg->seg_meta.main_seg_len = off1;
            cur_seg->seg_meta.local_depth = local_depth + 1; // IMPORTANT: local_depth+1，这是让客户端感知到split的关键
            // cur_seg->seg_meta.sign = !cur_seg->seg_meta.sign; // 对old cur_seg的清空放到最后?保证同步。
            memset(cur_seg->seg_meta.fp_bitmap, 0, sizeof(uint64_t) * 16);
            wo_wait_conn->pure_write(cur_seg->seg_meta.main_seg_ptr, seg_rmr.rkey, new_seg_1, sizeof(Slot) * off1, lmr->lkey);
            co_await conns[0]->write(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(CurSegMeta), lmr->lkey);
            // co_await conns[0]->write(seg_ptr + 2 * sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 2, sizeof(CurSegMeta) - sizeof(uint64_t), lmr->lkey);
            //         log_err("[%lu:%lu:%lu]更新旧的CurSeg->seg_meta.local_depth: %lu", cli_id, coro_id, this->key_num, new_cur_seg->seg_meta.local_depth);

            // 4.4 Change Sign (Equal to unlock this segment)
            cur_seg->seg_meta.sign = !cur_seg->seg_meta.sign;
            cur_seg->seg_meta.slot_cnt = 0;
#if MODIFIED
            log_merge("向远端地址%lx写入1，提醒为旧Segment发布RECV", seg_ptr + sizeof(uint64_t));
            signal_conn->pure_write_with_imm(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(uint64_t), lmr->lkey, first_original_seg_loc); // 为旧Segment发布RECV，新Segment会在第一次连接时发布RECV
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

        uintptr_t new_main_ptr = ralloc.alloc(main_seg_size + sizeof(Slot) * SLOT_PER_SEG, true); // IMPORTANT: 在MN分配new main seg，注意 FIXME: ralloc没有free功能
        uint64_t new_main_len = dir->segs[seg_loc].main_seg_len + SLOT_PER_SEG;
        wo_wait_conn->pure_write(new_main_ptr, seg_rmr.rkey, new_main_seg->slots,
                                 sizeof(Slot) * new_main_len, lmr->lkey);
        // co_await wo_wait_conn->write(new_main_ptr, seg_rmr.rkey, new_main_seg->slots,
        //                             sizeof(Slot) * new_main_len, lmr->lkey);

        // b. Update MainSegPtr/Len and fp_bitmap
        cur_seg->seg_meta.main_seg_ptr = new_main_ptr;
        cur_seg->seg_meta.main_seg_len = new_seg_len; // main_seg_size / sizeof(Slot) + SLOT_PER_SEG;
        // if (main_seg_size / sizeof(Slot) + SLOT_PER_SEG != new_seg_len)
        //     log_err("新的main_seg_len从原来的%lu，去除掉过时条目后减少为%lu", main_seg_size / sizeof(Slot) + SLOT_PER_SEG, new_seg_len);
        this->offset[seg_loc].offset = 0;
        memset(cur_seg->seg_meta.fp_bitmap, 0, sizeof(uint64_t) * 16);
        co_await conns[0]->write(seg_ptr + 2 * sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 2, sizeof(CurSegMeta) - sizeof(uint64_t), lmr->lkey);

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
                dir->segs[cur_seg_loc + offset].main_seg_len = new_main_len;
                memcpy(dir->segs[cur_seg_loc + offset].fp, fp_info, sizeof(FpInfo) * MAX_FP_INFO);
                dentry_ptr = seg_rmr.raddr + sizeof(uint64_t) + (cur_seg_loc + offset) * sizeof(DirEntry);
                // Update
                // 暂时还是co_await吧
                co_await conns[0]->write(dentry_ptr, seg_rmr.rkey, &dir->segs[cur_seg_loc + offset], sizeof(DirEntry), lmr->lkey);
                // conn->pure_write(dentry_ptr, seg_rmr.rkey,&dir->segs[cur_seg_loc+offset], sizeof(DirEntry), lmr->lkey);
                // log_err("[%lu:%lu:%lu]Merge At segloc:%lu depth:%lu with old_main_ptr:%lx new_main_ptr:%lx",cli_id,coro_id,this->key_num,cur_seg_loc+offset,local_depth,main_seg_ptr,new_main_ptr);
            }
        }

        // 5.3 Change Sign (Equal to unlock this segment)
        cur_seg->seg_meta.sign = !cur_seg->seg_meta.sign;
        cur_seg->seg_meta.slot_cnt = 0;
#if MODIFIED
        log_merge("segloc:%lu合并了，准备向远端地址%lx写入1，提醒为旧Segment发布RECV", seg_loc, seg_ptr + sizeof(uint64_t));
        signal_conn->pure_write_with_imm(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(uint64_t), lmr->lkey, seg_loc); // 为旧Segment发布RECV，此处的WRITE和上面的相同，无实际作用，只是为了触发信号
        log_merge("合并后提醒为segloc:%lu旧Segment发布RECV完成，main_seg_ptr:%lx,main_seg_len:%lu", seg_loc, new_main_ptr, new_main_len);
#else
        co_await wo_wait_conn->write(seg_ptr + sizeof(uint64_t), seg_rmr.rkey, ((uint64_t *)cur_seg) + 1, sizeof(uint64_t), lmr->lkey);
#endif
        sum_cost.end_merge();
    }

    uint64_t Client::merge_insert(Slot *data, uint64_t len, Slot *old_seg, uint64_t old_seg_len, Slot *new_seg, uint64_t local_depth)
    {
        std::sort(data, data + len);
        uint8_t sign = data[0].sign;
        int off_1 = 0, off_2 = 0;
        uint64_t new_seg_len = 0;
        if (len && old_seg_len) {
            for (uint64_t i = 0; i < len + old_seg_len; i++)
            {
                // if (data[off_1].sign != sign) // 现在不需要sign了
                // {
                //     // log_err("[%lu:%lu:%lu]wrong sign",cli_id,coro_id,this->key_num);
                //     // print_mainseg(data, len);
                //     // exit(-1);
                // }
                if (data[off_1].fp <= old_seg[off_2].fp)
                {
                    if (data[off_1].fp == old_seg[off_2].fp && data[off_1].fp_2 == old_seg[off_2].fp_2)
                    {
                        // log_err("[%lu:%lu:%lu]duplicate fp=%d fp2=%d", cli_id, coro_id, this->key_num, data[off_1].fp, data[off_1].fp_2);
                        // TODO: 读取完整key，对比
                    }
                    if (data[off_1].local_depth == local_depth) // 只需要检查CurSeg的local_depth
                        new_seg[new_seg_len++] = data[off_1];
                    // else data[off_1].print(std::format("[{}:{}:{}]发现过时条目，local_depth:{} != {}", cli_id, coro_id, this->key_num, data[off_1].local_depth, local_depth));
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
            memcpy(new_seg + len + off_2, old_seg + off_2, (old_seg_len - off_2) * sizeof(Slot));
            new_seg_len += old_seg_len - off_2;
        }
        return new_seg_len;
    }
}