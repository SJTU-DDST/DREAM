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
            { // 没分裂
                // log_err("[%lu:%lu]本地还没分裂，segloc:%lu用root_segloc:%lu代替", cli_id, coro_id, segloc, root_segloc);
                segloc = root_segloc;
            }
            else
            { // 同步
                // log_err("[%lu:%lu]本地已经分裂，root depth:%lu != segloc depth:%lu", cli_id, coro_id, dir->segs[root_segloc].local_depth, dir->segs[segloc].local_depth);
                // dir->print("同步之前");
                co_await check_gd(segloc);
                // co_await check_gd(root_segloc);
                // dir->print("同步之后");
            }
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
        // uint64_t dep = 0; // seg_meta->local_depth - (seg_meta->local_depth % 4); // 按4对齐 FIXME: 去掉before read后现在还没有seg_meta，可以用缓存的dir->segs[segloc].local_depth
        uint64_t dep = dir->segs[segloc].local_depth - (dir->segs[segloc].local_depth % 4);
        // 去掉before read后现在还没有seg_meta，可以用缓存的dir->segs[segloc].local_depth
        // 这个会影响分裂后去到哪个segment，发送slot时需要带上本地的local_depth，分裂时如果不匹配则读取远端的local_depth
        tmp->dep = pattern >> dep;
        tmp->fp = fp(pattern);
        tmp->len = (kvblock_len + ALIGNED_SIZE - 1) / ALIGNED_SIZE;
        tmp->sign = 0; // 目前sign的作用已经被slot_cnt替代
        tmp->offset = ralloc.offset(kvblock_ptr);
        tmp->fp_2 = fp2(pattern);

        // a2. send slot
        static int send_cnt = 0;
        log_test("[%lu:%lu]开始第%d次SEND slot segloc:%lu", cli_id, coro_id, send_cnt + 1, segloc);
        if (segloc >= conns.size())
        {
            clis.resize(segloc + 1, nullptr);
            conns.resize(segloc + 1, nullptr);
        }
        if (!conns[segloc])
        {
            // log_err("[%lu:%lu]创建新的rdma_conn, segloc:%lu", cli_id, coro_id,
            //         segloc);
            // dir->print(std::format("[{}:{}]segloc:{}创建新的rdma_conn之前", cli_id,
            //                        coro_id, segloc));
            co_await check_gd(segloc);                                                                                                                                                    // 先更新对应&dir->segs[segloc]
            clis[segloc] = new rdma_client(clis[0]->dev, so_qp_cap, rdma_default_tempmp_size, config.max_coro, config.cq_size, nullptr, clis[0]->cq, clis[0]->coros, clis[0]->free_head); // 复用clis[0]的cq和coros, free_head
            conns[segloc] = clis[segloc]->connect(config.server_ip.c_str(), rdma_default_port, 0, segloc);                                                                                // 一个cli只能有一个cli->conn，需要新建cli
            assert(conns[segloc] != nullptr);
            // log_err("[%lu:%lu]创建新的rdma_conn成功, segloc:%lu, qp_num:%d", cli_id,
            //         coro_id, segloc, conns[segloc]->qp->qp_num);
            // dir->print(std::format("[{}:{}]segloc:{}创建新的rdma_conn之后", cli_id,
            //                        coro_id, segloc));
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

        if (remote_local_depth > dir->segs[segloc].local_depth)
        { // 应该大于本地的local_depth就需要更新
            log_test("[%lu:%lu:%lu]远端分裂了！本次写入作废！需要重写！TODO: check_gd+重写。segloc:%lu, FAA地址:%p。", cli_id, coro_id, this->key_num, segloc, segptr + sizeof(uint64_t));
            // dir->print(std::format("[{}:{}]segloc:{}同步之前", cli_id, coro_id, segloc));
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
            // dir->print(std::format("[{}:{}]合并之前", cli_id, coro_id));
            uint64_t my_pattern = (uint64_t)hash(key->data, key->len);
            uint64_t my_segloc = get_seg_loc(my_pattern, dir->global_depth);
            uintptr_t my_segptr = dir->segs[my_segloc].cur_seg_ptr;
            // if (my_segptr == 0)
            // {                                          // 目前不会执行
            //     my_segptr = co_await check_gd(my_segloc); // 现在只会修改main_seg_len和main_seg_ptr
            //     uint64_t new_seg_loc = get_seg_loc(my_pattern, dir->global_depth);
            //     if (new_seg_loc != my_segloc)
            //     {
            //         retry_reason = 1;
            //         goto Retry;
            //     }
            // }

            CurSegMeta *my_seg_meta = (CurSegMeta *)alloc.alloc(sizeof(CurSegMeta));
            // auto read_meta =
            co_await conns[0]->read(my_segptr + sizeof(uint64_t), seg_rmr.rkey, my_seg_meta, sizeof(CurSegMeta), lmr->lkey);

            co_await Split(my_segloc, segptr, my_seg_meta);
            // dir->print(std::format("[{}:{}]合并之后", cli_id, coro_id));
            send_cnt = 0;
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
}