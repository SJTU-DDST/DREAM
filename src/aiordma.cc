#include "aiordma.h"
#include "sephash.h"

#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>

#include <ranges>
#include <algorithm>

#ifdef ENABLE_DOCA_DMA
#include <json-c/json.h>
#include <doca_mmap.h>
#include <doca_buf_inventory.h>
#include <doca_dma.h>
#endif

const ibv_qp_cap sr_qp_cap = {
    .max_send_wr = 1024,
    .max_recv_wr = 1024, // 32,
    .max_send_sge = 1,
    .max_recv_sge = 1,
    .max_inline_data = 256,
};

const ibv_qp_cap so_qp_cap = {
    .max_send_wr = 1024,
    .max_recv_wr = 0,
    .max_send_sge = 1,
    .max_recv_sge = 1,
    .max_inline_data = 256,
};

const ibv_qp_cap zero_qp_cap = {
    .max_send_wr = 0,
    .max_recv_wr = 0,
    .max_send_sge = 1,
    .max_recv_sge = 1,
    .max_inline_data = 0,
};

struct sock_select_list
{
    std::vector<int> sks;

    int maxfd{0};
    int timeout_us;
    bool dirty{false};
    timeval timeout{.tv_sec = 0, .tv_usec = 0};
    fd_set fds;

    sock_select_list(int _timeout_us) : timeout_us(_timeout_us) {}
    void add(int sk)
    {
        sks.emplace_back(sk);
        if (sk > maxfd)
            maxfd = sk;
    }
    void del(int sk)
    {
        auto psk = std::ranges::find(sks, sk);
        assert_require(psk != sks.end());
        *psk = -1;
        if (maxfd == sk)
            maxfd = *std::ranges::max_element(sks);
        dirty = true;
    }
    void cleanup()
    {
        size_t i = 0, j = 0;
        for (; j < sks.size(); ++i, ++j)
        {
            while (j < sks.size() && sks[j] == -1)
                ++j;
            sks[i] = sks[j];
        }
        for (; i < j; ++i)
            sks.pop_back();
        dirty = false;
    }
    int select()
    {
        if (dirty)
            cleanup();
        timeout.tv_usec = timeout_us;
        FD_ZERO(&fds);
        std::ranges::for_each(sks, [this](int sk)
                              { FD_SET(sk, &fds); });
        int ready_num = ::select(maxfd + 1, &fds, nullptr, nullptr, &timeout);
        if (ready_num < 0)
        {
            log_err("select failed with error: %s", strerror(errno));
        }
        assert_require(ready_num >= 0);
        return ready_num;
    }
    constexpr bool isset(int sk) { return FD_ISSET(sk, &fds); }
};

enum
{
    rdma_exchange_proto_invaild,
    rdma_exchange_proto_setup,
    rdma_exchange_proto_ready,
    rdma_exchange_proto_reconnect,
};

struct __rdma_exchange_t
{
    uint16_t proto;
    uint16_t lid;
    uint32_t qp_num;
    uint32_t rkey;
    uint64_t raddr;
    ibv_gid gid;
    ConnInfo conn_info;
};

rdma_dev::rdma_dev(const char *dev_name, int _ib_port, int _gid_idx)
    : ib_port(_ib_port), gid_idx(_gid_idx), conn_ids(65536)
{
    ibv_device **dev_list = nullptr;
    ibv_device *ib_dev = nullptr;
    int num_devices = 0;

    dev_list = ibv_get_device_list(&num_devices);
    assert_require(dev_list && num_devices);
    if (!dev_name)
        ib_dev = dev_list[0];
    else
        for (int i = 0; i < num_devices; i++)
            if (!strcmp(ibv_get_device_name(dev_list[i]), dev_name))
            {
                ib_dev = dev_list[i];
                break;
            }
    assert_require(ib_dev);

    assert_require(ib_ctx = ibv_open_device(ib_dev));

    ibv_free_device_list(dev_list);
    dev_list = nullptr;

    if (ibv_query_device(ib_ctx, &device_attr))
    {
        log_warn("failed to query device attributes");
        memset(&device_attr, 0, sizeof(device_attr));
    }
    assert_require(0 == ibv_query_port(ib_ctx, ib_port, &port_attr));

    // log_err("Device attributes:");
    // log_err("  fw_ver: %s", device_attr.fw_ver);
    // log_err("  node_guid: 0x%016llx", (unsigned long long)device_attr.node_guid);
    // log_err("  sys_image_guid: 0x%016llx", (unsigned long long)device_attr.sys_image_guid);
    // log_err("  max_mr_size: %llu", (unsigned long long)device_attr.max_mr_size);
    // log_err("  page_size_cap: %llu", (unsigned long long)device_attr.page_size_cap);
    // log_err("  vendor_id: 0x%06x", device_attr.vendor_id);
    // log_err("  vendor_part_id: 0x%06x", device_attr.vendor_part_id);
    // log_err("  hw_ver: 0x%x", device_attr.hw_ver);
    // log_err("  max_qp: %d", device_attr.max_qp);
    // log_err("  max_qp_wr: %d", device_attr.max_qp_wr);
    // log_err("  device_cap_flags: 0x%x", device_attr.device_cap_flags);
    // log_err("  max_sge: %d", device_attr.max_sge);
    // log_err("  max_sge_rd: %d", device_attr.max_sge_rd);
    // log_err("  max_cq: %d", device_attr.max_cq);
    // log_err("  max_cqe: %d", device_attr.max_cqe);
    // log_err("  max_mr: %d", device_attr.max_mr);
    // log_err("  max_pd: %d", device_attr.max_pd);
    // log_err("  max_qp_rd_atom: %d", device_attr.max_qp_rd_atom);
    // log_err("  max_ee_rd_atom: %d", device_attr.max_ee_rd_atom);
    // log_err("  max_res_rd_atom: %d", device_attr.max_res_rd_atom);
    // log_err("  max_qp_init_rd_atom: %d", device_attr.max_qp_init_rd_atom);
    // log_err("  max_ee_init_rd_atom: %d", device_attr.max_ee_init_rd_atom);
    // log_err("  atomic_cap: %d", device_attr.atomic_cap);
    // log_err("  max_ee: %d", device_attr.max_ee);
    // log_err("  max_rdd: %d", device_attr.max_rdd);
    // log_err("  max_mw: %d", device_attr.max_mw);
    // log_err("  max_raw_ipv6_qp: %d", device_attr.max_raw_ipv6_qp);
    // log_err("  max_raw_ethy_qp: %d", device_attr.max_raw_ethy_qp);
    // log_err("  max_mcast_grp: %d", device_attr.max_mcast_grp);
    // log_err("  max_mcast_qp_attach: %d", device_attr.max_mcast_qp_attach);
    // log_err("  max_total_mcast_qp_attach: %d", device_attr.max_total_mcast_qp_attach);
    // log_err("  max_ah: %d", device_attr.max_ah);
    // log_err("  max_fmr: %d", device_attr.max_fmr);
    // log_err("  max_map_per_fmr: %d", device_attr.max_map_per_fmr);
    // log_err("  max_srq: %d", device_attr.max_srq);
    // log_err("  max_srq_wr: %d", device_attr.max_srq_wr);
    // log_err("  max_srq_sge: %d", device_attr.max_srq_sge);
    // log_err("  max_pkeys: %u", device_attr.max_pkeys);
    // log_err("  local_ca_ack_delay: %u", device_attr.local_ca_ack_delay);
    // log_err("  phys_port_cnt: %u", device_attr.phys_port_cnt);

    // rdma_dev: Device attributes:
    //   fw_ver: 20.43.2026
    //   node_guid: 0xc874870003f6ceb8
    //   sys_image_guid: 0xc674870003f6ceb8
    //   max_mr_size: 18446744073709551615
    //   page_size_cap: 18446744073709547520
    //   vendor_id: 0x0002c9
    //   vendor_part_id: 0x00101b
    //   hw_ver: 0x0
    //   max_qp: 131072
    //   max_qp_wr: 32768
    //   device_cap_flags: 0x21361c36
    //   max_sge: 30
    //   max_sge_rd: 30
    //   max_cq: 16777216
    //   max_cqe: 4194303
    //   max_mr: 16777216
    //   max_pd: 8388608
    //   max_qp_rd_atom: 16
    //   max_ee_rd_atom: 0
    //   max_res_rd_atom: 2097152
    //   max_qp_init_rd_atom: 16 // 不要有太多同设备并发请求
    //   max_ee_init_rd_atom: 0
    //   atomic_cap: 1
    //   max_ee: 0
    //   max_rdd: 0
    //   max_mw: 16777216
    //   max_raw_ipv6_qp: 0
    //   max_raw_ethy_qp: 0
    //   max_mcast_grp: 2097152
    //   max_mcast_qp_attach: 240
    //   max_total_mcast_qp_attach: 503316480
    //   max_ah: 2147483647
    //   max_fmr: 0
    //   max_map_per_fmr: 0
    //   max_srq: 8388608
    //   max_srq_wr: 32767 // 不要有太多outstanding SRQ RECV WRs
    //   max_srq_sge: 31
    //   max_pkeys: 128
    //   local_ca_ack_delay: 16
    //   phys_port_cnt: 1

    assert_require(pd = ibv_alloc_pd(ib_ctx));

    fd = open("/tmp/xrc_domain", O_RDONLY | O_CREAT, S_IRUSR | S_IRGRP);
    if (fd < 0)
    {
        fprintf(stderr, "Couldn't create the file for the XRC Domain but not stopping %d\n", errno);
        fd = -1;
    }

    struct ibv_xrcd_init_attr xrcd_attr;
    memset(&xrcd_attr, 0, sizeof xrcd_attr);
    xrcd_attr.comp_mask = IBV_XRCD_INIT_ATTR_FD | IBV_XRCD_INIT_ATTR_OFLAGS;
    xrcd_attr.fd = fd;
    xrcd_attr.oflags = O_CREAT;
    assert_require(xrcd = ibv_open_xrcd(ib_ctx, &xrcd_attr));
    // log_err("创建xrcd成功");

    assert_require(info_mr = create_mr(rdma_info_mr_size, nullptr, mr_flag_ro));
    // log_err("创建dev时创建info_mr成功");
    auto hdr = (rdma_infomr_hdr *)info_mr->addr;
    hdr->cksum = 0;
    hdr->tail = 0;

    for (int i = 1; i < 65536; ++i)
        conn_ids.enqueue(i);

    log_info("rdma_dev init success");
}

rdma_dev::~rdma_dev()
{
    if (info_mr)
        rdma_free_mr(info_mr, true);
    if (pd && ibv_dealloc_pd(pd))
        log_warn("failed to dealloc pd");
    if (xrcd && ibv_close_xrcd(xrcd))
        log_warn("failed to close xrcd");
    if (fd >= 0 && close(fd))
        log_warn("failed to close the file for the XRC Domain");
    // log_err("关闭xrcd成功");
    if (ib_ctx && ibv_close_device(ib_ctx))
        log_warn("failed to close device");
#ifdef ENABLE_DOCA_DMA
    if (dma_dev && doca_dev_close(dma_dev) != DOCA_SUCCESS)
        log_warn("failed to close dma dev");
#endif
    log_info("rdma_dev destory success");
}

ibv_mr *rdma_dev::create_mr(size_t size, void *buf, int mr_flags)
{
    bool malloc_flag = false;
    if (buf == nullptr)
    {
        malloc_flag = true;
        buf = alloc_hugepage(size);
    }
    if (buf == nullptr)
        return nullptr;
    // printf("Registering memory region with the following parameters:\n");
    // printf("pd: %p\n", pd);
    // printf("buf: %p~%p\n", buf, (char *)buf + size);
    // printf("size: %zu\n", size);
    // printf("mr_flags: %d\n", mr_flags);
    auto mr = ibv_reg_mr(pd, buf, size, mr_flags);
    if (!mr && malloc_flag)
        free_hugepage(buf, upper_align(size, 1 << 21));
    // printf("lkey: %u\n", mr->lkey);
    return mr;
}

ibv_mr *rdma_dev::reg_mr(uint32_t mr_id, ibv_mr *mr)
{
    if (!mr)
        return nullptr;
    std::lock_guard _(info_lock);
    auto hdr = (rdma_infomr_hdr *)info_mr->addr;
    if (hdr->tail + sizeof(rdma_rmr) > rdma_info_mr_size || info_idx.contains(mr_id))
        return nullptr;
    auto rmr_info = (rdma_rmr *)(hdr->data + hdr->tail);
    rmr_info->type = rdma_info_type_mr;
    rmr_info->mr_id = mr_id;
    rmr_info->raddr = (uintptr_t)mr->addr;
    rmr_info->rkey = mr->rkey;
    rmr_info->rlen = mr->length;
    hdr->tail += sizeof(rdma_rmr);
    hdr->cksum = crc64(hdr->data, hdr->tail);

    info_idx.emplace(mr_id, mr);
    return mr;
}

ibv_mr *rdma_dev::reg_mr(uint32_t mr_id, size_t size, void *buf, int mr_flags)
{
    ibv_mr *res = create_mr(size, buf, mr_flags);
    // log_err("reg_mr时创建MR成功");
    if (res && !reg_mr(mr_id, res))
    {
        rdma_free_mr(res, buf == nullptr);
        return nullptr;
    }
    return res;
}

ibv_dm *rdma_dev::create_dm(size_t size, uint32_t log_align)
{
    ibv_alloc_dm_attr dm_attr = {.length = size, .log_align_req = log_align, .comp_mask = 0};
    return ibv_alloc_dm(ib_ctx, &dm_attr);
}

rdma_dmmr rdma_dev::create_dmmr(size_t size, uint32_t log_align, int mr_flags)
{
    ibv_dm *dm = create_dm(size, log_align);
    if (!dm)
        return {};
    ibv_mr *mr = ibv_reg_dm_mr(pd, dm, 0, size, mr_flags | IBV_ACCESS_ZERO_BASED);
    if (!mr)
    {
        ibv_free_dm(dm);
        return {};
    }
    return {dm, mr};
}

rdma_dmmr rdma_dev::reg_dmmr(uint32_t mr_id, size_t size, uint32_t log_align, int mr_flags)
{
    auto [dm, mr] = create_dmmr(size, log_align, mr_flags);
    if (mr && !reg_mr(mr_id, mr))
    {
        rdma_free_dmmr({dm, mr});
        return {};
    }
    return {dm, mr};
}

#ifdef ENABLE_DOCA_DMA
void rdma_dev::enable_dma(const char *dev_name)
{
    assert_require(dma_dev == nullptr);
    uint8_t property_buf[512];
    doca_pci_bdf pci;
    doca_devinfo **devlist;
    doca_devinfo *devinfo{nullptr};
    uint32_t devcount = 0;
    assert_require(doca_devinfo_list_create(&devlist, &devcount) == DOCA_SUCCESS);
    assert_require(devcount);
    if (!dev_name)
        devinfo = *devlist;
    else
    {
        const void *dev_name_raw = dev_name;
        size_t dev_name_len = strlen(dev_name);
        if (dev_name_len == 7 && dev_name[2] == ':' && dev_name[5] == '.')
        {
            uint32_t pci_bus, pci_dev, pci_func;
            auto res = sscanf(dev_name, "%u:%u.%u", &pci_bus, &pci_dev, &pci_func);
            assert_require(res == 3);
            pci.bus = pci_bus;
            pci.device = pci_dev;
            pci.function = pci_func;
            dev_name_raw = &pci;
            dev_name_len = sizeof(doca_pci_bdf);
        }
        for (uint32_t i = 0; !devinfo && i < devcount; ++i)
            for (auto [property, len] : std::initializer_list<std::tuple<doca_devinfo_property, uint32_t>>{
                     {DOCA_DEVINFO_PROPERTY_PCI_ADDR, sizeof(doca_pci_bdf)},
                     {DOCA_DEVINFO_PROPERTY_IFACE_NAME, 256},
                     {DOCA_DEVINFO_PROPERTY_IBDEV_NAME, 64},
                 })
                if (doca_devinfo_property_get(devlist[i], property, property_buf, len) == DOCA_SUCCESS && memcmp(dev_name_raw, property_buf, dev_name_len) == 0)
                {
                    devinfo = devlist[i];
                    break;
                }
    }
    assert_require(devinfo);
    assert_require(doca_dev_open(devinfo, &dma_dev) == DOCA_SUCCESS);
    assert_require(doca_devinfo_list_destroy(devlist) == DOCA_SUCCESS);
}

std::tuple<doca_mmap *, void *> rdma_dev::create_mmap(uint32_t mmap_id, size_t len, void *addr)
{
    bool alloc_flag = false;
    doca_mmap *mmp = nullptr;
    assert_require(doca_mmap_create(std::to_string(mmap_id).c_str(), &mmp) == DOCA_SUCCESS);
    assert_require(doca_mmap_start(mmp) == DOCA_SUCCESS);
    assert_require(doca_mmap_dev_add(mmp, dma_dev) == DOCA_SUCCESS);
    assert_require(addr != nullptr || (alloc_flag = (addr = alloc_hugepage(len)) != nullptr));
    assert_require(doca_mmap_populate(mmp, (char *)addr, len, 1 << 21, alloc_flag ? free_mmap_mem : nullptr, nullptr) == DOCA_SUCCESS);
    return {mmp, addr};
}

std::tuple<doca_mmap *, void *> rdma_dev::reg_mmap(uint32_t mmap_id, std::tuple<doca_mmap *, void *> &mmpaddr)
{
    void *export_desc;
    size_t export_desc_len;
    auto [mmp, addr] = mmpaddr;
    if (!mmp)
        return {nullptr, nullptr};
    assert_require(doca_mmap_export(mmp, dma_dev, &export_desc, &export_desc_len) == DOCA_SUCCESS);
    std::lock_guard _(info_lock);
    auto hdr = (rdma_infomr_hdr *)info_mr->addr;
    if (hdr->tail + sizeof(rdma_rmmap) + export_desc_len > rdma_info_mr_size || info_idx.contains(mmap_id))
    {
        free(export_desc);
        return {nullptr, nullptr};
    }
    auto rmmap_info = (rdma_rmmap *)(hdr->data + hdr->tail);
    rmmap_info->type = rdma_info_type_mmap;
    rmmap_info->mmap_id = mmap_id;
    rmmap_info->len = export_desc_len;
    memcpy(rmmap_info->data, export_desc, export_desc_len);
    free(export_desc);
    hdr->tail += sizeof(rdma_rmmap) + export_desc_len;
    hdr->cksum = crc64(hdr->data, hdr->tail);
    info_idx.emplace(mmap_id, mmp);
    return mmpaddr;
}

std::tuple<doca_mmap *, void *> rdma_dev::reg_mmap(uint32_t mmap_id, size_t len, void *addr)
{
    auto mmpaddr = create_mmap(mmap_id, len, addr);
    if (std::get<0>(mmpaddr) && !std::get<0>(reg_mmap(mmap_id, mmpaddr)))
    {
        free_mmap(std::get<0>(mmpaddr));
        return {nullptr, nullptr};
    }
    return mmpaddr;
}
#endif

rdma_worker::rdma_worker(rdma_dev &_dev, const ibv_qp_cap &_qp_cap,
                         int tempmp_size, int _max_coros, int cq_size,
                         Directory *_dir)
    : dev(_dev), qp_cap(_qp_cap), max_coros(_max_coros), dir(_dir)
{
    // log_err("rdma_worker init start, tempmp_size: %d, max_coros: %d, cq_size: %d, max_send_wr: %d, max_recv_wr: %d", tempmp_size, max_coros, cq_size, qp_cap.max_send_wr, qp_cap.max_recv_wr);
    // server: max_send_wr: 0, max_recv_wr: 0
    // client: max_send_wr: 64, max_recv_wr: 0
    if (qp_cap.max_send_wr == 0 && qp_cap.max_recv_wr == 0)
    {
        if (!cq)
            assert_require(cq = dev.create_cq(1));
        return;
    }
    if (tempmp_size > 0)
    {
        assert_require(mp = new tempmp(tempmp_size));
        assert_require(mpmr = dev.create_mr(mp->get_data_len(), mp->get_data_addr()));
        // log_err("创建mpmr成功");
    }
    if (max_coros > 8)
    {
        assert_require(max_coros < rdma_coro_none);
        coros.resize(max_coros);
        free_head = 0;
        coros[0].id = 0;
        for (int i = 1; i < max_coros; ++i)
        {
            coros[i].id = i;
            coros[i - 1].next = i;
        }
        coros[max_coros - 1].next = rdma_coro_none;
    }
    if (!cq) {
        assert_require(cq = dev.create_cq(cq_size));
        // log_err("创建CQ: %p, cq_size: %d", cq, cq_size); // server: 1024, client: 64
    }
    // else log_err("cq: %p已经存在，不需要创建", cq);
#if !USE_TICKET_HASH
    if (qp_cap.max_recv_wr) {
        struct ibv_sge sge;
        struct ibv_recv_wr wr;
        ibv_recv_wr *bad;

        assert_require(srqs.size() == 0);
        for (uint64_t segloc = 0; segloc < (1 << SEPHASH::INIT_DEPTH); segloc++) {
#if MODIFIED
            CurSeg *cur_seg = reinterpret_cast<CurSeg *>(this->dir->segs[segloc].cur_seg_ptr);
#endif
#if USE_XRC
            ibv_srq *srq;
            assert_require(srq = dev.create_srq_ex(SLOT_PER_SEG));
            srqs.emplace_back(srq);

            uint32_t srq_num;
            assert_check(0 == ibv_get_srq_num(srqs[segloc], &srq_num));
            log_test("初始化时创建srqs[%d]: %p, srq_num: %u->%u, cq_size: %d，并发布%d个RECV", segloc, srqs[segloc], cur_seg->seg_meta.srq_num, srq_num, cq_size, SLOT_PER_SEG);
            cur_seg->seg_meta.srq_num = srq_num;
#else
            assert_require(srqs.emplace_back(dev.create_srq(cq_size)));
#endif
#if MODIFIED
            for (int i = 0; i < SLOT_PER_SEG; i++)
            {
                fill_recv_wr(&wr, &sge, &cur_seg->slots[i], sizeof(Slot), dev.seg_mr->lkey);
                wr.wr_id = wr_wo_await;
                assert_check(0 == ibv_post_srq_recv(srqs[segloc], &wr, &bad));
            }
            log_test("初始化时创建srqs[%d]: %p, cq_size: %d，并发布%d个RECV", segloc, srqs[segloc], cq_size, SLOT_PER_SEG);
#endif
        }
        _max_segloc = (1 << SEPHASH::INIT_DEPTH) - 1;
#if RDMA_SIGNAL
        assert_require(signal_srq = dev.create_srq(cq_size));   
        CurSeg *cur_seg = reinterpret_cast<CurSeg *>(this->dir->segs[0].cur_seg_ptr);
        for (int i = 0; i < SLOT_PER_SEG; i++)
        {
            fill_recv_wr(&wr, &sge, &cur_seg->slots[i], sizeof(Slot), dev.seg_mr->lkey); // 只用于接收RDMA_WRITE_IMM的imm_data，不会被写入
            wr.wr_id = wr_wo_await;
            assert_check(0 == ibv_post_srq_recv(signal_srq, &wr, &bad));
        }
#endif
        log_test("是server，创建srqs[0..15]和signal_srq: %p, cq_size: %d，并为signal_srq发布%d个RECV", signal_srq, cq_size, SLOT_PER_SEG);
        srq_cv.notify_all();
    } // else log_err("是client，不创建SRQ");
#endif
    if (qp_cap.max_recv_wr > 0)
        assert_require(pending_tasks = new task_ring());
    assert_require(yield_handler = new handle_ring(max_coros));
#if CORO_DEBUG
    if (!qp_cap.max_recv_wr) // client
        start_periodic_task();
#endif
}

rdma_worker::~rdma_worker()
{
#if CORO_DEBUG
    stop_periodic_task();
#endif
    if (cq && ibv_destroy_cq(cq))
        log_warn("failed to destory cq");
    // else if (cq) log_err("成功销毁CQ %p", cq);
    // else log_err("cq: %p已经销毁/未申请", cq);
    cq = nullptr;

    // log_err("准备销毁%d个SRQ", srqs.size());
    for (auto &srq : srqs)
    {
        if (srq && ibv_destroy_srq(srq))
        {
            log_warn("failed to destroy srq");
        }
        srq = nullptr;
    }
#if RDMA_SIGNAL
    if (signal_srq && ibv_destroy_srq(signal_srq))
        log_warn("failed to destory signal srq");
#endif
    if (mpmr)
        rdma_free_mr(mpmr, false);
    if (mp)
        delete mp;
    // if (coros)
    //     delete[] coros; // now the shared_ptr will delete it
    if (pending_tasks)
        delete pending_tasks;
    if (yield_handler)
        delete yield_handler;
#ifdef ENABLE_DOCA_DMA
    if (mpmmp && doca_mmap_destroy(mpmmp) != DOCA_SUCCESS)
        log_warn("failed to destroy tempmp mmap");
    if (dma_workq && doca_ctx_workq_rm(dma_ctx, dma_workq) != DOCA_SUCCESS)
        log_warn("failed to rm dma workq from ctx");
    if (dma_ctx && doca_ctx_stop(dma_ctx) != DOCA_SUCCESS)
        log_warn("failed to stop dma ctx");
    if (dma_ctx && doca_ctx_dev_rm(dma_ctx, dev.dma_dev) != DOCA_SUCCESS)
        log_warn("failed to rm ctx from dev");
    if (dma_workq && doca_workq_destroy(dma_workq) != DOCA_SUCCESS)
        log_warn("failed to destroy workq");
    if (dma && doca_dma_destroy(dma) != DOCA_SUCCESS)
        log_warn("failed to destroy dma");
    if (buf_inv && doca_buf_inventory_destroy(buf_inv) != DOCA_SUCCESS)
        log_warn("failed to destroy buf_inv");
    if (pending_dma_task)
        delete pending_dma_task;
#endif
}
rdma_coro *rdma_worker::alloc_coro(uint16_t conn_id DEBUG_LOCATION_DEFINE DEBUG_CORO_DEFINE)
{
    if (free_head == rdma_coro_none)
        return nullptr;
    auto local_head = free_head;
    rdma_coro *res = &coros[local_head];
#if ALLOC_CORO_THREAD_SAFE
    // 线程安全版本需要原子操作，但free_head不是指针了
    while (!__atomic_compare_exchange_n(&free_head, &local_head, res->next, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))
        res = &coros[local_head];
#else
    free_head = res->next;
#endif
    res->next = rdma_coro_none;
    res->coro_state = coro_state_invaild;
    res->resume_handler = nullptr;
    res->ctx = conn_id;
#if CORO_DEBUG
    res->location = location;
    res->coro_desc = desc;
    res->start_time = std::chrono::steady_clock::now();
#endif
    // log_err("从worker:%p,coros:%p中分配cor:%p, ID: %u, CtxID: %u, File: %s:%d", this, &coros, res, res->id, conn_id, res->location.file_name(), res->location.line());
    return res;
}

void rdma_worker::free_coro(rdma_coro *cor)
{
    cor->ctx = 0;
    cor->next = free_head;
#if ALLOC_CORO_THREAD_SAFE
    // 线程安全版本需要原子操作，但free_head不是指针了
    while (!__atomic_compare_exchange_n(&free_head, &cor->next, cor->id, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))
        ;
#else
    free_head = cor->id;
#endif
    // log_err("将cor:%p, ID: %u, CtxID: %u, File: %s:%d, 放回worker:%p, coros:%p的free_head中", cor, cor->id, cor->ctx, cor->location.file_name(), cor->location.line(), this, &coros);
}

#if CORO_DEBUG
void rdma_worker::print_running_coros()
{
    // std::map<std::string, int> location_count;
    auto now = std::chrono::steady_clock::now();

    // 遍历协程并统计每种 file:line 的出现次数
    for (uint16_t i = 0; i < max_coros; ++i)
    {
        // rdma_coro *cor = &coros[i];
        rdma_coro *cor = &coros[i]; // 使用 &(*coros)[index] 获取指针
        // if (cor->ctx != 0)
        // {
        //     auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - cor->start_time).count();
        //     if (elapsed >= 10)
        //     {
        //         log_err("终止超时coro ID: %u, CtxID: %u, File: %s:%d, 时间: %ld s", cor->id, cor->ctx, cor->location.file_name(), cor->location.line(), elapsed);
        //         cor->coro_state |= (coro_state_error | coro_state_ready);
        //         if (cor->coro_state & coro_state_inited)
        //             cor->resume_handler();
        //     }
        //     std::string location = std::string(cor->location.file_name()) + ":" + std::to_string(cor->location.line());
        //     if (elapsed >= 5)
        //     {
        //         // location_count[location]++;
        //         cor->print(std::format("运行超时，协程ID: %u, CtxID: %u, File: %s:%d, 时间: %ld s", cor->id, cor->ctx, cor->location.file_name(), cor->location.line(), elapsed));
        //     }
        // }
    }

    // 输出每种 file:line 的出现次数
    // if (!location_count.empty())
    // {
    //     log_err("========== Start of print_running_coros ==========");
    //     for (const auto &[location, count] : location_count)
    //     {
    //         log_err("File: %s, Count: %d" DEBUG_LOCATION_CALL_ARG.c_str(), count);
    //     }
    //     log_err("========== End of print_running_coros ==========");
    // }
}

void rdma_worker::start_periodic_task()
{
    // log_err("开始定期打印协程信息");
    periodic_thread = std::thread([this]()
                                  {
        std::unique_lock<std::mutex> lk(cv_m);
        while (!stop_flag.load()) {
            if (cv.wait_for(lk, std::chrono::seconds(5), [this] { return stop_flag.load(); })) {
                break;
            }
            print_running_coros();
        } });
}

void rdma_worker::stop_periodic_task()
{
    // log_err("停止定期打印协程信息");
    {
        std::lock_guard<std::mutex> lk(cv_m);
        stop_flag.store(true);
    }
    cv.notify_all();
    if (periodic_thread.joinable())
    {
        periodic_thread.join();
    }
}
#endif

task<> rdma_worker::cancel_coros(uint16_t conn_id, std::vector<int> &&cancel_list, volatile int &finish_flag)
{
    for (auto e : cancel_list)
        if (auto &&cor = coros[e]; cor.ctx == conn_id)
        {
            cor.coro_state |= (coro_state_error | coro_state_ready);
            if (cor.coro_state & coro_state_inited)
                cor.resume_handler();
        }
    finish_flag = 1;
    co_return;
}

void rdma_worker::worker_loop()
{
    ibv_wc wcs[rdma_max_wc_per_poll];
    int ready_num;
    loop_flag = true;
#ifdef ENABLE_DOCA_DMA
    struct doca_event event = {0};
    doca_error_t dma_workq_res;
#endif
#if CORO_DEBUG
    auto last_poll_time = std::chrono::steady_clock::now();
    int last_seconds = 0;
#endif
    while (loop_flag)
    {
        assert_require((ready_num = ibv_poll_cq(cq, rdma_max_wc_per_poll, wcs)) >= 0);
#if CORO_DEBUG
        auto current_time = std::chrono::steady_clock::now();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(current_time - last_poll_time).count();
        if (seconds != last_seconds) {
            if (seconds == 300) {
                log_err("卡住了%lu秒，输出所有segs=%lu的信息，global_depth: %lu", seconds, this->_max_segloc, dir->global_depth);
                for (int i = 0; i <= this->_max_segloc; i++) {
                    if (dir == nullptr || dir->segs == nullptr || !dir->segs[i].cur_seg_ptr) break;
                    CurSeg *cur_seg = reinterpret_cast<CurSeg *>(dir->segs[i].cur_seg_ptr);
                    cur_seg->seg_meta.print(std::format("segs[{}]的meta地址{}", i, static_cast<void *>(&cur_seg->seg_meta)));
                }
            }
        }
        last_seconds = seconds;
#endif

        for (int i = 0; i < ready_num; ++i)
        {
#if CORO_DEBUG
            last_poll_time = std::chrono::steady_clock::now();
#endif
            auto &&wc = wcs[i];
            // log_err("接收到 opcode: %d, status: %d, byte_len: %d, qp_num: %d, src_qp: %d, pkey_index: %d, slid: %d, sl: %d, dlid_path_bits: %d, imm_data: %d", wc.opcode, wc.status, wc.byte_len, wc.qp_num, wc.src_qp, wc.pkey_index, wc.slid, wc.sl, wc.dlid_path_bits, wc.imm_data);

            // if (wc.opcode == IBV_WC_SEND) {
            //     log_test("SEND完成, wr_id: %lu", wc.wr_id);
            // }
            // if (wc.opcode == IBV_WC_FETCH_ADD)
            // {
            //     log_test("FETCH_ADD完成, wr_id: %lu", wc.wr_id);
            // }
            // if (wc.opcode == IBV_WC_RDMA_READ) {
            //     log_err("RDMA_READ完成, wr_id: %lu", wc.wr_id);
            // }
            // if (wc.opcode == IBV_WC_RECV)
            // {
            //     static int recv_count = 0;
            //     // log_err("接收到RECV count: %d, slot_idx: %d", recv_count, recv_count % SLOT_PER_SEG);

            //     // if (recv_count % SLOT_PER_SEG == SLOT_PER_SEG - 1)
            //     // {
            //         // CurSeg *cur_seg = cur_seg_ptr_addr ? (CurSeg *)*cur_seg_ptr_addr : nullptr;
            //         // Slot *slot = &cur_seg->slots[SLOT_PER_SEG - 1];
            //         // slot->print();

            //     //     // 输出CurSegMeta->slot_cnt
            //     //     log_err("CurSegMeta->slot_cnt: %d", cur_seg->seg_meta.slot_cnt);
            //     // }
            //     recv_count++;
            // }
            if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
                struct ibv_sge sge;
                struct ibv_recv_wr wr;
                ibv_recv_wr *bad;

                uint32_t segloc = ntohl(wc.imm_data);
                std::bitset<32> segloc_bits(segloc);
                bool split_ok = segloc_bits.test(31);
                segloc_bits.reset(31);
                segloc = segloc_bits.to_ulong();

                // log_err("接收到RDMA_WITH_IMM, imm_data: %lu, ntohl: %lu, qp_num: %d", wc.imm_data, ntohl(wc.imm_data), wc.qp_num);
                CurSeg *cur_seg = reinterpret_cast<CurSeg *>(this->dir->segs[segloc].cur_seg_ptr);
                // cur_seg->seg_meta.print("before:");
#if RDMA_SIGNAL
                if (split_ok) {
                    // log_err("接收到SPLIT_OK信号，准备创建新的SRQ，并post RECV, segloc: %u", segloc);
                    if (segloc >= srqs.size())
                        srqs.resize(segloc + 1, nullptr);
                    assert_require(!srqs[segloc])
#if USE_XRC
                    assert_require(srqs[segloc] = dev.create_srq_ex(SLOT_PER_SEG));
#else
                    assert_require(srqs[segloc] = dev.create_srq(SLOT_PER_SEG));
#endif
                    for (int i = 0; i < SLOT_PER_SEG; i++)
                    {
                        fill_recv_wr(&wr, &sge, &cur_seg->slots[i], sizeof(Slot), dev.seg_mr->lkey);
                        wr.wr_id = wr_wo_await;
                        assert_check(0 == ibv_post_srq_recv(srqs[segloc], &wr, &bad));
                    }
                    log_test("分裂后创建新的srqs[%u]: %p，并发布%d个RECV", segloc, srqs[segloc], SLOT_PER_SEG);
#if USE_XRC
                    uint32_t srq_num;
                    assert_check(0 == ibv_get_srq_num(srqs[segloc], &srq_num));
                    log_test("分裂后创建新的srqs[%u]: %p, srq_num: %u->%u, 并发布%d个RECV", segloc, srqs[segloc], cur_seg->seg_meta.srq_num, srq_num, SLOT_PER_SEG);
                    cur_seg->seg_meta.srq_num = srq_num;
#endif
                    _max_segloc = segloc > _max_segloc ? segloc : _max_segloc;
                    srq_cv.notify_all();
                } else {
                    if (segloc >= srqs.size() || !srqs[segloc])
                    {
                        log_err("重新post RECV时，发现srqs[%u/%u]不存在，_max_segloc: %u，dir的global_depth: %u", segloc, srqs.size(), _max_segloc, dir->global_depth);
                        if (dir->segs[segloc].cur_seg_ptr)
                        {
                            CurSeg *cur_seg = reinterpret_cast<CurSeg *>(dir->segs[segloc].cur_seg_ptr);
                            cur_seg->seg_meta.print(std::format("第{}个CurSeg的meta地址{}", segloc, static_cast<void *>(&cur_seg->seg_meta)));
                        }
                        else
                            log_err("seg[%d] 不存在", segloc);
                    }
                    assert_require(segloc < srqs.size() && srqs[segloc])
                    for (int i = 0; i < SLOT_PER_SEG; i++)
                    {
                        fill_recv_wr(&wr, &sge, &cur_seg->slots[i], sizeof(Slot), dev.seg_mr->lkey);
                        wr.wr_id = wr_wo_await;
                        assert_check(0 == ibv_post_srq_recv(srqs[segloc], &wr, &bad));
                    }
                    // log_err("收到IMM，为srqs[%u]:%p发布%d个RECV, qp_num: %u", segloc, srqs[segloc], SLOT_PER_SEG, wc.qp_num);
                }
                if (signal_srq) {
                    fill_recv_wr(&wr, &sge, reinterpret_cast<CurSeg *>(dir->segs[0].cur_seg_ptr)->slots, sizeof(Slot), dev.seg_mr->lkey); // 象征性加上sizeof(Slot)，必须是合法地址不能是nullptr？
                    wr.wr_id = wr_wo_await;
                    assert_check(0 == ibv_post_srq_recv(signal_srq, &wr, &bad));
                }
#endif
                continue;
            }
            if(wc.wr_id == wr_wo_await){ // 可以修改为>=，这样当>=wr_wo_await时，可以减去wr_wo_await，得到存储的数据
                if (wc.status != IBV_WC_SUCCESS)
                {
                    log_err("got bad completion with status: 0x%x, vendor syndrome: 0x%x", wc.status, wc.vendor_err);
                }
                continue;
            }

            // auto cor = coros + wc.wr_id;
            rdma_coro *cor = &coros[wc.wr_id]; // 使用 &(*coros)[index] 获取指针
            if (wc.status != IBV_WC_SUCCESS)
            {
                log_err("got bad completion with status: 0x%x, vendor syndrome: 0x%x", wc.status, wc.vendor_err);
                cor->coro_state |= coro_state_error;
                cor->print("错误");
                assert_require(wc.status == IBV_WC_SUCCESS); // TODO: 细粒度合并
            }
            if (cor->coro_state & coro_state_inited)
            {
                log_debug("imm resume");
                cor->resume_handler();
            }
            else
            {
                log_debug("wait for resume");
                cor->coro_state |= coro_state_ready;
            }
        }
        if (pending_tasks && pending_tasks->count())
            pending_tasks->dequeue().start([](auto &&) {});
        if (yield_handler && yield_handler->count())
            yield_handler->dequeue()();
#ifdef ENABLE_DOCA_DMA
        if (dma_workq && (dma_workq_res = doca_workq_progress_retrieve(dma_workq, &event, DOCA_WORKQ_RETRIEVE_FLAGS_NONE)) != DOCA_ERROR_AGAIN)
        {
            assert_require(dma_workq_res == DOCA_SUCCESS && event.result.u64 == DOCA_SUCCESS); // TODO: state as future result
            pending_dma_task->dequeue()();
        }
#endif
    }
}

#ifdef ENABLE_DOCA_DMA
void rdma_worker::enable_dma(uint32_t workq_size, size_t buf_inv_size)
{
    assert_require(buf_inv == nullptr && dma == nullptr && dma_ctx == nullptr && dma_workq == nullptr);
    assert_require(doca_buf_inventory_create("buf_inventory", buf_inv_size, DOCA_BUF_EXTENSION_NONE, &buf_inv) == DOCA_SUCCESS);
    assert_require(doca_dma_create(&dma) == DOCA_SUCCESS);
    assert_require(dma_ctx = doca_dma_as_ctx(dma));
    assert_require(doca_workq_create(workq_size, &dma_workq) == DOCA_SUCCESS);
    assert_require(doca_buf_inventory_start(buf_inv) == DOCA_SUCCESS);
    assert_require(doca_ctx_dev_add(dma_ctx, dev.dma_dev) == DOCA_SUCCESS);
    assert_require(doca_ctx_start(dma_ctx) == DOCA_SUCCESS);
    assert_require(doca_ctx_workq_add(dma_ctx, dma_workq) == DOCA_SUCCESS);
    if (mp)
    {
        std::tie(mpmmp, std::ignore) = dev.create_mmap(dma_tempmp_mmap_name, mp->get_data_len(), mp->get_data_addr());
        assert_require(mpmmp);
    }
    assert_require(pending_dma_task = new handle_ring(workq_size));
}
#endif

rdma_server::~rdma_server()
{
    stop_serve();
}

void rdma_server::start_serve(std::function<task<>(rdma_conn*)> handler, int worker_num, const ibv_qp_cap &qp_cap,
                              int tempmp_size, int max_coros, int cq_size, int port, Directory *dir)
{
    // log_err("start_serve handler: %p, worker_num: %d, qp_cap: %p, tempmp_size: %d, max_coros: %d, cq_size: %d, port: %d", handler, worker_num, &qp_cap, tempmp_size, max_coros, cq_size, port);
    sockaddr_in local_addr;
    assert_require((listenfd = socket(AF_INET, SOCK_STREAM, 0)) != -1);

    // if (cur_seg_ptr_addr && *cur_seg_ptr_addr) {
    //     log_err("服务器提供了cur_seg_ptr: %lu", *cur_seg_ptr_addr);
    // } else log_err("客户端没有提供cur_seg_ptr");

    // set socket reuse and nonblock
    {
        int optval = 1;
        assert_require(0 == setsockopt(listenfd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)));

        int sflag;
        assert_require((sflag = fcntl(listenfd, F_GETFL, 0)) != -1 && fcntl(listenfd, F_SETFL, sflag | O_NONBLOCK) != -1);
    }

    bzero(&local_addr, sizeof(local_addr));
    local_addr.sin_family = AF_INET;
    local_addr.sin_port = htons(port);
    local_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    assert_require(::bind(listenfd, (sockaddr *)&local_addr, sizeof(sockaddr)) != -1);
    assert_require(::listen(listenfd, 5) != -1);

    if (handler) // client不调用start_serve，不会执行到这里
    {
        assert_require(worker_num >= 1);
        for (int i = 0; i < worker_num; ++i)
        {
            assert_require(workers.emplace_back(new rdma_worker(dev, qp_cap.max_send_wr == 0 && qp_cap.max_recv_wr == 0 ? sr_qp_cap : qp_cap, tempmp_size, max_coros, cq_size)));
            worker_threads.emplace_back(std::thread(&rdma_worker::worker_loop, workers[i]));
        }
    }
    else // server
    {
        assert_require(worker_num == 1);
        // assert_require(qp_cap.max_send_wr == 0 && qp_cap.max_recv_wr == 0);
        // workers.emplace_back(new rdma_worker(dev, qp_cap, 0, 0, 1));
        auto worker = new rdma_worker(dev, sr_qp_cap, tempmp_size, max_coros, cq_size, dir); // 允许server recv
        workers.emplace_back(worker);
        // worker_threads.emplace_back(std::thread()); // 之前server不轮询cq
        worker_threads.emplace_back(std::thread(&rdma_worker::worker_loop, workers[0])); // server只有一个worker，轮询它的cq
    }

    auto ser_loop = [this, handler, worker_num](const std::stop_token &st)
    {
        uint8_t recv_buf[rdma_sock_recv_buf_size];
        sock_select_list select_list(100);
        socklen_t addr_len = 0;
        sockaddr_in remote_addr;
        // std::map<uint64_t, bool> post_recv_flag_map; // 用于判断是否已经为srqs[i]post过recv
        // bool post_signal_recv_flag = false; // 用于signal_srq

        select_list.add(listenfd);
        while (!st.stop_requested())
        {
            if (select_list.select())
            {
                // auto &sks = select_list.sks;
                size_t sk_num = select_list.sks.size();
                for (size_t i = 0; i < sk_num; ++i)
                {
                    int sk = select_list.sks[i];
                    if (!select_list.isset(sk))
                        continue;
                    if (sk == listenfd)
                    {
                        int accepted_sock = ::accept(listenfd, (sockaddr *)&remote_addr, &addr_len);
                        assert_require(accepted_sock > 0);
                        select_list.add(accepted_sock);
                        log_info("accept conn: %d", accepted_sock);
                        ConnInfo conn_info = {ConnType::Normal, 0};
#if RDMA_SIGNAL
                        // 接收 conn_info
                        size_t recv_len = 0;
                        while ((recv_len += recv(accepted_sock, &conn_info + recv_len, sizeof(conn_info) - recv_len, 0)) < sizeof(conn_info))
                            ;
                        assert_require(recv_len == sizeof(conn_info));
                        assert_require(conn_info.segloc < 100000);
#endif
                        auto worker = workers[accepted_sock % worker_num];
                        if (conn_info.conn_type == ConnType::XRC_SEND) {
                            // log_err("接收到XRC_SEND的连接，类型更改为XRC_RECV");
                            conn_info.conn_type = ConnType::XRC_RECV;
                        }
                        // log_err("服务端接收到conn_info: %p, conn_type: %d, segloc: %lu", &conn_info, conn_info.conn_type, conn_info.segloc);
                        auto conn = new rdma_conn(worker, accepted_sock, conn_info); // IMPORTANT: 服务器创建QP
                        assert_require(conn);
                        sk2conn[accepted_sock] = conn;
#if RDMA_SIGNAL
                        log_test("创建qp_num: %d, 是否使用signal_srq: %d, is_signal_conn: %d, segloc: %lu", conn->qp->qp_num, conn->qp->srq == worker->signal_srq, conn_info.conn_type == ConnType::Signal, conn_info.segloc);
#endif
                    }
                    else
                    {
                        auto conn = sk2conn[sk];
                        // log_err("sock event: %d", sk);
                        auto recv_len = ::recv(sk, recv_buf, rdma_sock_recv_buf_size, 0);
                        if (recv_len <= 0)
                        {
// #if CLOSE_SOCKET
//                             if (!conn->segloc)
// #endif
                                delete conn;
                            sk2conn.erase(sk);
                            // log_err("thread: 关闭socket: %d, recv_len: %d", sk, recv_len);
                            close(sk);
                            select_list.del(sk);
                            log_info("connection closed: %d", sk);
                        }
                        else
                        {
                            switch (((__rdma_exchange_t *)recv_buf)->proto)
                            {
                            case rdma_exchange_proto_setup: {
                                ConnInfo conn_info = ((__rdma_exchange_t *)recv_buf)->conn_info;
                                conn->handle_recv_setup(recv_buf, recv_len, conn_info);
                                break;
                            }
                            case rdma_exchange_proto_ready:
                                if (handler)
                                    workers[sk % worker_num]->pending_tasks->enqueue(handler(conn));
#if CLOSE_SOCKET
                                sk2conn.erase(sk);
                                close(sk);
                                select_list.del(sk);
                                conn->sock = -1;
#endif
                                break;
                            case rdma_exchange_proto_reconnect:
                            {
                                int accepted_sock = conn->sock;
                                log_err("收到reconnect请求, sk: %d, accepted_sock: %d", sk, accepted_sock);
                                conn->sock = -1;
                                delete conn;
                                sk2conn.erase(sk);

                                ConnInfo conn_info = {ConnType::Normal, 0};
#if RDMA_SIGNAL
                                // 接收 conn_info
                                size_t recv_len = 0;
                                while ((recv_len += recv(accepted_sock, &conn_info + recv_len, sizeof(conn_info) - recv_len, 0)) < sizeof(conn_info))
                                    ;
                                assert_require(recv_len == sizeof(conn_info));
                                assert_require(conn_info.segloc < 100000);
#endif
                                auto worker = workers[accepted_sock % worker_num];
                                if (conn_info.conn_type == ConnType::XRC_SEND) {
                                    // log_err("接收到XRC_SEND的连接，类型更改为XRC_RECV");
                                    conn_info.conn_type = ConnType::XRC_RECV;
                                }
                                log_err("服务端接收到conn_info: %p, conn_type: %d, segloc: %lu", &conn_info, conn_info.conn_type, conn_info.segloc);
                                conn = new rdma_conn(worker, accepted_sock, conn_info); // IMPORTANT: 服务器创建QP
                                log_err("reconnect: 创建rdma_conn成功, sk: %d, conn: %p", accepted_sock, conn);
                                assert_require(conn);
                                sk2conn[accepted_sock] = conn;
                            }
                                break;

                            default:
                                break;
                            }
                        }
                    }
                }
            }
        }
    };
    sock_thread = std::jthread(ser_loop);
}

void rdma_server::stop_serve()
{
    sock_thread.request_stop();
    for (size_t i = 0; i < workers.size(); ++i)
    {
        workers[i]->loop_flag = false;
        if (worker_threads[i].joinable())
            worker_threads[i].join();
        delete workers[i];
    }
    worker_threads.clear();
    workers.clear();
    sk2conn.clear();
    sock_thread = std::jthread();
    if (listenfd != -1)
        close(listenfd);
    listenfd = -1;
}

rdma_conn *rdma_worker::connect(const char *host, int port, ConnInfo conn_info)
{
    uint8_t recv_buf[rdma_sock_recv_buf_size];
    int sock;
    sockaddr_in remote_addr;
    assert_require(-1 != (sock = socket(AF_INET, SOCK_STREAM, 0)));
    bzero(&remote_addr, sizeof(remote_addr));
    remote_addr.sin_family = AF_INET;
    remote_addr.sin_port = htons(port);
    remote_addr.sin_addr.s_addr = inet_addr(host);
    assert_require(-1 != ::connect(sock, (sockaddr *)&remote_addr, sizeof(sockaddr)));
    log_info("socket connected");

#if RDMA_SIGNAL
    conn_info.segloc = sock;
    assert_require(send(sock, &conn_info, sizeof(conn_info), 0) == sizeof(conn_info));
#endif

    auto conn = new rdma_conn(this, sock, conn_info); // IMPORTANT: 客户端创建QP
    if (!conn)
        log_err("failed to alloc conn");
    size_t recv_len = 0;
    while ((recv_len += recv(sock, recv_buf + recv_len, rdma_sock_recv_buf_size - recv_len, 0)) < sizeof(__rdma_exchange_t))
        ;
    if (recv_len == -1)
    {
        log_err("recv error, errno: %s, the server is DREAM server, but the client is RACE/SepHash client", strerror(errno));
        close(sock);
        return nullptr;
    }

    log_info("RTR -> RTS");
    conn->handle_recv_setup(recv_buf, recv_len, conn_info);
    recv_len = 0;
    while ((recv_len += recv(sock, recv_buf + recv_len, rdma_sock_recv_buf_size - recv_len, 0)) < sizeof(uint16_t))
        ;
    if (recv_len == -1)
    {
        log_err("recv error, errno: %s", strerror(errno));
        close(sock);
        return nullptr;
    }

    log_info("connect success");
    return conn;
}

// 新增：复用已有sock进行连接
rdma_conn *rdma_worker::reconnect(int sock, ConnInfo conn_info)
{
    uint8_t recv_buf[rdma_sock_recv_buf_size];

#if RDMA_SIGNAL
    conn_info.segloc = sock;
    // log_err("reconnect: 客户端调用send发送conn_info, sock: %d, conn_info: %p", sock, &conn_info);
    assert_require(send(sock, &conn_info, sizeof(conn_info), 0) == sizeof(conn_info));
#endif

    auto conn = new rdma_conn(this, sock, conn_info); // IMPORTANT: 客户端创建QP
    if (!conn)
        log_err("failed to alloc conn");
    size_t recv_len = 0;
    // log_err("reconnect: 客户端第一次调用recv, sock: %d, recv_len: %lu", sock, recv_len);
    while ((recv_len += recv(sock, recv_buf + recv_len, rdma_sock_recv_buf_size - recv_len, 0)) < sizeof(__rdma_exchange_t))
        ;
    // log_err("reconnect: 客户端第一次recv完成, sock: %d, recv_len: %lu", sock, recv_len);
    if (recv_len == -1)
    {
        log_err("reconnect: recv error, errno: %s", strerror(errno));
        close(sock);
        return nullptr;
    }

    log_info("reconnect: RTR -> RTS");
    conn->handle_recv_setup(recv_buf, recv_len, conn_info);
    recv_len = 0;
    // log_err("reconnect: 客户端第二次调用recv, sock: %d, recv_len: %lu", sock, recv_len);
    while ((recv_len += recv(sock, recv_buf + recv_len, rdma_sock_recv_buf_size - recv_len, 0)) < sizeof(uint16_t))
        ;
    // log_err("reconnect: 客户端第二次recv完成, sock: %d, recv_len: %lu", sock, recv_len);
    if (recv_len == -1)
    {
        log_err("reconnect: recv error, errno: %s", strerror(errno));
        close(sock);
        return nullptr;
    }

    log_info("reconnect: connect success");
    return conn;
}

rdma_conn::rdma_conn(rdma_worker *w, int _sock, ConnInfo conn_info)
    : dev(w->dev), worker(w), sock(_sock), conn_id(dev.alloc_conn_id())
{
    {
        if (conn_info.conn_type == ConnType::Normal || conn_info.conn_type == ConnType::Signal) {
            struct ibv_qp_init_attr qp_init_attr;
            memset(&qp_init_attr, 0, sizeof(qp_init_attr));
            qp_init_attr.qp_type = IBV_QPT_RC;
            qp_init_attr.sq_sig_all = 0;
            qp_init_attr.send_cq = worker->cq;
            qp_init_attr.recv_cq = worker->cq;
#if USE_XRC
            if (worker->qp_cap.max_recv_wr && conn_info.conn_type == ConnType::Signal)
                qp_init_attr.srq = worker->signal_srq;
#else
            if (worker->qp_cap.max_recv_wr) { // server
#if RDMA_SIGNAL
                if (conn_info.conn_type == ConnType::Signal)
                    qp_init_attr.srq = worker->signal_srq;
                else
#endif
#if !USE_TICKET_HASH
                {
                    if (conn_info.segloc >= worker->srqs.size() || !worker->srqs[conn_info.segloc])
                    {
                        std::unique_lock<std::mutex> lock(worker->srq_mutex);
                        log_test("等待srqs[%d]创建完成", conn_info.segloc);
                        worker->srq_cv.wait(lock, [this, &conn_info] { // 等待srq创建完成
                            return worker->srqs.size() > conn_info.segloc && worker->srqs[conn_info.segloc] != nullptr;
                        });
                        log_test("srqs[%d]创建完成", conn_info.segloc);
                    }
                    qp_init_attr.srq = worker->srqs[conn_info.segloc];
                }
#endif
            }
#endif
            qp_init_attr.cap = worker->qp_cap;
            assert_require(qp = ibv_create_qp(dev.pd, &qp_init_attr));
        } else {
            struct ibv_qp_init_attr_ex qp_init_attr;
            memset(&qp_init_attr, 0, sizeof(qp_init_attr));
            // qp_init_attr.qp_type = conn_info.conn_type == ConnType::XRC_RECV ? IBV_QPT_XRC_RECV : IBV_QPT_XRC_SEND;
            qp_init_attr.sq_sig_all = 0;
            

            if (conn_info.conn_type == ConnType::XRC_RECV) {
                qp_init_attr.qp_type = IBV_QPT_XRC_RECV;
                qp_init_attr.xrcd = dev.xrcd;
                qp_init_attr.comp_mask = IBV_QP_INIT_ATTR_XRCD;
                qp_init_attr.cap.max_recv_wr = 1024;
                qp_init_attr.cap.max_recv_sge = 1;
                qp_init_attr.cap.max_inline_data = 0;
            } else if (conn_info.conn_type == ConnType::XRC_SEND)
            {
                qp_init_attr.qp_type = IBV_QPT_XRC_SEND;
                qp_init_attr.send_cq = worker->cq;
                // qp_init_attr.recv_cq = worker->cq;

                qp_init_attr.cap.max_send_wr = 1024;
                qp_init_attr.cap.max_recv_wr = 0;
                qp_init_attr.cap.max_send_sge = 1;
                qp_init_attr.cap.max_recv_sge = 0;
                qp_init_attr.cap.max_inline_data = 0;

                qp_init_attr.comp_mask = IBV_QP_INIT_ATTR_PD;
                qp_init_attr.pd = dev.pd;
            }
            assert_require(qp = ibv_create_qp_ex(dev.ib_ctx, &qp_init_attr));
        }
    }

    assert_require(exchange_mr = dev.create_mr(rdma_info_mr_size, nullptr, mr_flag_lo));
    log_test("创建conn时创建exchange_mr成功");
    auto hdr = (rdma_infomr_hdr *)exchange_mr->addr;
    hdr->tail = 0;
    log_test("info_mr: %p, info_mr->addr: %p, info_mr->rkey: %u", dev.info_mr, dev.info_mr->addr, dev.info_mr->rkey);

    send_exchange(rdma_exchange_proto_setup, conn_info);
    log_info("new conn %d", sock);
}

rdma_conn::~rdma_conn()
{
    if (sock != -1)
        close(sock);
    if (qp && ibv_destroy_qp(qp))
        log_warn("failed to destroy qp");
    if (exchange_mr)
        rdma_free_mr(exchange_mr);
    release_working_coros();
    dev.free_conn_id(conn_id);
    log_info("close conn %d  %u", sock, conn_id);
}

void rdma_conn::release_working_coros()
{
    if (!worker || !worker->pending_tasks)
        return;
    std::vector<int> cancel_list;
    for (int i = 0; i < worker->max_coros; ++i)
        if (worker->coros[i].ctx == conn_id) cancel_list.push_back(i);
    volatile int finish_flag = 0;
    if (worker->loop_flag) // WARNING: should not close conn during stopping work loop
        worker->pending_tasks->enqueue(worker->cancel_coros(conn_id, std::move(cancel_list), finish_flag));
    else
        worker->run(worker->cancel_coros(conn_id, std::move(cancel_list), finish_flag));
    while (finish_flag == 0)
        ;
}

void rdma_conn::send_exchange(uint16_t proto, ConnInfo conn_info)
{
    __rdma_exchange_t exc;
    union ibv_gid local_gid;
    exc.proto = proto;
    exc.conn_info = conn_info;
    // log_err("send_exchange proto: %d", proto);
    if (proto == rdma_exchange_proto_setup)
    {
        if (dev.gid_idx >= 0)
        {
            assert_require(0 == ibv_query_gid(dev.ib_ctx, dev.ib_port, dev.gid_idx, &local_gid));
            memcpy(&exc.gid, &local_gid, sizeof(exc.gid));
        }
        else
            memset(&exc.gid, 0, sizeof(exc.gid));
        exc.lid = dev.port_attr.lid;
        exc.qp_num = qp->qp_num;
        exc.raddr = (uint64_t)dev.info_mr->addr;
        exc.rkey = dev.info_mr->rkey;
        assert_require(::send(sock, &exc, sizeof(__rdma_exchange_t), 0) == sizeof(__rdma_exchange_t));
    }
    else if (proto == rdma_exchange_proto_ready)
    {
        assert_require(::send(sock, &exc, sizeof(uint16_t), 0) == sizeof(uint16_t));
    }
    else if (proto == rdma_exchange_proto_reconnect)
    {
        assert_require(::send(sock, &exc, sizeof(uint16_t), 0) == sizeof(uint16_t));
    }
    else
        assert_require(0);
}

void rdma_conn::handle_recv_setup(const void *buf, size_t len, ConnInfo conn_info)
{
    auto exc = (const __rdma_exchange_t *)buf;
    memset(exchange_wr, 0, sizeof(ibv_send_wr));
    __exchange_sge.addr = (uint64_t)exchange_mr->addr;
    __exchange_sge.length = 0;
    __exchange_sge.lkey = exchange_mr->lkey;
    exchange_wr->sg_list = &__exchange_sge;
    exchange_wr->num_sge = 1;
    exchange_wr->opcode = IBV_WR_RDMA_READ;
    exchange_wr->wr.rdma.remote_addr = exc->raddr;
    exchange_wr->wr.rdma.rkey = exc->rkey;

    assert_require(!(modify_qp_to_init() ||
              modify_qp_to_rtr(exc->qp_num, exc->lid, &exc->gid) ||
              (modify_qp_to_rts())));

    send_exchange(rdma_exchange_proto_ready, conn_info);
}

int rdma_conn::modify_qp_to_init()
{
    const int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    struct ibv_qp_attr attr;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = dev.ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    if ((rc = ibv_modify_qp(qp, &attr, flags)))
        log_err("failed to modify QP state to INIT");
    return rc;
}

int rdma_conn::modify_qp_to_rtr(uint32_t rqp_num, uint16_t rlid, const ibv_gid *rgid)
{
    const int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                      IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    struct ibv_qp_attr attr;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    // attr.path_mtu = IBV_MTU_256;
    // attr.path_mtu = IBV_MTU_512;
    attr.path_mtu = IBV_MTU_1024;
    attr.dest_qp_num = rqp_num;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = rdma_max_rd_atomic;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = rlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = dev.ib_port;
    if (dev.gid_idx >= 0)
    {
        attr.ah_attr.is_global = 1;
        memcpy(&attr.ah_attr.grh.dgid, rgid, sizeof(ibv_gid));
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = dev.gid_idx;
        attr.ah_attr.grh.traffic_class = 0;
    }
    if ((rc = ibv_modify_qp(qp, &attr, flags)))
        log_err("failed to modify QP state to RTR");
    return rc;
}

int rdma_conn::modify_qp_to_rts()
{
#if LOW_MIN_RTR_TIMER
    const int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                      IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
#else
    const int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                      IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC; // | IBV_QP_MIN_RNR_TIMER;
#endif
    struct ibv_qp_attr attr;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7; // 7 仅对 rnr_retry 是无限值。对于 retry_cnt 7 实际上是 7 次重试。
    attr.rnr_retry = 7; // 值 7 是特殊的，指定在 RNR 的情况下重试无限次
#if LOW_MIN_RTR_TIMER
    attr.min_rnr_timer = 3; // 降低延迟。在开发过程中，最好将 attr.retry_cnt 和 attr.rnr_retry 设置为 0。
#endif
    attr.sq_psn = 0;
    attr.max_rd_atomic = rdma_max_rd_atomic; // FOR READ
    if ((rc = ibv_modify_qp(qp, &attr, flags)))
        log_err("failed to modify QP state to RTS");
    return rc;
}

struct rdma_rinfo
{
    uint32_t type : 2;
    uint32_t id : 30;
    union
    {
        uint32_t rkey;
        uint32_t len;
    };
    uint8_t data[0];
    uint64_t raddr;
};

task<> rdma_conn::update_cache()
{
    auto hdr = (rdma_infomr_hdr *)exchange_mr->addr;
    exchange_wr->sg_list->length = std::min((uint64_t)rdma_info_mr_size - sizeof(rdma_infomr_hdr), hdr->tail + rdma_info_mr_probing);
    assert_check(co_await do_send(exchange_wr, exchange_wr));
    while (crc64(hdr->data, std::min(hdr->tail, (uint64_t)rdma_info_mr_size - sizeof(rdma_infomr_hdr))) != hdr->cksum)
    {
        exchange_wr->sg_list->length = std::min((uint64_t)rdma_info_mr_size - sizeof(rdma_infomr_hdr), hdr->tail + rdma_info_mr_probing);
        assert_check(co_await do_send(exchange_wr, exchange_wr));
    }
    exchange_idx.clear();
    for (uint64_t i = 0; i < hdr->tail;)
    {
        auto info = (rdma_rinfo *)(hdr->data + i);
        if (info->type == rdma_info_type_mr)
        {
            exchange_idx.emplace((uint32_t)info->id, *(rdma_rmr *)(hdr->data + i));
            i += sizeof(rdma_rmr);
        }
        else if (info->type == rdma_info_type_mmap)
        {
            exchange_idx.emplace((uint32_t)info->id, std::string((char *)info->data, info->len));
            i += sizeof(rdma_rmmap) + info->len;
        }
        else
            log_err("unknown info type");
    }
}

task<rdma_rmr> rdma_conn::query_remote_mr(uint32_t mr_id)
{
    if (!exchange_idx.contains(mr_id))
        co_await update_cache();
    if (exchange_idx.contains(mr_id))
        if (auto res = std::get_if<rdma_rmr>(&exchange_idx[mr_id]))
            co_return *res;
    log_err("mr not found");
    co_return rdma_rmr{};
}

#ifdef ENABLE_DOCA_DMA
task<std::tuple<doca_mmap *, uint64_t>> rdma_conn::query_remote_mmap(uint32_t mmap_id)
{
    doca_mmap *mmp = nullptr;
    uint64_t addr = 0;
    if (!exchange_idx.contains(mmap_id))
        co_await update_cache();
    if (exchange_idx.contains(mmap_id))
        if (auto res = std::get_if<std::string>(&exchange_idx[mmap_id]))
        {
            assert_check(doca_mmap_create_from_export(std::to_string(mmap_id).c_str(), (uint8_t *)res->c_str(), res->length(), worker->dev.dma_dev, &mmp) == DOCA_SUCCESS);
            std::tie(addr, std::ignore) = get_addrlen_from_export(*res);
        }
    co_return std::make_tuple(mmp, addr);
}

void dma_future::await_resume()
{
    if (src_buf && doca_buf_refcount_rm(src_buf, nullptr) != DOCA_SUCCESS)
        log_warn("failed to free src buf");
    if (dst_buf && doca_buf_refcount_rm(dst_buf, nullptr) != DOCA_SUCCESS)
        log_warn("failed to free dst buf");
}

void *dma_buf_future::await_resume()
{
    dma_future::await_resume();
    return res_buf;
}

inline void fill_dma_job(doca_dma_job_memcpy *dma_job, doca_ctx *dma_ctx, doca_buf *src_buf, doca_buf *dst_buf, size_t len)
{
    dma_job->base.type = DOCA_DMA_JOB_MEMCPY;
    dma_job->base.flags = DOCA_JOB_FLAGS_NONE;
    dma_job->base.ctx = dma_ctx;
    dma_job->src_buff = src_buf;
    dma_job->dst_buff = dst_buf;
    dma_job->num_bytes_to_copy = len;
}

dma_buf_future rdma_conn::dma_read(doca_mmap *rmmp, uint64_t raddr, size_t len)
{
    auto [buf, job] = alloc_many(len, sizeof(doca_dma_job_memcpy));
    assert_check(buf);
    doca_buf *src_buf, *dst_buf;
    assert_check(doca_buf_inventory_buf_by_addr(worker->buf_inv, rmmp, (char *)raddr, len, &src_buf) == DOCA_SUCCESS);
    assert_check(doca_buf_inventory_buf_by_addr(worker->buf_inv, worker->mpmmp, (char *)buf, len, &dst_buf) == DOCA_SUCCESS);
    auto dma_job = (doca_dma_job_memcpy *)job;
    fill_dma_job(dma_job, worker->dma_ctx, src_buf, dst_buf, len);
    assert_check(doca_workq_submit(worker->dma_workq, &dma_job->base) == DOCA_SUCCESS);
    return dma_buf_future(worker, src_buf, dst_buf, buf);
}

dma_future rdma_conn::dma_read(doca_mmap *rmmp, uint64_t raddr, doca_mmap *lmmp, void *laddr, size_t len)
{
    auto dma_job = (doca_dma_job_memcpy *)alloc_buf(sizeof(doca_dma_job_memcpy));
    assert_check(dma_job);
    doca_buf *src_buf, *dst_buf;
    assert_check(doca_buf_inventory_buf_by_addr(worker->buf_inv, rmmp, (char *)raddr, len, &src_buf) == DOCA_SUCCESS);
    assert_check(doca_buf_inventory_buf_by_addr(worker->buf_inv, lmmp, (char *)laddr, len, &dst_buf) == DOCA_SUCCESS);
    fill_dma_job(dma_job, worker->dma_ctx, src_buf, dst_buf, len);
    assert_check(doca_workq_submit(worker->dma_workq, &dma_job->base) == DOCA_SUCCESS);
    auto res = dma_future(worker, src_buf, dst_buf);
    free_buf(dma_job);
    return res;
}

dma_future rdma_conn::dma_write(doca_mmap *rmmp, uint64_t raddr, doca_mmap *lmmp, void *laddr, size_t len)
{
    auto dma_job = (doca_dma_job_memcpy *)alloc_buf(sizeof(doca_dma_job_memcpy));
    assert_check(dma_job);
    doca_buf *src_buf, *dst_buf;
    assert_check(doca_buf_inventory_buf_by_addr(worker->buf_inv, rmmp, (char *)raddr, len, &dst_buf) == DOCA_SUCCESS);
    assert_check(doca_buf_inventory_buf_by_addr(worker->buf_inv, lmmp, (char *)laddr, len, &src_buf) == DOCA_SUCCESS);
    fill_dma_job(dma_job, worker->dma_ctx, src_buf, dst_buf, len);
    assert_check(doca_workq_submit(worker->dma_workq, &dma_job->base) == DOCA_SUCCESS);
    auto res = dma_future(worker, src_buf, dst_buf);
    free_buf(dma_job);
    return res;
}
#endif

rdma_future rdma_conn::do_send(ibv_send_wr *wr_begin, ibv_send_wr *wr_end DEBUG_LOCATION_DEFINE DEBUG_CORO_DEFINE)
{
    auto cor = worker->alloc_coro(conn_id DEBUG_LOCATION_CALL_ARG DEBUG_CORO_CALL_ARG);
    assert_check(cor);
    wr_end->wr_id = cor->id;
    wr_end->send_flags |= IBV_SEND_SIGNALED;
    ibv_send_wr *bad;
    int res = ibv_post_send(qp, wr_begin, &bad);
    if (res != 0) {
        log_err("ibv_post_send failed: %d", res);
        assert_check(0);
    }
    return rdma_future(cor, this);
}

rdma_future rdma_conn::do_recv(ibv_recv_wr *wr DEBUG_LOCATION_DEFINE)
{
    auto cor = worker->alloc_coro(conn_id DEBUG_LOCATION_CALL_ARG);
    assert_check(cor);
    wr->wr_id = cor->id; // wr_id不能存地址，因为已经用于协程。
    ibv_recv_wr *bad;
    if (qp->srq) {
        assert_check(0 == ibv_post_srq_recv(qp->srq, wr, &bad));
    }
    else {
        assert_check(0 == ibv_post_recv(qp, wr, &bad));
    }
    return rdma_future(cor, this);
}

int rdma_future::await_resume()
{
    if (conn && cor)
        conn->worker->free_coro(cor);
    return !(cor->coro_state & coro_state_error); // single thread thus cor is vaild after free
}

void *rdma_buffer_future::await_resume()
{
    if (!rdma_future::await_resume())
    {
        conn->free_buf(_res_buf);
        return nullptr;
    }
    return _res_buf;
}

int rdma_cas_future::await_resume()
{
    conn->free_buf(_res_buf); // single thread thus buf is vaild after free
    if (!rdma_future::await_resume())
        return false;
    auto res = *(uint64_t *)_res_buf == _cmpval;
    _cmpval = *(uint64_t *)_res_buf;
    return res;
}

int rdma_cas_n_future::await_resume()
{
    conn->free_buf(_res_buf);
    if (!rdma_future::await_resume())
        return false;
    return *(uint64_t *)_res_buf == _cmpval;
}

uint64_t rdma_faa_future::await_resume()
{
    conn->free_buf(_res_buf);
    if (!rdma_future::await_resume())
        throw "faa failed";
    return *(uint64_t *)_res_buf;
}

rdma_buffer_future rdma_conn::read(uint64_t raddr, uint32_t rkey, uint32_t len DEBUG_LOCATION_DEFINE DEBUG_CORO_DEFINE)
{
    auto [buf, sge, wr] = alloc_many(len, sizeof(ibv_sge), sizeof(ibv_send_wr));
    assert_check(buf);
    auto send_wr = (ibv_send_wr *)wr;
    fill_rw_wr<IBV_WR_RDMA_READ>(send_wr, (ibv_sge *)sge, raddr, rkey, buf, len, lkey());
    auto fur = do_send(send_wr, send_wr DEBUG_LOCATION_CALL_ARG DEBUG_CORO_CALL_ARG);
    return rdma_buffer_future(fur.cor, fur.conn, buf);
}

rdma_future rdma_conn::read(uint64_t raddr, uint32_t rkey, void *laddr, uint32_t len, uint32_t lkey DEBUG_LOCATION_DEFINE DEBUG_CORO_DEFINE)
{
    // if(raddr==0){
    //     log_err("zero raddr");
    //     // exit(-1);
    //     int* ptr = NULL;
    //     *ptr = 10; // 在这里引发段错误
    // }
    assert_check(laddr);
    auto [sge, wr] = alloc_many(sizeof(ibv_sge), sizeof(ibv_send_wr));
    assert_check(sge);
    auto send_wr = (ibv_send_wr *)wr;
    fill_rw_wr<IBV_WR_RDMA_READ>(send_wr, (ibv_sge *)sge, raddr, rkey, laddr, len, lkey);
    auto res = do_send(send_wr, send_wr DEBUG_LOCATION_CALL_ARG DEBUG_CORO_CALL_ARG);
    free_buf(sge);
    return res;
}

rdma_future rdma_conn::write(uint64_t raddr, uint32_t rkey, void *laddr, uint32_t len, uint32_t _lkey DEBUG_LOCATION_DEFINE)
{
    if(raddr==0){
        log_err("zero raddr");
        // exit(-1);
        int* ptr = NULL;
        *ptr = 10; // 在这里引发段错误
    }
    auto [sge,wr] = alloc_many(sizeof(ibv_sge),sizeof(ibv_send_wr));
    assert_check(sge);
    auto send_wr = (ibv_send_wr *)wr;
    fill_rw_wr<IBV_WR_RDMA_WRITE>(send_wr, (ibv_sge *)sge, raddr, rkey, laddr, len, _lkey);
    if (_lkey == 0)
        send_wr->send_flags = IBV_SEND_INLINE;
    auto res = do_send(send_wr, send_wr DEBUG_LOCATION_CALL_ARG);
    free_buf(sge);
    
    //For Flush
    // auto [sge,wr,rr] = alloc_many(sizeof(ibv_sge), sizeof(ibv_send_wr),sizeof(ibv_send_wr));
    // assert_check(sge);
    // auto send_wr = (ibv_send_wr *)wr;
    // auto read_wr = (ibv_send_wr *)rr;
    // fill_rw_wr<IBV_WR_RDMA_WRITE>(send_wr, (ibv_sge *)sge, raddr, rkey, laddr, len, _lkey);
    // fill_rw_wr<IBV_WR_RDMA_READ>(read_wr, (ibv_sge *)sge, raddr, rkey, laddr, len, _lkey);
    // send_wr->next = read_wr;
    // if (_lkey == 0)
    //     send_wr->send_flags = IBV_SEND_INLINE;
    // auto res = do_send(send_wr, send_wr);
    // free_buf(sge);
    return res;
}

rdma_future rdma_conn::write_with_imm(uint64_t raddr, uint32_t rkey, void *laddr, uint32_t len, uint32_t _lkey, uint32_t imm_data DEBUG_LOCATION_DEFINE)
{
    if (raddr == 0)
    {
        log_err("zero raddr");
        int *ptr = NULL;
        *ptr = 10; // 引发段错误
    }
    auto [sge, wr] = alloc_many(sizeof(ibv_sge), sizeof(ibv_send_wr));
    assert_check(sge);
    auto send_wr = (ibv_send_wr *)wr;
    fill_rw_wr<IBV_WR_RDMA_WRITE_WITH_IMM>(send_wr, (ibv_sge *)sge, raddr, rkey, laddr, len, _lkey);
    send_wr->imm_data = htonl(imm_data); // 设置 Immediate 数据
    if (_lkey == 0)
        send_wr->send_flags = IBV_SEND_INLINE;
    auto res = do_send(send_wr, send_wr DEBUG_LOCATION_CALL_ARG);
    free_buf(sge);
    return res;
}

void rdma_conn::pure_write(uint64_t raddr, uint32_t rkey, void *laddr, uint32_t len, uint32_t _lkey)
{    
    if(raddr==0){
        log_err("zero raddr");
        // exit(-1);
        int* ptr = NULL;
        *ptr = 10; // 在这里引发段错误
    }
    auto [sge,wr] = alloc_many(sizeof(ibv_sge),sizeof(ibv_send_wr));
    assert_check(sge);
    auto send_wr = (ibv_send_wr *)wr;
    fill_rw_wr<IBV_WR_RDMA_WRITE>(send_wr, (ibv_sge *)sge, raddr, rkey, laddr, len, _lkey);
    if (_lkey == 0)
        send_wr->send_flags = IBV_SEND_INLINE;
    send_wr->wr_id = wr_wo_await;
    send_wr->send_flags |= IBV_SEND_SIGNALED;
    ibv_send_wr *bad;
    int res = ibv_post_send(qp, send_wr, &bad);
    if (res != 0)
    {
        log_err("res:%d", res);
    }
    assert_check(0 == res);
    free_buf(sge);
}

void rdma_conn::pure_write_with_imm(uint64_t raddr, uint32_t rkey, void *laddr, uint32_t len, uint32_t _lkey, uint32_t imm_data)
{
    if (raddr == 0)
    {
        log_err("zero raddr");
        // exit(-1);
        int *ptr = NULL;
        *ptr = 10; // 在这里引发段错误
    }
    auto [sge, wr] = alloc_many(sizeof(ibv_sge), sizeof(ibv_send_wr));
    assert_check(sge);
    auto send_wr = (ibv_send_wr *)wr;
    fill_rw_wr<IBV_WR_RDMA_WRITE_WITH_IMM>(send_wr, (ibv_sge *)sge, raddr, rkey, laddr, len, _lkey);
    send_wr->imm_data = htonl(imm_data); // 设置 Immediate 数据
    if (_lkey == 0)
        send_wr->send_flags = IBV_SEND_INLINE;
    send_wr->wr_id = wr_wo_await;
    send_wr->send_flags |= IBV_SEND_SIGNALED;
    ibv_send_wr *bad;
    int res = ibv_post_send(qp, send_wr, &bad);
    if (res != 0)
    {
        log_err("res:%d", res);
    }
    assert_check(0 == res);
    free_buf(sge);
}

void rdma_conn::pure_send(void *laddr, uint32_t len, uint32_t _lkey)
{
    auto [sge, wr] = alloc_many(sizeof(ibv_sge), sizeof(ibv_send_wr));
    assert_check(sge);
    auto send_wr = (ibv_send_wr *)wr;
    fill_rw_wr<IBV_WR_SEND>(send_wr, (ibv_sge *)sge, 0, 0, laddr, len, _lkey);
    if (_lkey == 0)
        send_wr->send_flags = IBV_SEND_INLINE;
    send_wr->wr_id = wr_wo_await;
    send_wr->send_flags |= IBV_SEND_SIGNALED;
    ibv_send_wr *bad;
    assert_check(0 == ibv_post_send(qp, send_wr, &bad));
    free_buf(sge);
}

void rdma_conn::pure_read(uint64_t raddr, uint32_t rkey, void *laddr, uint32_t len, uint32_t _lkey)
{
    auto [sge, wr] = alloc_many(sizeof(ibv_sge), sizeof(ibv_send_wr));
    assert_check(sge);
    auto send_wr = (ibv_send_wr *)wr;
    fill_rw_wr<IBV_WR_RDMA_READ>(send_wr, (ibv_sge *)sge, raddr, rkey, laddr, len, _lkey);
    send_wr->wr_id = wr_wo_await;
    send_wr->send_flags |= IBV_SEND_SIGNALED;
    ibv_send_wr *bad;
    assert_check(0 == ibv_post_send(qp, send_wr, &bad));
    free_buf(sge);
}

void rdma_conn::pure_recv(void *laddr, uint32_t len, uint32_t lkey)
{
    auto [sge, wr] = alloc_many(sizeof(ibv_sge), sizeof(ibv_recv_wr));
    assert_check(sge);
    auto recv_wr = (ibv_recv_wr *)wr;
    fill_recv_wr(recv_wr, (ibv_sge *)sge, laddr, len, lkey);
    recv_wr->wr_id = wr_wo_await;
    ibv_recv_wr *bad;
    if (qp->srq) {
        assert_check(0 == ibv_post_srq_recv(qp->srq, recv_wr, &bad));
    }
    else {
        assert_check(0 == ibv_post_recv(qp, recv_wr, &bad));
    }
    free_buf(sge);
}

// rdma_cas_future rdma_conn::fetch_add(uint64_t raddr, uint32_t rkey, uint64_t &cmpval, uint64_t swapval)
rdma_cas_future rdma_conn::fetch_add(uint64_t raddr, uint32_t rkey, uint64_t &fetch, uint64_t addval DEBUG_LOCATION_DEFINE)
{
    auto [buf, sge, wr] = alloc_many(sizeof(uint64_t), sizeof(ibv_sge), sizeof(ibv_send_wr));
    assert_check(buf);
    auto send_wr = (ibv_send_wr *)wr;
    fill_atomic_wr<IBV_WR_ATOMIC_FETCH_AND_ADD>(send_wr, (ibv_sge *)sge, raddr, rkey, buf, lkey(), addval, 0);
    auto fur = do_send(send_wr, send_wr DEBUG_LOCATION_CALL_ARG);
    return rdma_cas_future(fur.cor, fur.conn, buf, fetch);
}

rdma_cas_future rdma_conn::cas(uint64_t raddr, uint32_t rkey, uint64_t &cmpval, uint64_t swapval DEBUG_LOCATION_DEFINE)
{
    if(raddr==0){
        log_err("zero raddr");
        // exit(-1);
        int* ptr = NULL;
        *ptr = 10; // 在这里引发段错误
    }
    auto [buf, sge, wr] = alloc_many(sizeof(uint64_t), sizeof(ibv_sge), sizeof(ibv_send_wr));
    assert_check(buf);
    auto send_wr = (ibv_send_wr *)wr;
    fill_atomic_wr<IBV_WR_ATOMIC_CMP_AND_SWP>(send_wr, (ibv_sge *)sge, raddr, rkey, buf, lkey(), cmpval, swapval);
    auto fur = do_send(send_wr, send_wr DEBUG_LOCATION_CALL_ARG);
    return rdma_cas_future(fur.cor, fur.conn, buf, cmpval);
}

rdma_cas_n_future rdma_conn::cas_n(uint64_t raddr, uint32_t rkey, uint64_t cmpval, uint64_t swapval DEBUG_LOCATION_DEFINE)
{
    if(raddr==0){
        log_err("zero raddr");
        // exit(-1);
        int* ptr = NULL;
        *ptr = 10; // 在这里引发段错误
    }
    auto [buf, sge, wr] = alloc_many(sizeof(uint64_t), sizeof(ibv_sge), sizeof(ibv_send_wr));
    assert_check(buf);
    auto send_wr = (ibv_send_wr *)wr;
    fill_atomic_wr<IBV_WR_ATOMIC_CMP_AND_SWP>(send_wr, (ibv_sge *)sge, raddr, rkey, buf, lkey(), cmpval, swapval);
    auto fur = do_send(send_wr, send_wr DEBUG_LOCATION_CALL_ARG);
    return rdma_cas_n_future(fur.cor, fur.conn, buf, cmpval);
}

rdma_faa_future rdma_conn::faa(uint64_t raddr, uint32_t rkey, uint64_t addval DEBUG_LOCATION_DEFINE)
{
    auto [buf, sge, wr] = alloc_many(sizeof(uint64_t), sizeof(ibv_sge), sizeof(ibv_send_wr));
    assert_check(buf);
    auto send_wr = (ibv_send_wr *)wr;
    fill_atomic_wr<IBV_WR_ATOMIC_FETCH_AND_ADD>(send_wr, (ibv_sge *)sge, raddr, rkey, buf, lkey(), addval, 0);
    auto fur = do_send(send_wr, send_wr DEBUG_LOCATION_CALL_ARG);
    return rdma_faa_future(fur.cor, fur.conn, buf);
}

rdma_future rdma_conn::send(void *laddr, uint32_t len, uint32_t _lkey, uint32_t remote_srqn DEBUG_LOCATION_DEFINE DEBUG_CORO_DEFINE)
{
    auto [sge, wr] = alloc_many(sizeof(ibv_sge), sizeof(ibv_send_wr));
    assert_check(sge);
    auto send_wr = (ibv_send_wr *)wr;
    fill_rw_wr<IBV_WR_SEND>(send_wr, (ibv_sge *)sge, 0, 0, laddr, len, _lkey);
    if (_lkey == 0)
        send_wr->send_flags = IBV_SEND_INLINE;
#if USE_XRC
    if (remote_srqn) {
        send_wr->qp_type.xrc.remote_srqn = remote_srqn;
        // 输出 wr 参数中的所有内容用于调试
        log_test("SEND: wr_id: %lu, next: %p, sg_list.addr: %lu, sg_list.length: %u, sg_list.lkey: %u, num_sge: %d, opcode: %d, send_flags: %d, qp_type.xrc.remote_srqn: %u\n", 
               send_wr->wr_id, send_wr->next, send_wr->sg_list->addr, send_wr->sg_list->length, send_wr->sg_list->lkey, send_wr->num_sge, send_wr->opcode, send_wr->send_flags, send_wr->qp_type.xrc.remote_srqn);
    }
#endif
    auto res = do_send(send_wr, send_wr DEBUG_LOCATION_CALL_ARG DEBUG_CORO_CALL_ARG);
    free_buf(sge);
    return res;
}

rdma_future rdma_conn::send_with_imm(void *laddr, uint32_t len, uint32_t _lkey, uint32_t imm_data DEBUG_LOCATION_DEFINE DEBUG_CORO_DEFINE)
{
    auto [sge, wr] = alloc_many(sizeof(ibv_sge), sizeof(ibv_send_wr));
    assert_check(sge);
    auto send_wr = (ibv_send_wr *)wr;
    fill_rw_wr<IBV_WR_SEND_WITH_IMM>(send_wr, (ibv_sge *)sge, 0, 0, laddr, len, _lkey);
    send_wr->imm_data = htonl(imm_data); // 设置 Immediate 数据
    if (_lkey == 0)
        send_wr->send_flags = IBV_SEND_INLINE;
    auto res = do_send(send_wr, send_wr DEBUG_LOCATION_CALL_ARG DEBUG_CORO_CALL_ARG);
    free_buf(sge);
    return res;
}

rdma_future rdma_conn::send_then_read(void *send_laddr, uint32_t send_len, uint32_t send_lkey, uint64_t read_raddr, uint32_t read_rkey, void *read_laddr, uint32_t read_len, uint32_t read_lkey DEBUG_LOCATION_DEFINE)
{
    // 分配多个 SGE 和 WR，用于 send 和 read 操作
    auto [sge_send, sge_read, wr_send, wr_read] = alloc_many(sizeof(ibv_sge), sizeof(ibv_sge),
                                                             sizeof(ibv_send_wr), sizeof(ibv_send_wr));
    assert_check(sge_send && sge_read && wr_send && wr_read);

    // 填充 send 的 WR
    auto send_wr = (ibv_send_wr *)wr_send;
    fill_rw_wr<IBV_WR_SEND>(send_wr, (ibv_sge *)sge_send, 0, 0, send_laddr, send_len, send_lkey);
    if (send_lkey == 0)
        send_wr->send_flags = IBV_SEND_INLINE;

    // 填充 read 的 WR
    auto read_wr = (ibv_send_wr *)wr_read;
    fill_rw_wr<IBV_WR_RDMA_READ>(read_wr, (ibv_sge *)sge_read, read_raddr, read_rkey, read_laddr, read_len, read_lkey);

    // 使用 IBV_SEND_FENCE 确保 send 完成后再执行 read
    send_wr->send_flags |= IBV_SEND_FENCE;

    // 将 send_wr 的 next 指针指向 read_wr，形成链表
    send_wr->next = read_wr;
    read_wr->next = nullptr;

    // 发起 send 和 read 操作，使用 doorbell batching
    auto res = do_send(send_wr, read_wr DEBUG_LOCATION_CALL_ARG);

    // 释放分配的内存
    free_buf(sge_send);
    free_buf(sge_read);

    return res;
}

rdma_future rdma_conn::send_then_fetch_add(void *send_laddr, uint32_t send_len, uint32_t send_lkey, uint64_t faa_raddr, uint32_t faa_rkey, uint64_t &faa_fetch, uint64_t faa_addval DEBUG_LOCATION_DEFINE)
{
    // 分配多个 SGE 和 WR，用于 send 和 fetch_add 操作
    auto [sge_send, sge_faa, wr_send, wr_faa, buf_faa] = alloc_many(sizeof(ibv_sge), sizeof(ibv_sge),
                                                                    sizeof(ibv_send_wr), sizeof(ibv_send_wr), sizeof(uint64_t));
    assert_check(sge_send && sge_faa && wr_send && wr_faa && buf_faa);

    // 填充 send 的 WR
    auto send_wr = (ibv_send_wr *)wr_send;
    fill_rw_wr<IBV_WR_SEND>(send_wr, (ibv_sge *)sge_send, 0, 0, send_laddr, send_len, send_lkey);
    if (send_lkey == 0)
        send_wr->send_flags = IBV_SEND_INLINE;

    // 填充 fetch_add 的 WR
    auto faa_wr = (ibv_send_wr *)wr_faa;
    fill_atomic_wr<IBV_WR_ATOMIC_FETCH_AND_ADD>(faa_wr, (ibv_sge *)sge_faa, faa_raddr, faa_rkey, buf_faa, lkey(), faa_addval, 0);

    // 使用 IBV_SEND_FENCE 确保 send 完成后再执行 fetch_add
    // send_wr->send_flags |= IBV_SEND_FENCE;

    // 将 send_wr 的 next 指针指向 faa_wr，形成链表
    send_wr->next = faa_wr;
    faa_wr->next = nullptr;

    // 发起 send 和 fetch_add 操作，使用 doorbell batching
    auto res = do_send(send_wr, faa_wr DEBUG_LOCATION_CALL_ARG);

    // 释放分配的内存
    free_buf(sge_send);
    free_buf(sge_faa);

    // return res;
    return rdma_cas_future(res.cor, res.conn, buf_faa, faa_fetch);
}

rdma_buffer_future rdma_conn::recv(uint32_t len DEBUG_LOCATION_DEFINE)
{
    auto [buf, sge, wr] = alloc_many(len, sizeof(ibv_sge), sizeof(ibv_recv_wr));
    assert_check(buf);
    auto recv_wr = (ibv_recv_wr *)wr;
    fill_recv_wr(recv_wr, (ibv_sge *)sge, buf, len, lkey());
    auto fur = do_recv(recv_wr DEBUG_LOCATION_CALL_ARG);
    return rdma_buffer_future(fur.cor, fur.conn, buf);
}

rdma_future rdma_conn::recv(void *laddr, uint32_t len, uint32_t lkey DEBUG_LOCATION_DEFINE)
{
    auto [sge, wr] = alloc_many(sizeof(ibv_sge), sizeof(ibv_recv_wr));
    assert_check(sge);
    auto recv_wr = (ibv_recv_wr *)wr;
    fill_recv_wr(recv_wr, (ibv_sge *)sge, laddr, len, lkey);
    auto fur = do_recv(recv_wr DEBUG_LOCATION_CALL_ARG);
    free_buf(sge);
    return fur;
}

task<uint32_t> rdma_conn::fill(uint64_t raddr, uint32_t rkey, uint32_t rlen, void *fill_val, uint32_t fill_val_len, uint32_t work_buf_size DEBUG_LOCATION_DEFINE)
{
    assert_check(is_times_ofN(work_buf_size, fill_val_len));
    auto [buf, sge, wr] = alloc_many(work_buf_size, sizeof(ibv_sge), sizeof(ibv_send_wr));
    assert_check(buf);
    uint32_t filled = 0;
    for (uint8_t *wbuf = (uint8_t *)buf; filled < work_buf_size;)
    {
        memcpy(wbuf, fill_val, fill_val_len);
        filled += fill_val_len;
        wbuf += fill_val_len;
    }
    auto send_wr = (ibv_send_wr *)wr;
    fill_rw_wr<IBV_WR_RDMA_WRITE>(send_wr, (ibv_sge *)sge, raddr, rkey, buf, work_buf_size, lkey());
    filled = 0;
    for (; filled + work_buf_size <= rlen;)
    {
        if (!co_await do_send(send_wr, send_wr DEBUG_LOCATION_CALL_ARG))
            co_return filled;
        send_wr->wr.rdma.remote_addr += work_buf_size;
        filled += work_buf_size;
    }
    if (filled < rlen)
    {
        send_wr->sg_list->length = rlen - filled;
        if (!co_await do_send(send_wr, send_wr DEBUG_LOCATION_CALL_ARG))
            co_return filled;
        filled = rlen;
    }
    free_buf(buf);
    co_return filled;
}

#ifdef ENABLE_DOCA_DMA
void free_mmap(doca_mmap *mmp)
{
    if (doca_mmap_destroy(mmp) != DOCA_SUCCESS)
        log_warn("failed to destroy mmap");
}
std::tuple<uint64_t, uint64_t> get_addrlen_from_export(std::string &export_str)
{
    struct json_object *from_export_json;
    struct json_object *addr;
    struct json_object *jlen;
    from_export_json = json_tokener_parse(export_str.c_str());
    json_object_object_get_ex(from_export_json, "addr", &addr);
    json_object_object_get_ex(from_export_json, "len", &jlen);
    auto res = std::make_tuple(json_object_get_int64(addr), json_object_get_int64(jlen));
    json_object_put(from_export_json);
    return res;
}
#endif

// crc64 impl
static const uint64_t crc64_tab[256] = {0x0000000000000000ULL, 0x7ad870c830358979ULL, 0xf5b0e190606b12f2ULL, 0x8f689158505e9b8bULL, 0xc038e5739841b68fULL, 0xbae095bba8743ff6ULL, 0x358804e3f82aa47dULL, 0x4f50742bc81f2d04ULL, 0xab28ecb46814fe75ULL, 0xd1f09c7c5821770cULL, 0x5e980d24087fec87ULL, 0x24407dec384a65feULL, 0x6b1009c7f05548faULL, 0x11c8790fc060c183ULL, 0x9ea0e857903e5a08ULL, 0xe478989fa00bd371ULL, 0x7d08ff3b88be6f81ULL, 0x07d08ff3b88be6f8ULL, 0x88b81eabe8d57d73ULL, 0xf2606e63d8e0f40aULL, 0xbd301a4810ffd90eULL, 0xc7e86a8020ca5077ULL, 0x4880fbd87094cbfcULL, 0x32588b1040a14285ULL, 0xd620138fe0aa91f4ULL, 0xacf86347d09f188dULL, 0x2390f21f80c18306ULL, 0x594882d7b0f40a7fULL, 0x1618f6fc78eb277bULL, 0x6cc0863448deae02ULL, 0xe3a8176c18803589ULL, 0x997067a428b5bcf0ULL, 0xfa11fe77117cdf02ULL, 0x80c98ebf2149567bULL, 0x0fa11fe77117cdf0ULL, 0x75796f2f41224489ULL, 0x3a291b04893d698dULL, 0x40f16bccb908e0f4ULL, 0xcf99fa94e9567b7fULL, 0xb5418a5cd963f206ULL, 0x513912c379682177ULL, 0x2be1620b495da80eULL, 0xa489f35319033385ULL, 0xde51839b2936bafcULL, 0x9101f7b0e12997f8ULL, 0xebd98778d11c1e81ULL, 0x64b116208142850aULL, 0x1e6966e8b1770c73ULL, 0x8719014c99c2b083ULL, 0xfdc17184a9f739faULL, 0x72a9e0dcf9a9a271ULL, 0x08719014c99c2b08ULL, 0x4721e43f0183060cULL, 0x3df994f731b68f75ULL, 0xb29105af61e814feULL, 0xc849756751dd9d87ULL, 0x2c31edf8f1d64ef6ULL, 0x56e99d30c1e3c78fULL, 0xd9810c6891bd5c04ULL, 0xa3597ca0a188d57dULL, 0xec09088b6997f879ULL, 0x96d1784359a27100ULL, 0x19b9e91b09fcea8bULL, 0x636199d339c963f2ULL, 0xdf7adabd7a6e2d6fULL, 0xa5a2aa754a5ba416ULL, 0x2aca3b2d1a053f9dULL, 0x50124be52a30b6e4ULL, 0x1f423fcee22f9be0ULL, 0x659a4f06d21a1299ULL, 0xeaf2de5e82448912ULL, 0x902aae96b271006bULL, 0x74523609127ad31aULL, 0x0e8a46c1224f5a63ULL, 0x81e2d7997211c1e8ULL, 0xfb3aa75142244891ULL, 0xb46ad37a8a3b6595ULL, 0xceb2a3b2ba0eececULL, 0x41da32eaea507767ULL, 0x3b024222da65fe1eULL, 0xa2722586f2d042eeULL, 0xd8aa554ec2e5cb97ULL, 0x57c2c41692bb501cULL, 0x2d1ab4dea28ed965ULL, 0x624ac0f56a91f461ULL, 0x1892b03d5aa47d18ULL, 0x97fa21650afae693ULL, 0xed2251ad3acf6feaULL, 0x095ac9329ac4bc9bULL, 0x7382b9faaaf135e2ULL, 0xfcea28a2faafae69ULL, 0x8632586aca9a2710ULL, 0xc9622c4102850a14ULL, 0xb3ba5c8932b0836dULL, 0x3cd2cdd162ee18e6ULL, 0x460abd1952db919fULL, 0x256b24ca6b12f26dULL, 0x5fb354025b277b14ULL, 0xd0dbc55a0b79e09fULL, 0xaa03b5923b4c69e6ULL, 0xe553c1b9f35344e2ULL, 0x9f8bb171c366cd9bULL, 0x10e3202993385610ULL, 0x6a3b50e1a30ddf69ULL, 0x8e43c87e03060c18ULL, 0xf49bb8b633338561ULL, 0x7bf329ee636d1eeaULL, 0x012b592653589793ULL, 0x4e7b2d0d9b47ba97ULL, 0x34a35dc5ab7233eeULL, 0xbbcbcc9dfb2ca865ULL, 0xc113bc55cb19211cULL, 0x5863dbf1e3ac9decULL, 0x22bbab39d3991495ULL, 0xadd33a6183c78f1eULL, 0xd70b4aa9b3f20667ULL, 0x985b3e827bed2b63ULL, 0xe2834e4a4bd8a21aULL, 0x6debdf121b863991ULL, 0x1733afda2bb3b0e8ULL, 0xf34b37458bb86399ULL, 0x8993478dbb8deae0ULL, 0x06fbd6d5ebd3716bULL, 0x7c23a61ddbe6f812ULL, 0x3373d23613f9d516ULL, 0x49aba2fe23cc5c6fULL, 0xc6c333a67392c7e4ULL, 0xbc1b436e43a74e9dULL, 0x95ac9329ac4bc9b5ULL, 0xef74e3e19c7e40ccULL, 0x601c72b9cc20db47ULL, 0x1ac40271fc15523eULL, 0x5594765a340a7f3aULL, 0x2f4c0692043ff643ULL, 0xa02497ca54616dc8ULL, 0xdafce7026454e4b1ULL, 0x3e847f9dc45f37c0ULL, 0x445c0f55f46abeb9ULL, 0xcb349e0da4342532ULL, 0xb1eceec59401ac4bULL, 0xfebc9aee5c1e814fULL, 0x8464ea266c2b0836ULL, 0x0b0c7b7e3c7593bdULL, 0x71d40bb60c401ac4ULL, 0xe8a46c1224f5a634ULL, 0x927c1cda14c02f4dULL, 0x1d148d82449eb4c6ULL, 0x67ccfd4a74ab3dbfULL, 0x289c8961bcb410bbULL, 0x5244f9a98c8199c2ULL, 0xdd2c68f1dcdf0249ULL, 0xa7f41839ecea8b30ULL, 0x438c80a64ce15841ULL, 0x3954f06e7cd4d138ULL, 0xb63c61362c8a4ab3ULL, 0xcce411fe1cbfc3caULL, 0x83b465d5d4a0eeceULL, 0xf96c151de49567b7ULL, 0x76048445b4cbfc3cULL, 0x0cdcf48d84fe7545ULL, 0x6fbd6d5ebd3716b7ULL, 0x15651d968d029fceULL, 0x9a0d8ccedd5c0445ULL, 0xe0d5fc06ed698d3cULL, 0xaf85882d2576a038ULL, 0xd55df8e515432941ULL, 0x5a3569bd451db2caULL, 0x20ed197575283bb3ULL, 0xc49581ead523e8c2ULL, 0xbe4df122e51661bbULL, 0x3125607ab548fa30ULL, 0x4bfd10b2857d7349ULL, 0x04ad64994d625e4dULL, 0x7e7514517d57d734ULL, 0xf11d85092d094cbfULL, 0x8bc5f5c11d3cc5c6ULL, 0x12b5926535897936ULL, 0x686de2ad05bcf04fULL, 0xe70573f555e26bc4ULL, 0x9ddd033d65d7e2bdULL, 0xd28d7716adc8cfb9ULL, 0xa85507de9dfd46c0ULL, 0x273d9686cda3dd4bULL, 0x5de5e64efd965432ULL, 0xb99d7ed15d9d8743ULL, 0xc3450e196da80e3aULL, 0x4c2d9f413df695b1ULL, 0x36f5ef890dc31cc8ULL, 0x79a59ba2c5dc31ccULL, 0x037deb6af5e9b8b5ULL, 0x8c157a32a5b7233eULL, 0xf6cd0afa9582aa47ULL, 0x4ad64994d625e4daULL, 0x300e395ce6106da3ULL, 0xbf66a804b64ef628ULL, 0xc5bed8cc867b7f51ULL, 0x8aeeace74e645255ULL, 0xf036dc2f7e51db2cULL, 0x7f5e4d772e0f40a7ULL, 0x05863dbf1e3ac9deULL, 0xe1fea520be311aafULL, 0x9b26d5e88e0493d6ULL, 0x144e44b0de5a085dULL, 0x6e963478ee6f8124ULL, 0x21c640532670ac20ULL, 0x5b1e309b16452559ULL, 0xd476a1c3461bbed2ULL, 0xaeaed10b762e37abULL, 0x37deb6af5e9b8b5bULL, 0x4d06c6676eae0222ULL, 0xc26e573f3ef099a9ULL, 0xb8b627f70ec510d0ULL, 0xf7e653dcc6da3dd4ULL, 0x8d3e2314f6efb4adULL, 0x0256b24ca6b12f26ULL, 0x788ec2849684a65fULL, 0x9cf65a1b368f752eULL, 0xe62e2ad306bafc57ULL, 0x6946bb8b56e467dcULL, 0x139ecb4366d1eea5ULL, 0x5ccebf68aecec3a1ULL, 0x2616cfa09efb4ad8ULL, 0xa97e5ef8cea5d153ULL, 0xd3a62e30fe90582aULL, 0xb0c7b7e3c7593bd8ULL, 0xca1fc72bf76cb2a1ULL, 0x45775673a732292aULL, 0x3faf26bb9707a053ULL, 0x70ff52905f188d57ULL, 0x0a2722586f2d042eULL, 0x854fb3003f739fa5ULL, 0xff97c3c80f4616dcULL, 0x1bef5b57af4dc5adULL, 0x61372b9f9f784cd4ULL, 0xee5fbac7cf26d75fULL, 0x9487ca0fff135e26ULL, 0xdbd7be24370c7322ULL, 0xa10fceec0739fa5bULL, 0x2e675fb4576761d0ULL, 0x54bf2f7c6752e8a9ULL, 0xcdcf48d84fe75459ULL, 0xb71738107fd2dd20ULL, 0x387fa9482f8c46abULL, 0x42a7d9801fb9cfd2ULL, 0x0df7adabd7a6e2d6ULL, 0x772fdd63e7936bafULL, 0xf8474c3bb7cdf024ULL, 0x829f3cf387f8795dULL, 0x66e7a46c27f3aa2cULL, 0x1c3fd4a417c62355ULL, 0x935745fc4798b8deULL, 0xe98f353477ad31a7ULL, 0xa6df411fbfb21ca3ULL, 0xdc0731d78f8795daULL, 0x536fa08fdfd90e51ULL, 0x29b7d047efec8728ULL};

uint64_t crc64(const void *data, size_t l)
{
    uint64_t crc = 0;
    uint8_t *s = (uint8_t *)data;

    for (size_t i = 0; i < l; ++i)
        crc = crc64_tab[(uint8_t)crc ^ s[i]] ^ (crc >> 8);

    return crc;
}