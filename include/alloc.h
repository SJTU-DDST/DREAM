#pragma once
#include <stdint.h>
#include <cassert>

template <class Alloc, class SizeT>
concept Alloc_Trait = requires(Alloc alloc,SizeT t) {
    alloc.alloc(t);
};

struct RAlloc{
    uint64_t raddr;
    uint64_t roffset;
    uint64_t rsize;
    uint64_t r_start; // end of the whole buf
    uint64_t r_total; // size of the whole buf

    void SetRemote(uint64_t _raddr, uint64_t _rsize, uint64_t _start, uint64_t _total)
    {
        raddr = _raddr;
        rsize = _rsize;
        roffset = 0;

        r_start = _start;
        r_total = _total;
    }

    // 从尾部开始分配
    uint64_t alloc(uint64_t size, bool align = false)
    {
        roffset += size;
        uint64_t res = raddr - roffset;
        if(align){
            res = (res>>3)<<3; //按八字节对齐
            roffset =  raddr - res;
        }
        // if (roffset > rsize * 0.9) {
        //     std::cerr << "Warning: Remote allocator space is running out!" << std::endl;
        // }
        assert(roffset < rsize); // FIXME: 空间可能不够。每次扩容时空间*2，之后如果装得下就原地写入（可能读到部分写入）。 or slab alloc。or 链表 main seg。
        return res;
    }

    inline uint64_t offset(uint64_t ptr){return r_start + r_total - ptr;}
    
    inline uint64_t ptr(uint64_t off){return r_start + r_total - off;}
};

struct Alloc{
    char *start;
    uint64_t offset;
    uint64_t buf_size;

    void Set(char *_start, uint64_t _size)
    {
        start = _start;
        buf_size = _size;
        offset = 0;
        // memset(_start, 0, _size);
    }

    void ReSet(uint64_t _start){
        offset=_start;
    }
    // 从前往后分配
    char * alloc(uint64_t size, bool align = false) // IMPORTANT: 经常被reset，只能申请临时变量
    {
        char *res = start + offset;
        if(align){
            uint64_t tmp = (uint64_t)res;
            tmp = ((tmp+7)>>3)<<3; //按八字节对齐
            res = (char*)tmp;
            offset =  res - start;
        }else{
            offset += size;
        }
        assert(offset < buf_size);
        return res;
    }

    void free(uint64_t size, bool align = false)
    {
        offset -= size;
    }
};