#include "pager.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>

#include "common.hpp"
#include "format.hpp"

namespace garner {

Pager::Pager(int fd, size_t degree) : fd(fd), degree(degree) {
    assert(fd > 0);
    assert(degree >= 2 && degree <= MAXNKEYS);

    // verify file integrity, at the same time build the freelist
    BPTreeStats stats;
    CheckStats(stats, true);
}

Pager::~Pager() {}

bool Pager::ReadPage(uint64_t pageid, void* buf) {
    off_t foff = pageid * BLKSIZE;
    ssize_t ret = pread(fd, buf, BLKSIZE, foff);
    if (ret != static_cast<ssize_t>(BLKSIZE)) return false;
    return true;
}

bool Pager::ReadPage(uint64_t pageid, size_t off, size_t len, void* buf) {
    if (off < 0 || off >= BLKSIZE || off + len > BLKSIZE) return false;
    off_t foff = (pageid * BLKSIZE) + off;
    ssize_t ret = pread(fd, buf, len, foff);
    if (ret != static_cast<ssize_t>(len)) return false;
    return true;
}

bool Pager::WritePage(uint64_t pageid, const void* buf) {
    off_t foff = pageid * BLKSIZE;
    ssize_t ret = pwrite(fd, buf, BLKSIZE, foff);
    if (ret != static_cast<ssize_t>(BLKSIZE)) return false;
    return true;
}

bool Pager::WritePage(uint64_t pageid, size_t off, size_t len,
                      const void* buf) {
    if (off < 0 || off >= BLKSIZE || off + len > BLKSIZE) return false;
    off_t foff = (pageid * BLKSIZE) + off;
    ssize_t ret = pwrite(fd, buf, len, foff);
    if (ret != static_cast<ssize_t>(len)) return false;
    return true;
}

uint64_t Pager::AllocPage() {
    uint64_t pageid;
    if (freelist.size() > 0) {
        // if there are empty pages in freelist, use one
        pageid = freelist.extract(freelist.begin()).value();
    } else {
        // otherwise, extend file by one page
        off_t filesize = lseek(fd, 0, SEEK_END);
        int ret = ftruncate(fd, filesize + BLKSIZE);
        if (ret != 0) throw BPTreeException("failed to ftruncate file");
        pageid = filesize / BLKSIZE;
    }

    // zero-fill page
    Page page;
    memset(page.content, 0, CONTENTLEN * sizeof(uint64_t));
    if (!WritePage(pageid, &page))
        throw BPTreeException("failed to zero-fill page at allocation");

    return pageid;
}

void Pager::CheckStats(BPTreeStats& stats, bool init) {
    if (init) freelist.clear();

    // get file size
    off_t filesize = lseek(fd, 0, SEEK_END);
    if (filesize % BLKSIZE != 0)
        throw BPTreeException("file size not a multiple of block size");

    stats.npages = filesize / BLKSIZE;
    stats.npages_itnl = 0;
    stats.npages_leaf = 0;
    stats.npages_empty = 0;
    stats.nkeys_itnl = 0;
    stats.nkeys_leaf = 0;

    // if root page not allocated yet, allocate it now
    if (filesize == 0) {
        int ret = ftruncate(fd, BLKSIZE);
        if (ret != 0) throw BPTreeException("failed to ftruncate file");

        Page page(PAGE_ROOT);
        memset(page.content, 0, CONTENTLEN * sizeof(uint64_t));
        page.header.depth = 1;
        if (!WritePage(0, &page))
            throw BPTreeException("failed to write root page");

        stats.npages++;
    }

    // scan through all blocks
    PageHeader header;
    for (uint64_t pageid = 0; pageid < stats.npages; ++pageid) {
        if (!ReadPage(pageid, 0, sizeof(PageHeader), &header))
            throw BPTreeException("error reading page header");

        // check page magic number
        if (header.magic != MAGIC)
            throw BPTreeException("page magic number mismatch");

        // page 0 must be the root node
        if (pageid == 0 && header.type != PAGE_ROOT)
            throw BPTreeException("page 0 is not root node");

        // check number of keys field
        if (header.nkeys > MAXNKEYS)
            throw BPTreeException("invalid number of keys in header");

        // update statistics accordingly
        if (header.type == PAGE_ROOT || header.type == PAGE_ITNL) {
            stats.npages_itnl++;
            stats.nkeys_itnl += header.nkeys;
        } else if (header.type == PAGE_LEAF) {
            stats.npages_leaf++;
            stats.nkeys_leaf += header.nkeys;
        } else if (header.type == PAGE_EMPTY) {
            stats.npages_empty++;
            // build freelist if initializing
            if (init) freelist.insert(pageid);
        } else
            throw BPTreeException("unknown page type code");
    }

    // if tree only has one page, root is the only leaf
    if (stats.npages - stats.npages_empty == 1) {
        assert(stats.npages_itnl == 1);
        assert(stats.npages_leaf == 0);
        assert(stats.nkeys_leaf == 0);
        stats.nkeys_leaf = stats.nkeys_itnl;
        stats.nkeys_itnl = 0;
        stats.npages_leaf = 1;
        stats.npages_itnl = 0;
    }
}

}  // namespace garner
