// Pager -- backing file allocation and I/O manager.

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

#pragma once

namespace bptree {

/**
 * Manages free pages of a BPTree backing file. Uses a simple freelist
 * mechanism. The file can grow but can never shrink.
 */
class Pager {
    template <typename K, typename V>
    friend class BPTree;

   private:
    int fd = -1;
    const size_t degree = 0;

    std::set<uint64_t> freelist;

   public:
    Pager(int fd, size_t degree);
    ~Pager();

    /** Data I/O on page. */
    bool ReadPage(uint64_t pageid, void* buf);
    bool ReadPage(uint64_t pageid, size_t off, size_t len, void* buf);
    bool WritePage(uint64_t pageid, const void* buf);
    bool WritePage(uint64_t pageid, size_t off, size_t len, const void* buf);

    /** Allocate one new page.*/
    uint64_t AllocPage();

    /**
     * Scan file and gather statistics.
     * If init is true, will initialize the freelist set.
     */
    void CheckStats(BPTreeStats& stats, bool init = false);
};

}  // namespace bptree
