// B+-tree node format definitions.

#include <iostream>

#pragma once

namespace garner {

/** Page size. */
constexpr size_t BLKSIZE = 8192;

/** Magic number for integrity check. */
constexpr uint64_t MAGIC = 0xB1237AEED6548CFF;

/** Page format. */
enum PageType {
    PAGE_EMPTY = 0,
    PAGE_ROOT = 1,  // root node of tree
    PAGE_ITNL = 2,  // internal node other than root
    PAGE_LEAF = 3,  // leaf node storing values
};

struct PageHeader {
    enum PageType type : 8 = PAGE_EMPTY;  // page type
    size_t nkeys : 56 = 0;                // number of keys in node
    uint64_t magic : 64 = MAGIC;          // magic number
    union {
        unsigned depth;     // in root page, stores the current depth of tree
        uint64_t next = 0;  // in other pages, stores pageid of right sibling
    };

    PageHeader() : type(PAGE_EMPTY), nkeys(0), magic(MAGIC), next(0) {}
    PageHeader(enum PageType type)
        : type(type), nkeys(0), magic(MAGIC), next(0) {}
};
static_assert(sizeof(PageHeader) == 24);

constexpr size_t CONTENTLEN = (BLKSIZE - 24) / sizeof(uint64_t);
constexpr size_t MAXNKEYS = (CONTENTLEN - 1) / 2;

struct Page {
    // first 16 bytes for the header
    PageHeader header;
    // content is:
    //   for internal node -- an interleaved array of v-k-v-k-...-v
    //   for leaf node -- first slot unused, later slots store key-value pairs
    uint64_t content[CONTENTLEN] = {0};

    Page() : header() {}
    Page(enum PageType type) : header(type) {}
};
static_assert(sizeof(Page) == BLKSIZE);

std::ostream& operator<<(std::ostream& s, const Page& stats);

}  // namespace garner
