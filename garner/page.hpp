// B+-tree node page format definitions.

#include <algorithm>
#include <atomic>
#include <iostream>
#include <optional>
#include <shared_mutex>
#include <vector>

#include "common.hpp"
#include "record.hpp"

#pragma once

namespace garner {

/**
 * Page types enum.
 */
typedef enum PageType {
    PAGE_EMPTY = 0,
    PAGE_ROOT = 1,  // root node of tree
    PAGE_ITNL = 2,  // internal node other than root
    PAGE_LEAF = 3,  // leaf node storing pointers to records
} PageType;

static inline std::string PageTypeStr(PageType type) {
    switch (type) {
        case PAGE_EMPTY:
            return "empty";
        case PAGE_ROOT:
            return "root";
        case PAGE_ITNL:
            return "itnl";
        case PAGE_LEAF:
            return "leaf";
        default:
            return "unknown";
    }
}

/**
 * Page base class, containing common metadata and vector of keys.
 * Each page type derives its own sub-type.
 *
 * All accessor methods to page content must have appropriate latch held.
 */
template <typename K>
struct Page {
    // page type
    const PageType type = PAGE_EMPTY;

    // max number of keys
    const size_t degree = 0;

    // height of this node in tree; height == 1 means leaf, > 1 means internal
    unsigned height = 0;

    // read-write mutex as latch
    std::shared_mutex latch;

    // tree node semaphore & version number for hierarchical validation
    std::atomic<uint64_t> hv_sem;
    std::atomic<uint64_t> hv_ver;

    // sorted list of keys
    std::vector<K> keys;

    Page() = delete;
    Page(PageType type, size_t degree, unsigned height)
        : type(type),
          degree(degree),
          height(height),
          latch(),
          hv_sem(0),
          hv_ver(0),
          keys() {
        keys.reserve(degree);
    }

    Page(const Page&) = delete;
    Page& operator=(const Page&) = delete;

    virtual ~Page() = default;

    /**
     * Get number of keys in page.
     *
     * Must have read latch held.
     */
    size_t NumKeys() const;

    /**
     * Search in page for the closest key that is <= given key. Returns its
     * index, or -1 if all existing keys are greater than given key.
     * Assumes keys vector is sorted accendingly, which should always be the
     * case.
     *
     * Must have read latch held.
     */
    ssize_t SearchKey(const K& key) const;
};

template <typename K>
std::ostream& operator<<(std::ostream& s, const Page<K>& page) {
    s << "Page{type=" << PageTypeStr(page.type) << ",height=" << page.height
      << ",nkeys=" << page.keys.size();
    s << ",keys=[";
    for (auto&& k : page.keys) s << k << ",";
    s << "]}";
    return s;
}

/**
 * Page type -- leaf.
 */
template <typename K, typename V>
struct PageLeaf : public Page<K> {
    // pointer to right sibling
    PageLeaf<K, V>* next = nullptr;

    // high key > all keys within the subtree rooted at this node
    // high key == the key in parent node that is right after the child
    // pointer to me (or the highkey of parent if I'm the right-most child)
    std::optional<K> highkey;

    // records according to sorted keys, keys[0] -> records[0], etc.
    std::vector<Record<K, V>*> records;

    PageLeaf() = delete;
    PageLeaf(size_t degree)
        : Page<K>(PAGE_LEAF, degree, 1),
          next(nullptr),
          highkey(std::nullopt),
          records() {
        records.reserve(degree);
    }

    PageLeaf(const PageLeaf&) = delete;
    PageLeaf& operator=(const PageLeaf&) = delete;

    ~PageLeaf() = default;

    /**
     * Insert a key-record pair into non-full leaf page, shifting array content
     * if necessary. serach_idx should be calculated through PageSearchKey.
     * Returns a pointer to the corresponding record struct. This record might
     * have existed before the injection if key already existed, or might be
     * just newly allocated.
     *
     * Must have page latch held in write mode when calling this.
     */
    Record<K, V>* Inject(ssize_t search_idx, K key);
};

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s, const PageLeaf<K, V>& page) {
    s << "Page{type=" << PageTypeStr(page.type) << ",height=" << page.height
      << ",next=" << page.next << ",highkey=" << OptionStr(page.highkey)
      << ",nkeys=" << page.keys.size();
    s << ",keys=[";
    for (auto&& k : page.keys) s << k << ",";
    s << "],records=[";
    for (auto* v : page.records) s << *v << ",";
    s << "]}";
    return s;
}

/**
 * Page type -- internal.
 */
template <typename K, typename V>
struct PageItnl : public Page<K> {
    // pointer to right sibling
    PageItnl<K, V>* next = nullptr;

    // high key > all keys within the subtree rooted at this node
    // high key == the key in parent node that is right after the child
    // pointer to me (or the highkey of parent if I'm the right-most child)
    std::optional<K> highkey;

    // pointers to child pages
    // children[0] is the one < keys[0];
    // children[1] is the one >= keys[0] and < keys[1], etc.
    std::vector<Page<K>*> children;

    PageItnl() = delete;
    PageItnl(size_t degree, unsigned height)
        : Page<K>(PAGE_ITNL, degree, height),
          next(nullptr),
          highkey(std::nullopt),
          children() {
        children.reserve(degree + 1);
    }

    PageItnl(const PageItnl&) = delete;
    PageItnl& operator=(const PageItnl&) = delete;

    ~PageItnl() = default;

    /**
     * Insert a key into non-empty internal node (carrying its left and right
     * child page pointers), shifting array content if necessary. search_idx
     * should be calculated through PageSearchKey.
     *
     * Must have page latch held in write mode when calling this.
     */
    void Inject(ssize_t search_idx, K key, Page<K>* lpage, Page<K>* rpage);
};

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s, const PageItnl<K, V>& page) {
    s << "Page{type=" << PageTypeStr(page.type) << ",height=" << page.height
      << ",next=" << page.next << ",highkey=" << OptionStr(page.highkey)
      << ",nkeys=" << page.keys.size();
    s << ",keys=[";
    for (auto&& k : page.keys) s << k << ",";
    s << "],children=[";
    for (auto* c : page.children) s << c << ",";
    s << "]}";
    return s;
}

/**
 * Page type -- root.
 */
template <typename K, typename V>
struct PageRoot : public Page<K> {
    // page content sorted according to key
    std::vector<Record<K, V>*> records;  // height == 1: root is the only leaf
    std::vector<Page<K>*> children;      // height > 1: root is non-leaf

    PageRoot() = delete;
    PageRoot(size_t degree)
        : Page<K>(PAGE_ROOT, degree, 1), records(), children() {
        records.reserve(degree);
        children.reserve(degree + 1);
    }

    PageRoot(const PageRoot&) = delete;
    PageRoot& operator=(const PageRoot&) = delete;

    ~PageRoot() = default;

    /**
     * Root page may act as either type, depending on height.
     */
    Record<K, V>* Inject(ssize_t search_idx, K key);
    void Inject(ssize_t search_idx, K key, Page<K>* lpage, Page<K>* rpage);
};

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s, const PageRoot<K, V>& page) {
    s << "Page{type=" << PageTypeStr(page.type) << ",height=" << page.height
      << ",nkeys=" << page.keys.size();
    s << ",keys=[";
    for (auto&& k : page.keys) s << k << ",";
    s << "],records=[";
    for (auto* v : page.records) s << *v << ",";
    s << "],children=[";
    for (auto* c : page.children) s << c << ",";
    s << "]}";
    return s;
}

}  // namespace garner

// Include template implementation in-place.
#include "page.tpl.hpp"
