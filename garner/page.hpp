// B+-tree node page format definitions.

#include <algorithm>
#include <iostream>
#include <shared_mutex>
#include <vector>

#pragma once

namespace garner {

/**
 * Page types enum.
 */
typedef enum PageType {
    PAGE_EMPTY = 0,
    PAGE_ROOT = 1,  // root node of tree
    PAGE_ITNL = 2,  // internal node other than root
    PAGE_LEAF = 3,  // leaf node storing values
} PageType;

std::string PageTypeStr(PageType type) {
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

    // read-write mutex as latch
    std::shared_mutex latch;

    // sorted list of keys
    std::vector<K> keys;

    Page() = delete;
    Page(PageType type, size_t degree)
        : type(type), degree(degree), latch(), keys() {
        keys.reserve(degree);
    }

    Page(const Page&) = delete;
    Page& operator=(const Page&) = delete;

    ~Page() = default;

    /**
     * Get number of keys in page.
     */
    size_t NumKeys() const;

    /**
     * Search in page for the closest key that is <= given key. Returns its
     * index, or -1 if all existing keys are greater than given key.
     * Assumes keys vector is sorted accendingly, which should always be the
     * case.
     */
    ssize_t SearchKey(const K& key) const;
};

template <typename K>
std::ostream& operator<<(std::ostream& s, const Page<K>& page) {
    s << "Page{type=" << PageTypeStr(page.type)
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
    Page<K>* next = nullptr;

    // values according to sorted keys, keys[0] -> values[0], etc.
    std::vector<V> values;

    PageLeaf() = delete;
    PageLeaf(size_t degree)
        : Page<K>(PAGE_LEAF, degree), next(nullptr), values() {
        values.reserve(degree);
    }

    PageLeaf(const PageLeaf&) = delete;
    PageLeaf& operator=(const PageLeaf&) = delete;

    ~PageLeaf() = default;

    /**
     * Insert a key-value pair into non-full leaf page, shifting array content
     * if necessary. serach_idx should be calculated through PageSearchKey.
     */
    void Inject(ssize_t search_idx, K key, V value);
};

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s, const PageLeaf<K, V>& page) {
    s << "Page{type=" << PageTypeStr(page.type)
      << ",nkeys=" << page.keys.size();
    s << ",keys=[";
    for (auto&& k : page.keys) s << k << ",";
    s << "],values=[";
    for (auto&& v : page.values) s << v << ",";
    s << "],next=" << page.next << "}";
    return s;
}

/**
 * Page type -- internal.
 */
template <typename K, typename V>
struct PageItnl : public Page<K> {
    // pointers to child pages
    // children[0] is the one < keys[0];
    // children[1] is the one >= keys[0] and < keys[1], etc.
    std::vector<Page<K>*> children;

    PageItnl() = delete;
    PageItnl(size_t degree) : Page<K>(PAGE_ITNL, degree), children() {
        children.reserve(degree + 1);
    }

    PageItnl(const PageItnl&) = delete;
    PageItnl& operator=(const PageItnl&) = delete;

    ~PageItnl() = default;

    /**
     * Insert a key into non-empty internal node (carrying its left and right
     * child page pointers), shifting array content if necessary. search_idx
     * should be calculated through PageSearchKey.
     */
    void Inject(ssize_t search_idx, K key, Page<K>* lpage, Page<K>* rpage);
};

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s, const PageItnl<K, V>& page) {
    s << "Page{type=" << PageTypeStr(page.type)
      << ",nkeys=" << page.keys.size();
    s << ",keys=[";
    for (auto&& k : page.keys) s << k << ",";
    s << "],children=[";
    for (auto&& c : page.children) s << c << ",";
    s << "]}";
    return s;
}

/**
 * Page type -- root.
 */
template <typename K, typename V>
struct PageRoot : public Page<K> {
    // current depth of tree
    unsigned depth = 0;

    // page content sorted according to key
    std::vector<V> values;           // depth == 1: root is the only leaf
    std::vector<Page<K>*> children;  // depth > 1: root is non-leaf

    PageRoot() = delete;
    PageRoot(size_t degree)
        : Page<K>(PAGE_ROOT, degree), depth(1), values(), children() {
        values.reserve(degree);
        children.reserve(degree + 1);
    }

    PageRoot(const PageRoot&) = delete;
    PageRoot& operator=(const PageRoot&) = delete;

    ~PageRoot() = default;

    /**
     * Root page may act as either type, depending on depth.
     */
    void Inject(ssize_t search_idx, K key, V value);
    void Inject(ssize_t search_idx, K key, Page<K>* lpage, Page<K>* rpage);
};

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s, const PageRoot<K, V>& page) {
    s << "Page{type=" << PageTypeStr(page.type)
      << ",nkeys=" << page.keys.size();
    s << ",keys=[";
    for (auto&& k : page.keys) s << k << ",";
    s << "],values=[";
    for (auto&& v : page.values) s << v << ",";
    s << "],children=[";
    for (auto&& c : page.children) s << c << ",";
    s << "],depth=" << page.depth << "}";
    return s;
}

}  // namespace garner

// Include template implementation in-place.
#include "page.tpl.hpp"
