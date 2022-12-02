// B+-tree node page format definitions.

#include <iostream>
#include <vector>

#pragma once

namespace garner {

/**
 * Page types enum.
 */
enum PageType {
    PAGE_EMPTY = 0,
    PAGE_ROOT = 1,  // root node of tree
    PAGE_ITNL = 2,  // internal node other than root
    PAGE_LEAF = 3,  // leaf node storing values
};

inline std::string PageTypeStr(enum PageType type) {
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
 */
template <typename K>
struct Page {
    // page type
    enum PageTYpe type = PAGE_EMPTY;

    // TODO: add latch field

    // sorted list of keys
    std::vector<K> keys;

    Page() = default;

    Page(const Page&) = delete;
    Page& operator=(const Page&) = delete;

    ~Page() = default;
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

    PageLeaf() : type(PAGE_LEAF), keys(), next(nullptr), values() {}

    PageLeaf(const PageLeaf&) = delete;
    PageLeaf& operator=(const PageLeaf&) = delete;

    ~PageLeaf() = default;
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
    std::vector<Page*> children;

    PageItnl() : type(PAGE_ITNL), keys(), children() {}

    PageItnl(const PageItnl&) = delete;
    PageItnl& operator=(const PageItnl&) = delete;

    ~PageItnl() = default;
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
    std::vector<V> values;        // case 1: root might be the only leaf
    std::vector<Page*> children;  // case 2: root is non-leaf

    PageRoot() : type(PAGE_ROOT), keys(), depth(0), values(), children() {}

    PageRoot(const PageRoot&) = delete;
    PageRoot& operator=(const PageRoot&) = delete;

    ~PageRoot() = default;
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
