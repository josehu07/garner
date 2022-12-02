// Template implementation included in-place by the ".hpp".

#pragma once

namespace garner {

template <typename K, typename V>
BPTree<K, V>::BPTree(size_t degree) : degree(degree) {
    // key and value type must be integral within 64-bit width for now
    static_assert(std::is_unsigned<K>::value, "key must be unsigned integral");
    static_assert(std::is_unsigned<V>::value,
                  "value must be unsigned integral");
    static_assert(sizeof(K) <= sizeof(uint64_t),
                  "key must be within 64-bit width");
    static_assert(sizeof(V) <= sizeof(uint64_t),
                  "value must be within 64-bit width");

    // degree must be between [2, MAXNKEYS]
    if (degree < 2 || degree > MAXNKEYS) {
        throw GarnerException("invalid degree parameter " +
                              std::to_string(degree));
    }

    // allocate root page
    root = AllocNewPage(PAGE_ROOT);
}

template <typename K, typename V>
BPTree<K, V>::~BPTree() {
    // TODO: correctly free all memory frames
}

template <typename K, typename V>
Page* AllocNewPage(PageType type) {
    Page* page = new Page(type);
    if (page == nullptr)
        throw GarnerException("failed to allocate new frame for page");
    return page;
}

template <typename K, typename V>
size_t BPTree<K, V>::PageSearchKey(const Page* page, K key) {
    size_t nkeys = page->header.nkeys;
    if (nkeys == 0) return 0;

    // binary search for key in content array
    size_t spos = 0, epos = nkeys;
    while (true) {
        size_t pos = (spos + epos) / 2;
        size_t idx = 1 + pos * 2;

        if (page->content[idx] == key) {
            // found equality
            return idx;
        } else {
            // shrink range
            if (page->content[idx] < key)
                spos = pos + 1;
            else
                epos = pos;
        }

        if (spos >= epos) break;
    }

    // no equality, return idx of last spos
    assert(spos == epos);
    if (spos == 0) return 0;
    return 1 + (spos - 1) * 2;
}

template <typename K, typename V>
void BPTree<K, V>::LeafPageInject(Page* page, size_t search_idx, K key,
                                  V value) {
    assert(page->header.nkeys < degree);
    assert(search_idx == 0 || search_idx % 2 == 1);

    // if key has exact match with the one on idx, simply update its value
    if (search_idx > 0) {
        size_t search_pos = (search_idx - 1) / 2;
        if (search_pos < page->header.nkeys &&
            page->content[search_idx] == key) {
            page->content[search_idx + 1] = value;
            return;
        }
    }

    // shift any array content with larger key to the right
    size_t inject_pos, inject_idx;
    if (search_idx == 0) {
        // if key is smaller than all existing keys
        inject_pos = 0;
        inject_idx = 1;
    } else {
        size_t search_pos = (search_idx - 1) / 2;
        inject_pos = search_pos + 1;
        inject_idx = 1 + inject_pos * 2;
    }
    if (inject_pos < page->header.nkeys) {
        size_t shift_src = 1 + inject_pos * 2;
        size_t shift_dst = shift_src + 2;
        size_t shift_len =
            (page->header.nkeys - inject_pos) * 2 * sizeof(uint64_t);
        memmove(&page->content[shift_dst], &page->content[shift_src],
                shift_len);
    }

    // inject key and value to its slot
    page->content[inject_idx] = key;
    page->content[inject_idx + 1] = value;

    // update number of keys
    page->header.nkeys++;
}

template <typename K, typename V>
void BPTree<K, V>::ItnlPageInject(Page* page, size_t search_idx, K key,
                                  Page* lpage, Page* rpage) {
    assert(page->header.nkeys < degree);
    assert(search_idx == 0 || search_idx % 2 == 1);

    // must not have duplicate internal node keys
    if (search_idx > 0) {
        size_t search_pos = (search_idx - 1) / 2;
        if (search_pos < page->header.nkeys && page->content[search_idx] == key)
            throw GarnerException("duplicate internal node keys detected");
    }

    // shift any array content with larger key to the right
    size_t inject_pos, inject_idx;
    if (search_idx == 0) {
        // if key is smaller than all existing keys
        inject_pos = 0;
        inject_idx = 1;
    } else {
        size_t search_pos = (search_idx - 1) / 2;
        inject_pos = search_pos + 1;
        inject_idx = 1 + inject_pos * 2;
    }
    if (inject_pos < page->header.nkeys) {
        size_t shift_src = 1 + inject_pos * 2;
        size_t shift_dst = shift_src + 2;
        size_t shift_len =
            (page->header.nkeys - inject_pos) * 2 * sizeof(uint64_t);
        memmove(&page->content[shift_dst], &page->content[shift_src],
                shift_len);
    }

    // the page to the left of inject slot must be equal to left child
    if (page->content[inject_idx - 1] != lpage)
        throw GarnerException("left child page does not match");

    // inject key and page to its slot
    page->content[inject_idx] = key;
    page->content[inject_idx + 1] = rpage;

    // update number of keys
    page->header.nkeys++;
}

template <typename K, typename V>
std::vector<Page*> BPTree<K, V>::TraverseToLeaf(K key) {
    Page* page = root;
    unsigned level = 0, depth;
    std::vector<Page*> path;

    // search through internal pages, starting from root
    while (true) {
        path.push_back(page);

        // if at root page, read out depth of tree
        if (level == 0) {
            depth = page->header.depth;
            if (depth == 1) {
                // root is the leaf, return
                return path;
            }
        }

        // search the nearest key that is <= given key in node
        size_t idx = PageSearchKey(page, key);
        idx = (idx == 0) ? 0 : (idx + 1);

        // fetch the correct child node page
        Page* child = page->content[idx];
        if (child == nullptr)
            throw GarnerException("got nullptr as child node page");

        level++;
        if (level == depth - 1) {
            path.push_back(child);
            return path;
        }

        page = child;
    }
}

template <typename K, typename V>
void BPTree<K, V>::SplitPage(Page* page, std::vector<Page*>& path) {
    if (page->header.type == PAGE_ROOT) {
        // if spliting root page, need to allocate two pages
        assert(path.size() == 1);
        assert(path[0] == 0);

        size_t mpos = page->header.nkeys / 2;

        if (page->header.depth == 1) {
            // special case of the very first split of root leaf
            Page* lpage = AllocNewPage(PAGE_LEAF);
            Page* rpage = AllocNewPage(PAGE_LEAF);

            // populate left child
            size_t lsize = mpos * 2 * sizeof(uint64_t);
            memcpy(&lpage->content[1], &page->content[1], lsize);
            lpage->header.nkeys = mpos;
            lpage->header.next = rpage;

            // populate right child
            size_t rsize = (page->header.nkeys - mpos) * 2 * sizeof(uint64_t);
            memcpy(&rpage->content[1], &page->content[1 + mpos * 2], rsize);
            rpage->header.nkeys = page->header.nkeys - mpos;

            // populate split node with first key of right child
            memset(page->content, 0, CONTENTLEN * sizeof(uint64_t));
            page->content[1] = rpage->content[1];

        } else {
            // splitting root into two internal nodes
            Page* lpage = AllocNewPage(PAGE_ITNL);
            Page* rpage = AllocNewPage(PAGE_ITNL);

            // populate left child
            size_t lsize = (1 + mpos * 2) * sizeof(uint64_t);
            memcpy(&lpage->content[0], &page->content[0], lsize);
            lpage->header.nkeys = mpos;
            lpage->header.next = rpage;

            // populate right child
            size_t rsize =
                ((page->header.nkeys - mpos) * 2 - 1) * sizeof(uint64_t);
            memcpy(&rpage->content[0], &page->content[2 + mpos * 2], rsize);
            rpage->header.nkeys = page->header.nkeys - mpos - 1;

            // populate split node with the middle key
            uint64_t mkey = page->content[1 + mpos * 2];
            memset(page->content, 0, CONTENTLEN * sizeof(uint64_t));
            page->content[1] = mkey;
        }

        // prepare new root info, increment tree depth
        page->content[0] = lpage;
        page->content[2] = rpage;
        page->header.nkeys = 1;
        page->header.depth++;

        // update path vector
        path.push_back(rpage);

    } else {
        // if splitting a non-root node
        assert(path.size() > 1);
        assert(path.back() == page);

        size_t mpos = page->header.nkeys / 2;
        Page* rpage;
        uint64_t mkey;

        // if splitting a non-root leaf node
        if (page->header.type == PAGE_LEAF) {
            rpage = AllocNewPage(PAGE_LEAF);

            // populate right child
            size_t rsize = (page->header.nkeys - mpos) * 2 * sizeof(uint64_t);
            memcpy(&rpage->content[1], &page->content[1 + mpos * 2], rsize);
            rpage->header.nkeys = page->header.nkeys - mpos;
            rpage->header.next = page->header.next;

            // trim current node
            mkey = rpage->content[1];
            memset(&page->content[1 + mpos * 2], 0, rsize);
            page->header.nkeys = mpos;

        } else if (page->header.type == PAGE_ITNL) {
            // if splitting a non-root internal node
            rpage = AllocNewPage(PAGE_ITNL);

            // populate right child
            size_t rsize =
                ((page->header.nkeys - mpos) * 2 - 1) * sizeof(uint64_t);
            memcpy(&rpage->content[0], &page->content[2 + mpos * 2], rsize);
            rpage->header.nkeys = page->header.nkeys - mpos - 1;
            rpage->header.next = page->header.next;

            // trim current node
            mkey = page->content[1 + mpos * 2];
            memset(&page->content[1 + mpos * 2], 0, rsize + sizeof(uint64_t));
            page->header.nkeys = mpos;
        } else
            throw GarnerException("unknown page type encountered");

        // make current node link to new right node
        page->header.next = rpage;

        // insert the uplifted key into parent node
        Page* parent = path[path.size() - 2];
        assert(parent->header.nkeys < degree);
        size_t idx = PageSearchKey(parent, mkey);
        ItnlPageInject(parent, idx, mkey, page, rpage);

        // if parent internal node becomes full, do split recursively
        if (parent->header.nkeys >= degree) {
            path.pop_back();
            SplitPage(parent, path);
            path.push_back(rpage);
        } else {
            path.back() = rpage;
        }
    }
}

template <typename K, typename V>
void BPTree<K, V>::Put(K key, V value) {
    // traverse to the correct leaf node and read
    std::vector<uint64_t> path = TraverseToLeaf(key);
    assert(path.size() > 0);
    Page* leaf = path.back();

    // inject key-value pair into the leaf node
    assert(leaf->header.nkeys < degree);
    size_t idx = PageSearchKey(leaf, key);
    LeafPageInject(leaf, idx, key, value);

    // if this leaf node becomes full, do split
    if (leaf->header.nkeys >= degree) SplitPage(leaf, path);
}

template <typename K, typename V>
bool BPTree<K, V>::Get(K key, V& value) {
    // traverse to the correct leaf node and read
    std::vector<Page*> path = TraverseToLeaf(key);
    assert(path.size() > 0);
    Page* leaf = path.back();

    // search in leaf node for key
    size_t idx = PageSearchKey(leaf, key);
    if (idx == 0 || leaf->content[idx] != key) {
        // not found
        return false;
    }

    // found match key, fetch value
    value = leaf->content[idx + 1];
    return true;
}

template <typename K, typename V>
bool BPTree<K, V>::Delete(K key) {
    throw GarnerException("Delete not implemented yet!");
}

template <typename K, typename V>
size_t BPTree<K, V>::Scan(K lkey, K rkey,
                          std::vector<std::tuple<K, V>>& results) {
    if (lkey > rkey) return 0;

    // traverse to leaf node for left bound of range
    std::vector<Page*> lpath = TraverseToLeaf(lkey);
    assert(lpath.size() > 0);
    Page* lleaf = lpath.back();

    // read out the leaf pages in a loop by following sibling chains,
    // gathering records in range
    Page* leaf = lleaf;
    size_t nrecords = 0;
    while (true) {
        // if tree is completely empty, directly return
        if (leaf->header.nkeys == 0) {
            assert(leaf.header.type == PAGE_ROOT);
            return 0;
        }

        // do a search if in left bound leaf page
        size_t lidx = 1, ridx = leaf->header.nkeys * 2 - 1;
        if (leafid == lleafid) {
            size_t idx = PageSearchKey(page, lkey);
            if (idx != 0 && leaf->content[idx] == lkey)
                lidx = idx;
            else if (idx == 0)
                lidx = 1;
            else
                lidx = idx + 2;
        }

        // gather records within range; watch out for right bound
        for (size_t idx = lidx; idx <= ridx; idx += 2) {
            K key = leaf->content[idx];
            V value = leaf->content[idx + 1];
            if (key <= rkey) {
                results.push_back(std::make_tuple(key, value));
                nrecords++;
            } else
                return nrecords;
        }

        // move on to right sibling
        leaf = leaf->header.next;
        if (leaf == nullptr) return nrecords;
    }
}

template <typename K, typename V>
void BPTree<K, V>::PrintStats(bool print_pages) {
    BPTreeStats stats;
    pager->CheckStats(stats);
    std::cout << stats << std::endl;

    if (print_pages) {
        Page page;
        for (uint64_t pageid = 0; pageid < stats.npages; ++pageid) {
            if (!pager->ReadPage(pageid, &page))
                throw GarnerException("failed to read page for stats");
            std::cout << " " << pageid << " - " << page << std::endl;
        }
    }
}

}  // namespace garner
