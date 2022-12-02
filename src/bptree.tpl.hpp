// Template implementation included in-place by the ".hpp".

#pragma once

namespace bptree {

template <typename K, typename V>
BPTree<K, V>::BPTree(std::string filename, size_t degree)
    : filename(filename), degree(degree) {
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
        throw BPTreeException("invalid degree parameter " +
                              std::to_string(degree));
    }

    // open the backing file
    fd = open(filename.c_str(), O_CREAT | O_RDWR,
              S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd <= 0) throw BPTreeException("failed to open backing file");

    // initialize pager
    pager = new Pager(fd, degree);
}

template <typename K, typename V>
BPTree<K, V>::~BPTree() {
    if (fd > 0) close(fd);

    delete pager;
}

template <typename K, typename V>
void BPTree<K, V>::ReopenBackingFile(int flags) {
    assert(fd > 0);
    close(fd);

    fd = open(filename.c_str(), flags | O_RDWR);
    if (fd <= 0) throw BPTreeException("failed to re-open backing file");

    pager->fd = fd;
}

template <typename K, typename V>
size_t BPTree<K, V>::PageSearchKey(const Page& page, K key) {
    size_t nkeys = page.header.nkeys;
    if (nkeys == 0) return 0;

    // binary search for key in content array
    size_t spos = 0, epos = nkeys;
    while (true) {
        size_t pos = (spos + epos) / 2;
        size_t idx = 1 + pos * 2;

        if (page.content[idx] == key) {
            // found equality
            return idx;
        } else {
            // shrink range
            if (page.content[idx] < key)
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
void BPTree<K, V>::LeafPageInject(Page& page, size_t search_idx, K key,
                                  V value) {
    assert(page.header.nkeys < degree);
    assert(search_idx == 0 || search_idx % 2 == 1);

    // if key has exact match with the one on idx, simply update its value
    if (search_idx > 0) {
        size_t search_pos = (search_idx - 1) / 2;
        if (search_pos < page.header.nkeys && page.content[search_idx] == key) {
            page.content[search_idx + 1] = value;
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
    if (inject_pos < page.header.nkeys) {
        size_t shift_src = 1 + inject_pos * 2;
        size_t shift_dst = shift_src + 2;
        size_t shift_len =
            (page.header.nkeys - inject_pos) * 2 * sizeof(uint64_t);
        memmove(&page.content[shift_dst], &page.content[shift_src], shift_len);
    }

    // inject key and value to its slot
    page.content[inject_idx] = key;
    page.content[inject_idx + 1] = value;

    // update number of keys
    page.header.nkeys++;
}

template <typename K, typename V>
void BPTree<K, V>::ItnlPageInject(Page& page, size_t search_idx, K key,
                                  uint64_t lpageid, uint64_t rpageid) {
    assert(page.header.nkeys < degree);
    assert(search_idx == 0 || search_idx % 2 == 1);

    // must not have duplicate internal node keys
    if (search_idx > 0) {
        size_t search_pos = (search_idx - 1) / 2;
        if (search_pos < page.header.nkeys && page.content[search_idx] == key)
            throw BPTreeException("duplicate internal node keys detected");
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
    if (inject_pos < page.header.nkeys) {
        size_t shift_src = 1 + inject_pos * 2;
        size_t shift_dst = shift_src + 2;
        size_t shift_len =
            (page.header.nkeys - inject_pos) * 2 * sizeof(uint64_t);
        memmove(&page.content[shift_dst], &page.content[shift_src], shift_len);
    }

    // the pageid to the left of inject slot must be equal to left child
    if (page.content[inject_idx - 1] != lpageid)
        throw BPTreeException("left child pageid does not match");

    // inject key and pageid to its slot
    page.content[inject_idx] = key;
    page.content[inject_idx + 1] = rpageid;

    // update number of keys
    page.header.nkeys++;
}

template <typename K, typename V>
std::tuple<std::vector<uint64_t>, size_t> BPTree<K, V>::TraverseToLeaf(K key) {
    Page page;
    uint64_t pageid = 0;
    unsigned level = 0, depth;
    std::vector<uint64_t> path;

    // search through internal pages, start from root
    while (true) {
        path.push_back(pageid);
        if (!pager->ReadPage(pageid, &page))
            throw BPTreeException("failed to read internal page");

        // if at root page, read out depth of tree
        if (level == 0) {
            depth = page.header.depth;
            if (depth == 1) {
                // root is the leaf, return
                return std::make_tuple(path, 0);
            }
        }

        // search the nearest key that is <= given key in node
        size_t idx = PageSearchKey(page, key);
        idx = (idx == 0) ? 0 : (idx + 1);

        // fetch the correct child node pageid
        uint64_t childid = page.content[idx];
        if (childid == 0) throw BPTreeException("got 0 as child node pageid");

        level++;
        if (level == depth - 1) {
            path.push_back(childid);
            return std::make_tuple(path, idx);
        }

        pageid = childid;
    }
}

template <typename K, typename V>
void BPTree<K, V>::SplitPage(uint64_t pageid, Page& page,
                             std::vector<uint64_t>& path) {
    // if spliting root page, need to allocate two pages
    if (page.header.type == PAGE_ROOT) {
        assert(pageid == 0);
        assert(path.size() == 1);
        assert(path[0] == 0);

        size_t mpos = page.header.nkeys / 2;
        uint64_t lpageid, rpageid;

        // special case of the very first split of root leaf
        if (page.header.depth == 1) {
            Page lpage(PAGE_LEAF), rpage(PAGE_LEAF);
            lpageid = pager->AllocPage(), rpageid = pager->AllocPage();

            // populate left child
            size_t lsize = mpos * 2 * sizeof(uint64_t);
            memcpy(&lpage.content[1], &page.content[1], lsize);
            lpage.header.nkeys = mpos;
            lpage.header.next = rpageid;
            if (!pager->WritePage(lpageid, &lpage))
                throw BPTreeException("failed to write left child page");

            // populate right child
            size_t rsize = (page.header.nkeys - mpos) * 2 * sizeof(uint64_t);
            memcpy(&rpage.content[1], &page.content[1 + mpos * 2], rsize);
            rpage.header.nkeys = page.header.nkeys - mpos;
            if (!pager->WritePage(rpageid, &rpage))
                throw BPTreeException("failed to write right child page");

            // populate split node with first key of right child
            memset(page.content, 0, CONTENTLEN * sizeof(uint64_t));
            page.content[1] = rpage.content[1];

            // splitting root into two internal nodes
        } else {
            Page lpage(PAGE_ITNL), rpage(PAGE_ITNL);
            lpageid = pager->AllocPage(), rpageid = pager->AllocPage();

            // populate left child
            size_t lsize = (1 + mpos * 2) * sizeof(uint64_t);
            memcpy(&lpage.content[0], &page.content[0], lsize);
            lpage.header.nkeys = mpos;
            lpage.header.next = rpageid;
            if (!pager->WritePage(lpageid, &lpage))
                throw BPTreeException("failed to write left child page");

            // populate right child
            size_t rsize =
                ((page.header.nkeys - mpos) * 2 - 1) * sizeof(uint64_t);
            memcpy(&rpage.content[0], &page.content[2 + mpos * 2], rsize);
            rpage.header.nkeys = page.header.nkeys - mpos - 1;
            if (!pager->WritePage(rpageid, &rpage))
                throw BPTreeException("failed to write right child page");

            // populate split node with the middle key
            uint64_t mkey = page.content[1 + mpos * 2];
            memset(page.content, 0, CONTENTLEN * sizeof(uint64_t));
            page.content[1] = mkey;
        }

        // prepare new root info, increment tree depth
        page.content[0] = lpageid;
        page.content[2] = rpageid;
        page.header.nkeys = 1;
        page.header.depth++;
        if (!pager->WritePage(pageid, &page))
            throw BPTreeException("failed to write root page after split");

        // update path vector
        path.push_back(rpageid);

        // if splitting a non-root node
    } else {
        assert(pageid != 0);
        assert(path.size() > 1);
        assert(path.back() == pageid);

        size_t mpos = page.header.nkeys / 2;
        uint64_t rpageid, mkey;

        // if splitting a non-root leaf node
        if (page.header.type == PAGE_LEAF) {
            Page rpage(PAGE_LEAF);
            rpageid = pager->AllocPage();

            // populate right child
            size_t rsize = (page.header.nkeys - mpos) * 2 * sizeof(uint64_t);
            memcpy(&rpage.content[1], &page.content[1 + mpos * 2], rsize);
            rpage.header.nkeys = page.header.nkeys - mpos;
            rpage.header.next = page.header.next;
            if (!pager->WritePage(rpageid, &rpage))
                throw BPTreeException("failed to write right child page");

            // trim current node
            mkey = rpage.content[1];
            memset(&page.content[1 + mpos * 2], 0, rsize);
            page.header.nkeys = mpos;

            // if splitting a non-root internal node
        } else if (page.header.type == PAGE_ITNL) {
            Page rpage(PAGE_ITNL);
            rpageid = pager->AllocPage();

            // populate right child
            size_t rsize =
                ((page.header.nkeys - mpos) * 2 - 1) * sizeof(uint64_t);
            memcpy(&rpage.content[0], &page.content[2 + mpos * 2], rsize);
            rpage.header.nkeys = page.header.nkeys - mpos - 1;
            rpage.header.next = page.header.next;
            if (!pager->WritePage(rpageid, &rpage))
                throw BPTreeException("failed to write right child page");

            // trim current node
            mkey = page.content[1 + mpos * 2];
            memset(&page.content[1 + mpos * 2], 0, rsize + sizeof(uint64_t));
            page.header.nkeys = mpos;
        } else
            throw BPTreeException("unknown page type encountered");

        // make current node link to new right node
        page.header.next = rpageid;
        if (!pager->WritePage(pageid, &page))
            throw BPTreeException("failed to write split page");

        // insert the uplifted key into parent node
        uint64_t parentid = path[path.size() - 2];
        Page parent;
        if (!pager->ReadPage(parentid, &parent))
            throw BPTreeException("failed to read parent page");
        assert(parent.header.nkeys < degree);
        size_t idx = PageSearchKey(parent, mkey);
        ItnlPageInject(parent, idx, mkey, pageid, rpageid);

        // if parent internal node becomes full, do split recursively
        if (parent.header.nkeys >= degree) {
            path.pop_back();
            SplitPage(parentid, parent, path);
            path.push_back(rpageid);
        } else {
            if (!pager->WritePage(parentid, &parent))
                throw BPTreeException("failed to update parent page");
            path.back() = rpageid;
        }
    }
}

template <typename K, typename V>
void BPTree<K, V>::Put(K key, V value) {
    // traverse to the correct leaf node and read
    std::vector<uint64_t> path;
    std::tie(path, std::ignore) = TraverseToLeaf(key);
    assert(path.size() > 0);
    uint64_t leafid = path.back();
    Page page;
    if (!pager->ReadPage(leafid, &page))
        throw BPTreeException("failed to read leaf page");

    // inject key-value pair into the leaf node
    assert(page.header.nkeys < degree);
    size_t idx = PageSearchKey(page, key);
    LeafPageInject(page, idx, key, value);

    // if this leaf node becomes full, do split
    if (page.header.nkeys >= degree)
        SplitPage(leafid, page, path);
    else if (!pager->WritePage(leafid, &page))
        throw BPTreeException("failed to update leaf page");
}

template <typename K, typename V>
bool BPTree<K, V>::Get(K key, V& value) {
    // traverse to the correct leaf node and read
    std::vector<uint64_t> path;
    std::tie(path, std::ignore) = TraverseToLeaf(key);
    assert(path.size() > 0);
    uint64_t leafid = path.back();
    Page page;
    if (!pager->ReadPage(leafid, &page))
        throw BPTreeException("failed to read leaf page");

    // search in leaf node for key
    size_t idx = PageSearchKey(page, key);
    if (idx == 0 || page.content[idx] != key) {
        // not found
        return false;
    }

    // found match key, fetch value
    value = page.content[idx + 1];
    return true;
}

template <typename K, typename V>
bool BPTree<K, V>::Delete(K key) {
    throw BPTreeException("unimplemented!");
}

template <typename K, typename V>
size_t BPTree<K, V>::Scan(K lkey, K rkey,
                          std::vector<std::tuple<K, V>>& results) {
    if (lkey > rkey) return 0;

    // traverse to leaf node for left bound of range
    std::vector<uint64_t> lpath;
    std::tie(lpath, std::ignore) = TraverseToLeaf(lkey);
    assert(lpath.size() > 0);
    uint64_t lleafid = lpath.back();

    // read out the leaf pages in a loop by following sibling chains,
    // gathering records in range
    Page page;
    uint64_t leafid = lleafid;
    size_t nrecords = 0;
    while (true) {
        if (!pager->ReadPage(leafid, &page))
            throw BPTreeException("failed to read out leaf page in scan");

        // do a search if in left bound leaf page
        size_t lidx = 1, ridx = page.header.nkeys * 2 - 1;
        if (leafid == lleafid) {
            size_t idx = PageSearchKey(page, lkey);
            if (idx != 0 && page.content[idx] == lkey)
                lidx = idx;
            else if (idx == 0)
                lidx = 1;
            else
                lidx = idx + 2;
        }

        // gather records within range; watch out for right bound
        for (size_t idx = lidx; idx <= ridx; idx += 2) {
            K key = page.content[idx];
            V value = page.content[idx + 1];
            if (key <= rkey) {
                results.push_back(std::make_tuple(key, value));
                nrecords++;
            } else
                return nrecords;
        }

        // move on to right sibling
        leafid = page.header.next;
        if (leafid == 0) return nrecords;
    }
}

template <typename K, typename V>
void BPTree<K, V>::Load(const std::vector<std::tuple<K, V>>& records) {
    if (records.size() == 0) return;

    // only supports bulk-loading on empty B+ tree
    Page itnl;
    if (!pager->ReadPage(0, &itnl))
        throw BPTreeException("failed to read out root page in load");
    if (itnl.header.depth != 1 || itnl.header.nkeys != 0)
        throw BPTreeException("only supports Load on new empty B+ tree");

    // ensure that the records array is sorted by key in increasing order
    K currkey = std::numeric_limits<K>::min();
    bool firstkey = true;
    for (auto&& record : records) {
        K key = std::get<0>(record);
        if (key <= currkey && !firstkey)
            throw BPTreeException("records vector input not valid");
        currkey = key;
        firstkey = false;
    }

    // special case where the root can directly hold all records
    if (records.size() < degree) {
        // simply put into root node as leaf
        for (size_t pos = 0; pos < records.size(); ++pos) {
            size_t idx = 1 + pos * 2;
            auto [key, value] = records[pos];
            itnl.content[idx] = key;
            itnl.content[idx + 1] = value;
        }
        itnl.header.nkeys = records.size();
        if (!pager->WritePage(0, &itnl))
            throw BPTreeException("failed to write root page in load");
        return;
    }

    // pre-allocate the required leaf pages at once
    size_t nkeys = degree - 1;
    size_t nleaves = records.size() / nkeys;
    if (records.size() % nkeys != 0) nleaves++;
    std::vector<uint64_t> leaves;
    leaves.reserve(nleaves);
    for (size_t i = 0; i < nleaves; ++i) leaves.push_back(pager->AllocPage());

    // the path list required for splits needs to be maintained along the
    // loading process
    std::vector<uint64_t> path{0};

    // directly write leaf pages and append to the right-most internal page
    uint64_t content[1 + nkeys * 2];
    size_t leafidx = 0, leafpos = 0, itnlpos = 0;
    Page leaf(PAGE_LEAF);
    bool is_first_leaf = true;
    for (auto&& record : records) {
        assert(leafpos < nkeys);
        size_t idx = 1 + leafpos * 2;
        auto [key, value] = record;
        content[idx] = key;
        content[idx + 1] = value;
        leafpos++;

        if (leafpos == nkeys) {
            // filled current leaf page buffer, write out
            uint64_t leafid = leaves[leafidx];
            memcpy(leaf.content, content, (1 + nkeys * 2) * sizeof(uint64_t));
            leaf.header.nkeys = nkeys;
            if (leafidx < nleaves - 1)
                leaf.header.next = leaves[leafidx + 1];
            else
                leaf.header.next = 0;
            if (!pager->WritePage(leafid, &leaf))
                throw BPTreeException("failed to write leaf page in load");

            if (is_first_leaf) {
                // for the very first leaf, only a single pageid is to be
                // written into root pages slot 0
                assert(itnl.header.type == PAGE_ROOT);
                itnl.content[0] = leafid;
                itnl.header.depth = 2;
                is_first_leaf = false;
            } else {
                // else, need to append a key-pageid pair into the current
                // internal node, possibly triggering splits
                size_t search_idx = (itnlpos == 0) ? 0 : (itnlpos * 2 - 1);
                ItnlPageInject(itnl, search_idx, content[1],
                               leaves[leafidx - 1], leafid);
                if (itnl.header.nkeys >= degree) {
                    SplitPage(path.back(), itnl, path);
                    if (!pager->ReadPage(path.back(), &itnl)) {
                        throw BPTreeException(
                            "failed to read internal page in load");
                    }
                    itnlpos = degree - (degree / 2) - 1;
                } else
                    itnlpos++;
            }

            memset(content, 0, (1 + nkeys * 2) * sizeof(uint64_t));
            leafpos = 0;
            leafidx++;
        }
    }

    // wrapping up for the last leaf and last internal node that could
    // possibly be left out
    if (records.size() % nkeys != 0) {
        memcpy(leaf.content, content, (1 + leafpos * 2) * sizeof(uint64_t));
        leaf.header.nkeys = leafpos;
        leaf.header.next = 0;
        if (!pager->WritePage(leaves.back(), &leaf))
            throw BPTreeException("failed to write leaf page in load");

        size_t search_idx = (itnlpos == 0) ? 0 : (itnlpos * 2 - 1);
        ItnlPageInject(itnl, search_idx, content[1], leaves[leaves.size() - 2],
                       leaves.back());
    }
    if (itnl.header.nkeys >= degree)
        SplitPage(path.back(), itnl, path);
    else if (!pager->WritePage(path.back(), &itnl))
        throw BPTreeException("failed to write internal page in load");
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
                throw BPTreeException("failed to read page for stats");
            std::cout << " " << pageid << " - " << page << std::endl;
        }
    }
}

}  // namespace bptree
