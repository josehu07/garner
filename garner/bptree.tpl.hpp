// Template implementation included in-place by the ".hpp".

#pragma once

namespace garner {

template <typename K, typename V>
BPTree<K, V>::BPTree(size_t degree)
    : degree(degree), all_pages(), all_pages_lock() {
    if (degree < 4) {
        throw GarnerException("degree parameter too small: " +
                              std::to_string(degree));
    }

    // allocate root page
    // root page never gets re-alloced, so it is thread-safe to just use the
    // root field as root page pointer
    root = new PageRoot<K, V>(degree);
    if (root == nullptr)
        throw GarnerException("failed to allocate memory for root page");
    all_pages.insert(root);
}

template <typename K, typename V>
BPTree<K, V>::~BPTree() {
    std::lock_guard<std::mutex> lg(all_pages_lock);
    for (auto* page : all_pages) delete page;
}

template <typename K, typename V>
PageLeaf<K, V>* BPTree<K, V>::NewPageLeaf() {
    auto* page = new PageLeaf<K, V>(degree);
    if (page == nullptr)
        throw GarnerException("failed to allocate memory for new page");
    std::lock_guard<std::mutex> lg(all_pages_lock);
    all_pages.insert(page);
    return page;
}

template <typename K, typename V>
PageItnl<K, V>* BPTree<K, V>::NewPageItnl() {
    auto* page = new PageItnl<K, V>(degree);
    if (page == nullptr)
        throw GarnerException("failed to allocate memory for new page");
    std::lock_guard<std::mutex> lg(all_pages_lock);
    all_pages.insert(page);
    return page;
}

template <typename K, typename V>
bool BPTree<K, V>::IsConcurrencySafe(const Page<K>* page) const {
    // TODO: add condition for deletion safety
    return page->NumKeys() < degree - 1;
}

template <typename K, typename V>
std::tuple<std::vector<Page<K>*>, std::vector<Page<K>*>>
BPTree<K, V>::TraverseToLeaf(const K& key, LatchMode latch_mode) {
    Page<K>* page = root;
    unsigned level = 0, depth;
    std::vector<Page<K>*> path;
    std::vector<Page<K>*> write_latched_pages;

    if (latch_mode == LATCH_READ) {
        page->latch.lock_shared();
        DEBUG("latch R acquire %p", static_cast<void*>(page));
    } else if (latch_mode == LATCH_WRITE) {
        page->latch.lock();
        DEBUG("latch W acquire %p", static_cast<void*>(page));
        write_latched_pages.push_back(page);
    }

    // read out depth of tree, check if root is the only leaf
    depth = reinterpret_cast<PageRoot<K, V>*>(page)->depth;
    if (depth == 1) {
        path.push_back(page);
        // latch on root still held on return
        return std::make_tuple(path, write_latched_pages);
    }

    // search through internal pages, starting from root
    while (true) {
        path.push_back(page);

        // search the nearest key that is <= given key in node
        ssize_t idx = page->SearchKey(key);

        // fetch the correct child node page
        Page<K>* child =
            (page->type == PAGE_ROOT)
                ? reinterpret_cast<PageRoot<K, V>*>(page)->children[idx + 1]
                : reinterpret_cast<PageItnl<K, V>*>(page)->children[idx + 1];
        if (child == nullptr)
            throw GarnerException("got nullptr as child node page");

        // latch crabbing
        if (latch_mode == LATCH_READ) {
            child->latch.lock_shared();
            DEBUG("latch R acquire %p", static_cast<void*>(child));
            page->latch.unlock_shared();
            DEBUG("latch R release %p", static_cast<void*>(page));
        } else if (latch_mode == LATCH_WRITE) {
            child->latch.lock();
            DEBUG("latch W acquire %p", static_cast<void*>(child));
            // if child is safe, release all ancestors' write latches
            if (IsConcurrencySafe(child)) {
                assert(write_latched_pages.back() == page);
                for (auto* ancestor : write_latched_pages) {
                    ancestor->latch.unlock();
                    DEBUG("latch W release %p", static_cast<void*>(ancestor));
                }
                write_latched_pages.clear();
            }
            write_latched_pages.push_back(child);
        }

        level++;
        if (level == depth - 1) {
            path.push_back(child);
            // proper latch(es) still held on return
            return std::make_tuple(path, write_latched_pages);
        }

        page = child;
    }
}

template <typename K, typename V>
void BPTree<K, V>::SplitPage(Page<K>* page, std::vector<Page<K>*>& path) {
    if (page->type == PAGE_ROOT) {
        // if spliting root page, need to allocate two pages
        auto* spage = reinterpret_cast<PageRoot<K, V>*>(page);
        assert(path.size() == 1);
        assert(spage == root);
        assert(path[0] == root);

        size_t mpos = spage->NumKeys() / 2;
        Page<K>*lpage_saved, *rpage_saved;

        if (spage->depth == 1) {
            // special case of the very first split of root leaf
            DEBUG("split root leaf %p", static_cast<void*>(spage));

            auto* lpage = NewPageLeaf();
            auto* rpage = NewPageLeaf();
            lpage_saved = lpage;
            rpage_saved = rpage;

            // populate left child
            std::copy(spage->keys.begin(), spage->keys.begin() + mpos,
                      std::back_inserter(lpage->keys));
            std::copy(spage->values.begin(), spage->values.begin() + mpos,
                      std::back_inserter(lpage->values));

            // set left child's next pointer
            lpage->next = rpage;

            // populate right child
            std::copy(spage->keys.begin() + mpos, spage->keys.end(),
                      std::back_inserter(rpage->keys));
            std::copy(spage->values.begin() + mpos, spage->values.end(),
                      std::back_inserter(rpage->values));

            // populate split node with first key of right child
            spage->keys.clear();
            spage->values.clear();
            spage->keys.push_back(rpage->keys[0]);

        } else {
            // splitting root into two internal nodes
            DEBUG("split root internal %p", static_cast<void*>(spage));

            auto* lpage = NewPageItnl();
            auto* rpage = NewPageItnl();
            lpage_saved = lpage;
            rpage_saved = rpage;

            // populate left child
            std::copy(spage->keys.begin(), spage->keys.begin() + mpos,
                      std::back_inserter(lpage->keys));
            std::copy(spage->children.begin(),
                      spage->children.begin() + mpos + 1,
                      std::back_inserter(lpage->children));

            // populate right child
            std::copy(spage->keys.begin() + mpos + 1, spage->keys.end(),
                      std::back_inserter(rpage->keys));
            std::copy(spage->children.begin() + mpos + 1, spage->children.end(),
                      std::back_inserter(rpage->children));

            // populate split node with the middle key
            K mkey = spage->keys[mpos];
            spage->keys.clear();
            spage->children.clear();
            spage->keys.push_back(mkey);
        }

        // set new root child pointers, increment tree depth
        spage->children.push_back(lpage_saved);
        spage->children.push_back(rpage_saved);
        spage->depth++;

        // update path vector
        path.push_back(rpage_saved);

    } else {
        // if splitting a non-root node
        assert(path.size() > 1);
        assert(path.back() == page);

        size_t mpos = page->NumKeys() / 2;
        Page<K>* rpage_saved;
        K mkey;

        if (page->type == PAGE_LEAF) {
            // if splitting a non-root leaf node
            DEBUG("split leaf %p", static_cast<void*>(page));

            auto* spage = reinterpret_cast<PageLeaf<K, V>*>(page);
            auto* rpage = NewPageLeaf();
            rpage_saved = rpage;

            // populate right child
            std::copy(spage->keys.begin() + mpos, spage->keys.end(),
                      std::back_inserter(rpage->keys));
            std::copy(spage->values.begin() + mpos, spage->values.end(),
                      std::back_inserter(rpage->values));

            // set right child's next pointer
            rpage->next = spage->next;

            // trim current node
            mkey = rpage->keys[0];
            spage->keys.erase(spage->keys.begin() + mpos, spage->keys.end());
            spage->values.erase(spage->values.begin() + mpos,
                                spage->values.end());

            // make current node's next link to new right node
            spage->next = rpage;

        } else if (page->type == PAGE_ITNL) {
            // if splitting a non-root internal node
            DEBUG("split internal %p", static_cast<void*>(page));

            auto* spage = reinterpret_cast<PageItnl<K, V>*>(page);
            auto* rpage = NewPageItnl();
            rpage_saved = rpage;

            // populate right child
            std::copy(spage->keys.begin() + mpos + 1, spage->keys.end(),
                      std::back_inserter(rpage->keys));
            std::copy(spage->children.begin() + mpos + 1, spage->children.end(),
                      std::back_inserter(rpage->children));

            // trim current node
            mkey = spage->keys[mpos];
            spage->keys.erase(spage->keys.begin() + mpos, spage->keys.end());
            spage->children.erase(spage->children.begin() + mpos + 1,
                                  spage->children.end());
        } else
            throw GarnerException("unknown page type encountered");

        // insert the uplifted key into parent node
        Page<K>* parent = path[path.size() - 2];
        assert(parent->NumKeys() < degree);
        ssize_t idx = parent->SearchKey(mkey);
        if (parent->type == PAGE_ROOT) {
            reinterpret_cast<PageRoot<K, V>*>(parent)->Inject(idx, mkey, page,
                                                              rpage_saved);
        } else {
            reinterpret_cast<PageItnl<K, V>*>(parent)->Inject(idx, mkey, page,
                                                              rpage_saved);
        }

        // if parent internal node becomes full, do split recursively
        if (parent->NumKeys() >= degree) {
            path.pop_back();
            SplitPage(parent, path);
            path.push_back(rpage_saved);
        } else {
            path.back() = rpage_saved;
        }
    }
}

template <typename K, typename V>
void BPTree<K, V>::Put(K key, V value) {
    DEBUG("req Put %s val %s", StreamStr(key).c_str(),
          StreamStr(value).c_str());

    // traverse to the correct leaf node and read
    std::vector<Page<K>*> path;
    std::vector<Page<K>*> write_latched_pages;
    std::tie(path, write_latched_pages) = TraverseToLeaf(key, LATCH_WRITE);
    assert(path.size() > 0);
    Page<K>* leaf = path.back();

    // inject key-value pair into the leaf node
    assert(leaf->NumKeys() < degree);
    ssize_t idx = leaf->SearchKey(key);
    if (leaf->type == PAGE_ROOT)
        reinterpret_cast<PageRoot<K, V>*>(leaf)->Inject(idx, key, value);
    else
        reinterpret_cast<PageLeaf<K, V>*>(leaf)->Inject(idx, key, value);

    // if this leaf node becomes full, do split
    if (leaf->NumKeys() >= degree) SplitPage(leaf, path);

    // release held write latch(es)
    assert(write_latched_pages.size() > 0);
    assert(write_latched_pages.back() == leaf);
    for (auto* page : write_latched_pages) {
        page->latch.unlock();
        DEBUG("latch W release %p", static_cast<void*>(page));
    }
}

template <typename K, typename V>
bool BPTree<K, V>::Get(const K& key, V& value) {
    DEBUG("req Get %s", StreamStr(key).c_str());

    // traverse to the correct leaf node and read
    std::vector<Page<K>*> path;
    std::tie(path, std::ignore) = TraverseToLeaf(key, LATCH_READ);
    assert(path.size() > 0);
    Page<K>* leaf = path.back();

    // search in leaf node for key
    ssize_t idx = leaf->SearchKey(key);
    if (idx == -1 || leaf->keys[idx] != key) {
        // not found
        // release held read latch
        leaf->latch.unlock_shared();
        DEBUG("latch R release %p", static_cast<void*>(leaf));
        return false;
    }

    // found match key, fetch value
    if (leaf->type == PAGE_ROOT)
        value = reinterpret_cast<PageRoot<K, V>*>(leaf)->values[idx];
    else
        value = reinterpret_cast<PageLeaf<K, V>*>(leaf)->values[idx];

    // release held read latch
    leaf->latch.unlock_shared();
    DEBUG("latch R release %p", static_cast<void*>(leaf));
    return true;
}

template <typename K, typename V>
bool BPTree<K, V>::Delete(__attribute__((unused)) const K& _key) {
    // TODO: implement me
    throw GarnerException("Delete not implemented yet!");
}

template <typename K, typename V>
size_t BPTree<K, V>::Scan(const K& lkey, const K& rkey,
                          std::vector<std::tuple<K, V>>& results) {
    DEBUG("req Scan %s to %s", StreamStr(lkey).c_str(),
          StreamStr(rkey).c_str());

    if (lkey > rkey) return 0;

    // traverse to leaf node for left bound of range
    std::vector<Page<K>*> lpath;
    std::tie(lpath, std::ignore) = TraverseToLeaf(lkey, LATCH_READ);
    assert(lpath.size() > 0);
    Page<K>* lleaf = lpath.back();

    // read out the leaf pages in a loop by following sibling chains,
    // gathering records in range
    Page<K>* leaf = lleaf;
    size_t nrecords = 0;
    while (true) {
        // if tree is completely empty, directly return
        if (leaf->NumKeys() == 0) {
            assert(leaf->type == PAGE_ROOT);
            leaf->latch.unlock_shared();
            DEBUG("latch R release %p", static_cast<void*>(leaf));
            return 0;
        }

        // do a search if in left bound leaf page
        size_t lidx = 0;
        if (leaf == lleaf) {
            ssize_t idx = leaf->SearchKey(lkey);
            if (idx >= 0 && leaf->keys[idx] == lkey)
                lidx = idx;
            else if (idx == -1)
                lidx = 0;
            else
                lidx = idx + 1;
        }

        // gather records within range; watch out for right bound
        for (size_t idx = lidx; idx < leaf->NumKeys(); ++idx) {
            K key = leaf->keys[idx];
            if (key > rkey) {
                leaf->latch.unlock_shared();
                DEBUG("latch R release %p", static_cast<void*>(leaf));
                return nrecords;
            }

            V value;
            if (leaf->type == PAGE_ROOT)
                value = reinterpret_cast<PageRoot<K, V>*>(leaf)->values[idx];
            else
                value = reinterpret_cast<PageLeaf<K, V>*>(leaf)->values[idx];
            results.push_back(
                std::make_tuple(std::move(key), std::move(value)));
            nrecords++;
        }

        // move on to the right sibling
        if (leaf->type == PAGE_LEAF) {
            Page<K>* next = reinterpret_cast<PageLeaf<K, V>*>(leaf)->next;
            if (next == nullptr) {
                leaf->latch.unlock_shared();
                DEBUG("latch R release %p", static_cast<void*>(leaf));
                return nrecords;
            }

            // latch crabbing in leaf chaining as well
            // TODO: this blocking acquisition may no longer to deadlock-free
            // once there are deletions
            next->latch.lock_shared();
            DEBUG("latch R acquire %p", static_cast<void*>(next));
            leaf->latch.unlock_shared();
            DEBUG("latch R release %p", static_cast<void*>(leaf));
            leaf = next;
        } else {
            leaf->latch.unlock_shared();
            DEBUG("latch R release %p", static_cast<void*>(leaf));
            return nrecords;
        }
    }
}

template <typename K, typename V>
void BPTree<K, V>::PrintStats(bool print_pages) {
    BPTreeStats stats;
    stats.npages = all_pages.size();
    stats.npages_itnl = 0;
    stats.npages_leaf = 0;
    stats.nkeys_itnl = 0;
    stats.nkeys_leaf = 0;

    if (print_pages) std::cout << "Pages:" << std::endl;

    // scan through all pages
    for (auto* page : all_pages) {
        if (page->type == PAGE_ROOT || page->type == PAGE_ITNL) {
            stats.npages_itnl++;
            stats.nkeys_itnl += page->NumKeys();
        } else if (page->type == PAGE_LEAF) {
            stats.npages_leaf++;
            stats.nkeys_leaf += page->NumKeys();
        } else
            throw GarnerException("unknown page type encountered");

        if (print_pages) {
            printf(" %p ", static_cast<void*>(page));
            if (page->type == PAGE_ROOT) {
                std::cout << *reinterpret_cast<PageRoot<K, V>*>(page)
                          << std::endl;
            } else if (page->type == PAGE_ITNL) {
                std::cout << *reinterpret_cast<PageItnl<K, V>*>(page)
                          << std::endl;
            } else {
                std::cout << *reinterpret_cast<PageLeaf<K, V>*>(page)
                          << std::endl;
            }
        }
    }

    // if tree only has one page, root is the only leaf
    if (stats.npages == 1) {
        assert(stats.npages_itnl == 1);
        assert(stats.npages_leaf == 0);
        assert(stats.nkeys_leaf == 0);
        stats.nkeys_leaf = stats.nkeys_itnl;
        stats.nkeys_itnl = 0;
        stats.npages_leaf = 1;
        stats.npages_itnl = 0;
    }

    assert(stats.npages == stats.npages_itnl + stats.npages_leaf);
    std::cout << stats << std::endl;
}

}  // namespace garner
