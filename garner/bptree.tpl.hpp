// Template implementation included in-place by the ".hpp".

#pragma once

namespace garner {

template <typename K, typename V>
BPTree<K, V>::BPTree(size_t degree) : degree(degree) {
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
}

template <typename K, typename V>
BPTree<K, V>::~BPTree() {
    auto iterate_func = [](Page<K>* page) {
        // deallocate all records
        if (page->type == PAGE_LEAF) {
            auto* leaf = reinterpret_cast<PageLeaf<K, V>*>(page);
            for (auto* record : leaf->records) delete record;
        } else if (page->type == PAGE_ROOT) {
            auto* root = reinterpret_cast<PageRoot<K, V>*>(page);
            if (root->height == 1)
                for (auto* record : root->records) delete record;
        }

        // deallocate page
        delete page;
    };

    DepthFirstIterate(iterate_func);
}

template <typename K, typename V>
PageLeaf<K, V>* BPTree<K, V>::NewPageLeaf() {
    auto* page = new PageLeaf<K, V>(degree);
    if (page == nullptr)
        throw GarnerException("failed to allocate memory for new page");
    return page;
}

template <typename K, typename V>
PageItnl<K, V>* BPTree<K, V>::NewPageItnl(unsigned height) {
    auto* page = new PageItnl<K, V>(degree, height);
    if (page == nullptr)
        throw GarnerException("failed to allocate memory for new page");
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
    unsigned level = 0, height;
    std::vector<Page<K>*> path;
    std::vector<Page<K>*> write_latched_pages;

    if (latch_mode == LATCH_READ) {
        page->latch.lock_shared();
        DEBUG("page latch R acquire %p", static_cast<void*>(page));
    } else if (latch_mode == LATCH_WRITE) {
        page->latch.lock();
        DEBUG("page latch W acquire %p", static_cast<void*>(page));
        write_latched_pages.push_back(page);
    }

    // read out height of tree, check if root is the only leaf
    height = reinterpret_cast<PageRoot<K, V>*>(page)->height;
    if (height == 1) {
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
            DEBUG("page latch R acquire %p", static_cast<void*>(child));
            page->latch.unlock_shared();
            DEBUG("page latch R release %p", static_cast<void*>(page));
        } else if (latch_mode == LATCH_WRITE) {
            child->latch.lock();
            DEBUG("page latch W acquire %p", static_cast<void*>(child));
            // if child is safe, release all ancestors' write latches
            if (IsConcurrencySafe(child)) {
                assert(write_latched_pages.back() == page);
                for (auto* ancestor : write_latched_pages) {
                    ancestor->latch.unlock();
                    DEBUG("page latch W release %p",
                          static_cast<void*>(ancestor));
                }
                write_latched_pages.clear();
            }
            write_latched_pages.push_back(child);
        }

        level++;
        if (level == height - 1) {
            path.push_back(child);
            // proper latch(es) still held on return
            return std::make_tuple(path, write_latched_pages);
        }

        page = child;
    }
}

template <typename K, typename V>
void BPTree<K, V>::SplitPage(Page<K>* page, std::vector<Page<K>*>& path,
                             const K& trigger_key) {
    if (page->type == PAGE_ROOT) {
        // if spliting root page, need to allocate two pages
        auto* spage = reinterpret_cast<PageRoot<K, V>*>(page);
        assert(path.size() == 1);
        assert(spage == root);
        assert(path[0] == root);

        size_t mpos = spage->NumKeys() / 2;
        Page<K>*lpage_saved, *rpage_saved;
        K mkey;

        if (spage->height == 1) {
            // special case of the very first split of root leaf
            DEBUG("split root leaf %p", static_cast<void*>(spage));

            auto* lpage = NewPageLeaf();
            auto* rpage = NewPageLeaf();
            lpage_saved = lpage;
            rpage_saved = rpage;

            // populate left child
            std::copy(spage->keys.begin(), spage->keys.begin() + mpos,
                      std::back_inserter(lpage->keys));
            std::copy(spage->records.begin(), spage->records.begin() + mpos,
                      std::back_inserter(lpage->records));

            // set left child's next pointer
            lpage->next = rpage;

            // populate right child
            std::copy(spage->keys.begin() + mpos, spage->keys.end(),
                      std::back_inserter(rpage->keys));
            std::copy(spage->records.begin() + mpos, spage->records.end(),
                      std::back_inserter(rpage->records));

            // populate split node with first key of right child
            mkey = rpage->keys[0];
            spage->keys.clear();
            spage->records.clear();
            spage->keys.push_back(mkey);

        } else {
            // splitting root into two internal nodes
            DEBUG("split root internal %p", static_cast<void*>(spage));

            auto* lpage = NewPageItnl(spage->height);
            auto* rpage = NewPageItnl(spage->height);
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
            mkey = spage->keys[mpos];
            spage->keys.clear();
            spage->children.clear();
            spage->keys.push_back(mkey);
        }

        // set new root child pointers, increment tree height
        spage->children.push_back(lpage_saved);
        spage->children.push_back(rpage_saved);
        spage->height++;

        // update path vector
        if (mkey <= trigger_key)
            path.push_back(rpage_saved);
        else
            path.push_back(lpage_saved);

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
            std::copy(spage->records.begin() + mpos, spage->records.end(),
                      std::back_inserter(rpage->records));

            // set right child's next pointer
            rpage->next = spage->next;

            // trim current node
            mkey = rpage->keys[0];
            spage->keys.erase(spage->keys.begin() + mpos, spage->keys.end());
            spage->records.erase(spage->records.begin() + mpos,
                                 spage->records.end());

            // make current node's next link to new right node
            spage->next = rpage;

        } else if (page->type == PAGE_ITNL) {
            // if splitting a non-root internal node
            DEBUG("split internal %p", static_cast<void*>(page));

            auto* spage = reinterpret_cast<PageItnl<K, V>*>(page);
            auto* rpage = NewPageItnl(page->height);
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
            SplitPage(parent, path, trigger_key);
            if (mkey <= trigger_key)
                path.push_back(rpage_saved);
            else
                path.push_back(page);
        } else {
            if (mkey <= trigger_key) path.back() = rpage_saved;
        }
    }
}

template <typename K, typename V>
template <typename Func>
void BPTree<K, V>::DepthFirstIterate(Func func) {
    // if root is the only leaf
    if (root->height == 1) {
        func(root);
        return;
    }

    // current traversal stack of (page, curr_child_idx) pairs
    std::vector<std::pair<Page<K>*, size_t>> stack = {std::make_pair(root, 0)};
    stack.reserve(root->height);
    assert(root->children.size() > 0);

    // depth-first post-order walk
    while (true) {
        Page<K>* page = stack.back().first;

        if (page->type == PAGE_ITNL) {
            // is non-root internal node
            auto* itnl = reinterpret_cast<PageItnl<K, V>*>(page);
            size_t child_idx = stack.back().second;
            if (child_idx < itnl->children.size())
                stack.emplace_back(itnl->children[child_idx], 0);
            else {
                func(itnl);
                stack.pop_back();
                assert(stack.size() > 0);
                stack.back().second++;
            }
        } else if (page->type == PAGE_ROOT) {
            // is internal root
            assert(page == root);
            size_t child_idx = stack.back().second;
            if (child_idx < root->children.size())
                stack.emplace_back(root->children[child_idx], 0);
            else {
                func(root);
                stack.pop_back();
                assert(stack.size() == 0);
                break;
            }
        } else {
            // is leaf node
            auto* leaf = reinterpret_cast<PageLeaf<K, V>*>(page);
            func(leaf);
            stack.pop_back();
            assert(stack.size() > 0);
            stack.back().second++;
        }
    }
}

template <typename K, typename V>
void BPTree<K, V>::Put(K key, V value, TxnCxt<K, V>* txn) {
    DEBUG("req Put %s val %s", StreamStr(key).c_str(),
          StreamStr(value).c_str());

    // traverse to the correct leaf node and read
    std::vector<Page<K>*> path;
    std::vector<Page<K>*> write_latched_pages;
    std::tie(path, write_latched_pages) = TraverseToLeaf(key, LATCH_WRITE);
    assert(path.size() > 0);
    Page<K>* leaf = path.back();

    // inject key into the leaf node and get pointer to record
    assert(leaf->NumKeys() < degree);
    Record<K, V>* record = nullptr;
    ssize_t idx = leaf->SearchKey(key);
    if (leaf->type == PAGE_ROOT)
        record = reinterpret_cast<PageRoot<K, V>*>(leaf)->Inject(idx, key);
    else
        record = reinterpret_cast<PageLeaf<K, V>*>(leaf)->Inject(idx, key);
    assert(record != nullptr);

    // if this leaf node becomes full, do split
    if (leaf->NumKeys() >= degree) SplitPage(leaf, path, key);

    // call concurrency control algorithm's traversal logic
    if (txn != nullptr)
        for (auto* page : path) txn->ExecWriteTraverseNode(page);

    // release held page write latch(es)
    assert(write_latched_pages.size() > 0);
    assert(write_latched_pages.back() == leaf);
    for (auto* page : write_latched_pages) {
        page->latch.unlock();
        DEBUG("page latch W release %p", static_cast<void*>(page));
    }

    // if no concurrency control, write now; otherwise call handler
    if (txn == nullptr) {
        record->latch.lock();
        DEBUG("record latch W acquire %p", static_cast<void*>(record));
        record->value = value;
        record->latch.unlock();
        DEBUG("record latch W release %p", static_cast<void*>(record));
    } else
        txn->ExecWriteRecord(record, std::move(value));
}

template <typename K, typename V>
bool BPTree<K, V>::Get(const K& key, V& value, TxnCxt<K, V>* txn) {
    DEBUG("req Get %s", StreamStr(key).c_str());

    // traverse to the correct leaf node and read
    std::vector<Page<K>*> path;
    std::tie(path, std::ignore) = TraverseToLeaf(key, LATCH_READ);
    assert(path.size() > 0);
    Page<K>* leaf = path.back();

    // search in leaf node for key
    ssize_t idx = leaf->SearchKey(key);
    if (idx == -1 || leaf->keys[idx] != key) {
        // not found; release held read latch
        // current concurrency control DOES NOT prevent phantoms
        leaf->latch.unlock_shared();
        DEBUG("page latch R release %p", static_cast<void*>(leaf));
        return false;
    }

    // found match key, fetch record
    Record<K, V>* record = nullptr;
    if (leaf->type == PAGE_ROOT)
        record = reinterpret_cast<PageRoot<K, V>*>(leaf)->records[idx];
    else
        record = reinterpret_cast<PageLeaf<K, V>*>(leaf)->records[idx];
    assert(record != nullptr);

    // call concurrency control algorithm's traversal logic
    if (txn != nullptr)
        for (auto* page : path) txn->ExecReadTraverseNode(page);

    // release held page read latch
    leaf->latch.unlock_shared();
    DEBUG("page latch R release %p", static_cast<void*>(leaf));

    // fetch value in record; if has concurrency control, use the algorithm's
    // read protocol
    if (txn == nullptr) {
        record->latch.lock_shared();
        DEBUG("record latch R acquire %p", static_cast<void*>(record));
        value = record->value;
        record->latch.unlock_shared();
        DEBUG("record latch R release %p", static_cast<void*>(record));
        return true;
    } else
        return txn->ExecReadRecord(record, value);
}

template <typename K, typename V>
bool BPTree<K, V>::Delete([[maybe_unused]] const K& key,
                          [[maybe_unused]] TxnCxt<K, V>* txn) {
    // TODO: implement me
    throw GarnerException("Delete not implemented yet!");
}

template <typename K, typename V>
size_t BPTree<K, V>::Scan(const K& lkey, const K& rkey,
                          std::vector<std::tuple<K, V>>& results,
                          TxnCxt<K, V>* txn) {
    DEBUG("req Scan %s to %s", StreamStr(lkey).c_str(),
          StreamStr(rkey).c_str());

    if (lkey > rkey) return 0;

    // traverse to leaf node for left bound of range
    std::vector<Page<K>*> lpath;
    std::tie(lpath, std::ignore) = TraverseToLeaf(lkey, LATCH_READ);
    assert(lpath.size() > 0);
    Page<K>* lleaf = lpath.back();

    // call concurrency control algorithm's traversal logic
    if (txn != nullptr)
        for (auto* page : lpath) txn->ExecReadTraverseNode(page);

    // read out the leaf pages in a loop by following sibling chains,
    // gathering records in range
    Page<K>* leaf = lleaf;
    size_t nrecords = 0;
    while (true) {
        // if tree is completely empty, directly return
        if (leaf->NumKeys() == 0) {
            assert(leaf->type == PAGE_ROOT);
            leaf->latch.unlock_shared();
            DEBUG("page latch R release %p", static_cast<void*>(leaf));
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
                DEBUG("page latch R release %p", static_cast<void*>(leaf));
                return nrecords;
            }

            Record<K, V>* record;
            if (leaf->type == PAGE_ROOT)
                record = reinterpret_cast<PageRoot<K, V>*>(leaf)->records[idx];
            else
                record = reinterpret_cast<PageLeaf<K, V>*>(leaf)->records[idx];
            assert(record != nullptr);

            // if has concurrency control, use algorithm's read protocol
            // current concurrency control DOES NOT prevent phantoms
            V value;
            bool valid = false;
            if (txn == nullptr) {
                record->latch.lock_shared();
                DEBUG("record latch R acquire %p", static_cast<void*>(record));
                value = record->value;
                record->latch.unlock_shared();
                DEBUG("record latch R release %p", static_cast<void*>(record));
                valid = true;
            } else
                valid = txn->ExecReadRecord(record, value);

            if (valid) {
                results.push_back(
                    std::make_tuple(std::move(key), std::move(value)));
                nrecords++;
            }
        }

        // move on to the right sibling
        if (leaf->type == PAGE_LEAF) {
            Page<K>* next = reinterpret_cast<PageLeaf<K, V>*>(leaf)->next;
            if (next == nullptr) {
                leaf->latch.unlock_shared();
                DEBUG("page latch R release %p", static_cast<void*>(leaf));
                return nrecords;
            }

            // latch crabbing in leaf chaining as well
            // TODO: this blocking acquisition may no longer to deadlock-free
            // once there are deletions
            next->latch.lock_shared();
            DEBUG("page latch R acquire %p", static_cast<void*>(next));
            leaf->latch.unlock_shared();
            DEBUG("page latch R release %p", static_cast<void*>(leaf));
            leaf = next;

            // call concurrency control algorithm's traversal logic on
            // pointer-chained leaf
            if (txn != nullptr) txn->ExecReadTraverseNode(leaf);
        } else {
            leaf->latch.unlock_shared();
            DEBUG("page latch R release %p", static_cast<void*>(leaf));
            return nrecords;
        }
    }
}

template <typename K, typename V>
BPTreeStats BPTree<K, V>::GatherStats(bool print_pages) {
    BPTreeStats stats;
    stats.height = 0;
    stats.npages = 0;
    stats.npages_itnl = 0;
    stats.npages_leaf = 0;
    stats.nkeys_itnl = 0;
    stats.nkeys_leaf = 0;

    PageLeaf<K, V>* last_leaf = nullptr;
    Page<K>* last_page = nullptr;

    auto iterate_func = [&](Page<K>* page) {
        if (page->type == PAGE_ROOT) stats.height = page->height;

        if (print_pages) {
            printf(" %p ", static_cast<void*>(page));
            if (page->type == PAGE_ROOT) {
                std::cout << StreamStr(*reinterpret_cast<PageRoot<K, V>*>(page))
                          << std::endl;
            } else if (page->type == PAGE_ITNL) {
                std::cout << StreamStr(*reinterpret_cast<PageItnl<K, V>*>(page))
                          << std::endl;
            } else {
                std::cout << StreamStr(*reinterpret_cast<PageLeaf<K, V>*>(page))
                          << std::endl;
            }
        }

        if (page->type == PAGE_ROOT || page->type == PAGE_ITNL) {
            if (last_page != nullptr && page->height != last_page->height + 1) {
                throw GarnerException("stats: incorrect height " +
                                      std::to_string(page->height) +
                                      " of an internal node" + ", expect " +
                                      std::to_string(last_page->height + 1));
            }

            stats.npages++;
            stats.npages_itnl++;
            stats.nkeys_itnl += page->NumKeys();

        } else if (page->type == PAGE_LEAF) {
            if (page->height != 1) {
                throw GarnerException("stats: invalid height " +
                                      std::to_string(page->height) +
                                      " of a leaf node");
            }
            if (last_leaf != nullptr && last_leaf->next != page)
                throw GarnerException("stats: incorrect leaf chain pointer");
            last_leaf = reinterpret_cast<PageLeaf<K, V>*>(page);

            stats.npages++;
            stats.npages_leaf++;
            stats.nkeys_leaf += page->NumKeys();

        } else
            throw GarnerException("unknown page type encountered");

        last_page = page;
    };

    // scan through all pages
    if (print_pages) std::cout << "Pages:" << std::endl;
    DepthFirstIterate(iterate_func);

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

    if (stats.height < 1) {
        throw GarnerException("stats: invalid tree height " +
                              std::to_string(stats.height));
    }
    if (stats.npages != stats.npages_itnl + stats.npages_leaf) {
        throw GarnerException(
            "stats: total #pages " + std::to_string(stats.npages) +
            " does not match #itnl " + std::to_string(stats.npages_itnl) +
            " + #leaf " + std::to_string(stats.npages_leaf));
    }

    return stats;
}

}  // namespace garner
