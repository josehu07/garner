// BPTree -- simple concurrent in-memory B+ tree class.

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <limits>
#include <mutex>
#include <new>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_set>
#include <vector>

#include "common.hpp"
#include "include/garner.hpp"
#include "page.hpp"
#include "record.hpp"
#include "txn.hpp"

#pragma once

namespace garner {

/**
 * Simple concurrent in-memory B+ tree.
 */
template <typename K, typename V>
class BPTree {
   private:
    /** Latch mode during traversal. */
    typedef enum LatchMode { LATCH_READ, LATCH_WRITE, LATCH_NONE } LatchMode;

    // max number of keys per node page
    const size_t degree = 0;

    // pointer to root page, set at initiailization
    PageRoot<K, V>* root = nullptr;

    /**
     * Allocate a new page of specific type.
     */
    PageLeaf<K, V>* NewPageLeaf();
    PageItnl<K, V>* NewPageItnl(unsigned height);

    /**
     * Returns true if given page is safe from structural mutations in
     * concurrent latching, otherwise false.
     */
    bool IsConcurrencySafe(const Page<K>* page) const;

    /**
     * Do B+ tree search to traverse through internal nodes and find the
     * correct leaf node.
     *
     * Does "latch crabbing" for safe concurrency:
     * https://15445.courses.cs.cmu.edu/fall2018/slides/09-indexconcurrency.pdf
     * After return, proper latches will stay held according to latch mode. It
     * is the caller's job to unlock them later.
     *
     * Returns a tuple of two vectors: (path, write_latched_pages)
     * - path: list of node pages starting from root to the searched leaf node.
     * - write_latched_pages: list of pages still latched in write mode
     */
    std::tuple<std::vector<Page<K>*>, std::vector<Page<K>*>> TraverseToLeaf(
        const K& key, LatchMode latch_mode);

    /**
     * Split the given page into two siblings, and propagate one new key
     * up to the parent node. May trigger cascading splits. The path
     * argument is a list of internal node pages, starting from root, leading
     * to the node to be split. The trigger_key argument is the key whose
     * insertion triggered this split.
     *
     * Must have write latches already held on possibly affected pages.
     *
     * After this function returns, the path vector will be updated to
     * reflect the new path to the right sibling node.
     */
    void SplitPage(Page<K>* page, std::vector<Page<K>*>& path,
                   const K& trigger_key);

    /**
     * Iterate through all pages in tree in depth-first post-order manner,
     * applying given function to each page.
     */
    template <typename Func>
    void DepthFirstIterate(Func func);

   public:
    BPTree(size_t degree);
    ~BPTree();

    /**
     * Insert a key-value pair into B+ tree.
     *
     * Exceptions might be thrown.
     */
    void Put(K key, V value, TxnCxt<K, V>* txn);

    /**
     * Search for a key, fill given reference with value.
     * Returns false if search failed or key not found.
     *
     * Exceptions might be thrown.
     */
    bool Get(const K& key, V& value, TxnCxt<K, V>* txn);

    /**
     * Delete the record matching key.
     * Returns true if key found, otherwise false.
     *
     * Exceptions might be thrown.
     */
    bool Delete(const K& key, TxnCxt<K, V>* txn);

    /**
     * Do a range scan over an inclusive key range [lkey, rkey], and
     * append found records to the given vector.
     * Returns the number of records found within range.
     *
     * Exceptions might be thrown.
     */
    size_t Scan(const K& lkey, const K& rkey,
                std::vector<std::tuple<K, V>>& results, TxnCxt<K, V>* txn);

    /**
     * Iterate through the whole B+-tree, gather and verify statistics. If
     * print_pages is true, also prints content of all pages.
     *
     * This method is only for debugging; it is NOT thread-safe.
     */
    BPTreeStats GatherStats(bool print_pages = false);
};

}  // namespace garner

// Include template implementation in-place.
#include "bptree.tpl.hpp"
