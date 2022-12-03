// BPTree -- simple concurrent in-memory B+ tree class.

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <limits>
#include <new>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "common.hpp"
#include "page.hpp"

#pragma once

namespace garner {

/**
 * Simple concurrent in-memory B+ tree.
 */
template <typename K, typename V>
class BPTree {
   private:
    // max number of keys per node page
    const size_t degree = 0;

    // pointer to root page, set at initiailization
    PageRoot<K, V>* root = nullptr;

    // set of all pages allocated
    std::set<Page<K>*> all_pages;

    /**
     * Allocate a new page of specific type.
     */
    PageLeaf<K, V>* NewPageLeaf();
    PageItnl<K, V>* NewPageItnl();

    /**
     * Do B+ tree search to traverse through internal nodes and find the
     * correct leaf node.
     * Returns a vector of node pages starting from root to the searched
     * leaf node.
     */
    std::vector<Page<K>*> TraverseToLeaf(const K& key) const;

    /**
     * Split the given page into two siblings, and propagate one new key
     * up to the parent node. May trigger cascading splits. The path
     * argument is a list of internal node pages, starting from root, leading
     * to the node to be split.
     * After this function returns, the path vector will be updated to
     * reflect the new path to the right sibling node.
     */
    void SplitPage(Page<K>* page, std::vector<Page<K>*>& path);

   public:
    BPTree(size_t degree);
    ~BPTree();

    /**
     * Insert a key-value pair into B+ tree.
     * Exceptions might be thrown.
     */
    void Put(K key, V value);

    /**
     * Search for a key, fill given reference with value.
     * Returns false if search failed or key not found.
     * Exceptions might be thrown.
     */
    bool Get(const K& key, V& value);

    /**
     * Delete the record matching key.
     * Returns true if key found, otherwise false.
     * Exceptions might be thrown.
     */
    bool Delete(const K& key);

    /**
     * Do a range scan over an inclusive key range [lkey, rkey], and
     * append found records to the given vector.
     * Returns the number of records found within range.
     * Exceptions might be thrown.
     */
    size_t Scan(const K& lkey, const K& rkey,
                std::vector<std::tuple<K, V>>& results);

    /**
     * Scan the whole B+-tree and print statistics.
     * If print_pages is true, also prints content of all pages.
     */
    void PrintStats(bool print_pages = false);
};

}  // namespace garner

// Include template implementation in-place.
#include "bptree.tpl.hpp"
