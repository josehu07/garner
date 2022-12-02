// BPTree -- simple concurrent in-memory B+ tree class.

#include <cassert>
#include <cstring>
#include <iostream>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "common.hpp"
#include "page.hpp"

#pragma once

namespace garner {

/**
 * Simple concurrent in-memory B+ tree. Supporting only integral key and
 * value types within 64-bit width.
 */
template <typename K, typename V>
class BPTree {
    friend GarnerException;
    friend BPTreeStats;

   private:
    const size_t degree = MAXNKEYS;

    // pointer to root page, set at initiailization
    Page* root = nullptr;

    /** Allocate a new memory frame for holding a page. */
    Page* AllocNewFrame(PageType type);

    /**
     * Search in page for the closest key that is <= given key.
     * Returns the index to the key in content array, or 0 if all existing
     * keys are greater than given key.
     */
    size_t PageSearchKey(const Page* page, K key);

    /**
     * Insert a key-value pair into leaf page, shifting array content if
     * necessary. serach_idx should be calculated through PageSearchKey.
     */
    void LeafPageInject(Page* page, size_t search_idx, K key, V value);

    /**
     * Insert a key into internal node (carrying its left and right child
     * page pointers), shifting array content if necessary. serach_idx should
     * be calculated through PageSearchKey.
     */
    void ItnlPageInject(Page* page, size_t search_idx, K key, Page* lpage,
                        Page* rpage);

    /**
     * Do B+ tree search to traverse through internal nodes and find the
     * correct leaf node.
     * Returns a vector of node pages starting from root to the searched
     * leaf node.
     */
    std::vector<Page*> TraverseToLeaf(K key);

    /**
     * Split the given page into two siblings, and propagate one new key
     * up to the parent node. May trigger cascading splits. The path
     * argument is a list of internal node pages, starting from root, leading
     * to the node to be split.
     * After this function returns, the path vector will be updated to
     * reflect the new path to the right sibling node.
     */
    void SplitPage(Page* page, std::vector<Page*>& path);

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
    bool Get(K key, V& value);

    /**
     * Delete the record matching key.
     * Returns true if key found, otherwise false.
     * Exceptions might be thrown.
     */
    bool Delete(K key);

    /**
     * Do a range scan over an inclusive key range [lkey, rkey], and
     * append found records to the given vector.
     * Returns the number of records found within range.
     * Exceptions might be thrown.
     */
    size_t Scan(K lkey, K rkey, std::vector<std::tuple<K, V>>& results);

    /**
     * Scan the whole B+-tree and print statistics.
     * If print_pages is true, also prints content of all pages.
     */
    void PrintStats(bool print_pages = false);
};

}  // namespace garner

// Include template implementation in-place.
#include "bptree.tpl.hpp"
