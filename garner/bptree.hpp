// BPTree -- simple B+ tree class.

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
#include "format.hpp"
#include "pager.hpp"

#pragma once

namespace garner {

/**
 * Single-file backed simple B+ tree. Non thread-safe. Supporting only
 * integral key and value types within 64-bit width.
 */
template <typename K, typename V>
class BPTree {
    friend GarnerException;
    friend BPTreeStats;
    friend Pager;

   private:
    const std::string filename;
    int fd = -1;

    const size_t degree = MAXNKEYS;

    Pager* pager = nullptr;

    /** Reopen the backing file with possibly different flags. */
    void ReopenBackingFile(int flags = 0);

    /**
     * Search in page for the closest key that is <= given key.
     * Returns the index to the key in content array, or 0 if all existing
     * keys are greater than given key.
     */
    size_t PageSearchKey(const Page& page, K key);

    /**
     * Insert a key-value pair into leaf page, shifting array content if
     * necessary. serach_idx should be calculated through PageSearchKey.
     */
    void LeafPageInject(Page& page, size_t search_idx, K key, V value);

    /**
     * Insert a key into internal node (carrying its left and right child
     * pageids), shifting array content if necessary. serach_idx should
     * be calculated through PageSearchKey.
     */
    void ItnlPageInject(Page& page, size_t search_idx, K key, uint64_t lpageid,
                        uint64_t rpageid);

    /**
     * Do B+ tree search to traverse through internal nodes and find the
     * correct leaf node.
     * Returns a tuple, where the first element is a vector of node pageids
     * starting from root to the searched leaf node, and the second element
     * being the key's index within the last-level internal node.
     */
    std::tuple<std::vector<uint64_t>, size_t> TraverseToLeaf(K key);

    /**
     * Split the given page into two siblings, and propagate one new key
     * up to the parent node. May trigger cascading splits. The path
     * argument is a list of internal node pageids, starting root, leading
     * to the node to be split.
     * After this function returns, the path vector will be updated to
     * reflect the new path to the right sibling node.
     */
    void SplitPage(uint64_t pageid, Page& page, std::vector<uint64_t>& path);

   public:
    BPTree(const std::string& filename, size_t degree);
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
     * Delete the record mathcing key.
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
     * Bulk-load a collection of records into an empty B+ tree instance.
     * Only works on a new empty backing file. The records vector must be
     * sorted on key in increasing order and must contain no duplicate
     * keys, otherwise an exception will be thrown.
     */
    void Load(const std::vector<std::tuple<K, V>>& records);

    /**
     * Scan the whole backing file and print statistics.
     * If print_pages is true, also prints content of all pages.
     */
    void PrintStats(bool print_pages = false);
};

}  // namespace garner

// Include template implementation in-place.
#include "bptree.tpl.hpp"
