// Garner -- simple transactional DB interface to a B+-tree.

#include <cstdlib>
#include <string>
#include <vector>

#pragma once

namespace garner {

/**
 * Garner KV-DB interface struct.
 * Currently hardcodes key and value types as uint64_t.
 */
class Garner {
   public:
    typedef uint64_t KType;
    typedef uint64_t VType;

    /**
     * Opens a Gerner DB with specified backing file, returning a pointer to
     * the interface struct on success and nullptr otherwise.
     * Exceptions might be thrown.
     */
    static Garner* Open(const std::string& bptree_backfile, size_t degree);

    Garner() = default;

    Garner(const Garner&) = delete;
    Garner& operator=(const Garner&) = delete;

    ~Garner() = default;

    /**
     * Insert a key-value pair into B+ tree.
     * Exceptions might be thrown.
     */
    virtual void Put(KType key, VType value) = 0;

    /**
     * Search for a key, fill given reference with value.
     * Returns false if search failed or key not found.
     * Exceptions might be thrown.
     */
    virtual bool Get(KType key, VType& value) = 0;

    /**
     * Delete the record mathcing key.
     * Returns true if key found, otherwise false.
     * Exceptions might be thrown.
     */
    virtual bool Delete(KType key) = 0;

    /**
     * Do a range scan over an inclusive key range [lkey, rkey], and
     * append found records to the given vector.
     * Returns the number of records found within range.
     * Exceptions might be thrown.
     */
    virtual size_t Scan(KType lkey, KType rkey,
                        std::vector<std::tuple<KType, VType>>& results) = 0;

    /**
     * Scan the whole backing file and print statistics.
     * If print_pages is true, also prints content of all pages.
     */
    virtual void PrintStats(bool print_pages = false) = 0;
};

}  // namespace garner
