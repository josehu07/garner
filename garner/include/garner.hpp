// Garner -- simple transactional DB interface to an in-memory B+-tree.

#include <string>
#include <vector>

#pragma once

namespace garner {

/**
 * Garner in-memory KV-DB interface.
 * Currently hardcodes both key and value types as std::string. Exposing
 * generic types may require Garner be included as a pure template library.
 */
class Garner {
   public:
    typedef std::string KType;
    typedef std::string VType;

    /**
     * Opens a Gerner KV-DB, returning a pointer to the interface on success.
     * Exceptions might be thrown.
     */
    static Garner* Open(size_t degree);

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
    virtual bool Get(const KType& key, VType& value) = 0;

    /**
     * Delete the record matching key.
     * Returns true if key found, otherwise false.
     * Exceptions might be thrown.
     */
    virtual bool Delete(const KType& key) = 0;

    /**
     * Do a range scan over an inclusive key range [lkey, rkey], and
     * append found records to the given vector.
     * Returns the number of records found within range.
     * Exceptions might be thrown.
     */
    virtual size_t Scan(const KType& lkey, const KType& rkey,
                        std::vector<std::tuple<KType, VType>>& results) = 0;

    /**
     * Scan the whole B+-tree and print statistics.
     * If print_pages is true, also prints content of all pages.
     */
    virtual void PrintStats(bool print_pages = false) = 0;
};

}  // namespace garner
