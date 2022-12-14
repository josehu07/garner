// Garner -- simple transactional DB interface to an in-memory B+-tree.

#include <atomic>
#include <iostream>
#include <string>
#include <vector>

#pragma once

namespace garner {

// forward declare TxnCxt type
// Clients should never need to call any methods on this type, but rather
// just passing pointers around through Garner's public interface.
template <typename K, typename V>
class TxnCxt;

/** Statistics buffer. */
struct BPTreeStats {
    unsigned height;
    size_t npages;
    size_t npages_itnl;  // includes root page if it's not the only leaf
    size_t npages_leaf;
    size_t nkeys_itnl;
    size_t nkeys_leaf;
};

std::ostream& operator<<(std::ostream& s, const BPTreeStats& stats);

/**
 * Transaction concurrency control protocols enum.
 */
typedef enum TxnProtocol {
    PROTOCOL_NONE,    // no concurrency control
    PROTOCOL_SILO,    // simplified Silo
    PROTOCOL_SILO_HV  // Silo with hierarchical validation
} TxnProtocol;

/**
 * Garner in-memory KV-DB interface.
 *
 * Currently hardcodes both key and value types as std::string. Exposing
 * generic types may require Garner be included as a pure template library.
 */
class Garner {
   public:
    typedef std::string KType;
    typedef std::string VType;

    /**
     * Opens a Gerner KV-DB, returning a pointer to the interface on success.
     * The returned interface is thread-safe and can be used by multiple
     * client threads.
     *
     * Exceptions might be thrown.
     *
     * The returned struct should be deleted when no longer needed.
     */
    static Garner* Open(size_t degree, TxnProtocol protocol);

    Garner() = default;

    Garner(const Garner&) = delete;
    Garner& operator=(const Garner&) = delete;

    virtual ~Garner() = default;

    /**
     * Start a transaction by creating a transaction context to be passed in
     * to subsequent operations of the transactio.
     *
     * Exceptions might be thrown.
     */
    virtual TxnCxt<KType, VType>* StartTxn() = 0;

    /**
     * Attempt validation and commit of transaction.
     *
     * The arguments are for returning the serialization point order for
     * testing purposes.
     *
     * Returns true if commited, or false if aborted.
     */
    virtual bool FinishTxn(TxnCxt<KType, VType>* txn,
                           std::atomic<uint64_t>* ser_counter = nullptr,
                           uint64_t* ser_order = nullptr) = 0;

    /**
     * Insert a key-value pair into B+ tree.
     *
     * If txn is nullptr, this operation will automatically be treated as a
     * single-op transaction.
     *
     * If txn is nullptr, returns true if successfully committed, or false if
     * aborted. If txn is given, always returns false.
     *
     * Exceptions might be thrown.
     */
    virtual bool Put(KType key, VType value,
                     TxnCxt<KType, VType>* txn = nullptr) = 0;

    /**
     * Search for a key, fill given reference with value and set found to true.
     * If not found, set found to false.
     *
     * If txn is nullptr, this operation will automatically be treated as a
     * single-op transaction.
     *
     * If txn is nullptr, returns true if successfully committed, or false if
     * aborted. If txn is given, always returns false.
     *
     * Exceptions might be thrown.
     */
    virtual bool Get(const KType& key, VType& value, bool& found,
                     TxnCxt<KType, VType>* txn = nullptr) = 0;

    /**
     * Delete the record matching key and set found to true. If not found, set
     * found to false.
     *
     * If txn is nullptr, this operation will automatically be treated as a
     * single-op transaction.
     *
     * If txn is nullptr, returns true if successfully committed, or false if
     * aborted. If txn is given, always returns false.
     *
     * Exceptions might be thrown.
     */
    virtual bool Delete(const KType& key, bool& found,
                        TxnCxt<KType, VType>* txn = nullptr) = 0;

    /**
     * Do a range scan over an inclusive key range [lkey, rkey], and
     * append found records to the given vector. Sets nrecords to the number
     * of records found within range.
     *
     * If txn is nullptr, this operation will automatically be treated as a
     * single-op transaction.
     *
     * If txn is nullptr, returns true if successfully committed, or false if
     * aborted. If txn is given, always returns false.
     *
     * Exceptions might be thrown.
     */
    virtual bool Scan(const KType& lkey, const KType& rkey,
                      std::vector<std::tuple<KType, VType>>& results,
                      size_t& nrecords,
                      TxnCxt<KType, VType>* txn = nullptr) = 0;

    /**
     * Iterate through the whole B+-tree, gather and verify statistics. If
     * print_pages is true, also prints content of all pages.
     *
     * This method is only for debugging; it is NOT thread-safe.
     */
    virtual BPTreeStats GatherStats(bool print_pages = false) = 0;
};

}  // namespace garner
