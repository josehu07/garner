// TxnCxt -- base transaction and concurrency control context type.

#include <atomic>
#include <iostream>

#include "record.hpp"

#pragma once

namespace garner {

/**
 * Base transaction context type.
 */
template <typename K, typename V>
class TxnCxt {
   public:
    // a transaction starts upon the construction of a TxnCxt
    TxnCxt() = default;

    TxnCxt(const TxnCxt&) = delete;
    TxnCxt& operator=(const TxnCxt&) = delete;

    virtual ~TxnCxt() = default;

    /**
     * Called upon a specific operation type within a transaction.
     * Concurrency control sub-types should implement these methods.
     */
    virtual bool ExecReadRecord(Record<V>* record, V& value) = 0;
    virtual void ExecWriteRecord(Record<V>* record, V value) = 0;
    virtual void ExecReadTraverseNode(Page<K>* page) = 0;
    virtual void ExecWriteTraverseNode(Page<K>* page) = 0;

    /**
     * Validate upon transaction commit. If can commit, reflect its effect to
     * the database; otherwise, must abort.
     *
     * The arguments are for returning the serialization point order for
     * testing purposes.
     *
     * Returns true if committed, or false if aborted.
     */
    virtual bool TryCommit(std::atomic<uint64_t>* ser_counter = nullptr,
                           uint64_t* ser_order = nullptr) = 0;
};

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s, const TxnCxt<K, V>& txn) {
    s << "TxnCxt{}";
    return s;
}

}  // namespace garner
