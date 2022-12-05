// TxnCxt -- base transaction and concurrency control context type.

#include <iostream>

#include "record.hpp"

#pragma once

namespace garner {

/**
 * Base transaction context type.
 */
template <typename V>
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
    virtual void ExecReadRecord(Record<V>* record, V& value) = 0;
    virtual void ExecWriteRecord(Record<V>* record, V value) = 0;

    /**
     * Validate upon transaction commit. If can commit, reflect its effect to
     * the database; otherwise, must abort.
     *
     * Returns true if committed, or false if aborted.
     */
    virtual bool TryCommit() = 0;
};

template <typename V>
std::ostream& operator<<(std::ostream& s, const TxnCxt<V>& txn) {
    s << "TxnCxt{}";
    return s;
}

}  // namespace garner
