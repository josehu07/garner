// TxnCxt -- base transaction and concurrency control context type.

#include <atomic>
#include <iostream>

#ifdef TXN_STAT
#include <chrono>
#endif

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
    virtual bool ExecReadRecord(Record<K, V>* record, V& value) = 0;
    virtual void ExecWriteRecord(Record<K, V>* record, V value) = 0;
    virtual void ExecReadTraverseNode(Page<K>* page) = 0;
    virtual void ExecWriteTraverseNode(Page<K>* page, unsigned height) = 0;
    virtual void ExecEnterPut() = 0;
    virtual void ExecLeavePut() = 0;
    virtual void ExecEnterGet() = 0;
    virtual void ExecLeaveGet() = 0;
    virtual void ExecEnterDelete() = 0;
    virtual void ExecLeaveDelete() = 0;
    virtual void ExecEnterScan() = 0;
    virtual void ExecLeaveScan() = 0;

    /**
     * Validate upon transaction commit. If can commit, reflect its effect to
     * the database; otherwise, must abort.
     *
     * The arguments are for returning the serialization point order for
     * testing purposes.
     *
     * Returns true if committed, or false if aborted.
     */
#ifndef TXN_STAT
    virtual bool TryCommit(std::atomic<uint64_t>* ser_counter = nullptr,
                           uint64_t* ser_order = nullptr) = 0;
#else
    virtual bool TryCommit(std::atomic<uint64_t>* ser_counter = nullptr,
                           uint64_t* ser_order = nullptr,
                           TxnStats* stats = nullptr) = 0;
#endif
};

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s, const TxnCxt<K, V>& txn) {
    s << "TxnCxt{}";
    return s;
}

}  // namespace garner
