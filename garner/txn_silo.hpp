// TxnSilo -- simplified Silo concurrency control protocol.

#include <algorithm>
#include <atomic>
#include <iostream>
#include <map>
#include <vector>

#include "common.hpp"
#include "record.hpp"
#include "txn.hpp"

#pragma once

namespace garner {

/**
 * Silo transaction context type.
 * https://dl.acm.org/doi/10.1145/2517349.2522713
 */
template <typename V>
class TxnSilo : public TxnCxt<V> {
   private:
    // read set storing record -> read version
    std::map<Record<V>*, uint64_t> read_set;

    // write set storing record -> new value
    std::map<Record<V>*, V> write_set;

    // true if abort decision already made during execution
    bool must_abort = false;

   public:
    TxnSilo() : TxnCxt<V>(), read_set(), write_set(), must_abort(false) {}

    TxnSilo(const TxnSilo&) = delete;
    TxnSilo& operator=(const TxnSilo&) = delete;

    ~TxnSilo() = default;

    /**
     * Save record to read set, set value to its current read value.
     * Returns true if read is successful, or false if reading a phantom
     * record inserted by some other transaction without filled value.
     */
    bool ExecReadRecord(Record<V>* record, V& value);

    /**
     * Save record to write set and locally remember attempted write value.
     */
    void ExecWriteRecord(Record<V>* record, V value);

    /**
     * Silo validation and commit protocol.
     */
    bool TryCommit(std::atomic<uint64_t>* ser_counter = nullptr,
                   uint64_t* ser_order = nullptr);

    template <typename VV>
    friend std::ostream& operator<<(std::ostream& s, const TxnSilo<VV>& txn);
};

template <typename V>
std::ostream& operator<<(std::ostream& s, const TxnSilo<V>& txn) {
    s << "TxnSilo{read_set=[";
    for (auto&& [r, ver] : txn.read_set) s << "(" << r << "-" << ver << "),";
    s << "],write_set=[";
    for (auto&& [r, val] : txn.write_set) s << "(" << r << "-" << val << "),";
    s << "],must_abort=" << txn.must_abort << "}";
    return s;
}

}  // namespace garner

// Include template implementation in-place.
#include "txn_silo.tpl.hpp"
