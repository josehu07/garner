// TxnSilo -- simplified Silo concurrency control protocol.

#include <algorithm>
#include <atomic>
#include <iostream>
#include <map>
#include <vector>

#include "build_options.hpp"
#include "common.hpp"
#include "record.hpp"
#include "txn.hpp"

#pragma once

namespace garner {

/**
 * Silo transaction context type.
 * https://dl.acm.org/doi/10.1145/2517349.2522713
 */
template <typename K, typename V>
class TxnSilo : public TxnCxt<K, V> {
   private:
    // read list storing record -> read version
    // using an std::vector of items to speed up the sequential loop of
    // generating new version number
    struct RecordListItem {
        Record<K, V>* record;
        uint64_t version;
    };

    std::vector<RecordListItem> read_vec;

    // read set storing record -> index in read_vec
    std::unordered_map<Record<K, V>*, size_t> read_set;

    // write set storing record -> new value
    std::map<Record<K, V>*, V> write_set;

    // true if abort decision already made during execution
    bool must_abort = false;

   public:
    TxnSilo() : TxnCxt<K, V>(), read_set(), write_set(), must_abort(false) {}

    TxnSilo(const TxnSilo&) = delete;
    TxnSilo& operator=(const TxnSilo&) = delete;

    ~TxnSilo() = default;

    /**
     * Save record to read set, set value to its current read value.
     * Returns true if read is successful, or false if reading a phantom
     * record inserted by some other transaction without filled value.
     */
    bool ExecReadRecord(Record<K, V>* record, V& value);

    /**
     * Save record to write set and locally remember attempted write value.
     */
    void ExecWriteRecord(Record<K, V>* record, V value);

    /**
     * Not used.
     */
    void ExecReadTraverseNode([[maybe_unused]] Page<K>* page) {}
    void ExecWriteTraverseNode([[maybe_unused]] Page<K>* page,
                               [[maybe_unused]] unsigned height) {}
    void ExecEnterPut() {}
    void ExecLeavePut() {}
    void ExecEnterGet() {}
    void ExecLeaveGet() {}
    void ExecEnterDelete() {}
    void ExecLeaveDelete() {}
    void ExecEnterScan() {}
    void ExecLeaveScan() {}

    /**
     * Silo validation and commit protocol.
     */
    bool TryCommit(std::atomic<uint64_t>* ser_counter = nullptr,
                   uint64_t* ser_order = nullptr, TxnStats* stats = nullptr);

    template <typename KK, typename VV>
    friend std::ostream& operator<<(
        std::ostream& s, const typename TxnSilo<KK, VV>::RecordListItem& ritem);

    template <typename KK, typename VV>
    friend std::ostream& operator<<(std::ostream& s,
                                    const TxnSilo<KK, VV>& txn);
};

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s,
                         const typename TxnSilo<K, V>::RecordListItem& ritem) {
    s << "RLItem{record=" << ritem.record << ",version=" << ritem.version
      << "}";
    return s;
}

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s, const TxnSilo<K, V>& txn) {
    s << "TxnSilo{read_vec=[";
    for (auto&& [r, ver] : txn.read_vec) s << "(" << r << "-" << ver << "),";
    s << "],write_set=[";
    for (auto&& [r, val] : txn.write_set) s << "(" << r << "-" << val << "),";
    s << "],must_abort=" << txn.must_abort << "}";
    return s;
}

}  // namespace garner

// Include template implementation in-place.
#include "txn_silo.tpl.hpp"
