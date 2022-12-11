// TxnSiloHV -- Silo concurrency control with hierarchical validation.

#include <algorithm>
#include <atomic>
#include <iostream>
#include <map>
#include <tuple>
#include <utility>
#include <vector>

#include "common.hpp"
#include "record.hpp"
#include "txn.hpp"

#pragma once

namespace garner {

/**
 * Silo transaction context type with hierarchical validation.
 */
template <typename K, typename V>
class TxnSiloHV : public TxnCxt<K, V> {
   private:
    // read list storing node/record -> read version in traversal order
    struct ReadListItem {
        bool is_record;
        union {
            Page<K>* page;
            Record<K, V>* record;
        };
        uint64_t version;
    };

    std::vector<ReadListItem> read_list;

    // write list storing node/record -> new value in traversal order
    // first field true means a B+-tree node, else a record
    struct WriteListItem {
        bool is_record;
        union {
            Page<K>* page;
            Record<K, V>* record;
        };
        V value;
    };

    std::vector<WriteListItem> write_list;

    // true if abort decision already made during execution
    bool must_abort = false;

   public:
    TxnSiloHV()
        : TxnCxt<K, V>(), read_list(), write_list(), must_abort(false) {}

    TxnSiloHV(const TxnSiloHV&) = delete;
    TxnSiloHV& operator=(const TxnSiloHV&) = delete;

    ~TxnSiloHV() = default;

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
     * Save traversal information on page node for read.
     */
    void ExecReadTraverseNode(Page<K>* page);

    /**
     * Save traversal information on page node for write.
     */
    void ExecWriteTraverseNode(Page<K>* page);

    /**
     * Silo hierarchical validation and commit protocol.
     */
    bool TryCommit(std::atomic<uint64_t>* ser_counter = nullptr,
                   uint64_t* ser_order = nullptr);

    template <typename KK, typename VV>
    friend std::ostream& operator<<(
        std::ostream& s, const typename TxnSiloHV<KK, VV>::ReadListItem& ritem);

    template <typename KK, typename VV>
    friend std::ostream& operator<<(
        std::ostream& s,
        const typename TxnSiloHV<KK, VV>::WriteListItem& witem);

    template <typename KK, typename VV>
    friend std::ostream& operator<<(std::ostream& s,
                                    const TxnSiloHV<KK, VV>& txn);
};

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s,
                         const typename TxnSiloHV<K, V>::ReadListItem& ritem) {
    s << "RLItem{is_record=" << ritem.is_record;
    if (ritem.is_record)
        s << ",record=" << ritem.record;
    else
        s << ",page=" << ritem.page;
    s << ",version=" << ritem.version << "}";
    return s;
}

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s,
                         const typename TxnSiloHV<K, V>::WriteListItem& witem) {
    s << "WLItem{is_record=" << witem.is_record;
    if (witem.is_record)
        s << ",record=" << witem.record;
    else {
        s << ",page=" << witem.page;
        s << ",value=" << witem.value << "}";
    }
    return s;
}

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s, const TxnSiloHV<K, V>& txn) {
    s << "TxnSiloHV{read_list=[";
    for (auto&& ritem : txn.read_list) s << ritem << ",";
    s << "],write_list=[";
    for (auto&& witem : txn.write_list) s << witem << ",";
    s << "],must_abort=" << txn.must_abort << "}";
    return s;
}

}  // namespace garner

// Include template implementation in-place.
#include "txn_silo_hv.tpl.hpp"
