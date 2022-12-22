// TxnSiloHV -- Silo concurrency control with hierarchical validation.

#include <algorithm>
#include <atomic>
#include <iostream>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "build_options.hpp"
#include "common.hpp"
#include "page.hpp"
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
    // we split the read_list into two vectors: one for tree nodes (pages) and
    // the other for records, to give better memory performance

    // record list storing record -> read version in traversal order
    struct RecordListItem {
        Record<K, V>* record;
        uint64_t version;
    };

    std::vector<RecordListItem> record_list;

    // page list storing node -> read version and children skipping info in
    // traversal order
    struct PageListItem {
        Page<K>* page;
        uint64_t version;
        size_t record_idx_start;
        size_t record_idx_end;
        size_t page_skip_to;
    };

    std::vector<PageListItem> page_list;

    // still maintain a map from node/record -> index in record_list/page_list,
    // for fast lookups
    std::unordered_map<Record<K, V>*, size_t> record_set;
    std::unordered_map<Page<K>*, size_t> page_set;

    // auxiliary map from height -> index of last enqueued node item, used for
    // setting skip_to information during Scan execution
    std::unordered_map<unsigned, size_t> last_read_node;
    bool in_scan = false;

    // write list storing node/record -> new value in traversal order
    // first field true means a B+-tree node, else a record
    struct WriteListItem {
        bool is_record;
        union {
            Page<K>* page;
            Record<K, V>* record;
        };
        std::variant<unsigned, V> height_or_value;
    };

    std::vector<WriteListItem> write_list;

    // still maintain a map from node/record -> index in write_list, for fast
    // lookups
    std::unordered_map<void*, size_t> write_set;

    // true if abort decision already made during execution
    bool must_abort = false;

    // set true to completely turn off read validation as performance roofline
    const bool no_read_validation = false;

   public:
    TxnSiloHV(bool no_read_validation = false)
        : TxnCxt<K, V>(),
          record_list(),
          page_list(),
          record_set(),
          page_set(),
          last_read_node(),
          in_scan(false),
          write_list(),
          write_set(),
          must_abort(false),
          no_read_validation(no_read_validation) {}

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
    void ExecWriteTraverseNode(Page<K>* page, unsigned height);

    /**
     * Not used.
     */
    void ExecEnterPut() {}
    void ExecLeavePut() {}
    void ExecEnterGet() {}
    void ExecLeaveGet() {}
    void ExecEnterDelete() {}
    void ExecLeaveDelete() {}

    /**
     * Turn on/off subtree crossing logic when entering/leaving a Scan.
     */
    void ExecEnterScan();
    void ExecLeaveScan();

    /**
     * Silo hierarchical validation and commit protocol.
     */
    bool TryCommit(std::atomic<uint64_t>* ser_counter = nullptr,
                   uint64_t* ser_order = nullptr, TxnStats* stats = nullptr);

    template <typename KK, typename VV>
    friend std::ostream& operator<<(
        std::ostream& s,
        const typename TxnSiloHV<KK, VV>::RecordListItem& ritem);

    template <typename KK, typename VV>
    friend std::ostream& operator<<(
        std::ostream& s, const typename TxnSiloHV<KK, VV>::PageListItem& pitem);

    template <typename KK, typename VV>
    friend std::ostream& operator<<(
        std::ostream& s,
        const typename TxnSiloHV<KK, VV>::WriteListItem& witem);

    template <typename KK, typename VV>
    friend std::ostream& operator<<(std::ostream& s,
                                    const TxnSiloHV<KK, VV>& txn);
};

template <typename K, typename V>
std::ostream& operator<<(
    std::ostream& s, const typename TxnSiloHV<K, V>::RecordListItem& ritem) {
    s << "RLItem{record=" << ritem.record << ",version=" << ritem.version
      << "}";
    return s;
}

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s,
                         const typename TxnSiloHV<K, V>::PageListItem& pitem) {
    s << "PLItem{page=" << pitem.page << ",version=" << pitem.version
      << ",record_idx_start=" << pitem.record_idx_start
      << ",record_idx_end=" << pitem.record_idx_end << "}";
    return s;
}

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s,
                         const typename TxnSiloHV<K, V>::WriteListItem& witem) {
    s << "WLItem{is_record=" << witem.is_record;
    if (witem.is_record) {
        s << ",record=" << witem.record;
        s << ",value=" << std::get<V>(witem.height_or_value) << "}";
    } else {
        s << ",page=" << witem.page;
        s << ",height=" << std::get<unsigned>(witem.height_or_value) << "}";
    }
    return s;
}

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s, const TxnSiloHV<K, V>& txn) {
    s << "TxnSiloHV{record_list=[";
    for (auto&& ritem : txn.record_list) s << ritem << ",";
    s << "],page_list=[";
    for (auto&& pitem : txn.page_list) s << pitem << ",";
    s << "],write_list=[";
    for (auto&& witem : txn.write_list) s << witem << ",";
    s << "],must_abort=" << txn.must_abort
      << ",no_read_validation=" << txn.no_read_validation << "}";
    return s;
}

}  // namespace garner

// Include template implementation in-place.
#include "txn_silo_hv.tpl.hpp"
