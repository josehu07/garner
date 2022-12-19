// Template implementation included in-place by the ".hpp".

#pragma once

namespace garner {

template <typename K, typename V>
bool TxnSilo<K, V>::ExecReadRecord(Record<K, V>* record, V& value) {
    // fetch value and version
    record->latch.lock_shared();
    DEBUG("record latch R acquire %p", static_cast<void*>(record));
    bool valid = record->valid;
    V read_value = record->value;
    uint64_t read_version = record->version;
    record->latch.unlock_shared();
    DEBUG("record latch R release %p", static_cast<void*>(record));

    // if is a phantom record without filled value, ignore
    if (!write_set.contains(record) && !valid) return false;

    // if in my local write set, read from there instead
    if (write_set.contains(record))
        value = write_set[record];
    else
        value = std::move(read_value);

    // insert into read set if not in it yet
    if (read_set.contains(record)) {
        if (read_list[read_set[record]].version != read_version) {
            // same record read multiple times by the transaction and versions
            // already mismatch
            // we could just early abort here, but for simplicity, we save
            // this decision and abort at finish time
            must_abort = true;
        }
    } else {
        read_list.push_back(RecordListItem{.record = record,
                                            .version = read_version});
        read_set[record] = read_list.size() - 1;
    }

    return true;
}

template <typename K, typename V>
void TxnSilo<K, V>::ExecWriteRecord(Record<K, V>* record, V value) {
    // do not actually write; save value locally
    write_set[record] = std::move(value);
}

template <typename K, typename V>
bool TxnSilo<K, V>::TryCommit(std::atomic<uint64_t>* ser_counter,
                              uint64_t* ser_order, TxnStats* stats) {
    if (must_abort) return false;

    std::chrono::time_point<std::chrono::high_resolution_clock> start_tp;
    if constexpr (build_options.txn_stat)
        start_tp = std::chrono::high_resolution_clock::now();

    // phase 1: lock for writes
    // lock in memory address order to prevent deadlocks
    // std::map is already sorted by key
    for (auto&& [record, _] : write_set) {
        record->latch.lock();
        DEBUG("record latch W acquire %p", static_cast<void*>(record));
    }

    auto release_all_write_latches = [&]() {
        for (auto&& [record, _] : write_set) {
            record->latch.unlock();
            DEBUG("record latch W release %p", static_cast<void*>(record));
        }
    };

    std::chrono::time_point<std::chrono::high_resolution_clock> end_lock_tp;
    if constexpr (build_options.txn_stat)
        end_lock_tp = std::chrono::high_resolution_clock::now();

    // <-- serialization point -->
    if (ser_counter != nullptr && ser_order != nullptr)
        *ser_order = (*ser_counter)++;

    // phase 2
    for (auto&& ritem : read_list) {
        auto&& record = ritem.record;
        auto&& version = ritem.version;

        // if possibly locked by some writer other than me, abort
        bool latched = false;
        bool me_writing = write_set.contains(record);
        if (!me_writing) {
            latched = record->latch.try_lock_shared();
            DEBUG("record latch R try_acquire %p %s",
                  static_cast<void*>(record), latched ? "yes" : "no");
            if (!latched) {
                release_all_write_latches();
                return false;
            }
        }

        // if version mismatch, abort
        if (!me_writing && !latched) {
            record->latch.lock_shared();
            DEBUG("record latch R acquire %p", static_cast<void*>(record));
        }
        uint64_t curr_version = record->version;
        if (!me_writing) {
            record->latch.unlock_shared();
            DEBUG("record latch R release %p", static_cast<void*>(record));
        }

        if (version != curr_version) {
            release_all_write_latches();
            return false;
        }
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> end_validate_tp;
    if constexpr (build_options.txn_stat)
        end_validate_tp = std::chrono::high_resolution_clock::now();

    // generate new version number, one greater than all versions seen by
    // this transaction
    uint64_t new_version = 0;
    for (auto&& ritem : read_list)
        if (ritem.version > new_version) new_version = ritem.version;
    for (auto&& [record, _] : write_set)
        if (record->version > new_version) new_version = record->version;
    new_version++;

    // phase 3: reflect writes with new version number
    for (auto&& [record, value] : write_set) {
        record->value = std::move(value);
        record->version = new_version;
        record->valid = true;

        record->latch.unlock();
        DEBUG("record latch W release %p", static_cast<void*>(record));
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> end_commit_tp;
    if constexpr (build_options.txn_stat)
        end_commit_tp = std::chrono::high_resolution_clock::now();

    // record latency breakdown in microseconds
    if constexpr (build_options.txn_stat) {
        if (stats != nullptr) {
            stats->lock_time =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end_lock_tp - start_tp)
                    .count();
            stats->validate_time =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end_validate_tp - end_lock_tp)
                    .count();
            stats->commit_time =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end_commit_tp - end_validate_tp)
                    .count();
        }
    }

    return true;
}

}  // namespace garner
