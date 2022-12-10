// Template implementation included in-place by the ".hpp".

#pragma once

namespace garner {

template <typename K, typename V>
bool TxnSilo<K, V>::ExecReadRecord(Record<V>* record, V& value) {
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

    // insert into read set
    if (read_set.contains(record)) {
        if (read_set[record] != read_version) {
            // same record read multiple times by the transaction and versions
            // already mismatch
            // we could just early abort here, but for simplicity, we save
            // this decision and abort at finish time
            must_abort = true;
        }
    } else
        read_set[record] = read_version;

    return true;
}

template <typename K, typename V>
void TxnSilo<K, V>::ExecWriteRecord(Record<V>* record, V value) {
    // do not actually write; save value locally
    write_set[record] = std::move(value);
}

template <typename K, typename V>
bool TxnSilo<K, V>::TryCommit(std::atomic<uint64_t>* ser_counter,
                              uint64_t* ser_order) {
    if (must_abort) return false;

    // phase 1: lock for writes
    // sort in memory address order to prevent deadlocks
    std::vector<Record<V>*> write_vec;
    write_vec.reserve(write_set.size());

    for (auto&& [record, _] : write_set) write_vec.push_back(record);
    std::sort(write_vec.begin(), write_vec.end(), [](void* ra, void* rb) {
        return reinterpret_cast<uint64_t>(ra) < reinterpret_cast<uint64_t>(rb);
    });

    for (auto* record : write_vec) {
        record->latch.lock();
        DEBUG("record latch W acquire %p", static_cast<void*>(record));
    }

    auto release_all_write_latches = [&]() {
        for (auto* record : write_vec) {
            record->latch.unlock();
            DEBUG("record latch W release %p", static_cast<void*>(record));
        }
    };

    // <-- serialization point -->
    if (ser_counter != nullptr && ser_order != nullptr)
        *ser_order = (*ser_counter)++;

    // phase 2
    for (auto&& [record, version] : read_set) {
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

    // generate new version number, one greater than all versions seen by
    // this transaction
    uint64_t new_version = 0;
    for (auto&& [record, version] : read_set)
        if (version > new_version) new_version = version;
    for (auto&& [record, _] : write_set)
        if (record->version > new_version) new_version = record->version;
    new_version++;

    // phase 3: reflect writes with new version number
    for (auto* record : write_vec) {
        record->value = std::move(write_set[record]);
        record->version = new_version;
        record->valid = true;

        record->latch.unlock();
        DEBUG("record latch W release %p", static_cast<void*>(record));
    }

    return true;
}

}  // namespace garner
