// Template implementation included in-place by the ".hpp".

#pragma once

namespace garner {

template <typename V>
void TxnSilo<V>::ExecReadRecord(Record<V>* record, V& value) {
    // fetch value and version
    record->latch.lock_shared();
    V read_value = record->value;
    uint64_t read_version = record->version;
    record->latch.unlock_shared();

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
}

template <typename V>
void TxnSilo<V>::ExecWriteRecord(Record<V>* record, V value) {
    // do not actually write; save value locally
    write_set[record] = value;
}

template <typename V>
bool TxnSilo<V>::TryCommit() {
    if (must_abort) return false;

    // phase 1: lock for writes
    // std::map is sorted by key, so we are iterating in memory address order,
    // which provides deadlock prevention
    for (auto&& [record, _] : write_set) record->latch.lock();

    // <-- serialization point -->

    // phase 2
    for (auto&& [record, version] : read_set) {
        bool latched = record->latch.try_lock_shared();

        // if possibly locked by some writer other than me, abort
        if (!write_set.contains(record) && !latched) return false;

        // if version mismatch, abort
        if (!latched) record->latch.lock_shared();
        uint64_t curr_version = record->version;
        if (version != curr_version) return false;

        record->latch.unlock_shared();
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
    for (auto&& [record, value] : write_set) {
        record->value = std::move(value);
        record->version = new_version;

        record->latch.unlock();
    }

    return true;
}

}  // namespace garner
