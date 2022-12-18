// Template implementation included in-place by the ".hpp".

#pragma once

namespace garner {

template <typename K, typename V>
bool TxnSiloHV<K, V>::ExecReadRecord(Record<K, V>* record, V& value) {
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
    if (write_set.contains(record)) {
        assert(write_list[write_set[record]].is_record);
        value = std::get<V>(write_list[write_set[record]].height_or_value);
    } else
        value = std::move(read_value);

    // insert into read set if not in it yet
    if (read_set.contains(record)) {
        assert(read_list[read_set[record]].is_record);
        if (read_list[read_set[record]].version != read_version) {
            // same record read multiple times by the transaction and versions
            // already mismatch
            // we could just early abort here, but for simplicity, we save
            // this decision and abort at finish time
            must_abort = true;
        }
    } else {
        read_list.push_back(ReadListItem{.is_record = true,
                                         .record = record,
                                         .version = read_version,
                                         .skip_to = 0});
        read_set[record] = read_list.size() - 1;
    }

    return true;
}

template <typename K, typename V>
void TxnSiloHV<K, V>::ExecWriteRecord(Record<K, V>* record, V value) {
    // do not actually write; save value locally
    if (write_set.contains(record)) {
        assert(write_list[write_set[record]].is_record);
        write_list[write_set[record]].height_or_value = std::move(value);
    } else {
        write_list.push_back(
            WriteListItem{.is_record = true,
                          .record = record,
                          .height_or_value = std::move(value)});
        write_set[record] = write_list.size() - 1;
    }
}

template <typename K, typename V>
void TxnSiloHV<K, V>::ExecReadTraverseNode(Page<K>* page) {
    // TODO: only doing skipping for Scans for now
    if (in_scan) {
        // if there is a node item at the same height, set its skip_to
        // TODO: reading root page's height may not be thread-safe
        unsigned height = page->height;
        if (last_read_node.contains(height)) {
            read_list[last_read_node[height]].skip_to = read_list.size();
            last_read_node.erase(height);
        }

        // append to read list if not already in the list pushed by a previous
        // operation in the same transaction
        if (!read_set.contains(page)) {
            read_list.push_back(ReadListItem{.is_record = false,
                                             .page = page,
                                             .version = page->hv_ver,
                                             .skip_to = 0});
            read_set[page] = read_list.size() - 1;
            last_read_node[height] = read_list.size() - 1;
        }
    }
}

template <typename K, typename V>
void TxnSiloHV<K, V>::ExecWriteTraverseNode(Page<K>* page, unsigned height) {
    // append to write list if not already in the list pushed by a previous
    // operation in the same transaction
    if (!write_set.contains(page)) {
        write_list.push_back(WriteListItem{
            .is_record = false, .page = page, .height_or_value = height});
        write_set[page] = write_list.size() - 1;
    }
}

template <typename K, typename V>
void TxnSiloHV<K, V>::ExecEnterScan() {
    in_scan = true;
    assert(last_read_node.empty());
}

template <typename K, typename V>
void TxnSiloHV<K, V>::ExecLeaveScan() {
    in_scan = false;
    // set dangling node items' skip_to
    for (auto&& [_, idx] : last_read_node)
        read_list[idx].skip_to = read_list.size();
    last_read_node.clear();
}

template <typename K, typename V>
bool TxnSiloHV<K, V>::TryCommit(std::atomic<uint64_t>* ser_counter,
                                uint64_t* ser_order, TxnStats* stats) {
    if (must_abort) return false;

    std::chrono::time_point<std::chrono::high_resolution_clock> start_tp;
    if constexpr (build_options.txn_stat)
        start_tp = std::chrono::high_resolution_clock::now();

    // phase 1: lock for writes
    // sort records in the following order to prevent deadlocks:
    // - tree node < record
    // - if both are tree nodes, larger height < smaller height,
    //   then compare memory address if same height
    // - if both are records, compare memory address
    std::sort(write_list.begin(), write_list.end(),
              [](const WriteListItem& wa, const WriteListItem& wb) {
                  if (!wa.is_record && wb.is_record)
                      return true;
                  else if (wa.is_record && !wb.is_record)
                      return false;
                  else if (wa.is_record && wb.is_record) {
                      return reinterpret_cast<uint64_t>(wa.record) <
                             reinterpret_cast<uint64_t>(wb.record);
                  } else {
                      if (wa.height_or_value > wb.height_or_value)
                          return true;
                      else if (wa.height_or_value < wb.height_or_value)
                          return false;
                      else
                          return reinterpret_cast<uint64_t>(wa.record) <
                                 reinterpret_cast<uint64_t>(wb.record);
                  }
              });

    for (auto&& witem : write_list) {
        if (witem.is_record) {
            witem.record->latch.lock();
            DEBUG("record latch W acquire %p",
                  static_cast<void*>(witem.record));
        } else {
            ++witem.page->hv_sem;
            DEBUG("page hv_sem increment %p", static_cast<void*>(witem.page));
        }
    }

    auto release_all_write_latches = [&]() {
        for (auto&& witem : write_list) {
            if (witem.is_record) {
                witem.record->latch.unlock();
                DEBUG("record latch W release %p",
                      static_cast<void*>(witem.record));
            } else {
                --witem.page->hv_sem;
                DEBUG("page hv_sem decrement %p",
                      static_cast<void*>(witem.page));
            }
        }
    };

    std::chrono::time_point<std::chrono::high_resolution_clock> end_lock_tp;
    if constexpr (build_options.txn_stat)
        end_lock_tp = std::chrono::high_resolution_clock::now();

    // <-- serialization point -->
    if (ser_counter != nullptr && ser_order != nullptr)
        *ser_order = (*ser_counter)++;

    // phase 2
    size_t idx = 0;
    while (!no_read_validation && idx < read_list.size()) {
        auto&& ritem = read_list[idx];

        if (ritem.is_record) {
            // if possibly locked by some writer other than me, abort
            bool latched = false;
            bool me_writing = write_set.contains(ritem.record);
            if (!me_writing) {
                latched = ritem.record->latch.try_lock_shared();
                DEBUG("record latch R try_acquire %p %s",
                      static_cast<void*>(ritem.record), latched ? "yes" : "no");
                if (!latched) {
                    release_all_write_latches();
                    return false;
                }
            }

            // if version mismatch, abort
            if (!me_writing && !latched) {
                ritem.record->latch.lock_shared();
                DEBUG("record latch R acquire %p",
                      static_cast<void*>(ritem.record));
            }
            uint64_t curr_version = ritem.record->version;
            if (!me_writing) {
                ritem.record->latch.unlock_shared();
                DEBUG("record latch R release %p",
                      static_cast<void*>(ritem.record));
            }

            if (ritem.version != curr_version) {
                release_all_write_latches();
                return false;
            }

            idx++;

        } else {
            // check semaphore field of tree page
            uint64_t hv_sem = ritem.page->hv_sem;
            if (hv_sem > 1 ||
                (hv_sem == 1 && !write_set.contains(ritem.page))) {
                idx++;
                continue;
            }

            // check if tree page version changed by any committed writer
            if (ritem.version != ritem.page->hv_ver) {
                idx++;
                continue;
            }

            // everything under this subtree have not been modified, skip
            // children nodes
            if (ritem.skip_to > 0) {
                assert(ritem.skip_to > idx);
                idx = ritem.skip_to;
            } else
                idx++;
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
    for (auto&& witem : write_list) {
        if (witem.is_record) {
            if (witem.record->version > new_version)
                new_version = witem.record->version;
        } else {
            uint64_t hv_ver = witem.page->hv_ver;
            if (hv_ver > new_version) new_version = hv_ver;
        }
    }
    new_version++;

    // phase 3: reflect writes with new version number
    for (auto&& witem : write_list) {
        if (witem.is_record) {
            witem.record->value = std::move(std::get<V>(witem.height_or_value));
            witem.record->version = new_version;
            witem.record->valid = true;

            witem.record->latch.unlock();
            DEBUG("record latch W release %p",
                  static_cast<void*>(witem.record));
        } else {
            witem.page->hv_ver = new_version;
            --witem.page->hv_sem;
            DEBUG("page hv_sem decrement %p", static_cast<void*>(witem.page));
        }
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
