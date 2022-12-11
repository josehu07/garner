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
    auto wit = std::find_if(write_list.begin(), write_list.end(),
                            [&](const WriteListItem& w) {
                                return w.is_record && w.record == record;
                            });
    if (wit == write_list.end() && !valid) return false;

    // if in my local write set, read from there instead
    if (wit != write_list.end())
        value = wit->value;
    else
        value = std::move(read_value);

    // insert into read set
    auto rit = std::find_if(read_list.begin(), read_list.end(),
                            [&](const ReadListItem& r) {
                                return r.is_record && r.record == record;
                            });
    if (rit != read_list.end()) {
        if (rit->version != read_version) {
            // same record read multiple times by the transaction and versions
            // already mismatch
            // we could just early abort here, but for simplicity, we save
            // this decision and abort at finish time
            must_abort = true;
        }
    } else {
        read_list.push_back(ReadListItem{
            .is_record = true, .record = record, .version = read_version});
    }

    return true;
}

template <typename K, typename V>
void TxnSiloHV<K, V>::ExecWriteRecord(Record<K, V>* record, V value) {
    // do not actually write; save value locally
    auto wit = std::find_if(write_list.begin(), write_list.end(),
                            [&](const WriteListItem& w) {
                                return w.is_record && w.record == record;
                            });
    if (wit != write_list.end())
        wit->value = std::move(value);
    else
        write_list.push_back(WriteListItem{
            .is_record = true, .record = record, .value = std::move(value)});
}

template <typename K, typename V>
void TxnSiloHV<K, V>::ExecReadTraverseNode(Page<K>* page) {
    // append to read list
    assert(!std::any_of(
        read_list.begin(), read_list.end(),
        [&](const ReadListItem& r) { return !r.is_record && r.page == page; }));
    read_list.push_back(ReadListItem{
        .is_record = false, .page = page, .version = page->hv_ver});
}

template <typename K, typename V>
void TxnSiloHV<K, V>::ExecWriteTraverseNode(Page<K>* page) {
    // append to write list
    assert(!std::any_of(write_list.begin(), write_list.end(),
                        [&](const WriteListItem& w) {
                            return !w.is_record && w.page == page;
                        }));
    write_list.push_back(
        WriteListItem{.is_record = false, .page = page, .value = V()});
}

template <typename K, typename V>
bool TxnSiloHV<K, V>::TryCommit(std::atomic<uint64_t>* ser_counter,
                                uint64_t* ser_order) {
    if (must_abort) return false;

    // phase 1: lock for writes
    // sort record chunks in memory address order to prevent deadlocks
    auto witb = write_list.begin(), wite = write_list.begin();
    while (witb != write_list.end()) {
        // find the next contiguous chunk of records in list
        while (witb != write_list.end() && !witb->is_record) ++witb;
        if (witb == write_list.end()) break;
        wite = witb;
        while (wite != write_list.end() && wite->is_record) ++wite;
        // sort the chunk
        if (wite - witb > 1) {
            std::sort(witb, wite,
                      [](const WriteListItem& wa, const WriteListItem& wb) {
                          return reinterpret_cast<uint64_t>(wa.record) <
                                 reinterpret_cast<uint64_t>(wb.record);
                      });
        }
        witb = wite;
    }

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

    // <-- serialization point -->
    if (ser_counter != nullptr && ser_order != nullptr)
        *ser_order = (*ser_counter)++;

    // phase 2
    //
    // TODO: this protocol currently does not work with on-the-fly insertions!
    // TODO: do better than comparing keys in skipping children nodes.
    K skip_until;
    bool skipping = false;

    for (auto&& ritem : read_list) {
        if (ritem.is_record) {
            // if during skipping, check against skip_until key
            if (skipping) {
                if (ritem.record->key <= skip_until)
                    continue;
                else
                    skipping = false;
            }

            // if possibly locked by some writer other than me, abort
            bool latched = false;
            bool me_writing =
                std::any_of(write_list.begin(), write_list.end(),
                            [&](const WriteListItem& w) {
                                return w.is_record && w.record == ritem.record;
                            });
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

        } else {
            // if during skipping, check against skip_until key
            if (skipping) {
                if (ritem.page->keys.back() <= skip_until)
                    continue;
                else
                    skipping = false;
            }

            // check semaphore field of tree page
            uint64_t hv_sem = ritem.page->hv_sem;
            if (hv_sem > 1 || (hv_sem == 1 &&
                               std::any_of(write_list.begin(), write_list.end(),
                                           [&](const WriteListItem& w) {
                                               return !w.is_record &&
                                                      w.page == ritem.page;
                                           }))) {
                continue;
            }

            // check if tree page version changed by any committed writer
            if (ritem.version != ritem.page->hv_ver) continue;

            // everything under this subtree have not been modified, skip
            // children nodes
            skip_until = ritem.page->keys.back();
            skipping = true;
        }
    }

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
            witem.record->value = std::move(witem.value);
            witem.record->version = new_version;
            witem.record->valid = true;

            witem.record->latch.unlock();
            DEBUG("record latch W release %p",
                  static_cast<void*>(witem.record));
        } else {
            witem.page->hv_ver = new_version;
            --witem.page->hv_sem;
        }
    }

    return true;
}

}  // namespace garner
