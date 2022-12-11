// Record -- record/row struct containing value, pointed to by leaf nodes.

#include <iostream>
#include <shared_mutex>

#pragma once

namespace garner {

/**
 * Record struct containing user value. Leaf nodes of the B+-tree point to
 * such record structs.
 *
 * Before accessing the value, should have appropriate latch held.
 */
template <typename K, typename V>
struct Record {
    // read-write mutex as latch
    std::shared_mutex latch;

    // a copy of key is stored in the record
    // this field should never be modified after the creation of record, so is
    // safe for reader to access without latching
    const K key;

    // user value
    V value;

    // version number
    // effective only when concurerncy control is on
    uint64_t version = 0;

    // valid flag, set at first write
    // effective only when concurerncy control is on
    bool valid = false;

    Record() = delete;
    Record(K key) : latch(), key(key), value(), version(0), valid(false) {}

    Record(const Record&) = delete;
    Record& operator=(const Record&) = delete;

    ~Record() = default;
};

template <typename K, typename V>
std::ostream& operator<<(std::ostream& s, const Record<K, V>& record) {
    s << "Record{key=" << record.key << ",value=" << record.value << "}";
    return s;
}

}  // namespace garner
