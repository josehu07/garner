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
template <typename V>
struct Record {
    // read-write mutex as latch
    std::shared_mutex latch;

    // user value
    V value;

    // version number
    // effective only when concurerncy control is on
    uint64_t version = 0;

    // valid flag, set at first write
    // effective only when concurerncy control is on
    bool valid = false;

    Record() : latch(), value(), version(0), valid(false) {}

    Record(const Record&) = delete;
    Record& operator=(const Record&) = delete;

    ~Record() = default;
};

template <typename V>
std::ostream& operator<<(std::ostream& s, const Record<V>& record) {
    s << "Record{value=" << record.value << "}";
    return s;
}

}  // namespace garner
