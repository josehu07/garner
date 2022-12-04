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

    Record() : latch(), value() {}
    Record(V value) : latch(), value(value) {}

    Record(const Record&) = delete;
    Record& operator=(const Record&) = delete;

    ~Record() = default;
};

template <typename K>
std::ostream& operator<<(std::ostream& s, const Record<K>& record) {
    s << "Record{value=" << record.value << "}";
    return s;
}

}  // namespace garner

// Include template implementation in-place.
#include "record.tpl.hpp"
