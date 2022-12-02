// Common helper classes and functions.

#include <iostream>
#include <stdexcept>
#include <string>

#pragma once

namespace garner {

/** Exception type. */
class GarnerException : public std::exception {
    std::string what_msg;

   public:
    GarnerException(std::string&& what_msg)
        : what_msg("GarnerException: " + what_msg) {}
    ~GarnerException() = default;

    const char* what() const noexcept override { return what_msg.c_str(); }
};

/** Statistics buffer. */
struct BPTreeStats {
    size_t npages;
    size_t npages_itnl;
    size_t npages_leaf;
    size_t npages_empty;
    size_t nkeys_itnl;
    size_t nkeys_leaf;
};

std::ostream& operator<<(std::ostream& s, const BPTreeStats& stats) {
    return s << "BPTreeStats{npages=" << stats.npages
             << ",npages_itnl=" << stats.npages_itnl
             << ",npages_leaf=" << stats.npages_leaf
             << ",npages_empty=" << stats.npages_empty
             << ",nkeys_itnl=" << stats.nkeys_itnl
             << ",nkeys_leaf=" << stats.nkeys_leaf << "}";
}

}  // namespace garner
