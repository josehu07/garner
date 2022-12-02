// Global consts and format definitions.

#include <iostream>
#include <stdexcept>
#include <string>

#pragma once

namespace bptree {

/** Exception type. */
class BPTreeException : public std::exception {
    std::string what_msg;

   public:
    BPTreeException(std::string&& what_msg) : what_msg(what_msg) {}
    ~BPTreeException() = default;

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

std::ostream& operator<<(std::ostream& s, const BPTreeStats& stats);

}  // namespace bptree
