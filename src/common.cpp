#include "common.hpp"

#include <iostream>
#include <stdexcept>
#include <string>

namespace bptree {

std::ostream& operator<<(std::ostream& s, const BPTreeStats& stats) {
    return s << "BPTreeStats{npages=" << stats.npages
             << ",npages_itnl=" << stats.npages_itnl
             << ",npages_leaf=" << stats.npages_leaf
             << ",npages_empty=" << stats.npages_empty
             << ",nkeys_itnl=" << stats.nkeys_itnl
             << ",nkeys_leaf=" << stats.nkeys_leaf << "}";
}

}  // namespace bptree
