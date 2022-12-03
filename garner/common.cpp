#include "common.hpp"

#include <syscall.h>
#include <unistd.h>

namespace garner {

std::ostream& operator<<(std::ostream& s, const BPTreeStats& stats) {
    return s << "BPTreeStats{npages=" << stats.npages
             << ",npages_itnl=" << stats.npages_itnl
             << ",npages_leaf=" << stats.npages_leaf
             << ",nkeys_itnl=" << stats.nkeys_itnl
             << ",nkeys_leaf=" << stats.nkeys_leaf << "}";
}

thread_local const pid_t tid = static_cast<pid_t>(syscall(SYS_gettid));

}  // namespace garner
