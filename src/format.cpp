#include "format.hpp"

#include <iostream>

namespace bptree {

static inline std::string PageTypeStr(enum PageType type) {
    switch (type) {
        case PAGE_EMPTY:
            return "empty";
        case PAGE_ROOT:
            return "root";
        case PAGE_ITNL:
            return "itnl";
        case PAGE_LEAF:
            return "leaf";
        default:
            return "unknown";
    }
}

std::ostream& operator<<(std::ostream& s, const Page& page) {
    s << "Page{type=" << PageTypeStr(page.header.type)
      << ",nkeys=" << page.header.nkeys << ",depth/next=" << page.header.next
      << ",content=[";
    for (size_t idx = 0; idx < 1 + page.header.nkeys * 2; ++idx)
        s << page.content[idx] << ",";
    return s << "]}";
}

}  // namespace bptree
