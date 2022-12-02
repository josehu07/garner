#include "include/garner.hpp"

#include <iostream>
#include <string>

#include "common.hpp"
#include "garner_impl.hpp"

namespace garner {

Garner* Garner::Open(const std::string& bptree_backfile, size_t degree) {
    GarnerImpl* impl = new GarnerImpl(bptree_backfile, degree);
    if (impl == nullptr)
        throw GarnerException("failed to allocate GarnerImpl instance");

    return static_cast<Garner*>(impl);
}

}  // namespace garner
