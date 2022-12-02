#include "include/garner.hpp"

#include "common.hpp"
#include "garner_impl.hpp"

namespace garner {

Garner* Garner::Open(size_t degree) {
    GarnerImpl* impl = new GarnerImpl(degree);
    if (impl == nullptr)
        throw GarnerException("failed to allocate GarnerImpl instance");

    return static_cast<Garner*>(impl);
}

}  // namespace garner
