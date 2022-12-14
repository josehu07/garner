#include "common.hpp"
#include "garner_impl.hpp"
#include "include/garner.hpp"

namespace garner {

Garner* Garner::Open(size_t degree, TxnProtocol protocol) {
    GarnerImpl* impl = new GarnerImpl(degree, protocol);
    if (impl == nullptr)
        throw GarnerException("failed to allocate GarnerImpl instance");

    return impl;
}

}  // namespace garner
