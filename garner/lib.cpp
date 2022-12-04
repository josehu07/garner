#include "common.hpp"
#include "garner_impl.hpp"
#include "include/garner.hpp"

namespace garner {

// Called when the shared library is first loaded.
void __attribute__((constructor)) garner_ctor() {}

// Called when the shared library is unloaded.
void __attribute__((destructor)) garner_dtor() {}

Garner* Garner::Open(size_t degree) {
    GarnerImpl* impl = new GarnerImpl(degree);
    if (impl == nullptr)
        throw GarnerException("failed to allocate GarnerImpl instance");

    return impl;
}

}  // namespace garner
