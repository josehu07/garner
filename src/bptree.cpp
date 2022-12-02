#include "bptree.hpp"

namespace bptree {

// Called when the shared library is first loaded.
void __attribute__((constructor)) foreactor_ctor() {}

// Called when the shared library is unloaded.
void __attribute__((destructor)) foreactor_dtor() {}

}  // namespace bptree
