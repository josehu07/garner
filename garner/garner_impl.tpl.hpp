// Template implementation included in-place by the ".hpp".

#pragma once

namespace garner {

GarnerImpl::GarnerImpl(size_t degree) {
    bptree = new BPTree<KType, VType>(degree);
    if (bptree == nullptr)
        throw GarnerException("failed to allocate BPtree instance");
}

GarnerImpl::~GarnerImpl() { delete bptree; }

void GarnerImpl::Put(KType key, VType value) {
    // TODO: finish me transactionally
    bptree->Put(key, value);
}

bool GarnerImpl::Get(KType key, VType& value) {
    // TODO: finish me transactionally
    return bptree->Get(key, value);
}

bool GarnerImpl::Delete(KType key) {
    // TODO: finish me transactionally
    return bptree->Delete(key);
}

size_t GarnerImpl::Scan(KType lkey, KType rkey,
                        std::vector<std::tuple<KType, VType>>& results) {
    // TODO: finish me transactionally
    return bptree->Scan(lkey, rkey, results);
}

void GarnerImpl::PrintStats(bool print_pages) {
    bptree->PrintStats(print_pages);
}

}  // namespace garner
