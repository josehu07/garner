// Template implementation included in-place by the ".hpp".

#pragma once

namespace garner {

GarnerImpl::GarnerImpl(size_t degree, TxnProtocol protocol)
    : protocol(protocol) {
    bptree = new BPTree<KType, VType>(degree);
    if (bptree == nullptr)
        throw GarnerException("failed to allocate BPtree instance");
}

GarnerImpl::~GarnerImpl() { delete bptree; }

TxnCxt<Garner::VType>* GarnerImpl::StartTxn() {
    // allocate new TxnCxt struct
    TxnCxt<VType>* txn = nullptr;
    switch (protocol) {
        case PROTOCOL_NONE:
            return nullptr;
        case PROTOCOL_SILO:
            txn = new TxnSilo<VType>();
            break;
        default:
            throw GarnerException("unknown transaction protocol type");
    }

    if (txn == nullptr)
        throw GarnerException("failed to allocate transaction context");
    DEBUG("txn %p starts", static_cast<void*>(txn));
    return txn;
}

bool GarnerImpl::FinishTxn(TxnCxt<VType>* txn) {
    DEBUG("txn %p finishing", static_cast<void*>(txn));
    bool committed = false;
    if (txn != nullptr) {
        committed = txn->TryCommit();
        delete txn;  // deallocate at finish
    }
    return committed;
}

bool GarnerImpl::Put(KType key, VType value, TxnCxt<VType>* txn) {
    TxnCxt<VType>* this_txn = txn;
    if (txn == nullptr) this_txn = StartTxn();

    bptree->Put(key, value, this_txn);

    if (txn != nullptr)
        return false;
    else
        return FinishTxn(this_txn);
}

bool GarnerImpl::Get(const KType& key, VType& value, bool& found,
                     TxnCxt<VType>* txn) {
    TxnCxt<VType>* this_txn = txn;
    if (txn == nullptr) this_txn = StartTxn();

    found = bptree->Get(key, value, this_txn);

    if (txn != nullptr)
        return false;
    else
        return FinishTxn(this_txn);
}

bool GarnerImpl::Delete(const KType& key, bool& found, TxnCxt<VType>* txn) {
    TxnCxt<VType>* this_txn = txn;
    if (txn == nullptr) this_txn = StartTxn();

    found = bptree->Delete(key, this_txn);

    if (txn != nullptr)
        return false;
    else
        return FinishTxn(this_txn);
}

bool GarnerImpl::Scan(const KType& lkey, const KType& rkey,
                      std::vector<std::tuple<KType, VType>>& results,
                      size_t& nrecords, TxnCxt<VType>* txn) {
    TxnCxt<VType>* this_txn = txn;
    if (txn == nullptr) this_txn = StartTxn();

    nrecords = bptree->Scan(lkey, rkey, results, this_txn);

    if (txn != nullptr)
        return false;
    else
        return FinishTxn(this_txn);
}

void GarnerImpl::PrintStats(bool print_pages) {
    bptree->PrintStats(print_pages);
}

}  // namespace garner
