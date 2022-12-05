// GarnerImpl -- internal implementation of Garner DB interface struct.

#include <string>
#include <vector>

#include "bptree.hpp"
#include "common.hpp"
#include "include/garner.hpp"
#include "page.hpp"
#include "txn.hpp"
#include "txn_silo.hpp"

#pragma once

namespace garner {

/**
 * Implementation of Garner DB interface.
 */
class GarnerImpl : public Garner {
   private:
    // B+-tree index data structure.
    BPTree<KType, VType>* bptree;

    // type of transaction OCC protocol to use
    TxnProtocol protocol;

   public:
    GarnerImpl(size_t degree, TxnProtocol protocol);

    GarnerImpl(const GarnerImpl&) = delete;
    GarnerImpl& operator=(const GarnerImpl&) = delete;

    ~GarnerImpl();

    TxnCxt<VType>* StartTxn() override;
    bool FinishTxn(TxnCxt<VType>* txn) override;

    bool Put(KType key, VType value, TxnCxt<VType>* txn = nullptr) override;
    bool Get(const KType& key, VType& value, bool& found,
             TxnCxt<VType>* txn = nullptr) override;
    bool Delete(const KType& key, bool& found,
                TxnCxt<VType>* txn = nullptr) override;
    bool Scan(const KType& lkey, const KType& rkey,
              std::vector<std::tuple<KType, VType>>& results, size_t& nrecords,
              TxnCxt<VType>* txn = nullptr) override;

    void PrintStats(bool print_pages = false) override;
};

}  // namespace garner

// Include template implementation in-place.
#include "garner_impl.tpl.hpp"
