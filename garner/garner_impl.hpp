// GarnerImpl -- internal implementation of Garner DB interface struct.

#include <string>
#include <vector>

#include "bptree.hpp"
#include "common.hpp"
#include "include/garner.hpp"
#include "page.hpp"

#pragma once

namespace garner {

/**
 * Implementation of Garner DB interface.
 */
class GarnerImpl : public Garner {
   private:
    BPTree<KType, VType>* bptree;

   public:
    GarnerImpl(size_t degree);

    GarnerImpl(const GarnerImpl&) = delete;
    GarnerImpl& operator=(const GarnerImpl&) = delete;

    ~GarnerImpl();

    void Put(KType key, VType value) override;
    bool Get(const KType& key, VType& value) override;
    bool Delete(const KType& key) override;
    size_t Scan(const KType& lkey, const KType& rkey,
                std::vector<std::tuple<KType, VType>>& results) override;
    void PrintStats(bool print_pages = false) override;
};

}  // namespace garner

// Include template implementation in-place.
#include "garner_impl.tpl.hpp"
