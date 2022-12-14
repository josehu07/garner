// Template implementation included in-place by the ".hpp".

#pragma once

namespace garner {

template <typename K>
size_t Page<K>::NumKeys() const {
    return keys.size();
}

template <typename K>
ssize_t Page<K>::SearchKey(const K& key) const {
    size_t nkeys = NumKeys();
    if (nkeys == 0) return -1;

    // binary search for key in content array
    size_t spos = 0, epos = nkeys;
    while (true) {
        size_t pos = (spos + epos) / 2;

        if (keys[pos] == key) {
            // found equality
            return pos;
        } else {
            // shrink range
            if (keys[pos] < key)
                spos = pos + 1;
            else
                epos = pos;
        }

        if (spos >= epos) break;
    }

    // no equality, return spos - 1
    assert(spos == epos);
    return static_cast<ssize_t>(spos) - 1;
}

template <typename K, typename V>
Record<K, V>* PageLeaf<K, V>::Inject(ssize_t search_idx, K key) {
    assert(this->NumKeys() < this->degree);
    assert(search_idx >= -1 &&
           search_idx < static_cast<ssize_t>(this->NumKeys()));
    assert(records.size() == this->NumKeys());

    // if key has exact match with the one on idx, simply return that record
    if (search_idx >= 0 && this->keys[search_idx] == key) {
        Record<K, V>* record = records[search_idx];
        assert(record != nullptr);
        return record;
    }

    // otherwise, shift any array content with larger key to the right, and
    // inject key and empty record
    size_t shift_idx = search_idx + 1;
    this->keys.insert(this->keys.begin() + shift_idx, key);

    Record<K, V>* record = new Record<K, V>(key);
    if (record == nullptr)
        throw GarnerException("failed to allocate new record");
    records.insert(records.begin() + shift_idx, record);
    return record;
}

template <typename K, typename V>
void PageItnl<K, V>::Inject(ssize_t search_idx, K key, Page<K>* lpage,
                            Page<K>* rpage) {
    assert(this->NumKeys() < this->degree);
    assert(search_idx >= -1 &&
           search_idx < static_cast<ssize_t>(this->NumKeys()));
    assert(children.size() == this->keys.size() + 1);

    // must not have duplicate internal node keys
    if (search_idx >= 0 && this->keys[search_idx] == key)
        throw GarnerException("duplicate internal node keys detected");

    // the page to the left of inject slot must be equal to left child
    size_t shift_idx = search_idx + 1;
    if (children[shift_idx] != lpage)
        throw GarnerException("left child page does not match");

    // shift any array content with larger key to the right, and inject key
    // and right child
    this->keys.insert(this->keys.begin() + shift_idx, key);
    children.insert(children.begin() + shift_idx + 1, rpage);
}

template <typename K, typename V>
Record<K, V>* PageRoot<K, V>::Inject(ssize_t search_idx, K key) {
    assert(this->NumKeys() < this->degree);
    assert(search_idx >= -1 &&
           search_idx < static_cast<ssize_t>(this->NumKeys()));
    assert(records.size() == this->NumKeys());
    assert(height == 1);
    assert(children.empty());

    // if key has exact match with the one on idx, simply return that record
    if (search_idx >= 0 && this->keys[search_idx] == key) {
        Record<K, V>* record = records[search_idx];
        assert(record != nullptr);
        return record;
    }

    // otherwise, shift any array content with larger key to the right, and
    // inject key and empty record
    size_t shift_idx = search_idx + 1;
    this->keys.insert(this->keys.begin() + shift_idx, key);

    Record<K, V>* record = new Record<K, V>(key);
    if (record == nullptr)
        throw GarnerException("failed to allocate new record");
    records.insert(records.begin() + shift_idx, record);
    return record;
}

template <typename K, typename V>
void PageRoot<K, V>::Inject(ssize_t search_idx, K key, Page<K>* lpage,
                            Page<K>* rpage) {
    assert(this->NumKeys() < this->degree);
    assert(search_idx >= -1 &&
           search_idx < static_cast<ssize_t>(this->NumKeys()));
    assert(children.size() == this->keys.size() + 1);
    assert(height > 1);
    assert(records.empty());

    // must not have duplicate internal node keys
    if (search_idx >= 0 && this->keys[search_idx] == key)
        throw GarnerException("duplicate internal node keys detected");

    // the page to the left of inject slot must be equal to left child
    size_t shift_idx = search_idx + 1;
    if (children[shift_idx] != lpage)
        throw GarnerException("left child page does not match");

    // shift any array content with larger key to the right, and inject key
    // and right child
    this->keys.insert(this->keys.begin() + shift_idx, key);
    children.insert(children.begin() + shift_idx + 1, rpage);
}

}  // namespace garner
