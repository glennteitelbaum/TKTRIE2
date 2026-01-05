#pragma once

// This file contains implementation details for tktrie
// It should only be included from tktrie.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

// -----------------------------------------------------------------------------
// Static helpers
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
void TKTRIE_CLASS::node_deleter(void* ptr) {
    if (!ptr) return;
    builder_t::delete_node(static_cast<ptr_t>(ptr));
}

TKTRIE_TEMPLATE
std::string_view TKTRIE_CLASS::get_skip(ptr_t n) noexcept {
    if (n->type() == TYPE_EOS) return {};
    const std::string* skip_ptr = reinterpret_cast<const std::string*>(
        reinterpret_cast<const char*>(n) + NODE_SKIP_OFFSET);
    return *skip_ptr;
}

TKTRIE_TEMPLATE
T* TKTRIE_CLASS::get_eos_ptr(ptr_t n) noexcept {
    if (n->is_leaf()) return nullptr;
    auto* ptr_loc = reinterpret_cast<atomic_storage<T*, THREADED>*>(
        reinterpret_cast<char*>(n) + NODE_EOS_OFFSET);
    return ptr_loc->load();
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::set_eos_ptr(ptr_t n, T* p) noexcept {
    auto* ptr_loc = reinterpret_cast<atomic_storage<T*, THREADED>*>(
        reinterpret_cast<char*>(n) + NODE_EOS_OFFSET);
    ptr_loc->store(p);
}

// -----------------------------------------------------------------------------
// Instance helpers
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
void TKTRIE_CLASS::retire_node(ptr_t n) {
    if (!n) return;
    if constexpr (THREADED) {
        auto& ebr = ebr_global::instance();
        ebr.retire(n, node_deleter);
        ebr.advance_epoch();
    } else {
        node_deleter(n);
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::find_child(ptr_t n, unsigned char c) const noexcept {
    if (n->is_list()) {
        int idx = n->as_list()->chars.find(c);
        return idx >= 0 ? n->as_list()->children[idx].load() : nullptr;
    }
    if (n->is_full() && n->as_full()->valid.test(c)) {
        return n->as_full()->children[c].load();
    }
    return nullptr;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::atomic_ptr* TKTRIE_CLASS::get_child_slot(ptr_t n, unsigned char c) noexcept {
    if (n->is_list()) {
        int idx = n->as_list()->chars.find(c);
        return idx >= 0 ? &n->as_list()->children[idx] : nullptr;
    }
    if (n->is_full() && n->as_full()->valid.test(c)) {
        return &n->as_full()->children[c];
    }
    return nullptr;
}

// -----------------------------------------------------------------------------
// Read operations
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_impl(ptr_t n, std::string_view key, T& out) const noexcept {
    if (!n) return false;
    
    // Loop only on interior nodes
    while (!n->is_leaf()) {
        std::string_view skip = get_skip(n);
        size_t m = match_skip_impl(skip, key);
        if (m < skip.size()) return false;
        key.remove_prefix(m);

        if (key.empty()) {
            T* p = get_eos_ptr(n);
            if (p) { out = *p; return true; }
            return false;
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);

        n = find_child(n, c);
        if (!n) return false;
    }
    
    // n is now a leaf
    return read_from_leaf(n, key, out);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_from_leaf(ptr_t leaf, std::string_view key, T& out) const noexcept {
    std::string_view skip = get_skip(leaf);
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) return false;
    key.remove_prefix(m);

    if (leaf->is_eos() | leaf->is_skip()) {
        if (!key.empty()) return false;
        out = leaf->is_eos() ? leaf->as_eos()->leaf_value : leaf->as_skip()->leaf_value;
        return true;
    }
    if (key.size() != 1) return false;

    unsigned char c = static_cast<unsigned char>(key[0]);
    if (leaf->is_list()) {
        int idx = leaf->as_list()->chars.find(c);
        if (idx < 0) return false;
        out = leaf->as_list()->leaf_values[idx];
        return true;
    }
    // FULL
    if (!leaf->as_full()->valid.test(c)) return false;
    out = leaf->as_full()->leaf_values[c];
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::contains_impl(ptr_t n, std::string_view key) const noexcept {
    T dummy;
    return read_impl(n, key, dummy);
}

// -----------------------------------------------------------------------------
// Public interface
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
TKTRIE_CLASS::~tktrie() { clear(); }

TKTRIE_TEMPLATE
TKTRIE_CLASS::tktrie(const tktrie& other) {
    ptr_t other_root = other.root_.load();
    if (other_root) root_.store(builder_.deep_copy(other_root));
    size_.store(other.size_.load());
}

TKTRIE_TEMPLATE
TKTRIE_CLASS& TKTRIE_CLASS::operator=(const tktrie& other) {
    if (this != &other) {
        clear();
        ptr_t other_root = other.root_.load();
        if (other_root) root_.store(builder_.deep_copy(other_root));
        size_.store(other.size_.load());
    }
    return *this;
}

TKTRIE_TEMPLATE
TKTRIE_CLASS::tktrie(tktrie&& other) noexcept {
    root_.store(other.root_.load());
    other.root_.store(nullptr);
    size_.store(other.size_.exchange(0));
}

TKTRIE_TEMPLATE
TKTRIE_CLASS& TKTRIE_CLASS::operator=(tktrie&& other) noexcept {
    if (this != &other) {
        clear();
        root_.store(other.root_.load());
        other.root_.store(nullptr);
        size_.store(other.size_.exchange(0));
    }
    return *this;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::clear() {
    ptr_t r = root_.load();
    root_.store(nullptr);
    if (r) builder_.dealloc_node(r);
    size_.store(0);
    if constexpr (THREADED) {
        ebr_global::instance().try_reclaim();
    }
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::contains(const Key& key) const {
    auto kb = traits::to_bytes(key);
    if constexpr (THREADED) {
        auto& slot = get_ebr_slot();
        auto guard = slot.get_guard();
        return contains_impl(root_.load(), kb);
    } else {
        return contains_impl(root_.load(), kb);
    }
}

TKTRIE_TEMPLATE
std::pair<typename TKTRIE_CLASS::iterator, bool> TKTRIE_CLASS::insert(const std::pair<const Key, T>& kv) {
    auto kb = traits::to_bytes(kv.first);
    return insert_locked(kv.first, kb, kv.second);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::erase(const Key& key) {
    auto kb = traits::to_bytes(key);
    return erase_locked(kb);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::iterator TKTRIE_CLASS::find(const Key& key) const {
    auto kb = traits::to_bytes(key);
    T value;
    if constexpr (THREADED) {
        auto& slot = get_ebr_slot();
        auto guard = slot.get_guard();
        if (read_impl(root_.load(), kb, value)) {
            return iterator(this, std::string(kb), value);
        }
    } else {
        if (read_impl(root_.load(), kb, value)) {
            return iterator(this, std::string(kb), value);
        }
    }
    return end();
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::reclaim_retired() noexcept {
    if constexpr (THREADED) {
        ebr_global::instance().force_reclaim_all();
    }
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_insert.h"
