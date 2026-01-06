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
    auto* n = static_cast<ptr_t>(ptr);
    if (builder_t::is_sentinel(n)) return;
    builder_t::delete_node(n);
}

TKTRIE_TEMPLATE
std::string_view TKTRIE_CLASS::get_skip(ptr_t n) noexcept {
    return n->skip_str();
}

TKTRIE_TEMPLATE
T* TKTRIE_CLASS::get_eos_ptr(ptr_t n) noexcept {
    if (n->is_leaf()) return nullptr;
    if (n->is_list()) [[likely]] return n->template as_list<false>()->eos_ptr;
    return n->template as_full<false>()->eos_ptr;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::set_eos_ptr(ptr_t n, T* p) noexcept {
    if (n->is_list()) [[likely]] n->template as_list<false>()->eos_ptr = p;
    else n->template as_full<false>()->eos_ptr = p;
}

// -----------------------------------------------------------------------------
// Instance helpers
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
void TKTRIE_CLASS::retire_node(ptr_t n) {
    if (!n) return;
    if (builder_t::is_sentinel(n)) return;
    if constexpr (THREADED) {
        n->poison();
        uint64_t epoch = ebr_slot::global_epoch().load(std::memory_order_acquire);
        ebr_retire(n, epoch);
        ebr_global::instance().advance_epoch();
    } else {
        node_deleter(n);
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::maybe_reclaim() noexcept {
    if constexpr (THREADED) {
        thread_local uint32_t reclaim_counter = 0;
        if ((++reclaim_counter & 0x3FF) == 0) {
            ebr_try_reclaim();
        }
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::ebr_retire(ptr_t n, uint64_t epoch) {
    if constexpr (THREADED) {
        std::lock_guard<std::mutex> lock(ebr_mutex_);
        retired_.push_back({n, epoch});
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::ebr_try_reclaim() {
    if constexpr (THREADED) {
        uint64_t safe = ebr_global::instance().compute_safe_epoch();
        
        std::vector<retired_node> still_retired;
        std::lock_guard<std::mutex> lock(ebr_mutex_);
        
        for (auto& r : retired_) {
            if (r.epoch + 2 <= safe) {
                node_deleter(r.ptr);
            } else {
                still_retired.push_back(r);
            }
        }
        retired_ = std::move(still_retired);
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::find_child(ptr_t n, unsigned char c) const noexcept {
    if (n->is_list()) [[likely]] {
        int idx = n->template as_list<false>()->chars.find(c);
        return idx >= 0 ? n->template as_list<false>()->children[idx].load() : nullptr;
    }
    if (n->is_full()) {
        return n->template as_full<false>()->children[c].load();
    }
    return nullptr;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::atomic_ptr* TKTRIE_CLASS::get_child_slot(ptr_t n, unsigned char c) noexcept {
    if (n->is_list()) [[likely]] {
        int idx = n->template as_list<false>()->chars.find(c);
        return idx >= 0 ? &n->template as_list<false>()->children[idx] : nullptr;
    }
    if (n->is_full() && n->template as_full<false>()->valid.template atomic_test<THREADED>(c)) {
        return &n->template as_full<false>()->children[c];
    }
    return nullptr;
}

// -----------------------------------------------------------------------------
// Read operations
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_impl(ptr_t n, std::string_view key, T& out) const noexcept {
    if (!n) return false;
    
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
    
    return read_from_leaf(n, key, out);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_from_leaf(ptr_t leaf, std::string_view key, T& out) const noexcept {
    if constexpr (THREADED) {
        if (leaf->is_poisoned()) return false;
    }
    
    std::string_view skip = get_skip(leaf);
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) return false;
    key.remove_prefix(m);

    if (leaf->is_skip()) {
        if (!key.empty()) return false;
        out = leaf->as_skip()->leaf_value;
        return true;
    }
    
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (leaf->is_list()) [[likely]] {
        int idx = leaf->template as_list<true>()->chars.find(c);
        if (idx < 0) return false;
        out = leaf->template as_list<true>()->leaf_values[idx];
        return true;
    }
    if (!leaf->template as_full<true>()->valid.template atomic_test<THREADED>(c)) return false;
    out = leaf->template as_full<true>()->leaf_values[c];
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_impl_optimistic(ptr_t n, std::string_view key, T& out, read_path& path) const noexcept {
    if (!n || n->is_poisoned()) return false;
    
    if (!path.push(n)) return false;
    
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
        if (!n || n->is_poisoned()) return false;
        
        if (!path.push(n)) return false;
    }
    
    return read_from_leaf(n, key, out);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::validate_read_path(const read_path& path) const noexcept {
    for (int i = 0; i < path.len; ++i) {
        if (path.nodes[i]->is_poisoned()) return false;
        if (path.nodes[i]->version() != path.versions[i]) return false;
    }
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
TKTRIE_CLASS::tktrie() : root_(nullptr) {}

TKTRIE_TEMPLATE
TKTRIE_CLASS::~tktrie() { clear(); }

TKTRIE_TEMPLATE
TKTRIE_CLASS::tktrie(const tktrie& other) : root_(nullptr) {
    ptr_t other_root = other.root_.load();
    if (other_root && !builder_t::is_sentinel(other_root)) {
        root_.store(builder_.deep_copy(other_root));
    }
    size_.store(other.size_.load());
}

TKTRIE_TEMPLATE
TKTRIE_CLASS& TKTRIE_CLASS::operator=(const tktrie& other) {
    if (this != &other) {
        clear();
        ptr_t other_root = other.root_.load();
        if (other_root && !builder_t::is_sentinel(other_root)) {
            root_.store(builder_.deep_copy(other_root));
        }
        size_.store(other.size_.load());
    }
    return *this;
}

TKTRIE_TEMPLATE
TKTRIE_CLASS::tktrie(tktrie&& other) noexcept : root_(nullptr) {
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
    if (r && !builder_t::is_sentinel(r)) {
        builder_.dealloc_node(r);
    }
    size_.store(0);
    if constexpr (THREADED) {
        std::lock_guard<std::mutex> lock(ebr_mutex_);
        for (auto& rn : retired_) {
            node_deleter(rn.ptr);
        }
        retired_.clear();
    }
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::contains(const Key& key) const {
    auto kb = traits::to_bytes(key);
    if constexpr (THREADED) {
        auto& slot = get_ebr_slot();
        auto guard = slot.get_guard();
        
        for (int attempts = 0; attempts < 10; ++attempts) {
            T dummy;
            read_path path;
            
            ptr_t root = root_.load();
            if (!root) return false;
            if (root->is_poisoned()) continue;
            
            bool found = read_impl_optimistic(root, kb, dummy, path);
            if (validate_read_path(path)) return found;
        }
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
        
        for (int attempts = 0; attempts < 10; ++attempts) {
            read_path path;
            
            ptr_t root = root_.load();
            if (!root) return end();
            if (root->is_poisoned()) continue;
            
            bool found = read_impl_optimistic(root, kb, value, path);
            if (validate_read_path(path)) {
                if (found) return iterator(this, std::string(kb), value);
                return end();
            }
        }
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
        std::lock_guard<std::mutex> lock(ebr_mutex_);
        for (auto& rn : retired_) {
            node_deleter(rn.ptr);
        }
        retired_.clear();
    }
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_insert.h"
