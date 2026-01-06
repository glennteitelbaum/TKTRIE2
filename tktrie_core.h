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

// -----------------------------------------------------------------------------
// Instance helpers
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
void TKTRIE_CLASS::retire_node(ptr_t n) {
    if (!n || builder_t::is_sentinel(n)) return;
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

// -----------------------------------------------------------------------------
// Read operations
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_impl(ptr_t n, std::string_view key, T& out) const noexcept {
    if (!n) return false;
    
    while (!n->is_leaf()) {
        std::string_view skip = n->skip_str();
        size_t m = match_skip_impl(skip, key);
        if (m < skip.size()) return false;
        key.remove_prefix(m);

        if (key.empty()) {
            return n->try_read_eos(false, out);
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);

        n = n->get_child(false, c);
        if (!n || builder_t::is_sentinel(n)) return false;
    }
    
    return read_from_leaf(n, key, out);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_from_leaf(ptr_t leaf, std::string_view key, T& out) const noexcept {
    if constexpr (THREADED) {
        if (leaf->is_poisoned()) return false;
    }
    
    std::string_view skip = leaf->skip_str();
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) return false;
    key.remove_prefix(m);

    if (leaf->is_skip()) {
        if (!key.empty()) return false;
        return leaf->as_skip()->value.try_read(out);
    }
    
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    return leaf->try_read_value(true, c, out);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_impl_optimistic(ptr_t n, std::string_view key, T& out, read_path& path) const noexcept {
    if (!n || n->is_poisoned()) return false;
    
    if (!path.push(n)) return false;
    
    while (!n->is_leaf()) {
        std::string_view skip = n->skip_str();
        size_t m = match_skip_impl(skip, key);
        if (m < skip.size()) return false;
        key.remove_prefix(m);

        if (key.empty()) {
            return n->try_read_eos(false, out);
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);

        n = n->get_child(false, c);
        if (!n || n->is_poisoned() || builder_t::is_sentinel(n)) return false;
        
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
    std::string_view kbv(kb.data(), kb.size());
    if constexpr (THREADED) {
        auto& slot = get_ebr_slot();
        auto guard = slot.get_guard();
        
        for (int attempts = 0; attempts < 10; ++attempts) {
            T dummy;
            read_path path;
            
            ptr_t root = root_.load();
            if (!root) return false;
            if (root->is_poisoned()) continue;
            
            bool found = read_impl_optimistic(root, kbv, dummy, path);
            if (validate_read_path(path)) return found;
        }
        return contains_impl(root_.load(), kbv);
    } else {
        return contains_impl(root_.load(), kbv);
    }
}

TKTRIE_TEMPLATE
std::pair<typename TKTRIE_CLASS::iterator, bool> TKTRIE_CLASS::insert(const std::pair<const Key, T>& kv) {
    auto kb = traits::to_bytes(kv.first);
    std::string_view kbv(kb.data(), kb.size());
    return insert_locked(kv.first, kbv, kv.second);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::erase(const Key& key) {
    auto kb = traits::to_bytes(key);
    std::string_view kbv(kb.data(), kb.size());
    return erase_locked(kbv);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::iterator TKTRIE_CLASS::find(const Key& key) const {
    auto kb = traits::to_bytes(key);
    std::string_view kbv(kb.data(), kb.size());
    T value;
    if constexpr (THREADED) {
        auto& slot = get_ebr_slot();
        auto guard = slot.get_guard();
        
        for (int attempts = 0; attempts < 10; ++attempts) {
            read_path path;
            
            ptr_t root = root_.load();
            if (!root) return end();
            if (root->is_poisoned()) continue;
            
            bool found = read_impl_optimistic(root, kbv, value, path);
            if (validate_read_path(path)) {
                if (found) return iterator(this, std::string(kbv), value);
                return end();
            }
        }
        if (read_impl(root_.load(), kbv, value)) {
            return iterator(this, std::string(kbv), value);
        }
    } else {
        if (read_impl(root_.load(), kbv, value)) {
            return iterator(this, std::string(kbv), value);
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
