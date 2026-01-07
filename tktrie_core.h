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
        std::lock_guard<std::mutex> lock(ebr_mutex_);
        uint64_t epoch = ebr_global::instance().advance_epoch();
        ebr_retire(n, epoch);
        ebr_cleanup();  // Check and cleanup while holding lock
    } else {
        node_deleter(n);
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::ebr_retire(ptr_t n, uint64_t epoch) {
    // Called under ebr_mutex_
    if constexpr (THREADED) {
        retired_list_.push_back({n, epoch});
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::ebr_cleanup() {
    // Called under ebr_mutex_
    if constexpr (THREADED) {
        uint64_t min_epoch = ebr_global::instance().compute_min_epoch();
        
        auto it = retired_list_.begin();
        while (it != retired_list_.end()) {
            if (it->epoch + 2 <= min_epoch) {
                node_deleter(it->ptr);
                it = retired_list_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::ebr_should_cleanup() const {
    if constexpr (THREADED) {
        uint64_t current = ebr_global::instance().current_epoch();
        uint64_t min_epoch = ebr_global::instance().compute_min_epoch();
        return (current - min_epoch) > EBR_CLEANUP_THRESHOLD;
    }
    return false;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::ebr_maybe_cleanup() {
    if constexpr (THREADED) {
        if (ebr_should_cleanup()) {
            std::lock_guard<std::mutex> lock(ebr_mutex_);
            ebr_cleanup();
        }
    }
}

// -----------------------------------------------------------------------------
// Read operations
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl(ptr_t n, std::string_view key, T& out) const noexcept
    requires NEED_VALUE {
    if constexpr (THREADED) {
        if (!n || n->is_poisoned()) return false;
    } else {
        if (!n) return false;
    }
    
    // Unified loop: consume skip for every node (interior or leaf)
    while (true) {
        if (!consume_prefix(key, n->skip_str())) return false;
        
        if (n->is_leaf()) break;
        
        // Interior node: check EOS or descend
        if (key.empty()) {
            return n->try_read_eos(out);
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        n = n->get_child(c);
        
        if constexpr (THREADED) {
            if (!n || n->is_poisoned()) return false;
        } else {
            if (!n) return false;
        }
    }
    
    // Leaf node: skip already consumed, poison already checked
    if (n->is_skip()) {
        if (!key.empty()) return false;
        return n->as_skip()->value.try_read(out);
    }
    
    // LIST or FULL leaf - need exactly 1 char remaining
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        int idx = ln->find(c);
        if (idx < 0) return false;
        return ln->read_value(idx, out);
    }
    auto* fn = n->template as_full<true>();
    if (!fn->has(c)) return false;
    return fn->read_value(c, out);
}

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl(ptr_t n, std::string_view key) const noexcept
    requires (!NEED_VALUE) {
    if constexpr (THREADED) {
        if (!n || n->is_poisoned()) return false;
    } else {
        if (!n) return false;
    }
    
    // Unified loop: consume skip for every node (interior or leaf)
    while (true) {
        if (!consume_prefix(key, n->skip_str())) return false;
        
        if (n->is_leaf()) break;
        
        // Interior node: check EOS or descend
        if (key.empty()) {
            return n->has_eos();
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        n = n->get_child(c);
        
        if constexpr (THREADED) {
            if (!n || n->is_poisoned()) return false;
        } else {
            if (!n) return false;
        }
    }
    
    // Leaf node: skip already consumed, poison already checked
    if (n->is_skip()) {
        return key.empty();
    }
    
    // LIST or FULL leaf - need exactly 1 char remaining
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_list()) [[likely]] {
        return n->template as_list<true>()->has(c);
    }
    return n->template as_full<true>()->has(c);
}

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl_optimistic(ptr_t n, std::string_view key, T& out, read_path& path) const noexcept
    requires NEED_VALUE {
    if (!n || n->is_poisoned()) return false;
    
    if (!path.push(n)) return false;
    
    // Unified loop: consume skip for every node
    while (true) {
        if (!consume_prefix(key, n->skip_str())) return false;
        
        if (n->is_leaf()) break;
        
        // Interior node: check EOS or descend
        if (key.empty()) {
            return n->try_read_eos(out);
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        n = n->get_child(c);
        
        if (!n || n->is_poisoned()) return false;
        if (!path.push(n)) return false;
    }
    
    // Leaf node: skip already consumed, poison already checked in loop
    if (n->is_skip()) {
        if (!key.empty()) return false;
        return n->as_skip()->value.try_read(out);
    }
    
    // LIST or FULL leaf
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        int idx = ln->find(c);
        if (idx < 0) return false;
        return ln->read_value(idx, out);
    }
    auto* fn = n->template as_full<true>();
    if (!fn->has(c)) return false;
    return fn->read_value(c, out);
}

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl_optimistic(ptr_t n, std::string_view key, read_path& path) const noexcept
    requires (!NEED_VALUE) {
    if (!n || n->is_poisoned()) return false;
    
    if (!path.push(n)) return false;
    
    // Unified loop: consume skip for every node
    while (true) {
        if (!consume_prefix(key, n->skip_str())) return false;
        
        if (n->is_leaf()) break;
        
        // Interior node: check EOS or descend
        if (key.empty()) {
            return n->has_eos();
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        n = n->get_child(c);
        
        if (!n || n->is_poisoned()) return false;
        if (!path.push(n)) return false;
    }
    
    // Leaf node: skip already consumed, poison already checked in loop
    if (n->is_skip()) {
        return key.empty();
    }
    
    // LIST or FULL leaf
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_list()) [[likely]] {
        return n->template as_list<true>()->has(c);
    }
    return n->template as_full<true>()->has(c);
}

TKTRIE_TEMPLATE
inline bool TKTRIE_CLASS::validate_read_path(const read_path& path) const noexcept {
    for (int i = 0; i < path.len; ++i) {
        if (path.nodes[i]->is_poisoned()) return false;
        if (path.nodes[i]->version() != path.versions[i]) return false;
    }
    return true;
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
        // Drain and delete entire retired list
        std::lock_guard<std::mutex> lock(ebr_mutex_);
        for (auto& rn : retired_list_) {
            node_deleter(rn.ptr);
        }
        retired_list_.clear();
    }
}

TKTRIE_TEMPLATE
inline bool TKTRIE_CLASS::contains(const Key& key) const {
    auto kb = traits::to_bytes(key);
    std::string_view kbv(kb.data(), kb.size());
    if constexpr (THREADED) {
        // Check cleanup BEFORE enter so our epoch doesn't block our own cleanup
        const_cast<tktrie*>(this)->ebr_maybe_cleanup();
        
        auto& slot = get_ebr_slot();
        uint64_t epoch = ebr_global::instance().current_epoch();
        slot.enter(epoch);
        
        for (int attempts = 0; attempts < 10; ++attempts) {
            read_path path;
            
            ptr_t root = root_.load();
            if (!root) return false;
            if (root->is_poisoned()) continue;  // Write in progress, retry
            
            bool found = read_impl_optimistic<false>(root, kbv, path);
            if (validate_read_path(path)) return found;
        }
        return read_impl<false>(root_.load(), kbv);
    } else {
        return read_impl<false>(root_.load(), kbv);
    }
}

TKTRIE_TEMPLATE
std::pair<typename TKTRIE_CLASS::iterator, bool> TKTRIE_CLASS::insert(const std::pair<const Key, T>& kv) {
    auto kb = traits::to_bytes(kv.first);
    std::string_view kbv(kb.data(), kb.size());
    bool retired_any = false;
    auto result = insert_locked(kv.first, kbv, kv.second, &retired_any);
    return result;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::erase(const Key& key) {
    auto kb = traits::to_bytes(key);
    std::string_view kbv(kb.data(), kb.size());
    auto [erased, retired_any] = erase_locked(kbv);
    return erased;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::iterator TKTRIE_CLASS::find(const Key& key) const {
    auto kb = traits::to_bytes(key);
    std::string_view kbv(kb.data(), kb.size());
    T value;
    if constexpr (THREADED) {
        // Check cleanup BEFORE enter so our epoch doesn't block our own cleanup
        const_cast<tktrie*>(this)->ebr_maybe_cleanup();
        
        auto& slot = get_ebr_slot();
        uint64_t epoch = ebr_global::instance().current_epoch();
        slot.enter(epoch);
        
        for (int attempts = 0; attempts < 10; ++attempts) {
            read_path path;
            
            ptr_t root = root_.load();
            if (!root) return end();
            if (root->is_poisoned()) continue;
            
            bool found = read_impl_optimistic<true>(root, kbv, value, path);
            if (validate_read_path(path)) {
                if (found) return iterator(this, kbv, value);
                return end();
            }
        }
        if (read_impl<true>(root_.load(), kbv, value)) {
            return iterator(this, kbv, value);
        }
    } else {
        if (read_impl<true>(root_.load(), kbv, value)) {
            return iterator(this, kbv, value);
        }
    }
    return end();
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::reclaim_retired() noexcept {
    if constexpr (THREADED) {
        std::lock_guard<std::mutex> lock(ebr_mutex_);
        for (auto& r : retired_list_) {
            node_deleter(r.ptr);
        }
        retired_list_.clear();
    }
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_insert.h"
