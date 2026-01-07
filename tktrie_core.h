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
        ebr_retire_node(n, epoch);  // Lock-free push
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
void TKTRIE_CLASS::ebr_retire_node(ptr_t n, uint64_t epoch) {
    if constexpr (THREADED) {
        // Lock-free push to retired list
        auto* node = new retired_node{n, epoch, nullptr};
        retired_node* old_head = retired_head_.load(std::memory_order_relaxed);
        do {
            node->next = old_head;
        } while (!retired_head_.compare_exchange_weak(old_head, node,
                    std::memory_order_release, std::memory_order_relaxed));
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::ebr_try_reclaim() {
    if constexpr (THREADED) {
        uint64_t safe = ebr_global::instance().compute_safe_epoch();
        
        // Atomically take ownership of entire retired list
        retired_node* list = retired_head_.exchange(nullptr, std::memory_order_acquire);
        if (!list) return;
        
        // Partition into safe-to-delete and still-retired
        retired_node* still_head = nullptr;
        retired_node* still_tail = nullptr;
        
        while (list) {
            retired_node* curr = list;
            list = list->next;
            
            if (curr->epoch + 2 <= safe) {
                // Safe to delete
                node_deleter(curr->ptr);
                delete curr;
            } else {
                // Still needs protection - add to still-retired list
                curr->next = still_head;
                still_head = curr;
                if (!still_tail) still_tail = curr;
            }
        }
        
        // Prepend still-retired back to head (other threads may have added more)
        if (still_head) {
            retired_node* old_head = retired_head_.load(std::memory_order_relaxed);
            do {
                still_tail->next = old_head;
            } while (!retired_head_.compare_exchange_weak(old_head, still_head,
                        std::memory_order_release, std::memory_order_relaxed));
        }
    }
}

// -----------------------------------------------------------------------------
// Read operations
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_impl(ptr_t n, std::string_view key, T* out) const noexcept {
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
        if (key.empty()) return out ? n->try_read_eos(*out) : n->has_eos();
        
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
        return !out || n->as_skip()->value.try_read(*out);
    }
    
    // LIST or FULL leaf - need exactly 1 char remaining
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        int idx = ln->find(c);
        if (idx < 0) return false;
        return !out || ln->read_value(idx, *out);
    }
    auto* fn = n->template as_full<true>();
    if (!fn->has(c)) return false;
    return !out || fn->read_value(c, *out);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_impl_optimistic(ptr_t n, std::string_view key, T* out, read_path& path) const noexcept {
    if (!n || n->is_poisoned()) return false;
    
    if (!path.push(n)) return false;
    
    // Unified loop: consume skip for every node
    while (true) {
        if (!consume_prefix(key, n->skip_str())) return false;
        
        if (n->is_leaf()) break;
        
        // Interior node: check EOS or descend
        if (key.empty()) return out ? n->try_read_eos(*out) : n->has_eos();
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        n = n->get_child(c);
        
        if (!n || n->is_poisoned()) return false;
        if (!path.push(n)) return false;
    }
    
    // Leaf node: skip already consumed, poison already checked in loop
    if (n->is_skip()) {
        if (!key.empty()) return false;
        return !out || n->as_skip()->value.try_read(*out);
    }
    
    // LIST or FULL leaf
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        int idx = ln->find(c);
        if (idx < 0) return false;
        return !out || ln->read_value(idx, *out);
    }
    auto* fn = n->template as_full<true>();
    if (!fn->has(c)) return false;
    return !out || fn->read_value(c, *out);
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
    return read_impl(n, key, nullptr);  // nullptr = don't read value
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
        retired_node* list = retired_head_.exchange(nullptr, std::memory_order_acquire);
        while (list) {
            retired_node* curr = list;
            list = list->next;
            node_deleter(curr->ptr);
            delete curr;
        }
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
            read_path path;
            
            ptr_t root = root_.load();
            if (root->is_poisoned()) continue;  // Write in progress, retry
            
            bool found = read_impl_optimistic(root, kbv, nullptr, path);
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
    bool retired_any = false;
    auto result = insert_locked(kv.first, kbv, kv.second, &retired_any);
    if constexpr (THREADED) {
        if (retired_any) {
            ebr_global::instance().advance_epoch();
        }
    }
    return result;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::erase(const Key& key) {
    auto kb = traits::to_bytes(key);
    std::string_view kbv(kb.data(), kb.size());
    auto [erased, retired_any] = erase_locked(kbv);
    if constexpr (THREADED) {
        if (retired_any) {
            ebr_global::instance().advance_epoch();
        }
    }
    return erased;
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
            
            bool found = read_impl_optimistic(root, kbv, &value, path);
            if (validate_read_path(path)) {
                if (found) return iterator(this, kbv, value);
                return end();
            }
        }
        if (read_impl(root_.load(), kbv, &value)) {
            return iterator(this, kbv, value);
        }
    } else {
        if (read_impl(root_.load(), kbv, &value)) {
            return iterator(this, kbv, value);
        }
    }
    return end();
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::reclaim_retired() noexcept {
    if constexpr (THREADED) {
        // Drain and delete entire retired list
        retired_node* list = retired_head_.exchange(nullptr, std::memory_order_acquire);
        while (list) {
            retired_node* curr = list;
            list = list->next;
            node_deleter(curr->ptr);
            delete curr;
        }
    }
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_insert.h"
