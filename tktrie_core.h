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
    if (n->is_full() && n->as_full()->valid.template atomic_test<THREADED>(c)) {
        return n->as_full()->children[c].load();
    }
    return nullptr;
}

// Hybrid spin-then-lock-wait version for sentinel-based synchronization
// Returns (child_ptr, hit_sentinel) - if hit_sentinel, caller should reset path
TKTRIE_TEMPLATE
std::pair<typename TKTRIE_CLASS::ptr_t, bool> TKTRIE_CLASS::find_child_wait(
    ptr_t n, unsigned char c) const noexcept {
    ptr_t child = nullptr;
    bool hit_sentinel = false;
    
    auto reload_child = [&]() -> ptr_t {
        if (n->is_list()) {
            int idx = n->as_list()->chars.find(c);
            return (idx >= 0) ? n->as_list()->children[idx].load() : nullptr;
        } else if (n->is_full() && n->as_full()->valid.template atomic_test<THREADED>(c)) {
            return n->as_full()->children[c].load();
        }
        return nullptr;
    };
    
    child = reload_child();
    if (!child) return {nullptr, false};
    
    // Check for sentinel - if hit, try spin then lock-wait
    if constexpr (THREADED) {
        if (is_retry_sentinel(child)) {
            hit_sentinel = true;
            
            // Fast path: spin for a while (writer might be almost done)
            for (int spin = 0; spin < 48; ++spin) {
                child = reload_child();
                if (!is_retry_sentinel(child)) {
                    return {child, hit_sentinel};
                }
                // Pause instruction hint for spin-wait
                #if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
                    __builtin_ia32_pause();
                #elif defined(__aarch64__) || defined(_M_ARM64)
                    asm volatile("yield" ::: "memory");
                #endif
            }
            
            // Slow path: still sentinel, block on writer's mutex
            mutex_.lock();
            mutex_.unlock();
            child = reload_child();
        }
    }
    
    return {child, hit_sentinel};
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::atomic_ptr* TKTRIE_CLASS::get_child_slot(ptr_t n, unsigned char c) noexcept {
    if (n->is_list()) {
        int idx = n->as_list()->chars.find(c);
        return idx >= 0 ? &n->as_list()->children[idx] : nullptr;
    }
    if (n->is_full() && n->as_full()->valid.template atomic_test<THREADED>(c)) {
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
    if (!leaf->as_full()->valid.template atomic_test<THREADED>(c)) return false;
    out = leaf->as_full()->leaf_values[c];
    return true;
}

// -----------------------------------------------------------------------------
// Optimistic read operations (lock-free fast path for THREADED mode)
// Uses sentinel wait + version validation for safe lock-free reads
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_impl_optimistic(ptr_t n, std::string_view key, T& out, read_path& path) const noexcept {
    if (!n) return false;
    
    // Record root in path
    if (!path.push(n)) return false;
    
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

        // Use wait-load - blocks on mutex if sentinel hit
        auto [child, hit_sentinel] = find_child_wait(n, c);
        if (!child) return false;
        
        if (hit_sentinel) {
            // We synchronized with writer via mutex lock/unlock
            // Clear old path - it's stale but we've synced
            path.clear();
        }
        
        // Record this node in path
        n = child;
        if (!path.push(n)) return false;
    }
    
    // n is now a leaf
    return read_from_leaf_optimistic(n, key, out, path);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_from_leaf_optimistic(ptr_t leaf, std::string_view key, T& out, read_path& path) const noexcept {
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
    // FULL - must use atomic test to avoid data race with concurrent writers
    if (!leaf->as_full()->valid.template atomic_test<THREADED>(c)) return false;
    out = leaf->as_full()->leaf_values[c];
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::validate_read_path(const read_path& path) const noexcept {
    for (int i = 0; i < path.len; ++i) {
        if (path.nodes[i]->version() != path.versions[i]) {
            return false;
        }
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
        // Lock-free read with hybrid sentinel handling + version validation
        for (int attempts = 0; attempts < 10; ++attempts) {
            T dummy;
            read_path path;
            
            // Load root, hybrid spin-then-lock on sentinel
            ptr_t root = root_.load();
            if (is_retry_sentinel(root)) {
                // Fast path: spin for a while
                for (int spin = 0; spin < 48; ++spin) {
                    root = root_.load();
                    if (!is_retry_sentinel(root)) goto root_ready;
                    #if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
                        __builtin_ia32_pause();
                    #elif defined(__aarch64__) || defined(_M_ARM64)
                        asm volatile("yield" ::: "memory");
                    #endif
                }
                // Slow path: block on mutex
                mutex_.lock();
                mutex_.unlock();
                root = root_.load();
            }
            root_ready:
            if (!root) return false;
            
            bool found = read_impl_optimistic(root, kb, dummy, path);
            
            // Validate all versions unchanged
            if (validate_read_path(path)) {
                return found;
            }
            // Version mismatch - concurrent modification, retry
        }
        // Too many retries - fall back to guarded read (shouldn't happen normally)
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
        // Lock-free read with hybrid sentinel handling + version validation
        for (int attempts = 0; attempts < 10; ++attempts) {
            read_path path;
            
            // Load root, hybrid spin-then-lock on sentinel
            ptr_t root = root_.load();
            if (is_retry_sentinel(root)) {
                // Fast path: spin for a while
                for (int spin = 0; spin < 48; ++spin) {
                    root = root_.load();
                    if (!is_retry_sentinel(root)) goto root_ready;
                    #if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
                        __builtin_ia32_pause();
                    #elif defined(__aarch64__) || defined(_M_ARM64)
                        asm volatile("yield" ::: "memory");
                    #endif
                }
                // Slow path: block on mutex
                mutex_.lock();
                mutex_.unlock();
                root = root_.load();
            }
            root_ready:
            if (!root) return end();
            
            bool found = read_impl_optimistic(root, kb, value, path);
            
            // Validate all versions unchanged
            if (validate_read_path(path)) {
                if (found) {
                    return iterator(this, std::string(kb), value);
                }
                return end();
            }
            // Version mismatch - concurrent modification, retry
        }
        // Too many retries - fall back to guarded read
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
