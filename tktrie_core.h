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
        uint64_t epoch = epoch_.load(std::memory_order_acquire);
        ebr_retire(n, epoch);  // Lock-free
    } else {
        node_deleter(n);
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::ebr_retire(ptr_t n, uint64_t epoch) {
    // Lock-free push using external retire_entry wrapper (saves 16 bytes per node)
    if constexpr (THREADED) {
        auto* entry = new retire_entry_t(n, epoch);
        retire_entry_t* old_head = retired_head_.load(std::memory_order_relaxed);
        do {
            entry->next = old_head;
        } while (!retired_head_.compare_exchange_weak(old_head, entry,
                    std::memory_order_release, std::memory_order_relaxed));
        retired_count_.fetch_add(1, std::memory_order_relaxed);
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::reader_enter() const noexcept {
    if constexpr (THREADED) {
        size_t slot = thread_slot_hash(EBR_PADDED_SLOTS);
        uint64_t epoch = epoch_.load(std::memory_order_acquire);
        const_cast<PaddedReaderSlot&>(reader_epochs_[slot])
            .epoch.store(epoch, std::memory_order_release);
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::reader_exit() const noexcept {
    if constexpr (THREADED) {
        size_t slot = thread_slot_hash(EBR_PADDED_SLOTS);
        const_cast<PaddedReaderSlot&>(reader_epochs_[slot])
            .epoch.store(0, std::memory_order_release);
    }
}

TKTRIE_TEMPLATE
uint64_t TKTRIE_CLASS::min_reader_epoch() const noexcept {
    if constexpr (THREADED) {
        uint64_t current = epoch_.load(std::memory_order_acquire);
        uint64_t min_e = current;
        for (size_t i = 0; i < EBR_PADDED_SLOTS; ++i) {
            uint64_t e = reader_epochs_[i].epoch.load(std::memory_order_acquire);
            if (e != 0 && e < min_e) {
                min_e = e;
            }
        }
        return min_e;
    }
    return 0;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::ebr_cleanup() {
    // Grab mutex, atomically take list, process, push back unreclaimable
    if constexpr (THREADED) {
        std::lock_guard<std::mutex> lock(ebr_mutex_);
        
        // Atomically take ownership of entire retired list
        retire_entry_t* list = retired_head_.exchange(nullptr, std::memory_order_acquire);
        retired_count_.store(0, std::memory_order_relaxed);
        if (!list) return;
        
        uint64_t min_epoch = min_reader_epoch();
        
        // Partition into freeable and still-retired
        retire_entry_t* still_head = nullptr;
        size_t still_count = 0;
        
        while (list) {
            retire_entry_t* curr = list;
            list = list->next;
            
            // Safe if retired at least 2 epochs before min reader epoch
            if (curr->epoch + 2 <= min_epoch) {
                node_deleter(curr->node);
                delete curr;
            } else {
                // Still needs protection - prepend to still list
                curr->next = still_head;
                still_head = curr;
                ++still_count;
            }
        }
        
        // Push still-retired back (other threads may have added more)
        if (still_head) {
            // Find tail of still list
            retire_entry_t* still_tail = still_head;
            while (still_tail->next) still_tail = still_tail->next;
            
            retire_entry_t* old_head = retired_head_.load(std::memory_order_relaxed);
            do {
                still_tail->next = old_head;
            } while (!retired_head_.compare_exchange_weak(old_head, still_head,
                        std::memory_order_release, std::memory_order_relaxed));
            retired_count_.fetch_add(still_count, std::memory_order_relaxed);
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
    
    // BINARY, LIST, POP, or FULL leaf - need exactly 1 char remaining
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_binary()) {
        auto* bn = n->template as_binary<true>();
        int idx = bn->find(c);
        if (idx < 0) return false;
        return bn->values[idx].try_read(out);
    }
    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        int idx = ln->find(c);
        if (idx < 0) return false;
        return ln->read_value(idx, out);
    }
    if (n->is_pop()) {
        auto* pn = n->template as_pop<true>();
        return pn->read_value(c, out);
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
    
    // BINARY, LIST, POP, or FULL leaf - need exactly 1 char remaining
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_binary()) {
        return n->template as_binary<true>()->has(c);
    }
    if (n->is_list()) [[likely]] {
        return n->template as_list<true>()->has(c);
    }
    if (n->is_pop()) {
        return n->template as_pop<true>()->has(c);
    }
    return n->template as_full<true>()->has(c);
}

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl_optimistic(ptr_t n, std::string_view key, T& out, read_path& path) const noexcept
    requires NEED_VALUE {
    if (!n) return false;
    if (!path.push_checked(n)) return false;  // Single header load for poison + version
    
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
        
        if (!n) return false;
        if (!path.push_checked(n)) return false;  // Single header load
    }
    
    // Leaf node: skip already consumed, poison already checked in push_checked
    if (n->is_skip()) {
        if (!key.empty()) return false;
        return n->as_skip()->value.try_read(out);
    }
    
    // BINARY, LIST, POP, or FULL leaf
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_binary()) {
        auto* bn = n->template as_binary<true>();
        int idx = bn->find(c);
        if (idx < 0) return false;
        return bn->values[idx].try_read(out);
    }
    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        int idx = ln->find(c);
        if (idx < 0) return false;
        return ln->read_value(idx, out);
    }
    if (n->is_pop()) {
        auto* pn = n->template as_pop<true>();
        return pn->read_value(c, out);
    }
    auto* fn = n->template as_full<true>();
    if (!fn->has(c)) return false;
    return fn->read_value(c, out);
}

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl_optimistic(ptr_t n, std::string_view key, read_path& path) const noexcept
    requires (!NEED_VALUE) {
    if (!n) return false;
    if (!path.push_checked(n)) return false;  // Single header load for poison + version
    
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
        
        if (!n) return false;
        if (!path.push_checked(n)) return false;  // Single header load
    }
    
    // Leaf node: skip already consumed, poison already checked in push_checked
    if (n->is_skip()) {
        return key.empty();
    }
    
    // BINARY, LIST, POP, or FULL leaf
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_binary()) {
        return n->template as_binary<true>()->has(c);
    }
    if (n->is_list()) [[likely]] {
        return n->template as_list<true>()->has(c);
    }
    if (n->is_pop()) {
        return n->template as_pop<true>()->has(c);
    }
    return n->template as_full<true>()->has(c);
}

TKTRIE_TEMPLATE
inline bool TKTRIE_CLASS::validate_read_path(const read_path& path) const noexcept {
    [[assume(path.len >= 0 && path.len <= 64)]];
    // Version check is sufficient - poison() bumps version
    for (int i = 0; i < path.len; ++i) {
        if (get_version(path.nodes[i]->header()) != path.versions[i]) {
            return false;
        }
    }
    return true;
}

// -----------------------------------------------------------------------------
// Public interface
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
TKTRIE_CLASS::tktrie() : root_(nullptr) {
    // For fixed-length keys, create permanent interior root node
    // This ensures all skips are at most (FIXED_LEN - 1) bytes
    if constexpr (FIXED_LEN > 0) {
        root_.store(builder_.make_interior_list(""));
    }
}

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
    
    if constexpr (FIXED_LEN > 0) {
        // For fixed-length keys, reset to empty permanent root
        root_.store(builder_.make_interior_list(""));
    } else {
        root_.store(nullptr);
    }
    
    if (r && !builder_t::is_sentinel(r)) {
        builder_.dealloc_node(r);
    }
    size_.store(0);
    if constexpr (THREADED) {
        // Drain and delete entire retired list
        std::lock_guard<std::mutex> lock(ebr_mutex_);
        retire_entry_t* list = retired_head_.exchange(nullptr, std::memory_order_acquire);
        retired_count_.store(0, std::memory_order_relaxed);
        while (list) {
            retire_entry_t* curr = list;
            list = list->next;
            node_deleter(curr->node);
            delete curr;
        }
    }
}

TKTRIE_TEMPLATE
inline bool TKTRIE_CLASS::contains(const Key& key) const {
    auto kb = traits::to_bytes(key);
    std::string_view kbv(kb.data(), kb.size());
    if constexpr (THREADED) {
        // Readers cleanup at 2x threshold (backstop only)
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED * 2) {
            const_cast<tktrie*>(this)->ebr_cleanup();
        }
        
        reader_enter();
        
        for (int attempts = 0; attempts < 10; ++attempts) {
            // Fast path: check epoch before and after traversal
            uint64_t epoch_before = epoch_.load(std::memory_order_acquire);
            
            ptr_t root = root_.load();
            if (!root) {
                reader_exit();
                return false;
            }
            
            // Must check poison on root to catch sentinel (infinite loop otherwise)
            if (root->is_poisoned()) continue;
            
            // Simple traversal without path recording
            bool found = read_impl<false>(root, kbv);
            
            // Validate: if epoch unchanged, result is valid
            uint64_t epoch_after = epoch_.load(std::memory_order_acquire);
            if (epoch_before == epoch_after) {
                reader_exit();
                return found;
            }
            // Write happened during traversal, retry
        }
        // Fallback: too many retries, use locked read
        bool result = read_impl<false>(root_.load(), kbv);
        reader_exit();
        return result;
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
    if constexpr (THREADED) {
        if (retired_any) {
            epoch_.fetch_add(1, std::memory_order_acq_rel);
        }
        // Writers cleanup at 1x threshold
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED) {
            ebr_cleanup();
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
            epoch_.fetch_add(1, std::memory_order_acq_rel);
        }
        // Writers cleanup at 1x threshold
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED) {
            ebr_cleanup();
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
        // Readers cleanup at 2x threshold (backstop only)
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED * 2) {
            const_cast<tktrie*>(this)->ebr_cleanup();
        }
        
        reader_enter();
        
        for (int attempts = 0; attempts < 10; ++attempts) {
            read_path path;
            
            ptr_t root = root_.load();
            if (!root) {
                reader_exit();
                return end();
            }
            if (root->is_poisoned()) continue;
            
            bool found = read_impl_optimistic<true>(root, kbv, value, path);
            if (validate_read_path(path)) {
                reader_exit();
                if (found) return iterator(this, kbv, value);
                return end();
            }
        }
        bool found = read_impl<true>(root_.load(), kbv, value);
        reader_exit();
        if (found) return iterator(this, kbv, value);
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
        // Take ownership of entire list
        retire_entry_t* list = retired_head_.exchange(nullptr, std::memory_order_acquire);
        retired_count_.store(0, std::memory_order_relaxed);
        // Free everything (both nodes and entries)
        while (list) {
            retire_entry_t* curr = list;
            list = list->next;
            node_deleter(curr->node);
            delete curr;
        }
    }
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_insert.h"
