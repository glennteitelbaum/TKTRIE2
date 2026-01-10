#pragma once

// This file contains implementation details for tktrie
// It should only be included from tktrie.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

TKTRIE_TEMPLATE
void TKTRIE_CLASS::node_deleter(void* ptr) {
    if (!ptr) return;
    auto* n = static_cast<ptr_t>(ptr);
    if (builder_t::is_sentinel(n)) return;
    builder_t::delete_node(n);
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::retire_node(ptr_t n) {
    if (!n || builder_t::is_sentinel(n)) return;
    if constexpr (THREADED) {
        n->poison();
        uint64_t epoch = epoch_.load(std::memory_order_acquire);
        ebr_retire(n, epoch);
    } else {
        node_deleter(n);
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::ebr_retire(ptr_t n, uint64_t epoch) {
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
size_t TKTRIE_CLASS::reader_enter() const noexcept {
    if constexpr (THREADED) {
        size_t start = thread_slot_hash(EBR_PADDED_SLOTS);
        uint64_t my_epoch = epoch_.load(std::memory_order_seq_cst);
        
        size_t slot = start;
        while (true) {
            uint64_t expected = 0;
            if (const_cast<PaddedReaderSlot&>(reader_epochs_[slot])
                    .epoch.compare_exchange_strong(expected, my_epoch,
                        std::memory_order_seq_cst, std::memory_order_relaxed)) {
                return slot;
            }
            slot = (slot + 1) % EBR_PADDED_SLOTS;
        }
    }
    return 0;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::reader_exit(size_t slot) const noexcept {
    if constexpr (THREADED) {
        const_cast<PaddedReaderSlot&>(reader_epochs_[slot])
            .epoch.store(0, std::memory_order_seq_cst);
    }
}

TKTRIE_TEMPLATE
uint64_t TKTRIE_CLASS::min_reader_epoch() const noexcept {
    if constexpr (THREADED) {
        uint64_t current = epoch_.load(std::memory_order_seq_cst);
        uint64_t min_e = current;
        for (size_t i = 0; i < EBR_PADDED_SLOTS; ++i) {
            uint64_t e = reader_epochs_[i].epoch.load(std::memory_order_seq_cst);
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
    if constexpr (THREADED) {
        std::lock_guard<std::mutex> lock(ebr_mutex_);
        
        retire_entry_t* list = retired_head_.exchange(nullptr, std::memory_order_acquire);
        retired_count_.store(0, std::memory_order_relaxed);
        if (!list) return;
        
        uint64_t min_epoch = min_reader_epoch();
        
        retire_entry_t* still_head = nullptr;
        size_t still_count = 0;
        
        while (list) {
            retire_entry_t* curr = list;
            list = list->next;
            
            if (curr->epoch + 8 <= min_epoch) {
                node_deleter(curr->node);
                delete curr;
            } else {
                curr->next = still_head;
                still_head = curr;
                ++still_count;
            }
        }
        
        if (still_head) {
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

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl(ptr_t n, std::string_view key, T& out) const noexcept
    requires NEED_VALUE {
    if (!n) return false;
    
    uint64_t h;
    while (true) {
        h = n->header();
        
        if constexpr (THREADED) {
            if (h & FLAG_POISON) return false;
        }
        
        if (h & FLAG_SKIP_USED) {
            if (!consume_prefix(key, n->skip_str())) return false;
        }
        
        if (h & FLAG_LEAF) [[unlikely]] break;
        
        // Interior node: check EOS or descend
        if (key.empty()) {
            if constexpr (FIXED_LEN > 0) {
                return false;
            } else {
                if (!(h & FLAG_HAS_EOS)) return false;
                return n->try_read_eos(out);
            }
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        n = n->get_child(c);
        
        if (!n) return false;
    }
    
    // Leaf node
    if (h & FLAG_SKIP) {
        if (!key.empty()) return false;
        return n->as_skip()->value.try_read(out);
    }
    
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    return n->try_read_leaf_value(c, out);
}

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl(ptr_t n, std::string_view key) const noexcept
    requires (!NEED_VALUE) {
    if (!n) return false;
    
    uint64_t h;
    while (true) {
        h = n->header();
        
        if constexpr (THREADED) {
            if (h & FLAG_POISON) return false;
        }
        
        if (h & FLAG_SKIP_USED) {
            if (!consume_prefix(key, n->skip_str())) return false;
        }
        
        if (h & FLAG_LEAF) [[unlikely]] break;
        
        // Interior node: check EOS or descend
        if (key.empty()) {
            if constexpr (FIXED_LEN > 0) {
                return false;
            } else {
                return (h & FLAG_HAS_EOS) != 0;
            }
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        n = n->get_child(c);
        
        if (!n) return false;
    }
    
    // Leaf node
    if (h & FLAG_SKIP) {
        return key.empty();
    }
    
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    return n->has_leaf_entry(c);
}

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl_optimistic(ptr_t n, std::string_view key, T& out, read_path& path) const noexcept
    requires NEED_VALUE {
    if (!n) return false;
    
    uint64_t h;
    while (true) {
        h = n->header();
        
        if (h & FLAG_POISON) return false;
        
        if (path.len >= read_path::MAX_DEPTH) return false;
        path.nodes[path.len] = n;
        path.versions[path.len] = h & VERSION_MASK;
        ++path.len;
        
        if (h & FLAG_SKIP_USED) {
            if (!consume_prefix(key, n->skip_str())) return false;
        }
        
        if (h & FLAG_LEAF) [[unlikely]] break;
        
        // Interior node: check EOS or descend
        if (key.empty()) {
            if constexpr (FIXED_LEN > 0) {
                return false;
            } else {
                if (!(h & FLAG_HAS_EOS)) return false;
                return n->try_read_eos(out);
            }
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        n = n->get_child(c);
        
        if (!n) return false;
    }
    
    // Leaf node
    if (h & FLAG_SKIP) {
        if (!key.empty()) return false;
        return n->as_skip()->value.try_read(out);
    }
    
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    return n->try_read_leaf_value(c, out);
}

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl_optimistic(ptr_t n, std::string_view key, read_path& path) const noexcept
    requires (!NEED_VALUE) {
    if (!n) return false;
    
    uint64_t h;
    while (true) {
        h = n->header();
        
        if (h & FLAG_POISON) return false;
        
        if (path.len >= read_path::MAX_DEPTH) return false;
        path.nodes[path.len] = n;
        path.versions[path.len] = h & VERSION_MASK;
        ++path.len;
        
        if (h & FLAG_SKIP_USED) {
            if (!consume_prefix(key, n->skip_str())) return false;
        }
        
        if (h & FLAG_LEAF) [[unlikely]] break;
        
        // Interior node: check EOS or descend
        if (key.empty()) {
            if constexpr (FIXED_LEN > 0) {
                return false;
            } else {
                return (h & FLAG_HAS_EOS) != 0;
            }
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        n = n->get_child(c);
        
        if (!n) return false;
    }
    
    // Leaf node
    if (h & FLAG_SKIP) {
        return key.empty();
    }
    
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    return n->has_leaf_entry(c);
}

TKTRIE_TEMPLATE
inline bool TKTRIE_CLASS::validate_read_path(const read_path& path) const noexcept {
    [[assume(path.len >= 0 && path.len <= 64)]];
    for (int i = 0; i < path.len; ++i) {
        if (get_version(path.nodes[i]->header()) != path.versions[i]) {
            return false;
        }
    }
    return true;
}

TKTRIE_TEMPLATE
TKTRIE_CLASS::tktrie() : root_(nullptr) {
    if constexpr (FIXED_LEN > 0) {
        root_.store(builder_.make_interior_list(""));
    }
}

TKTRIE_TEMPLATE
TKTRIE_CLASS::~tktrie() {
    ptr_t r = root_.load();
    root_.store(nullptr);
    if (r && !builder_t::is_sentinel(r)) {
        builder_.dealloc_node(r);
    }
    if constexpr (THREADED) {
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
        root_.store(builder_.make_interior_list(""));
    } else {
        root_.store(nullptr);
    }
    
    if (r && !builder_t::is_sentinel(r)) {
        builder_.dealloc_node(r);
    }
    size_.store(0);
    if constexpr (THREADED) {
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
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED * 2) {
            const_cast<tktrie*>(this)->ebr_cleanup();
        }
        
        size_t slot = reader_enter();
        
        for (int attempts = 0; attempts < 10; ++attempts) {
            uint64_t epoch_before = epoch_.load(std::memory_order_acquire);
            
            ptr_t root = root_.load();
            if (!root) {
                reader_exit(slot);
                return false;
            }
            
            if (root->is_poisoned()) continue;
            
            bool found = read_impl<false>(root, kbv);
            
            uint64_t epoch_after = epoch_.load(std::memory_order_acquire);
            if (epoch_before == epoch_after) {
                reader_exit(slot);
                return found;
            }
        }
        bool result = read_impl<false>(root_.load(), kbv);
        reader_exit(slot);
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
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED) {
            ebr_cleanup();
        }
    }
    return erased;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::iterator TKTRIE_CLASS::find(const Key& key) {
    auto kb = traits::to_bytes(key);
    std::string_view kbv(kb.data(), kb.size());
    T value;
    if constexpr (THREADED) {
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED * 2) {
            ebr_cleanup();
        }
        
        size_t slot = reader_enter();
        
        for (int attempts = 0; attempts < 10; ++attempts) {
            uint64_t epoch_before = epoch_.load(std::memory_order_acquire);
            
            ptr_t root = root_.load();
            if (!root) {
                reader_exit(slot);
                return end();
            }
            if (root->is_poisoned()) continue;
            
            bool found = read_impl<true>(root, kbv, value);
            
            uint64_t epoch_after = epoch_.load(std::memory_order_acquire);
            if (epoch_before == epoch_after) {
                reader_exit(slot);
                if (found) return iterator(this, kbv, value);
                return end();
            }
        }
        bool found = read_impl<true>(root_.load(), kbv, value);
        reader_exit(slot);
        if (found) return iterator(this, kbv, value);
    } else {
        if (read_impl<true>(root_.load(), kbv, value)) {
            return iterator(this, kbv, value);
        }
    }
    return end();
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::const_iterator TKTRIE_CLASS::find(const Key& key) const {
    auto kb = traits::to_bytes(key);
    std::string_view kbv(kb.data(), kb.size());
    T value;
    if constexpr (THREADED) {
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED * 2) {
            const_cast<tktrie*>(this)->ebr_cleanup();
        }
        
        size_t slot = reader_enter();
        
        for (int attempts = 0; attempts < 10; ++attempts) {
            uint64_t epoch_before = epoch_.load(std::memory_order_acquire);
            
            ptr_t root = root_.load();
            if (!root) {
                reader_exit(slot);
                return end();
            }
            if (root->is_poisoned()) continue;
            
            bool found = read_impl<true>(root, kbv, value);
            
            uint64_t epoch_after = epoch_.load(std::memory_order_acquire);
            if (epoch_before == epoch_after) {
                reader_exit(slot);
                if (found) return const_iterator(this, kbv, value);
                return end();
            }
        }
        bool found = read_impl<true>(root_.load(), kbv, value);
        reader_exit(slot);
        if (found) return const_iterator(this, kbv, value);
    } else {
        if (read_impl<true>(root_.load(), kbv, value)) {
            return const_iterator(this, kbv, value);
        }
    }
    return end();
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::reclaim_retired() noexcept {
    if constexpr (THREADED) {
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

// -----------------------------------------------------------------------------
// Iterator helpers: find first/last/greater/less
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::find_first_bytes(std::string& out_key, T& out_value) const {
    ptr_t n = root_.load();
    if (!n || n->is_poisoned() || builder_t::is_sentinel(n)) return false;
    
    out_key.clear();
    
    while (n && !n->is_poisoned()) {
        out_key.append(n->skip_str());
        
        if (n->is_leaf()) {
            if (n->is_skip()) {
                n->as_skip()->value.try_read(out_value);
                return true;
            }
            // Multi-entry leaf: get first (smallest) entry
            if (n->is_binary()) {
                auto* bn = n->template as_binary<true>();
                if (bn->count() > 0) {
                    out_key.push_back(static_cast<char>(bn->char_at(0)));
                    bn->read_value(0, out_value);
                    return true;
                }
            } else if (n->is_list()) {
                auto* ln = n->template as_list<true>();
                if (ln->count() > 0) {
                    out_key.push_back(static_cast<char>(ln->char_at(0)));
                    ln->read_value(0, out_value);
                    return true;
                }
            } else if (n->is_pop()) {
                auto* pn = n->template as_pop<true>();
                if (pn->count() > 0) {
                    unsigned char c = pn->valid().first();
                    out_key.push_back(static_cast<char>(c));
                    pn->element_at_slot(0).try_read(out_value);
                    return true;
                }
            } else {
                auto* fn = n->template as_full<true>();
                if (fn->count() > 0) {
                    unsigned char c = fn->valid().first();
                    out_key.push_back(static_cast<char>(c));
                    fn->read_value(c, out_value);
                    return true;
                }
            }
            return false;
        }
        
        // Interior: check EOS first (smallest), then descend to first child
        if constexpr (FIXED_LEN == 0) {
            if (n->has_eos()) {
                n->try_read_eos(out_value);
                return true;
            }
        }
        
        // Get first child
        ptr_t child = nullptr;
        unsigned char first_c = 0;
        if (n->is_binary()) {
            auto* bn = n->template as_binary<false>();
            if (bn->count() > 0) { first_c = bn->char_at(0); child = bn->child_at_slot(0); }
        } else if (n->is_list()) {
            auto* ln = n->template as_list<false>();
            if (ln->count() > 0) { first_c = ln->char_at(0); child = ln->child_at_slot(0); }
        } else if (n->is_pop()) {
            auto* pn = n->template as_pop<false>();
            if (pn->count() > 0) { first_c = pn->valid().first(); child = pn->get_child(first_c); }
        } else {
            auto* fn = n->template as_full<false>();
            if (fn->count() > 0) { first_c = fn->valid().first(); child = fn->get_child(first_c); }
        }
        
        if (!child) return false;
        out_key.push_back(static_cast<char>(first_c));
        n = child;
    }
    return false;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::find_last_bytes(std::string& out_key, T& out_value) const {
    ptr_t n = root_.load();
    if (!n || n->is_poisoned() || builder_t::is_sentinel(n)) return false;
    
    out_key.clear();
    
    while (n && !n->is_poisoned()) {
        out_key.append(n->skip_str());
        
        if (n->is_leaf()) {
            if (n->is_skip()) {
                n->as_skip()->value.try_read(out_value);
                return true;
            }
            // Multi-entry leaf: get last (largest) entry
            if (n->is_binary()) {
                auto* bn = n->template as_binary<true>();
                int cnt = bn->count();
                if (cnt > 0) {
                    out_key.push_back(static_cast<char>(bn->char_at(cnt - 1)));
                    bn->read_value(cnt - 1, out_value);
                    return true;
                }
            } else if (n->is_list()) {
                auto* ln = n->template as_list<true>();
                int cnt = ln->count();
                if (cnt > 0) {
                    out_key.push_back(static_cast<char>(ln->char_at(cnt - 1)));
                    ln->read_value(cnt - 1, out_value);
                    return true;
                }
            } else if (n->is_pop()) {
                auto* pn = n->template as_pop<true>();
                int cnt = pn->count();
                if (cnt > 0) {
                    // Find last set bit
                    unsigned char last_c = 255;
                    while (last_c > 0 && !pn->valid().test(last_c)) --last_c;
                    out_key.push_back(static_cast<char>(last_c));
                    pn->element_at_slot(cnt - 1).try_read(out_value);
                    return true;
                }
            } else {
                auto* fn = n->template as_full<true>();
                if (fn->count() > 0) {
                    unsigned char last_c = 255;
                    while (last_c > 0 && !fn->valid().test(last_c)) --last_c;
                    out_key.push_back(static_cast<char>(last_c));
                    fn->read_value(last_c, out_value);
                    return true;
                }
            }
            return false;
        }
        
        // Interior: descend to last child, then check EOS last (EOS is smallest for a node)
        ptr_t child = nullptr;
        unsigned char last_c = 0;
        if (n->is_binary()) {
            auto* bn = n->template as_binary<false>();
            int cnt = bn->count();
            if (cnt > 0) { last_c = bn->char_at(cnt - 1); child = bn->child_at_slot(cnt - 1); }
        } else if (n->is_list()) {
            auto* ln = n->template as_list<false>();
            int cnt = ln->count();
            if (cnt > 0) { last_c = ln->char_at(cnt - 1); child = ln->child_at_slot(cnt - 1); }
        } else if (n->is_pop()) {
            auto* pn = n->template as_pop<false>();
            int cnt = pn->count();
            if (cnt > 0) {
                unsigned char c = 255;
                while (c > 0 && !pn->valid().test(c)) --c;
                last_c = c;
                child = pn->get_child(last_c);
            }
        } else {
            auto* fn = n->template as_full<false>();
            if (fn->count() > 0) {
                unsigned char c = 255;
                while (c > 0 && !fn->valid().test(c)) --c;
                last_c = c;
                child = fn->get_child(last_c);
            }
        }
        
        if (child) {
            out_key.push_back(static_cast<char>(last_c));
            n = child;
        } else if constexpr (FIXED_LEN == 0) {
            // No children, check EOS
            if (n->has_eos()) {
                n->try_read_eos(out_value);
                return true;
            }
            return false;
        } else {
            return false;
        }
    }
    return false;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::find_greater_bytes(const std::string& key, std::string& out_key, T& out_value) const {
    // Find smallest key strictly greater than 'key'
    // Strategy: traverse to key's position, then advance
    ptr_t n = root_.load();
    if (!n || n->is_poisoned() || builder_t::is_sentinel(n)) return false;
    
    struct frame { ptr_t node; size_t key_pos; int child_idx; };
    std::vector<frame> stack;
    
    std::string_view kv(key);
    out_key.clear();
    
    // Navigate toward key, recording path
    while (n && !n->is_poisoned()) {
        std::string_view skip = n->skip_str();
        size_t m = match_skip_impl(skip, kv);
        
        if (m < skip.size() && m < kv.size()) {
            // Divergence in skip: if key char < skip char, this subtree is all greater
            if (static_cast<unsigned char>(kv[m]) < static_cast<unsigned char>(skip[m])) {
                out_key.append(skip);
                if (n->is_leaf()) {
                    if (n->is_skip()) {
                        n->as_skip()->value.try_read(out_value);
                        return true;
                    }
                    // First entry of leaf
                    if (n->is_binary()) {
                        auto* bn = n->template as_binary<true>();
                        out_key.push_back(static_cast<char>(bn->char_at(0)));
                        bn->read_value(0, out_value);
                        return true;
                    }
                    // ... similar for other types - descend left
                }
                // Descend to first value in this subtree
                goto descend_left;
            }
            // key char > skip char: need to backtrack
            goto backtrack;
        }
        
        if (m < skip.size()) {
            // Key exhausted but skip continues: key is a prefix, first in subtree is answer
            out_key.append(skip);
            goto descend_left;
        }
        
        out_key.append(skip);
        kv.remove_prefix(m);
        
        if (n->is_leaf()) {
            if (n->is_skip()) {
                if (kv.empty()) {
                    // Exact match - need to backtrack to find next
                    goto backtrack;
                }
                // Key is longer - no match, backtrack
                goto backtrack;
            }
            // Multi-entry leaf: find first entry > remaining key
            if (kv.size() == 1) {
                unsigned char c = static_cast<unsigned char>(kv[0]);
                // Find first entry with char > c
                if (n->is_binary()) {
                    auto* bn = n->template as_binary<true>();
                    for (int i = 0; i < bn->count(); ++i) {
                        if (bn->char_at(i) > c) {
                            out_key.push_back(static_cast<char>(bn->char_at(i)));
                            bn->read_value(i, out_value);
                            return true;
                        }
                    }
                } else if (n->is_list()) {
                    auto* ln = n->template as_list<true>();
                    for (int i = 0; i < ln->count(); ++i) {
                        if (ln->char_at(i) > c) {
                            out_key.push_back(static_cast<char>(ln->char_at(i)));
                            ln->read_value(i, out_value);
                            return true;
                        }
                    }
                } else if (n->is_pop()) {
                    auto* pn = n->template as_pop<true>();
                    int slot = 0;
                    bool found = false;
                    pn->valid().for_each_set([&](unsigned char ch) {
                        if (!found && ch > c) {
                            out_key.push_back(static_cast<char>(ch));
                            pn->element_at_slot(slot).try_read(out_value);
                            found = true;
                        }
                        ++slot;
                    });
                    if (found) return true;
                } else {
                    auto* fn = n->template as_full<true>();
                    for (unsigned next = c + 1; next <= 255; ++next) {
                        if (fn->valid().test(static_cast<unsigned char>(next))) {
                            out_key.push_back(static_cast<char>(next));
                            fn->read_value(static_cast<unsigned char>(next), out_value);
                            return true;
                        }
                    }
                }
            }
            goto backtrack;
        }
        
        // Interior node
        if (kv.empty()) {
            // Key matches this interior node's EOS position
            // Find first child (all children are greater than EOS)
            goto descend_left_from_interior;
        }
        
        unsigned char c = static_cast<unsigned char>(kv[0]);
        kv.remove_prefix(1);
        
        // Find child for c, record position for backtracking
        int child_idx = -1;
        ptr_t child = nullptr;
        if (n->is_binary()) {
            auto* bn = n->template as_binary<false>();
            child_idx = bn->find(c);
            if (child_idx >= 0) child = bn->child_at_slot(child_idx);
        } else if (n->is_list()) {
            auto* ln = n->template as_list<false>();
            child_idx = ln->find(c);
            if (child_idx >= 0) child = ln->child_at_slot(child_idx);
        } else if (n->is_pop()) {
            auto* pn = n->template as_pop<false>();
            if (pn->valid().test(c)) {
                child_idx = pn->valid().slot_for(c);
                child = pn->get_child(c);
            }
        } else {
            auto* fn = n->template as_full<false>();
            if (fn->valid().test(c)) {
                child_idx = c;  // For full, use char as index
                child = fn->get_child(c);
            }
        }
        
        stack.push_back({n, out_key.size(), child_idx});
        out_key.push_back(static_cast<char>(c));
        
        if (child) {
            n = child;
            continue;
        }
        
        // No exact child, find first child > c
        goto find_next_sibling;
    }
    
backtrack:
    // Pop stack and find next sibling
    while (!stack.empty()) {
        frame f = stack.back();
        stack.pop_back();
        out_key.resize(f.key_pos);
        n = f.node;
        
        // Find next child after f.child_idx
        ptr_t next_child = nullptr;
        unsigned char next_c = 0;
        
        if (n->is_binary()) {
            auto* bn = n->template as_binary<false>();
            if (f.child_idx + 1 < bn->count()) {
                next_c = bn->char_at(f.child_idx + 1);
                next_child = bn->child_at_slot(f.child_idx + 1);
            }
        } else if (n->is_list()) {
            auto* ln = n->template as_list<false>();
            if (f.child_idx + 1 < ln->count()) {
                next_c = ln->char_at(f.child_idx + 1);
                next_child = ln->child_at_slot(f.child_idx + 1);
            }
        } else if (n->is_pop()) {
            auto* pn = n->template as_pop<false>();
            int slot = 0;
            bool found_current = false;
            pn->valid().for_each_set([&](unsigned char ch) {
                if (found_current && !next_child) {
                    next_c = ch;
                    next_child = pn->get_child(ch);
                }
                if (slot == f.child_idx) found_current = true;
                ++slot;
            });
        } else {
            auto* fn = n->template as_full<false>();
            for (unsigned c = static_cast<unsigned>(f.child_idx) + 1; c <= 255; ++c) {
                if (fn->valid().test(static_cast<unsigned char>(c))) {
                    next_c = static_cast<unsigned char>(c);
                    next_child = fn->get_child(next_c);
                    break;
                }
            }
        }
        
        if (next_child) {
            out_key.push_back(static_cast<char>(next_c));
            n = next_child;
            goto descend_left;
        }
    }
    return false;
    
find_next_sibling:
    {
        if (stack.empty()) return false;
        frame f = stack.back();
        stack.pop_back();
        out_key.resize(f.key_pos);
        n = f.node;
        
        unsigned char prev_c = static_cast<unsigned char>(key[f.key_pos]);
        ptr_t next_child = nullptr;
        unsigned char next_c = 0;
        
        // Find first child > prev_c
        if (n->is_binary()) {
            auto* bn = n->template as_binary<false>();
            for (int i = 0; i < bn->count(); ++i) {
                if (bn->char_at(i) > prev_c) {
                    next_c = bn->char_at(i);
                    next_child = bn->child_at_slot(i);
                    break;
                }
            }
        } else if (n->is_list()) {
            auto* ln = n->template as_list<false>();
            for (int i = 0; i < ln->count(); ++i) {
                if (ln->char_at(i) > prev_c) {
                    next_c = ln->char_at(i);
                    next_child = ln->child_at_slot(i);
                    break;
                }
            }
        } else if (n->is_pop()) {
            auto* pn = n->template as_pop<false>();
            pn->valid().for_each_set([&](unsigned char ch) {
                if (!next_child && ch > prev_c) {
                    next_c = ch;
                    next_child = pn->get_child(ch);
                }
            });
        } else {
            auto* fn = n->template as_full<false>();
            for (unsigned c = prev_c + 1; c <= 255; ++c) {
                if (fn->valid().test(static_cast<unsigned char>(c))) {
                    next_c = static_cast<unsigned char>(c);
                    next_child = fn->get_child(next_c);
                    break;
                }
            }
        }
        
        if (next_child) {
            out_key.push_back(static_cast<char>(next_c));
            n = next_child;
            goto descend_left;
        }
        goto backtrack;
    }

descend_left_from_interior:
    {
        ptr_t child = nullptr;
        unsigned char first_c = 0;
        if (n->is_binary()) {
            auto* bn = n->template as_binary<false>();
            if (bn->count() > 0) { first_c = bn->char_at(0); child = bn->child_at_slot(0); }
        } else if (n->is_list()) {
            auto* ln = n->template as_list<false>();
            if (ln->count() > 0) { first_c = ln->char_at(0); child = ln->child_at_slot(0); }
        } else if (n->is_pop()) {
            auto* pn = n->template as_pop<false>();
            if (pn->count() > 0) { first_c = pn->valid().first(); child = pn->get_child(first_c); }
        } else {
            auto* fn = n->template as_full<false>();
            if (fn->count() > 0) { first_c = fn->valid().first(); child = fn->get_child(first_c); }
        }
        if (child) {
            out_key.push_back(static_cast<char>(first_c));
            n = child;
            goto descend_left;
        }
        goto backtrack;
    }

descend_left:
    while (n && !n->is_poisoned()) {
        out_key.append(n->skip_str());
        
        if (n->is_leaf()) {
            if (n->is_skip()) {
                n->as_skip()->value.try_read(out_value);
                return true;
            }
            // First entry
            if (n->is_binary()) {
                auto* bn = n->template as_binary<true>();
                out_key.push_back(static_cast<char>(bn->char_at(0)));
                bn->read_value(0, out_value);
                return true;
            } else if (n->is_list()) {
                auto* ln = n->template as_list<true>();
                out_key.push_back(static_cast<char>(ln->char_at(0)));
                ln->read_value(0, out_value);
                return true;
            } else if (n->is_pop()) {
                auto* pn = n->template as_pop<true>();
                unsigned char c = pn->valid().first();
                out_key.push_back(static_cast<char>(c));
                pn->element_at_slot(0).try_read(out_value);
                return true;
            } else {
                auto* fn = n->template as_full<true>();
                unsigned char c = fn->valid().first();
                out_key.push_back(static_cast<char>(c));
                fn->read_value(c, out_value);
                return true;
            }
        }
        
        if constexpr (FIXED_LEN == 0) {
            if (n->has_eos()) {
                n->try_read_eos(out_value);
                return true;
            }
        }
        
        ptr_t child = nullptr;
        unsigned char first_c = 0;
        if (n->is_binary()) {
            auto* bn = n->template as_binary<false>();
            if (bn->count() > 0) { first_c = bn->char_at(0); child = bn->child_at_slot(0); }
        } else if (n->is_list()) {
            auto* ln = n->template as_list<false>();
            if (ln->count() > 0) { first_c = ln->char_at(0); child = ln->child_at_slot(0); }
        } else if (n->is_pop()) {
            auto* pn = n->template as_pop<false>();
            if (pn->count() > 0) { first_c = pn->valid().first(); child = pn->get_child(first_c); }
        } else {
            auto* fn = n->template as_full<false>();
            if (fn->count() > 0) { first_c = fn->valid().first(); child = fn->get_child(first_c); }
        }
        
        if (!child) return false;
        out_key.push_back(static_cast<char>(first_c));
        n = child;
    }
    return false;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::find_less_bytes(const std::string& key, std::string& out_key, T& out_value) const {
    // Find largest key strictly less than 'key'
    // This is symmetric to find_greater_bytes but descending right instead of left
    ptr_t n = root_.load();
    if (!n || n->is_poisoned() || builder_t::is_sentinel(n)) return false;
    
    struct frame { ptr_t node; size_t key_pos; int child_idx; bool has_eos; };
    std::vector<frame> stack;
    
    std::string_view kv(key);
    out_key.clear();
    
    // Navigate toward key, recording path
    while (n && !n->is_poisoned()) {
        std::string_view skip = n->skip_str();
        size_t m = match_skip_impl(skip, kv);
        
        if (m < skip.size() && m < kv.size()) {
            // Divergence: if key char > skip char, need to backtrack
            if (static_cast<unsigned char>(kv[m]) > static_cast<unsigned char>(skip[m])) {
                // This subtree is all less - descend to rightmost
                out_key.append(skip);
                goto descend_right;
            }
            // key char < skip char: backtrack
            goto backtrack;
        }
        
        if (m < skip.size()) {
            // Key exhausted but skip continues: all in this subtree are greater
            goto backtrack;
        }
        
        out_key.append(skip);
        kv.remove_prefix(m);
        
        if (n->is_leaf()) {
            if (n->is_skip()) {
                if (kv.empty()) {
                    // Exact match - need to backtrack to find previous
                    goto backtrack;
                }
                // Key is longer than skip - this value is less
                n->as_skip()->value.try_read(out_value);
                return true;
            }
            // Multi-entry leaf: find last entry < remaining key
            if (kv.size() == 1) {
                unsigned char c = static_cast<unsigned char>(kv[0]);
                // Find last entry with char < c
                if (n->is_binary()) {
                    auto* bn = n->template as_binary<true>();
                    for (int i = bn->count() - 1; i >= 0; --i) {
                        if (bn->char_at(i) < c) {
                            out_key.push_back(static_cast<char>(bn->char_at(i)));
                            bn->read_value(i, out_value);
                            return true;
                        }
                    }
                } else if (n->is_list()) {
                    auto* ln = n->template as_list<true>();
                    for (int i = ln->count() - 1; i >= 0; --i) {
                        if (ln->char_at(i) < c) {
                            out_key.push_back(static_cast<char>(ln->char_at(i)));
                            ln->read_value(i, out_value);
                            return true;
                        }
                    }
                } else if (n->is_pop()) {
                    auto* pn = n->template as_pop<true>();
                    unsigned char found_c = 0;
                    int found_slot = -1;
                    int slot = 0;
                    pn->valid().for_each_set([&](unsigned char ch) {
                        if (ch < c) { found_c = ch; found_slot = slot; }
                        ++slot;
                    });
                    if (found_slot >= 0) {
                        out_key.push_back(static_cast<char>(found_c));
                        pn->element_at_slot(found_slot).try_read(out_value);
                        return true;
                    }
                } else {
                    auto* fn = n->template as_full<true>();
                    for (int prev = static_cast<int>(c) - 1; prev >= 0; --prev) {
                        if (fn->valid().test(static_cast<unsigned char>(prev))) {
                            out_key.push_back(static_cast<char>(prev));
                            fn->read_value(static_cast<unsigned char>(prev), out_value);
                            return true;
                        }
                    }
                }
            } else if (kv.empty()) {
                // Key matches interior position, need EOS which doesn't exist for leaf
                goto backtrack;
            }
            goto backtrack;
        }
        
        // Interior node
        if (kv.empty()) {
            // Key matches this interior node's EOS position
            // EOS is smallest in this subtree, so we need to backtrack
            goto backtrack;
        }
        
        unsigned char c = static_cast<unsigned char>(kv[0]);
        kv.remove_prefix(1);
        
        bool has_eos = false;
        if constexpr (FIXED_LEN == 0) {
            has_eos = n->has_eos();
        }
        
        // Find child for c
        int child_idx = -1;
        ptr_t child = nullptr;
        if (n->is_binary()) {
            auto* bn = n->template as_binary<false>();
            child_idx = bn->find(c);
            if (child_idx >= 0) child = bn->child_at_slot(child_idx);
        } else if (n->is_list()) {
            auto* ln = n->template as_list<false>();
            child_idx = ln->find(c);
            if (child_idx >= 0) child = ln->child_at_slot(child_idx);
        } else if (n->is_pop()) {
            auto* pn = n->template as_pop<false>();
            if (pn->valid().test(c)) {
                child_idx = pn->valid().slot_for(c);
                child = pn->get_child(c);
            }
        } else {
            auto* fn = n->template as_full<false>();
            if (fn->valid().test(c)) {
                child_idx = c;
                child = fn->get_child(c);
            }
        }
        
        stack.push_back({n, out_key.size(), child_idx, has_eos});
        out_key.push_back(static_cast<char>(c));
        
        if (child) {
            n = child;
            continue;
        }
        
        // No exact child, find last child < c
        goto find_prev_sibling;
    }
    
backtrack:
    while (!stack.empty()) {
        frame f = stack.back();
        stack.pop_back();
        out_key.resize(f.key_pos);
        n = f.node;
        
        unsigned char prev_c = 0;
        if (f.key_pos < key.size()) {
            prev_c = static_cast<unsigned char>(key[f.key_pos]);
        }
        
        // Find last child < prev_c
        ptr_t prev_child = nullptr;
        unsigned char pc = 0;
        
        if (n->is_binary()) {
            auto* bn = n->template as_binary<false>();
            for (int i = bn->count() - 1; i >= 0; --i) {
                if (bn->char_at(i) < prev_c) {
                    pc = bn->char_at(i);
                    prev_child = bn->child_at_slot(i);
                    break;
                }
            }
        } else if (n->is_list()) {
            auto* ln = n->template as_list<false>();
            for (int i = ln->count() - 1; i >= 0; --i) {
                if (ln->char_at(i) < prev_c) {
                    pc = ln->char_at(i);
                    prev_child = ln->child_at_slot(i);
                    break;
                }
            }
        } else if (n->is_pop()) {
            auto* pn = n->template as_pop<false>();
            pn->valid().for_each_set([&](unsigned char ch) {
                if (ch < prev_c) { pc = ch; prev_child = pn->get_child(ch); }
            });
        } else {
            auto* fn = n->template as_full<false>();
            for (int c = static_cast<int>(prev_c) - 1; c >= 0; --c) {
                if (fn->valid().test(static_cast<unsigned char>(c))) {
                    pc = static_cast<unsigned char>(c);
                    prev_child = fn->get_child(pc);
                    break;
                }
            }
        }
        
        if (prev_child) {
            out_key.push_back(static_cast<char>(pc));
            n = prev_child;
            goto descend_right;
        }
        
        // No smaller child, check EOS
        if constexpr (FIXED_LEN == 0) {
            if (f.has_eos) {
                n->try_read_eos(out_value);
                return true;
            }
        }
    }
    return false;
    
find_prev_sibling:
    {
        if (stack.empty()) return false;
        frame f = stack.back();
        stack.pop_back();
        out_key.resize(f.key_pos);
        n = f.node;
        
        unsigned char prev_c = static_cast<unsigned char>(key[f.key_pos]);
        ptr_t prev_child = nullptr;
        unsigned char pc = 0;
        
        if (n->is_binary()) {
            auto* bn = n->template as_binary<false>();
            for (int i = bn->count() - 1; i >= 0; --i) {
                if (bn->char_at(i) < prev_c) {
                    pc = bn->char_at(i);
                    prev_child = bn->child_at_slot(i);
                    break;
                }
            }
        } else if (n->is_list()) {
            auto* ln = n->template as_list<false>();
            for (int i = ln->count() - 1; i >= 0; --i) {
                if (ln->char_at(i) < prev_c) {
                    pc = ln->char_at(i);
                    prev_child = ln->child_at_slot(i);
                    break;
                }
            }
        } else if (n->is_pop()) {
            auto* pn = n->template as_pop<false>();
            pn->valid().for_each_set([&](unsigned char ch) {
                if (ch < prev_c) { pc = ch; prev_child = pn->get_child(ch); }
            });
        } else {
            auto* fn = n->template as_full<false>();
            for (int c = static_cast<int>(prev_c) - 1; c >= 0; --c) {
                if (fn->valid().test(static_cast<unsigned char>(c))) {
                    pc = static_cast<unsigned char>(c);
                    prev_child = fn->get_child(pc);
                    break;
                }
            }
        }
        
        if (prev_child) {
            out_key.push_back(static_cast<char>(pc));
            n = prev_child;
            goto descend_right;
        }
        
        // Check EOS
        if constexpr (FIXED_LEN == 0) {
            if (f.has_eos) {
                n->try_read_eos(out_value);
                return true;
            }
        }
        goto backtrack;
    }

descend_right:
    while (n && !n->is_poisoned()) {
        out_key.append(n->skip_str());
        
        if (n->is_leaf()) {
            if (n->is_skip()) {
                n->as_skip()->value.try_read(out_value);
                return true;
            }
            // Last entry
            if (n->is_binary()) {
                auto* bn = n->template as_binary<true>();
                int cnt = bn->count();
                out_key.push_back(static_cast<char>(bn->char_at(cnt - 1)));
                bn->read_value(cnt - 1, out_value);
                return true;
            } else if (n->is_list()) {
                auto* ln = n->template as_list<true>();
                int cnt = ln->count();
                out_key.push_back(static_cast<char>(ln->char_at(cnt - 1)));
                ln->read_value(cnt - 1, out_value);
                return true;
            } else if (n->is_pop()) {
                auto* pn = n->template as_pop<true>();
                unsigned char last_c = 255;
                while (last_c > 0 && !pn->valid().test(last_c)) --last_c;
                out_key.push_back(static_cast<char>(last_c));
                pn->element_at_slot(pn->count() - 1).try_read(out_value);
                return true;
            } else {
                auto* fn = n->template as_full<true>();
                unsigned char last_c = 255;
                while (last_c > 0 && !fn->valid().test(last_c)) --last_c;
                out_key.push_back(static_cast<char>(last_c));
                fn->read_value(last_c, out_value);
                return true;
            }
        }
        
        // Interior: descend to rightmost child
        ptr_t child = nullptr;
        unsigned char last_c = 0;
        if (n->is_binary()) {
            auto* bn = n->template as_binary<false>();
            int cnt = bn->count();
            if (cnt > 0) { last_c = bn->char_at(cnt - 1); child = bn->child_at_slot(cnt - 1); }
        } else if (n->is_list()) {
            auto* ln = n->template as_list<false>();
            int cnt = ln->count();
            if (cnt > 0) { last_c = ln->char_at(cnt - 1); child = ln->child_at_slot(cnt - 1); }
        } else if (n->is_pop()) {
            auto* pn = n->template as_pop<false>();
            if (pn->count() > 0) {
                unsigned char c = 255;
                while (c > 0 && !pn->valid().test(c)) --c;
                last_c = c;
                child = pn->get_child(last_c);
            }
        } else {
            auto* fn = n->template as_full<false>();
            if (fn->count() > 0) {
                unsigned char c = 255;
                while (c > 0 && !fn->valid().test(c)) --c;
                last_c = c;
                child = fn->get_child(last_c);
            }
        }
        
        if (child) {
            out_key.push_back(static_cast<char>(last_c));
            n = child;
        } else if constexpr (FIXED_LEN == 0) {
            if (n->has_eos()) {
                n->try_read_eos(out_value);
                return true;
            }
            return false;
        } else {
            return false;
        }
    }
    return false;
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_insert.h"
