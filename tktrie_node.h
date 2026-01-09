#pragma once

#include <atomic>
#include <array>
#include <memory>
#include <string>
#include "tktrie_defines.h"
#include "tktrie_dataptr.h"

namespace gteitelbaum {

// =============================================================================
// FORWARD DECLARATIONS
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN> struct node_base;
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN> struct node_with_skip;
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN> struct skip_node;
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF> struct binary_node;  // 2 entries
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF> struct list_node;    // 3-7 entries
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF> struct pop_node;     // 8-32 entries
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF> struct full_node;    // 33+ entries
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN> class node_builder;

// Forward declare retry sentinel getter
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
node_base<T, THREADED, Allocator, FIXED_LEN>* get_retry_sentinel() noexcept;

// =============================================================================
// SKIP_STRING - fixed or variable length skip storage
// For FIXED_LEN > 0: stores length in last byte, data in first (FIXED_LEN-1) bytes
// For FIXED_LEN = 0: uses std::string (variable length)
// =============================================================================

template <size_t FIXED_LEN>
struct skip_string {
    // Compact storage: last byte is length, first (FIXED_LEN-1) bytes are data
    // For FIXED_LEN=8: 7 bytes data + 1 byte len = 8 bytes total
    static constexpr size_t MAX_DATA = FIXED_LEN - 1;
    char storage_[FIXED_LEN] = {};
    
    skip_string() = default;
    skip_string(std::string_view sv) { assign(sv); }
    
    void assign(std::string_view sv) {
        size_t n = sv.size() <= MAX_DATA ? sv.size() : MAX_DATA;
        std::memset(storage_, 0, FIXED_LEN);
        std::memcpy(storage_, sv.data(), n);
        storage_[FIXED_LEN - 1] = static_cast<char>(n);
    }
    
    std::string_view view() const noexcept { 
        return {storage_, static_cast<unsigned char>(storage_[FIXED_LEN - 1])}; 
    }
    size_t size() const noexcept { return static_cast<unsigned char>(storage_[FIXED_LEN - 1]); }
    bool empty() const noexcept { return storage_[FIXED_LEN - 1] == 0; }
    char operator[](size_t i) const noexcept { return storage_[i]; }
    void clear() noexcept { std::memset(storage_, 0, FIXED_LEN); }
};

template <>
struct skip_string<0> {
    std::string data;
    
    skip_string() = default;
    skip_string(std::string_view sv) : data(sv) {}
    
    std::string_view view() const noexcept { return data; }
    size_t size() const noexcept { return data.size(); }
    bool empty() const noexcept { return data.empty(); }
    char operator[](size_t i) const noexcept { return data[i]; }
    
    void assign(std::string_view sv) { data = std::string(sv); }
    void clear() { data.clear(); }
};

// =============================================================================
// ATOMIC_NODE_PTR - defaults to nullptr
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct atomic_node_ptr {
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = base_t*;
    
    std::atomic<ptr_t> ptr_;
    
    atomic_node_ptr() noexcept : ptr_(nullptr) {}
    explicit atomic_node_ptr(ptr_t p) noexcept : ptr_(p) {}
    
    // Copy/move for use in generic shift operations
    atomic_node_ptr(const atomic_node_ptr& other) noexcept : ptr_(other.ptr_.load(std::memory_order_acquire)) {}
    atomic_node_ptr& operator=(const atomic_node_ptr& other) noexcept {
        ptr_.store(other.ptr_.load(std::memory_order_acquire), std::memory_order_release);
        return *this;
    }
    atomic_node_ptr(atomic_node_ptr&& other) noexcept : ptr_(other.ptr_.exchange(nullptr, std::memory_order_acq_rel)) {}
    atomic_node_ptr& operator=(atomic_node_ptr&& other) noexcept {
        ptr_.store(other.ptr_.exchange(nullptr, std::memory_order_acq_rel), std::memory_order_release);
        return *this;
    }
    
    ptr_t load() const noexcept { return ptr_.load(std::memory_order_acquire); }
    void store(ptr_t p) noexcept { ptr_.store(p, std::memory_order_release); }
    ptr_t exchange(ptr_t p) noexcept { return ptr_.exchange(p, std::memory_order_acq_rel); }
};

// =============================================================================
// NODE_BASE - header only, type queries and dispatchers
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct node_base {
    using self_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = self_t*;
    using atomic_ptr = atomic_node_ptr<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = dataptr<T, THREADED, Allocator, false>;      // Required values
    using eos_data_t = dataptr<T, THREADED, Allocator, true>;   // Optional EOS values
    using skip_t = skip_string<FIXED_LEN>;
    
    atomic_storage<uint64_t, THREADED> header_;
    
    constexpr node_base() noexcept = default;
    constexpr explicit node_base(uint64_t initial_header) noexcept : header_(initial_header) {}
    
    // Header access
    uint64_t header() const noexcept { return header_.load(); }
    void set_header(uint64_t h) noexcept { header_.store(h); }
    
    // Version and poison
    uint64_t version() const noexcept { return get_version(header()); }
    void bump_version() noexcept { header_.store(gteitelbaum::bump_version(header_.load())); }
    void poison() noexcept {
        // Bump version AND set poison - ensures version check catches poisoned nodes
        uint64_t h = header_.load();
        header_.store(gteitelbaum::bump_version(h) | FLAG_POISON);
    }
    void unpoison() noexcept { header_.store(header_.load() & ~FLAG_POISON); }
    bool is_poisoned() const noexcept { return is_poisoned_header(header()); }
    
    // Type queries (exactly one type flag is set)
    bool is_leaf() const noexcept { return gteitelbaum::is_leaf(header()); }
    bool is_skip() const noexcept { return header() & FLAG_SKIP; }
    bool is_binary() const noexcept { return header() & FLAG_BINARY; }
    bool is_list() const noexcept { return header() & FLAG_LIST; }
    bool is_pop() const noexcept { return header() & FLAG_POP; }
    bool is_full() const noexcept { return header() & FLAG_FULL; }
    
    // Capacity flag queries
    bool at_floor() const noexcept { return is_at_floor(header()); }
    bool at_ceil() const noexcept { return is_at_ceil(header()); }
    bool skip_used() const noexcept { return has_skip_used(header()); }
    bool eos_flag() const noexcept { return has_eos_flag(header()); }
    
    // Flag manipulation (atomic read-modify-write)
    void set_floor() noexcept { header_.store(set_flag(header_.load(), FLAG_IS_FLOOR)); }
    void clear_floor() noexcept { header_.store(clear_flag(header_.load(), FLAG_IS_FLOOR)); }
    void set_ceil() noexcept { header_.store(set_flag(header_.load(), FLAG_IS_CEIL)); }
    void clear_ceil() noexcept { header_.store(clear_flag(header_.load(), FLAG_IS_CEIL)); }
    void set_eos_flag() noexcept { header_.store(set_flag(header_.load(), FLAG_HAS_EOS)); }
    void clear_eos_flag() noexcept { header_.store(clear_flag(header_.load(), FLAG_HAS_EOS)); }
    void set_skip_used_flag() noexcept { header_.store(set_flag(header_.load(), FLAG_SKIP_USED)); }
    void clear_skip_used_flag() noexcept { header_.store(clear_flag(header_.load(), FLAG_SKIP_USED)); }
    
    // Downcasts
    skip_node<T, THREADED, Allocator, FIXED_LEN>* as_skip() noexcept {
        return static_cast<skip_node<T, THREADED, Allocator, FIXED_LEN>*>(this);
    }
    template <bool IS_LEAF>
    binary_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_binary() noexcept {
        return static_cast<binary_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    template <bool IS_LEAF>
    list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_list() noexcept {
        return static_cast<list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    template <bool IS_LEAF>
    pop_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_pop() noexcept {
        return static_cast<pop_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    template <bool IS_LEAF>
    full_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_full() noexcept {
        return static_cast<full_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    
    const skip_node<T, THREADED, Allocator, FIXED_LEN>* as_skip() const noexcept {
        return static_cast<const skip_node<T, THREADED, Allocator, FIXED_LEN>*>(this);
    }
    template <bool IS_LEAF>
    const binary_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_binary() const noexcept {
        return static_cast<const binary_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    template <bool IS_LEAF>
    const list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_list() const noexcept {
        return static_cast<const list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    template <bool IS_LEAF>
    const pop_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_pop() const noexcept {
        return static_cast<const pop_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    template <bool IS_LEAF>
    const full_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_full() const noexcept {
        return static_cast<const full_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    
    // Skip access - all node types have skip via node_with_skip
    std::string_view skip_str() const noexcept {
        return static_cast<const node_with_skip<T, THREADED, Allocator, FIXED_LEN>*>(this)->skip.view();
    }
    
    // =========================================================================
    // DISPATCHERS - is_leaf param allows compiler to eliminate dead branches
    // =========================================================================
    
    // Child access (interior nodes only)
    ptr_t get_child(unsigned char c) const noexcept {
        if (is_binary()) {
            return as_binary<false>()->get_child(c);
        }
        if (is_list()) [[likely]] {
            return as_list<false>()->get_child(c);
        }
        if (is_pop()) {
            return as_pop<false>()->get_child(c);
        }
        return as_full<false>()->get_child(c);
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        if (is_binary()) {
            return as_binary<false>()->get_child_slot(c);
        }
        if (is_list()) [[likely]] {
            return as_list<false>()->get_child_slot(c);
        }
        if (is_pop()) {
            return as_pop<false>()->get_child_slot(c);
        }
        return as_full<false>()->get_child_slot(c);
    }
    
    int child_count() const noexcept {
        if (is_binary()) return as_binary<false>()->count();
        if (is_list()) [[likely]] return as_list<false>()->count();
        if (is_pop()) return as_pop<false>()->count();
        return as_full<false>()->count();
    }
    
    // Entry count for leaf nodes (excludes SKIP which has exactly 1)
    int leaf_entry_count() const noexcept {
        if (is_binary()) return as_binary<true>()->count();
        if (is_list()) [[likely]] return as_list<true>()->count();
        if (is_pop()) return as_pop<true>()->count();
        return as_full<true>()->count();
    }
    
    // Check if interior node has a child at given char
    bool has_child(unsigned char c) const noexcept {
        if (is_binary()) return as_binary<false>()->has(c);
        if (is_list()) [[likely]] return as_list<false>()->has(c);
        if (is_pop()) return as_pop<false>()->has(c);
        return as_full<false>()->has(c);
    }
    
    // Get first child's char and pointer (for collapse operations)
    // Only valid when child_count() == 1
    std::pair<unsigned char, ptr_t> first_child_info() const noexcept {
        if (is_binary()) {
            auto* bn = as_binary<false>();
            return {bn->first_char(), bn->child_at_slot(0)};
        }
        if (is_list()) [[likely]] {
            auto* ln = as_list<false>();
            return {ln->char_at(0), ln->child_at_slot(0)};
        }
        if (is_pop()) {
            auto* pn = as_pop<false>();
            return {pn->first_char(), pn->child_at_slot(0)};
        }
        auto* fn = as_full<false>();
        unsigned char c = fn->valid().first();
        return {c, fn->get_child(c)};
    }
    
    // EOS access (interior nodes only, FIXED_LEN==0 only)
    bool has_eos() const noexcept {
        if constexpr (FIXED_LEN > 0) {
            return false;
        } else {
            return eos_flag();  // Use cached flag instead of pointer check
        }
    }
    
    bool try_read_eos(T& out) const noexcept {
        if constexpr (FIXED_LEN > 0) {
            (void)out;
            return false;
        } else {
            if (!eos_flag()) return false;  // Fast path: check flag first
            if (is_binary()) return as_binary<false>()->eos().try_read(out);
            if (is_list()) [[likely]] return as_list<false>()->eos().try_read(out);
            if (is_pop()) return as_pop<false>()->eos().try_read(out);
            return as_full<false>()->eos().try_read(out);
        }
    }
    
    void set_eos(const T& value) {
        if constexpr (FIXED_LEN > 0) {
            (void)value;
        } else {
            if (is_binary()) as_binary<false>()->eos().set(value);
            else if (is_list()) [[likely]] as_list<false>()->eos().set(value);
            else if (is_pop()) as_pop<false>()->eos().set(value);
            else as_full<false>()->eos().set(value);
            set_eos_flag();  // Update header flag
        }
    }
    
    void clear_eos() {
        if constexpr (FIXED_LEN > 0) {
            // No EOS for fixed-length keys
        } else {
            if (is_binary()) as_binary<false>()->eos().clear();
            else if (is_list()) [[likely]] as_list<false>()->eos().clear();
            else if (is_pop()) as_pop<false>()->eos().clear();
            else as_full<false>()->eos().clear();
            clear_eos_flag();  // Update header flag
        }
    }
    
    // Leaf value access (leaf nodes only, not SKIP)
    bool has_leaf_entry(unsigned char c) const noexcept {
        if (is_binary()) return as_binary<true>()->has(c);
        if (is_list()) [[likely]] return as_list<true>()->has(c);
        if (is_pop()) return as_pop<true>()->has(c);
        return as_full<true>()->has(c);
    }
    
    bool try_read_leaf_value(unsigned char c, T& out) const noexcept {
        if (is_binary()) {
            auto* bn = as_binary<true>();
            int idx = bn->find(c);
            if (idx < 0) return false;
            return bn->read_value(idx, out);
        }
        if (is_list()) [[likely]] {
            auto* ln = as_list<true>();
            int idx = ln->find(c);
            if (idx < 0) return false;
            return ln->read_value(idx, out);
        }
        if (is_pop()) {
            return as_pop<true>()->read_value(c, out);
        }
        auto* fn = as_full<true>();
        if (!fn->has(c)) return false;
        return fn->read_value(c, out);
    }
    
    // Iterate over all leaf entries, calling fn(char, value) for each
    // Only valid for leaf nodes (not SKIP)
    template <typename Fn>
    void for_each_leaf_entry(Fn&& fn) const {
        if (is_binary()) {
            auto* bn = as_binary<true>();
            for (int i = 0; i < bn->count(); ++i) {
                T val{};
                bn->value_at(i).try_read(val);
                fn(bn->char_at(i), val);
            }
        } else if (is_list()) [[likely]] {
            auto* ln = as_list<true>();
            int cnt = ln->count();
            for (int i = 0; i < cnt; ++i) {
                T val{};
                ln->value_at(i).try_read(val);
                fn(ln->char_at(i), val);
            }
        } else if (is_pop()) {
            auto* pn = as_pop<true>();
            int slot = 0;
            pn->valid().for_each_set([pn, &fn, &slot](unsigned char c) {
                T val{};
                pn->element_at_slot(slot).try_read(val);
                fn(c, val);
                ++slot;
            });
        } else {
            auto* fn_node = as_full<true>();
            fn_node->valid().for_each_set([fn_node, &fn](unsigned char c) {
                T val{};
                fn_node->read_value(c, val);
                fn(c, val);
            });
        }
    }
};

// =============================================================================
// NODE_WITH_SKIP - intermediate base with skip field first
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct node_with_skip : node_base<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using skip_t = typename base_t::skip_t;
    
    skip_t skip;
    
    constexpr node_with_skip() noexcept = default;
    constexpr explicit node_with_skip(uint64_t h) noexcept : base_t(h) {}
};

}  // namespace gteitelbaum

#include "tktrie_node_types.h"
