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
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF> struct list_node;
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF> struct full_node;
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN> class node_builder;

// Forward declare sentinel getters
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
node_base<T, THREADED, Allocator, FIXED_LEN>* get_not_found_sentinel() noexcept;

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
node_base<T, THREADED, Allocator, FIXED_LEN>* get_retry_sentinel() noexcept;

// =============================================================================
// SKIP_STRING - fixed or variable length skip storage
// =============================================================================

template <size_t FIXED_LEN>
struct skip_string {
    std::array<char, FIXED_LEN> data{};
    uint8_t len = 0;
    
    skip_string() = default;
    skip_string(std::string_view sv) : len(static_cast<uint8_t>(sv.size())) {
        std::memcpy(data.data(), sv.data(), sv.size());
    }
    
    std::string_view view() const noexcept { return {data.data(), len}; }
    size_t size() const noexcept { return len; }
    bool empty() const noexcept { return len == 0; }
    char operator[](size_t i) const noexcept { return data[i]; }
    
    void assign(std::string_view sv) {
        len = static_cast<uint8_t>(sv.size());
        std::memcpy(data.data(), sv.data(), sv.size());
    }
    void clear() noexcept { len = 0; }
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
// ATOMIC_NODE_PTR - defaults to NOT_FOUND sentinel
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct atomic_node_ptr {
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = base_t*;
    
    std::atomic<ptr_t> ptr_;
    
    atomic_node_ptr() noexcept : ptr_(get_not_found_sentinel<T, THREADED, Allocator, FIXED_LEN>()) {}
    explicit atomic_node_ptr(ptr_t p) noexcept : ptr_(p) {}
    
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
    void poison() noexcept { header_.store(header_.load() | FLAG_POISON); }
    void unpoison() noexcept { header_.store(header_.load() & ~FLAG_POISON); }
    bool is_poisoned() const noexcept { return is_poisoned_header(header()); }
    
    // Type queries
    bool is_leaf() const noexcept { return gteitelbaum::is_leaf(header()); }
    bool is_skip() const noexcept { return header() & FLAG_SKIP; }
    bool is_list() const noexcept { return header() & FLAG_LIST; }
    bool is_full() const noexcept { return !(header() & (FLAG_SKIP | FLAG_LIST)); }
    
    // Downcasts
    skip_node<T, THREADED, Allocator, FIXED_LEN>* as_skip() noexcept {
        return static_cast<skip_node<T, THREADED, Allocator, FIXED_LEN>*>(this);
    }
    template <bool IS_LEAF>
    list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_list() noexcept {
        return static_cast<list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    template <bool IS_LEAF>
    full_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_full() noexcept {
        return static_cast<full_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    
    const skip_node<T, THREADED, Allocator, FIXED_LEN>* as_skip() const noexcept {
        return static_cast<const skip_node<T, THREADED, Allocator, FIXED_LEN>*>(this);
    }
    template <bool IS_LEAF>
    const list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_list() const noexcept {
        return static_cast<const list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
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
        if (is_list()) [[likely]] {
            return as_list<false>()->get_child(c);
        }
        return as_full<false>()->get_child(c);
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        if (is_list()) [[likely]] {
            return as_list<false>()->get_child_slot(c);
        }
        return as_full<false>()->get_child_slot(c);
    }
    
    int child_count() const noexcept {
        if (is_list()) [[likely]] return as_list<false>()->count();
        return as_full<false>()->count();
    }
    
    // EOS access (interior nodes only, FIXED_LEN==0 only)
    bool has_eos() const noexcept {
        if constexpr (FIXED_LEN > 0) {
            return false;
        } else {
            if (is_list()) [[likely]] return as_list<false>()->eos.has_data();
            return as_full<false>()->eos.has_data();
        }
    }
    
    bool try_read_eos(T& out) const noexcept {
        if constexpr (FIXED_LEN > 0) {
            (void)out;
            return false;
        } else {
            if (is_list()) [[likely]] return as_list<false>()->eos.try_read(out);
            return as_full<false>()->eos.try_read(out);
        }
    }
    
    void set_eos(const T& value) {
        if constexpr (FIXED_LEN > 0) {
            (void)value;
        } else {
            if (is_list()) [[likely]] as_list<false>()->eos.set(value);
            else as_full<false>()->eos.set(value);
        }
    }
    
    void clear_eos() {
        if constexpr (FIXED_LEN > 0) {
            // No EOS for fixed-length keys
        } else {
            if (is_list()) [[likely]] as_list<false>()->eos.clear();
            else as_full<false>()->eos.clear();
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

// =============================================================================
// SKIP_NODE - skip string + single value (always leaf)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct skip_node : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    
    data_t value;
    
    skip_node() = default;
    ~skip_node() = default;
};

// =============================================================================
// LIST_NODE - LEAF specialization (stores values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct list_node<T, THREADED, Allocator, FIXED_LEN, true> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    
    static constexpr int MAX_CHILDREN = 7;
    
    small_list chars;
    std::array<data_t, MAX_CHILDREN> values;
    
    list_node() = default;
    ~list_node() = default;
    
    // Unified interface
    int count() const noexcept { return chars.count(); }
    int find(unsigned char c) const noexcept { return chars.find(c); }
    bool has(unsigned char c) const noexcept { return chars.find(c) >= 0; }
    
    // Caller must verify find(c) >= 0 first
    bool read_value(int idx, T& out) const noexcept {
        return values[idx].try_read(out);
    }
    
    void set_value(unsigned char c, const T& val) {
        int idx = chars.find(c);
        if (idx >= 0) {
            values[idx].set(val);
        } else {
            idx = chars.add(c);
            values[idx].set(val);
        }
    }
    
    int add_value(unsigned char c, const T& val) {
        int idx = chars.add(c);
        values[idx].set(val);
        return idx;
    }
    
    void remove_value(unsigned char c) {
        int idx = chars.find(c);
        if (idx < 0) return;
        int cnt = chars.count();
        for (int i = idx; i < cnt - 1; ++i) {
            values[i] = std::move(values[i + 1]);
        }
        values[cnt - 1].clear();
        chars.remove_at(idx);
    }
    
    void copy_values_to(list_node* dest) const {
        dest->chars = chars;
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) {
            dest->values[i].deep_copy_from(values[i]);
        }
    }
};

// =============================================================================
// LIST_NODE - INTERIOR specialization, FIXED_LEN > 0 (no eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct list_node<T, THREADED, Allocator, FIXED_LEN, false> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    static constexpr int MAX_CHILDREN = 7;
    
    small_list chars;
    std::array<atomic_ptr, MAX_CHILDREN> children;
    
    list_node() = default;
    ~list_node() = default;
    
    // Unified interface
    int count() const noexcept { return chars.count(); }
    bool has(unsigned char c) const noexcept { return chars.find(c) >= 0; }
    
    ptr_t get_child(unsigned char c) const noexcept {
        int idx = chars.find(c);
        return idx >= 0 ? children[idx].load() : nullptr;
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        int idx = chars.find(c);
        return idx >= 0 ? &children[idx] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        int idx = chars.add(c);
        children[idx].store(child);
    }
    
    void add_two_children(unsigned char c1, ptr_t child1, unsigned char c2, ptr_t child2) {
        chars.add(c1);
        chars.add(c2);
        children[0].store(child1);
        children[1].store(child2);
    }
    
    void remove_child(unsigned char c) {
        int idx = chars.find(c);
        if (idx < 0) return;
        int cnt = chars.count();
        for (int i = idx; i < cnt - 1; ++i) {
            children[i].store(children[i + 1].load());
        }
        children[cnt - 1].store(nullptr);
        chars.remove_at(idx);
    }
    
    void move_children_to(list_node* dest) {
        dest->chars = chars;
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void copy_children_to(list_node* dest) const {
        dest->chars = chars;
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
        }
    }
    
    void move_interior_to(list_node* dest) {
        move_children_to(dest);
    }
    
    void copy_interior_to(list_node* dest) const {
        copy_children_to(dest);
    }
    
    void move_interior_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest);
    void copy_interior_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) const;
};

// =============================================================================
// LIST_NODE - INTERIOR specialization, FIXED_LEN == 0 (has eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct list_node<T, THREADED, Allocator, 0, false> 
    : node_with_skip<T, THREADED, Allocator, 0> {
    using base_t = node_with_skip<T, THREADED, Allocator, 0>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using data_t = typename base_t::data_t;
    using eos_data_t = typename base_t::eos_data_t;
    
    static constexpr int MAX_CHILDREN = 7;
    
    eos_data_t eos;
    small_list chars;
    std::array<atomic_ptr, MAX_CHILDREN> children;
    
    list_node() = default;
    ~list_node() = default;
    
    // Unified interface
    int count() const noexcept { return chars.count(); }
    bool has(unsigned char c) const noexcept { return chars.find(c) >= 0; }
    
    ptr_t get_child(unsigned char c) const noexcept {
        int idx = chars.find(c);
        return idx >= 0 ? children[idx].load() : nullptr;
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        int idx = chars.find(c);
        return idx >= 0 ? &children[idx] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        int idx = chars.add(c);
        children[idx].store(child);
    }
    
    void add_two_children(unsigned char c1, ptr_t child1, unsigned char c2, ptr_t child2) {
        chars.add(c1);
        chars.add(c2);
        children[0].store(child1);
        children[1].store(child2);
    }
    
    void remove_child(unsigned char c) {
        int idx = chars.find(c);
        if (idx < 0) return;
        int cnt = chars.count();
        for (int i = idx; i < cnt - 1; ++i) {
            children[i].store(children[i + 1].load());
        }
        children[cnt - 1].store(nullptr);
        chars.remove_at(idx);
    }
    
    void move_children_to(list_node* dest) {
        dest->chars = chars;
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void copy_children_to(list_node* dest) const {
        dest->chars = chars;
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
        }
    }
    
    void move_interior_to(list_node* dest) {
        dest->eos = std::move(eos);
        move_children_to(dest);
    }
    
    void copy_interior_to(list_node* dest) const {
        dest->eos.deep_copy_from(eos);
        copy_children_to(dest);
    }
    
    void move_interior_to_full(full_node<T, THREADED, Allocator, 0, false>* dest);
    void copy_interior_to_full(full_node<T, THREADED, Allocator, 0, false>* dest) const;
};

// =============================================================================
// FULL_NODE - LEAF specialization (stores values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct full_node<T, THREADED, Allocator, FIXED_LEN, true> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    
    bitmap256 valid;
    std::array<data_t, 256> values;
    
    full_node() = default;
    ~full_node() = default;
    
    // Unified interface
    int count() const noexcept { return valid.count(); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }  // Non-atomic read
    
    // Caller must verify has(c) first
    bool read_value(unsigned char c, T& out) const noexcept {
        return values[c].try_read(out);
    }
    
    void set_value(unsigned char c, const T& val) {
        values[c].set(val);
        valid.template atomic_set<THREADED>(c);
    }
    
    void add_value(unsigned char c, const T& val) {
        values[c].set(val);
        valid.set(c);
    }
    
    void add_value_atomic(unsigned char c, const T& val) {
        values[c].set(val);
        valid.template atomic_set<THREADED>(c);
    }
    
    void remove_value(unsigned char c) {
        values[c].clear();
        valid.template atomic_clear<THREADED>(c);
    }
    
    void copy_values_to(full_node* dest) const {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->values[c].deep_copy_from(values[c]);
        });
    }
};

// =============================================================================
// FULL_NODE - INTERIOR specialization, FIXED_LEN > 0 (no eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct full_node<T, THREADED, Allocator, FIXED_LEN, false> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    bitmap256 valid;
    std::array<atomic_ptr, 256> children;
    
    full_node() = default;
    ~full_node() = default;
    
    // Unified interface
    int count() const noexcept { return valid.count(); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }
    
    ptr_t get_child(unsigned char c) const noexcept {
        return children[c].load();
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        return valid.test(c) ? &children[c] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        children[c].store(child);
        valid.set(c);
    }
    
    void add_child_atomic(unsigned char c, ptr_t child) {
        children[c].store(child);
        valid.template atomic_set<THREADED>(c);
    }
    
    void remove_child(unsigned char c) {
        valid.template atomic_clear<THREADED>(c);
        children[c].store(nullptr);
    }
    
    void move_interior_to(full_node* dest) {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
            children[c].store(nullptr);
        });
    }
    
    void copy_interior_to(full_node* dest) const {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
        });
    }
};

// =============================================================================
// FULL_NODE - INTERIOR specialization, FIXED_LEN == 0 (has eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct full_node<T, THREADED, Allocator, 0, false> 
    : node_with_skip<T, THREADED, Allocator, 0> {
    using base_t = node_with_skip<T, THREADED, Allocator, 0>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using data_t = typename base_t::data_t;
    using eos_data_t = typename base_t::eos_data_t;
    
    eos_data_t eos;
    bitmap256 valid;
    std::array<atomic_ptr, 256> children;
    
    full_node() = default;
    ~full_node() = default;
    
    // Unified interface
    int count() const noexcept { return valid.count(); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }
    
    ptr_t get_child(unsigned char c) const noexcept {
        return children[c].load();
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        return valid.test(c) ? &children[c] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        children[c].store(child);
        valid.set(c);
    }
    
    void add_child_atomic(unsigned char c, ptr_t child) {
        children[c].store(child);
        valid.template atomic_set<THREADED>(c);
    }
    
    void remove_child(unsigned char c) {
        valid.template atomic_clear<THREADED>(c);
        children[c].store(nullptr);
    }
    
    void move_interior_to(full_node* dest) {
        dest->eos = std::move(eos);
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
            children[c].store(nullptr);
        });
    }
    
    void copy_interior_to(full_node* dest) const {
        dest->eos.deep_copy_from(eos);
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
        });
    }
};

// Out-of-line definitions for move_interior_to_full
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
void list_node<T, THREADED, Allocator, FIXED_LEN, false>::move_interior_to_full(
    full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) {
    int cnt = chars.count();
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars.char_at(i);
        dest->valid.set(ch);
        dest->children[ch].store(children[i].load());
        children[i].store(nullptr);
    }
}

template <typename T, bool THREADED, typename Allocator>
void list_node<T, THREADED, Allocator, 0, false>::move_interior_to_full(
    full_node<T, THREADED, Allocator, 0, false>* dest) {
    dest->eos = std::move(eos);
    int cnt = chars.count();
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars.char_at(i);
        dest->valid.set(ch);
        dest->children[ch].store(children[i].load());
        children[i].store(nullptr);
    }
}

// Out-of-line definitions for copy_interior_to_full
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
void list_node<T, THREADED, Allocator, FIXED_LEN, false>::copy_interior_to_full(
    full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) const {
    int cnt = chars.count();
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars.char_at(i);
        dest->valid.set(ch);
        dest->children[ch].store(children[i].load());
    }
}

template <typename T, bool THREADED, typename Allocator>
void list_node<T, THREADED, Allocator, 0, false>::copy_interior_to_full(
    full_node<T, THREADED, Allocator, 0, false>* dest) const {
    dest->eos.deep_copy_from(eos);
    int cnt = chars.count();
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars.char_at(i);
        dest->valid.set(ch);
        dest->children[ch].store(children[i].load());
    }
}

// =============================================================================
// SENTINEL STORAGE TYPES - constinit compatible
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct not_found_storage : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    small_list chars{};
    std::array<void*, 7> dummy_children{};
    
    constexpr not_found_storage() noexcept 
        : node_with_skip<T, THREADED, Allocator, FIXED_LEN>(NOT_FOUND_SENTINEL_HEADER) {}
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct retry_storage : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    bitmap256 valid{};
    std::array<void*, 256> dummy_children{};
    
    constexpr retry_storage() noexcept 
        : node_with_skip<T, THREADED, Allocator, FIXED_LEN>(RETRY_SENTINEL_HEADER) {}
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct sentinel_holder {
    static constinit not_found_storage<T, THREADED, Allocator, FIXED_LEN> not_found;
    static constinit retry_storage<T, THREADED, Allocator, FIXED_LEN> retry;
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
constinit not_found_storage<T, THREADED, Allocator, FIXED_LEN> 
    sentinel_holder<T, THREADED, Allocator, FIXED_LEN>::not_found{};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
constinit retry_storage<T, THREADED, Allocator, FIXED_LEN> 
    sentinel_holder<T, THREADED, Allocator, FIXED_LEN>::retry{};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
node_base<T, THREADED, Allocator, FIXED_LEN>* get_not_found_sentinel() noexcept {
    if constexpr (!THREADED) {
        return nullptr;
    } else {
        return reinterpret_cast<node_base<T, THREADED, Allocator, FIXED_LEN>*>(
            &sentinel_holder<T, THREADED, Allocator, FIXED_LEN>::not_found);
    }
}

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
node_base<T, THREADED, Allocator, FIXED_LEN>* get_retry_sentinel() noexcept {
    return reinterpret_cast<node_base<T, THREADED, Allocator, FIXED_LEN>*>(
        &sentinel_holder<T, THREADED, Allocator, FIXED_LEN>::retry);
}

// =============================================================================
// NODE_BUILDER - allocation and type-safe construction
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
class node_builder {
public:
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = base_t*;
    using skip_t = skip_node<T, THREADED, Allocator, FIXED_LEN>;
    using leaf_list_t = list_node<T, THREADED, Allocator, FIXED_LEN, true>;
    using interior_list_t = list_node<T, THREADED, Allocator, FIXED_LEN, false>;
    using leaf_full_t = full_node<T, THREADED, Allocator, FIXED_LEN, true>;
    using interior_full_t = full_node<T, THREADED, Allocator, FIXED_LEN, false>;
    
    // not_found_sentinel: nullptr for non-threaded, actual sentinel for threaded
    // retry_sentinel: threaded-only
    static constexpr bool is_not_found_sentinel(ptr_t n) noexcept {
        if constexpr (!THREADED) {
            return n == nullptr;  // not_found IS nullptr for non-threaded
        } else {
            return n == get_not_found_sentinel<T, THREADED, Allocator, FIXED_LEN>();
        }
    }
    
    static constexpr bool is_retry_sentinel(ptr_t n) noexcept {
        if constexpr (!THREADED) {
            (void)n;
            return false;
        } else {
            return n == get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>();
        }
    }
    
    static constexpr bool is_sentinel(ptr_t n) noexcept {
        if constexpr (!THREADED) {
            return n == nullptr;  // Only not_found (which is nullptr)
        } else {
            return is_not_found_sentinel(n) || is_retry_sentinel(n);
        }
    }
    
    static void delete_node(ptr_t n) {
        if (!n || is_sentinel(n)) return;
        if (n->is_skip()) {
            delete n->as_skip();
        } else if (n->is_list()) [[likely]] {
            if (n->is_leaf()) delete n->template as_list<true>();
            else delete n->template as_list<false>();
        } else {
            if (n->is_leaf()) delete n->template as_full<true>();
            else delete n->template as_full<false>();
        }
    }
    
    ptr_t make_leaf_skip(std::string_view sk, const T& value) {
        auto* n = new skip_t();
        n->set_header(make_header(true, FLAG_SKIP));
        n->skip.assign(sk);
        n->value.set(value);
        return n;
    }
    
    ptr_t make_leaf_list(std::string_view sk) {
        auto* n = new leaf_list_t();
        n->set_header(make_header(true, FLAG_LIST));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_leaf_full(std::string_view sk) {
        auto* n = new leaf_full_t();
        n->set_header(make_header(true, 0));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_list(std::string_view sk) {
        auto* n = new interior_list_t();
        n->set_header(make_header(false, FLAG_LIST));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_full(std::string_view sk) {
        auto* n = new interior_full_t();
        n->set_header(make_header(false, 0));
        n->skip.assign(sk);
        return n;
    }
    
    void dealloc_node(ptr_t n) {
        if (!n || is_sentinel(n)) return;
        
        // If poisoned, this is a speculative node with borrowed children - don't recurse
        if (n->is_poisoned()) {
            delete_node(n);
            return;
        }
        
        if (!n->is_leaf()) {
            if (n->is_list()) [[likely]] {
                auto* ln = n->template as_list<false>();
                int cnt = ln->count();
                for (int i = 0; i < cnt; ++i) {
                    dealloc_node(ln->children[i].load());
                }
            } else {
                auto* fn = n->template as_full<false>();
                fn->valid.for_each_set([this, fn](unsigned char c) {
                    dealloc_node(fn->children[c].load());
                });
            }
        }
        delete_node(n);
    }
    
    ptr_t deep_copy(ptr_t src) {
        if (!src || is_sentinel(src)) return nullptr;
        
        if (src->is_leaf()) {
            if (src->is_skip()) {
                auto* s = src->as_skip();
                auto* d = new skip_t();
                d->set_header(s->header());
                d->skip = s->skip;
                d->value.deep_copy_from(s->value);
                return d;
            }
            if (src->is_list()) [[likely]] {
                auto* s = src->template as_list<true>();
                auto* d = new leaf_list_t();
                d->set_header(s->header());
                d->skip = s->skip;
                s->copy_values_to(d);
                return d;
            }
            auto* s = src->template as_full<true>();
            auto* d = new leaf_full_t();
            d->set_header(s->header());
            d->skip = s->skip;
            s->copy_values_to(d);
            return d;
        }
        
        // Interior
        if (src->is_list()) [[likely]] {
            auto* s = src->template as_list<false>();
            auto* d = new interior_list_t();
            d->set_header(s->header());
            d->skip = s->skip;
            d->chars = s->chars;
            if constexpr (FIXED_LEN == 0) {
                d->eos.deep_copy_from(s->eos);
            }
            int cnt = s->count();
            for (int i = 0; i < cnt; ++i) {
                d->children[i].store(deep_copy(s->children[i].load()));
            }
            return d;
        }
        auto* s = src->template as_full<false>();
        auto* d = new interior_full_t();
        d->set_header(s->header());
        d->skip = s->skip;
        d->valid = s->valid;
        if constexpr (FIXED_LEN == 0) {
            d->eos.deep_copy_from(s->eos);
        }
        s->valid.for_each_set([this, s, d](unsigned char c) {
            d->children[c].store(deep_copy(s->children[c].load()));
        });
        return d;
    }
};

}  // namespace gteitelbaum
