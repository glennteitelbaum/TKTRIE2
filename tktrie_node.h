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
    
    // Type queries
    bool is_leaf() const noexcept { return gteitelbaum::is_leaf(header()); }
    bool is_skip() const noexcept { return header() & FLAG_SKIP; }
    bool is_binary() const noexcept { return header() & FLAG_BINARY; }
    bool is_list() const noexcept { return header() & FLAG_LIST; }
    bool is_pop() const noexcept { return header() & FLAG_POP; }
    bool is_full() const noexcept { return !(header() & TYPE_FLAGS_MASK); }
    
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
    
    // EOS access (interior nodes only, FIXED_LEN==0 only)
    bool has_eos() const noexcept {
        if constexpr (FIXED_LEN > 0) {
            return false;
        } else {
            if (is_binary()) return as_binary<false>()->eos.has_data();
            if (is_list()) [[likely]] return as_list<false>()->eos.has_data();
            if (is_pop()) return as_pop<false>()->eos.has_data();
            return as_full<false>()->eos.has_data();
        }
    }
    
    bool try_read_eos(T& out) const noexcept {
        if constexpr (FIXED_LEN > 0) {
            (void)out;
            return false;
        } else {
            if (is_binary()) return as_binary<false>()->eos.try_read(out);
            if (is_list()) [[likely]] return as_list<false>()->eos.try_read(out);
            if (is_pop()) return as_pop<false>()->eos.try_read(out);
            return as_full<false>()->eos.try_read(out);
        }
    }
    
    void set_eos(const T& value) {
        if constexpr (FIXED_LEN > 0) {
            (void)value;
        } else {
            if (is_binary()) as_binary<false>()->eos.set(value);
            else if (is_list()) [[likely]] as_list<false>()->eos.set(value);
            else if (is_pop()) as_pop<false>()->eos.set(value);
            else as_full<false>()->eos.set(value);
        }
    }
    
    void clear_eos() {
        if constexpr (FIXED_LEN > 0) {
            // No EOS for fixed-length keys
        } else {
            if (is_binary()) as_binary<false>()->eos.clear();
            else if (is_list()) [[likely]] as_list<false>()->eos.clear();
            else if (is_pop()) as_pop<false>()->eos.clear();
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
// BINARY_NODE - LEAF specialization (stores 2 values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct binary_node<T, THREADED, Allocator, FIXED_LEN, true> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    
    static constexpr int MAX_ENTRIES = 2;
    
    unsigned char chars[2] = {};
    int count_ = 0;
    std::array<data_t, 2> values;
    
    binary_node() = default;
    ~binary_node() = default;
    
    int count() const noexcept { return count_; }
    bool has(unsigned char c) const noexcept { 
        return (count_ > 0 && chars[0] == c) || (count_ > 1 && chars[1] == c); 
    }
    
    int find(unsigned char c) const noexcept {
        if (count_ > 0 && chars[0] == c) return 0;
        if (count_ > 1 && chars[1] == c) return 1;
        return -1;
    }
    
    void add_entry(unsigned char c, const T& value) {
        chars[count_] = c;
        values[count_].set(value);
        ++count_;
    }
    
    void remove_entry(int idx) {
        if (idx == 0 && count_ == 2) {
            chars[0] = chars[1];
            values[0] = std::move(values[1]);
        }
        --count_;
    }
    
    void copy_values_to(binary_node* dest) const {
        dest->count_ = count_;
        for (int i = 0; i < count_; ++i) {
            dest->chars[i] = chars[i];
            dest->values[i].deep_copy_from(values[i]);
        }
    }
};

// =============================================================================
// BINARY_NODE - INTERIOR specialization, FIXED_LEN > 0 (no eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
    requires (FIXED_LEN > 0)
struct binary_node<T, THREADED, Allocator, FIXED_LEN, false> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    static constexpr int MAX_CHILDREN = 2;
    
    unsigned char chars[2] = {};
    int count_ = 0;
    std::array<atomic_ptr, 2> children;
    
    binary_node() = default;
    ~binary_node() = default;
    
    int count() const noexcept { return count_; }
    bool has(unsigned char c) const noexcept { 
        return (count_ > 0 && chars[0] == c) || (count_ > 1 && chars[1] == c); 
    }
    
    int find(unsigned char c) const noexcept {
        if (count_ > 0 && chars[0] == c) return 0;
        if (count_ > 1 && chars[1] == c) return 1;
        return -1;
    }
    
    ptr_t get_child(unsigned char c) const noexcept {
        int idx = find(c);
        return idx >= 0 ? children[idx].load() : nullptr;
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        int idx = find(c);
        return idx >= 0 ? &children[idx] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        chars[count_] = c;
        children[count_].store(child);
        ++count_;
    }
    
    void remove_child(int idx) {
        if (idx == 0 && count_ == 2) {
            chars[0] = chars[1];
            children[0].store(children[1].load());
        }
        children[count_ - 1].store(nullptr);
        --count_;
    }
    
    unsigned char first_char() const noexcept { return chars[0]; }
    ptr_t child_at_slot(int slot) const noexcept { return children[slot].load(); }
    
    void move_children_to(binary_node* dest) {
        dest->count_ = count_;
        for (int i = 0; i < count_; ++i) {
            dest->chars[i] = chars[i];
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
        count_ = 0;
    }
    
    void copy_children_to(binary_node* dest) const {
        dest->count_ = count_;
        for (int i = 0; i < count_; ++i) {
            dest->chars[i] = chars[i];
            dest->children[i].store(children[i].load());
        }
    }
    
    void copy_interior_to(binary_node* dest) const {
        copy_children_to(dest);
    }
};

// =============================================================================
// BINARY_NODE - INTERIOR specialization, FIXED_LEN == 0 (has eos)
// Entry count = child_count + (has_eos ? 1 : 0), supports 1-2 entries
// Examples: 1 child + EOS = 2, 2 children = 2, 1 child alone = 1
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct binary_node<T, THREADED, Allocator, 0, false> 
    : node_with_skip<T, THREADED, Allocator, 0> {
    using base_t = node_with_skip<T, THREADED, Allocator, 0>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using eos_data_t = typename base_t::eos_data_t;
    
    static constexpr int MAX_CHILDREN = 2;
    
    eos_data_t eos;
    unsigned char chars[2] = {};
    int count_ = 0;  // child count (0-2), entry count = count_ + eos.has_data()
    std::array<atomic_ptr, 2> children;
    
    binary_node() = default;
    ~binary_node() = default;
    
    int count() const noexcept { return count_; }  // child count
    int entry_count() const noexcept { return count_ + (eos.has_data() ? 1 : 0); }
    
    bool has(unsigned char c) const noexcept { 
        return (count_ > 0 && chars[0] == c) || (count_ > 1 && chars[1] == c); 
    }
    
    int find(unsigned char c) const noexcept {
        if (count_ > 0 && chars[0] == c) return 0;
        if (count_ > 1 && chars[1] == c) return 1;
        return -1;
    }
    
    ptr_t get_child(unsigned char c) const noexcept {
        int idx = find(c);
        return idx >= 0 ? children[idx].load() : nullptr;
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        int idx = find(c);
        return idx >= 0 ? &children[idx] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        chars[count_] = c;
        children[count_].store(child);
        ++count_;
    }
    
    void remove_child(int idx) {
        if (idx == 0 && count_ == 2) {
            chars[0] = chars[1];
            children[0].store(children[1].load());
        }
        children[count_ - 1].store(nullptr);
        --count_;
    }
    
    unsigned char first_char() const noexcept { return chars[0]; }
    ptr_t child_at_slot(int slot) const noexcept { return children[slot].load(); }
    
    void move_children_to(binary_node* dest) {
        dest->count_ = count_;
        for (int i = 0; i < count_; ++i) {
            dest->chars[i] = chars[i];
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
        count_ = 0;
    }
    
    void copy_children_to(binary_node* dest) const {
        dest->count_ = count_;
        for (int i = 0; i < count_; ++i) {
            dest->chars[i] = chars[i];
            dest->children[i].store(children[i].load());
        }
    }
    
    void move_interior_to(binary_node* dest) {
        dest->eos = std::move(eos);
        move_children_to(dest);
    }
    
    void copy_interior_to(binary_node* dest) const {
        dest->eos.deep_copy_from(eos);
        copy_children_to(dest);
    }
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
    
    small_list<THREADED> chars;
    std::array<data_t, MAX_CHILDREN> values;
    
    list_node() = default;
    ~list_node() = default;
    
    // Unified interface
    int count() const noexcept { return chars.count(); }
    int find(unsigned char c) const noexcept { return chars.find(c); }
    bool has(unsigned char c) const noexcept { return chars.find(c) >= 0; }
    
    // Caller must verify find(c) >= 0 first
    bool read_value(int idx, T& out) const noexcept {
        [[assume(idx >= 0 && idx < 7)]];
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
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = idx; i < cnt - 1; ++i) {
            values[i] = std::move(values[i + 1]);
        }
        values[cnt - 1].clear();
        chars.remove_at(idx);
    }
    
    void copy_values_to(list_node* dest) const {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
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
    
    small_list<THREADED> chars;
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
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = idx; i < cnt - 1; ++i) {
            children[i].store(children[i + 1].load());
        }
        children[cnt - 1].store(nullptr);
        chars.remove_at(idx);
    }
    
    void move_children_to(list_node* dest) {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void copy_children_to(list_node* dest) const {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
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
    small_list<THREADED> chars;
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
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = idx; i < cnt - 1; ++i) {
            children[i].store(children[i + 1].load());
        }
        children[cnt - 1].store(nullptr);
        chars.remove_at(idx);
    }
    
    void move_children_to(list_node* dest) {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void copy_children_to(list_node* dest) const {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
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
// POP_NODE - LEAF specialization (8-32 values using popcount indexing)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct pop_node<T, THREADED, Allocator, FIXED_LEN, true> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    
    static constexpr int MAX_ENTRIES = POP_MAX;  // 32
    
    bitmap256 valid;
    std::array<data_t, MAX_ENTRIES> values;
    
    pop_node() = default;
    ~pop_node() = default;
    
    // Get slot index for character c using popcount
    int slot_for(unsigned char c) const noexcept {
        int word = c >> 6;
        int bit = c & 63;
        int slot = 0;
        for (int w = 0; w < word; ++w) {
            slot += std::popcount(valid.word(w));
        }
        uint64_t mask = (1ULL << bit) - 1;
        slot += std::popcount(valid.word(word) & mask);
        return slot;
    }
    
    int count() const noexcept { return valid.count(); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }
    
    int find(unsigned char c) const noexcept {
        return valid.test(c) ? slot_for(c) : -1;
    }
    
    bool read_value(unsigned char c, T& out) const noexcept {
        if (!valid.test(c)) return false;
        return values[slot_for(c)].try_read(out);
    }
    
    void add_value(unsigned char c, const T& val) {
        int slot = slot_for(c);
        int cnt = count();
        // Shift values up to make room
        for (int i = cnt; i > slot; --i) {
            values[i] = std::move(values[i-1]);
        }
        values[slot].set(val);
        valid.set(c);
    }
    
    void remove_value(unsigned char c) {
        int slot = slot_for(c);
        valid.clear(c);
        int new_count = count();
        // Shift values down
        for (int i = slot; i < new_count; ++i) {
            values[i] = std::move(values[i+1]);
        }
        values[new_count].clear();
    }
    
    void copy_values_to(pop_node* dest) const {
        dest->valid = valid;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->values[i].deep_copy_from(values[i]);
        }
    }
    
    unsigned char first_char() const noexcept { return valid.first(); }
};

// =============================================================================
// POP_NODE - INTERIOR specialization, FIXED_LEN > 0 (no eos)
// 8-32 children using popcount indexing
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
    requires (FIXED_LEN > 0)
struct pop_node<T, THREADED, Allocator, FIXED_LEN, false> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    static constexpr int MAX_CHILDREN = POP_MAX;  // 32
    
    bitmap256 valid;
    std::array<atomic_ptr, MAX_CHILDREN> children;
    
    pop_node() = default;
    ~pop_node() = default;
    
    // Get slot index for character c using popcount
    int slot_for(unsigned char c) const noexcept {
        int word = c >> 6;
        int bit = c & 63;
        int slot = 0;
        for (int w = 0; w < word; ++w) {
            slot += std::popcount(valid.word(w));
        }
        uint64_t mask = (1ULL << bit) - 1;
        slot += std::popcount(valid.word(word) & mask);
        return slot;
    }
    
    int count() const noexcept { return valid.count(); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }
    
    ptr_t get_child(unsigned char c) const noexcept {
        if (!valid.test(c)) return nullptr;
        return children[slot_for(c)].load();
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        if (!valid.test(c)) return nullptr;
        return &children[slot_for(c)];
    }
    
    void add_child(unsigned char c, ptr_t child) {
        int slot = slot_for(c);
        int cnt = count();
        for (int i = cnt; i > slot; --i) {
            children[i].store(children[i-1].load());
        }
        children[slot].store(child);
        valid.set(c);
    }
    
    void remove_child(unsigned char c) {
        int slot = slot_for(c);
        valid.clear(c);
        int new_count = count();
        for (int i = slot; i < new_count; ++i) {
            children[i].store(children[i+1].load());
        }
        children[new_count].store(nullptr);
    }
    
    void move_children_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) {
        int idx = 0;
        valid.for_each_set([&](unsigned char c) {
            dest->valid.set(c);
            dest->children[c].store(children[idx].load());
            children[idx].store(nullptr);
            ++idx;
        });
    }
    
    void copy_children_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) const {
        int idx = 0;
        valid.for_each_set([&](unsigned char c) {
            dest->valid.set(c);
            dest->children[c].store(children[idx].load());
            ++idx;
        });
    }
    
    void move_children_to(pop_node* dest) {
        dest->valid = valid;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void copy_children_to(pop_node* dest) const {
        dest->valid = valid;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
        }
    }
    
    void copy_interior_to(pop_node* dest) const {
        copy_children_to(dest);
    }
    
    unsigned char first_char() const noexcept { return valid.first(); }
    ptr_t child_at_slot(int slot) const noexcept { return children[slot].load(); }
};

// =============================================================================
// POP_NODE - INTERIOR specialization, FIXED_LEN == 0 (has eos)
// Entry count = child_count + (has_eos ? 1 : 0), supports 8-32 entries
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct pop_node<T, THREADED, Allocator, 0, false> 
    : node_with_skip<T, THREADED, Allocator, 0> {
    using base_t = node_with_skip<T, THREADED, Allocator, 0>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using eos_data_t = typename base_t::eos_data_t;
    
    static constexpr int MAX_CHILDREN = POP_MAX;  // 32
    
    eos_data_t eos;
    bitmap256 valid;
    std::array<atomic_ptr, MAX_CHILDREN> children;
    
    pop_node() = default;
    ~pop_node() = default;
    
    int slot_for(unsigned char c) const noexcept {
        int word = c >> 6;
        int bit = c & 63;
        int slot = 0;
        for (int w = 0; w < word; ++w) {
            slot += std::popcount(valid.word(w));
        }
        uint64_t mask = (1ULL << bit) - 1;
        slot += std::popcount(valid.word(word) & mask);
        return slot;
    }
    
    int count() const noexcept { return valid.count(); }  // child count
    int entry_count() const noexcept { return count() + (eos.has_data() ? 1 : 0); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }
    
    ptr_t get_child(unsigned char c) const noexcept {
        if (!valid.test(c)) return nullptr;
        return children[slot_for(c)].load();
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        if (!valid.test(c)) return nullptr;
        return &children[slot_for(c)];
    }
    
    void add_child(unsigned char c, ptr_t child) {
        int slot = slot_for(c);
        int cnt = count();
        for (int i = cnt; i > slot; --i) {
            children[i].store(children[i-1].load());
        }
        children[slot].store(child);
        valid.set(c);
    }
    
    void remove_child(unsigned char c) {
        int slot = slot_for(c);
        valid.clear(c);
        int new_count = count();
        for (int i = slot; i < new_count; ++i) {
            children[i].store(children[i+1].load());
        }
        children[new_count].store(nullptr);
    }
    
    void move_children_to_full(full_node<T, THREADED, Allocator, 0, false>* dest) {
        int idx = 0;
        valid.for_each_set([&](unsigned char c) {
            dest->valid.set(c);
            dest->children[c].store(children[idx].load());
            children[idx].store(nullptr);
            ++idx;
        });
    }
    
    void copy_children_to_full(full_node<T, THREADED, Allocator, 0, false>* dest) const {
        int idx = 0;
        valid.for_each_set([&](unsigned char c) {
            dest->valid.set(c);
            dest->children[c].store(children[idx].load());
            ++idx;
        });
    }
    
    void move_interior_to(pop_node* dest) {
        dest->eos = std::move(eos);
        dest->valid = valid;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void copy_interior_to(pop_node* dest) const {
        dest->eos.deep_copy_from(eos);
        dest->valid = valid;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
        }
    }
    
    // Alias for code that doesn't need to move eos
    void move_children_to(pop_node* dest) {
        dest->valid = valid;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    unsigned char first_char() const noexcept { return valid.first(); }
    ptr_t child_at_slot(int slot) const noexcept { return children[slot].load(); }
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
        [[assume(cnt >= 0 && cnt < 8)]];
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
        [[assume(cnt >= 0 && cnt < 8)]];
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
        [[assume(cnt >= 0 && cnt < 8)]];
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
        [[assume(cnt >= 0 && cnt < 8)]];
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars.char_at(i);
        dest->valid.set(ch);
        dest->children[ch].store(children[i].load());
    }
}

// =============================================================================
// RETRY SENTINEL STORAGE - constinit compatible
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct retry_storage : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    bitmap256 valid{};
    std::array<void*, 256> dummy_children{};
    
    constexpr retry_storage() noexcept 
        : node_with_skip<T, THREADED, Allocator, FIXED_LEN>(RETRY_SENTINEL_HEADER) {}
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct sentinel_holder {
    static constinit retry_storage<T, THREADED, Allocator, FIXED_LEN> retry;
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
constinit retry_storage<T, THREADED, Allocator, FIXED_LEN> 
    sentinel_holder<T, THREADED, Allocator, FIXED_LEN>::retry{};

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
    using leaf_binary_t = binary_node<T, THREADED, Allocator, FIXED_LEN, true>;
    using interior_binary_t = binary_node<T, THREADED, Allocator, FIXED_LEN, false>;
    using leaf_list_t = list_node<T, THREADED, Allocator, FIXED_LEN, true>;
    using interior_list_t = list_node<T, THREADED, Allocator, FIXED_LEN, false>;
    using leaf_pop_t = pop_node<T, THREADED, Allocator, FIXED_LEN, true>;
    using interior_pop_t = pop_node<T, THREADED, Allocator, FIXED_LEN, false>;
    using leaf_full_t = full_node<T, THREADED, Allocator, FIXED_LEN, true>;
    using interior_full_t = full_node<T, THREADED, Allocator, FIXED_LEN, false>;
    
    static constexpr bool is_retry_sentinel(ptr_t n) noexcept {
        if constexpr (!THREADED) {
            (void)n;
            return false;
        } else {
            return n == get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>();
        }
    }
    
    // is_sentinel is now just is_retry_sentinel (no more not_found sentinel)
    static constexpr bool is_sentinel(ptr_t n) noexcept {
        return is_retry_sentinel(n);
    }
    
    static void delete_node(ptr_t n) {
        if (!n || is_sentinel(n)) return;
        if (n->is_skip()) {
            delete n->as_skip();
        } else if (n->is_binary()) {
            if (n->is_leaf()) delete n->template as_binary<true>();
            else delete n->template as_binary<false>();
        } else if (n->is_list()) [[likely]] {
            if (n->is_leaf()) delete n->template as_list<true>();
            else delete n->template as_list<false>();
        } else if (n->is_pop()) {
            if (n->is_leaf()) delete n->template as_pop<true>();
            else delete n->template as_pop<false>();
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
    
    ptr_t make_leaf_binary(std::string_view sk) {
        auto* n = new leaf_binary_t();
        n->set_header(make_header(true, FLAG_BINARY));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_leaf_list(std::string_view sk) {
        auto* n = new leaf_list_t();
        n->set_header(make_header(true, FLAG_LIST));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_leaf_pop(std::string_view sk) {
        auto* n = new leaf_pop_t();
        n->set_header(make_header(true, FLAG_POP));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_leaf_full(std::string_view sk) {
        auto* n = new leaf_full_t();
        n->set_header(make_header(true, 0));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_binary(std::string_view sk) {
        auto* n = new interior_binary_t();
        n->set_header(make_header(false, FLAG_BINARY));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_list(std::string_view sk) {
        auto* n = new interior_list_t();
        n->set_header(make_header(false, FLAG_LIST));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_pop(std::string_view sk) {
        auto* n = new interior_pop_t();
        n->set_header(make_header(false, FLAG_POP));
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
            if (n->is_binary()) {
                auto* bn = n->template as_binary<false>();
                int cnt = bn->count();
                for (int i = 0; i < cnt; ++i) {
                    dealloc_node(bn->children[i].load());
                }
            } else if (n->is_list()) [[likely]] {
                auto* ln = n->template as_list<false>();
                int cnt = ln->count();
                for (int i = 0; i < cnt; ++i) {
                    dealloc_node(ln->children[i].load());
                }
            } else if (n->is_pop()) {
                auto* pn = n->template as_pop<false>();
                int cnt = pn->count();
                for (int i = 0; i < cnt; ++i) {
                    dealloc_node(pn->children[i].load());
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
            if (src->is_binary()) {
                auto* s = src->template as_binary<true>();
                auto* d = new leaf_binary_t();
                d->set_header(s->header());
                d->skip = s->skip;
                s->copy_values_to(d);
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
            if (src->is_pop()) {
                auto* s = src->template as_pop<true>();
                auto* d = new leaf_pop_t();
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
        if (src->is_binary()) {
            auto* s = src->template as_binary<false>();
            auto* d = new interior_binary_t();
            d->set_header(s->header());
            d->skip = s->skip;
            if constexpr (FIXED_LEN == 0) {
                d->eos.deep_copy_from(s->eos);
            }
            s->copy_children_to(d);
            for (int i = 0; i < d->count_; ++i) {
                d->children[i].store(deep_copy(d->children[i].load()));
            }
            return d;
        }
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
        if (src->is_pop()) {
            auto* s = src->template as_pop<false>();
            auto* d = new interior_pop_t();
            d->set_header(s->header());
            d->skip = s->skip;
            d->valid = s->valid;
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
