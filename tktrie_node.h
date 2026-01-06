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
// ATOMIC_NODE_PTR - uses atomic storage, defaults to NOT_FOUND sentinel
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
// NODE_BASE - common header for all node types
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct node_base {
    using self_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = self_t*;
    using atomic_ptr = atomic_node_ptr<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = dataptr<T, THREADED, Allocator>;
    using skip_t = skip_string<FIXED_LEN>;
    
    atomic_storage<uint64_t, THREADED> header_;
    
    constexpr node_base() noexcept = default;
    constexpr explicit node_base(uint64_t initial_header) noexcept : header_(initial_header) {}
    
    uint64_t header() const noexcept { return header_.load(); }
    void set_header(uint64_t h) noexcept { header_.store(h); }
    
    bool is_leaf() const noexcept { return gteitelbaum::is_leaf(header()); }
    uint64_t version() const noexcept { return get_version(header()); }
    
    void bump_version() noexcept { header_.store(gteitelbaum::bump_version(header_.load())); }
    void poison() noexcept { header_.store(header_.load() | FLAG_POISON); }
    bool is_poisoned() const noexcept { return is_poisoned_header(header()); }
    
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
    
    std::string_view skip_str() const noexcept {
        if (is_skip()) return as_skip()->skip.view();
        if (is_list()) [[likely]] {
            if (is_leaf()) return as_list<true>()->skip.view();
            return as_list<false>()->skip.view();
        }
        if (is_leaf()) return as_full<true>()->skip.view();
        return as_full<false>()->skip.view();
    }
    
    int child_count() const noexcept {
        if (is_leaf()) return 0;
        if (is_list()) [[likely]] return as_list<false>()->chars.count();
        return as_full<false>()->valid.count();
    }
};

// =============================================================================
// SKIP_NODE - skip string + value (always leaf)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct skip_node : node_base<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    using skip_t = typename base_t::skip_t;
    
    data_t value;
    skip_t skip;
    
    skip_node() = default;
    ~skip_node() = default;
};

// =============================================================================
// LIST_NODE - LEAF specialization (stores values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct list_node<T, THREADED, Allocator, FIXED_LEN, true> : node_base<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    using skip_t = typename base_t::skip_t;
    
    static constexpr int MAX_CHILDREN = 7;
    
    skip_t skip;
    small_list chars;
    std::array<data_t, MAX_CHILDREN> values;
    
    list_node() = default;
    ~list_node() = default;
    
    int add_entry(unsigned char c, const T& val) {
        int idx = chars.add(c);
        values[idx].set(val);
        return idx;
    }
    
    void shift_values_down(int idx) {
        int count = chars.count();
        for (int i = idx; i < count - 1; ++i) {
            values[i] = std::move(values[i + 1]);
        }
        values[count - 1].clear();
        chars.remove_at(idx);
    }
    
    void copy_values_to(list_node* dest) {
        dest->chars = chars;
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) {
            dest->values[i].deep_copy_from(values[i]);
        }
    }
};

// =============================================================================
// LIST_NODE - INTERIOR specialization (stores child pointers)
// FIXED_LEN > 0: no eos (fixed-length keys can't be prefixes)
// FIXED_LEN == 0: has eos_ptr (variable-length keys can be prefixes)
// =============================================================================

// FIXED_LEN > 0: Interior LIST without eos
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct list_node<T, THREADED, Allocator, FIXED_LEN, false> : node_base<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using atomic_ptr = typename base_t::atomic_ptr;
    using skip_t = typename base_t::skip_t;
    
    static constexpr int MAX_CHILDREN = 7;
    
    skip_t skip;
    small_list chars;
    std::array<atomic_ptr, MAX_CHILDREN> children;
    
    list_node() = default;
    ~list_node() = default;
    
    void add_child(unsigned char c, typename base_t::ptr_t child) {
        int idx = chars.add(c);
        children[idx].store(child);
    }
    
    void add_two_children(unsigned char c1, typename base_t::ptr_t child1,
                          unsigned char c2, typename base_t::ptr_t child2) {
        chars.add(c1);
        chars.add(c2);
        children[0].store(child1);
        children[1].store(child2);
    }
    
    void shift_children_down(int idx) {
        int count = chars.count();
        for (int i = idx; i < count - 1; ++i) {
            children[i].store(children[i + 1].load());
        }
        children[count - 1].store(nullptr);
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
    
    void move_interior_to(list_node* dest) {
        dest->chars = chars;
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void move_interior_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest);
};

// FIXED_LEN == 0: Interior LIST with eos_ptr
template <typename T, bool THREADED, typename Allocator>
struct list_node<T, THREADED, Allocator, 0, false> : node_base<T, THREADED, Allocator, 0> {
    using base_t = node_base<T, THREADED, Allocator, 0>;
    using atomic_ptr = typename base_t::atomic_ptr;
    using data_t = typename base_t::data_t;
    using skip_t = typename base_t::skip_t;
    
    static constexpr int MAX_CHILDREN = 7;
    
    data_t eos;              // Value if key terminates here
    skip_t skip;
    small_list chars;
    std::array<atomic_ptr, MAX_CHILDREN> children;
    
    list_node() = default;
    ~list_node() = default;
    
    void add_child(unsigned char c, typename base_t::ptr_t child) {
        int idx = chars.add(c);
        children[idx].store(child);
    }
    
    void add_two_children(unsigned char c1, typename base_t::ptr_t child1,
                          unsigned char c2, typename base_t::ptr_t child2) {
        chars.add(c1);
        chars.add(c2);
        children[0].store(child1);
        children[1].store(child2);
    }
    
    void shift_children_down(int idx) {
        int count = chars.count();
        for (int i = idx; i < count - 1; ++i) {
            children[i].store(children[i + 1].load());
        }
        children[count - 1].store(nullptr);
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
    
    void move_interior_to(list_node* dest) {
        dest->chars = chars;
        dest->eos = std::move(eos);
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void move_interior_to_full(full_node<T, THREADED, Allocator, 0, false>* dest);
};

// =============================================================================
// FULL_NODE - LEAF specialization (stores values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct full_node<T, THREADED, Allocator, FIXED_LEN, true> : node_base<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    using skip_t = typename base_t::skip_t;
    
    skip_t skip;
    bitmap256 valid;
    std::array<data_t, 256> values;
    
    full_node() = default;
    ~full_node() = default;
    
    void add_entry(unsigned char c, const T& val) {
        values[c].set(val);
        valid.set(c);
    }
    
    template <bool THR>
    void add_entry_atomic(unsigned char c, const T& val) {
        values[c].set(val);
        valid.template atomic_set<THR>(c);
    }
    
    template <bool THR>
    void remove_entry(unsigned char c) {
        values[c].clear();
        valid.template atomic_clear<THR>(c);
    }
    
    void copy_values_to(full_node* dest) {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->values[c].deep_copy_from(values[c]);
        });
    }
};

// =============================================================================
// FULL_NODE - INTERIOR specialization (stores child pointers)
// FIXED_LEN > 0: no eos
// FIXED_LEN == 0: has eos_ptr
// =============================================================================

// FIXED_LEN > 0: Interior FULL without eos
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct full_node<T, THREADED, Allocator, FIXED_LEN, false> : node_base<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using atomic_ptr = typename base_t::atomic_ptr;
    using skip_t = typename base_t::skip_t;
    
    skip_t skip;
    bitmap256 valid;
    std::array<atomic_ptr, 256> children;
    
    full_node() = default;
    ~full_node() = default;
    
    void add_child(unsigned char c, typename base_t::ptr_t child) {
        children[c].store(child);
        valid.set(c);
    }
    
    template <bool THR>
    void add_child_atomic(unsigned char c, typename base_t::ptr_t child) {
        children[c].store(child);
        valid.template atomic_set<THR>(c);
    }
    
    template <bool THR>
    void remove_child(unsigned char c) {
        valid.template atomic_clear<THR>(c);
        children[c].store(nullptr);
    }
    
    void move_interior_to(full_node* dest) {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
            children[c].store(nullptr);
        });
    }
};

// FIXED_LEN == 0: Interior FULL with eos
template <typename T, bool THREADED, typename Allocator>
struct full_node<T, THREADED, Allocator, 0, false> : node_base<T, THREADED, Allocator, 0> {
    using base_t = node_base<T, THREADED, Allocator, 0>;
    using atomic_ptr = typename base_t::atomic_ptr;
    using data_t = typename base_t::data_t;
    using skip_t = typename base_t::skip_t;
    
    data_t eos;              // Value if key terminates here
    skip_t skip;
    bitmap256 valid;
    std::array<atomic_ptr, 256> children;
    
    full_node() = default;
    ~full_node() = default;
    
    void add_child(unsigned char c, typename base_t::ptr_t child) {
        children[c].store(child);
        valid.set(c);
    }
    
    template <bool THR>
    void add_child_atomic(unsigned char c, typename base_t::ptr_t child) {
        children[c].store(child);
        valid.template atomic_set<THR>(c);
    }
    
    template <bool THR>
    void remove_child(unsigned char c) {
        valid.template atomic_clear<THR>(c);
        children[c].store(nullptr);
    }
    
    void move_interior_to(full_node* dest) {
        dest->valid = valid;
        dest->eos = std::move(eos);
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
            children[c].store(nullptr);
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

// =============================================================================
// SENTINEL STORAGE TYPES - constinit compatible
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct not_found_list_storage : node_base<T, THREADED, Allocator, FIXED_LEN> {
    skip_string<FIXED_LEN> skip{};
    small_list chars{};
    std::array<void*, 7> dummy_children{};
    
    constexpr not_found_list_storage() noexcept 
        : node_base<T, THREADED, Allocator, FIXED_LEN>(NOT_FOUND_SENTINEL_HEADER) {}
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct retry_full_storage : node_base<T, THREADED, Allocator, FIXED_LEN> {
    skip_string<FIXED_LEN> skip{};
    bitmap256 valid{};
    std::array<void*, 256> dummy_children{};
    
    constexpr retry_full_storage() noexcept 
        : node_base<T, THREADED, Allocator, FIXED_LEN>(RETRY_SENTINEL_HEADER) {}
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
node_base<T, THREADED, Allocator, FIXED_LEN>* get_not_found_sentinel() noexcept {
    constinit static not_found_list_storage<T, THREADED, Allocator, FIXED_LEN> storage{};
    return reinterpret_cast<node_base<T, THREADED, Allocator, FIXED_LEN>*>(&storage);
}

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
node_base<T, THREADED, Allocator, FIXED_LEN>* get_retry_sentinel() noexcept {
    constinit static retry_full_storage<T, THREADED, Allocator, FIXED_LEN> storage{};
    return reinterpret_cast<node_base<T, THREADED, Allocator, FIXED_LEN>*>(&storage);
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
    
    static bool is_sentinel(ptr_t n) noexcept {
        return n == get_not_found_sentinel<T, THREADED, Allocator, FIXED_LEN>() ||
               n == get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>();
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
        
        if (!n->is_leaf()) {
            if (n->is_list()) [[likely]] {
                auto* ln = n->template as_list<false>();
                int cnt = ln->chars.count();
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
                d->chars = s->chars;
                int cnt = s->chars.count();
                for (int i = 0; i < cnt; ++i) {
                    d->values[i].deep_copy_from(s->values[i]);
                }
                return d;
            }
            auto* s = src->template as_full<true>();
            auto* d = new leaf_full_t();
            d->set_header(s->header());
            d->skip = s->skip;
            d->valid = s->valid;
            s->valid.for_each_set([s, d](unsigned char c) {
                d->values[c].deep_copy_from(s->values[c]);
            });
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
            int cnt = s->chars.count();
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
