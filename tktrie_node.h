#pragma once

#include <atomic>
#include <array>
#include <memory>
#include <string>
#include "tktrie_defines.h"

namespace gteitelbaum {

// =============================================================================
// FORWARD DECLARATIONS
// =============================================================================

template <typename T, bool THREADED, typename Allocator> struct node_base;
template <typename T, bool THREADED, typename Allocator> struct skip_node;
template <typename T, bool THREADED, typename Allocator, bool IS_LEAF> struct list_node;
template <typename T, bool THREADED, typename Allocator, bool IS_LEAF> struct full_node;
template <typename T, bool THREADED, typename Allocator> class node_builder;

// Forward declare sentinel getters
template <typename T, bool THREADED, typename Allocator>
node_base<T, THREADED, Allocator>* get_not_found_sentinel() noexcept;

template <typename T, bool THREADED, typename Allocator>
node_base<T, THREADED, Allocator>* get_retry_sentinel() noexcept;

// =============================================================================
// ATOMIC_NODE_PTR - uses atomic storage, defaults to NOT_FOUND sentinel
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct atomic_node_ptr {
    using base_t = node_base<T, THREADED, Allocator>;
    using ptr_t = base_t*;
    
    std::atomic<ptr_t> ptr_;
    
    // Default constructor initializes to NOT_FOUND sentinel
    atomic_node_ptr() noexcept : ptr_(get_not_found_sentinel<T, THREADED, Allocator>()) {}
    
    // Explicit constructor for specific pointer
    explicit atomic_node_ptr(ptr_t p) noexcept : ptr_(p) {}
    
    ptr_t load() const noexcept { return ptr_.load(std::memory_order_acquire); }
    void store(ptr_t p) noexcept { ptr_.store(p, std::memory_order_release); }
    ptr_t exchange(ptr_t p) noexcept { return ptr_.exchange(p, std::memory_order_acq_rel); }
};

// =============================================================================
// NODE_BASE - common header for all node types
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct node_base {
    using self_t = node_base<T, THREADED, Allocator>;
    using ptr_t = self_t*;
    using atomic_ptr = atomic_node_ptr<T, THREADED, Allocator>;
    
    atomic_storage<uint64_t, THREADED> header_;
    
    // Constructors
    constexpr node_base() noexcept = default;
    constexpr explicit node_base(uint64_t initial_header) noexcept : header_(initial_header) {}
    
    // Header access
    uint64_t header() const noexcept { return header_.load(); }
    void set_header(uint64_t h) noexcept { header_.store(h); }
    
    // Type queries
    bool is_leaf() const noexcept { return gteitelbaum::is_leaf(header()); }
    uint64_t version() const noexcept { return get_version(header()); }
    
    void bump_version() noexcept {
        header_.store(gteitelbaum::bump_version(header_.load()));
    }
    
    void poison() noexcept {
        header_.store(header_.load() | FLAG_POISON);
    }
    
    bool is_poisoned() const noexcept {
        return is_poisoned_header(header());
    }
    
    bool is_skip() const noexcept { return header() & FLAG_SKIP; }
    bool is_list() const noexcept { return header() & FLAG_LIST; }
    bool is_full() const noexcept { return !(header() & (FLAG_SKIP | FLAG_LIST)); }
    
    // Downcasts - template parameter specifies leaf/interior
    skip_node<T, THREADED, Allocator>* as_skip() noexcept {
        return static_cast<skip_node<T, THREADED, Allocator>*>(this);
    }
    
    template <bool IS_LEAF>
    list_node<T, THREADED, Allocator, IS_LEAF>* as_list() noexcept {
        return static_cast<list_node<T, THREADED, Allocator, IS_LEAF>*>(this);
    }
    
    template <bool IS_LEAF>
    full_node<T, THREADED, Allocator, IS_LEAF>* as_full() noexcept {
        return static_cast<full_node<T, THREADED, Allocator, IS_LEAF>*>(this);
    }
    
    const skip_node<T, THREADED, Allocator>* as_skip() const noexcept {
        return static_cast<const skip_node<T, THREADED, Allocator>*>(this);
    }
    
    template <bool IS_LEAF>
    const list_node<T, THREADED, Allocator, IS_LEAF>* as_list() const noexcept {
        return static_cast<const list_node<T, THREADED, Allocator, IS_LEAF>*>(this);
    }
    
    template <bool IS_LEAF>
    const full_node<T, THREADED, Allocator, IS_LEAF>* as_full() const noexcept {
        return static_cast<const full_node<T, THREADED, Allocator, IS_LEAF>*>(this);
    }
    
    // Common accessors - all node types have skip string
    std::string_view skip_str() const noexcept {
        if (is_skip()) return as_skip()->skip;
        if (is_list()) [[likely]] {
            if (is_leaf()) return as_list<true>()->skip;
            return as_list<false>()->skip;
        }
        if (is_leaf()) return as_full<true>()->skip;
        return as_full<false>()->skip;
    }
    
    int child_count() const noexcept {
        if (is_leaf()) return 0;  // Leaf nodes have entries, not children
        if (is_list()) [[likely]] return as_list<false>()->chars.count();
        return as_full<false>()->valid.count();
    }
};

// =============================================================================
// SKIP_NODE - skip string + value (always leaf)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct skip_node : node_base<T, THREADED, Allocator> {
    T leaf_value;
    std::string skip;
    
    skip_node() = default;
    ~skip_node() {
        leaf_value.~T();
    }
};

// =============================================================================
// LIST_NODE - LEAF specialization (stores values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct list_node<T, THREADED, Allocator, true> : node_base<T, THREADED, Allocator> {
    static constexpr int MAX_CHILDREN = 7;
    
    std::string skip;
    small_list chars;
    std::array<T, MAX_CHILDREN> leaf_values;
    
    list_node() = default;
    ~list_node() {
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) leaf_values[i].~T();
    }
    
    void construct_leaf_value(int idx, const T& value) {
        new (&leaf_values[idx]) T(value);
    }
    
    void destroy_leaf_value(int idx) {
        leaf_values[idx].~T();
    }
    
    int add_leaf_entry(unsigned char c, const T& value) {
        int idx = chars.add(c);
        construct_leaf_value(idx, value);
        return idx;
    }
    
    void shift_leaf_values_down(int idx) {
        int count = chars.count();
        for (int i = idx; i < count - 1; ++i) {
            leaf_values[i] = leaf_values[i + 1];
        }
        destroy_leaf_value(count - 1);
        chars.remove_at(idx);
    }
    
    void copy_leaf_values_to(list_node* dest) {
        dest->chars = chars;
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) {
            dest->construct_leaf_value(i, leaf_values[i]);
        }
    }
};

// =============================================================================
// LIST_NODE - INTERIOR specialization (stores child pointers)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct list_node<T, THREADED, Allocator, false> : node_base<T, THREADED, Allocator> {
    using base_t = node_base<T, THREADED, Allocator>;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    static constexpr int MAX_CHILDREN = 7;
    
    T* eos_ptr;
    std::string skip;
    small_list chars;
    std::array<atomic_ptr, MAX_CHILDREN> children;  // Auto-inits to NOT_FOUND!
    
    list_node() : eos_ptr(nullptr) {}
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
        dest->eos_ptr = eos_ptr;
        eos_ptr = nullptr;
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void move_interior_to_full(full_node<T, THREADED, Allocator, false>* dest);
};

// =============================================================================
// FULL_NODE - LEAF specialization (stores values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct full_node<T, THREADED, Allocator, true> : node_base<T, THREADED, Allocator> {
    std::string skip;
    bitmap256 valid;
    std::array<T, 256> leaf_values;
    
    full_node() = default;
    ~full_node() {
        valid.for_each_set([this](unsigned char c) {
            leaf_values[c].~T();
        });
    }
    
    void construct_leaf_value(unsigned char c, const T& value) {
        new (&leaf_values[c]) T(value);
    }
    
    void destroy_leaf_value(unsigned char c) {
        leaf_values[c].~T();
    }
    
    void add_leaf_entry(unsigned char c, const T& value) {
        construct_leaf_value(c, value);
        valid.set(c);
    }
    
    template <bool THR>
    void add_leaf_entry_atomic(unsigned char c, const T& value) {
        construct_leaf_value(c, value);
        valid.template atomic_set<THR>(c);
    }
    
    template <bool THR>
    void remove_leaf_entry(unsigned char c) {
        destroy_leaf_value(c);
        valid.template atomic_clear<THR>(c);
    }
    
    void copy_leaf_values_to(full_node* dest) {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->construct_leaf_value(c, leaf_values[c]);
        });
    }
};

// =============================================================================
// FULL_NODE - INTERIOR specialization (stores child pointers)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct full_node<T, THREADED, Allocator, false> : node_base<T, THREADED, Allocator> {
    using base_t = node_base<T, THREADED, Allocator>;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    T* eos_ptr;
    std::string skip;
    bitmap256 valid;
    std::array<atomic_ptr, 256> children;  // Auto-inits to NOT_FOUND!
    
    full_node() : eos_ptr(nullptr) {}
    ~full_node() = default;
    
    // Store child BEFORE setting bitmap (memory ordering)
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
        dest->eos_ptr = eos_ptr;
        eos_ptr = nullptr;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
            children[c].store(nullptr);
        });
    }
};

// Out-of-line definition (needs full_node<false> to be complete)
template <typename T, bool THREADED, typename Allocator>
void list_node<T, THREADED, Allocator, false>::move_interior_to_full(
    full_node<T, THREADED, Allocator, false>* dest) {
    dest->eos_ptr = eos_ptr;
    eos_ptr = nullptr;
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

template <typename T, bool THREADED, typename Allocator>
struct not_found_list_storage : node_base<T, THREADED, Allocator> {
    T* eos_ptr = nullptr;
    std::string skip{};
    small_list chars{};
    std::array<void*, 7> dummy_children{};
    
    constexpr not_found_list_storage() noexcept 
        : node_base<T, THREADED, Allocator>(NOT_FOUND_SENTINEL_HEADER) {}
};

template <typename T, bool THREADED, typename Allocator>
struct retry_full_storage : node_base<T, THREADED, Allocator> {
    T* eos_ptr = nullptr;
    std::string skip{};
    bitmap256 valid{};
    std::array<void*, 256> dummy_children{};
    
    constexpr retry_full_storage() noexcept 
        : node_base<T, THREADED, Allocator>(RETRY_SENTINEL_HEADER) {}
};

template <typename T, bool THREADED, typename Allocator>
node_base<T, THREADED, Allocator>* get_not_found_sentinel() noexcept {
    constinit static not_found_list_storage<T, THREADED, Allocator> storage{};
    return reinterpret_cast<node_base<T, THREADED, Allocator>*>(&storage);
}

template <typename T, bool THREADED, typename Allocator>
node_base<T, THREADED, Allocator>* get_retry_sentinel() noexcept {
    constinit static retry_full_storage<T, THREADED, Allocator> storage{};
    return reinterpret_cast<node_base<T, THREADED, Allocator>*>(&storage);
}

// =============================================================================
// NODE_BUILDER - allocation and type-safe construction
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
class node_builder {
public:
    using base_t = node_base<T, THREADED, Allocator>;
    using ptr_t = base_t*;
    using skip_t = skip_node<T, THREADED, Allocator>;
    using leaf_list_t = list_node<T, THREADED, Allocator, true>;
    using interior_list_t = list_node<T, THREADED, Allocator, false>;
    using leaf_full_t = full_node<T, THREADED, Allocator, true>;
    using interior_full_t = full_node<T, THREADED, Allocator, false>;
    
    static bool is_sentinel(ptr_t n) noexcept {
        return n == get_not_found_sentinel<T, THREADED, Allocator>() ||
               n == get_retry_sentinel<T, THREADED, Allocator>();
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
    
    // SKIP is always leaf
    ptr_t make_leaf_skip(std::string_view sk, const T& value) {
        auto* n = new skip_t();
        n->set_header(make_header(true, FLAG_SKIP));
        n->skip = std::string(sk);
        new (&n->leaf_value) T(value);
        return n;
    }
    
    ptr_t make_leaf_list(std::string_view sk) {
        auto* n = new leaf_list_t();
        n->set_header(make_header(true, FLAG_LIST));
        n->skip = std::string(sk);
        return n;
    }
    
    ptr_t make_leaf_full(std::string_view sk) {
        auto* n = new leaf_full_t();
        n->set_header(make_header(true, 0));
        n->skip = std::string(sk);
        return n;
    }
    
    ptr_t make_interior_list(std::string_view sk) {
        auto* n = new interior_list_t();
        n->set_header(make_header(false, FLAG_LIST));
        n->skip = std::string(sk);
        // children auto-initialized to NOT_FOUND by atomic_node_ptr default ctor!
        return n;
    }
    
    ptr_t make_interior_full(std::string_view sk) {
        auto* n = new interior_full_t();
        n->set_header(make_header(false, 0));
        n->skip = std::string(sk);
        // children auto-initialized to NOT_FOUND by atomic_node_ptr default ctor!
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
                delete ln->eos_ptr;
            } else {
                auto* fn = n->template as_full<false>();
                fn->valid.for_each_set([this, fn](unsigned char c) {
                    dealloc_node(fn->children[c].load());
                });
                delete fn->eos_ptr;
            }
        }
        delete_node(n);
    }
    
    ptr_t deep_copy(ptr_t src) {
        if (!src || is_sentinel(src)) return nullptr;
        
        if (src->is_leaf()) {
            if (src->is_skip()) {
                return make_leaf_skip(src->as_skip()->skip, src->as_skip()->leaf_value);
            }
            if (src->is_list()) [[likely]] {
                auto* s = src->template as_list<true>();
                auto* d = new leaf_list_t();
                d->set_header(s->header());
                d->skip = s->skip;
                d->chars = s->chars;
                int cnt = s->chars.count();
                for (int i = 0; i < cnt; ++i) {
                    d->construct_leaf_value(i, s->leaf_values[i]);
                }
                return d;
            }
            auto* s = src->template as_full<true>();
            auto* d = new leaf_full_t();
            d->set_header(s->header());
            d->skip = s->skip;
            d->valid = s->valid;
            s->valid.for_each_set([s, d](unsigned char c) {
                d->construct_leaf_value(c, s->leaf_values[c]);
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
            d->eos_ptr = s->eos_ptr ? new T(*s->eos_ptr) : nullptr;
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
        d->eos_ptr = s->eos_ptr ? new T(*s->eos_ptr) : nullptr;
        s->valid.for_each_set([this, s, d](unsigned char c) {
            d->children[c].store(deep_copy(s->children[c].load()));
        });
        return d;
    }
};

}  // namespace gteitelbaum
