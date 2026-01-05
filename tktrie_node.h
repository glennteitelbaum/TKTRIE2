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
template <typename T, bool THREADED, typename Allocator> struct eos_node;
template <typename T, bool THREADED, typename Allocator> struct skip_node;
template <typename T, bool THREADED, typename Allocator> struct list_node;
template <typename T, bool THREADED, typename Allocator> struct full_node;
template <typename T, bool THREADED, typename Allocator> class node_builder;

// =============================================================================
// ATOMIC_NODE_PTR - uses atomic_storage internally
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct atomic_node_ptr {
    using base_t = node_base<T, THREADED, Allocator>;
    using ptr_t = base_t*;
    
    atomic_storage<ptr_t, THREADED> ptr_;
    
    ptr_t load() const noexcept { return ptr_.load(); }
    void store(ptr_t p) noexcept { ptr_.store(p); }
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
    
    // Header access - simplified using atomic_storage
    uint64_t header() const noexcept { return header_.load(); }
    void set_header(uint64_t h) noexcept { header_.store(h); }
    
    // Type queries
    bool is_leaf() const noexcept { return gteitelbaum::is_leaf(header()); }
    uint64_t type() const noexcept { return get_type(header()); }
    uint64_t version() const noexcept { return get_version(header()); }
    
    void bump_version() noexcept {
        header_.store(gteitelbaum::bump_version(header_.load()));
    }
    
    bool is_eos() const noexcept { return type() == TYPE_EOS; }
    bool is_skip() const noexcept { return type() == TYPE_SKIP; }
    bool is_list() const noexcept { return type() == TYPE_LIST; }
    bool is_full() const noexcept { return type() == TYPE_FULL; }
    
    // Downcasts
    eos_node<T, THREADED, Allocator>* as_eos() noexcept {
        return static_cast<eos_node<T, THREADED, Allocator>*>(this);
    }
    skip_node<T, THREADED, Allocator>* as_skip() noexcept {
        return static_cast<skip_node<T, THREADED, Allocator>*>(this);
    }
    list_node<T, THREADED, Allocator>* as_list() noexcept {
        return static_cast<list_node<T, THREADED, Allocator>*>(this);
    }
    full_node<T, THREADED, Allocator>* as_full() noexcept {
        return static_cast<full_node<T, THREADED, Allocator>*>(this);
    }
    
    const eos_node<T, THREADED, Allocator>* as_eos() const noexcept {
        return static_cast<const eos_node<T, THREADED, Allocator>*>(this);
    }
    const skip_node<T, THREADED, Allocator>* as_skip() const noexcept {
        return static_cast<const skip_node<T, THREADED, Allocator>*>(this);
    }
    const list_node<T, THREADED, Allocator>* as_list() const noexcept {
        return static_cast<const list_node<T, THREADED, Allocator>*>(this);
    }
    const full_node<T, THREADED, Allocator>* as_full() const noexcept {
        return static_cast<const full_node<T, THREADED, Allocator>*>(this);
    }
    
    // Common accessors
    std::string_view skip_str() const noexcept {
        switch (type()) {
            case TYPE_SKIP: return as_skip()->skip;
            case TYPE_LIST: return as_list()->skip;
            case TYPE_FULL: return as_full()->skip;
            default: return {};
        }
    }
    
    int child_count() const noexcept {
        if (is_list()) return as_list()->chars.count();
        if (is_full()) return as_full()->valid.count();
        return 0;
    }
};

// =============================================================================
// EOS_NODE - minimal node with just a value
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct eos_node : node_base<T, THREADED, Allocator> {
    union {
        T leaf_value;
        T* eos_ptr;
    };
    
    eos_node() {}
    ~eos_node() {
        if (this->is_leaf()) leaf_value.~T();
    }
};

// =============================================================================
// SKIP_NODE - skip string + value
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct skip_node : node_base<T, THREADED, Allocator> {
    union {
        T leaf_value;
        T* eos_ptr;
    };
    std::string skip;
    
    skip_node() {}
    ~skip_node() {
        if (this->is_leaf()) leaf_value.~T();
    }
};

// =============================================================================
// LIST_NODE - up to 7 children
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct list_node : node_base<T, THREADED, Allocator> {
    using base_t = node_base<T, THREADED, Allocator>;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    static constexpr int MAX_CHILDREN = 7;
    
    T* eos_ptr;
    std::string skip;
    small_list chars;
    union {
        std::array<T, MAX_CHILDREN> leaf_values;
        std::array<atomic_ptr, MAX_CHILDREN> children;
    };
    
    list_node() : eos_ptr(nullptr) {}
    ~list_node() {
        if (this->is_leaf()) {
            int cnt = chars.count();
            for (int i = 0; i < cnt; ++i) leaf_values[i].~T();
        }
    }
    
    void construct_leaf_value(int idx, const T& value) {
        new (&leaf_values[idx]) T(value);
    }
    
    void destroy_leaf_value(int idx) {
        leaf_values[idx].~T();
    }
};

// =============================================================================
// FULL_NODE - 256 children
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct full_node : node_base<T, THREADED, Allocator> {
    using base_t = node_base<T, THREADED, Allocator>;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    T* eos_ptr;
    std::string skip;
    bitmap256 valid;
    union {
        std::array<T, 256> leaf_values;
        std::array<atomic_ptr, 256> children;
    };
    
    full_node() : eos_ptr(nullptr) {}
    ~full_node() {
        if (this->is_leaf()) {
            valid.for_each_set([this](unsigned char c) {
                leaf_values[c].~T();
            });
        }
    }
    
    void construct_leaf_value(unsigned char c, const T& value) {
        new (&leaf_values[c]) T(value);
    }
    
    void destroy_leaf_value(unsigned char c) {
        leaf_values[c].~T();
    }
};

// =============================================================================
// LAYOUT CONSTANTS
// =============================================================================

static constexpr size_t NODE_EOS_OFFSET = 8;
static constexpr size_t NODE_SKIP_OFFSET = 16;

// =============================================================================
// NODE_BUILDER - allocation and type-safe construction
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
class node_builder {
public:
    using base_t = node_base<T, THREADED, Allocator>;
    using ptr_t = base_t*;
    using eos_t = eos_node<T, THREADED, Allocator>;
    using skip_t = skip_node<T, THREADED, Allocator>;
    using list_t = list_node<T, THREADED, Allocator>;
    using full_t = full_node<T, THREADED, Allocator>;
    
    // Unified node deletion - eliminates repeated switch statements
    static void delete_node(ptr_t n) {
        if (!n) return;
        switch (n->type()) {
            case TYPE_EOS: delete n->as_eos(); break;
            case TYPE_SKIP: delete n->as_skip(); break;
            case TYPE_LIST: delete n->as_list(); break;
            case TYPE_FULL: delete n->as_full(); break;
        }
    }
    
    // Leaf builders
    ptr_t make_leaf_eos(const T& value) {
        auto* n = new eos_t();
        n->set_header(make_header(true, TYPE_EOS));
        new (&n->leaf_value) T(value);
        return n;
    }
    
    ptr_t make_leaf_skip(std::string_view sk, const T& value) {
        auto* n = new skip_t();
        n->set_header(make_header(true, TYPE_SKIP));
        n->skip = std::string(sk);
        new (&n->leaf_value) T(value);
        return n;
    }
    
    ptr_t make_leaf_list(std::string_view sk) {
        auto* n = new list_t();
        n->set_header(make_header(true, TYPE_LIST));
        n->skip = std::string(sk);
        return n;
    }
    
    ptr_t make_leaf_full(std::string_view sk) {
        auto* n = new full_t();
        n->set_header(make_header(true, TYPE_FULL));
        n->skip = std::string(sk);
        return n;
    }
    
    // Interior builders
    ptr_t make_interior_eos() {
        auto* n = new eos_t();
        n->set_header(make_header(false, TYPE_EOS));
        n->eos_ptr = nullptr;
        return n;
    }
    
    ptr_t make_interior_skip(std::string_view sk) {
        auto* n = new skip_t();
        n->set_header(make_header(false, TYPE_SKIP));
        n->skip = std::string(sk);
        n->eos_ptr = nullptr;
        return n;
    }
    
    ptr_t make_interior_list(std::string_view sk) {
        auto* n = new list_t();
        n->set_header(make_header(false, TYPE_LIST));
        n->skip = std::string(sk);
        for (int i = 0; i < list_t::MAX_CHILDREN; ++i) {
            n->children[i].store(nullptr);
        }
        return n;
    }
    
    ptr_t make_interior_full(std::string_view sk) {
        auto* n = new full_t();
        n->set_header(make_header(false, TYPE_FULL));
        n->skip = std::string(sk);
        for (int i = 0; i < 256; ++i) {
            n->children[i].store(nullptr);
        }
        return n;
    }
    
    // Recursive deallocation
    void dealloc_node(ptr_t n) {
        if (!n) return;
        
        if (!n->is_leaf()) {
            if (n->is_list()) {
                auto* ln = n->as_list();
                int cnt = ln->chars.count();
                for (int i = 0; i < cnt; ++i) {
                    dealloc_node(ln->children[i].load());
                }
                delete ln->eos_ptr;
            } else if (n->is_full()) {
                auto* fn = n->as_full();
                fn->valid.for_each_set([this, fn](unsigned char c) {
                    dealloc_node(fn->children[c].load());
                });
                delete fn->eos_ptr;
            } else if (n->is_skip()) {
                delete n->as_skip()->eos_ptr;
            } else {
                delete n->as_eos()->eos_ptr;
            }
        }
        delete_node(n);
    }
    
    // Deep copy
    ptr_t deep_copy(ptr_t src) {
        if (!src) return nullptr;
        
        if (src->is_leaf()) {
            switch (src->type()) {
                case TYPE_EOS: return make_leaf_eos(src->as_eos()->leaf_value);
                case TYPE_SKIP: return make_leaf_skip(src->as_skip()->skip, src->as_skip()->leaf_value);
                case TYPE_LIST: {
                    auto* s = src->as_list();
                    auto* d = new list_t();
                    d->set_header(s->header());
                    d->skip = s->skip;
                    d->chars = s->chars;
                    int cnt = s->chars.count();
                    for (int i = 0; i < cnt; ++i) {
                        d->construct_leaf_value(i, s->leaf_values[i]);
                    }
                    return d;
                }
                case TYPE_FULL: {
                    auto* s = src->as_full();
                    auto* d = new full_t();
                    d->set_header(s->header());
                    d->skip = s->skip;
                    d->valid = s->valid;
                    s->valid.for_each_set([s, d](unsigned char c) {
                        d->construct_leaf_value(c, s->leaf_values[c]);
                    });
                    return d;
                }
            }
        } else {
            switch (src->type()) {
                case TYPE_EOS: {
                    auto* s = src->as_eos();
                    auto* d = new eos_t();
                    d->set_header(s->header());
                    d->eos_ptr = s->eos_ptr ? new T(*s->eos_ptr) : nullptr;
                    return d;
                }
                case TYPE_SKIP: {
                    auto* s = src->as_skip();
                    auto* d = new skip_t();
                    d->set_header(s->header());
                    d->skip = s->skip;
                    d->eos_ptr = s->eos_ptr ? new T(*s->eos_ptr) : nullptr;
                    return d;
                }
                case TYPE_LIST: {
                    auto* s = src->as_list();
                    auto* d = new list_t();
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
                case TYPE_FULL: {
                    auto* s = src->as_full();
                    auto* d = new full_t();
                    d->set_header(s->header());
                    d->skip = s->skip;
                    d->valid = s->valid;
                    d->eos_ptr = s->eos_ptr ? new T(*s->eos_ptr) : nullptr;
                    s->valid.for_each_set([this, s, d](unsigned char c) {
                        d->children[c].store(deep_copy(s->children[c].load()));
                    });
                    return d;
                }
            }
        }
        return nullptr;
    }
};

}  // namespace gteitelbaum
