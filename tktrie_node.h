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
// ATOMIC_NODE_PTR
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct atomic_node_ptr {
    using base_t = node_base<T, THREADED, Allocator>;
    using ptr_t = base_t*;
    
    std::conditional_t<THREADED, std::atomic<ptr_t>, ptr_t> ptr_{nullptr};
    
    ptr_t load() const noexcept;
    void store(ptr_t p) noexcept;
};

// =============================================================================
// NODE_BASE - common header for all node types
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct node_base {
    using self_t = node_base<T, THREADED, Allocator>;
    using ptr_t = self_t*;
    using atomic_ptr = atomic_node_ptr<T, THREADED, Allocator>;
    
    std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> header_;
    
    // Header access
    uint64_t header() const noexcept;
    void set_header(uint64_t h) noexcept;
    
    // Type queries
    bool is_leaf() const noexcept;
    uint64_t type() const noexcept;
    uint64_t version() const noexcept;
    void bump_version() noexcept;
    
    bool is_eos() const noexcept;
    bool is_skip() const noexcept;
    bool is_list() const noexcept;
    bool is_full() const noexcept;
    
    // Downcasts
    eos_node<T, THREADED, Allocator>* as_eos() noexcept;
    skip_node<T, THREADED, Allocator>* as_skip() noexcept;
    list_node<T, THREADED, Allocator>* as_list() noexcept;
    full_node<T, THREADED, Allocator>* as_full() noexcept;
    
    const eos_node<T, THREADED, Allocator>* as_eos() const noexcept;
    const skip_node<T, THREADED, Allocator>* as_skip() const noexcept;
    const list_node<T, THREADED, Allocator>* as_list() const noexcept;
    const full_node<T, THREADED, Allocator>* as_full() const noexcept;
    
    // Common accessors
    std::string_view skip_str() const noexcept;
    int child_count() const noexcept;
};

// =============================================================================
// EOS_NODE - minimal node with just a value
// Layout: header (8) → value/eos_ptr (8) = 16 bytes
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct eos_node : node_base<T, THREADED, Allocator> {
    union {
        T leaf_value;
        T* eos_ptr;
    };
    
    eos_node();
    ~eos_node();
};

// =============================================================================
// SKIP_NODE - skip string + value
// Layout: header (8) → value/eos_ptr (8) → skip (32) = 48 bytes
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct skip_node : node_base<T, THREADED, Allocator> {
    union {
        T leaf_value;
        T* eos_ptr;
    };
    std::string skip;
    
    skip_node();
    ~skip_node();
};

// =============================================================================
// LIST_NODE - up to 7 children
// Layout: header (8) → eos_ptr (8) → skip (32) → chars (8) → children (56) = 112 bytes
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
    
    list_node();
    ~list_node();
    
    // Helper to properly construct a leaf value at index
    void construct_leaf_value(int idx, const T& value) {
        new (&leaf_values[idx]) T(value);
    }
    
    // Helper to destroy a leaf value at index
    void destroy_leaf_value(int idx) {
        leaf_values[idx].~T();
    }
};

// =============================================================================
// FULL_NODE - 256 children
// Layout: header (8) → eos_ptr (8) → skip (32) → valid (32) → children (2048) = 2128 bytes
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
    
    full_node();
    ~full_node();
    
    // Helper to properly construct a leaf value at index
    void construct_leaf_value(unsigned char c, const T& value) {
        new (&leaf_values[c]) T(value);
    }
    
    // Helper to destroy a leaf value at index
    void destroy_leaf_value(unsigned char c) {
        leaf_values[c].~T();
    }
};

// =============================================================================
// LAYOUT CONSTANTS - for offset-based access
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
    
    // Leaf builders
    ptr_t make_leaf_eos(const T& value);
    ptr_t make_leaf_skip(std::string_view sk, const T& value);
    ptr_t make_leaf_list(std::string_view sk);
    ptr_t make_leaf_full(std::string_view sk);
    
    // Interior builders
    ptr_t make_interior_eos();
    ptr_t make_interior_skip(std::string_view sk);
    ptr_t make_interior_list(std::string_view sk);
    ptr_t make_interior_full(std::string_view sk);
    
    // Deallocation and copying
    void dealloc_node(ptr_t n);
    ptr_t deep_copy(ptr_t src);
};

// =============================================================================
// ATOMIC_NODE_PTR DEFINITIONS
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
typename atomic_node_ptr<T, THREADED, Allocator>::ptr_t 
atomic_node_ptr<T, THREADED, Allocator>::load() const noexcept {
    if constexpr (THREADED) return ptr_.load(std::memory_order_acquire);
    else return ptr_;
}

template <typename T, bool THREADED, typename Allocator>
void atomic_node_ptr<T, THREADED, Allocator>::store(ptr_t p) noexcept {
    if constexpr (THREADED) ptr_.store(p, std::memory_order_release);
    else ptr_ = p;
}

// =============================================================================
// NODE_BASE DEFINITIONS
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
uint64_t node_base<T, THREADED, Allocator>::header() const noexcept {
    if constexpr (THREADED) return header_.load(std::memory_order_acquire);
    else return header_;
}

template <typename T, bool THREADED, typename Allocator>
void node_base<T, THREADED, Allocator>::set_header(uint64_t h) noexcept {
    if constexpr (THREADED) header_.store(h, std::memory_order_release);
    else header_ = h;
}

template <typename T, bool THREADED, typename Allocator>
bool node_base<T, THREADED, Allocator>::is_leaf() const noexcept {
    return gteitelbaum::is_leaf(header());
}

template <typename T, bool THREADED, typename Allocator>
uint64_t node_base<T, THREADED, Allocator>::type() const noexcept {
    return get_type(header());
}

template <typename T, bool THREADED, typename Allocator>
uint64_t node_base<T, THREADED, Allocator>::version() const noexcept {
    return get_version(header());
}

template <typename T, bool THREADED, typename Allocator>
void node_base<T, THREADED, Allocator>::bump_version() noexcept {
    if constexpr (THREADED) {
        uint64_t old_h = header_.load(std::memory_order_acquire);
        uint64_t new_h = gteitelbaum::bump_version(old_h);
        header_.store(new_h, std::memory_order_release);
    } else {
        header_ = gteitelbaum::bump_version(header_);
    }
}

template <typename T, bool THREADED, typename Allocator>
bool node_base<T, THREADED, Allocator>::is_eos() const noexcept { return type() == TYPE_EOS; }

template <typename T, bool THREADED, typename Allocator>
bool node_base<T, THREADED, Allocator>::is_skip() const noexcept { return type() == TYPE_SKIP; }

template <typename T, bool THREADED, typename Allocator>
bool node_base<T, THREADED, Allocator>::is_list() const noexcept { return type() == TYPE_LIST; }

template <typename T, bool THREADED, typename Allocator>
bool node_base<T, THREADED, Allocator>::is_full() const noexcept { return type() == TYPE_FULL; }

template <typename T, bool THREADED, typename Allocator>
eos_node<T, THREADED, Allocator>* node_base<T, THREADED, Allocator>::as_eos() noexcept {
    return static_cast<eos_node<T, THREADED, Allocator>*>(this);
}

template <typename T, bool THREADED, typename Allocator>
skip_node<T, THREADED, Allocator>* node_base<T, THREADED, Allocator>::as_skip() noexcept {
    return static_cast<skip_node<T, THREADED, Allocator>*>(this);
}

template <typename T, bool THREADED, typename Allocator>
list_node<T, THREADED, Allocator>* node_base<T, THREADED, Allocator>::as_list() noexcept {
    return static_cast<list_node<T, THREADED, Allocator>*>(this);
}

template <typename T, bool THREADED, typename Allocator>
full_node<T, THREADED, Allocator>* node_base<T, THREADED, Allocator>::as_full() noexcept {
    return static_cast<full_node<T, THREADED, Allocator>*>(this);
}

template <typename T, bool THREADED, typename Allocator>
const eos_node<T, THREADED, Allocator>* node_base<T, THREADED, Allocator>::as_eos() const noexcept {
    return static_cast<const eos_node<T, THREADED, Allocator>*>(this);
}

template <typename T, bool THREADED, typename Allocator>
const skip_node<T, THREADED, Allocator>* node_base<T, THREADED, Allocator>::as_skip() const noexcept {
    return static_cast<const skip_node<T, THREADED, Allocator>*>(this);
}

template <typename T, bool THREADED, typename Allocator>
const list_node<T, THREADED, Allocator>* node_base<T, THREADED, Allocator>::as_list() const noexcept {
    return static_cast<const list_node<T, THREADED, Allocator>*>(this);
}

template <typename T, bool THREADED, typename Allocator>
const full_node<T, THREADED, Allocator>* node_base<T, THREADED, Allocator>::as_full() const noexcept {
    return static_cast<const full_node<T, THREADED, Allocator>*>(this);
}

template <typename T, bool THREADED, typename Allocator>
std::string_view node_base<T, THREADED, Allocator>::skip_str() const noexcept {
    switch (type()) {
        case TYPE_SKIP: return as_skip()->skip;
        case TYPE_LIST: return as_list()->skip;
        case TYPE_FULL: return as_full()->skip;
        default: return {};
    }
}

template <typename T, bool THREADED, typename Allocator>
int node_base<T, THREADED, Allocator>::child_count() const noexcept {
    if (is_list()) return as_list()->chars.count();
    if (is_full()) return as_full()->valid.count();
    return 0;
}

// =============================================================================
// NODE CONSTRUCTORS
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
eos_node<T, THREADED, Allocator>::eos_node() {
    // Don't initialize union - builder will do it after setting header
}

template <typename T, bool THREADED, typename Allocator>
eos_node<T, THREADED, Allocator>::~eos_node() {
    if (this->is_leaf()) {
        leaf_value.~T();
    }
    // eos_ptr is managed externally, don't delete here
}

template <typename T, bool THREADED, typename Allocator>
skip_node<T, THREADED, Allocator>::skip_node() {
    // Don't initialize union - builder will do it after setting header
}

template <typename T, bool THREADED, typename Allocator>
skip_node<T, THREADED, Allocator>::~skip_node() {
    if (this->is_leaf()) {
        leaf_value.~T();
    }
    // eos_ptr is managed externally, don't delete here
}

template <typename T, bool THREADED, typename Allocator>
list_node<T, THREADED, Allocator>::list_node() : eos_ptr(nullptr) {
    // Don't initialize union - builder will do it after setting header
}

template <typename T, bool THREADED, typename Allocator>
list_node<T, THREADED, Allocator>::~list_node() {
    if (this->is_leaf()) {
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) {
            leaf_values[i].~T();
        }
    }
    // children and eos_ptr are managed externally
}

template <typename T, bool THREADED, typename Allocator>
full_node<T, THREADED, Allocator>::full_node() : eos_ptr(nullptr) {
    // Don't initialize union - builder will do it after setting header
}

template <typename T, bool THREADED, typename Allocator>
full_node<T, THREADED, Allocator>::~full_node() {
    if (this->is_leaf()) {
        valid.for_each_set([this](unsigned char c) {
            leaf_values[c].~T();
        });
    }
    // children and eos_ptr are managed externally
}

// =============================================================================
// NODE_BUILDER DEFINITIONS
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
typename node_builder<T, THREADED, Allocator>::ptr_t
node_builder<T, THREADED, Allocator>::make_leaf_eos(const T& value) {
    auto* n = new eos_t();
    n->set_header(make_header(true, TYPE_EOS));
    new (&n->leaf_value) T(value);
    return n;
}

template <typename T, bool THREADED, typename Allocator>
typename node_builder<T, THREADED, Allocator>::ptr_t
node_builder<T, THREADED, Allocator>::make_leaf_skip(std::string_view sk, const T& value) {
    auto* n = new skip_t();
    n->set_header(make_header(true, TYPE_SKIP));
    n->skip = std::string(sk);
    new (&n->leaf_value) T(value);
    return n;
}

template <typename T, bool THREADED, typename Allocator>
typename node_builder<T, THREADED, Allocator>::ptr_t
node_builder<T, THREADED, Allocator>::make_leaf_list(std::string_view sk) {
    auto* n = new list_t();
    n->set_header(make_header(true, TYPE_LIST));
    n->skip = std::string(sk);
    return n;
}

template <typename T, bool THREADED, typename Allocator>
typename node_builder<T, THREADED, Allocator>::ptr_t
node_builder<T, THREADED, Allocator>::make_leaf_full(std::string_view sk) {
    auto* n = new full_t();
    n->set_header(make_header(true, TYPE_FULL));
    n->skip = std::string(sk);
    return n;
}

template <typename T, bool THREADED, typename Allocator>
typename node_builder<T, THREADED, Allocator>::ptr_t
node_builder<T, THREADED, Allocator>::make_interior_eos() {
    auto* n = new eos_t();
    n->set_header(make_header(false, TYPE_EOS));
    n->eos_ptr = nullptr;
    return n;
}

template <typename T, bool THREADED, typename Allocator>
typename node_builder<T, THREADED, Allocator>::ptr_t
node_builder<T, THREADED, Allocator>::make_interior_skip(std::string_view sk) {
    auto* n = new skip_t();
    n->set_header(make_header(false, TYPE_SKIP));
    n->skip = std::string(sk);
    n->eos_ptr = nullptr;
    return n;
}

template <typename T, bool THREADED, typename Allocator>
typename node_builder<T, THREADED, Allocator>::ptr_t
node_builder<T, THREADED, Allocator>::make_interior_list(std::string_view sk) {
    auto* n = new list_t();
    n->set_header(make_header(false, TYPE_LIST));
    n->skip = std::string(sk);
    // Initialize children array for interior node
    for (int i = 0; i < list_t::MAX_CHILDREN; ++i) {
        n->children[i].store(nullptr);
    }
    return n;
}

template <typename T, bool THREADED, typename Allocator>
typename node_builder<T, THREADED, Allocator>::ptr_t
node_builder<T, THREADED, Allocator>::make_interior_full(std::string_view sk) {
    auto* n = new full_t();
    n->set_header(make_header(false, TYPE_FULL));
    n->skip = std::string(sk);
    // Initialize children array for interior node
    for (int i = 0; i < 256; ++i) {
        n->children[i].store(nullptr);
    }
    return n;
}

template <typename T, bool THREADED, typename Allocator>
void node_builder<T, THREADED, Allocator>::dealloc_node(ptr_t n) {
    if (!n) return;
    
    if (!n->is_leaf()) {
        if (n->is_list()) {
            auto* ln = n->as_list();
            int cnt = ln->chars.count();
            for (int i = 0; i < cnt; ++i) {
                dealloc_node(ln->children[i].load());
            }
            delete ln->eos_ptr;
            delete ln;
        } else if (n->is_full()) {
            auto* fn = n->as_full();
            fn->valid.for_each_set([this, fn](unsigned char c) {
                dealloc_node(fn->children[c].load());
            });
            delete fn->eos_ptr;
            delete fn;
        } else if (n->is_skip()) {
            auto* sn = n->as_skip();
            delete sn->eos_ptr;
            delete sn;
        } else {
            auto* en = n->as_eos();
            delete en->eos_ptr;
            delete en;
        }
    } else {
        switch (n->type()) {
            case TYPE_EOS: delete n->as_eos(); break;
            case TYPE_SKIP: delete n->as_skip(); break;
            case TYPE_LIST: delete n->as_list(); break;
            case TYPE_FULL: delete n->as_full(); break;
        }
    }
}

template <typename T, bool THREADED, typename Allocator>
typename node_builder<T, THREADED, Allocator>::ptr_t
node_builder<T, THREADED, Allocator>::deep_copy(ptr_t src) {
    if (!src) return nullptr;
    
    if (src->is_leaf()) {
        switch (src->type()) {
            case TYPE_EOS: {
                auto* s = src->as_eos();
                return make_leaf_eos(s->leaf_value);
            }
            case TYPE_SKIP: {
                auto* s = src->as_skip();
                return make_leaf_skip(s->skip, s->leaf_value);
            }
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

}  // namespace gteitelbaum
