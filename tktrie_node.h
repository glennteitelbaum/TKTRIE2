#pragma once

#include <atomic>
#include <array>
#include <memory>
#include <string>
#include "tktrie_defines.h"

namespace gteitelbaum {

// Forward declarations
template <typename T, bool THREADED, typename Allocator> struct node_base;
template <typename T, bool THREADED, typename Allocator> struct eos_node;
template <typename T, bool THREADED, typename Allocator> struct skip_node;
template <typename T, bool THREADED, typename Allocator> struct list_node;
template <typename T, bool THREADED, typename Allocator> struct full_node;

// Atomic wrapper for node pointers
template <typename T, bool THREADED, typename Allocator>
struct atomic_node_ptr {
    using base_t = node_base<T, THREADED, Allocator>;
    using ptr_t = base_t*;
    std::conditional_t<THREADED, std::atomic<ptr_t>, ptr_t> ptr_{nullptr};
    
    ptr_t load() const noexcept {
        if constexpr (THREADED) return ptr_.load(std::memory_order_acquire);
        else return ptr_;
    }
    void store(ptr_t p) noexcept {
        if constexpr (THREADED) ptr_.store(p, std::memory_order_release);
        else ptr_ = p;
    }
};

// Base node - just header, used as common pointer type
template <typename T, bool THREADED, typename Allocator>
struct node_base {
    using self_t = node_base<T, THREADED, Allocator>;
    using ptr_t = self_t*;
    using atomic_ptr = atomic_node_ptr<T, THREADED, Allocator>;
    
    std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> header_;
    
    uint64_t header() const noexcept {
        if constexpr (THREADED) return header_.load(std::memory_order_acquire);
        else return header_;
    }
    void set_header(uint64_t h) noexcept {
        if constexpr (THREADED) header_.store(h, std::memory_order_release);
        else header_ = h;
    }
    
    bool is_leaf() const noexcept { return gteitelbaum::is_leaf(header()); }
    uint64_t type() const noexcept { return get_type(header()); }
    
    void bump_version() noexcept {
        if constexpr (THREADED) {
            uint64_t old_h = header_.load(std::memory_order_acquire);
            uint64_t new_h = gteitelbaum::bump_version(old_h);
            header_.store(new_h, std::memory_order_release);
        } else {
            header_ = gteitelbaum::bump_version(header_);
        }
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
    
    // Common accessors that dispatch based on type
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

// EOS node - minimal, just a value (leaf) or eos_ptr (interior)
template <typename T, bool THREADED, typename Allocator>
struct eos_node : node_base<T, THREADED, Allocator> {
    union {
        T leaf_value;
        T* eos_ptr;
    };
    
    eos_node() : eos_ptr(nullptr) {}
};

// SKIP node - skip string + value
template <typename T, bool THREADED, typename Allocator>
struct skip_node : node_base<T, THREADED, Allocator> {
    std::string skip;
    union {
        T leaf_value;
        T* eos_ptr;
    };
    
    skip_node() : eos_ptr(nullptr) {}
};

// LIST node - skip + up to 7 children
template <typename T, bool THREADED, typename Allocator>
struct list_node : node_base<T, THREADED, Allocator> {
    using base_t = node_base<T, THREADED, Allocator>;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    static constexpr int MAX_CHILDREN = 8;
    
    std::string skip;
    small_list chars;
    union {
        std::array<T, MAX_CHILDREN> leaf_values;
        std::array<atomic_ptr, MAX_CHILDREN> children;
    };
    T* eos_ptr;  // interior only
    
    list_node() : eos_ptr(nullptr) {
        for (int i = 0; i < MAX_CHILDREN; ++i) {
            children[i].store(nullptr);
        }
    }
};

// FULL node - skip + 256 children
template <typename T, bool THREADED, typename Allocator>
struct full_node : node_base<T, THREADED, Allocator> {
    using base_t = node_base<T, THREADED, Allocator>;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    std::string skip;
    bitmap256 valid;
    union {
        std::array<T, 256> leaf_values;
        std::array<atomic_ptr, 256> children;
    };
    T* eos_ptr;  // interior only
    
    full_node() : eos_ptr(nullptr) {
        for (int i = 0; i < 256; ++i) {
            children[i].store(nullptr);
        }
    }
};

// Node builder - handles allocation and type-safe construction
template <typename T, bool THREADED, typename Allocator>
class node_builder {
public:
    using base_t = node_base<T, THREADED, Allocator>;
    using ptr_t = base_t*;
    using eos_t = eos_node<T, THREADED, Allocator>;
    using skip_t = skip_node<T, THREADED, Allocator>;
    using list_t = list_node<T, THREADED, Allocator>;
    using full_t = full_node<T, THREADED, Allocator>;
    
    // === LEAF builders ===
    
    ptr_t make_leaf_eos(const T& value) {
        auto* n = new eos_t();
        n->set_header(make_header(true, TYPE_EOS));
        n->leaf_value = value;
        return n;
    }
    
    ptr_t make_leaf_skip(std::string_view sk, const T& value) {
        auto* n = new skip_t();
        n->set_header(make_header(true, TYPE_SKIP));
        n->skip = std::string(sk);
        n->leaf_value = value;
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
    
    // === INTERIOR builders ===
    
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
        return n;
    }
    
    ptr_t make_interior_full(std::string_view sk) {
        auto* n = new full_t();
        n->set_header(make_header(false, TYPE_FULL));
        n->skip = std::string(sk);
        return n;
    }
    
    // === Deallocation ===
    
    void dealloc_node(ptr_t n) {
        if (!n) return;
        
        // Recursively free children for interior nodes
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
                // Use Kernighan iteration - O(k) where k = child count
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
            // Leaf nodes - just delete the right type
            switch (n->type()) {
                case TYPE_EOS: delete n->as_eos(); break;
                case TYPE_SKIP: delete n->as_skip(); break;
                case TYPE_LIST: delete n->as_list(); break;
                case TYPE_FULL: delete n->as_full(); break;
            }
        }
    }
    
    // === Deep copy ===
    
    ptr_t deep_copy(ptr_t src) {
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
                        d->leaf_values[i] = s->leaf_values[i];
                    }
                    return d;
                }
                case TYPE_FULL: {
                    auto* s = src->as_full();
                    auto* d = new full_t();
                    d->set_header(s->header());
                    d->skip = s->skip;
                    d->valid = s->valid;
                    // Use Kernighan iteration
                    s->valid.for_each_set([s, d](unsigned char c) {
                        d->leaf_values[c] = s->leaf_values[c];
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
                    // Use Kernighan iteration
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
