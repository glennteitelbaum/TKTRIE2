#pragma once

#include <atomic>
#include <array>
#include <memory>
#include <string>
#include "tktrie_defines.h"

namespace gteitelbaum {

// Forward declarations
template <typename T, bool THREADED, typename Allocator> struct node;
template <typename T, bool THREADED, typename Allocator> struct node_ptr;

// Atomic wrapper for node pointers
template <typename T, bool THREADED, typename Allocator>
struct atomic_node_ptr {
    using ptr_t = node<T, THREADED, Allocator>*;
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

// Unified node structure - can be leaf or interior
template <typename T, bool THREADED, typename Allocator>
struct node {
    using self_t = node<T, THREADED, Allocator>;
    using ptr_t = self_t*;
    using atomic_ptr = atomic_node_ptr<T, THREADED, Allocator>;
    using value_ptr = std::conditional_t<THREADED, std::atomic<T*>, T*>;
    
    std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> header_;
    std::string skip;           // For SKIP/LIST/FULL
    small_list chars;           // For LIST
    bitmap256 valid;            // For FULL
    
    // Storage - interpretation depends on LEAF flag and type
    // LEAF + EOS: values[0] is the value
    // LEAF + SKIP: values[0] is the value at end of skip
    // LEAF + LIST: values[i] for chars[i]
    // LEAF + FULL: values[c] for each valid c
    // Interior + EOS: eos_ptr points to value
    // Interior + SKIP: skip_eos_ptr points to value at end of skip
    // Interior + LIST: children[i] for chars[i]
    // Interior + FULL: children[c] for each valid c
    
    union {
        std::array<T, 256> values;                  // LEAF
        std::array<atomic_ptr, 256> children;       // Interior
    };
    value_ptr eos_ptr{nullptr};       // Interior EOS/SKIP value pointer
    
    // Transfer eos_ptr from another node (nulls the source)
    void take_eos_from(node& other) noexcept {
        T* p;
        if constexpr (THREADED) {
            p = other.eos_ptr.exchange(nullptr, std::memory_order_acq_rel);
            eos_ptr.store(p, std::memory_order_release);
        } else {
            p = other.eos_ptr;
            other.eos_ptr = nullptr;
            eos_ptr = p;
        }
    }
    
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
    bool is_eos() const noexcept { return type() == TYPE_EOS; }
    bool is_skip() const noexcept { return type() == TYPE_SKIP; }
    bool is_list() const noexcept { return type() == TYPE_LIST; }
    bool is_full() const noexcept { return type() == TYPE_FULL; }
    
    // Get child count for interior nodes
    int child_count() const noexcept {
        if (is_list()) return chars.count();
        if (is_full()) return valid.count();
        return 0;
    }
    
    // Find child slot
    atomic_ptr* find_child_slot(unsigned char c) noexcept {
        if (is_list()) {
            int idx = chars.find(c);
            return idx >= 0 ? &children[idx] : nullptr;
        }
        if (is_full() && valid.test(c)) {
            return &children[c];
        }
        return nullptr;
    }
    
    // Get value for leaf nodes
    T* find_value_slot(unsigned char c) noexcept {
        KTRIE_DEBUG_ASSERT(is_leaf());
        if (is_list()) {
            int idx = chars.find(c);
            return idx >= 0 ? &values[idx] : nullptr;
        }
        if (is_full() && valid.test(c)) {
            return &values[c];
        }
        return nullptr;
    }
    
    node() : values{} {}
    ~node() {
        if (!is_leaf() && eos_ptr) {
            T* p;
            if constexpr (THREADED) p = eos_ptr.load(std::memory_order_acquire);
            else p = eos_ptr;
            delete p;
        }
    }
};

// Node builder
template <typename T, bool THREADED, typename Allocator>
class node_builder {
public:
    using node_t = node<T, THREADED, Allocator>;
    using ptr_t = node_t*;
    
private:
    Allocator alloc_;
    
    ptr_t alloc_node() {
        ptr_t n = new node_t();
        return n;
    }
    
public:
    explicit node_builder(const Allocator& alloc = Allocator()) : alloc_(alloc) {}
    
    void dealloc_node(ptr_t n) {
        if (!n) return;
        // Recursively free children for interior nodes
        if (!n->is_leaf()) {
            if (n->is_list()) {
                for (int i = 0; i < n->chars.count(); ++i) {
                    dealloc_node(n->children[i].load());
                }
            } else if (n->is_full()) {
                for (int c = 0; c < 256; ++c) {
                    if (n->valid.test(static_cast<unsigned char>(c))) {
                        dealloc_node(n->children[c].load());
                    }
                }
            }
        }
        delete n;
    }
    
    // Create leaf EOS (value with no remaining key)
    ptr_t make_leaf_eos(const T& value) {
        ptr_t n = alloc_node();
        n->set_header(make_header(true, TYPE_EOS));
        n->values[0] = value;
        return n;
    }
    
    // Create leaf SKIP (value after skip prefix)
    ptr_t make_leaf_skip(std::string_view sk, const T& value) {
        ptr_t n = alloc_node();
        n->set_header(make_header(true, TYPE_SKIP));
        n->skip = std::string(sk);
        n->values[0] = value;
        return n;
    }
    
    // Create leaf LIST (values indexed by single char after skip)
    ptr_t make_leaf_list(std::string_view sk) {
        ptr_t n = alloc_node();
        n->set_header(make_header(true, TYPE_LIST));
        n->skip = std::string(sk);
        return n;
    }
    
    // Create leaf FULL (values indexed by single char after skip)
    ptr_t make_leaf_full(std::string_view sk) {
        ptr_t n = alloc_node();
        n->set_header(make_header(true, TYPE_FULL));
        n->skip = std::string(sk);
        return n;
    }
    
    // Create interior EOS (eos_ptr for value)
    ptr_t make_interior_eos() {
        ptr_t n = alloc_node();
        n->set_header(make_header(false, TYPE_EOS));
        return n;
    }
    
    // Create interior SKIP
    ptr_t make_interior_skip(std::string_view sk) {
        ptr_t n = alloc_node();
        n->set_header(make_header(false, TYPE_SKIP));
        n->skip = std::string(sk);
        return n;
    }
    
    // Create interior LIST
    ptr_t make_interior_list(std::string_view sk) {
        ptr_t n = alloc_node();
        n->set_header(make_header(false, TYPE_LIST));
        n->skip = std::string(sk);
        return n;
    }
    
    // Create interior FULL
    ptr_t make_interior_full(std::string_view sk) {
        ptr_t n = alloc_node();
        n->set_header(make_header(false, TYPE_FULL));
        n->skip = std::string(sk);
        return n;
    }
    
    // Deep copy
    ptr_t deep_copy(ptr_t src) {
        if (!src) return nullptr;
        ptr_t dst = alloc_node();
        dst->set_header(src->header());
        dst->skip = src->skip;
        dst->chars = src->chars;
        dst->valid = src->valid;
        
        if (src->is_leaf()) {
            if (src->is_eos() || src->is_skip()) {
                dst->values[0] = src->values[0];
            } else if (src->is_list()) {
                for (int i = 0; i < src->chars.count(); ++i) {
                    dst->values[i] = src->values[i];
                }
            } else {
                for (int c = 0; c < 256; ++c) {
                    if (src->valid.test(static_cast<unsigned char>(c))) {
                        dst->values[c] = src->values[c];
                    }
                }
            }
        } else {
            if (src->eos_ptr) {
                T* p;
                if constexpr (THREADED) p = src->eos_ptr.load(std::memory_order_acquire);
                else p = src->eos_ptr;
                if (p) {
                    T* np = new T(*p);
                    if constexpr (THREADED) dst->eos_ptr.store(np, std::memory_order_release);
                    else dst->eos_ptr = np;
                }
            }
            if (src->is_list()) {
                for (int i = 0; i < src->chars.count(); ++i) {
                    dst->children[i].store(deep_copy(src->children[i].load()));
                }
            } else if (src->is_full()) {
                for (int c = 0; c < 256; ++c) {
                    if (src->valid.test(static_cast<unsigned char>(c))) {
                        dst->children[c].store(deep_copy(src->children[c].load()));
                    }
                }
            }
        }
        return dst;
    }
};

}  // namespace gteitelbaum
