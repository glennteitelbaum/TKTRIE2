#pragma once

#include <atomic>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>

#include "tktrie_defines.h"

namespace gteitelbaum {

// Forward declarations
template <typename T, bool THREADED, typename Allocator> struct trie_node;
template <typename T, bool THREADED, typename Allocator> class node_builder;

// Atomic-aware pointer wrapper
template <typename T, bool THREADED>
struct atomic_ptr {
    std::conditional_t<THREADED, std::atomic<T*>, T*> ptr_{nullptr};

    T* load() const noexcept {
        if constexpr (THREADED) return ptr_.load(std::memory_order_acquire);
        else return ptr_;
    }

    void store(T* p) noexcept {
        if constexpr (THREADED) ptr_.store(p, std::memory_order_release);
        else ptr_ = p;
    }

    T* exchange(T* p) noexcept {
        if constexpr (THREADED) return ptr_.exchange(p, std::memory_order_acq_rel);
        else { T* old = ptr_; ptr_ = p; return old; }
    }

    bool cas(T*& expected, T* desired) noexcept {
        if constexpr (THREADED) {
            return ptr_.compare_exchange_weak(expected, desired, 
                std::memory_order_acq_rel, std::memory_order_acquire);
        } else {
            if (ptr_ == expected) { ptr_ = desired; return true; }
            expected = ptr_;
            return false;
        }
    }
};

// Atomic-aware node pointer
template <typename T, bool THREADED, typename Allocator>
using node_ptr = atomic_ptr<trie_node<T, THREADED, Allocator>, THREADED>;

// EOS-only node data (NODE_EOS)
template <typename T, bool THREADED>
struct eos_data {
    atomic_ptr<T, THREADED> eos;
};

// Skip node data (NODE_SKIP) - has skip but no children
template <typename T, bool THREADED, typename Allocator>
struct skip_data {
    atomic_ptr<T, THREADED> eos;
    std::string skip;
    atomic_ptr<T, THREADED> skip_eos;
};

// List node data (NODE_LIST) - skip + up to 7 children
template <typename T, bool THREADED, typename Allocator>
struct list_data {
    atomic_ptr<T, THREADED> eos;
    std::string skip;
    atomic_ptr<T, THREADED> skip_eos;
    small_list chars;
    node_ptr<T, THREADED, Allocator> children[LIST_MAX];
};

// Full node data (NODE_FULL) - skip + 256 children with bitmap
template <typename T, bool THREADED, typename Allocator>
struct full_data {
    atomic_ptr<T, THREADED> eos;
    std::string skip;
    atomic_ptr<T, THREADED> skip_eos;
    bitmap256 valid;
    node_ptr<T, THREADED, Allocator> children[256];
};

// Main trie node - union of all types
template <typename T, bool THREADED, typename Allocator>
struct trie_node {
    using self_t = trie_node<T, THREADED, Allocator>;
    
    std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> header_;
    
    union {
        eos_data<T, THREADED> eos;
        skip_data<T, THREADED, Allocator> skip;
        list_data<T, THREADED, Allocator> list;
        full_data<T, THREADED, Allocator> full;
    };

    // Header access
    uint64_t header() const noexcept {
        if constexpr (THREADED) return header_.load(std::memory_order_acquire);
        else return header_;
    }

    void set_header(uint64_t h) noexcept {
        if constexpr (THREADED) header_.store(h, std::memory_order_release);
        else header_ = h;
    }

    uint64_t node_type() const noexcept { return get_node_type(header()); }
    uint64_t version() const noexcept { return get_version(header()); }

    // Type checks
    bool is_eos() const noexcept { return is_eos_node(header()); }
    bool is_skip() const noexcept { return is_skip_node(header()); }
    bool is_list() const noexcept { return is_list_node(header()); }
    bool is_full() const noexcept { return is_full_node(header()); }

    // EOS access (available on all node types)
    T* get_eos() const noexcept {
        switch (node_type()) {
            case NODE_EOS:  return eos.eos.load();
            case NODE_SKIP: return skip.eos.load();
            case NODE_LIST: return list.eos.load();
            case NODE_FULL: return full.eos.load();
            default: return nullptr;
        }
    }

    void set_eos(T* p) noexcept {
        switch (node_type()) {
            case NODE_EOS:  eos.eos.store(p); break;
            case NODE_SKIP: skip.eos.store(p); break;
            case NODE_LIST: list.eos.store(p); break;
            case NODE_FULL: full.eos.store(p); break;
        }
    }

    T* exchange_eos(T* p) noexcept {
        switch (node_type()) {
            case NODE_EOS:  return eos.eos.exchange(p);
            case NODE_SKIP: return skip.eos.exchange(p);
            case NODE_LIST: return list.eos.exchange(p);
            case NODE_FULL: return full.eos.exchange(p);
            default: return nullptr;
        }
    }

    // Skip access (not available on NODE_EOS)
    const std::string& get_skip() const noexcept {
        KTRIE_DEBUG_ASSERT(!is_eos());
        switch (node_type()) {
            case NODE_SKIP: return skip.skip;
            case NODE_LIST: return list.skip;
            case NODE_FULL: return full.skip;
            default: {
                static const std::string empty;
                return empty;
            }
        }
    }

    T* get_skip_eos() const noexcept {
        KTRIE_DEBUG_ASSERT(!is_eos());
        switch (node_type()) {
            case NODE_SKIP: return skip.skip_eos.load();
            case NODE_LIST: return list.skip_eos.load();
            case NODE_FULL: return full.skip_eos.load();
            default: return nullptr;
        }
    }

    void set_skip_eos(T* p) noexcept {
        KTRIE_DEBUG_ASSERT(!is_eos());
        switch (node_type()) {
            case NODE_SKIP: skip.skip_eos.store(p); break;
            case NODE_LIST: list.skip_eos.store(p); break;
            case NODE_FULL: full.skip_eos.store(p); break;
        }
    }

    T* exchange_skip_eos(T* p) noexcept {
        KTRIE_DEBUG_ASSERT(!is_eos());
        switch (node_type()) {
            case NODE_SKIP: return skip.skip_eos.exchange(p);
            case NODE_LIST: return list.skip_eos.exchange(p);
            case NODE_FULL: return full.skip_eos.exchange(p);
            default: return nullptr;
        }
    }

    // Child access (only for LIST and FULL)
    int child_count() const noexcept {
        switch (node_type()) {
            case NODE_LIST: return list.chars.count();
            case NODE_FULL: return full.valid.count();
            default: return 0;
        }
    }

    // Find child slot for character c, returns nullptr if not found
    self_t* find_child(unsigned char c) const noexcept {
        switch (node_type()) {
            case NODE_LIST: {
                int idx = list.chars.find(c);
                if (idx < 0) return nullptr;
                return list.children[idx].load();
            }
            case NODE_FULL: {
                if (!full.valid.test(c)) return nullptr;
                return full.children[c].load();
            }
            default: return nullptr;
        }
    }

    // Get child at specific index (for iteration)
    self_t* get_child_at(int idx) const noexcept {
        switch (node_type()) {
            case NODE_LIST: {
                if (idx >= list.chars.count()) return nullptr;
                return list.children[idx].load();
            }
            case NODE_FULL: {
                // idx is the actual character value for FULL
                if (!full.valid.test(static_cast<unsigned char>(idx))) return nullptr;
                return full.children[idx].load();
            }
            default: return nullptr;
        }
    }

    // Get character at index (for LIST iteration)
    unsigned char get_char_at(int idx) const noexcept {
        KTRIE_DEBUG_ASSERT(is_list());
        return list.chars.char_at(idx);
    }

    // Get smallest child character (for iteration)
    unsigned char first_child_char() const noexcept {
        switch (node_type()) {
            case NODE_LIST: return list.chars.smallest();
            case NODE_FULL: return full.valid.first_set();
            default: return 255;
        }
    }

    // Get next child character after c (for iteration)
    unsigned char next_child_char(unsigned char c) const noexcept {
        switch (node_type()) {
            case NODE_LIST: {
                // Find next smallest char > c
                unsigned char next = 255;
                int n = list.chars.count();
                for (int i = 0; i < n; ++i) {
                    unsigned char ch = list.chars.char_at(i);
                    if (ch > c && ch < next) next = ch;
                }
                return next;
            }
            case NODE_FULL: return full.valid.next_set(c);
            default: return 255;
        }
    }
};

// Node builder - handles allocation and construction
template <typename T, bool THREADED, typename Allocator>
class node_builder {
public:
    using node_t = trie_node<T, THREADED, Allocator>;
    using alloc_traits = std::allocator_traits<Allocator>;
    using node_alloc_t = typename alloc_traits::template rebind_alloc<node_t>;
    using node_alloc_traits = std::allocator_traits<node_alloc_t>;
    using value_alloc_t = typename alloc_traits::template rebind_alloc<T>;
    using value_alloc_traits = std::allocator_traits<value_alloc_t>;

private:
    node_alloc_t node_alloc_;
    value_alloc_t value_alloc_;

public:
    explicit node_builder(const Allocator& alloc = Allocator())
        : node_alloc_(alloc), value_alloc_(alloc) {}

    // Allocate and copy a value
    T* alloc_value(const T& val) {
        T* p = value_alloc_traits::allocate(value_alloc_, 1);
        try {
            std::construct_at(p, val);
        } catch (...) {
            value_alloc_traits::deallocate(value_alloc_, p, 1);
            throw;
        }
        return p;
    }

    T* alloc_value(T&& val) {
        T* p = value_alloc_traits::allocate(value_alloc_, 1);
        try {
            std::construct_at(p, std::move(val));
        } catch (...) {
            value_alloc_traits::deallocate(value_alloc_, p, 1);
            throw;
        }
        return p;
    }

    void free_value(T* p) noexcept {
        if (p) {
            std::destroy_at(p);
            value_alloc_traits::deallocate(value_alloc_, p, 1);
        }
    }

    // Build EOS node
    node_t* build_eos(T* eos_val = nullptr) {
        node_t* n = node_alloc_traits::allocate(node_alloc_, 1);
        n->set_header(make_header(NODE_EOS));
        new (&n->eos) eos_data<T, THREADED>{};
        n->eos.eos.store(eos_val);
        return n;
    }

    // Build SKIP node
    node_t* build_skip(const std::string& skip_str, T* eos_val = nullptr, T* skip_eos_val = nullptr) {
        node_t* n = node_alloc_traits::allocate(node_alloc_, 1);
        n->set_header(make_header(NODE_SKIP));
        new (&n->skip) skip_data<T, THREADED, Allocator>{};
        n->skip.eos.store(eos_val);
        n->skip.skip = skip_str;
        n->skip.skip_eos.store(skip_eos_val);
        return n;
    }

    // Build LIST node
    node_t* build_list(const std::string& skip_str, T* eos_val = nullptr, T* skip_eos_val = nullptr) {
        node_t* n = node_alloc_traits::allocate(node_alloc_, 1);
        n->set_header(make_header(NODE_LIST));
        new (&n->list) list_data<T, THREADED, Allocator>{};
        n->list.eos.store(eos_val);
        n->list.skip = skip_str;
        n->list.skip_eos.store(skip_eos_val);
        n->list.chars = small_list{};
        for (int i = 0; i < LIST_MAX; ++i) n->list.children[i].store(nullptr);
        return n;
    }

    // Build FULL node
    node_t* build_full(const std::string& skip_str, T* eos_val = nullptr, T* skip_eos_val = nullptr) {
        node_t* n = node_alloc_traits::allocate(node_alloc_, 1);
        n->set_header(make_header(NODE_FULL));
        new (&n->full) full_data<T, THREADED, Allocator>{};
        n->full.eos.store(eos_val);
        n->full.skip = skip_str;
        n->full.skip_eos.store(skip_eos_val);
        n->full.valid = bitmap256{};
        for (int i = 0; i < 256; ++i) n->full.children[i].store(nullptr);
        return n;
    }

    // Deallocate node (does NOT free children or values - caller responsibility)
    void deallocate_node(node_t* n) noexcept {
        if (!n) return;
        switch (n->node_type()) {
            case NODE_EOS:
                n->eos.~eos_data<T, THREADED>();
                break;
            case NODE_SKIP:
                n->skip.~skip_data<T, THREADED, Allocator>();
                break;
            case NODE_LIST:
                n->list.~list_data<T, THREADED, Allocator>();
                break;
            case NODE_FULL:
                n->full.~full_data<T, THREADED, Allocator>();
                break;
        }
        node_alloc_traits::deallocate(node_alloc_, n, 1);
    }

    // Deep copy a node (recursive)
    node_t* deep_copy(node_t* src) {
        if (!src) return nullptr;

        node_t* dst = nullptr;
        T* eos_copy = nullptr;
        T* skip_eos_copy = nullptr;

        // Copy EOS value if present
        T* src_eos = src->get_eos();
        if (src_eos) eos_copy = alloc_value(*src_eos);

        switch (src->node_type()) {
            case NODE_EOS:
                dst = build_eos(eos_copy);
                break;

            case NODE_SKIP:
                if (src->get_skip_eos()) skip_eos_copy = alloc_value(*src->get_skip_eos());
                dst = build_skip(src->get_skip(), eos_copy, skip_eos_copy);
                break;

            case NODE_LIST:
                if (src->get_skip_eos()) skip_eos_copy = alloc_value(*src->get_skip_eos());
                dst = build_list(src->get_skip(), eos_copy, skip_eos_copy);
                dst->list.chars = src->list.chars;
                for (int i = 0; i < src->list.chars.count(); ++i) {
                    dst->list.children[i].store(deep_copy(src->list.children[i].load()));
                }
                break;

            case NODE_FULL:
                if (src->get_skip_eos()) skip_eos_copy = alloc_value(*src->get_skip_eos());
                dst = build_full(src->get_skip(), eos_copy, skip_eos_copy);
                dst->full.valid = src->full.valid;
                for (int i = 0; i < 256; ++i) {
                    if (src->full.valid.test(static_cast<unsigned char>(i))) {
                        dst->full.children[i].store(deep_copy(src->full.children[i].load()));
                    }
                }
                break;
        }
        return dst;
    }

    // Free entire subtree (recursive)
    void free_subtree(node_t* n) noexcept {
        if (!n) return;

        // Free EOS value
        free_value(n->get_eos());

        // Free skip_eos if not EOS node
        if (!n->is_eos()) {
            free_value(n->get_skip_eos());
        }

        // Recurse into children
        switch (n->node_type()) {
            case NODE_LIST:
                for (int i = 0; i < n->list.chars.count(); ++i) {
                    free_subtree(n->list.children[i].load());
                }
                break;
            case NODE_FULL:
                for (int i = 0; i < 256; ++i) {
                    if (n->full.valid.test(static_cast<unsigned char>(i))) {
                        free_subtree(n->full.children[i].load());
                    }
                }
                break;
            default:
                break;
        }

        deallocate_node(n);
    }
};

}  // namespace gteitelbaum
