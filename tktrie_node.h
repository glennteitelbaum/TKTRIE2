#pragma once

#include <atomic>
#include <array>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>

#include "tktrie_defines.h"

namespace gteitelbaum {

// Forward declarations
template <typename T, bool THREADED, typename Allocator> struct eos_node;
template <typename T, bool THREADED, typename Allocator> struct skip_node;
template <typename T, bool THREADED, typename Allocator> struct list_node;
template <typename T, bool THREADED, typename Allocator> struct full_node;
template <typename T, bool THREADED, typename Allocator> union node_ptr;
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
};

// =============================================================================
// Node types (has-a composition)
// =============================================================================

// EOS node: header + eos pointer (~16 bytes)
template <typename T, bool THREADED, typename Allocator>
struct eos_node {
    std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> header_;
    atomic_ptr<T, THREADED> eos;

    uint64_t header() const noexcept {
        if constexpr (THREADED) return header_.load(std::memory_order_acquire);
        else return header_;
    }

    void set_header(uint64_t h) noexcept {
        if constexpr (THREADED) header_.store(h, std::memory_order_release);
        else header_ = h;
    }
};

// SKIP node: eos_node + skip + skip_eos (~56 bytes with SSO)
template <typename T, bool THREADED, typename Allocator>
struct skip_node {
    eos_node<T, THREADED, Allocator> base;
    std::string skip;
    atomic_ptr<T, THREADED> skip_eos;

    uint64_t header() const noexcept { return base.header(); }
    void set_header(uint64_t h) noexcept { base.set_header(h); }
};

// Forward declare node_ptr for use in node types
template <typename T, bool THREADED, typename Allocator> union node_ptr;

// Atomic node pointer wrapper
template <typename T, bool THREADED, typename Allocator>
struct atomic_node_ptr {
    using ptr_t = node_ptr<T, THREADED, Allocator>;
    
    std::conditional_t<THREADED, std::atomic<void*>, void*> ptr_{nullptr};

    ptr_t load() const noexcept {
        ptr_t p;
        if constexpr (THREADED) p.raw = ptr_.load(std::memory_order_acquire);
        else p.raw = ptr_;
        return p;
    }

    void store(ptr_t p) noexcept {
        if constexpr (THREADED) ptr_.store(p.raw, std::memory_order_release);
        else ptr_ = p.raw;
    }

    ptr_t exchange(ptr_t p) noexcept {
        ptr_t old;
        if constexpr (THREADED) old.raw = ptr_.exchange(p.raw, std::memory_order_acq_rel);
        else { old.raw = ptr_; ptr_ = p.raw; }
        return old;
    }
};

// LIST node: skip_node + chars + 7 children (~120 bytes)
template <typename T, bool THREADED, typename Allocator>
struct list_node {
    skip_node<T, THREADED, Allocator> base;
    small_list chars;
    std::array<atomic_node_ptr<T, THREADED, Allocator>, LIST_MAX> children;

    uint64_t header() const noexcept { return base.header(); }
    void set_header(uint64_t h) noexcept { base.set_header(h); }
};

// FULL node: skip_node + bitmap + 256 children (~2KB)
template <typename T, bool THREADED, typename Allocator>
struct full_node {
    skip_node<T, THREADED, Allocator> base;
    bitmap256 valid;
    std::array<atomic_node_ptr<T, THREADED, Allocator>, 256> children;

    uint64_t header() const noexcept { return base.header(); }
    void set_header(uint64_t h) noexcept { base.set_header(h); }
};

// =============================================================================
// Node pointer union
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
union node_ptr {
    using eos_t = eos_node<T, THREADED, Allocator>;
    using skip_t = skip_node<T, THREADED, Allocator>;
    using list_t = list_node<T, THREADED, Allocator>;
    using full_t = full_node<T, THREADED, Allocator>;

    void* raw;
    eos_t* eos;
    skip_t* skip;
    list_t* list;
    full_t* full;

    node_ptr() noexcept : raw(nullptr) {}
    node_ptr(std::nullptr_t) noexcept : raw(nullptr) {}
    node_ptr(eos_t* p) noexcept : eos(p) {}
    node_ptr(skip_t* p) noexcept : skip(p) {}
    node_ptr(list_t* p) noexcept : list(p) {}
    node_ptr(full_t* p) noexcept : full(p) {}

    explicit operator bool() const noexcept { return raw != nullptr; }
    bool operator==(std::nullptr_t) const noexcept { return raw == nullptr; }
    bool operator!=(std::nullptr_t) const noexcept { return raw != nullptr; }

    // Header access (valid for any node type)
    uint64_t header() const noexcept {
        if constexpr (THREADED) return eos->header_.load(std::memory_order_acquire);
        else return eos->header_;
    }

    uint64_t node_type() const noexcept { return get_node_type(header()); }
    bool is_eos() const noexcept { return is_eos_node(header()); }
    bool is_skip() const noexcept { return is_skip_node(header()); }
    bool is_list() const noexcept { return is_list_node(header()); }
    bool is_full() const noexcept { return is_full_node(header()); }

    // EOS access (valid for any node - all have eos at same offset via base)
    T* get_eos() const noexcept { return eos->eos.load(); }
    void set_eos(T* p) noexcept { eos->eos.store(p); }
    T* exchange_eos(T* p) noexcept { return eos->eos.exchange(p); }

    // Skip access (valid for SKIP, LIST, FULL - use skip pointer)
    const std::string& get_skip() const noexcept {
        KTRIE_DEBUG_ASSERT(!is_eos());
        return skip->skip;
    }

    T* get_skip_eos() const noexcept {
        KTRIE_DEBUG_ASSERT(!is_eos());
        return skip->skip_eos.load();
    }

    void set_skip_eos(T* p) noexcept {
        KTRIE_DEBUG_ASSERT(!is_eos());
        skip->skip_eos.store(p);
    }

    T* exchange_skip_eos(T* p) noexcept {
        KTRIE_DEBUG_ASSERT(!is_eos());
        return skip->skip_eos.exchange(p);
    }

    // Child count
    int child_count() const noexcept {
        if (is_list()) return list->chars.count();
        if (is_full()) return full->valid.count();
        return 0;
    }

    // Find child
    node_ptr find_child(unsigned char c) const noexcept {
        if (is_list()) {
            int idx = list->chars.find(c);
            if (idx < 0) return nullptr;
            return list->children[idx].load();
        }
        if (is_full()) {
            if (!full->valid.test(c)) return nullptr;
            return full->children[c].load();
        }
        return nullptr;
    }

    // First child char (for iteration)
    unsigned char first_child_char() const noexcept {
        if (is_list()) return list->chars.smallest();
        if (is_full()) return full->valid.first_set();
        return 255;
    }

    // Next child char after c
    unsigned char next_child_char(unsigned char c) const noexcept {
        if (is_list()) {
            unsigned char next = 255;
            int n = list->chars.count();
            for (int i = 0; i < n; ++i) {
                unsigned char ch = list->chars.char_at(i);
                if (ch > c && ch < next) next = ch;
            }
            return next;
        }
        if (is_full()) return full->valid.next_set(c);
        return 255;
    }
};

// =============================================================================
// Node builder
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
class node_builder {
public:
    using ptr_t = node_ptr<T, THREADED, Allocator>;
    using eos_t = eos_node<T, THREADED, Allocator>;
    using skip_t = skip_node<T, THREADED, Allocator>;
    using list_t = list_node<T, THREADED, Allocator>;
    using full_t = full_node<T, THREADED, Allocator>;

    using alloc_traits = std::allocator_traits<Allocator>;
    using eos_alloc_t = typename alloc_traits::template rebind_alloc<eos_t>;
    using skip_alloc_t = typename alloc_traits::template rebind_alloc<skip_t>;
    using list_alloc_t = typename alloc_traits::template rebind_alloc<list_t>;
    using full_alloc_t = typename alloc_traits::template rebind_alloc<full_t>;
    using value_alloc_t = typename alloc_traits::template rebind_alloc<T>;

private:
    eos_alloc_t eos_alloc_;
    skip_alloc_t skip_alloc_;
    list_alloc_t list_alloc_;
    full_alloc_t full_alloc_;
    value_alloc_t value_alloc_;

public:
    explicit node_builder(const Allocator& alloc = Allocator())
        : eos_alloc_(alloc), skip_alloc_(alloc), list_alloc_(alloc),
          full_alloc_(alloc), value_alloc_(alloc) {}

    // Value allocation
    T* alloc_value(const T& val) {
        T* p = std::allocator_traits<value_alloc_t>::allocate(value_alloc_, 1);
        try { std::construct_at(p, val); }
        catch (...) { std::allocator_traits<value_alloc_t>::deallocate(value_alloc_, p, 1); throw; }
        return p;
    }

    T* alloc_value(T&& val) {
        T* p = std::allocator_traits<value_alloc_t>::allocate(value_alloc_, 1);
        try { std::construct_at(p, std::move(val)); }
        catch (...) { std::allocator_traits<value_alloc_t>::deallocate(value_alloc_, p, 1); throw; }
        return p;
    }

    void free_value(T* p) noexcept {
        if (p) {
            std::destroy_at(p);
            std::allocator_traits<value_alloc_t>::deallocate(value_alloc_, p, 1);
        }
    }

    // Build EOS node
    ptr_t build_eos(T* eos_val = nullptr) {
        eos_t* n = std::allocator_traits<eos_alloc_t>::allocate(eos_alloc_, 1);
        std::construct_at(n);
        n->set_header(make_header(NODE_EOS));
        n->eos.store(eos_val);
        return ptr_t(n);
    }

    // Build SKIP node
    ptr_t build_skip(const std::string& skip_str, T* eos_val = nullptr, T* skip_eos_val = nullptr) {
        skip_t* n = std::allocator_traits<skip_alloc_t>::allocate(skip_alloc_, 1);
        std::construct_at(n);
        n->set_header(make_header(NODE_SKIP));
        n->base.eos.store(eos_val);
        n->skip = skip_str;
        n->skip_eos.store(skip_eos_val);
        return ptr_t(n);
    }

    // Build LIST node
    ptr_t build_list(const std::string& skip_str, T* eos_val = nullptr, T* skip_eos_val = nullptr) {
        list_t* n = std::allocator_traits<list_alloc_t>::allocate(list_alloc_, 1);
        std::construct_at(n);
        n->set_header(make_header(NODE_LIST));
        n->base.base.eos.store(eos_val);
        n->base.skip = skip_str;
        n->base.skip_eos.store(skip_eos_val);
        n->chars = small_list{};
        for (auto& c : n->children) c.store(nullptr);
        return ptr_t(n);
    }

    // Build FULL node
    ptr_t build_full(const std::string& skip_str, T* eos_val = nullptr, T* skip_eos_val = nullptr) {
        full_t* n = std::allocator_traits<full_alloc_t>::allocate(full_alloc_, 1);
        std::construct_at(n);
        n->set_header(make_header(NODE_FULL));
        n->base.base.eos.store(eos_val);
        n->base.skip = skip_str;
        n->base.skip_eos.store(skip_eos_val);
        n->valid = bitmap256{};
        for (auto& c : n->children) c.store(nullptr);
        return ptr_t(n);
    }

    // Deallocate node (does NOT free children or values)
    void deallocate_node(ptr_t p) noexcept {
        if (!p) return;

        if (p.is_eos()) {
            std::destroy_at(p.eos);
            std::allocator_traits<eos_alloc_t>::deallocate(eos_alloc_, p.eos, 1);
        } else if (p.is_skip()) {
            std::destroy_at(p.skip);
            std::allocator_traits<skip_alloc_t>::deallocate(skip_alloc_, p.skip, 1);
        } else if (p.is_list()) {
            std::destroy_at(p.list);
            std::allocator_traits<list_alloc_t>::deallocate(list_alloc_, p.list, 1);
        } else {
            std::destroy_at(p.full);
            std::allocator_traits<full_alloc_t>::deallocate(full_alloc_, p.full, 1);
        }
    }

    // Deep copy
    ptr_t deep_copy(ptr_t src) {
        if (!src) return nullptr;

        T* eos_copy = nullptr;
        T* skip_eos_copy = nullptr;

        T* src_eos = src.get_eos();
        if (src_eos) eos_copy = alloc_value(*src_eos);

        if (src.is_eos()) {
            return build_eos(eos_copy);
        }

        T* src_skip_eos = src.get_skip_eos();
        if (src_skip_eos) skip_eos_copy = alloc_value(*src_skip_eos);

        if (src.is_skip()) {
            return build_skip(src.get_skip(), eos_copy, skip_eos_copy);
        }

        if (src.is_list()) {
            ptr_t dst = build_list(src.get_skip(), eos_copy, skip_eos_copy);
            dst.list->chars = src.list->chars;
            for (int i = 0; i < src.list->chars.count(); ++i) {
                dst.list->children[i].store(deep_copy(src.list->children[i].load()));
            }
            return dst;
        }

        // FULL
        ptr_t dst = build_full(src.get_skip(), eos_copy, skip_eos_copy);
        dst.full->valid = src.full->valid;
        for (int i = 0; i < 256; ++i) {
            if (src.full->valid.test(static_cast<unsigned char>(i))) {
                dst.full->children[i].store(deep_copy(src.full->children[i].load()));
            }
        }
        return dst;
    }

    // Free entire subtree
    void free_subtree(ptr_t p) noexcept {
        if (!p) return;

        free_value(p.get_eos());

        if (p.is_eos()) {
            deallocate_node(p);
            return;
        }

        free_value(p.get_skip_eos());

        if (p.is_list()) {
            for (int i = 0; i < p.list->chars.count(); ++i) {
                free_subtree(p.list->children[i].load());
            }
        } else if (p.is_full()) {
            for (int i = 0; i < 256; ++i) {
                if (p.full->valid.test(static_cast<unsigned char>(i))) {
                    free_subtree(p.full->children[i].load());
                }
            }
        }

        deallocate_node(p);
    }
};

}  // namespace gteitelbaum
