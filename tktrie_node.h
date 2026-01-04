#pragma once

#include <array>
#include <atomic>
#include <memory>
#include <string>
#include <type_traits>

#include "tktrie_defines.h"

namespace gteitelbaum {

// Forward declarations
template <typename T, bool THREADED, typename Allocator, bool IS_LEAF, bool VAR_LEN> struct eos_node;
template <typename T, bool THREADED, typename Allocator, bool IS_LEAF, bool VAR_LEN> struct skip_node;
template <typename T, bool THREADED, typename Allocator, bool IS_LEAF, bool VAR_LEN> struct list_node;
template <typename T, bool THREADED, typename Allocator, bool IS_LEAF, bool VAR_LEN> struct full_node;
template <typename T, bool THREADED, typename Allocator, bool IS_LEAF, bool VAR_LEN> union node_ptr;
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN> class node_builder;

// Atomic pointer wrapper for T*
template <typename T, bool THREADED>
struct atomic_val_ptr {
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

// Atomic pointer wrapper for node_ptr
template <typename T, bool THREADED, typename Allocator, bool IS_LEAF, bool VAR_LEN>
struct atomic_node_ptr {
    using ptr_t = node_ptr<T, THREADED, Allocator, IS_LEAF, VAR_LEN>;
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

// =============================================================================
// VAR_LEN=true (FIXED_LEN==0): always IS_LEAF=false, uses T* pointers
// =============================================================================

// EOS: header + T* eos
template <typename T, bool THREADED, typename Allocator>
struct eos_node<T, THREADED, Allocator, false, true> {
    std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> header_;
    atomic_val_ptr<T, THREADED> eos;

    uint64_t header() const noexcept {
        if constexpr (THREADED) return header_.load(std::memory_order_acquire);
        else return header_;
    }
    void set_header(uint64_t h) noexcept {
        if constexpr (THREADED) header_.store(h, std::memory_order_release);
        else header_ = h;
    }
};

// SKIP: header + T* eos + string skip + T* skip_eos
template <typename T, bool THREADED, typename Allocator>
struct skip_node<T, THREADED, Allocator, false, true> {
    eos_node<T, THREADED, Allocator, false, true> base;
    std::string skip;
    atomic_val_ptr<T, THREADED> skip_eos;

    uint64_t header() const noexcept { return base.header(); }
    void set_header(uint64_t h) noexcept { base.set_header(h); }
};

// LIST: skip_node + chars + 7 child ptrs
template <typename T, bool THREADED, typename Allocator>
struct list_node<T, THREADED, Allocator, false, true> {
    using child_ptr_t = atomic_node_ptr<T, THREADED, Allocator, false, true>;
    skip_node<T, THREADED, Allocator, false, true> base;
    small_list chars;
    std::array<child_ptr_t, LIST_MAX> children;

    uint64_t header() const noexcept { return base.header(); }
    void set_header(uint64_t h) noexcept { base.set_header(h); }
};

// FULL: skip_node + bitmap + 256 child ptrs
template <typename T, bool THREADED, typename Allocator>
struct full_node<T, THREADED, Allocator, false, true> {
    using child_ptr_t = atomic_node_ptr<T, THREADED, Allocator, false, true>;
    skip_node<T, THREADED, Allocator, false, true> base;
    bitmap256 valid;
    std::array<child_ptr_t, 256> children;

    uint64_t header() const noexcept { return base.header(); }
    void set_header(uint64_t h) noexcept { base.set_header(h); }
};

// =============================================================================
// VAR_LEN=false, IS_LEAF=false: interior nodes, no values, ptr children
// =============================================================================

// Interior EOS: doesn't exist (interior can't terminate for FIXED_LEN>0)
// We still define it for completeness but it should never be allocated
template <typename T, bool THREADED, typename Allocator>
struct eos_node<T, THREADED, Allocator, false, false> {
    std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> header_;
    uint64_t header() const noexcept {
        if constexpr (THREADED) return header_.load(std::memory_order_acquire);
        else return header_;
    }
    void set_header(uint64_t h) noexcept {
        if constexpr (THREADED) header_.store(h, std::memory_order_release);
        else header_ = h;
    }
};

// Interior SKIP: header + string skip (no values)
template <typename T, bool THREADED, typename Allocator>
struct skip_node<T, THREADED, Allocator, false, false> {
    std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> header_;
    std::string skip;

    uint64_t header() const noexcept {
        if constexpr (THREADED) return header_.load(std::memory_order_acquire);
        else return header_;
    }
    void set_header(uint64_t h) noexcept {
        if constexpr (THREADED) header_.store(h, std::memory_order_release);
        else header_ = h;
    }
};

// Interior LIST: skip_node + chars + 7 child ptrs (children can be interior or leaf)
template <typename T, bool THREADED, typename Allocator>
struct list_node<T, THREADED, Allocator, false, false> {
    // Children point to interior nodes; leaf transition handled in code
    using child_ptr_t = atomic_node_ptr<T, THREADED, Allocator, false, false>;
    skip_node<T, THREADED, Allocator, false, false> base;
    small_list chars;
    std::array<child_ptr_t, LIST_MAX> children;

    uint64_t header() const noexcept { return base.header(); }
    void set_header(uint64_t h) noexcept { base.set_header(h); }
};

// Interior FULL: skip_node + bitmap + 256 child ptrs
template <typename T, bool THREADED, typename Allocator>
struct full_node<T, THREADED, Allocator, false, false> {
    using child_ptr_t = atomic_node_ptr<T, THREADED, Allocator, false, false>;
    skip_node<T, THREADED, Allocator, false, false> base;
    bitmap256 valid;
    std::array<child_ptr_t, 256> children;

    uint64_t header() const noexcept { return base.header(); }
    void set_header(uint64_t h) noexcept { base.set_header(h); }
};

// =============================================================================
// VAR_LEN=false, IS_LEAF=true: leaf nodes, inline T values, no children
// =============================================================================

// Leaf EOS: header + inline T
template <typename T, bool THREADED, typename Allocator>
struct eos_node<T, THREADED, Allocator, true, false> {
    std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> header_;
    T value;

    uint64_t header() const noexcept {
        if constexpr (THREADED) return header_.load(std::memory_order_acquire);
        else return header_;
    }
    void set_header(uint64_t h) noexcept {
        if constexpr (THREADED) header_.store(h, std::memory_order_release);
        else header_ = h;
    }
};

// Leaf SKIP: header + string skip + inline T
template <typename T, bool THREADED, typename Allocator>
struct skip_node<T, THREADED, Allocator, true, false> {
    std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> header_;
    std::string skip;
    T value;

    uint64_t header() const noexcept {
        if constexpr (THREADED) return header_.load(std::memory_order_acquire);
        else return header_;
    }
    void set_header(uint64_t h) noexcept {
        if constexpr (THREADED) header_.store(h, std::memory_order_release);
        else header_ = h;
    }
};

// Leaf LIST: header + string skip + chars + 7 inline T values
template <typename T, bool THREADED, typename Allocator>
struct list_node<T, THREADED, Allocator, true, false> {
    std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> header_;
    std::string skip;
    small_list chars;
    std::array<T, LIST_MAX> values;

    uint64_t header() const noexcept {
        if constexpr (THREADED) return header_.load(std::memory_order_acquire);
        else return header_;
    }
    void set_header(uint64_t h) noexcept {
        if constexpr (THREADED) header_.store(h, std::memory_order_release);
        else header_ = h;
    }
};

// Leaf FULL: header + string skip + bitmap + 256 inline T values
template <typename T, bool THREADED, typename Allocator>
struct full_node<T, THREADED, Allocator, true, false> {
    std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> header_;
    std::string skip;
    bitmap256 valid;
    std::array<T, 256> values;

    uint64_t header() const noexcept {
        if constexpr (THREADED) return header_.load(std::memory_order_acquire);
        else return header_;
    }
    void set_header(uint64_t h) noexcept {
        if constexpr (THREADED) header_.store(h, std::memory_order_release);
        else header_ = h;
    }
};

// =============================================================================
// Node pointer union
// =============================================================================

template <typename T, bool THREADED, typename Allocator, bool IS_LEAF, bool VAR_LEN>
union node_ptr {
    using eos_t = eos_node<T, THREADED, Allocator, IS_LEAF, VAR_LEN>;
    using skip_t = skip_node<T, THREADED, Allocator, IS_LEAF, VAR_LEN>;
    using list_t = list_node<T, THREADED, Allocator, IS_LEAF, VAR_LEN>;
    using full_t = full_node<T, THREADED, Allocator, IS_LEAF, VAR_LEN>;

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
    explicit node_ptr(void* p) noexcept : raw(p) {}

    explicit operator bool() const noexcept { return raw != nullptr; }
    bool operator==(std::nullptr_t) const noexcept { return raw == nullptr; }
    bool operator!=(std::nullptr_t) const noexcept { return raw != nullptr; }

    uint64_t header() const noexcept { return eos->header(); }
    uint64_t node_type() const noexcept { return get_type(header()); }
    bool is_eos() const noexcept { return is_eos_type(header()); }
    bool is_skip() const noexcept { return is_skip_type(header()); }
    bool is_list() const noexcept { return is_list_type(header()); }
    bool is_full() const noexcept { return is_full_type(header()); }
};

// =============================================================================
// VAR_LEN node_ptr accessors (uses T* pointers)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct var_len_accessors {
    using ptr_t = node_ptr<T, THREADED, Allocator, false, true>;

    static T* get_eos(ptr_t p) noexcept {
        return p.eos->eos.load();
    }
    static void set_eos(ptr_t p, T* val) noexcept {
        p.eos->eos.store(val);
    }
    static const std::string& get_skip(ptr_t p) noexcept {
        KTRIE_DEBUG_ASSERT(!p.is_eos());
        return p.skip->skip;
    }
    static T* get_skip_eos(ptr_t p) noexcept {
        KTRIE_DEBUG_ASSERT(!p.is_eos());
        return p.skip->skip_eos.load();
    }
    static void set_skip_eos(ptr_t p, T* val) noexcept {
        KTRIE_DEBUG_ASSERT(!p.is_eos());
        p.skip->skip_eos.store(val);
    }
    static int child_count(ptr_t p) noexcept {
        if (p.is_list()) return p.list->chars.count();
        if (p.is_full()) return p.full->valid.count();
        return 0;
    }
    static ptr_t find_child(ptr_t p, unsigned char c) noexcept {
        if (p.is_list()) {
            int idx = p.list->chars.find(c);
            if (idx < 0) return nullptr;
            return p.list->children[idx].load();
        }
        if (p.is_full()) {
            if (!p.full->valid.test(c)) return nullptr;
            return p.full->children[c].load();
        }
        return nullptr;
    }
    static unsigned char first_child_char(ptr_t p) noexcept {
        if (p.is_list()) return p.list->chars.smallest();
        if (p.is_full()) return p.full->valid.first_set();
        return 255;
    }
};

// =============================================================================
// FIXED_LEN interior node_ptr accessors (no values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct interior_accessors {
    using ptr_t = node_ptr<T, THREADED, Allocator, false, false>;

    static const std::string& get_skip(ptr_t p) noexcept {
        KTRIE_DEBUG_ASSERT(!p.is_eos());
        return p.skip->skip;
    }
    static int child_count(ptr_t p) noexcept {
        if (p.is_list()) return p.list->chars.count();
        if (p.is_full()) return p.full->valid.count();
        return 0;
    }
    static ptr_t find_child(ptr_t p, unsigned char c) noexcept {
        if (p.is_list()) {
            int idx = p.list->chars.find(c);
            if (idx < 0) return nullptr;
            return p.list->children[idx].load();
        }
        if (p.is_full()) {
            if (!p.full->valid.test(c)) return nullptr;
            return p.full->children[c].load();
        }
        return nullptr;
    }
    static unsigned char first_child_char(ptr_t p) noexcept {
        if (p.is_list()) return p.list->chars.smallest();
        if (p.is_full()) return p.full->valid.first_set();
        return 255;
    }
};

// =============================================================================
// FIXED_LEN leaf node_ptr accessors (inline T values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct leaf_accessors {
    using ptr_t = node_ptr<T, THREADED, Allocator, true, false>;

    static const T& get_value(ptr_t p) noexcept {
        KTRIE_DEBUG_ASSERT(p.is_eos() || p.is_skip());
        if (p.is_eos()) return p.eos->value;
        return p.skip->value;
    }
    static void set_value(ptr_t p, const T& val) noexcept {
        KTRIE_DEBUG_ASSERT(p.is_eos() || p.is_skip());
        if (p.is_eos()) p.eos->value = val;
        else p.skip->value = val;
    }
    static const std::string& get_skip(ptr_t p) noexcept {
        KTRIE_DEBUG_ASSERT(p.is_skip() || p.is_list() || p.is_full());
        if (p.is_skip()) return p.skip->skip;
        if (p.is_list()) return p.list->skip;
        return p.full->skip;
    }
    static int child_count(ptr_t p) noexcept {
        if (p.is_list()) return p.list->chars.count();
        if (p.is_full()) return p.full->valid.count();
        return 0;
    }
    static bool find_value(ptr_t p, unsigned char c, T& out) noexcept {
        if (p.is_list()) {
            int idx = p.list->chars.find(c);
            if (idx < 0) return false;
            out = p.list->values[idx];
            return true;
        }
        if (p.is_full()) {
            if (!p.full->valid.test(c)) return false;
            out = p.full->values[c];
            return true;
        }
        return false;
    }
    static unsigned char first_child_char(ptr_t p) noexcept {
        if (p.is_list()) return p.list->chars.smallest();
        if (p.is_full()) return p.full->valid.first_set();
        return 255;
    }
};

// =============================================================================
// Node builder
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
class node_builder {
    static constexpr bool VAR_LEN = (FIXED_LEN == 0);

public:
    using interior_ptr = node_ptr<T, THREADED, Allocator, false, VAR_LEN>;
    using leaf_ptr = node_ptr<T, THREADED, Allocator, !VAR_LEN, false>;
    // For VAR_LEN: leaf_ptr is same as interior_ptr (IS_LEAF=false)
    // For FIXED_LEN>0: leaf_ptr is IS_LEAF=true

    using eos_interior_t = eos_node<T, THREADED, Allocator, false, VAR_LEN>;
    using skip_interior_t = skip_node<T, THREADED, Allocator, false, VAR_LEN>;
    using list_interior_t = list_node<T, THREADED, Allocator, false, VAR_LEN>;
    using full_interior_t = full_node<T, THREADED, Allocator, false, VAR_LEN>;

    using eos_leaf_t = eos_node<T, THREADED, Allocator, true, false>;
    using skip_leaf_t = skip_node<T, THREADED, Allocator, true, false>;
    using list_leaf_t = list_node<T, THREADED, Allocator, true, false>;
    using full_leaf_t = full_node<T, THREADED, Allocator, true, false>;

    using alloc_traits = std::allocator_traits<Allocator>;
    using value_alloc_t = typename alloc_traits::template rebind_alloc<T>;

private:
    Allocator alloc_;
    value_alloc_t value_alloc_;

    template <typename NodeType>
    NodeType* alloc_node() {
        using node_alloc_t = typename alloc_traits::template rebind_alloc<NodeType>;
        node_alloc_t node_alloc(alloc_);
        NodeType* n = std::allocator_traits<node_alloc_t>::allocate(node_alloc, 1);
        std::construct_at(n);
        return n;
    }

    template <typename NodeType>
    void dealloc_node(NodeType* n) noexcept {
        if (!n) return;
        using node_alloc_t = typename alloc_traits::template rebind_alloc<NodeType>;
        node_alloc_t node_alloc(alloc_);
        std::destroy_at(n);
        std::allocator_traits<node_alloc_t>::deallocate(node_alloc, n, 1);
    }

public:
    explicit node_builder(const Allocator& alloc = Allocator())
        : alloc_(alloc), value_alloc_(alloc) {}

    // Value allocation (for VAR_LEN)
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

    // =========================================================================
    // Interior node builders (for both VAR_LEN and FIXED_LEN)
    // =========================================================================

    interior_ptr build_interior_eos(T* eos_val = nullptr) {
        static_assert(VAR_LEN, "Interior EOS only valid for VAR_LEN");
        auto* n = alloc_node<eos_interior_t>();
        n->set_header(make_header(false, TYPE_EOS));
        n->eos.store(eos_val);
        return interior_ptr(n);
    }

    interior_ptr build_interior_skip(const std::string& skip_str, T* eos_val = nullptr, T* skip_eos_val = nullptr) {
        auto* n = alloc_node<skip_interior_t>();
        n->set_header(make_header(false, TYPE_SKIP));
        if constexpr (VAR_LEN) {
            n->base.eos.store(eos_val);
            n->skip = skip_str;
            n->skip_eos.store(skip_eos_val);
        } else {
            n->skip = skip_str;
            (void)eos_val; (void)skip_eos_val;
        }
        return interior_ptr(n);
    }

    interior_ptr build_interior_list(const std::string& skip_str, T* eos_val = nullptr, T* skip_eos_val = nullptr) {
        auto* n = alloc_node<list_interior_t>();
        n->set_header(make_header(false, TYPE_LIST));
        if constexpr (VAR_LEN) {
            n->base.base.eos.store(eos_val);
            n->base.skip = skip_str;
            n->base.skip_eos.store(skip_eos_val);
        } else {
            n->base.skip = skip_str;
            (void)eos_val; (void)skip_eos_val;
        }
        n->chars = small_list{};
        for (auto& c : n->children) c.store(nullptr);
        return interior_ptr(n);
    }

    interior_ptr build_interior_full(const std::string& skip_str, T* eos_val = nullptr, T* skip_eos_val = nullptr) {
        auto* n = alloc_node<full_interior_t>();
        n->set_header(make_header(false, TYPE_FULL));
        if constexpr (VAR_LEN) {
            n->base.base.eos.store(eos_val);
            n->base.skip = skip_str;
            n->base.skip_eos.store(skip_eos_val);
        } else {
            n->base.skip = skip_str;
            (void)eos_val; (void)skip_eos_val;
        }
        n->valid = bitmap256{};
        for (auto& c : n->children) c.store(nullptr);
        return interior_ptr(n);
    }

    // =========================================================================
    // Leaf node builders (only for FIXED_LEN > 0)
    // =========================================================================

    leaf_ptr build_leaf_eos(const T& val) {
        static_assert(!VAR_LEN, "Leaf EOS only valid for FIXED_LEN>0");
        auto* n = alloc_node<eos_leaf_t>();
        n->set_header(make_header(true, TYPE_EOS));
        n->value = val;
        return leaf_ptr(n);
    }

    leaf_ptr build_leaf_skip(const std::string& skip_str, const T& val) {
        static_assert(!VAR_LEN, "Leaf SKIP only valid for FIXED_LEN>0");
        auto* n = alloc_node<skip_leaf_t>();
        n->set_header(make_header(true, TYPE_SKIP));
        n->skip = skip_str;
        n->value = val;
        return leaf_ptr(n);
    }

    leaf_ptr build_leaf_list(const std::string& skip_str) {
        static_assert(!VAR_LEN, "Leaf LIST only valid for FIXED_LEN>0");
        auto* n = alloc_node<list_leaf_t>();
        n->set_header(make_header(true, TYPE_LIST));
        n->skip = skip_str;
        n->chars = small_list{};
        return leaf_ptr(n);
    }

    leaf_ptr build_leaf_full(const std::string& skip_str) {
        static_assert(!VAR_LEN, "Leaf FULL only valid for FIXED_LEN>0");
        auto* n = alloc_node<full_leaf_t>();
        n->set_header(make_header(true, TYPE_FULL));
        n->skip = skip_str;
        n->valid = bitmap256{};
        return leaf_ptr(n);
    }

    // =========================================================================
    // Deallocate
    // =========================================================================

    void deallocate_interior(interior_ptr p) noexcept {
        if (!p) return;
        if (p.is_eos()) dealloc_node(p.eos);
        else if (p.is_skip()) dealloc_node(p.skip);
        else if (p.is_list()) dealloc_node(p.list);
        else dealloc_node(p.full);
    }

    void deallocate_leaf(leaf_ptr p) noexcept {
        if constexpr (VAR_LEN) {
            deallocate_interior(p);
        } else {
            if (!p) return;
            if (p.is_eos()) dealloc_node(p.eos);
            else if (p.is_skip()) dealloc_node(p.skip);
            else if (p.is_list()) dealloc_node(p.list);
            else dealloc_node(p.full);
        }
    }

    // Free interior subtree (for VAR_LEN or interior of FIXED_LEN)
    void free_interior_subtree(interior_ptr p) noexcept {
        if (!p) return;

        if constexpr (VAR_LEN) {
            free_value(p.eos->eos.load());
            if (!p.is_eos()) {
                free_value(p.skip->skip_eos.load());
            }
        }

        if (p.is_list()) {
            int n = p.list->chars.count();
            for (int i = 0; i < n; ++i) {
                free_interior_subtree(p.list->children[i].load());
            }
        } else if (p.is_full()) {
            for (int i = 0; i < 256; ++i) {
                if (p.full->valid.test(static_cast<unsigned char>(i))) {
                    free_interior_subtree(p.full->children[i].load());
                }
            }
        }
        deallocate_interior(p);
    }
};

}  // namespace gteitelbaum
