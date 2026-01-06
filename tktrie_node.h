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
template <typename T, bool THREADED, typename Allocator> struct list_node;
template <typename T, bool THREADED, typename Allocator> struct full_node;
template <typename T, bool THREADED, typename Allocator> class node_builder;

// Forward declare sentinel getters
template <typename T, bool THREADED, typename Allocator>
node_base<T, THREADED, Allocator>* get_not_found_sentinel() noexcept;

template <typename T, bool THREADED, typename Allocator>
node_base<T, THREADED, Allocator>* get_retry_sentinel() noexcept;

// =============================================================================
// ATOMIC_NODE_PTR - uses atomic_storage internally
// Defaults to NOT_FOUND sentinel - compiler optimizes array initialization
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct atomic_node_ptr {
    using base_t = node_base<T, THREADED, Allocator>;
    using ptr_t = base_t*;
    
    std::atomic<ptr_t> ptr_;
    
    // Default constructor initializes to NOT_FOUND sentinel
    // Compiler optimizes std::array<atomic_node_ptr, N> initialization
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
    
    // Header access - simplified using atomic_storage
    uint64_t header() const noexcept { return header_.load(); }
    void set_header(uint64_t h) noexcept { header_.store(h); }
    
    // Type queries
    bool is_leaf() const noexcept { return gteitelbaum::is_leaf(header()); }
    uint64_t version() const noexcept { return get_version(header()); }
    
    void bump_version() noexcept {
        header_.store(gteitelbaum::bump_version(header_.load()));
    }
    
    void poison() noexcept {
        // Set the poison flag bit
        header_.store(header_.load() | FLAG_POISON);
    }
    
    bool is_poisoned() const noexcept {
        return is_poisoned_header(header());
    }
    
    bool is_skip() const noexcept { return header() & FLAG_SKIP; }
    bool is_list() const noexcept { return header() & FLAG_LIST; }
    bool is_full() const noexcept { return !(header() & (FLAG_SKIP | FLAG_LIST)); }
    
    // Downcasts
    skip_node<T, THREADED, Allocator>* as_skip() noexcept {
        return static_cast<skip_node<T, THREADED, Allocator>*>(this);
    }
    list_node<T, THREADED, Allocator>* as_list() noexcept {
        return static_cast<list_node<T, THREADED, Allocator>*>(this);
    }
    full_node<T, THREADED, Allocator>* as_full() noexcept {
        return static_cast<full_node<T, THREADED, Allocator>*>(this);
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
    
    // Common accessors - all node types have skip string
    std::string_view skip_str() const noexcept {
        if (is_skip()) return as_skip()->skip;
        if (is_list()) [[likely]] return as_list()->skip;
        return as_full()->skip;
    }
    
    int child_count() const noexcept {
        if (is_list()) [[likely]] return as_list()->chars.count();
        if (is_full()) return as_full()->valid.count();
        return 0;  // SKIP has no children
    }
};

// =============================================================================
// SKIP_NODE - skip string + value (always leaf)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct skip_node : node_base<T, THREADED, Allocator> {
    T leaf_value;
    std::string skip;
    
    skip_node() {}
    ~skip_node() {
        leaf_value.~T();
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
    
    // Helper to add a single child (for interior nodes)
    void add_child(unsigned char c, typename base_t::ptr_t child) {
        int idx = chars.add(c);
        children[idx].store(child);
    }
    
    // Helper to add two children at once (common in split operations)
    void add_two_children(unsigned char c1, typename base_t::ptr_t child1,
                          unsigned char c2, typename base_t::ptr_t child2) {
        chars.add(c1);
        chars.add(c2);
        children[0].store(child1);
        children[1].store(child2);
    }
    
    // Helper to add a leaf value entry
    int add_leaf_entry(unsigned char c, const T& value) {
        int idx = chars.add(c);
        construct_leaf_value(idx, value);
        return idx;
    }
    
    // Helper to move children from this node to another list_node (nulls out source)
    void move_children_to(list_node* dest) {
        dest->chars = chars;
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    // Helper to shift children down after removal at idx
    void shift_children_down(int idx) {
        int count = chars.count();
        for (int i = idx; i < count - 1; ++i) {
            children[i].store(children[i + 1].load());
        }
        children[count - 1].store(nullptr);
        chars.remove_at(idx);
    }
    
    // Helper to shift leaf values down after removal at idx
    void shift_leaf_values_down(int idx) {
        int count = chars.count();
        for (int i = idx; i < count - 1; ++i) {
            leaf_values[i] = leaf_values[i + 1];
        }
        destroy_leaf_value(count - 1);
        chars.remove_at(idx);
    }
    
    // Helper to copy leaf values to another list_node
    void copy_leaf_values_to(list_node* dest) {
        dest->chars = chars;
        int cnt = chars.count();
        for (int i = 0; i < cnt; ++i) {
            dest->construct_leaf_value(i, leaf_values[i]);
        }
    }
    
    // Helper to move interior node contents to another list_node (nulls out source eos_ptr)
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
    
    // Helper to move list interior to full_node (for LIST->FULL conversion)
    void move_interior_to_full(full_node<T, THREADED, Allocator>* dest) {
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
    
    // Helper to add a child (for interior nodes)
    // IMPORTANT: Store child BEFORE setting bitmap (memory ordering)
    void add_child(unsigned char c, typename base_t::ptr_t child) {
        children[c].store(child);
        valid.set(c);
    }
    
    // Helper to add a child atomically (for threaded interior nodes)
    // IMPORTANT: Store child BEFORE setting bitmap (memory ordering)
    template <bool THR>
    void add_child_atomic(unsigned char c, typename base_t::ptr_t child) {
        children[c].store(child);
        valid.template atomic_set<THR>(c);
    }
    
    // Helper to remove a child
    template <bool THR>
    void remove_child(unsigned char c) {
        valid.template atomic_clear<THR>(c);
        children[c].store(nullptr);
    }
    
    // Helper to add a leaf value entry
    void add_leaf_entry(unsigned char c, const T& value) {
        construct_leaf_value(c, value);
        valid.set(c);
    }
    
    // Helper to add a leaf value entry atomically
    template <bool THR>
    void add_leaf_entry_atomic(unsigned char c, const T& value) {
        construct_leaf_value(c, value);
        valid.template atomic_set<THR>(c);
    }
    
    // Helper to remove a leaf entry
    template <bool THR>
    void remove_leaf_entry(unsigned char c) {
        destroy_leaf_value(c);
        valid.template atomic_clear<THR>(c);
    }
    
    // Helper to copy leaf values to another full_node
    void copy_leaf_values_to(full_node* dest) {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->construct_leaf_value(c, leaf_values[c]);
        });
    }
    
    // Helper to move a child from this node to another (nulls out source)
    void move_child_to(unsigned char c, full_node* dest) {
        dest->children[c].store(children[c].load());
        dest->valid.set(c);
        children[c].store(nullptr);
    }
    
    // Helper to copy a child to another node
    void copy_child_to(unsigned char c, full_node* dest) {
        dest->valid.set(c);
        dest->children[c].store(children[c].load());
    }
    
    // Helper to move all children to another full_node (nulls out source)
    void move_all_children_to(full_node* dest) {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
            children[c].store(nullptr);
        });
    }
    
    // Helper to move interior node contents to another full_node (nulls out source eos_ptr)
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

// =============================================================================
// SENTINEL STORAGE TYPES - constinit compatible
// =============================================================================

// Storage for NOT_FOUND sentinel - interior LIST with empty skip/chars
// Layout compatible with list_node for reinterpret_cast
template <typename T, bool THREADED, typename Allocator>
struct not_found_list_storage : node_base<T, THREADED, Allocator> {
    T* eos_ptr = nullptr;
    std::string skip{};  // Empty - uses SSO, no allocation
    small_list chars{};  // count=0 - find() always returns -1
    std::array<void*, 7> dummy_children{};  // Never accessed (chars is empty)
    
    constexpr not_found_list_storage() noexcept 
        : node_base<T, THREADED, Allocator>(NOT_FOUND_SENTINEL_HEADER) {}
};

// Storage for RETRY sentinel - interior FULL with poison flag
// Layout compatible with full_node for reinterpret_cast
template <typename T, bool THREADED, typename Allocator>
struct retry_full_storage : node_base<T, THREADED, Allocator> {
    T* eos_ptr = nullptr;
    std::string skip{};  // Empty - uses SSO, no allocation
    bitmap256 valid{};   // All zeros - no valid children
    std::array<void*, 256> dummy_children{};  // All nullptr
    
    constexpr retry_full_storage() noexcept 
        : node_base<T, THREADED, Allocator>(RETRY_SENTINEL_HEADER) {}
};

// =============================================================================
// NOT_FOUND SENTINEL - returns nullptr from find_child naturally
// Reader hits this → interior loop → chars.find() → -1 → return nullptr
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
node_base<T, THREADED, Allocator>* get_not_found_sentinel() noexcept {
    constinit static not_found_list_storage<T, THREADED, Allocator> storage{};
    return reinterpret_cast<node_base<T, THREADED, Allocator>*>(&storage);
}

// =============================================================================
// RETRY SENTINEL - poisoned FULL node, blocks concurrent readers
// is_poisoned() check catches this and triggers retry
// =============================================================================

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
    using list_t = list_node<T, THREADED, Allocator>;
    using full_t = full_node<T, THREADED, Allocator>;
    
    // Check if pointer is a sentinel (never delete sentinels)
    static bool is_sentinel(ptr_t n) noexcept {
        return n == get_not_found_sentinel<T, THREADED, Allocator>() ||
               n == get_retry_sentinel<T, THREADED, Allocator>();
    }
    
    // Unified node deletion
    static void delete_node(ptr_t n) {
        if (!n || is_sentinel(n)) return;
        if (n->is_skip()) delete n->as_skip();
        else if (n->is_list()) [[likely]] delete n->as_list();
        else delete n->as_full();
    }
    
    // Leaf builder - SKIP is always leaf
    ptr_t make_leaf_skip(std::string_view sk, const T& value) {
        auto* n = new skip_t();
        n->set_header(make_header(true, FLAG_SKIP));
        n->skip = std::string(sk);
        new (&n->leaf_value) T(value);
        return n;
    }
    
    ptr_t make_leaf_list(std::string_view sk) {
        auto* n = new list_t();
        n->set_header(make_header(true, FLAG_LIST));
        n->skip = std::string(sk);
        return n;
    }
    
    ptr_t make_leaf_full(std::string_view sk) {
        auto* n = new full_t();
        n->set_header(make_header(true, 0));  // FULL = no type flags
        n->skip = std::string(sk);
        return n;
    }
    
    // Interior builders - only LIST and FULL can be interior
    // Note: children array auto-initializes to NOT_FOUND via atomic_node_ptr default ctor
    ptr_t make_interior_list(std::string_view sk) {
        auto* n = new list_t();
        n->set_header(make_header(false, FLAG_LIST));
        n->skip = std::string(sk);
        // children[] default-constructed to NOT_FOUND sentinel
        return n;
    }
    
    ptr_t make_interior_full(std::string_view sk) {
        auto* n = new full_t();
        n->set_header(make_header(false, 0));  // FULL = no type flags
        n->skip = std::string(sk);
        // children[] default-constructed to NOT_FOUND sentinel (compiler optimizes)
        return n;
    }
    
    // Recursive deallocation
    void dealloc_node(ptr_t n) {
        if (!n || is_sentinel(n)) return;
        
        if (!n->is_leaf()) {
            // Interior nodes are only LIST or FULL
            if (n->is_list()) [[likely]] {
                auto* ln = n->as_list();
                int cnt = ln->chars.count();
                for (int i = 0; i < cnt; ++i) {
                    dealloc_node(ln->children[i].load());
                }
                delete ln->eos_ptr;
            } else {
                auto* fn = n->as_full();
                fn->valid.for_each_set([this, fn](unsigned char c) {
                    dealloc_node(fn->children[c].load());
                });
                delete fn->eos_ptr;
            }
        }
        delete_node(n);
    }
    
    // Deep copy
    ptr_t deep_copy(ptr_t src) {
        if (!src || is_sentinel(src)) return nullptr;
        
        if (src->is_leaf()) {
            if (src->is_skip()) {
                return make_leaf_skip(src->as_skip()->skip, src->as_skip()->leaf_value);
            }
            if (src->is_list()) [[likely]] {
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
            // FULL leaf
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
        
        // Interior - only LIST or FULL
        if (src->is_list()) [[likely]] {
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
        // FULL interior
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
};

}  // namespace gteitelbaum
