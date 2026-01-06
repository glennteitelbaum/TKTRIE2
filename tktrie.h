#pragma once

#include <cstring>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_node.h"
#include "tktrie_ebr.h"

namespace gteitelbaum {

// =============================================================================
// KEY TRAITS
// =============================================================================

template <typename Key> struct tktrie_traits;

template <>
struct tktrie_traits<std::string> {
    static std::string_view to_bytes(const std::string& k) noexcept { return k; }
    static std::string from_bytes(std::string_view b) { return std::string(b); }
};

template <typename T> requires std::is_integral_v<T>
struct tktrie_traits<T> {
    using unsigned_t = std::make_unsigned_t<T>;
    static std::string to_bytes(T k) {
        unsigned_t sortable;
        if constexpr (std::is_signed_v<T>)
            sortable = static_cast<unsigned_t>(k) ^ (unsigned_t{1} << (sizeof(T) * 8 - 1));
        else
            sortable = k;
        unsigned_t be = to_big_endian(sortable);
        char buf[sizeof(T)];
        std::memcpy(buf, &be, sizeof(T));
        return std::string(buf, sizeof(T));
    }
    static T from_bytes(std::string_view b) {
        unsigned_t be;
        std::memcpy(&be, b.data(), sizeof(T));
        unsigned_t sortable = from_big_endian(be);
        if constexpr (std::is_signed_v<T>)
            return static_cast<T>(sortable ^ (unsigned_t{1} << (sizeof(T) * 8 - 1)));
        else
            return static_cast<T>(sortable);
    }
};

// Forward declarations
template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie_iterator;

// =============================================================================
// TKTRIE CLASS DECLARATION
// =============================================================================

template <typename Key, typename T, bool THREADED = false, typename Allocator = std::allocator<uint64_t>>
class tktrie {
public:
    using traits = tktrie_traits<Key>;
    using ptr_t = node_base<T, THREADED, Allocator>*;
    using atomic_ptr = atomic_node_ptr<T, THREADED, Allocator>;
    using builder_t = node_builder<T, THREADED, Allocator>;
    using skip_t = skip_node<T, THREADED, Allocator>;
    using list_t = list_node<T, THREADED, Allocator>;
    using full_t = full_node<T, THREADED, Allocator>;
    using iterator = tktrie_iterator<Key, T, THREADED, Allocator>;
    using mutex_t = std::conditional_t<THREADED, std::mutex, empty_mutex>;

    // -------------------------------------------------------------------------
    // Result types
    // -------------------------------------------------------------------------
    struct insert_result {
        ptr_t new_node = nullptr;
        std::vector<ptr_t> old_nodes;
        bool inserted = false;
        bool in_place = false;
    };

    struct erase_result {
        ptr_t new_node = nullptr;
        std::vector<ptr_t> old_nodes;
        bool erased = false;
        bool deleted_subtree = false;
    };

    struct path_entry {
        ptr_t node;
        uint64_t version;
        unsigned char edge;
    };

    // -------------------------------------------------------------------------
    // Optimistic read types (for lock-free reads)
    // -------------------------------------------------------------------------
    struct read_path {
        static constexpr int MAX_DEPTH = 64;
        ptr_t nodes[MAX_DEPTH];
        uint64_t versions[MAX_DEPTH];
        int len = 0;
        
        void clear() noexcept { len = 0; }
        bool push(ptr_t n) noexcept {
            if (len >= MAX_DEPTH) return false;
            nodes[len] = n;
            versions[len] = n->version();
            ++len;
            return true;
        }
    };

    // -------------------------------------------------------------------------
    // Speculative insert types
    // -------------------------------------------------------------------------
    enum class spec_op {
        EXISTS, IN_PLACE_LEAF, IN_PLACE_INTERIOR, EMPTY_TREE,
        SPLIT_LEAF_SKIP, PREFIX_LEAF_SKIP, EXTEND_LEAF_SKIP,
        SPLIT_LEAF_LIST, PREFIX_LEAF_LIST, ADD_EOS_LEAF_LIST, LIST_TO_FULL_LEAF,
        DEMOTE_LEAF_LIST, SPLIT_INTERIOR, PREFIX_INTERIOR, ADD_CHILD_CONVERT,
    };

    struct speculative_info {
        static constexpr int MAX_PATH = 64;
        path_entry path[MAX_PATH];
        int path_len = 0;
        spec_op op = spec_op::EXISTS;
        ptr_t target = nullptr;
        uint64_t target_version = 0;
        unsigned char c = 0;
        bool is_eos = false;
        size_t match_pos = 0;
        std::string target_skip;
        std::string remaining_key;
    };

    struct pre_alloc {
        ptr_t nodes[8] = {};
        int count = 0;
        ptr_t root_replacement = nullptr;
        T* in_place_eos = nullptr;
        void add(ptr_t n) { if (n) nodes[count++] = n; }
        void clear() { for (int i = 0; i < count; ++i) nodes[i] = nullptr; count = 0; root_replacement = nullptr; in_place_eos = nullptr; }
    };

    // -------------------------------------------------------------------------
    // Speculative erase types
    // -------------------------------------------------------------------------
    enum class erase_op {
        NOT_FOUND,
        IN_PLACE_LEAF_LIST, IN_PLACE_LEAF_FULL,
    };

    struct erase_spec_info {
        static constexpr int MAX_PATH = 64;
        path_entry path[MAX_PATH];
        int path_len = 0;
        erase_op op = erase_op::NOT_FOUND;
        ptr_t target = nullptr;
        uint64_t target_version = 0;
        unsigned char c = 0;
    };

    // -------------------------------------------------------------------------
    // Per-trie EBR retired node tracking
    // -------------------------------------------------------------------------
    struct retired_node {
        ptr_t ptr;
        uint64_t epoch;
    };

private:
    atomic_ptr root_;
    atomic_counter<THREADED> size_;
    mutable mutex_t mutex_;  // mutable so const readers can lock for sentinel wait
    builder_t builder_;
    
    // Per-trie EBR state (only used when THREADED=true)
    mutable std::conditional_t<THREADED, std::mutex, empty_mutex> ebr_mutex_;
    std::conditional_t<THREADED, std::vector<retired_node>, int> retired_;  // int placeholder for non-threaded
    
    // Per-trie EBR methods
    void ebr_retire(ptr_t n, uint64_t epoch);
    void ebr_try_reclaim();

    // -------------------------------------------------------------------------
    // Static helpers
    // -------------------------------------------------------------------------
    static void node_deleter(void* ptr);
    static std::string_view get_skip(ptr_t n) noexcept;
    static T* get_eos_ptr(ptr_t n) noexcept;
    static void set_eos_ptr(ptr_t n, T* p) noexcept;

    // -------------------------------------------------------------------------
    // Instance helpers
    // -------------------------------------------------------------------------
    void retire_node(ptr_t n);
    ptr_t find_child(ptr_t n, unsigned char c) const noexcept;
    std::pair<ptr_t, bool> find_child_wait(ptr_t n, unsigned char c) const noexcept;
    atomic_ptr* get_child_slot(ptr_t n, unsigned char c) noexcept;

    // -------------------------------------------------------------------------
    // Read operations
    // -------------------------------------------------------------------------
    bool read_impl(ptr_t n, std::string_view key, T& out) const noexcept;
    bool read_from_leaf(ptr_t leaf, std::string_view key, T& out) const noexcept;
    bool contains_impl(ptr_t n, std::string_view key) const noexcept;
    
    // Optimistic read operations (lock-free fast path)
    bool read_impl_optimistic(ptr_t n, std::string_view key, T& out, read_path& path) const noexcept;
    bool read_from_leaf_optimistic(ptr_t leaf, std::string_view key, T& out, read_path& path) const noexcept;
    bool validate_read_path(const read_path& path) const noexcept;

    // -------------------------------------------------------------------------
    // Insert operations
    // -------------------------------------------------------------------------
    insert_result insert_impl(atomic_ptr* slot, ptr_t n, std::string_view key, const T& value);
    insert_result insert_into_leaf(atomic_ptr* slot, ptr_t leaf, std::string_view key, const T& value);
    insert_result insert_into_interior(atomic_ptr* slot, ptr_t n, std::string_view key, const T& value);
    ptr_t create_leaf_for_key(std::string_view key, const T& value);
    insert_result split_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m);
    insert_result prefix_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m);
    insert_result extend_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m);
    insert_result split_leaf_list(ptr_t leaf, std::string_view key, const T& value, size_t m);
    insert_result prefix_leaf_list(ptr_t leaf, std::string_view key, const T& value, size_t m);
    ptr_t clone_leaf_with_skip(ptr_t leaf, std::string_view new_skip);
    insert_result add_eos_to_leaf_list(ptr_t leaf, const T& value);
    insert_result add_char_to_leaf(ptr_t leaf, unsigned char c, const T& value);
    insert_result demote_leaf_list(ptr_t leaf, std::string_view key, const T& value);
    insert_result split_interior(ptr_t n, std::string_view key, const T& value, size_t m);
    ptr_t clone_interior_with_skip(ptr_t n, std::string_view new_skip);
    insert_result prefix_interior(ptr_t n, std::string_view key, const T& value, size_t m);
    insert_result set_interior_eos(ptr_t n, const T& value);
    insert_result add_child_to_interior(ptr_t n, unsigned char c, std::string_view remaining, const T& value);

    // -------------------------------------------------------------------------
    // Speculative insert operations
    // -------------------------------------------------------------------------
    speculative_info probe_speculative(ptr_t n, std::string_view key) const noexcept;
    speculative_info probe_leaf_speculative(ptr_t n, std::string_view key, speculative_info& info) const noexcept;
    pre_alloc allocate_speculative(const speculative_info& info, const T& value);
    bool validate_path(const speculative_info& info) const noexcept;
    atomic_ptr* find_slot_for_commit(const speculative_info& info) noexcept;
    atomic_ptr* get_verified_slot(const speculative_info& info) noexcept;
    void commit_to_slot(atomic_ptr* slot, ptr_t new_node, const speculative_info& info) noexcept;
    bool commit_speculative(speculative_info& info, pre_alloc& alloc, const T& value);
    void dealloc_speculation(pre_alloc& alloc);
    std::pair<iterator, bool> insert_locked(const Key& key, std::string_view kb, const T& value);

    // -------------------------------------------------------------------------
    // Erase operations
    // -------------------------------------------------------------------------
    erase_spec_info probe_erase(ptr_t n, std::string_view key) const noexcept;
    erase_spec_info probe_leaf_erase(ptr_t n, std::string_view key, erase_spec_info& info) const noexcept;
    bool do_inplace_leaf_list_erase(ptr_t leaf, unsigned char c, uint64_t expected_version);
    bool do_inplace_leaf_full_erase(ptr_t leaf, unsigned char c, uint64_t expected_version);
    bool erase_locked(std::string_view kb);
    erase_result erase_impl(atomic_ptr* slot, ptr_t n, std::string_view key);
    erase_result erase_from_leaf(ptr_t leaf, std::string_view key);
    erase_result erase_from_interior(ptr_t n, std::string_view key);
    erase_result try_collapse_interior(ptr_t n);
    erase_result try_collapse_after_child_removal(ptr_t n, unsigned char removed_c, erase_result& child_res);
    erase_result collapse_single_child(ptr_t n, unsigned char c, ptr_t child, erase_result& res);

public:
    // -------------------------------------------------------------------------
    // Constructors / Destructor
    // -------------------------------------------------------------------------
    tktrie() = default;
    ~tktrie();
    tktrie(const tktrie& other);
    tktrie& operator=(const tktrie& other);
    tktrie(tktrie&& other) noexcept;
    tktrie& operator=(tktrie&& other) noexcept;

    // -------------------------------------------------------------------------
    // Public interface
    // -------------------------------------------------------------------------
    void clear();
    size_t size() const noexcept { return size_.load(); }
    bool empty() const noexcept { return size() == 0; }
    bool contains(const Key& key) const;
    std::pair<iterator, bool> insert(const std::pair<const Key, T>& kv);
    bool erase(const Key& key);
    iterator find(const Key& key) const;
    iterator end() const noexcept { return iterator(); }
    void reclaim_retired() noexcept;
};

// =============================================================================
// TKTRIE_ITERATOR CLASS
// =============================================================================

template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie_iterator {
public:
    using trie_t = tktrie<Key, T, THREADED, Allocator>;
    using traits = tktrie_traits<Key>;

private:
    const trie_t* trie_ = nullptr;
    std::string key_bytes_;
    T value_{};
    bool valid_ = false;

public:
    tktrie_iterator() = default;
    tktrie_iterator(const trie_t* t, const std::string& kb, const T& v)
        : trie_(t), key_bytes_(kb), value_(v), valid_(true) {}

    Key key() const { return traits::from_bytes(key_bytes_); }
    const T& value() const { return value_; }
    bool valid() const { return valid_; }
    explicit operator bool() const { return valid_; }

    bool operator==(const tktrie_iterator& o) const {
        if ((!valid_) & (!o.valid_)) return true;
        return (valid_ == o.valid_) & (key_bytes_ == o.key_bytes_);
    }
    bool operator!=(const tktrie_iterator& o) const { return !(*this == o); }
};

// =============================================================================
// TYPE ALIASES
// =============================================================================

template <typename T, typename Allocator = std::allocator<uint64_t>>
using string_trie = tktrie<std::string, T, false, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_string_trie = tktrie<std::string, T, true, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using int32_trie = tktrie<int32_t, T, false, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_int32_trie = tktrie<int32_t, T, true, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using int64_trie = tktrie<int64_t, T, false, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_int64_trie = tktrie<int64_t, T, true, Allocator>;

}  // namespace gteitelbaum

// Include implementation
#include "tktrie_core.h"
