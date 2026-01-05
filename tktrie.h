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
    using eos_t = eos_node<T, THREADED, Allocator>;
    using skip_t = skip_node<T, THREADED, Allocator>;
    using list_t = list_node<T, THREADED, Allocator>;
    using full_t = full_node<T, THREADED, Allocator>;
    using iterator = tktrie_iterator<Key, T, THREADED, Allocator>;
    using mutex_t = std::conditional_t<THREADED, std::mutex, empty_mutex>;

    // -------------------------------------------------------------------------
    // Insert types
    // -------------------------------------------------------------------------
    struct insert_result {
        ptr_t new_node = nullptr;
        std::vector<ptr_t> old_nodes;
        bool inserted = false;
        bool in_place = false;
    };

    struct path_entry {
        ptr_t node;
        uint64_t version;
        unsigned char edge;
    };

    enum class spec_op {
        EXISTS, IN_PLACE_LEAF, IN_PLACE_INTERIOR, EMPTY_TREE,
        DEMOTE_LEAF_EOS, SPLIT_LEAF_SKIP, PREFIX_LEAF_SKIP, EXTEND_LEAF_SKIP,
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
    // Erase types
    // -------------------------------------------------------------------------
    struct erase_result {
        ptr_t new_node = nullptr;
        std::vector<ptr_t> old_nodes;
        bool erased = false;
        bool deleted_subtree = false;
    };

    enum class erase_op {
        NOT_FOUND, DELETE_LEAF_EOS, DELETE_LEAF_SKIP, DELETE_LAST_LEAF_LIST,
        IN_PLACE_LEAF_LIST, IN_PLACE_LEAF_FULL, DELETE_EOS_INTERIOR,
        IN_PLACE_INTERIOR_LIST, IN_PLACE_INTERIOR_FULL, COLLAPSE_AFTER_REMOVE,
    };

    struct erase_spec_info {
        static constexpr int MAX_PATH = 64;
        path_entry path[MAX_PATH];
        int path_len = 0;
        erase_op op = erase_op::NOT_FOUND;
        ptr_t target = nullptr;
        uint64_t target_version = 0;
        unsigned char c = 0;
        ptr_t collapse_child = nullptr;
        uint64_t collapse_child_version = 0;
        unsigned char collapse_char = 0;
        std::string target_skip;
        std::string child_skip;
        // Parent collapse info
        ptr_t parent = nullptr;
        uint64_t parent_version = 0;
        unsigned char parent_edge = 0;
        std::string parent_skip;
        ptr_t parent_collapse_child = nullptr;
        uint64_t parent_collapse_child_version = 0;
        unsigned char parent_collapse_char = 0;
        std::string parent_child_skip;
    };

    struct erase_pre_alloc {
        ptr_t merged = nullptr;
        ptr_t parent_merged = nullptr;
        void clear() { merged = nullptr; parent_merged = nullptr; }
    };

private:
    atomic_ptr root_;
    std::conditional_t<THREADED, std::atomic<size_t>, size_t> size_{0};
    mutable mutex_t mutex_;
    builder_t builder_;

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
    atomic_ptr* get_child_slot(ptr_t n, unsigned char c) noexcept;

    // -------------------------------------------------------------------------
    // Read operations
    // -------------------------------------------------------------------------
    bool read_impl(ptr_t n, std::string_view key, T& out) const noexcept;
    bool read_from_leaf(ptr_t leaf, std::string_view key, T& out) const noexcept;
    bool contains_impl(ptr_t n, std::string_view key) const noexcept;

    // -------------------------------------------------------------------------
    // Insert operations
    // -------------------------------------------------------------------------
    insert_result insert_impl(atomic_ptr* slot, ptr_t n, std::string_view key, const T& value);
    insert_result insert_into_leaf(atomic_ptr* slot, ptr_t leaf, std::string_view key, const T& value);
    insert_result insert_into_interior(atomic_ptr* slot, ptr_t n, std::string_view key, const T& value);
    ptr_t create_leaf_for_key(std::string_view key, const T& value);
    insert_result demote_leaf_eos(ptr_t leaf, std::string_view key, const T& value);
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
    pre_alloc allocate_speculative(const speculative_info& info, const T& value);
    bool validate_path(const speculative_info& info) const noexcept;
    atomic_ptr* find_slot_for_commit(const speculative_info& info) noexcept;
    bool commit_speculative(speculative_info& info, pre_alloc& alloc, const T& value);
    void dealloc_speculation(pre_alloc& alloc);
    std::pair<iterator, bool> insert_locked(const Key& key, std::string_view kb, const T& value);

    // -------------------------------------------------------------------------
    // Erase operations
    // -------------------------------------------------------------------------
    erase_spec_info probe_erase(ptr_t n, std::string_view key) const noexcept;
    void capture_parent_collapse_info(erase_spec_info& info) const noexcept;
    bool check_collapse_needed(ptr_t parent, unsigned char removed_c, unsigned char& collapse_c, ptr_t& collapse_child) const noexcept;
    ptr_t allocate_collapse_node(const erase_spec_info& info);
    ptr_t allocate_parent_collapse_node(const erase_spec_info& info);
    erase_pre_alloc allocate_erase_speculative(const erase_spec_info& info);
    void dealloc_erase_speculation(erase_pre_alloc& alloc);
    void fill_collapse_node(ptr_t merged, ptr_t child);
    bool validate_erase_path(const erase_spec_info& info) const noexcept;
    bool do_inplace_leaf_list_erase(ptr_t leaf, unsigned char c, uint64_t expected_version);
    bool do_inplace_leaf_full_erase(ptr_t leaf, unsigned char c, uint64_t expected_version);
    bool do_inplace_interior_list_erase(ptr_t n, unsigned char c, uint64_t expected_version);
    bool do_inplace_interior_full_erase(ptr_t n, unsigned char c, uint64_t expected_version);
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
    size_t size() const noexcept;
    bool empty() const noexcept;
    bool contains(const Key& key) const;
    std::pair<iterator, bool> insert(const std::pair<const Key, T>& kv);
    bool erase(const Key& key);
    iterator find(const Key& key) const;
    iterator end() const noexcept;
    
    // Call when trie is quiescent (no concurrent operations) to free retired nodes
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
