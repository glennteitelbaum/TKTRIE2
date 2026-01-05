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
    bool do_inplace_leaf_list_erase(ptr_t leaf, unsigned char c);
    bool do_inplace_leaf_full_erase(ptr_t leaf, unsigned char c);
    bool do_inplace_interior_list_erase(ptr_t n, unsigned char c);
    bool do_inplace_interior_full_erase(ptr_t n, unsigned char c);
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

// =============================================================================
// TKTRIE MEMBER FUNCTION DEFINITIONS
// =============================================================================

// Shorthand for template prefix
#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

// -----------------------------------------------------------------------------
// Static helpers
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
void TKTRIE_CLASS::node_deleter(void* ptr) {
    if (!ptr) return;
    ptr_t n = static_cast<ptr_t>(ptr);
    switch (n->type()) {
        case TYPE_EOS: delete n->as_eos(); break;
        case TYPE_SKIP: delete n->as_skip(); break;
        case TYPE_LIST: delete n->as_list(); break;
        case TYPE_FULL: delete n->as_full(); break;
    }
}

TKTRIE_TEMPLATE
std::string_view TKTRIE_CLASS::get_skip(ptr_t n) noexcept {
    if (n->type() == TYPE_EOS) return {};
    const std::string* skip_ptr = reinterpret_cast<const std::string*>(
        reinterpret_cast<const char*>(n) + NODE_SKIP_OFFSET);
    return *skip_ptr;
}

TKTRIE_TEMPLATE
T* TKTRIE_CLASS::get_eos_ptr(ptr_t n) noexcept {
    if (n->is_leaf()) return nullptr;
    return *reinterpret_cast<T**>(reinterpret_cast<char*>(n) + NODE_EOS_OFFSET);
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::set_eos_ptr(ptr_t n, T* p) noexcept {
    *reinterpret_cast<T**>(reinterpret_cast<char*>(n) + NODE_EOS_OFFSET) = p;
}

// -----------------------------------------------------------------------------
// Instance helpers
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
void TKTRIE_CLASS::retire_node(ptr_t n) {
    if (!n) return;
    if constexpr (THREADED) {
        ebr_global::instance().retire(n, node_deleter);
    } else {
        node_deleter(n);
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::find_child(ptr_t n, unsigned char c) const noexcept {
    if (n->is_list()) {
        int idx = n->as_list()->chars.find(c);
        return idx >= 0 ? n->as_list()->children[idx].load() : nullptr;
    }
    if (n->is_full() && n->as_full()->valid.test(c)) {
        return n->as_full()->children[c].load();
    }
    return nullptr;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::atomic_ptr* TKTRIE_CLASS::get_child_slot(ptr_t n, unsigned char c) noexcept {
    if (n->is_list()) {
        int idx = n->as_list()->chars.find(c);
        return idx >= 0 ? &n->as_list()->children[idx] : nullptr;
    }
    if (n->is_full() && n->as_full()->valid.test(c)) {
        return &n->as_full()->children[c];
    }
    return nullptr;
}

// -----------------------------------------------------------------------------
// Read operations
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_impl(ptr_t n, std::string_view key, T& out) const noexcept {
    while (n) {
        if (n->is_leaf()) return read_from_leaf(n, key, out);

        std::string_view skip = get_skip(n);
        size_t m = match_skip_impl(skip, key);
        if (m < skip.size()) return false;
        key.remove_prefix(m);

        if (key.empty()) {
            T* p = get_eos_ptr(n);
            if (p) { out = *p; return true; }
            return false;
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);

        ptr_t child = find_child(n, c);
        if (!child) return false;
        n = child;
    }
    return false;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::read_from_leaf(ptr_t leaf, std::string_view key, T& out) const noexcept {
    std::string_view skip = get_skip(leaf);
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) return false;
    key.remove_prefix(m);

    if (leaf->is_eos()) {
        if (!key.empty()) return false;
        out = leaf->as_eos()->leaf_value;
        return true;
    }
    if (leaf->is_skip()) {
        if (!key.empty()) return false;
        out = leaf->as_skip()->leaf_value;
        return true;
    }
    if (key.size() != 1) return false;

    unsigned char c = static_cast<unsigned char>(key[0]);
    if (leaf->is_list()) {
        int idx = leaf->as_list()->chars.find(c);
        if (idx < 0) return false;
        out = leaf->as_list()->leaf_values[idx];
        return true;
    }
    if (leaf->is_full()) {
        if (!leaf->as_full()->valid.test(c)) return false;
        out = leaf->as_full()->leaf_values[c];
        return true;
    }
    return false;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::contains_impl(ptr_t n, std::string_view key) const noexcept {
    T dummy;
    return read_impl(n, key, dummy);
}

// -----------------------------------------------------------------------------
// Public interface
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
TKTRIE_CLASS::~tktrie() { clear(); }

TKTRIE_TEMPLATE
TKTRIE_CLASS::tktrie(const tktrie& other) {
    ptr_t other_root = other.root_.load();
    if (other_root) root_.store(builder_.deep_copy(other_root));
    if constexpr (THREADED) size_.store(other.size_.load());
    else size_ = other.size_;
}

TKTRIE_TEMPLATE
TKTRIE_CLASS& TKTRIE_CLASS::operator=(const tktrie& other) {
    if (this != &other) {
        clear();
        ptr_t other_root = other.root_.load();
        if (other_root) root_.store(builder_.deep_copy(other_root));
        if constexpr (THREADED) size_.store(other.size_.load());
        else size_ = other.size_;
    }
    return *this;
}

TKTRIE_TEMPLATE
TKTRIE_CLASS::tktrie(tktrie&& other) noexcept {
    root_.store(other.root_.load());
    other.root_.store(nullptr);
    if constexpr (THREADED) {
        size_.store(other.size_.exchange(0));
    } else {
        size_ = other.size_;
        other.size_ = 0;
    }
}

TKTRIE_TEMPLATE
TKTRIE_CLASS& TKTRIE_CLASS::operator=(tktrie&& other) noexcept {
    if (this != &other) {
        clear();
        root_.store(other.root_.load());
        other.root_.store(nullptr);
        if constexpr (THREADED) {
            size_.store(other.size_.exchange(0));
        } else {
            size_ = other.size_;
            other.size_ = 0;
        }
    }
    return *this;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::clear() {
    ptr_t r = root_.load();
    root_.store(nullptr);
    if (r) builder_.dealloc_node(r);
    if constexpr (THREADED) size_.store(0);
    else size_ = 0;
}

TKTRIE_TEMPLATE
size_t TKTRIE_CLASS::size() const noexcept {
    if constexpr (THREADED) return size_.load();
    else return size_;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::empty() const noexcept { return size() == 0; }

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::contains(const Key& key) const {
    auto kb = traits::to_bytes(key);
    if constexpr (THREADED) {
        auto& slot = get_ebr_slot();
        auto guard = slot.get_guard();
        return contains_impl(root_.load(), kb);
    } else {
        return contains_impl(root_.load(), kb);
    }
}

TKTRIE_TEMPLATE
std::pair<typename TKTRIE_CLASS::iterator, bool> TKTRIE_CLASS::insert(const std::pair<const Key, T>& kv) {
    auto kb = traits::to_bytes(kv.first);
    return insert_locked(kv.first, kb, kv.second);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::erase(const Key& key) {
    auto kb = traits::to_bytes(key);
    return erase_locked(kb);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::iterator TKTRIE_CLASS::find(const Key& key) const {
    auto kb = traits::to_bytes(key);
    T value;
    if constexpr (THREADED) {
        auto& slot = get_ebr_slot();
        auto guard = slot.get_guard();
        if (read_impl(root_.load(), kb, value)) {
            return iterator(this, std::string(kb), value);
        }
    } else {
        if (read_impl(root_.load(), kb, value)) {
            return iterator(this, std::string(kb), value);
        }
    }
    return end();
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::iterator TKTRIE_CLASS::end() const noexcept { return iterator(); }

// -----------------------------------------------------------------------------
// Insert operations
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::insert_impl(
    atomic_ptr* slot, ptr_t n, std::string_view key, const T& value) {
    insert_result res;

    if (!n) {
        res.new_node = create_leaf_for_key(key, value);
        res.inserted = true;
        return res;
    }

    if (n->is_leaf()) return insert_into_leaf(slot, n, key, value);
    return insert_into_interior(slot, n, key, value);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::insert_into_leaf(
    atomic_ptr*, ptr_t leaf, std::string_view key, const T& value) {
    insert_result res;
    std::string_view leaf_skip = get_skip(leaf);

    if (leaf->is_eos()) {
        if (key.empty()) return res;
        return demote_leaf_eos(leaf, key, value);
    }

    if (leaf->is_skip()) {
        size_t m = match_skip_impl(leaf_skip, key);
        if ((m == leaf_skip.size()) & (m == key.size())) return res;
        if ((m < leaf_skip.size()) & (m < key.size())) return split_leaf_skip(leaf, key, value, m);
        if (m == key.size()) return prefix_leaf_skip(leaf, key, value, m);
        return extend_leaf_skip(leaf, key, value, m);
    }

    // LIST or FULL
    size_t m = match_skip_impl(leaf_skip, key);
    if ((m < leaf_skip.size()) & (m < key.size())) return split_leaf_list(leaf, key, value, m);
    if (m < leaf_skip.size()) return prefix_leaf_list(leaf, key, value, m);
    key.remove_prefix(m);

    if (key.empty()) return add_eos_to_leaf_list(leaf, value);
    if (key.size() == 1) {
        unsigned char c = static_cast<unsigned char>(key[0]);
        return add_char_to_leaf(leaf, c, value);
    }
    return demote_leaf_list(leaf, key, value);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::insert_into_interior(
    atomic_ptr*, ptr_t n, std::string_view key, const T& value) {
    insert_result res;
    std::string_view skip = get_skip(n);

    size_t m = match_skip_impl(skip, key);
    if ((m < skip.size()) & (m < key.size())) return split_interior(n, key, value, m);
    if (m < skip.size()) return prefix_interior(n, key, value, m);
    key.remove_prefix(m);

    if (key.empty()) return set_interior_eos(n, value);

    unsigned char c = static_cast<unsigned char>(key[0]);
    key.remove_prefix(1);

    ptr_t child = find_child(n, c);
    if (child) {
        atomic_ptr* child_slot = get_child_slot(n, c);
        auto child_res = insert_impl(child_slot, child, key, value);
        if (child_res.new_node && child_res.new_node != child) {
            child_slot->store(child_res.new_node);
        }
        res.inserted = child_res.inserted;
        res.in_place = child_res.in_place;
        res.old_nodes = std::move(child_res.old_nodes);
        return res;
    }

    return add_child_to_interior(n, c, key, value);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::create_leaf_for_key(std::string_view key, const T& value) {
    if (key.empty()) return builder_.make_leaf_eos(value);
    if (key.size() == 1) {
        ptr_t leaf = builder_.make_leaf_list("");
        unsigned char c = static_cast<unsigned char>(key[0]);
        leaf->as_list()->chars.add(c);
        leaf->as_list()->leaf_values[0] = value;
        return leaf;
    }
    ptr_t leaf = builder_.make_leaf_list(key.substr(0, key.size() - 1));
    unsigned char c = static_cast<unsigned char>(key.back());
    leaf->as_list()->chars.add(c);
    leaf->as_list()->leaf_values[0] = value;
    return leaf;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::demote_leaf_eos(
    ptr_t leaf, std::string_view key, const T& value) {
    insert_result res;
    ptr_t interior = builder_.make_interior_list("");
    interior->as_list()->eos_ptr = new T(leaf->as_eos()->leaf_value);

    unsigned char c = static_cast<unsigned char>(key[0]);
    ptr_t child = create_leaf_for_key(key.substr(1), value);
    interior->as_list()->chars.add(c);
    interior->as_list()->children[0].store(child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::split_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->as_skip()->skip;

    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);

    ptr_t interior = builder_.make_interior_list(common);
    ptr_t old_child = builder_.make_leaf_skip(old_skip.substr(m + 1), leaf->as_skip()->leaf_value);
    ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);

    interior->as_list()->chars.add(old_c);
    interior->as_list()->chars.add(new_c);
    interior->as_list()->children[0].store(old_child);
    interior->as_list()->children[1].store(new_child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->as_skip()->skip;

    ptr_t interior = builder_.make_interior_list(std::string(key));
    interior->as_list()->eos_ptr = new T(value);

    unsigned char c = static_cast<unsigned char>(old_skip[m]);
    ptr_t child = builder_.make_leaf_skip(old_skip.substr(m + 1), leaf->as_skip()->leaf_value);
    interior->as_list()->chars.add(c);
    interior->as_list()->children[0].store(child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::extend_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->as_skip()->skip;

    ptr_t interior = builder_.make_interior_list(std::string(old_skip));
    interior->as_list()->eos_ptr = new T(leaf->as_skip()->leaf_value);

    unsigned char c = static_cast<unsigned char>(key[m]);
    ptr_t child = create_leaf_for_key(key.substr(m + 1), value);
    interior->as_list()->chars.add(c);
    interior->as_list()->children[0].store(child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::split_leaf_list(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = get_skip(leaf);

    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);

    ptr_t interior = builder_.make_interior_list(common);

    ptr_t old_child = clone_leaf_with_skip(leaf, old_skip.substr(m + 1));
    ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);

    interior->as_list()->chars.add(old_c);
    interior->as_list()->chars.add(new_c);
    interior->as_list()->children[0].store(old_child);
    interior->as_list()->children[1].store(new_child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_leaf_list(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = get_skip(leaf);

    ptr_t interior = builder_.make_interior_list(std::string(key));
    interior->as_list()->eos_ptr = new T(value);

    unsigned char c = static_cast<unsigned char>(old_skip[m]);
    ptr_t old_child = clone_leaf_with_skip(leaf, old_skip.substr(m + 1));
    interior->as_list()->chars.add(c);
    interior->as_list()->children[0].store(old_child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::clone_leaf_with_skip(ptr_t leaf, std::string_view new_skip) {
    if (leaf->is_list()) {
        ptr_t n = builder_.make_leaf_list(new_skip);
        n->as_list()->chars = leaf->as_list()->chars;
        int cnt = leaf->as_list()->chars.count();
        for (int i = 0; i < cnt; ++i) {
            n->as_list()->leaf_values[i] = leaf->as_list()->leaf_values[i];
        }
        return n;
    }
    // FULL
    ptr_t n = builder_.make_leaf_full(new_skip);
    n->as_full()->valid = leaf->as_full()->valid;
    leaf->as_full()->valid.for_each_set([leaf, n](unsigned char c) {
        n->as_full()->leaf_values[c] = leaf->as_full()->leaf_values[c];
    });
    return n;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_eos_to_leaf_list(ptr_t leaf, const T& value) {
    insert_result res;
    std::string_view leaf_skip = get_skip(leaf);

    ptr_t interior = builder_.make_interior_list(leaf_skip);
    interior->as_list()->eos_ptr = new T(value);

    if (leaf->is_list()) {
        int cnt = leaf->as_list()->chars.count();
        for (int i = 0; i < cnt; ++i) {
            unsigned char c = leaf->as_list()->chars.char_at(i);
            ptr_t child = builder_.make_leaf_eos(leaf->as_list()->leaf_values[i]);
            interior->as_list()->chars.add(c);
            interior->as_list()->children[i].store(child);
        }
    } else {
        leaf->as_full()->valid.for_each_set([this, leaf, interior](unsigned char c) {
            ptr_t child = builder_.make_leaf_eos(leaf->as_full()->leaf_values[c]);
            int idx = interior->as_list()->chars.add(c);
            interior->as_list()->children[idx].store(child);
        });
    }

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_char_to_leaf(
    ptr_t leaf, unsigned char c, const T& value) {
    insert_result res;

    if (leaf->is_list()) {
        int idx = leaf->as_list()->chars.find(c);
        if (idx >= 0) return res;

        if (leaf->as_list()->chars.count() < LIST_MAX) {
            idx = leaf->as_list()->chars.add(c);
            leaf->as_list()->leaf_values[idx] = value;
            res.in_place = true;
            res.inserted = true;
            return res;
        }

        ptr_t full = builder_.make_leaf_full(leaf->as_list()->skip);
        for (int i = 0; i < leaf->as_list()->chars.count(); ++i) {
            unsigned char ch = leaf->as_list()->chars.char_at(i);
            full->as_full()->valid.set(ch);
            full->as_full()->leaf_values[ch] = leaf->as_list()->leaf_values[i];
        }
        full->as_full()->valid.set(c);
        full->as_full()->leaf_values[c] = value;

        res.new_node = full;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }

    // FULL
    if (leaf->as_full()->valid.test(c)) return res;
    leaf->as_full()->valid.template atomic_set<THREADED>(c);
    leaf->as_full()->leaf_values[c] = value;
    res.in_place = true;
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::demote_leaf_list(
    ptr_t leaf, std::string_view key, const T& value) {
    insert_result res;
    std::string_view leaf_skip = get_skip(leaf);
    unsigned char first_c = static_cast<unsigned char>(key[0]);

    ptr_t interior = builder_.make_interior_list(leaf_skip);

    if (leaf->is_list()) {
        for (int i = 0; i < leaf->as_list()->chars.count(); ++i) {
            unsigned char c = leaf->as_list()->chars.char_at(i);
            ptr_t child = builder_.make_leaf_eos(leaf->as_list()->leaf_values[i]);
            interior->as_list()->chars.add(c);
            interior->as_list()->children[i].store(child);
        }
    } else {
        leaf->as_full()->valid.for_each_set([this, leaf, interior](unsigned char c) {
            ptr_t child = builder_.make_leaf_eos(leaf->as_full()->leaf_values[c]);
            int idx = interior->as_list()->chars.add(c);
            interior->as_list()->children[idx].store(child);
        });
    }

    int existing_idx = interior->as_list()->chars.find(first_c);
    if (existing_idx >= 0) {
        ptr_t child = interior->as_list()->children[existing_idx].load();
        auto child_res = insert_impl(&interior->as_list()->children[existing_idx], child, key.substr(1), value);
        if (child_res.new_node) {
            interior->as_list()->children[existing_idx].store(child_res.new_node);
        }
        for (auto* old : child_res.old_nodes) res.old_nodes.push_back(old);
    } else {
        ptr_t child = create_leaf_for_key(key.substr(1), value);
        int idx = interior->as_list()->chars.add(first_c);
        interior->as_list()->children[idx].store(child);
    }

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::split_interior(
    ptr_t n, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = get_skip(n);

    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);

    ptr_t new_int = builder_.make_interior_list(common);
    ptr_t old_child = clone_interior_with_skip(n, old_skip.substr(m + 1));
    ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);

    new_int->as_list()->chars.add(old_c);
    new_int->as_list()->chars.add(new_c);
    new_int->as_list()->children[0].store(old_child);
    new_int->as_list()->children[1].store(new_child);

    res.new_node = new_int;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::clone_interior_with_skip(ptr_t n, std::string_view new_skip) {
    if (n->is_list()) {
        ptr_t clone = builder_.make_interior_list(new_skip);
        clone->as_list()->chars = n->as_list()->chars;
        clone->as_list()->eos_ptr = n->as_list()->eos_ptr;
        n->as_list()->eos_ptr = nullptr;
        for (int i = 0; i < n->as_list()->chars.count(); ++i) {
            clone->as_list()->children[i].store(n->as_list()->children[i].load());
            n->as_list()->children[i].store(nullptr);
        }
        return clone;
    }
    if (n->is_full()) {
        ptr_t clone = builder_.make_interior_full(new_skip);
        clone->as_full()->valid = n->as_full()->valid;
        clone->as_full()->eos_ptr = n->as_full()->eos_ptr;
        n->as_full()->eos_ptr = nullptr;
        for (int c = 0; c < 256; ++c) {
            if (n->as_full()->valid.test(static_cast<unsigned char>(c))) {
                clone->as_full()->children[c].store(n->as_full()->children[c].load());
                n->as_full()->children[c].store(nullptr);
            }
        }
        return clone;
    }
    // EOS or SKIP
    ptr_t clone = builder_.make_interior_skip(new_skip);
    clone->as_skip()->eos_ptr = get_eos_ptr(n);
    set_eos_ptr(n, nullptr);
    return clone;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_interior(
    ptr_t n, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = get_skip(n);

    ptr_t new_int = builder_.make_interior_list(std::string(key));
    new_int->as_list()->eos_ptr = new T(value);

    unsigned char c = static_cast<unsigned char>(old_skip[m]);
    ptr_t old_child = clone_interior_with_skip(n, old_skip.substr(m + 1));
    new_int->as_list()->chars.add(c);
    new_int->as_list()->children[0].store(old_child);

    res.new_node = new_int;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::set_interior_eos(ptr_t n, const T& value) {
    insert_result res;
    T* p = get_eos_ptr(n);
    if (p) return res;

    set_eos_ptr(n, new T(value));
    res.in_place = true;
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_child_to_interior(
    ptr_t n, unsigned char c, std::string_view remaining, const T& value) {
    insert_result res;
    ptr_t child = create_leaf_for_key(remaining, value);

    if (n->is_list()) {
        if (n->as_list()->chars.count() < LIST_MAX) {
            int idx = n->as_list()->chars.add(c);
            n->as_list()->children[idx].store(child);
            res.in_place = true;
            res.inserted = true;
            return res;
        }
        // Convert to FULL
        ptr_t full = builder_.make_interior_full(n->as_list()->skip);
        full->as_full()->eos_ptr = n->as_list()->eos_ptr;
        n->as_list()->eos_ptr = nullptr;
        for (int i = 0; i < n->as_list()->chars.count(); ++i) {
            unsigned char ch = n->as_list()->chars.char_at(i);
            full->as_full()->valid.set(ch);
            full->as_full()->children[ch].store(n->as_list()->children[i].load());
            n->as_list()->children[i].store(nullptr);
        }
        full->as_full()->valid.set(c);
        full->as_full()->children[c].store(child);

        res.new_node = full;
        res.old_nodes.push_back(n);
        res.inserted = true;
        return res;
    }

    if (n->is_full()) {
        n->as_full()->valid.template atomic_set<THREADED>(c);
        n->as_full()->children[c].store(child);
        res.in_place = true;
        res.inserted = true;
        return res;
    }

    // EOS or SKIP - convert to LIST
    ptr_t list = builder_.make_interior_list(get_skip(n));
    list->as_list()->eos_ptr = get_eos_ptr(n);
    set_eos_ptr(n, nullptr);
    list->as_list()->chars.add(c);
    list->as_list()->children[0].store(child);

    res.new_node = list;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}

// -----------------------------------------------------------------------------
// Speculative insert operations  
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::speculative_info TKTRIE_CLASS::probe_speculative(
    ptr_t n, std::string_view key) const noexcept {
    speculative_info info;
    info.remaining_key = std::string(key);

    if (!n) {
        info.op = spec_op::EMPTY_TREE;
        return info;
    }

    info.path[info.path_len++] = {n, n->version(), 0};

    while (n) {
        if (n->is_leaf()) {
            std::string_view skip = get_skip(n);
            size_t m = match_skip_impl(skip, key);

            if (n->is_eos()) {
                if (key.empty()) {
                    info.op = spec_op::EXISTS;
                    return info;
                }
                info.op = spec_op::DEMOTE_LEAF_EOS;
                info.target = n;
                info.target_version = n->version();
                info.remaining_key = std::string(key);
                return info;
            }

            if (n->is_skip()) {
                if ((m == skip.size()) & (m == key.size())) {
                    info.op = spec_op::EXISTS;
                    return info;
                }
                info.target = n;
                info.target_version = n->version();
                info.target_skip = std::string(skip);
                info.match_pos = m;

                if ((m < skip.size()) & (m < key.size())) {
                    info.op = spec_op::SPLIT_LEAF_SKIP;
                    info.remaining_key = std::string(key);
                    return info;
                }
                if (m == key.size()) {
                    info.op = spec_op::PREFIX_LEAF_SKIP;
                    info.remaining_key = std::string(key);
                    return info;
                }
                info.op = spec_op::EXTEND_LEAF_SKIP;
                info.remaining_key = std::string(key);
                return info;
            }

            // LIST or FULL leaf
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);

            if ((m < skip.size()) & (m < key.size())) {
                info.op = spec_op::SPLIT_LEAF_LIST;
                info.match_pos = m;
                info.remaining_key = std::string(key);
                return info;
            }
            if (m < skip.size()) {
                info.op = spec_op::PREFIX_LEAF_LIST;
                info.match_pos = m;
                info.remaining_key = std::string(key);
                return info;
            }
            key.remove_prefix(m);
            info.remaining_key = std::string(key);

            if (key.empty()) {
                info.op = spec_op::ADD_EOS_LEAF_LIST;
                return info;
            }
            if (key.size() != 1) {
                info.op = spec_op::DEMOTE_LEAF_LIST;
                return info;
            }

            unsigned char c = static_cast<unsigned char>(key[0]);
            info.c = c;

            if (n->is_list()) {
                if (n->as_list()->chars.find(c) >= 0) {
                    info.op = spec_op::EXISTS;
                    return info;
                }
                if (n->as_list()->chars.count() < LIST_MAX) {
                    info.op = spec_op::IN_PLACE_LEAF;
                    return info;
                }
                info.op = spec_op::LIST_TO_FULL_LEAF;
                return info;
            }
            // FULL
            if (n->as_full()->valid.test(c)) {
                info.op = spec_op::EXISTS;
                return info;
            }
            info.op = spec_op::IN_PLACE_LEAF;
            return info;
        }

        // Interior node
        std::string_view skip = get_skip(n);
        size_t m = match_skip_impl(skip, key);

        if ((m < skip.size()) & (m < key.size())) {
            info.op = spec_op::SPLIT_INTERIOR;
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            info.match_pos = m;
            info.remaining_key = std::string(key);
            return info;
        }
        if (m < skip.size()) {
            info.op = spec_op::PREFIX_INTERIOR;
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            info.match_pos = m;
            info.remaining_key = std::string(key);
            return info;
        }
        key.remove_prefix(m);

        if (key.empty()) {
            T* p = get_eos_ptr(n);
            if (p) {
                info.op = spec_op::EXISTS;
                return info;
            }
            info.op = spec_op::IN_PLACE_INTERIOR;
            info.target = n;
            info.target_version = n->version();
            info.is_eos = true;
            return info;
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = find_child(n, c);

        if (!child) {
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            info.c = c;
            info.remaining_key = std::string(key.substr(1));

            if (n->is_list() && n->as_list()->chars.count() < LIST_MAX) {
                info.op = spec_op::IN_PLACE_INTERIOR;
                return info;
            }
            if (n->is_full()) {
                info.op = spec_op::IN_PLACE_INTERIOR;
                return info;
            }
            info.op = spec_op::ADD_CHILD_CONVERT;
            return info;
        }

        key.remove_prefix(1);
        n = child;
        if (info.path_len < speculative_info::MAX_PATH) {
            info.path[info.path_len++] = {n, n->version(), c};
        }
    }

    info.op = spec_op::EMPTY_TREE;
    return info;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::pre_alloc TKTRIE_CLASS::allocate_speculative(
    const speculative_info& info, const T& value) {
    pre_alloc alloc;
    std::string_view key = info.remaining_key;
    std::string_view skip = info.target_skip;
    size_t m = info.match_pos;

    switch (info.op) {
    case spec_op::EMPTY_TREE: {
        alloc.root_replacement = create_leaf_for_key(key, value);
        alloc.add(alloc.root_replacement);
        break;
    }
    case spec_op::DEMOTE_LEAF_EOS: {
        ptr_t interior = builder_.make_interior_list("");
        interior->as_list()->eos_ptr = new T();
        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = create_leaf_for_key(key.substr(1), value);
        interior->as_list()->chars.add(c);
        interior->as_list()->children[0].store(child);
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::SPLIT_LEAF_SKIP: {
        std::string common(skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);

        ptr_t interior = builder_.make_interior_list(common);
        ptr_t old_child = builder_.make_leaf_skip(skip.substr(m + 1), T{});
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);

        interior->as_list()->chars.add(old_c);
        interior->as_list()->chars.add(new_c);
        interior->as_list()->children[0].store(old_child);
        interior->as_list()->children[1].store(new_child);

        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(old_child);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_LEAF_SKIP: {
        ptr_t interior = builder_.make_interior_list(std::string(key));
        interior->as_list()->eos_ptr = new T(value);

        unsigned char c = static_cast<unsigned char>(skip[m]);
        ptr_t child = builder_.make_leaf_skip(skip.substr(m + 1), T{});
        interior->as_list()->chars.add(c);
        interior->as_list()->children[0].store(child);

        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::EXTEND_LEAF_SKIP: {
        ptr_t interior = builder_.make_interior_list(std::string(skip));
        interior->as_list()->eos_ptr = new T();

        unsigned char c = static_cast<unsigned char>(key[m]);
        ptr_t child = create_leaf_for_key(key.substr(m + 1), value);
        interior->as_list()->chars.add(c);
        interior->as_list()->children[0].store(child);

        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::SPLIT_LEAF_LIST: {
        std::string common(skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);

        ptr_t interior = builder_.make_interior_list(common);
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);

        interior->as_list()->chars.add(old_c);
        interior->as_list()->chars.add(new_c);
        interior->as_list()->children[1].store(new_child);

        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_LEAF_LIST: {
        ptr_t interior = builder_.make_interior_list(std::string(key));
        interior->as_list()->eos_ptr = new T(value);

        unsigned char c = static_cast<unsigned char>(skip[m]);
        interior->as_list()->chars.add(c);

        alloc.root_replacement = interior;
        alloc.add(interior);
        break;
    }
    case spec_op::LIST_TO_FULL_LEAF: {
        ptr_t full = builder_.make_leaf_full(std::string(skip));
        full->as_full()->valid.set(info.c);
        full->as_full()->leaf_values[info.c] = value;

        alloc.root_replacement = full;
        alloc.add(full);
        break;
    }
    case spec_op::IN_PLACE_INTERIOR: {
        if (info.is_eos) {
            alloc.in_place_eos = new T(value);
        } else {
            ptr_t child = create_leaf_for_key(info.remaining_key, value);
            alloc.root_replacement = child;
            alloc.add(child);
        }
        break;
    }
    case spec_op::ADD_EOS_LEAF_LIST:
    case spec_op::DEMOTE_LEAF_LIST:
    case spec_op::ADD_CHILD_CONVERT:
    case spec_op::SPLIT_INTERIOR:
    case spec_op::PREFIX_INTERIOR:
        break;
    default:
        break;
    }

    return alloc;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::validate_path(const speculative_info& info) const noexcept {
    for (int i = 0; i < info.path_len; ++i) {
        if (info.path[i].node->version() != info.path[i].version) {
            return false;
        }
    }
    if (info.target && (info.path_len == 0 || info.path[info.path_len-1].node != info.target)) {
        if (info.target->version() != info.target_version) {
            return false;
        }
    }
    return true;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::atomic_ptr* TKTRIE_CLASS::find_slot_for_commit(
    const speculative_info& info) noexcept {
    if (info.path_len <= 1) {
        return &root_;
    }
    ptr_t parent = info.path[info.path_len - 2].node;
    unsigned char edge = info.path[info.path_len - 1].edge;
    return get_child_slot(parent, edge);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::commit_speculative(
    speculative_info& info, pre_alloc& alloc, const T& /*value*/) {
    switch (info.op) {
    case spec_op::EMPTY_TREE:
        root_.store(alloc.root_replacement);
        return true;

    case spec_op::DEMOTE_LEAF_EOS: {
        ptr_t interior = alloc.root_replacement;
        *interior->as_list()->eos_ptr = info.target->as_eos()->leaf_value;

        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::SPLIT_LEAF_SKIP: {
        ptr_t interior = alloc.root_replacement;
        ptr_t old_child = interior->as_list()->children[0].load();
        old_child->as_skip()->leaf_value = info.target->as_skip()->leaf_value;

        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::PREFIX_LEAF_SKIP: {
        ptr_t interior = alloc.root_replacement;
        ptr_t child = interior->as_list()->children[0].load();
        child->as_skip()->leaf_value = info.target->as_skip()->leaf_value;

        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::EXTEND_LEAF_SKIP: {
        ptr_t interior = alloc.root_replacement;
        *interior->as_list()->eos_ptr = info.target->as_skip()->leaf_value;

        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::SPLIT_LEAF_LIST: {
        ptr_t interior = alloc.root_replacement;
        std::string_view skip = info.target_skip;
        size_t m = info.match_pos;

        ptr_t old_child = clone_leaf_with_skip(info.target, skip.substr(m + 1));
        interior->as_list()->children[0].store(old_child);
        alloc.add(old_child);

        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::PREFIX_LEAF_LIST: {
        ptr_t interior = alloc.root_replacement;
        std::string_view skip = info.target_skip;
        size_t m = info.match_pos;

        ptr_t old_child = clone_leaf_with_skip(info.target, skip.substr(m + 1));
        interior->as_list()->children[0].store(old_child);
        alloc.add(old_child);

        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::LIST_TO_FULL_LEAF: {
        ptr_t full = alloc.root_replacement;
        auto* list = info.target->as_list();
        for (int i = 0; i < list->chars.count(); ++i) {
            unsigned char ch = list->chars.char_at(i);
            full->as_full()->valid.set(ch);
            full->as_full()->leaf_values[ch] = list->leaf_values[i];
        }

        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(full);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::IN_PLACE_LEAF: {
        ptr_t n = info.target;
        unsigned char c = info.c;

        if (n->is_list()) {
            if (n->as_list()->chars.find(c) >= 0) return false;
            if (n->as_list()->chars.count() >= LIST_MAX) return false;

            int idx = n->as_list()->chars.add(c);
            n->as_list()->leaf_values[idx] = alloc.root_replacement ? 
                alloc.root_replacement->as_eos()->leaf_value : T{};
            n->bump_version();

            if (alloc.root_replacement) {
                delete alloc.root_replacement->as_eos();
                alloc.clear();
            }
            return true;
        }
        // FULL
        if (n->as_full()->valid.test(c)) return false;
        n->as_full()->valid.template atomic_set<THREADED>(c);
        n->bump_version();
        return true;
    }
    case spec_op::IN_PLACE_INTERIOR: {
        ptr_t n = info.target;

        if (info.is_eos) {
            if (get_eos_ptr(n)) {
                delete alloc.in_place_eos;
                return false;
            }
            set_eos_ptr(n, alloc.in_place_eos);
            alloc.in_place_eos = nullptr;
            n->bump_version();
            return true;
        }

        unsigned char c = info.c;
        ptr_t child = alloc.root_replacement;

        if (n->is_list()) {
            if (n->as_list()->chars.find(c) >= 0) return false;
            if (n->as_list()->chars.count() >= LIST_MAX) return false;

            int idx = n->as_list()->chars.add(c);
            n->as_list()->children[idx].store(child);
            n->bump_version();
            return true;
        }
        if (n->is_full()) {
            if (n->as_full()->valid.test(c)) return false;
            n->as_full()->valid.template atomic_set<THREADED>(c);
            n->as_full()->children[c].store(child);
            n->bump_version();
            return true;
        }
        return false;
    }
    case spec_op::ADD_EOS_LEAF_LIST:
    case spec_op::DEMOTE_LEAF_LIST:
    case spec_op::SPLIT_INTERIOR:
    case spec_op::PREFIX_INTERIOR:
    case spec_op::ADD_CHILD_CONVERT:
        return false;

    default:
        return false;
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::dealloc_speculation(pre_alloc& alloc) {
    if (alloc.in_place_eos) {
        delete alloc.in_place_eos;
        alloc.in_place_eos = nullptr;
    }
    for (int i = 0; i < alloc.count; ++i) {
        ptr_t n = alloc.nodes[i];
        if (!n) continue;

        if (!n->is_leaf()) {
            T* eos = get_eos_ptr(n);
            if (eos) delete eos;
        }

        switch (n->type()) {
            case TYPE_EOS: delete n->as_eos(); break;
            case TYPE_SKIP: delete n->as_skip(); break;
            case TYPE_LIST: delete n->as_list(); break;
            case TYPE_FULL: delete n->as_full(); break;
        }
    }
    alloc.clear();
}

TKTRIE_TEMPLATE
std::pair<typename TKTRIE_CLASS::iterator, bool> TKTRIE_CLASS::insert_locked(
    const Key& key, std::string_view kb, const T& value) {
    if constexpr (!THREADED) {
        std::lock_guard<mutex_t> lock(mutex_);

        ptr_t root = root_.load();
        auto res = insert_impl(&root_, root, kb, value);

        if (!res.inserted) {
            for (auto* old : res.old_nodes) retire_node(old);
            return {find(key), false};
        }

        if (res.new_node) root_.store(res.new_node);
        for (auto* old : res.old_nodes) retire_node(old);
        ++size_;

        return {iterator(this, std::string(kb), value), true};
    } else {
        while (true) {
            speculative_info spec;
            {
                auto& slot = get_ebr_slot();
                auto guard = slot.get_guard();
                spec = probe_speculative(root_.load(), kb);
            }

            if (spec.op == spec_op::EXISTS) {
                return {find(key), false};
            }

            if ((spec.op == spec_op::IN_PLACE_LEAF) | (spec.op == spec_op::IN_PLACE_INTERIOR)) {
                pre_alloc alloc = allocate_speculative(spec, value);

                {
                    std::lock_guard<mutex_t> lock(mutex_);

                    if (!validate_path(spec)) {
                        dealloc_speculation(alloc);
                        continue;
                    }

                    if (commit_speculative(spec, alloc, value)) {
                        size_.fetch_add(1);
                        return {iterator(this, std::string(kb), value), true};
                    }
                }
                dealloc_speculation(alloc);
                continue;
            }

            pre_alloc alloc = allocate_speculative(spec, value);

            {
                std::lock_guard<mutex_t> lock(mutex_);

                if (!validate_path(spec)) {
                    dealloc_speculation(alloc);
                    continue;
                }

                if (alloc.root_replacement && commit_speculative(spec, alloc, value)) {
                    if (spec.target) {
                        retire_node(spec.target);
                    }
                    size_.fetch_add(1);
                    return {iterator(this, std::string(kb), value), true};
                }

                if ((spec.op == spec_op::ADD_EOS_LEAF_LIST) |
                    (spec.op == spec_op::DEMOTE_LEAF_LIST) |
                    (spec.op == spec_op::SPLIT_INTERIOR) |
                    (spec.op == spec_op::PREFIX_INTERIOR) |
                    (spec.op == spec_op::ADD_CHILD_CONVERT)) {

                    dealloc_speculation(alloc);

                    if (validate_path(spec)) {
                        ptr_t root = root_.load();
                        auto res = insert_impl(&root_, root, kb, value);

                        if (!res.inserted) {
                            for (auto* old : res.old_nodes) retire_node(old);
                            return {find(key), false};
                        }

                        if (res.new_node) root_.store(res.new_node);
                        for (auto* old : res.old_nodes) retire_node(old);
                        size_.fetch_add(1);

                        return {iterator(this, std::string(kb), value), true};
                    }
                }
            }

            dealloc_speculation(alloc);
        }
    }
}

// -----------------------------------------------------------------------------
// Erase operations
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_spec_info TKTRIE_CLASS::probe_erase(
    ptr_t n, std::string_view key) const noexcept {
    erase_spec_info info;

    if (!n) {
        info.op = erase_op::NOT_FOUND;
        return info;
    }

    info.path[info.path_len++] = {n, n->version(), 0};

    while (n) {
        if (n->is_leaf()) {
            std::string_view skip = get_skip(n);
            size_t m = match_skip_impl(skip, key);
            if (m < skip.size()) {
                info.op = erase_op::NOT_FOUND;
                return info;
            }
            key.remove_prefix(m);

            if (n->is_eos()) {
                if (!key.empty()) {
                    info.op = erase_op::NOT_FOUND;
                    return info;
                }
                info.op = erase_op::DELETE_LEAF_EOS;
                info.target = n;
                info.target_version = n->version();
                return info;
            }

            if (n->is_skip()) {
                if (!key.empty()) {
                    info.op = erase_op::NOT_FOUND;
                    return info;
                }
                info.op = erase_op::DELETE_LEAF_SKIP;
                info.target = n;
                info.target_version = n->version();
                return info;
            }

            if (key.size() != 1) {
                info.op = erase_op::NOT_FOUND;
                return info;
            }

            unsigned char c = static_cast<unsigned char>(key[0]);
            info.c = c;
            info.target = n;
            info.target_version = n->version();

            if (n->is_list()) {
                int idx = n->as_list()->chars.find(c);
                if (idx < 0) {
                    info.op = erase_op::NOT_FOUND;
                    return info;
                }
                if (n->as_list()->chars.count() == 1) {
                    info.op = erase_op::DELETE_LAST_LEAF_LIST;
                } else {
                    info.op = erase_op::IN_PLACE_LEAF_LIST;
                }
                return info;
            }

            if (!n->as_full()->valid.test(c)) {
                info.op = erase_op::NOT_FOUND;
                return info;
            }
            info.op = erase_op::IN_PLACE_LEAF_FULL;
            return info;
        }

        std::string_view skip = get_skip(n);
        size_t m = match_skip_impl(skip, key);
        if (m < skip.size()) {
            info.op = erase_op::NOT_FOUND;
            return info;
        }
        key.remove_prefix(m);

        if (key.empty()) {
            T* p = get_eos_ptr(n);
            if (!p) {
                info.op = erase_op::NOT_FOUND;
                return info;
            }
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);

            int child_cnt = n->child_count();
            if (child_cnt == 0) {
                info.op = erase_op::DELETE_EOS_INTERIOR;
                return info;
            }
            if (child_cnt == 1) {
                info.op = erase_op::COLLAPSE_AFTER_REMOVE;
                if (n->is_list()) {
                    info.collapse_char = n->as_list()->chars.char_at(0);
                    info.collapse_child = n->as_list()->children[0].load();
                } else if (n->is_full()) {
                    info.collapse_char = n->as_full()->valid.first();
                    info.collapse_child = n->as_full()->children[info.collapse_char].load();
                }
                if (info.collapse_child) {
                    info.collapse_child_version = info.collapse_child->version();
                    info.child_skip = std::string(get_skip(info.collapse_child));
                }
                return info;
            }
            info.op = erase_op::DELETE_EOS_INTERIOR;
            return info;
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = find_child(n, c);
        if (!child) {
            info.op = erase_op::NOT_FOUND;
            return info;
        }

        key.remove_prefix(1);
        n = child;
        if (info.path_len < erase_spec_info::MAX_PATH) {
            info.path[info.path_len++] = {n, n->version(), c};
        }
    }

    info.op = erase_op::NOT_FOUND;
    return info;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::capture_parent_collapse_info(erase_spec_info& info) const noexcept {
    if (info.path_len < 2) return;

    ptr_t parent = info.path[info.path_len - 2].node;
    unsigned char edge = info.path[info.path_len - 1].edge;

    info.parent = parent;
    info.parent_version = parent->version();
    info.parent_edge = edge;
    info.parent_skip = std::string(get_skip(parent));

    T* eos = get_eos_ptr(parent);
    if (eos) return;

    int remaining = parent->child_count() - 1;
    if (remaining != 1) return;

    if (parent->is_list()) {
        for (int i = 0; i < parent->as_list()->chars.count(); ++i) {
            unsigned char ch = parent->as_list()->chars.char_at(i);
            if (ch != edge) {
                info.parent_collapse_char = ch;
                info.parent_collapse_child = parent->as_list()->children[i].load();
                break;
            }
        }
    } else if (parent->is_full()) {
        for (int i = 0; i < 256; ++i) {
            unsigned char ch = static_cast<unsigned char>(i);
            if ((parent->as_full()->valid.test(ch)) & (ch != edge)) {
                info.parent_collapse_char = ch;
                info.parent_collapse_child = parent->as_full()->children[ch].load();
                break;
            }
        }
    }

    if (info.parent_collapse_child) {
        info.parent_collapse_child_version = info.parent_collapse_child->version();
        info.parent_child_skip = std::string(get_skip(info.parent_collapse_child));
    }
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::check_collapse_needed(
    ptr_t parent, unsigned char removed_c,
    unsigned char& collapse_c, ptr_t& collapse_child) const noexcept {
    T* eos = get_eos_ptr(parent);
    if (eos) return false;

    int remaining = parent->child_count();

    if (parent->is_list()) {
        int idx = parent->as_list()->chars.find(removed_c);
        if (idx >= 0) remaining--;

        if (remaining != 1) return false;

        for (int i = 0; i < parent->as_list()->chars.count(); ++i) {
            unsigned char ch = parent->as_list()->chars.char_at(i);
            if (ch != removed_c) {
                collapse_c = ch;
                collapse_child = parent->as_list()->children[i].load();
                return collapse_child != nullptr;
            }
        }
    } else if (parent->is_full()) {
        if (parent->as_full()->valid.test(removed_c)) remaining--;

        if (remaining != 1) return false;

        for (int i = 0; i < 256; ++i) {
            unsigned char ch = static_cast<unsigned char>(i);
            if ((parent->as_full()->valid.test(ch)) & (ch != removed_c)) {
                collapse_c = ch;
                collapse_child = parent->as_full()->children[ch].load();
                return collapse_child != nullptr;
            }
        }
    }
    return false;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::allocate_collapse_node(const erase_spec_info& info) {
    std::string new_skip = info.target_skip;
    new_skip.push_back(static_cast<char>(info.collapse_char));
    new_skip.append(info.child_skip);

    ptr_t child = info.collapse_child;
    if (!child) return nullptr;

    if (child->is_leaf()) {
        if (child->is_eos() | child->is_skip()) {
            return builder_.make_leaf_skip(new_skip, T{});
        } else if (child->is_list()) {
            return builder_.make_leaf_list(new_skip);
        } else {
            return builder_.make_leaf_full(new_skip);
        }
    } else {
        if (child->is_eos() | child->is_skip()) {
            return builder_.make_interior_skip(new_skip);
        } else if (child->is_list()) {
            return builder_.make_interior_list(new_skip);
        } else {
            return builder_.make_interior_full(new_skip);
        }
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::allocate_parent_collapse_node(const erase_spec_info& info) {
    if (!info.parent_collapse_child) return nullptr;

    std::string new_skip = info.parent_skip;
    new_skip.push_back(static_cast<char>(info.parent_collapse_char));
    new_skip.append(info.parent_child_skip);

    ptr_t child = info.parent_collapse_child;

    if (child->is_leaf()) {
        if (child->is_eos() | child->is_skip()) {
            return builder_.make_leaf_skip(new_skip, T{});
        } else if (child->is_list()) {
            return builder_.make_leaf_list(new_skip);
        } else {
            return builder_.make_leaf_full(new_skip);
        }
    } else {
        if (child->is_eos() | child->is_skip()) {
            return builder_.make_interior_skip(new_skip);
        } else if (child->is_list()) {
            return builder_.make_interior_list(new_skip);
        } else {
            return builder_.make_interior_full(new_skip);
        }
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_pre_alloc TKTRIE_CLASS::allocate_erase_speculative(
    const erase_spec_info& info) {
    erase_pre_alloc alloc;

    if (info.op == erase_op::COLLAPSE_AFTER_REMOVE) {
        alloc.merged = allocate_collapse_node(info);
    }

    if (info.parent_collapse_child) {
        alloc.parent_merged = allocate_parent_collapse_node(info);
    }

    return alloc;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::dealloc_erase_speculation(erase_pre_alloc& alloc) {
    if (alloc.merged) {
        switch (alloc.merged->type()) {
            case TYPE_EOS: delete alloc.merged->as_eos(); break;
            case TYPE_SKIP: delete alloc.merged->as_skip(); break;
            case TYPE_LIST: delete alloc.merged->as_list(); break;
            case TYPE_FULL: delete alloc.merged->as_full(); break;
        }
        alloc.merged = nullptr;
    }
    if (alloc.parent_merged) {
        switch (alloc.parent_merged->type()) {
            case TYPE_EOS: delete alloc.parent_merged->as_eos(); break;
            case TYPE_SKIP: delete alloc.parent_merged->as_skip(); break;
            case TYPE_LIST: delete alloc.parent_merged->as_list(); break;
            case TYPE_FULL: delete alloc.parent_merged->as_full(); break;
        }
        alloc.parent_merged = nullptr;
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::fill_collapse_node(ptr_t merged, ptr_t child) {
    if (child->is_leaf()) {
        if (child->is_eos()) {
            merged->as_skip()->leaf_value = child->as_eos()->leaf_value;
        } else if (child->is_skip()) {
            merged->as_skip()->leaf_value = child->as_skip()->leaf_value;
        } else if (child->is_list()) {
            merged->as_list()->chars = child->as_list()->chars;
            for (int i = 0; i < child->as_list()->chars.count(); ++i) {
                merged->as_list()->leaf_values[i] = child->as_list()->leaf_values[i];
            }
        } else {
            merged->as_full()->valid = child->as_full()->valid;
            for (int i = 0; i < 256; ++i) {
                if (child->as_full()->valid.test(static_cast<unsigned char>(i))) {
                    merged->as_full()->leaf_values[i] = child->as_full()->leaf_values[i];
                }
            }
        }
    } else {
        if (child->is_eos() | child->is_skip()) {
            merged->as_skip()->eos_ptr = get_eos_ptr(child);
            set_eos_ptr(child, nullptr);
        } else if (child->is_list()) {
            merged->as_list()->eos_ptr = child->as_list()->eos_ptr;
            child->as_list()->eos_ptr = nullptr;
            merged->as_list()->chars = child->as_list()->chars;
            for (int i = 0; i < child->as_list()->chars.count(); ++i) {
                merged->as_list()->children[i].store(child->as_list()->children[i].load());
                child->as_list()->children[i].store(nullptr);
            }
        } else {
            merged->as_full()->eos_ptr = child->as_full()->eos_ptr;
            child->as_full()->eos_ptr = nullptr;
            merged->as_full()->valid = child->as_full()->valid;
            for (int i = 0; i < 256; ++i) {
                if (child->as_full()->valid.test(static_cast<unsigned char>(i))) {
                    merged->as_full()->children[i].store(child->as_full()->children[i].load());
                    child->as_full()->children[i].store(nullptr);
                }
            }
        }
    }
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::validate_erase_path(const erase_spec_info& info) const noexcept {
    for (int i = 0; i < info.path_len; ++i) {
        if (info.path[i].node->version() != info.path[i].version) {
            return false;
        }
    }
    if (info.target && (info.path_len == 0 || info.path[info.path_len-1].node != info.target)) {
        if (info.target->version() != info.target_version) {
            return false;
        }
    }
    if (info.collapse_child) {
        if (info.collapse_child->version() != info.collapse_child_version) {
            return false;
        }
    }
    if (info.parent) {
        if (info.parent->version() != info.parent_version) {
            return false;
        }
    }
    if (info.parent_collapse_child) {
        if (info.parent_collapse_child->version() != info.parent_collapse_child_version) {
            return false;
        }
    }
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_leaf_list_erase(ptr_t leaf, unsigned char c) {
    int idx = leaf->as_list()->chars.find(c);
    if (idx < 0) return false;

    int count = leaf->as_list()->chars.count();
    if (count <= 1) return false;

    for (int i = idx; i < count - 1; ++i) {
        leaf->as_list()->leaf_values[i] = leaf->as_list()->leaf_values[i + 1];
    }
    leaf->as_list()->chars.remove_at(idx);
    leaf->bump_version();
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_leaf_full_erase(ptr_t leaf, unsigned char c) {
    if (!leaf->as_full()->valid.test(c)) return false;
    leaf->as_full()->valid.template atomic_clear<THREADED>(c);
    leaf->bump_version();
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_interior_list_erase(ptr_t n, unsigned char c) {
    int idx = n->as_list()->chars.find(c);
    if (idx < 0) return false;

    int count = n->as_list()->chars.count();
    for (int i = idx; i < count - 1; ++i) {
        n->as_list()->children[i].store(n->as_list()->children[i + 1].load());
    }
    n->as_list()->children[count - 1].store(nullptr);
    n->as_list()->chars.remove_at(idx);
    n->bump_version();
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_interior_full_erase(ptr_t n, unsigned char c) {
    if (!n->as_full()->valid.test(c)) return false;
    n->as_full()->valid.template atomic_clear<THREADED>(c);
    n->as_full()->children[c].store(nullptr);
    n->bump_version();
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::erase_locked(std::string_view kb) {
    if constexpr (!THREADED) {
        std::lock_guard<mutex_t> lock(mutex_);

        ptr_t root = root_.load();
        auto res = erase_impl(&root_, root, kb);

        if (!res.erased) return false;

        if (res.deleted_subtree) root_.store(nullptr);
        else if (res.new_node) root_.store(res.new_node);

        for (auto* old : res.old_nodes) retire_node(old);
        --size_;

        return true;
    } else {
        while (true) {
            erase_spec_info info;
            {
                auto& slot = get_ebr_slot();
                auto guard = slot.get_guard();
                info = probe_erase(root_.load(), kb);
                if (info.op != erase_op::NOT_FOUND) {
                    capture_parent_collapse_info(info);
                }
            }

            if (info.op == erase_op::NOT_FOUND) {
                return false;
            }

            if ((info.op == erase_op::IN_PLACE_LEAF_LIST) |
                (info.op == erase_op::IN_PLACE_LEAF_FULL) |
                (info.op == erase_op::IN_PLACE_INTERIOR_LIST) |
                (info.op == erase_op::IN_PLACE_INTERIOR_FULL)) {

                std::lock_guard<mutex_t> lock(mutex_);

                if (!validate_erase_path(info)) continue;

                bool success = false;
                switch (info.op) {
                    case erase_op::IN_PLACE_LEAF_LIST:
                        success = do_inplace_leaf_list_erase(info.target, info.c);
                        break;
                    case erase_op::IN_PLACE_LEAF_FULL:
                        success = do_inplace_leaf_full_erase(info.target, info.c);
                        break;
                    case erase_op::IN_PLACE_INTERIOR_LIST:
                        success = do_inplace_interior_list_erase(info.target, info.c);
                        break;
                    case erase_op::IN_PLACE_INTERIOR_FULL:
                        success = do_inplace_interior_full_erase(info.target, info.c);
                        break;
                    default:
                        break;
                }

                if (success) {
                    size_.fetch_sub(1);
                    return true;
                }
                continue;
            }

            if ((info.op == erase_op::DELETE_LEAF_EOS) |
                (info.op == erase_op::DELETE_LEAF_SKIP) |
                (info.op == erase_op::DELETE_LAST_LEAF_LIST)) {

                erase_pre_alloc alloc = allocate_erase_speculative(info);

                {
                    std::lock_guard<mutex_t> lock(mutex_);

                    if (!validate_erase_path(info)) {
                        dealloc_erase_speculation(alloc);
                        continue;
                    }

                    if (info.path_len <= 1) {
                        root_.store(nullptr);
                        retire_node(info.target);
                        size_.fetch_sub(1);
                        dealloc_erase_speculation(alloc);
                        return true;
                    }

                    ptr_t parent = info.path[info.path_len - 2].node;
                    unsigned char edge = info.path[info.path_len - 1].edge;

                    if (alloc.parent_merged && info.parent_collapse_child) {
                        fill_collapse_node(alloc.parent_merged, info.parent_collapse_child);

                        if (info.path_len <= 2) {
                            root_.store(alloc.parent_merged);
                        } else {
                            ptr_t grandparent = info.path[info.path_len - 3].node;
                            unsigned char parent_edge = info.path[info.path_len - 2].edge;
                            atomic_ptr* slot = get_child_slot(grandparent, parent_edge);
                            if (slot) {
                                slot->store(alloc.parent_merged);
                                grandparent->bump_version();
                            }
                        }
                        retire_node(info.target);
                        retire_node(parent);
                        retire_node(info.parent_collapse_child);
                        alloc.parent_merged = nullptr;
                        size_.fetch_sub(1);
                        dealloc_erase_speculation(alloc);
                        return true;
                    }

                    if (parent->is_list()) {
                        do_inplace_interior_list_erase(parent, edge);
                    } else if (parent->is_full()) {
                        do_inplace_interior_full_erase(parent, edge);
                    }

                    retire_node(info.target);
                    size_.fetch_sub(1);
                    dealloc_erase_speculation(alloc);
                    return true;
                }
            }

            if (info.op == erase_op::DELETE_EOS_INTERIOR) {
                std::lock_guard<mutex_t> lock(mutex_);

                if (!validate_erase_path(info)) continue;

                T* p = get_eos_ptr(info.target);
                if (!p) continue;

                delete p;
                set_eos_ptr(info.target, nullptr);
                info.target->bump_version();

                int child_cnt = info.target->child_count();
                if (child_cnt == 0) {
                    if (info.path_len <= 1) {
                        root_.store(nullptr);
                        retire_node(info.target);
                    } else {
                        ptr_t parent = info.path[info.path_len - 2].node;
                        unsigned char edge = info.path[info.path_len - 1].edge;

                        unsigned char collapse_c = 0;
                        ptr_t collapse_child = nullptr;
                        if (check_collapse_needed(parent, edge, collapse_c, collapse_child)) {
                            ptr_t root = root_.load();
                            auto res = erase_impl(&root_, root, kb);
                            if (res.new_node) root_.store(res.new_node);
                            for (auto* old : res.old_nodes) retire_node(old);
                            size_.fetch_sub(1);
                            return true;
                        }

                        if (parent->is_list()) {
                            do_inplace_interior_list_erase(parent, edge);
                        } else if (parent->is_full()) {
                            do_inplace_interior_full_erase(parent, edge);
                        }
                        retire_node(info.target);
                    }
                } else if (child_cnt == 1) {
                    ptr_t root = root_.load();
                    auto res = erase_impl(&root_, root, kb);
                    if (res.new_node) {
                        if (info.path_len <= 1) {
                            root_.store(res.new_node);
                        } else {
                            ptr_t parent = info.path[info.path_len - 2].node;
                            unsigned char edge = info.path[info.path_len - 1].edge;
                            atomic_ptr* slot = get_child_slot(parent, edge);
                            if (slot) slot->store(res.new_node);
                            parent->bump_version();
                        }
                    }
                    for (auto* old : res.old_nodes) retire_node(old);
                }

                size_.fetch_sub(1);
                return true;
            }

            if (info.op == erase_op::COLLAPSE_AFTER_REMOVE) {
                erase_pre_alloc alloc = allocate_erase_speculative(info);
                if (!alloc.merged) {
                    std::lock_guard<mutex_t> lock(mutex_);
                    ptr_t root = root_.load();
                    auto res = erase_impl(&root_, root, kb);
                    if (!res.erased) return false;
                    if (res.deleted_subtree) root_.store(nullptr);
                    else if (res.new_node) root_.store(res.new_node);
                    for (auto* old : res.old_nodes) retire_node(old);
                    size_.fetch_sub(1);
                    return true;
                }

                {
                    std::lock_guard<mutex_t> lock(mutex_);

                    if (!validate_erase_path(info)) {
                        dealloc_erase_speculation(alloc);
                        continue;
                    }

                    T* p = get_eos_ptr(info.target);
                    if (p) {
                        delete p;
                        set_eos_ptr(info.target, nullptr);
                    }

                    fill_collapse_node(alloc.merged, info.collapse_child);

                    if (info.path_len <= 1) {
                        root_.store(alloc.merged);
                    } else {
                        ptr_t parent = info.path[info.path_len - 2].node;
                        unsigned char edge = info.path[info.path_len - 1].edge;
                        atomic_ptr* slot = get_child_slot(parent, edge);
                        if (slot) {
                            slot->store(alloc.merged);
                            parent->bump_version();
                        }
                    }

                    retire_node(info.target);
                    retire_node(info.collapse_child);
                    alloc.merged = nullptr;
                    size_.fetch_sub(1);
                    dealloc_erase_speculation(alloc);
                    return true;
                }
            }

            {
                std::lock_guard<mutex_t> lock(mutex_);
                ptr_t root = root_.load();
                auto res = erase_impl(&root_, root, kb);
                if (!res.erased) return false;
                if (res.deleted_subtree) root_.store(nullptr);
                else if (res.new_node) root_.store(res.new_node);
                for (auto* old : res.old_nodes) retire_node(old);
                size_.fetch_sub(1);
                return true;
            }
        }
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::erase_impl(
    atomic_ptr*, ptr_t n, std::string_view key) {
    erase_result res;
    if (!n) return res;
    if (n->is_leaf()) return erase_from_leaf(n, key);
    return erase_from_interior(n, key);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::erase_from_leaf(
    ptr_t leaf, std::string_view key) {
    erase_result res;
    std::string_view skip = get_skip(leaf);
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) return res;
    key.remove_prefix(m);

    if (leaf->is_eos()) {
        if (!key.empty()) return res;
        res.erased = true;
        res.deleted_subtree = true;
        res.old_nodes.push_back(leaf);
        return res;
    }

    if (leaf->is_skip()) {
        if (!key.empty()) return res;
        res.erased = true;
        res.deleted_subtree = true;
        res.old_nodes.push_back(leaf);
        return res;
    }

    if (key.size() != 1) return res;
    unsigned char c = static_cast<unsigned char>(key[0]);

    if (leaf->is_list()) {
        int idx = leaf->as_list()->chars.find(c);
        if (idx < 0) return res;

        int count = leaf->as_list()->chars.count();
        if (count == 1) {
            res.erased = true;
            res.deleted_subtree = true;
            res.old_nodes.push_back(leaf);
            return res;
        }

        for (int i = idx; i < count - 1; ++i) {
            leaf->as_list()->leaf_values[i] = leaf->as_list()->leaf_values[i + 1];
        }
        leaf->as_list()->chars.remove_at(idx);
        leaf->bump_version();
        res.erased = true;
        return res;
    }

    // FULL
    if (!leaf->as_full()->valid.test(c)) return res;
    leaf->as_full()->valid.template atomic_clear<THREADED>(c);
    leaf->bump_version();
    res.erased = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::erase_from_interior(
    ptr_t n, std::string_view key) {
    erase_result res;
    std::string_view skip = get_skip(n);
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) return res;
    key.remove_prefix(m);

    if (key.empty()) {
        T* p = get_eos_ptr(n);
        if (!p) return res;
        delete p;
        set_eos_ptr(n, nullptr);
        n->bump_version();
        res.erased = true;
        return try_collapse_interior(n);
    }

    unsigned char c = static_cast<unsigned char>(key[0]);
    ptr_t child = find_child(n, c);
    if (!child) return res;

    auto child_res = erase_impl(get_child_slot(n, c), child, key.substr(1));
    if (!child_res.erased) return res;

    if (child_res.deleted_subtree) {
        return try_collapse_after_child_removal(n, c, child_res);
    }

    if (child_res.new_node) {
        get_child_slot(n, c)->store(child_res.new_node);
        n->bump_version();
    }
    res.erased = true;
    res.old_nodes = std::move(child_res.old_nodes);
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::try_collapse_interior(ptr_t n) {
    erase_result res;
    res.erased = true;

    T* eos = get_eos_ptr(n);
    if (eos) return res;

    int child_cnt = n->child_count();
    if (child_cnt == 0) {
        res.deleted_subtree = true;
        res.old_nodes.push_back(n);
        return res;
    }
    if (child_cnt != 1) return res;

    unsigned char c = 0;
    ptr_t child = nullptr;
    if (n->is_list()) {
        c = n->as_list()->chars.char_at(0);
        child = n->as_list()->children[0].load();
    } else if (n->is_full()) {
        c = n->as_full()->valid.first();
        child = n->as_full()->children[c].load();
    }
    if (!child) return res;

    return collapse_single_child(n, c, child, res);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::try_collapse_after_child_removal(
    ptr_t n, unsigned char removed_c, erase_result& child_res) {
    erase_result res;
    res.old_nodes = std::move(child_res.old_nodes);
    res.erased = true;

    T* eos = get_eos_ptr(n);
    int remaining = n->child_count();

    if (n->is_list()) {
        int idx = n->as_list()->chars.find(removed_c);
        if (idx >= 0) remaining--;
    } else if (n->is_full()) {
        if (n->as_full()->valid.test(removed_c)) remaining--;
    }

    if ((!eos) & (remaining == 0)) {
        res.deleted_subtree = true;
        res.old_nodes.push_back(n);
        return res;
    }

    if (n->is_list()) {
        int idx = n->as_list()->chars.find(removed_c);
        if (idx >= 0) {
            int count = n->as_list()->chars.count();
            for (int i = idx; i < count - 1; ++i) {
                n->as_list()->children[i].store(n->as_list()->children[i + 1].load());
            }
            n->as_list()->children[count - 1].store(nullptr);
            n->as_list()->chars.remove_at(idx);
            n->bump_version();
        }
    } else if (n->is_full()) {
        n->as_full()->valid.template atomic_clear<THREADED>(removed_c);
        n->as_full()->children[removed_c].store(nullptr);
        n->bump_version();
    }

    bool can_collapse = false;
    unsigned char c = 0;
    ptr_t child = nullptr;

    if (n->is_list() && n->as_list()->chars.count() == 1 && !eos) {
        c = n->as_list()->chars.char_at(0);
        child = n->as_list()->children[0].load();
        can_collapse = (child != nullptr);
    } else if (n->is_full() && !eos) {
        int cnt = n->as_full()->valid.count();
        if (cnt == 1) {
            c = n->as_full()->valid.first();
            child = n->as_full()->children[c].load();
            can_collapse = (child != nullptr);
        }
    }

    if (can_collapse) {
        return collapse_single_child(n, c, child, res);
    }
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::collapse_single_child(
    ptr_t n, unsigned char c, ptr_t child, erase_result& res) {
    std::string new_skip(get_skip(n));
    new_skip.push_back(static_cast<char>(c));
    new_skip.append(get_skip(child));

    ptr_t merged;
    if (child->is_leaf()) {
        if (child->is_eos()) {
            merged = builder_.make_leaf_skip(new_skip, child->as_eos()->leaf_value);
        } else if (child->is_skip()) {
            merged = builder_.make_leaf_skip(new_skip, child->as_skip()->leaf_value);
        } else if (child->is_list()) {
            merged = builder_.make_leaf_list(new_skip);
            merged->as_list()->chars = child->as_list()->chars;
            for (int i = 0; i < child->as_list()->chars.count(); ++i) {
                merged->as_list()->leaf_values[i] = child->as_list()->leaf_values[i];
            }
        } else {
            merged = builder_.make_leaf_full(new_skip);
            merged->as_full()->valid = child->as_full()->valid;
            for (int i = 0; i < 256; ++i) {
                if (child->as_full()->valid.test(static_cast<unsigned char>(i))) {
                    merged->as_full()->leaf_values[i] = child->as_full()->leaf_values[i];
                }
            }
        }
    } else {
        if (child->is_eos() | child->is_skip()) {
            merged = builder_.make_interior_skip(new_skip);
            merged->as_skip()->eos_ptr = get_eos_ptr(child);
            set_eos_ptr(child, nullptr);
        } else if (child->is_list()) {
            merged = builder_.make_interior_list(new_skip);
            merged->as_list()->eos_ptr = child->as_list()->eos_ptr;
            child->as_list()->eos_ptr = nullptr;
            merged->as_list()->chars = child->as_list()->chars;
            for (int i = 0; i < child->as_list()->chars.count(); ++i) {
                merged->as_list()->children[i].store(child->as_list()->children[i].load());
                child->as_list()->children[i].store(nullptr);
            }
        } else {
            merged = builder_.make_interior_full(new_skip);
            merged->as_full()->eos_ptr = child->as_full()->eos_ptr;
            child->as_full()->eos_ptr = nullptr;
            merged->as_full()->valid = child->as_full()->valid;
            for (int i = 0; i < 256; ++i) {
                if (child->as_full()->valid.test(static_cast<unsigned char>(i))) {
                    merged->as_full()->children[i].store(child->as_full()->children[i].load());
                    child->as_full()->children[i].store(nullptr);
                }
            }
        }
    }

    res.new_node = merged;
    res.old_nodes.push_back(n);
    res.old_nodes.push_back(child);
    return res;
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum
