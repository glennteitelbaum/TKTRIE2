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
// KEY TRAITS - provides to_bytes, from_bytes, and FIXED_LEN
// =============================================================================

template <typename Key> struct tktrie_traits;

template <>
struct tktrie_traits<std::string> {
    static constexpr size_t FIXED_LEN = 0;  // Variable length
    static std::string_view to_bytes(const std::string& k) noexcept { return k; }
    static std::string from_bytes(std::string_view b) { return std::string(b); }
};

template <typename T> requires std::is_integral_v<T>
struct tktrie_traits<T> {
    static constexpr size_t FIXED_LEN = sizeof(T);  // Fixed length
    using unsigned_t = std::make_unsigned_t<T>;
    using bytes_t = std::array<char, sizeof(T)>;
    
    static bytes_t to_bytes(T k) noexcept {
        unsigned_t sortable;
        if constexpr (std::is_signed_v<T>)
            sortable = static_cast<unsigned_t>(k) ^ (unsigned_t{1} << (sizeof(T) * 8 - 1));
        else
            sortable = k;
        unsigned_t be = to_big_endian(sortable);
        bytes_t result;
        std::memcpy(result.data(), &be, sizeof(T));
        return result;
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
    static constexpr size_t FIXED_LEN = traits::FIXED_LEN;
    
    using ptr_t = node_base<T, THREADED, Allocator, FIXED_LEN>*;
    using atomic_ptr = atomic_node_ptr<T, THREADED, Allocator, FIXED_LEN>;
    using builder_t = node_builder<T, THREADED, Allocator, FIXED_LEN>;
    using skip_t = skip_node<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = dataptr<T, THREADED, Allocator>;
    using iterator = tktrie_iterator<Key, T, THREADED, Allocator>;
    using mutex_t = std::conditional_t<THREADED, std::mutex, empty_mutex>;

    // -------------------------------------------------------------------------
    // Result types
    // -------------------------------------------------------------------------
    
    // Small fixed-capacity list for retired nodes (avoids heap allocation)
    // Max 4: typical is 1-2, worst case split/collapse is 3
    struct retired_list {
        ptr_t nodes[4];  // Not initialized - only count elements are valid
        uint8_t count = 0;
        
        void push_back(ptr_t n) noexcept { nodes[count++] = n; }
        bool empty() const noexcept { return count == 0; }
        ptr_t* begin() noexcept { return nodes; }
        ptr_t* end() noexcept { return nodes + count; }
    };
    
    struct insert_result {
        ptr_t new_node = nullptr;
        retired_list old_nodes;
        bool inserted = false;
        bool in_place = false;
    };

    struct erase_result {
        ptr_t new_node = nullptr;
        retired_list old_nodes;
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
        std::array<ptr_t, MAX_DEPTH> nodes{};
        std::array<uint64_t, MAX_DEPTH> versions{};
        int len = 0;
        
        void clear() noexcept { len = 0; }
        
        // Original push (for non-THREADED or when poison already checked)
        bool push(ptr_t n) noexcept {
            if (len >= MAX_DEPTH) return false;
            nodes[len] = n;
            versions[len] = n->version();
            ++len;
            return true;
        }
        
        // Combined push + poison check (single header load)
        bool push_checked(ptr_t n) noexcept {
            if (len >= MAX_DEPTH) return false;
            uint64_t h = n->header();  // Single atomic load
            if (is_poisoned_header(h)) return false;
            nodes[len] = n;
            versions[len] = get_version(h);
            ++len;
            return true;
        }
    };

    // -------------------------------------------------------------------------
    // Speculative insert types
    // -------------------------------------------------------------------------
    enum class spec_op {
        EXISTS, RETRY,  // RETRY = need to re-probe (concurrent write detected)
        IN_PLACE_LEAF, IN_PLACE_INTERIOR, EMPTY_TREE,
        SPLIT_LEAF_SKIP, PREFIX_LEAF_SKIP, EXTEND_LEAF_SKIP,
        SPLIT_LEAF_LIST, PREFIX_LEAF_LIST, ADD_EOS_LEAF_LIST, LIST_TO_FULL_LEAF,
        DEMOTE_LEAF_LIST, SPLIT_INTERIOR, PREFIX_INTERIOR, ADD_CHILD_CONVERT,
    };

    struct speculative_info {
        static constexpr int MAX_PATH = 64;
        std::array<path_entry, MAX_PATH> path{};
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
        ptr_t nodes[8];  // Not initialized
        int count = 0;
        ptr_t root_replacement = nullptr;
        void add(ptr_t n) { nodes[count++] = n; }
    };
    
#ifdef TKTRIE_INSTRUMENT_RETRIES
    struct retry_stats {
        std::atomic<uint64_t> speculative_attempts{0};
        std::atomic<uint64_t> speculative_successes{0};
        std::atomic<uint64_t> retries[8]{};  // retries[i] = count that needed i retries
        std::atomic<uint64_t> fallbacks{0};  // exceeded max retries
    };
    static retry_stats& get_retry_stats() {
        static retry_stats stats;
        return stats;
    }
    static void stat_attempt() { get_retry_stats().speculative_attempts.fetch_add(1, std::memory_order_relaxed); }
    static void stat_success(int r) { 
        get_retry_stats().speculative_successes.fetch_add(1, std::memory_order_relaxed);
        if (r < 8) get_retry_stats().retries[r].fetch_add(1, std::memory_order_relaxed);
    }
    static void stat_fallback() { get_retry_stats().fallbacks.fetch_add(1, std::memory_order_relaxed); }
#else
    static void stat_attempt() {}
    static void stat_success(int) {}
    static void stat_fallback() {}
#endif

    // -------------------------------------------------------------------------
    // Speculative erase types
    // -------------------------------------------------------------------------
    enum class erase_op {
        NOT_FOUND,
        // In-place operations (no structural change)
        IN_PLACE_LEAF_LIST, IN_PLACE_LEAF_FULL,
        // Structural operations
        DELETE_SKIP_LEAF,           // Delete entire SKIP leaf
        DELETE_LAST_LEAF_ENTRY,     // Delete last entry from LIST/FULL leaf  
        DELETE_EOS_INTERIOR,        // Remove EOS from interior (may collapse)
        DELETE_CHILD_COLLAPSE,      // Remove child and collapse to merged node
        DELETE_CHILD_NO_COLLAPSE,   // Remove child, no collapse needed
    };

    struct erase_spec_info {
        static constexpr int MAX_PATH = 64;
        std::array<path_entry, MAX_PATH> path{};
        int path_len = 0;
        erase_op op = erase_op::NOT_FOUND;
        ptr_t target = nullptr;
        uint64_t target_version = 0;
        unsigned char c = 0;
        bool is_eos = false;
        // For collapse operations
        ptr_t collapse_child = nullptr;
        unsigned char collapse_char = 0;
        std::string target_skip;
        std::string child_skip;
    };

    struct erase_pre_alloc {
        ptr_t nodes[4];
        int count = 0;
        ptr_t replacement = nullptr;
        void add(ptr_t n) { nodes[count++] = n; }
    };

    // -------------------------------------------------------------------------
    // Per-trie EBR - epoch-based reclamation with per-trie reader tracking
    // No global state - each trie manages its own readers and retired nodes
    // -------------------------------------------------------------------------
    static constexpr size_t EBR_MIN_RETIRED = 64;      // Writers cleanup at this threshold

private:
    atomic_ptr root_;
    atomic_counter<THREADED> size_;
    mutable mutex_t mutex_;
    builder_t builder_;
    
    // Epoch counter: bumped on writes, used for read validation AND EBR
    alignas(64) std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> epoch_{1};
    
    // Per-trie reader tracking with cache-line padding to prevent false sharing
    // Each slot is 64 bytes to ensure no two threads share a cache line
    // Using 16 slots = 1KB per trie (reasonable memory, good coverage)
    static constexpr size_t EBR_PADDED_SLOTS = 16;
    
    struct alignas(64) PaddedReaderSlot {
        std::atomic<uint64_t> epoch{0};  // 0 = inactive
    };
    
    std::conditional_t<THREADED, 
        std::array<PaddedReaderSlot, EBR_PADDED_SLOTS>,
        std::array<uint64_t, 1>> reader_epochs_{};
    
    // Lock-free retired list using embedded fields in nodes (MPSC)
    std::conditional_t<THREADED, std::atomic<ptr_t>, ptr_t> retired_head_{nullptr};
    std::conditional_t<THREADED, std::atomic<size_t>, size_t> retired_count_{0};
    mutable std::conditional_t<THREADED, std::mutex, empty_mutex> ebr_mutex_;  // Only for cleanup
    
    // EBR helpers
    void ebr_retire(ptr_t n, uint64_t epoch);      // Lock-free push
    void ebr_cleanup();                             // Free reclaimable nodes (grabs ebr_mutex_)
    uint64_t min_reader_epoch() const noexcept;    // Scan slots for oldest active reader
    void reader_enter() const noexcept;            // Store epoch in thread's slot
    void reader_exit() const noexcept;             // Clear thread's slot

    // -------------------------------------------------------------------------
    // Static helpers
    // -------------------------------------------------------------------------
    static void node_deleter(void* ptr);

    // -------------------------------------------------------------------------
    // Instance helpers
    // -------------------------------------------------------------------------
    void retire_node(ptr_t n);

    // -------------------------------------------------------------------------
    // Read operations
    // -------------------------------------------------------------------------
    template <bool NEED_VALUE>
    bool read_impl(ptr_t n, std::string_view key, T& out) const noexcept
        requires NEED_VALUE;
    
    template <bool NEED_VALUE>
    bool read_impl(ptr_t n, std::string_view key) const noexcept
        requires (!NEED_VALUE);
    
    template <bool NEED_VALUE>
    bool read_impl_optimistic(ptr_t n, std::string_view key, T& out, read_path& path) const noexcept
        requires NEED_VALUE;
    
    template <bool NEED_VALUE>
    bool read_impl_optimistic(ptr_t n, std::string_view key, read_path& path) const noexcept
        requires (!NEED_VALUE);
    
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
    std::pair<iterator, bool> insert_locked(const Key& key, std::string_view kb, const T& value, bool* retired_any);

    // -------------------------------------------------------------------------
    // Erase operations
    // -------------------------------------------------------------------------
    erase_spec_info probe_erase(ptr_t n, std::string_view key) const noexcept;
    erase_spec_info probe_leaf_erase(ptr_t n, std::string_view key, erase_spec_info& info) const noexcept;
    erase_spec_info probe_interior_erase(ptr_t n, std::string_view key, erase_spec_info& info) const noexcept;
    bool do_inplace_leaf_list_erase(ptr_t leaf, unsigned char c, uint64_t expected_version);
    bool do_inplace_leaf_full_erase(ptr_t leaf, unsigned char c, uint64_t expected_version);
    erase_pre_alloc allocate_erase_speculative(const erase_spec_info& info);
    bool validate_erase_path(const erase_spec_info& info) const noexcept;
    bool commit_erase_speculative(erase_spec_info& info, erase_pre_alloc& alloc);
    void dealloc_erase_speculation(erase_pre_alloc& alloc);
    std::pair<bool, bool> erase_locked(std::string_view kb);  // Returns (erased, retired_any)
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
    tktrie();
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
    
    // For diagnostics/testing only
    ptr_t test_root() const noexcept { return root_.load(); }
};

// =============================================================================
// TKTRIE_ITERATOR CLASS
// =============================================================================

template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie_iterator {
public:
    using trie_t = tktrie<Key, T, THREADED, Allocator>;
    using traits = tktrie_traits<Key>;
    static constexpr size_t FIXED_LEN = traits::FIXED_LEN;
    
    // Store bytes: array for fixed-length, string for variable-length
    // We already converted for lookup - just keep them
    using key_storage_t = std::conditional_t<(FIXED_LEN > 0),
        std::array<char, FIXED_LEN>,
        std::string>;

private:
    const trie_t* trie_ = nullptr;
    key_storage_t key_bytes_{};
    T value_{};
    bool valid_ = false;

public:
    tktrie_iterator() = default;
    
    // Constructor from string_view - stores the already-converted bytes
    tktrie_iterator(const trie_t* t, std::string_view kb, const T& v)
        : trie_(t), value_(v), valid_(true) {
        if constexpr (FIXED_LEN > 0) {
            std::memcpy(key_bytes_.data(), kb.data(), FIXED_LEN);
        } else {
            key_bytes_ = std::string(kb);
        }
    }

    // Convert bytes back to Key only when requested
    Key key() const { 
        if constexpr (FIXED_LEN > 0) {
            return traits::from_bytes(std::string_view(key_bytes_.data(), FIXED_LEN));
        } else {
            return traits::from_bytes(key_bytes_);
        }
    }
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

#include "tktrie_core.h"
