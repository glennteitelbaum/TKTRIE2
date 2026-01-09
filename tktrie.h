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
// RETIRE_ENTRY - External wrapper for retired nodes
// =============================================================================

template <typename NodePtr>
struct retire_entry {
    NodePtr node;
    uint64_t epoch;
    retire_entry* next;
    
    retire_entry(NodePtr n, uint64_t e) noexcept : node(n), epoch(e), next(nullptr) {}
};

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
    using retire_entry_t = retire_entry<ptr_t>;

    // -------------------------------------------------------------------------
    // Result types
    // -------------------------------------------------------------------------
    
    struct retired_list {
        ptr_t nodes[4];
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
        
        bool push(ptr_t n) noexcept {
            if (len >= MAX_DEPTH) return false;
            nodes[len] = n;
            versions[len] = n->version();
            ++len;
            return true;
        }
        
        bool push_checked(ptr_t n) noexcept {
            if (len >= MAX_DEPTH) return false;
            uint64_t h = n->header();
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
        EXISTS, RETRY,
        IN_PLACE_LEAF, IN_PLACE_INTERIOR, EMPTY_TREE,
        SPLIT_LEAF_SKIP, PREFIX_LEAF_SKIP, EXTEND_LEAF_SKIP,
        SPLIT_LEAF_MULTI, PREFIX_LEAF_MULTI, ADD_EOS_LEAF_MULTI,
        BINARY_TO_LIST_LEAF, LIST_TO_POP_LEAF, POP_TO_FULL_LEAF,
        DEMOTE_LEAF_MULTI, SPLIT_INTERIOR, PREFIX_INTERIOR,
        BINARY_TO_LIST_INTERIOR, LIST_TO_POP_INTERIOR, POP_TO_FULL_INTERIOR,
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
        ptr_t nodes[8];
        int count = 0;
        ptr_t root_replacement = nullptr;
        void add(ptr_t n) { nodes[count++] = n; }
    };
    
#ifdef TKTRIE_INSTRUMENT_RETRIES
    struct retry_stats {
        std::atomic<uint64_t> speculative_attempts{0};
        std::atomic<uint64_t> speculative_successes{0};
        std::atomic<uint64_t> retries[8]{};
        std::atomic<uint64_t> fallbacks{0};
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
        IN_PLACE_LEAF,
        BINARY_TO_SKIP,
        DELETE_SKIP_LEAF,
        DELETE_LAST_LEAF_ENTRY,
        DELETE_EOS_INTERIOR,
        DELETE_CHILD_COLLAPSE,
        DELETE_CHILD_NO_COLLAPSE,
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
    // Per-trie EBR
    // -------------------------------------------------------------------------
    static constexpr size_t EBR_MIN_RETIRED = 64;

private:
    atomic_ptr root_;
    atomic_counter<THREADED> size_;
    mutable mutex_t mutex_;
    builder_t builder_;
    
    alignas(64) std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> epoch_{1};
    
    static constexpr size_t EBR_PADDED_SLOTS = 16;
    
    struct alignas(64) PaddedReaderSlot {
        std::atomic<uint64_t> epoch{0};
    };
    
    std::conditional_t<THREADED, 
        std::array<PaddedReaderSlot, EBR_PADDED_SLOTS>,
        std::array<uint64_t, 1>> reader_epochs_{};
    
    std::conditional_t<THREADED, std::atomic<retire_entry_t*>, retire_entry_t*> retired_head_{nullptr};
    std::conditional_t<THREADED, std::atomic<size_t>, size_t> retired_count_{0};
    mutable std::conditional_t<THREADED, std::mutex, empty_mutex> ebr_mutex_;
    
    void ebr_retire(ptr_t n, uint64_t epoch);
    void ebr_cleanup();
    uint64_t min_reader_epoch() const noexcept;
    size_t reader_enter() const noexcept;
    void reader_exit(size_t slot) const noexcept;

    static void node_deleter(void* ptr);
    void retire_node(ptr_t n);

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

    insert_result insert_impl(atomic_ptr* slot, ptr_t n, std::string_view key, const T& value);
    insert_result insert_into_leaf(atomic_ptr* slot, ptr_t leaf, std::string_view key, const T& value);
    insert_result insert_into_interior(atomic_ptr* slot, ptr_t n, std::string_view key, const T& value);
    ptr_t create_leaf_for_key(std::string_view key, const T& value);
    insert_result split_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m);
    insert_result prefix_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m);
    insert_result extend_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m);
    insert_result split_leaf_multi(ptr_t leaf, std::string_view key, const T& value, size_t m);
    insert_result prefix_leaf_multi(ptr_t leaf, std::string_view key, const T& value, size_t m);
    ptr_t clone_leaf_with_skip(ptr_t leaf, std::string_view new_skip);
    insert_result add_eos_to_leaf_multi(ptr_t leaf, const T& value);
    insert_result add_char_to_leaf(ptr_t leaf, unsigned char c, const T& value);
    insert_result demote_leaf_multi(ptr_t leaf, std::string_view key, const T& value);
    insert_result split_interior(ptr_t n, std::string_view key, const T& value, size_t m);
    ptr_t clone_interior_with_skip(ptr_t n, std::string_view new_skip);
    insert_result prefix_interior(ptr_t n, std::string_view key, const T& value, size_t m);
    insert_result set_interior_eos(ptr_t n, const T& value);
    insert_result add_child_to_interior(ptr_t n, unsigned char c, std::string_view remaining, const T& value);

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

    erase_spec_info probe_erase(ptr_t n, std::string_view key) const noexcept;
    erase_spec_info probe_leaf_erase(ptr_t n, std::string_view key, erase_spec_info& info) const noexcept;
    erase_spec_info probe_interior_erase(ptr_t n, std::string_view key, erase_spec_info& info) const noexcept;
    bool do_inplace_leaf_erase(ptr_t leaf, unsigned char c, uint64_t expected_version);
    erase_pre_alloc allocate_erase_speculative(const erase_spec_info& info);
    bool validate_erase_path(const erase_spec_info& info) const noexcept;
    bool commit_erase_speculative(erase_spec_info& info, erase_pre_alloc& alloc);
    void dealloc_erase_speculation(erase_pre_alloc& alloc);
    std::pair<bool, bool> erase_locked(std::string_view kb);
    erase_result erase_impl(atomic_ptr* slot, ptr_t n, std::string_view key);
    erase_result erase_from_leaf(ptr_t leaf, std::string_view key);
    erase_result erase_from_interior(ptr_t n, std::string_view key);
    erase_result try_collapse_interior(ptr_t n);
    erase_result try_collapse_after_child_removal(ptr_t n, unsigned char removed_c, erase_result& child_res);
    erase_result collapse_single_child(ptr_t n, unsigned char c, ptr_t child, erase_result& res);

public:
    tktrie();
    ~tktrie();
    tktrie(const tktrie& other);
    tktrie& operator=(const tktrie& other);
    tktrie(tktrie&& other) noexcept;
    tktrie& operator=(tktrie&& other) noexcept;

    void clear();
    size_t size() const noexcept { return size_.load(); }
    bool empty() const noexcept { return size() == 0; }
    bool contains(const Key& key) const;
    std::pair<iterator, bool> insert(const std::pair<const Key, T>& kv);
    bool erase(const Key& key);
    iterator find(const Key& key) const;
    iterator begin() const { return iterator::make_begin(this); }
    iterator end() const noexcept { return iterator::make_end(this); }
    void reclaim_retired() noexcept;
    
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
    using ptr_t = node_base<T, THREADED, Allocator, FIXED_LEN>*;
    
    using key_storage_t = std::conditional_t<(FIXED_LEN > 0),
        std::array<char, FIXED_LEN>,
        std::string>;

private:
    // Stack entry for traversal
    struct stack_entry {
        ptr_t node;
        int index;      // Current position within node (-1 = at EOS, 0+ = child/entry index)
        unsigned char edge;  // Edge char used to reach this node (0 for root)
    };
    
    static constexpr int MAX_DEPTH = 64;
    
    const trie_t* trie_ = nullptr;
    std::array<stack_entry, MAX_DEPTH> stack_{};
    int stack_depth_ = 0;
    std::string key_bytes_;  // Accumulated key
    T value_{};
    bool valid_ = false;
    bool at_eos_ = false;  // True if current position is at an EOS value

    // Push node onto stack
    void push(ptr_t node, int index, unsigned char edge) {
        if (stack_depth_ < MAX_DEPTH) {
            stack_[stack_depth_++] = {node, index, edge};
        }
    }
    
    // Pop from stack
    bool pop() {
        if (stack_depth_ > 0) {
            --stack_depth_;
            return true;
        }
        return false;
    }
    
    // Get current stack top
    stack_entry& top() { return stack_[stack_depth_ - 1]; }
    const stack_entry& top() const { return stack_[stack_depth_ - 1]; }
    
    // Descend to leftmost value from current node
    void descend_left(ptr_t node) {
        while (node && !node->is_poisoned()) {
            if (node->is_leaf()) {
                if (node->is_skip()) {
                    // SKIP leaf - single value
                    key_bytes_.append(node->skip_str());
                    push(node, 0, 0);
                    node->as_skip()->value.try_read(value_);
                    valid_ = true;
                    return;
                }
                // Multi-entry leaf - go to first entry
                key_bytes_.append(node->skip_str());
                push(node, 0, 0);
                load_leaf_value_at(node, 0);
                return;
            }
            
            // Interior node
            key_bytes_.append(node->skip_str());
            
            // Check EOS first (it's the "smallest" - empty suffix)
            if constexpr (FIXED_LEN == 0) {
                if (node->has_eos()) {
                    push(node, -1, 0);  // -1 indicates EOS position
                    node->try_read_eos(value_);
                    valid_ = true;
                    at_eos_ = true;
                    return;
                }
            }
            
            // Go to first child
            auto [c, child] = get_first_child(node);
            if (!child) {
                valid_ = false;
                return;
            }
            push(node, 0, c);
            key_bytes_.push_back(static_cast<char>(c));
            node = child;
        }
        valid_ = false;
    }
    
    // Descend to rightmost value from current node
    void descend_right(ptr_t node) {
        while (node && !node->is_poisoned()) {
            if (node->is_leaf()) {
                if (node->is_skip()) {
                    key_bytes_.append(node->skip_str());
                    push(node, 0, 0);
                    node->as_skip()->value.try_read(value_);
                    valid_ = true;
                    return;
                }
                // Multi-entry leaf - go to last entry
                key_bytes_.append(node->skip_str());
                int last = node->leaf_entry_count() - 1;
                push(node, last, 0);
                load_leaf_value_at(node, last);
                return;
            }
            
            // Interior node - go to last child first
            key_bytes_.append(node->skip_str());
            
            auto [c, child] = get_last_child(node);
            if (child) {
                push(node, get_child_count(node) - 1, c);
                key_bytes_.push_back(static_cast<char>(c));
                node = child;
            } else if constexpr (FIXED_LEN == 0) {
                // No children, check EOS
                if (node->has_eos()) {
                    push(node, -1, 0);
                    node->try_read_eos(value_);
                    valid_ = true;
                    at_eos_ = true;
                    return;
                }
                valid_ = false;
                return;
            } else {
                valid_ = false;
                return;
            }
        }
        valid_ = false;
    }
    
    // Get first child (smallest char)
    std::pair<unsigned char, ptr_t> get_first_child(ptr_t node) const {
        if (node->is_binary()) {
            auto* bn = node->template as_binary<false>();
            if (bn->count() > 0) return {bn->char_at(0), bn->child_at_slot(0)};
        } else if (node->is_list()) {
            auto* ln = node->template as_list<false>();
            if (ln->count() > 0) return {ln->char_at(0), ln->child_at_slot(0)};
        } else if (node->is_pop()) {
            auto* pn = node->template as_pop<false>();
            if (pn->count() > 0) {
                unsigned char c = pn->valid().first();
                return {c, pn->get_child(c)};
            }
        } else if (node->is_full()) {
            auto* fn = node->template as_full<false>();
            if (fn->count() > 0) {
                unsigned char c = fn->valid().first();
                return {c, fn->get_child(c)};
            }
        }
        return {0, nullptr};
    }
    
    // Get last child (largest char)
    std::pair<unsigned char, ptr_t> get_last_child(ptr_t node) const {
        if (node->is_binary()) {
            auto* bn = node->template as_binary<false>();
            int cnt = bn->count();
            if (cnt > 0) return {bn->char_at(cnt - 1), bn->child_at_slot(cnt - 1)};
        } else if (node->is_list()) {
            auto* ln = node->template as_list<false>();
            int cnt = ln->count();
            if (cnt > 0) return {ln->char_at(cnt - 1), ln->child_at_slot(cnt - 1)};
        } else if (node->is_pop()) {
            auto* pn = node->template as_pop<false>();
            unsigned char last = 255;
            while (last > 0 && !pn->valid().test(last)) --last;
            if (pn->valid().test(last)) return {last, pn->get_child(last)};
        } else if (node->is_full()) {
            auto* fn = node->template as_full<false>();
            unsigned char last = 255;
            while (last > 0 && !fn->valid().test(last)) --last;
            if (fn->valid().test(last)) return {last, fn->get_child(last)};
        }
        return {0, nullptr};
    }
    
    // Get child count
    int get_child_count(ptr_t node) const {
        return node->child_count();
    }
    
    // Get child at index (for BINARY/LIST) or by iteration (for POP/FULL)
    std::pair<unsigned char, ptr_t> get_child_at(ptr_t node, int idx) const {
        if (node->is_binary()) {
            auto* bn = node->template as_binary<false>();
            return {bn->char_at(idx), bn->child_at_slot(idx)};
        } else if (node->is_list()) {
            auto* ln = node->template as_list<false>();
            return {ln->char_at(idx), ln->child_at_slot(idx)};
        } else if (node->is_pop()) {
            auto* pn = node->template as_pop<false>();
            int i = 0;
            unsigned char result_c = 0;
            ptr_t result_p = nullptr;
            pn->valid().for_each_set([&](unsigned char c) {
                if (i == idx) { result_c = c; result_p = pn->get_child(c); }
                ++i;
            });
            return {result_c, result_p};
        } else {
            auto* fn = node->template as_full<false>();
            int i = 0;
            unsigned char result_c = 0;
            ptr_t result_p = nullptr;
            fn->valid().for_each_set([&](unsigned char c) {
                if (i == idx) { result_c = c; result_p = fn->get_child(c); }
                ++i;
            });
            return {result_c, result_p};
        }
    }
    
    // Load value from leaf at given index
    void load_leaf_value_at(ptr_t leaf, int idx) {
        unsigned char c = 0;
        if (leaf->is_binary()) {
            auto* bn = leaf->template as_binary<true>();
            c = bn->char_at(idx);
            bn->read_value(idx, value_);
        } else if (leaf->is_list()) {
            auto* ln = leaf->template as_list<true>();
            c = ln->char_at(idx);
            ln->read_value(idx, value_);
        } else if (leaf->is_pop()) {
            auto* pn = leaf->template as_pop<true>();
            int i = 0;
            pn->valid().for_each_set([&](unsigned char ch) {
                if (i == idx) {
                    c = ch;
                    pn->element_at_slot(idx).try_read(value_);
                }
                ++i;
            });
        } else {
            auto* fn = leaf->template as_full<true>();
            int i = 0;
            fn->valid().for_each_set([&](unsigned char ch) {
                if (i == idx) {
                    c = ch;
                    fn->read_value(ch, value_);
                }
                ++i;
            });
        }
        key_bytes_.push_back(static_cast<char>(c));
        valid_ = true;
    }

public:
    tktrie_iterator() = default;
    
    // Constructor from find() result
    tktrie_iterator(const trie_t* t, std::string_view kb, const T& v)
        : trie_(t), key_bytes_(kb), value_(v), valid_(true) {}
    
    // Constructor for begin()
    static tktrie_iterator make_begin(const trie_t* t) {
        tktrie_iterator it;
        it.trie_ = t;
        ptr_t root = t->test_root();
        if (root && !root->is_poisoned()) {
            it.descend_left(root);
        }
        return it;
    }
    
    // Constructor for end()
    static tktrie_iterator make_end(const trie_t* t) {
        tktrie_iterator it;
        it.trie_ = t;
        it.valid_ = false;
        return it;
    }
    
    // Constructor for end() before begin
    static tktrie_iterator make_rend(const trie_t* t) {
        tktrie_iterator it;
        it.trie_ = t;
        it.valid_ = false;
        return it;
    }

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
        if (!valid_ && !o.valid_) return true;
        if (valid_ != o.valid_) return false;
        return key_bytes_ == o.key_bytes_;
    }
    bool operator!=(const tktrie_iterator& o) const { return !(*this == o); }
    
    // Pre-increment: advance to next entry
    tktrie_iterator& operator++() {
        if (!valid_ || stack_depth_ == 0) {
            valid_ = false;
            return *this;
        }
        
        stack_entry& cur = top();
        ptr_t node = cur.node;
        
        if (node->is_leaf()) {
            if (node->is_skip()) {
                // SKIP has one value, go up
                key_bytes_.resize(key_bytes_.size() - node->skip_str().size());
                pop();
                advance_from_parent();
                return *this;
            }
            
            // Multi-entry leaf - try next entry
            int cnt = node->leaf_entry_count();
            key_bytes_.pop_back();  // Remove current entry's char
            
            if (cur.index + 1 < cnt) {
                cur.index++;
                load_leaf_value_at(node, cur.index);
                return *this;
            }
            
            // No more entries in this leaf, go up
            key_bytes_.resize(key_bytes_.size() - node->skip_str().size());
            pop();
            advance_from_parent();
            return *this;
        }
        
        // Interior node - we were at EOS or a child
        if (at_eos_) {
            at_eos_ = false;
            // Move to first child
            if (node->child_count() > 0) {
                auto [c, child] = get_first_child(node);
                cur.index = 0;
                cur.edge = c;
                key_bytes_.push_back(static_cast<char>(c));
                descend_left(child);
                return *this;
            }
            // No children, go up
            key_bytes_.resize(key_bytes_.size() - node->skip_str().size());
            pop();
            advance_from_parent();
            return *this;
        }
        
        // Should not reach here during normal iteration
        valid_ = false;
        return *this;
    }
    
    // Post-increment
    tktrie_iterator operator++(int) {
        tktrie_iterator tmp = *this;
        ++(*this);
        return tmp;
    }
    
    // Pre-decrement: go to previous entry
    tktrie_iterator& operator--() {
        if (stack_depth_ == 0) {
            // At end() or rend(), go to last element
            if (trie_) {
                ptr_t root = trie_->test_root();
                if (root && !root->is_poisoned()) {
                    descend_right(root);
                }
            }
            return *this;
        }
        
        stack_entry& cur = top();
        ptr_t node = cur.node;
        
        if (node->is_leaf()) {
            if (node->is_skip()) {
                // SKIP has one value, go up and left
                key_bytes_.resize(key_bytes_.size() - node->skip_str().size());
                pop();
                retreat_from_parent();
                return *this;
            }
            
            // Multi-entry leaf - try previous entry
            key_bytes_.pop_back();
            
            if (cur.index > 0) {
                cur.index--;
                load_leaf_value_at(node, cur.index);
                return *this;
            }
            
            // No more entries, go up
            key_bytes_.resize(key_bytes_.size() - node->skip_str().size());
            pop();
            retreat_from_parent();
            return *this;
        }
        
        // Interior node at EOS - nothing before EOS at this node, go up
        if (at_eos_) {
            at_eos_ = false;
            key_bytes_.resize(key_bytes_.size() - node->skip_str().size());
            pop();
            retreat_from_parent();
            return *this;
        }
        
        valid_ = false;
        return *this;
    }
    
    // Post-decrement
    tktrie_iterator operator--(int) {
        tktrie_iterator tmp = *this;
        --(*this);
        return tmp;
    }

private:
    // After popping from a leaf, try to advance within/from parent
    void advance_from_parent() {
        while (stack_depth_ > 0) {
            stack_entry& parent = top();
            ptr_t pnode = parent.node;
            
            // Remove edge char from key
            if (parent.edge != 0 || parent.index >= 0) {
                if (!key_bytes_.empty()) key_bytes_.pop_back();
            }
            
            int child_cnt = pnode->child_count();
            
            // Try next child
            if (parent.index + 1 < child_cnt) {
                parent.index++;
                auto [c, child] = get_child_at(pnode, parent.index);
                parent.edge = c;
                key_bytes_.push_back(static_cast<char>(c));
                descend_left(child);
                return;
            }
            
            // No more children, go up
            key_bytes_.resize(key_bytes_.size() - pnode->skip_str().size());
            pop();
        }
        valid_ = false;
    }
    
    // After popping from a leaf, try to retreat within/from parent
    void retreat_from_parent() {
        while (stack_depth_ > 0) {
            stack_entry& parent = top();
            ptr_t pnode = parent.node;
            
            // Remove edge char
            if (parent.edge != 0 || parent.index >= 0) {
                if (!key_bytes_.empty()) key_bytes_.pop_back();
            }
            
            // Try previous child
            if (parent.index > 0) {
                parent.index--;
                auto [c, child] = get_child_at(pnode, parent.index);
                parent.edge = c;
                key_bytes_.push_back(static_cast<char>(c));
                descend_right(child);
                return;
            }
            
            // At first child, check EOS
            if constexpr (FIXED_LEN == 0) {
                if (parent.index == 0 && pnode->has_eos()) {
                    parent.index = -1;
                    pnode->try_read_eos(value_);
                    valid_ = true;
                    at_eos_ = true;
                    return;
                }
            }
            
            // No EOS or already past it, go up
            key_bytes_.resize(key_bytes_.size() - pnode->skip_str().size());
            pop();
        }
        valid_ = false;
    }
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
