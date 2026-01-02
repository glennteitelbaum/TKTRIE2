#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_dataptr.h"
#include "tktrie_node.h"
#include "tktrie_help_common.h"
#include "tktrie_help_nav.h"
#include "tktrie_help_insert.h"
#include "tktrie_help_remove.h"

// Forward declarations - traits and iterator defined in tktrie.h
namespace gteitelbaum {
template <typename Key> struct tktrie_traits;
template <typename Key, typename T, bool THREADED, typename Allocator> class tktrie_iterator;
}

namespace gteitelbaum {

/**
 * Thread-safe trie with optimistic locking
 * 
 * WRITER PROTOCOL (THREADED mode):
 * 1. OUTSIDE LOCK: Collect pathA (slot addresses + values), build new nodes
 * 2. LOCK mutex
 * 3. Recollect pathB from root
 * 4. Compare pathA vs pathB (slot addresses and values)
 * 5. If different: add built nodes to unneeded_list, rebuild, goto 3
 * 6. At leaf: spin until READ_BIT clear on data ptr, if WRITE_BIT restart, else set WRITE_BIT
 * 7. If READ_BIT set on any path child slot: goto 3
 * 8. Set WRITE_BIT on all child slots from leaf up
 * 9. Set READ_BIT on old child slots (guards against other writers)
 * 10. Swap root
 * 11. UNLOCK mutex
 * 12. OUTSIDE LOCK: Delete unneeded_list (safe - never visible)
 * 13. Delete old nodes (safe - WRITE_BIT on data blocks readers, READ_BIT blocks writers)
 * 
 * READER PROTOCOL (THREADED mode):
 * - Check WRITE_BIT|READ_BIT on child slots before dereferencing → restart if set
 * - Double-check slot unchanged after loading pointer
 * - Data pointer: spin on READ_BIT, CAS to set, copy, clear
 */
template <typename Key, typename T, bool THREADED = false,
          typename Allocator = std::allocator<uint64_t>>
class tktrie {
public:
    using traits = tktrie_traits<Key>;
    static constexpr size_t fixed_len = traits::fixed_len;
    using size_type = std::size_t;
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<const Key, T>;
    using iterator = tktrie_iterator<Key, T, THREADED, Allocator>;
    using const_iterator = iterator;

private:
    using slot_type = slot_type_t<THREADED>;
    using node_view_t = node_view<T, THREADED, Allocator, fixed_len>;
    using node_builder_t = node_builder<T, THREADED, Allocator, fixed_len>;
    using dataptr_t = dataptr<T, THREADED, Allocator>;
    using nav_t = nav_helpers<T, THREADED, Allocator, fixed_len>;
    using insert_t = insert_helpers<T, THREADED, Allocator, fixed_len>;
    using remove_t = remove_helpers<T, THREADED, Allocator, fixed_len>;
    using debug_t = trie_debug<Key, T, THREADED, Allocator, fixed_len>;
    using path_step_t = path_step<THREADED>;

    // Mutex type based on THREADED
    using mutex_type = std::conditional_t<THREADED, std::mutex, empty_mutex>;
    
    // Root slot - stores pointer + control bits just like any child pointer
    slot_type root_slot_;
    std::conditional_t<THREADED, std::atomic<size_type>, size_type> elem_count_{0};
    mutable mutex_type write_mutex_;
    Allocator alloc_;
    node_builder_t builder_;

    friend class tktrie_iterator<Key, T, THREADED, Allocator>;

    // Helper to safely read root pointer (masks off control bits)
    slot_type* get_root() const noexcept {
        uint64_t val = load_slot<THREADED>(const_cast<slot_type*>(&root_slot_));
        if constexpr (THREADED) {
            return reinterpret_cast<slot_type*>(val & PTR_MASK);
        } else {
            return reinterpret_cast<slot_type*>(val);
        }
    }
    
    // Helper to get raw root slot value
    uint64_t get_root_slot_value() const noexcept {
        return load_slot<THREADED>(const_cast<slot_type*>(&root_slot_));
    }
    
    // Helper to safely write root pointer
    void set_root(slot_type* new_root) noexcept {
        store_slot<THREADED>(&root_slot_, reinterpret_cast<uint64_t>(new_root));
    }

public:
    // =========================================================================
    // Constructors and Destructor
    // =========================================================================

    tktrie() 
        : root_slot_{}
        , builder_(alloc_) {
        store_slot<THREADED>(&root_slot_, 0);
    }

    explicit tktrie(const Allocator& alloc)
        : root_slot_{}
        , alloc_(alloc)
        , builder_(alloc) {
        store_slot<THREADED>(&root_slot_, 0);
    }

    ~tktrie() {
        clear();
    }

    // Copy constructor (deep copy)
    tktrie(const tktrie& other)
        : root_slot_{}
        , alloc_(other.alloc_)
        , builder_(other.alloc_) {
        if constexpr (THREADED) {
            std::lock_guard<mutex_type> lock(other.write_mutex_);
        }
        slot_type* other_root = const_cast<tktrie&>(other).get_root();
        if (other_root) {
            set_root(builder_.deep_copy(other_root));
        } else {
            store_slot<THREADED>(&root_slot_, 0);
        }
        if constexpr (THREADED) {
            elem_count_.store(other.elem_count_.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);
        } else {
            elem_count_ = other.elem_count_;
        }
    }

    // Copy assignment (deep copy)
    tktrie& operator=(const tktrie& other) {
        if (this == &other) return *this;
        
        tktrie tmp(other);
        swap(tmp);
        return *this;
    }

    // Move constructor
    tktrie(tktrie&& other) noexcept
        : root_slot_{}
        , alloc_(std::move(other.alloc_))
        , builder_(alloc_) {
        if constexpr (THREADED) {
            std::lock_guard<mutex_type> lock(other.write_mutex_);
        }
        uint64_t other_val = load_slot<THREADED>(&other.root_slot_);
        store_slot<THREADED>(&root_slot_, other_val & PTR_MASK);
        store_slot<THREADED>(&other.root_slot_, 0);
        if constexpr (THREADED) {
            elem_count_.store(other.elem_count_.exchange(0, std::memory_order_relaxed),
                             std::memory_order_relaxed);
        } else {
            elem_count_ = other.elem_count_;
            other.elem_count_ = 0;
        }
    }

    // Move assignment
    tktrie& operator=(tktrie&& other) noexcept {
        if (this != &other) {
            clear();
            if constexpr (THREADED) {
                std::lock_guard<mutex_type> lock(other.write_mutex_);
            }
            uint64_t other_val = load_slot<THREADED>(&other.root_slot_);
            store_slot<THREADED>(&root_slot_, other_val & PTR_MASK);
            store_slot<THREADED>(&other.root_slot_, 0);
            alloc_ = std::move(other.alloc_);
            builder_ = node_builder_t(alloc_);
            if constexpr (THREADED) {
                elem_count_.store(other.elem_count_.exchange(0, std::memory_order_relaxed),
                                 std::memory_order_relaxed);
            } else {
                elem_count_ = other.elem_count_;
                other.elem_count_ = 0;
            }
        }
        return *this;
    }

    // Swap
    void swap(tktrie& other) noexcept {
        if constexpr (THREADED) {
            // Lock both in address order to prevent deadlock
            mutex_type* first = &write_mutex_ < &other.write_mutex_ ? &write_mutex_ : &other.write_mutex_;
            mutex_type* second = &write_mutex_ < &other.write_mutex_ ? &other.write_mutex_ : &write_mutex_;
            std::lock_guard<mutex_type> lock1(*first);
            std::lock_guard<mutex_type> lock2(*second);
        }
        uint64_t tmp = load_slot<THREADED>(&root_slot_);
        store_slot<THREADED>(&root_slot_, load_slot<THREADED>(&other.root_slot_) & PTR_MASK);
        store_slot<THREADED>(&other.root_slot_, tmp & PTR_MASK);
        std::swap(alloc_, other.alloc_);
        if constexpr (THREADED) {
            size_type tmp_count = elem_count_.load(std::memory_order_relaxed);
            elem_count_.store(other.elem_count_.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);
            other.elem_count_.store(tmp_count, std::memory_order_relaxed);
        } else {
            std::swap(elem_count_, other.elem_count_);
        }
    }

    // =========================================================================
    // Capacity
    // =========================================================================

    bool empty() const noexcept {
        return size() == 0;
    }

    size_type size() const noexcept {
        if constexpr (THREADED) {
            return elem_count_.load(std::memory_order_relaxed);
        } else {
            return elem_count_;
        }
    }

    // =========================================================================
    // Lookup
    // =========================================================================

    bool contains(const Key& key) const {
        std::string key_bytes;
        if constexpr (fixed_len > 0) {
            key_bytes = traits::to_bytes(key);
        } else {
            key_bytes = std::string(traits::to_bytes(key));
        }
        
        bool hit_write = false;
        
        if constexpr (THREADED) {
            while (true) {
                uint64_t root_val = get_root_slot_value();
                if (root_val & (WRITE_BIT | READ_BIT)) {
                    cpu_pause();
                    continue;
                }
                slot_type* root = reinterpret_cast<slot_type*>(root_val & PTR_MASK);
                bool result = nav_t::contains(root, key_bytes, hit_write);
                if (!hit_write) return result;
                cpu_pause();
            }
        } else {
            return nav_t::contains(get_root(), key_bytes, hit_write);
        }
    }

    iterator find(const Key& key) const {
        std::string key_bytes;
        if constexpr (fixed_len > 0) {
            key_bytes = traits::to_bytes(key);
        } else {
            key_bytes = std::string(traits::to_bytes(key));
        }
        
        T value;
        bool hit_write = false;
        
        if constexpr (THREADED) {
            while (true) {
                uint64_t root_val = get_root_slot_value();
                if (root_val & (WRITE_BIT | READ_BIT)) {
                    cpu_pause();
                    continue;
                }
                slot_type* root = reinterpret_cast<slot_type*>(root_val & PTR_MASK);
                bool found = nav_t::read(root, key_bytes, value, hit_write);
                if (!hit_write) {
                    if (found) {
                        return iterator(this, key_bytes, value);
                    }
                    return end();
                }
                cpu_pause();
            }
        } else {
            bool found = nav_t::read(get_root(), key_bytes, value, hit_write);
            if (found) {
                return iterator(this, key_bytes, value);
            }
            return end();
        }
    }

    iterator end() const noexcept {
        return iterator::end_iterator();
    }

    // =========================================================================
    // Modifiers
    // =========================================================================

    std::pair<iterator, bool> insert(const std::pair<const Key, T>& value) {
        return insert_impl(value.first, value.second);
    }

    std::pair<iterator, bool> insert(std::pair<const Key, T>&& value) {
        return insert_impl(value.first, std::move(value.second));
    }

    template <typename... Args>
    std::pair<iterator, bool> emplace(const Key& key, Args&&... args) {
        return insert_impl(key, T(std::forward<Args>(args)...));
    }

    bool erase(const Key& key) {
        return erase_impl(key);
    }

    void clear() {
        if constexpr (THREADED) {
            clear_threaded();
        } else {
            delete_tree_simple(get_root());
            set_root(nullptr);
            elem_count_ = 0;
        }
    }

    // =========================================================================
    // Iteration
    // =========================================================================

    iterator begin() const {
        if constexpr (THREADED) {
            while (true) {
                uint64_t root_val = get_root_slot_value();
                if (root_val & (WRITE_BIT | READ_BIT)) {
                    cpu_pause();
                    continue;
                }
                slot_type* root = reinterpret_cast<slot_type*>(root_val & PTR_MASK);
                if (!root) return end();
                
                std::string key;
                bool hit_write = false;
                slot_type* data_slot = nav_t::find_first_leaf(root, key, hit_write);
                if (hit_write) {
                    cpu_pause();
                    continue;
                }
                if (!data_slot) return end();
                
                dataptr_t* dp = reinterpret_cast<dataptr_t*>(data_slot);
                T value;
                if (!dp->try_read(value)) {
                    cpu_pause();
                    continue;
                }
                return iterator(this, key, value);
            }
        } else {
            slot_type* root = get_root();
            if (!root) return end();
            
            std::string key;
            bool hit_write = false;
            slot_type* data_slot = nav_t::find_first_leaf(root, key, hit_write);
            if (!data_slot) return end();
            
            dataptr_t* dp = reinterpret_cast<dataptr_t*>(data_slot);
            T value;
            if (!dp->try_read(value)) return end();
            return iterator(this, key, value);
        }
    }

    iterator next_after(const std::string& key_bytes) const {
        // TODO: Implement proper in-order traversal
        return end();
    }

    // =========================================================================
    // Debug
    // =========================================================================

    void pretty_print(std::ostream& os = std::cout) const {
        os << "tktrie<" << (THREADED ? "THREADED" : "SINGLE") 
           << ", fixed_len=" << fixed_len << "> size=" << size() << "\n";
        if (get_root()) {
            debug_t::pretty_print_node(get_root(), os, 0, "", 0);
        } else {
            os << "  (empty)\n";
        }
    }

    void validate() const {
        if constexpr (k_validate) {
            std::string err = debug_t::validate_node(get_root(), 0);
            KTRIE_DEBUG_ASSERT(err.empty());
        }
    }

    // =========================================================================
    // Prefix Operations (stubs)
    // =========================================================================

    std::pair<iterator, iterator> prefix_range(const std::string& prefix) const
        requires (fixed_len == 0) {
        return {end(), end()};
    }

    std::pair<iterator, iterator> prefix_range(const Key& key, size_t depth) const
        requires (fixed_len > 0) {
        return {end(), end()};
    }

private:
    // =========================================================================
    // Simple Delete (non-threaded or after locking)
    // =========================================================================

    void delete_tree_simple(slot_type* node, size_t depth = 0) {
        if (!node) return;

        node_view_t view(node);
        size_t skip_len = view.has_skip() ? view.skip_length() : 0;
        
        // Recursively delete children
        int num_children = view.child_count();
        for (int i = 0; i < num_children; ++i) {
            uint64_t child_ptr = view.get_child_ptr(i);
            child_ptr &= PTR_MASK;

            // FIXED_LEN leaf optimization
            if constexpr (fixed_len > 0 && !THREADED) {
                if (depth + skip_len == fixed_len - 1) {
                    dataptr_t* dp = reinterpret_cast<dataptr_t*>(&view.child_ptrs()[i]);
                    dp->~dataptr_t();
                    continue;
                }
            }

            slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
            if (child) {
                delete_tree_simple(child, depth + skip_len + 1);
            }
        }

        builder_.deallocate_node(node);
    }

    // =========================================================================
    // Insert Implementation
    // =========================================================================

    template <typename U>
    std::pair<iterator, bool> insert_impl(const Key& key, U&& value) {
        std::string key_bytes;
        if constexpr (fixed_len > 0) {
            key_bytes = traits::to_bytes(key);
        } else {
            key_bytes = std::string(traits::to_bytes(key));
        }

        if constexpr (THREADED) {
            return insert_threaded(key, key_bytes, std::forward<U>(value));
        } else {
            return insert_single(key, key_bytes, std::forward<U>(value));
        }
    }

    template <typename U>
    std::pair<iterator, bool> insert_single(const Key& key, 
                                             const std::string& key_bytes,
                                             U&& value) {
        auto result = insert_t::build_insert_path(builder_, get_root(), key_bytes, 
                                                   std::forward<U>(value));

        if (result.already_exists) {
            for (auto* n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            return {find(key), false};
        }

        if (result.new_root) {
            set_root(result.new_root);
        }

        for (auto* n : result.old_nodes) {
            if (n != result.new_root) {
                builder_.deallocate_node(n);
            }
        }

        ++elem_count_;
        validate_trie_impl<Key, T, THREADED, Allocator, fixed_len>(get_root());

        T val;
        if constexpr (std::is_rvalue_reference_v<U&&>) {
            val = std::forward<U>(value);
        } else {
            val = value;
        }
        return {iterator(this, key_bytes, val), true};
    }

    template <typename U>
    std::pair<iterator, bool> insert_threaded(const Key& key,
                                               const std::string& key_bytes,
                                               U&& value) {
        std::vector<slot_type*> unneeded;
        std::vector<slot_type*> to_free;
        unneeded.reserve(16);
        to_free.reserve(16);
        bool already_exists_exit = false;

        // Step 1: OUTSIDE LOCK - collect pathA and build optimistically
        auto resultA = insert_t::build_insert_path(builder_, get_root(), key_bytes,
                                                    std::forward<U>(value));
        
        if (resultA.hit_write || resultA.hit_read) {
            // Hit control bits, deallocate and restart
            for (auto* n : resultA.new_nodes) builder_.deallocate_node(n);
            cpu_pause();
            return insert_threaded(key, key_bytes, std::forward<U>(value));
        }

        {
            std::lock_guard<mutex_type> lock(write_mutex_);

        retry_inside_lock:
            // Step 3: Recollect pathB
            auto resultB = insert_t::build_insert_path(builder_, get_root(), key_bytes,
                                                        std::forward<U>(value));

            // Step 4: Compare paths
            bool paths_match = compare_paths(resultA.path, resultB.path);

            // Step 5: If different, add to unneeded and rebuild
            if (!paths_match || resultA.expected_root != resultB.expected_root) {
                for (auto* n : resultA.new_nodes) unneeded.push_back(n);
                resultA = std::move(resultB);
                // Now resultA has the freshly collected path
                
                if (resultA.hit_write || resultA.hit_read) {
                    // Hit control bits during recollection, retry
                    for (auto* n : resultA.new_nodes) unneeded.push_back(n);
                    resultA = insert_t::build_insert_path(builder_, get_root(), key_bytes,
                                                          std::forward<U>(value));
                }
                goto retry_inside_lock;
            }

            // At this point resultA == resultB
            if (resultA.already_exists) {
                for (auto* n : resultA.new_nodes) unneeded.push_back(n);
                already_exists_exit = true;
                // Exit lock scope, deallocate outside
            } else {
                // Step 6: Handle data pointer at leaf
                // The leaf data pointer is in the NEW node we created, not old tree
                // Old tree's data (if any) is in old_nodes - handled by deletion

                // Step 7: Check READ_BIT on path child slots (would indicate another writer)
                for (const auto& step : resultA.path) {
                    if (step.child_slot) {
                        uint64_t current = load_slot<THREADED>(step.child_slot);
                        if (current & READ_BIT) {
                            // Another writer is deleting - rebuild
                            for (auto* n : resultA.new_nodes) unneeded.push_back(n);
                            resultA = insert_t::build_insert_path(builder_, get_root(), key_bytes,
                                                                  std::forward<U>(value));
                            goto retry_inside_lock;
                        }
                    }
                }

                // Step 8: Set WRITE_BIT on all old path child slots (leaf to root order)
                // This blocks readers from traversing into old subtree
                for (auto it = resultA.path.rbegin(); it != resultA.path.rend(); ++it) {
                    if (it->child_slot) {
                        fetch_or_slot<THREADED>(it->child_slot, WRITE_BIT);
                    }
                }

                // Step 9: Set READ_BIT on old child slots (guards against other writers)
                for (const auto& step : resultA.path) {
                    if (step.child_slot) {
                        fetch_or_slot<THREADED>(step.child_slot, READ_BIT);
                    }
                }

                // Step 10: Swap root
                if (resultA.new_root) {
                    set_root(resultA.new_root);
                }

                elem_count_.fetch_add(1, std::memory_order_relaxed);
                to_free = std::move(resultA.old_nodes);
            }

        }  // Step 11: UNLOCK

        // Step 12: Delete unneeded (outside lock - safe, never visible)
        for (auto* n : unneeded) {
            builder_.deallocate_node(n);
        }

        // Handle early exit for already_exists case
        if (already_exists_exit) {
            return {find(key), false};
        }

        // Step 13: Delete old nodes (outside lock)
        // WRITE_BIT on data pointer blocks readers
        // READ_BIT on child slots blocks other writers from double-delete
        for (auto* n : to_free) {
            if (n) {
                node_view_t view(n);
                // Wait for any readers finishing data copy
                if (view.has_eos()) view.eos_data()->begin_write();
                if (view.has_skip_eos()) view.skip_eos_data()->begin_write();
                builder_.deallocate_node(n);
            }
        }

        validate_trie_impl<Key, T, THREADED, Allocator, fixed_len>(get_root());

        T val;
        if constexpr (std::is_rvalue_reference_v<U&&>) {
            val = std::forward<U>(value);
        } else {
            val = value;
        }
        return {iterator(this, key_bytes, val), true};
    }

    // =========================================================================
    // Erase Implementation
    // =========================================================================

    bool erase_impl(const Key& key) {
        std::string key_bytes;
        if constexpr (fixed_len > 0) {
            key_bytes = traits::to_bytes(key);
        } else {
            key_bytes = std::string(traits::to_bytes(key));
        }

        if constexpr (THREADED) {
            return erase_threaded(key_bytes);
        } else {
            return erase_single(key_bytes);
        }
    }

    bool erase_single(const std::string& key_bytes) {
        auto result = remove_t::build_remove_path(builder_, get_root(), key_bytes);

        if (!result.found) {
            return false;
        }

        if (result.root_deleted) {
            set_root(nullptr);
        } else if (result.new_root) {
            set_root(result.new_root);
        }

        for (auto* n : result.old_nodes) {
            if (n != result.new_root) {
                builder_.deallocate_node(n);
            }
        }

        --elem_count_;
        validate_trie_impl<Key, T, THREADED, Allocator, fixed_len>(get_root());

        return true;
    }

    bool erase_threaded(const std::string& key_bytes) {
        std::vector<slot_type*> unneeded;
        std::vector<slot_type*> to_free;
        unneeded.reserve(16);
        to_free.reserve(16);
        bool not_found_exit = false;

        // Step 1: OUTSIDE LOCK - collect pathA and build optimistically
        auto resultA = remove_t::build_remove_path(builder_, get_root(), key_bytes);
        
        if (resultA.hit_write || resultA.hit_read) {
            for (auto* n : resultA.new_nodes) builder_.deallocate_node(n);
            cpu_pause();
            return erase_threaded(key_bytes);
        }

        {
            std::lock_guard<mutex_type> lock(write_mutex_);

        retry_inside_lock:
            auto resultB = remove_t::build_remove_path(builder_, get_root(), key_bytes);

            bool paths_match = compare_paths(resultA.path, resultB.path);

            if (!paths_match || resultA.expected_root != resultB.expected_root) {
                for (auto* n : resultA.new_nodes) unneeded.push_back(n);
                resultA = std::move(resultB);
                
                if (resultA.hit_write || resultA.hit_read) {
                    for (auto* n : resultA.new_nodes) unneeded.push_back(n);
                    resultA = remove_t::build_remove_path(builder_, get_root(), key_bytes);
                }
                goto retry_inside_lock;
            }

            if (!resultA.found) {
                for (auto* n : resultA.new_nodes) unneeded.push_back(n);
                not_found_exit = true;
                // Exit lock scope, deallocate outside
            } else {
                // Check READ_BIT on path
                for (const auto& step : resultA.path) {
                    if (step.child_slot) {
                        uint64_t current = load_slot<THREADED>(step.child_slot);
                        if (current & READ_BIT) {
                            for (auto* n : resultA.new_nodes) unneeded.push_back(n);
                            resultA = remove_t::build_remove_path(builder_, get_root(), key_bytes);
                            goto retry_inside_lock;
                        }
                    }
                }

                // Set WRITE_BIT leaf to root
                for (auto it = resultA.path.rbegin(); it != resultA.path.rend(); ++it) {
                    if (it->child_slot) {
                        fetch_or_slot<THREADED>(it->child_slot, WRITE_BIT);
                    }
                }

                // Set READ_BIT on old slots
                for (const auto& step : resultA.path) {
                    if (step.child_slot) {
                        fetch_or_slot<THREADED>(step.child_slot, READ_BIT);
                    }
                }

                // Swap root
                if (resultA.root_deleted) {
                    set_root(nullptr);
                } else if (resultA.new_root) {
                    set_root(resultA.new_root);
                }

                elem_count_.fetch_sub(1, std::memory_order_relaxed);
                to_free = std::move(resultA.old_nodes);
            }

        }  // UNLOCK

        for (auto* n : unneeded) {
            builder_.deallocate_node(n);
        }

        // Handle early exit for not found case
        if (not_found_exit) {
            return false;
        }

        for (auto* n : to_free) {
            if (n) {
                node_view_t view(n);
                if (view.has_eos()) view.eos_data()->begin_write();
                if (view.has_skip_eos()) view.skip_eos_data()->begin_write();
                builder_.deallocate_node(n);
            }
        }

        validate_trie_impl<Key, T, THREADED, Allocator, fixed_len>(get_root());

        return true;
    }

    // =========================================================================
    // Clear Implementation (DELETE ALL)
    // =========================================================================

    void clear_threaded() {
        std::vector<slot_type*> to_free;
        to_free.reserve(16);

        {
            std::lock_guard<mutex_type> lock(write_mutex_);

            slot_type* root = get_root();
            if (!root) {
                elem_count_.store(0, std::memory_order_relaxed);
                return;
            }

            // Mark entire tree for deletion (depth-first, leaf to root)
            // Sets WRITE_BIT on all child slots, waits for READ_BIT on data pointers
            if (!mark_tree_for_deletion(root, to_free)) {
                // Hit WRITE_BIT - shouldn't happen under mutex, but handle it
                // This means something is very wrong
                return;
            }

            // Swap root to nullptr
            set_root(nullptr);
            elem_count_.store(0, std::memory_order_relaxed);

        }  // UNLOCK

        // Delete all nodes (outside lock)
        for (auto* n : to_free) {
            if (n) {
                node_view_t view(n);
                if (view.has_eos()) view.eos_data()->begin_write();
                if (view.has_skip_eos()) view.skip_eos_data()->begin_write();
                builder_.deallocate_node(n);
            }
        }
    }

    /**
     * Recursively mark tree for deletion
     * Depth-first traversal: process children first, then current node
     * Sets WRITE_BIT on child pointers, waits for READ_BIT on data pointers
     * Returns false if hit WRITE_BIT (another writer - shouldn't happen under mutex)
     */
    bool mark_tree_for_deletion(slot_type* node, std::vector<slot_type*>& to_free,
                                 size_t depth = 0) {
        if (!node) return true;

        node_view_t view(node);
        size_t skip_len = view.has_skip() ? view.skip_length() : 0;

        // Recursively process children first (depth-first)
        int num_children = view.child_count();
        for (int i = 0; i < num_children; ++i) {
            slot_type* child_slot = &view.child_ptrs()[i];
            uint64_t child_ptr = load_slot<THREADED>(child_slot);

            // Check for WRITE_BIT (shouldn't happen under mutex)
            if (child_ptr & WRITE_BIT) {
                return false;  // Another writer active
            }

            // FIXED_LEN leaf: children are inline dataptr
            if constexpr (fixed_len > 0) {
                if (depth + skip_len == fixed_len - 1) {
                    // Don't recurse - this is an inline dataptr
                    continue;
                }
            }

            slot_type* child = reinterpret_cast<slot_type*>(child_ptr & PTR_MASK);
            if (child) {
                // Recurse into child
                if (!mark_tree_for_deletion(child, to_free, depth + skip_len + 1)) {
                    return false;
                }
            }

            // After processing child subtree, set WRITE_BIT on this slot
            fetch_or_slot<THREADED>(child_slot, WRITE_BIT);
        }

        // Process data pointers at this node
        // Wait for READ_BIT to clear, then set WRITE_BIT
        if (view.has_eos()) {
            dataptr_t* dp = view.eos_data();
            // Spin until READ_BIT clear
            while (true) {
                uint64_t bits = dp->to_u64();
                if (bits & WRITE_BIT) return false;  // Shouldn't happen
                if (!(bits & READ_BIT)) break;
                cpu_pause();
            }
            dp->begin_write();  // Sets WRITE_BIT, waits for READ_BIT
        }

        if (view.has_skip_eos()) {
            dataptr_t* dp = view.skip_eos_data();
            while (true) {
                uint64_t bits = dp->to_u64();
                if (bits & WRITE_BIT) return false;
                if (!(bits & READ_BIT)) break;
                cpu_pause();
            }
            dp->begin_write();
        }

        // Set READ_BIT on this node's slots and add to free list
        for (int i = 0; i < num_children; ++i) {
            fetch_or_slot<THREADED>(&view.child_ptrs()[i], READ_BIT);
        }

        to_free.push_back(node);
        return true;
    }

    // =========================================================================
    // Helper Functions
    // =========================================================================

    /**
     * Compare two paths for equality
     * Paths match if same length and all steps have same slot address and value
     */
    static bool compare_paths(const std::vector<path_step_t>& pathA,
                               const std::vector<path_step_t>& pathB) {
        if (pathA.size() != pathB.size()) return false;
        
        for (size_t i = 0; i < pathA.size(); ++i) {
            // Compare slot addresses
            if (pathA[i].child_slot != pathB[i].child_slot) return false;
            // Compare expected values (including bits)
            if (pathA[i].expected_ptr != pathB[i].expected_ptr) return false;
        }
        return true;
    }
};

// Swap specialization
template <typename Key, typename T, bool THREADED, typename Allocator>
void swap(tktrie<Key, T, THREADED, Allocator>& lhs,
          tktrie<Key, T, THREADED, Allocator>& rhs) noexcept {
    lhs.swap(rhs);
}

}  // namespace gteitelbaum
