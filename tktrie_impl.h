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
 * @tparam Key     Key type (string or integral)
 * @tparam T       Value type
 * @tparam THREADED Enable thread safety (default: false)
 * @tparam Allocator Allocator type (default: std::allocator<uint64_t>)
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
    
    // Helper to safely write root pointer (preserves no control bits for fresh writes)
    void set_root(slot_type* new_root) noexcept {
        store_slot<THREADED>(&root_slot_, reinterpret_cast<uint64_t>(new_root));
    }
    
    // Helper to check if root has WRITE_BIT set
    bool root_has_write_bit() const noexcept {
        if constexpr (THREADED) {
            return (get_root_slot_value() & WRITE_BIT) != 0;
        } else {
            return false;
        }
    }
    
    // Helper to set WRITE_BIT on root
    void set_root_write_bit() noexcept {
        if constexpr (THREADED) {
            fetch_or_slot<THREADED>(&root_slot_, WRITE_BIT);
        }
    }
    
    // Helper to wait for READ_BIT to clear on root
    void wait_for_root_readers() noexcept {
        if constexpr (THREADED) {
            while (get_root_slot_value() & READ_BIT) {
                cpu_pause();
            }
        }
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
        if constexpr (THREADED) {
            // Lock mutex to prevent concurrent writers
            std::lock_guard<mutex_type> lock(write_mutex_);
            // Set WRITE_BIT and wait for readers before destroying
            set_root_write_bit();
            wait_for_root_readers();
        }
        delete_tree(get_root());
        store_slot<THREADED>(&root_slot_, 0);
    }

    // Copy constructor (deep copy) - locks source to prevent concurrent writes
    tktrie(const tktrie& other)
        : root_slot_{}
        , alloc_(other.alloc_)
        , builder_(other.alloc_) {
        if constexpr (THREADED) {
            std::lock_guard<mutex_type> lock(other.write_mutex_);
            slot_type* other_root = const_cast<tktrie&>(other).get_root();
            if (other_root) {
                set_root(builder_.deep_copy(other_root));
            } else {
                store_slot<THREADED>(&root_slot_, 0);
            }
            elem_count_.store(other.elem_count_.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);
        } else {
            slot_type* other_root = const_cast<tktrie&>(other).get_root();
            if (other_root) {
                set_root(builder_.deep_copy(other_root));
            } else {
                store_slot<THREADED>(&root_slot_, 0);
            }
            elem_count_ = other.elem_count_;
        }
    }

    // Copy assignment (deep copy) - locks both to prevent concurrent writes
    tktrie& operator=(const tktrie& other) {
        if (this == &other) return *this;
        
        if constexpr (THREADED) {
            // Lock both mutexes in address order to prevent deadlock
            mutex_type* first = &write_mutex_ < &other.write_mutex_ ? &write_mutex_ : &other.write_mutex_;
            mutex_type* second = &write_mutex_ < &other.write_mutex_ ? &other.write_mutex_ : &write_mutex_;
            
            std::lock_guard<mutex_type> lock1(*first);
            std::lock_guard<mutex_type> lock2(*second);
            
            // Set WRITE_BIT and wait for readers on destination
            set_root_write_bit();
            wait_for_root_readers();
            
            slot_type* old_root = get_root();
            
            // Copy from source
            slot_type* other_root = const_cast<tktrie&>(other).get_root();
            if (other_root) {
                set_root(builder_.deep_copy(other_root));
            } else {
                store_slot<THREADED>(&root_slot_, 0);
            }
            elem_count_.store(other.elem_count_.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);
            
            // Delete old tree (still inside lock, but readers already blocked)
            delete_tree(old_root);
        } else {
            slot_type* old_root = get_root();
            slot_type* other_root = const_cast<tktrie&>(other).get_root();
            if (other_root) {
                set_root(builder_.deep_copy(other_root));
            } else {
                store_slot<THREADED>(&root_slot_, 0);
            }
            elem_count_ = other.elem_count_;
            delete_tree(old_root);
        }
        return *this;
    }

    // Move constructor - locks source mutex to prevent concurrent access
    tktrie(tktrie&& other) noexcept
        : root_slot_{}
        , alloc_(std::move(other.alloc_))
        , builder_(alloc_) {
        if constexpr (THREADED) {
            std::lock_guard<mutex_type> lock(other.write_mutex_);
            
            // Set WRITE_BIT on source and wait for readers
            fetch_or_slot<THREADED>(&other.root_slot_, WRITE_BIT);
            while (load_slot<THREADED>(&other.root_slot_) & READ_BIT) {
                cpu_pause();
            }
            
            // Take ownership of other's root
            uint64_t other_val = load_slot<THREADED>(&other.root_slot_);
            store_slot<THREADED>(&root_slot_, other_val & PTR_MASK);
            store_slot<THREADED>(&other.root_slot_, 0);
            
            elem_count_.store(other.elem_count_.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);
            other.elem_count_.store(0, std::memory_order_relaxed);
        } else {
            uint64_t other_val = load_slot<THREADED>(&other.root_slot_);
            store_slot<THREADED>(&root_slot_, other_val);
            store_slot<THREADED>(&other.root_slot_, 0);
            elem_count_ = other.elem_count_;
            other.elem_count_ = 0;
        }
    }

    // Move assignment - locks both mutexes to prevent concurrent access
    tktrie& operator=(tktrie&& other) noexcept {
        if (this == &other) return *this;
        
        if constexpr (THREADED) {
            // Lock both mutexes in address order to prevent deadlock
            mutex_type* first = &write_mutex_ < &other.write_mutex_ ? &write_mutex_ : &other.write_mutex_;
            mutex_type* second = &write_mutex_ < &other.write_mutex_ ? &other.write_mutex_ : &write_mutex_;
            
            std::lock_guard<mutex_type> lock1(*first);
            std::lock_guard<mutex_type> lock2(*second);
            
            // Set WRITE_BIT and wait for readers on both
            set_root_write_bit();
            wait_for_root_readers();
            fetch_or_slot<THREADED>(&other.root_slot_, WRITE_BIT);
            while (load_slot<THREADED>(&other.root_slot_) & READ_BIT) {
                cpu_pause();
            }
            
            slot_type* old_root = get_root();
            
            // Take ownership of other's root
            uint64_t other_val = load_slot<THREADED>(&other.root_slot_);
            store_slot<THREADED>(&root_slot_, other_val & PTR_MASK);
            store_slot<THREADED>(&other.root_slot_, 0);
            
            elem_count_.store(other.elem_count_.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);
            other.elem_count_.store(0, std::memory_order_relaxed);
            
            // Delete old tree
            delete_tree(old_root);
        } else {
            slot_type* old_root = get_root();
            uint64_t other_val = load_slot<THREADED>(&other.root_slot_);
            store_slot<THREADED>(&root_slot_, other_val);
            store_slot<THREADED>(&other.root_slot_, 0);
            alloc_ = std::move(other.alloc_);
            builder_ = node_builder_t(alloc_);
            elem_count_ = other.elem_count_;
            other.elem_count_ = 0;
            delete_tree(old_root);
        }
        return *this;
    }

    // Swap - locks both mutexes to prevent concurrent access
    void swap(tktrie& other) noexcept {
        if (this == &other) return;
        
        if constexpr (THREADED) {
            // Lock both mutexes in address order to prevent deadlock
            mutex_type* first = &write_mutex_ < &other.write_mutex_ ? &write_mutex_ : &other.write_mutex_;
            mutex_type* second = &write_mutex_ < &other.write_mutex_ ? &other.write_mutex_ : &write_mutex_;
            
            std::lock_guard<mutex_type> lock1(*first);
            std::lock_guard<mutex_type> lock2(*second);
            
            // Set WRITE_BIT on both and wait for readers
            set_root_write_bit();
            wait_for_root_readers();
            fetch_or_slot<THREADED>(&other.root_slot_, WRITE_BIT);
            while (load_slot<THREADED>(&other.root_slot_) & READ_BIT) cpu_pause();
            
            uint64_t tmp_val = load_slot<THREADED>(&root_slot_);
            uint64_t other_val = load_slot<THREADED>(&other.root_slot_);
            
            // Swap pointers only (clear control bits)
            store_slot<THREADED>(&root_slot_, other_val & PTR_MASK);
            store_slot<THREADED>(&other.root_slot_, tmp_val & PTR_MASK);
            
            size_type tmp = elem_count_.load(std::memory_order_relaxed);
            elem_count_.store(other.elem_count_.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);
            other.elem_count_.store(tmp, std::memory_order_relaxed);
        } else {
            uint64_t tmp_val = load_slot<THREADED>(&root_slot_);
            uint64_t other_val = load_slot<THREADED>(&other.root_slot_);
            store_slot<THREADED>(&root_slot_, other_val);
            store_slot<THREADED>(&other.root_slot_, tmp_val);
            std::swap(elem_count_, other.elem_count_);
        }
        
        std::swap(alloc_, other.alloc_);
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
                // Check WRITE_BIT on root first
                uint64_t root_val = get_root_slot_value();
                if (root_val & WRITE_BIT) {
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
                // Check WRITE_BIT on root first
                uint64_t root_val = get_root_slot_value();
                if (root_val & WRITE_BIT) {
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
            std::lock_guard<mutex_type> lock(write_mutex_);
            set_root_write_bit();
            wait_for_root_readers();
        }
        
        delete_tree(get_root());
        store_slot<THREADED>(&root_slot_, 0);
        
        if constexpr (THREADED) {
            elem_count_.store(0, std::memory_order_relaxed);
        } else {
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
                if (root_val & WRITE_BIT) {
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

    /**
     * Find next iterator after given key
     * Used by iterator::operator++
     */
    iterator next_after(const std::string& key_bytes) const {
        // TODO: Implement proper in-order traversal
        // For now, return end
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
        // TODO: Implement
        return {end(), end()};
    }

    std::pair<iterator, iterator> prefix_range(const Key& key, size_t depth) const
        requires (fixed_len > 0) {
        // TODO: Implement
        return {end(), end()};
    }

private:
    // =========================================================================
    // Implementation Details
    // =========================================================================

    void delete_tree(slot_type* node, size_t depth = 0) {
        if (!node) return;

        node_view_t view(node);
        
        size_t skip_len = view.has_skip() ? view.skip_length() : 0;
        
        // For THREADED: wait for readers on this node's dataptrs before deallocating
        if constexpr (THREADED) {
            if (view.has_eos()) {
                view.eos_data()->begin_write();
            }
            if (view.has_skip_eos()) {
                view.skip_eos_data()->begin_write();
            }
        }
        
        // Recursively delete children
        int num_children = view.child_count();
        for (int i = 0; i < num_children; ++i) {
            uint64_t child_ptr = view.get_child_ptr(i);
            
            if constexpr (THREADED) {
                child_ptr &= PTR_MASK;
            }

            // FIXED_LEN leaf optimization: non-threaded stores dataptr inline at leaf depth
            if constexpr (fixed_len > 0 && !THREADED) {
                if (depth + skip_len == fixed_len - 1) {
                    // Child is inline dataptr - destroy it
                    dataptr_t* dp = reinterpret_cast<dataptr_t*>(&view.child_ptrs()[i]);
                    dp->~dataptr_t();
                    continue;
                }
            }

            slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
            if (child) {
                delete_tree(child, depth + skip_len + 1);
            }
        }

        builder_.deallocate_node(node);
    }

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
            // Clean up any allocated nodes
            for (auto* n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            return {find(key), false};
        }

        // Update root
        if (result.new_root) {
            set_root(result.new_root);
        }

        // Delete old nodes
        for (auto* n : result.old_nodes) {
            // Don't delete if it's the same as new_root (shouldn't happen)
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
        std::vector<slot_type*> to_free;
        std::vector<path_step<THREADED>> committed_path;

        {
            std::lock_guard<mutex_type> lock(write_mutex_);

            // Set WRITE_BIT on root first (blocks new readers)
            set_root_write_bit();

            // Build path INSIDE lock
            auto result = insert_t::build_insert_path(builder_, get_root(), key_bytes,
                                                       std::forward<U>(value));

            if (result.already_exists) {
                // Clear WRITE_BIT on root since we're not changing anything
                store_slot<THREADED>(&root_slot_, reinterpret_cast<uint64_t>(get_root()));
                for (auto* n : result.new_nodes) {
                    builder_.deallocate_node(n);
                }
                return {find(key), false};
            }

            // Set WRITE_BIT on old path child slots (blocks readers on those paths)
            // Set READ_BIT on slots pointing to nodes we'll delete (guards deletion)
            for (const auto& step : result.path) {
                if (step.child_slot) {
                    fetch_or_slot<THREADED>(step.child_slot, WRITE_BIT | READ_BIT);
                }
            }
            committed_path = std::move(result.path);

            // Swap root (clears WRITE_BIT since we're storing fresh pointer)
            if (result.new_root) {
                set_root(result.new_root);
            } else {
                // Clear WRITE_BIT but keep same pointer
                store_slot<THREADED>(&root_slot_, reinterpret_cast<uint64_t>(get_root()));
            }

            elem_count_.fetch_add(1, std::memory_order_relaxed);
            to_free = std::move(result.old_nodes);

        }  // Lock released

        // Dealloc OUTSIDE lock - wait for dataptr readers, then clear READ_BIT
        for (auto* n : to_free) {
            if (n) {
                node_view_t view(n);
                if (view.has_eos()) view.eos_data()->begin_write();
                if (view.has_skip_eos()) view.skip_eos_data()->begin_write();
                builder_.deallocate_node(n);
            }
        }

        // Clear READ_BIT on committed path (old slots now point to freed memory,
        // but READ_BIT prevented other writers from trying to delete same nodes)
        for (const auto& step : committed_path) {
            if (step.child_slot) {
                fetch_and_slot<THREADED>(step.child_slot, ~READ_BIT);
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

        // Update root
        if (result.root_deleted) {
            set_root(nullptr);
        } else if (result.new_root) {
            set_root(result.new_root);
        }

        // Delete old nodes
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
        std::vector<slot_type*> to_free;
        std::vector<path_step<THREADED>> committed_path;

        {
            std::lock_guard<mutex_type> lock(write_mutex_);

            // Set WRITE_BIT on root first (blocks new readers)
            set_root_write_bit();

            // Build removal path INSIDE lock
            auto result = remove_t::build_remove_path(builder_, get_root(), key_bytes);

            if (!result.found) {
                // Clear WRITE_BIT on root since we're not changing anything
                store_slot<THREADED>(&root_slot_, reinterpret_cast<uint64_t>(get_root()));
                for (auto* n : result.new_nodes) {
                    builder_.deallocate_node(n);
                }
                return false;
            }

            // Set WRITE_BIT and READ_BIT on old path child slots
            for (const auto& step : result.path) {
                if (step.child_slot) {
                    fetch_or_slot<THREADED>(step.child_slot, WRITE_BIT | READ_BIT);
                }
            }
            committed_path = std::move(result.path);

            // Swap root (clears WRITE_BIT)
            if (result.root_deleted) {
                set_root(nullptr);
            } else if (result.new_root) {
                set_root(result.new_root);
            } else {
                store_slot<THREADED>(&root_slot_, reinterpret_cast<uint64_t>(get_root()));
            }

            elem_count_.fetch_sub(1, std::memory_order_relaxed);
            to_free = std::move(result.old_nodes);

        }  // Lock released

        // Dealloc OUTSIDE lock - wait for dataptr readers
        for (auto* n : to_free) {
            if (n) {
                node_view_t view(n);
                if (view.has_eos()) view.eos_data()->begin_write();
                if (view.has_skip_eos()) view.skip_eos_data()->begin_write();
                builder_.deallocate_node(n);
            }
        }

        // Clear READ_BIT on committed path
        for (const auto& step : committed_path) {
            if (step.child_slot) {
                fetch_and_slot<THREADED>(step.child_slot, ~READ_BIT);
            }
        }

        validate_trie_impl<Key, T, THREADED, Allocator, fixed_len>(get_root());

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
