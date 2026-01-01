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
#include "tktrie_traits.h"
#include "tktrie_dataptr.h"
#include "tktrie_small_list.h"
#include "tktrie_popcount.h"
#include "tktrie_node.h"
#include "tktrie_help_common.h"
#include "tktrie_help_nav.h"
#include "tktrie_help_insert.h"
#include "tktrie_help_remove.h"
#include "tktrie_iterator.h"
#include "tktrie_debug.h"

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

    // Path container type based on fixed_len
    using path_entry_t = path_entry<THREADED>;
    using path_container = std::conditional_t<
        (fixed_len > 0),
        std::array<path_entry_t, fixed_len + 1>,
        std::vector<path_entry_t>
    >;

    // Mutex type based on THREADED
    using mutex_type = std::conditional_t<THREADED, std::mutex, empty_mutex>;

    slot_type* root_;
    std::conditional_t<THREADED, std::atomic<size_type>, size_type> elem_count_{0};
    mutable mutex_type write_mutex_;
    Allocator alloc_;
    node_builder_t builder_;

    friend class tktrie_iterator<Key, T, THREADED, Allocator>;

public:
    // =========================================================================
    // Constructors and Destructor
    // =========================================================================

    tktrie() 
        : root_(nullptr)
        , builder_(alloc_) {}

    explicit tktrie(const Allocator& alloc)
        : root_(nullptr)
        , alloc_(alloc)
        , builder_(alloc) {}

    ~tktrie() {
        clear();
    }

    // Copy constructor (deep copy)
    tktrie(const tktrie& other)
        : alloc_(other.alloc_)
        , builder_(other.alloc_) {
        if (other.root_) {
            root_ = builder_.deep_copy(other.root_);
        } else {
            root_ = nullptr;
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
        if (this != &other) {
            tktrie tmp(other);
            swap(tmp);
        }
        return *this;
    }

    // Move constructor
    tktrie(tktrie&& other) noexcept
        : root_(other.root_)
        , alloc_(std::move(other.alloc_))
        , builder_(alloc_) {
        if constexpr (THREADED) {
            elem_count_.store(other.elem_count_.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);
            other.elem_count_.store(0, std::memory_order_relaxed);
        } else {
            elem_count_ = other.elem_count_;
            other.elem_count_ = 0;
        }
        other.root_ = nullptr;
    }

    // Move assignment
    tktrie& operator=(tktrie&& other) noexcept {
        if (this != &other) {
            clear();
            root_ = other.root_;
            alloc_ = std::move(other.alloc_);
            builder_ = node_builder_t(alloc_);
            if constexpr (THREADED) {
                elem_count_.store(other.elem_count_.load(std::memory_order_relaxed),
                                 std::memory_order_relaxed);
                other.elem_count_.store(0, std::memory_order_relaxed);
            } else {
                elem_count_ = other.elem_count_;
                other.elem_count_ = 0;
            }
            other.root_ = nullptr;
        }
        return *this;
    }

    // Swap
    void swap(tktrie& other) noexcept {
        std::swap(root_, other.root_);
        std::swap(alloc_, other.alloc_);
        if constexpr (THREADED) {
            size_type tmp = elem_count_.load(std::memory_order_relaxed);
            elem_count_.store(other.elem_count_.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);
            other.elem_count_.store(tmp, std::memory_order_relaxed);
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
                bool result = nav_t::contains(root_, key_bytes, hit_write);
                if (!hit_write) return result;
                cpu_pause();
            }
        } else {
            return nav_t::contains(root_, key_bytes, hit_write);
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
                bool found = nav_t::read(root_, key_bytes, value, hit_write);
                if (!hit_write) {
                    if (found) {
                        return iterator(this, key_bytes, value);
                    }
                    return end();
                }
                cpu_pause();
            }
        } else {
            bool found = nav_t::read(root_, key_bytes, value, hit_write);
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
        }
        
        delete_tree(root_);
        root_ = nullptr;
        
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
        if (!root_) return end();
        
        std::string key;
        bool hit_write = false;
        
        if constexpr (THREADED) {
            while (true) {
                key.clear();
                slot_type* data_slot = nav_t::find_first_leaf(root_, key, hit_write);
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
            slot_type* data_slot = nav_t::find_first_leaf(root_, key, hit_write);
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
        if (root_) {
            debug_t::pretty_print_node(root_, os, 0, "", 0);
        } else {
            os << "  (empty)\n";
        }
    }

    void validate() const {
        if constexpr (k_validate) {
            std::string err = debug_t::validate_node(root_, 0);
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

    void delete_tree(slot_type* node) {
        if (!node) return;

        node_view_t view(node);
        
        // Recursively delete children
        int num_children = view.child_count();
        for (int i = 0; i < num_children; ++i) {
            uint64_t child_ptr = view.get_child_ptr(i);
            
            if constexpr (THREADED) {
                child_ptr &= PTR_MASK;
            }

            // For fixed_len at leaf depth, children are dataptr not nodes
            if constexpr (fixed_len > 0) {
                // We'd need depth tracking here - for simplicity, assume
                // variable-length behavior and recurse
            }

            slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
            if (child) {
                delete_tree(child);
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
        auto result = insert_t::build_insert_path(builder_, root_, key_bytes, 
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
            root_ = result.new_root;
        }

        // Delete old nodes
        for (auto* n : result.old_nodes) {
            // Don't delete if it's the same as new_root (shouldn't happen)
            if (n != result.new_root) {
                builder_.deallocate_node(n);
            }
        }

        ++elem_count_;

        validate_trie_impl<Key, T, THREADED, Allocator, fixed_len>(root_);

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
        // Phase 1: Build new path outside lock (optimistic)
        auto result = insert_t::build_insert_path(builder_, root_, key_bytes,
                                                   std::forward<U>(value));

        if (result.hit_write) {
            // Clean up and retry
            for (auto* n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            cpu_pause();
            return insert_threaded(key, key_bytes, std::forward<U>(value));
        }

        if (result.already_exists) {
            for (auto* n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            return {find(key), false};
        }

        // Phase 2: Collect path versions
        path_container path;
        if constexpr (fixed_len == 0) {
            path.reserve(16);
        }
        bool hit_write = false;
        nav_t::collect_path(root_, key_bytes, path, hit_write);

        if (hit_write) {
            for (auto* n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            cpu_pause();
            return insert_threaded(key, key_bytes, std::forward<U>(value));
        }

        // Phase 3: Lock
        std::lock_guard<mutex_type> lock(write_mutex_);

        // Phase 4: Verify path
        size_t path_size = (fixed_len > 0) ? fixed_len + 1 : path.size();
        if (!nav_t::verify_path(path, path_size)) {
            // Path changed - redo under lock
            for (auto* n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            result = insert_t::build_insert_path(builder_, root_, key_bytes,
                                                  std::forward<U>(value));
        }

        if (result.already_exists) {
            for (auto* n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            return {find(key), false};
        }

        // Phase 5: Update root and versions
        if (result.new_root) {
            root_ = result.new_root;
        }

        // Increment versions on modified path
        for (auto* n : result.new_nodes) {
            if (n) {
                node_view_t view(n);
                view.increment_version();
            }
        }

        elem_count_.fetch_add(1, std::memory_order_relaxed);

        // Phase 6: Cleanup (after lock release would be ideal, but ok here)
        for (auto* n : result.old_nodes) {
            if (n != result.new_root) {
                builder_.deallocate_node(n);
            }
        }

        validate_trie_impl<Key, T, THREADED, Allocator, fixed_len>(root_);

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
        auto result = remove_t::build_remove_path(builder_, root_, key_bytes);

        if (!result.found) {
            return false;
        }

        // Update root
        if (result.root_deleted) {
            root_ = nullptr;
        } else if (result.new_root) {
            root_ = result.new_root;
        }

        // Delete old nodes
        for (auto* n : result.old_nodes) {
            if (n != result.new_root) {
                builder_.deallocate_node(n);
            }
        }

        --elem_count_;

        validate_trie_impl<Key, T, THREADED, Allocator, fixed_len>(root_);

        return true;
    }

    bool erase_threaded(const std::string& key_bytes) {
        // Phase 1: Build removal path outside lock
        auto result = remove_t::build_remove_path(builder_, root_, key_bytes);

        if (result.hit_write) {
            for (auto* n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            cpu_pause();
            return erase_threaded(key_bytes);
        }

        if (!result.found) {
            return false;
        }

        // Phase 2: Collect path versions
        path_container path;
        if constexpr (fixed_len == 0) {
            path.reserve(16);
        }
        bool hit_write = false;
        nav_t::collect_path(root_, key_bytes, path, hit_write);

        if (hit_write) {
            for (auto* n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            cpu_pause();
            return erase_threaded(key_bytes);
        }

        // Phase 3: Lock
        std::lock_guard<mutex_type> lock(write_mutex_);

        // Phase 4: Verify path
        size_t path_size = (fixed_len > 0) ? fixed_len + 1 : path.size();
        if (!nav_t::verify_path(path, path_size)) {
            for (auto* n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            result = remove_t::build_remove_path(builder_, root_, key_bytes);
        }

        if (!result.found) {
            for (auto* n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            return false;
        }

        // Phase 5: Update root
        if (result.root_deleted) {
            root_ = nullptr;
        } else if (result.new_root) {
            root_ = result.new_root;
        }

        elem_count_.fetch_sub(1, std::memory_order_relaxed);

        // Phase 6: Cleanup
        for (auto* n : result.old_nodes) {
            if (n != result.new_root) {
                builder_.deallocate_node(n);
            }
        }

        validate_trie_impl<Key, T, THREADED, Allocator, fixed_len>(root_);

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
