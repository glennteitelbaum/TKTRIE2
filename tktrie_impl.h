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
#include "tktrie_ebr.h"

namespace gteitelbaum {

template <typename Key> struct tktrie_traits;
template <typename Key, typename T, bool THREADED, typename Allocator> class tktrie_iterator;

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
void static_node_deleter(void* ptr) {
    if (!ptr) return;
    using slot_type = slot_type_t<THREADED>;
    using node_view_t = node_view<T, THREADED, Allocator, FIXED_LEN>;
    using dataptr_t = dataptr<T, THREADED, Allocator>;

    slot_type* node = static_cast<slot_type*>(ptr);
    node_view_t view(node);

    if (!view.has_leaf()) {
        view.eos_data()->~dataptr_t();
        if (view.has_skip()) view.skip_eos_data()->~dataptr_t();
    } else if (view.leaf_has_eos()) {
        if (view.has_skip()) view.skip_eos_data()->~dataptr_t();
        else view.eos_data()->~dataptr_t();
    }

    using alloc_traits = std::allocator_traits<Allocator>;
    using slot_alloc_t = typename alloc_traits::template rebind_alloc<slot_type>;
    using slot_alloc_traits = std::allocator_traits<slot_alloc_t>;

    slot_alloc_t alloc;
    slot_alloc_traits::deallocate(alloc, node, view.size());
}

template <typename Key, typename T, bool THREADED = false, typename Allocator = std::allocator<uint64_t>>
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
    using mutex_type = std::conditional_t<THREADED, std::mutex, empty_mutex>;

    static constexpr auto node_deleter = &static_node_deleter<T, THREADED, Allocator, fixed_len>;

    slot_type root_slot_;
    std::conditional_t<THREADED, std::atomic<size_type>, size_type> elem_count_{0};
    mutable mutex_type write_mutex_;
    Allocator alloc_;
    node_builder_t builder_;

    friend class tktrie_iterator<Key, T, THREADED, Allocator>;

    void retire_node(slot_type* node) const {
        if constexpr (THREADED) { if (node) ebr_global::instance().retire(node, node_deleter); }
    }

    slot_type* get_root() const noexcept {
        return reinterpret_cast<slot_type*>(load_slot<THREADED>(const_cast<slot_type*>(&root_slot_)));
    }

    void set_root(slot_type* r) noexcept { store_slot<THREADED>(&root_slot_, reinterpret_cast<uint64_t>(r)); }

public:
    tktrie() : root_slot_{}, builder_(alloc_) { store_slot<THREADED>(&root_slot_, 0); }
    explicit tktrie(const Allocator& alloc) : root_slot_{}, alloc_(alloc), builder_(alloc) { store_slot<THREADED>(&root_slot_, 0); }
    ~tktrie() { clear(); }

    tktrie(const tktrie& other) : root_slot_{}, alloc_(other.alloc_), builder_(other.alloc_) {
        if constexpr (THREADED) { std::lock_guard<mutex_type> lock(other.write_mutex_); }
        slot_type* other_root = const_cast<tktrie&>(other).get_root();
        if (other_root) set_root(builder_.deep_copy(other_root));
        else store_slot<THREADED>(&root_slot_, 0);
        if constexpr (THREADED) elem_count_.store(other.elem_count_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        else elem_count_ = other.elem_count_;
    }

    tktrie& operator=(const tktrie& other) {
        if (this == &other) return *this;
        tktrie tmp(other);
        swap(tmp);
        return *this;
    }

    tktrie(tktrie&& other) noexcept : root_slot_{}, alloc_(std::move(other.alloc_)), builder_(alloc_) {
        if constexpr (THREADED) { std::lock_guard<mutex_type> lock(other.write_mutex_); }
        store_slot<THREADED>(&root_slot_, load_slot<THREADED>(&other.root_slot_));
        store_slot<THREADED>(&other.root_slot_, 0);
        if constexpr (THREADED) elem_count_.store(other.elem_count_.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
        else { elem_count_ = other.elem_count_; other.elem_count_ = 0; }
    }

    tktrie& operator=(tktrie&& other) noexcept {
        if (this != &other) {
            clear();
            if constexpr (THREADED) { std::lock_guard<mutex_type> lock(other.write_mutex_); }
            store_slot<THREADED>(&root_slot_, load_slot<THREADED>(&other.root_slot_));
            store_slot<THREADED>(&other.root_slot_, 0);
            alloc_ = std::move(other.alloc_);
            builder_ = node_builder_t(alloc_);
            if constexpr (THREADED) elem_count_.store(other.elem_count_.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
            else { elem_count_ = other.elem_count_; other.elem_count_ = 0; }
        }
        return *this;
    }

    void swap(tktrie& other) noexcept {
        if constexpr (THREADED) {
            mutex_type* first = &write_mutex_ < &other.write_mutex_ ? &write_mutex_ : &other.write_mutex_;
            mutex_type* second = &write_mutex_ < &other.write_mutex_ ? &other.write_mutex_ : &write_mutex_;
            std::lock_guard<mutex_type> l1(*first), l2(*second);
        }
        uint64_t tmp = load_slot<THREADED>(&root_slot_);
        store_slot<THREADED>(&root_slot_, load_slot<THREADED>(&other.root_slot_));
        store_slot<THREADED>(&other.root_slot_, tmp);
        std::swap(alloc_, other.alloc_);
        if constexpr (THREADED) {
            size_type tc = elem_count_.load(std::memory_order_relaxed);
            elem_count_.store(other.elem_count_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            other.elem_count_.store(tc, std::memory_order_relaxed);
        } else { std::swap(elem_count_, other.elem_count_); }
    }

    bool empty() const noexcept { return size() == 0; }
    size_type size() const noexcept {
        if constexpr (THREADED) return elem_count_.load(std::memory_order_relaxed);
        else return elem_count_;
    }

    bool contains(const Key& key) const {
        std::string kb = std::string(traits::to_bytes(key));
        if constexpr (THREADED) { auto g = get_ebr_slot().get_guard(); return nav_t::contains(get_root(), kb); }
        else return nav_t::contains(get_root(), kb);
    }

    iterator find(const Key& key) const {
        std::string kb = std::string(traits::to_bytes(key));
        T value;
        if constexpr (THREADED) {
            auto g = get_ebr_slot().get_guard();
            if (nav_t::read(get_root(), kb, value)) return iterator(this, kb, value);
            return end();
        } else {
            if (nav_t::read(get_root(), kb, value)) return iterator(this, kb, value);
            return end();
        }
    }

    iterator end() const noexcept { return iterator::end_iterator(); }

    std::pair<iterator, bool> insert(const std::pair<const Key, T>& value) { return insert_impl(value.first, value.second); }
    std::pair<iterator, bool> insert(std::pair<const Key, T>&& value) { return insert_impl(value.first, std::move(value.second)); }
    template <typename... Args> std::pair<iterator, bool> emplace(const Key& key, Args&&... args) { return insert_impl(key, T(std::forward<Args>(args)...)); }
    bool erase(const Key& key) { return erase_impl(key); }
    void clear() { if constexpr (THREADED) clear_threaded(); else { delete_tree_simple(get_root()); set_root(nullptr); elem_count_ = 0; } }

    iterator begin() const {
        if constexpr (THREADED) { auto g = get_ebr_slot().get_guard(); return begin_impl(); }
        else return begin_impl();
    }

    iterator next_after(const std::string&) const { return end(); }

private:
    iterator begin_impl() const {
        slot_type* root = get_root();
        if (!root) return end();
        std::string key;
        bool is_embedded = false;
        slot_type* ds = nav_t::find_first_leaf(root, key, is_embedded);
        if (!ds) return end();

        T value;
        if (is_embedded) {
            if constexpr (can_embed_leaf_v<T>) {
                uint64_t raw = load_slot<THREADED>(ds);
                std::memcpy(&value, &raw, sizeof(T));
                return iterator(this, key, value);
            }
            return end();
        }
        dataptr_t* dp = reinterpret_cast<dataptr_t*>(ds);
        if (dp->try_read(value)) return iterator(this, key, value);
        return end();
    }

    void delete_tree_simple(slot_type* node, size_t depth = 0) {
        if (!node) return;
        node_view_t view(node);
        size_t skip_len = view.has_skip() ? view.skip_length() : 0;

        if (!view.has_leaf() || !view.leaf_has_children()) {
            int nc = view.child_count();
            for (int i = 0; i < nc; ++i) {
                uint64_t cp = view.get_child_ptr(i);
                if (cp) delete_tree_simple(reinterpret_cast<slot_type*>(cp), depth + skip_len + 1);
            }
        }
        builder_.deallocate_node(node);
    }

    template <typename U>
    std::pair<iterator, bool> insert_impl(const Key& key, U&& value) {
        std::string kb = std::string(traits::to_bytes(key));
        if constexpr (THREADED) return insert_threaded(key, kb, std::forward<U>(value));
        else return insert_single(key, kb, std::forward<U>(value));
    }

    template <typename U>
    std::pair<iterator, bool> insert_single(const Key& key, const std::string& kb, U&& value) {
        auto result = insert_t::build_insert_path(builder_, &root_slot_, get_root(), kb, std::forward<U>(value));
        if (result.already_exists) {
            for (auto* n : result.new_nodes) builder_.deallocate_node(n);
            return {find(key), false};
        }
        store_slot<THREADED>(result.target_slot, reinterpret_cast<uint64_t>(result.new_subtree));
        for (auto* n : result.old_nodes) builder_.deallocate_node(n);
        ++elem_count_;
        T val;
        if constexpr (std::is_rvalue_reference_v<U&&>) val = std::forward<U>(value);
        else val = value;
        return {iterator(this, kb, val), true};
    }

    template <typename U>
    std::pair<iterator, bool> insert_threaded(const Key& key, const std::string& kb, U&& value) {
        while (true) {
            auto& slot = get_ebr_slot();
            auto guard = slot.get_guard();
            
            auto result = insert_t::build_insert_path(builder_, &root_slot_, get_root(), kb, std::forward<U>(value));
            
            if (result.already_exists) {
                for (auto* n : result.new_nodes) builder_.deallocate_node(n);
                return {find(key), false};
            }
            
            // In-place update already done atomically
            if (result.in_place) {
                elem_count_.fetch_add(1, std::memory_order_relaxed);
                T val;
                if constexpr (std::is_rvalue_reference_v<U&&>) val = std::forward<U>(value);
                else val = value;
                return {iterator(this, kb, val), true};
            }
            
            // Use striped lock based on target node address
            void* lock_key = (result.expected_ptr != 0) 
                ? reinterpret_cast<void*>(result.expected_ptr) 
                : static_cast<void*>(&root_slot_);
            std::lock_guard<std::mutex> lock(get_striped_locks().get(lock_key));
            
            // Verify path hasn't changed
            if (result.path_has_conflict()) {
                for (auto* n : result.new_nodes) builder_.deallocate_node(n);
                cpu_pause();
                continue;
            }
            
            // Commit
            store_slot<THREADED>(result.target_slot, reinterpret_cast<uint64_t>(result.new_subtree));
            elem_count_.fetch_add(1, std::memory_order_relaxed);
            
            // Retire old nodes
            for (auto* n : result.old_nodes) retire_node(n);
            ebr_global::instance().try_reclaim();
            
            T val;
            if constexpr (std::is_rvalue_reference_v<U&&>) val = std::forward<U>(value);
            else val = value;
            return {iterator(this, kb, val), true};
        }
    }

    bool erase_impl(const Key& key) {
        std::string kb = std::string(traits::to_bytes(key));
        if constexpr (THREADED) return erase_threaded(kb);
        else return erase_single(kb);
    }

    bool erase_single(const std::string& kb) {
        auto result = remove_t::build_remove_path(builder_, &root_slot_, get_root(), kb);
        if (!result.found) return false;
        if (result.subtree_deleted) store_slot<THREADED>(result.target_slot, 0);
        else store_slot<THREADED>(result.target_slot, reinterpret_cast<uint64_t>(result.new_subtree));
        for (auto* n : result.old_nodes) builder_.deallocate_node(n);
        --elem_count_;
        return true;
    }

    bool erase_threaded(const std::string& kb) {
        while (true) {
            auto& slot = get_ebr_slot();
            auto guard = slot.get_guard();
            
            auto result = remove_t::build_remove_path(builder_, &root_slot_, get_root(), kb);
            
            if (!result.found) {
                for (auto* n : result.new_nodes) builder_.deallocate_node(n);
                return false;
            }
            
            // In-place delete already done
            if (result.in_place) {
                elem_count_.fetch_sub(1, std::memory_order_relaxed);
                return true;
            }
            
            // Use striped lock
            void* lock_key = (result.expected_ptr != 0) 
                ? reinterpret_cast<void*>(result.expected_ptr) 
                : static_cast<void*>(&root_slot_);
            std::lock_guard<std::mutex> lock(get_striped_locks().get(lock_key));
            
            // Verify path
            if (result.path_has_conflict()) {
                for (auto* n : result.new_nodes) builder_.deallocate_node(n);
                cpu_pause();
                continue;
            }
            
            // Commit
            uint64_t np = result.subtree_deleted ? 0 : reinterpret_cast<uint64_t>(result.new_subtree);
            store_slot<THREADED>(result.target_slot, np);
            elem_count_.fetch_sub(1, std::memory_order_relaxed);
            
            // Retire old nodes
            for (auto* n : result.old_nodes) retire_node(n);
            ebr_global::instance().try_reclaim();
            
            return true;
        }
    }

    void clear_threaded() {
        slot_type* old_root = nullptr;
        {
            std::lock_guard<mutex_type> lock(write_mutex_);
            old_root = get_root();
            set_root(nullptr);
            elem_count_.store(0, std::memory_order_relaxed);
        }
        if (old_root) {
            ebr_global::instance().advance_epoch();
            ebr_global::instance().advance_epoch();
            ebr_global::instance().try_reclaim();
            delete_tree_simple(old_root);
        }
    }
};

template <typename Key, typename T, bool THREADED, typename Allocator>
void swap(tktrie<Key, T, THREADED, Allocator>& a, tktrie<Key, T, THREADED, Allocator>& b) noexcept { a.swap(b); }

}  // namespace gteitelbaum
