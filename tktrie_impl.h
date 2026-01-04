#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

#include "tktrie_defines.h"
#include "tktrie_node.h"
#include "tktrie_help_nav.h"
#include "tktrie_help_insert.h"
#include "tktrie_help_remove.h"
#include "tktrie_ebr.h"

namespace gteitelbaum {

// Forward declarations
template <typename Key> struct tktrie_traits;
template <typename Key, typename T, bool THREADED, typename Allocator> class tktrie_iterator;

// Static deleter for EBR
template <typename T, bool THREADED, typename Allocator>
void static_node_deleter(void* ptr) {
    if (!ptr) return;
    using ptr_t = node_ptr<T, THREADED, Allocator>;
    using builder_t = node_builder<T, THREADED, Allocator>;

    ptr_t p;
    p.raw = ptr;
    builder_t builder;
    builder.free_subtree(p);
}

template <typename Key, typename T, bool THREADED = false, typename Allocator = std::allocator<uint64_t>>
class tktrie {
public:
    using traits = tktrie_traits<Key>;
    using size_type = std::size_t;
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<const Key, T>;
    using iterator = tktrie_iterator<Key, T, THREADED, Allocator>;
    using const_iterator = iterator;

private:
    using ptr_t = node_ptr<T, THREADED, Allocator>;
    using builder_t = node_builder<T, THREADED, Allocator>;
    using nav_t = nav_helpers<T, THREADED, Allocator>;
    using insert_t = insert_helpers<T, THREADED, Allocator>;
    using remove_t = remove_helpers<T, THREADED, Allocator>;
    using mutex_type = std::conditional_t<THREADED, std::mutex, empty_mutex>;
    using insert_result_t = insert_result<T, THREADED, Allocator>;
    using remove_result_t = remove_result<T, THREADED, Allocator>;
    using atomic_ptr_t = atomic_node_ptr<T, THREADED, Allocator>;

    static constexpr auto node_deleter = &static_node_deleter<T, THREADED, Allocator>;

    atomic_ptr_t root_;
    std::conditional_t<THREADED, std::atomic<size_type>, size_type> elem_count_{0};
    mutable mutex_type write_mutex_;
    Allocator alloc_;
    builder_t builder_;

    friend class tktrie_iterator<Key, T, THREADED, Allocator>;

    ptr_t get_root() const noexcept { return root_.load(); }
    void set_root(ptr_t r) noexcept { root_.store(r); }

    void retire_node(ptr_t p) const {
        if constexpr (THREADED) {
            if (p) ebr_global::instance().retire(p.raw, node_deleter);
        }
    }

public:
    tktrie() : builder_(alloc_) {}

    explicit tktrie(const Allocator& alloc) : alloc_(alloc), builder_(alloc) {}

    ~tktrie() { clear(); }

    tktrie(const tktrie& other) : alloc_(other.alloc_), builder_(other.alloc_) {
        if constexpr (THREADED) {
            std::lock_guard<mutex_type> lock(other.write_mutex_);
        }
        ptr_t other_root = const_cast<tktrie&>(other).get_root();
        if (other_root) {
            set_root(builder_.deep_copy(other_root));
        }
        if constexpr (THREADED) {
            elem_count_.store(other.elem_count_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        } else {
            elem_count_ = other.elem_count_;
        }
    }

    tktrie& operator=(const tktrie& other) {
        if (this == &other) return *this;
        tktrie tmp(other);
        swap(tmp);
        return *this;
    }

    tktrie(tktrie&& other) noexcept : alloc_(std::move(other.alloc_)), builder_(alloc_) {
        if constexpr (THREADED) {
            std::lock_guard<mutex_type> lock(other.write_mutex_);
            root_.store(other.root_.exchange(nullptr));
            elem_count_.store(other.elem_count_.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
        } else {
            root_ = other.root_;
            other.root_.store(nullptr);
            elem_count_ = other.elem_count_;
            other.elem_count_ = 0;
        }
    }

    tktrie& operator=(tktrie&& other) noexcept {
        if (this != &other) {
            clear();
            if constexpr (THREADED) {
                std::lock_guard<mutex_type> lock(other.write_mutex_);
                root_.store(other.root_.exchange(nullptr));
                elem_count_.store(other.elem_count_.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
            } else {
                root_ = other.root_;
                other.root_.store(nullptr);
                elem_count_ = other.elem_count_;
                other.elem_count_ = 0;
            }
            alloc_ = std::move(other.alloc_);
            builder_ = builder_t(alloc_);
        }
        return *this;
    }

    void swap(tktrie& other) noexcept {
        if constexpr (THREADED) {
            mutex_type* first = &write_mutex_ < &other.write_mutex_ ? &write_mutex_ : &other.write_mutex_;
            mutex_type* second = &write_mutex_ < &other.write_mutex_ ? &other.write_mutex_ : &write_mutex_;
            std::lock_guard<mutex_type> l1(*first);
            std::lock_guard<mutex_type> l2(*second);
            ptr_t tmp = root_.exchange(other.root_.load());
            other.root_.store(tmp);
            size_type tc = elem_count_.exchange(other.elem_count_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            other.elem_count_.store(tc, std::memory_order_relaxed);
        } else {
            ptr_t tmp = root_.load();
            root_.store(other.root_.load());
            other.root_.store(tmp);
            std::swap(elem_count_, other.elem_count_);
        }
        std::swap(alloc_, other.alloc_);
    }

    bool empty() const noexcept { return size() == 0; }

    size_type size() const noexcept {
        if constexpr (THREADED) return elem_count_.load(std::memory_order_relaxed);
        else return elem_count_;
    }

    bool contains(const Key& key) const {
        std::string kb = traits::to_bytes(key);
        if constexpr (THREADED) {
            auto g = get_ebr_slot().get_guard();
            return nav_t::contains(get_root(), kb);
        } else {
            return nav_t::contains(get_root(), kb);
        }
    }

    iterator find(const Key& key) const {
        std::string kb = traits::to_bytes(key);
        T value;
        if constexpr (THREADED) {
            auto g = get_ebr_slot().get_guard();
            if (nav_t::read(get_root(), kb, value)) {
                return iterator(this, kb, value);
            }
            return end();
        } else {
            if (nav_t::read(get_root(), kb, value)) {
                return iterator(this, kb, value);
            }
            return end();
        }
    }

    iterator end() const noexcept { return iterator::end_iterator(); }

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
            builder_.free_subtree(get_root());
            set_root(nullptr);
            elem_count_ = 0;
        }
    }

    iterator begin() const {
        if constexpr (THREADED) {
            auto g = get_ebr_slot().get_guard();
            return begin_impl();
        } else {
            return begin_impl();
        }
    }

    iterator next_after(const std::string& current_key) const {
        if constexpr (THREADED) {
            auto g = get_ebr_slot().get_guard();
            return next_after_impl(current_key);
        } else {
            return next_after_impl(current_key);
        }
    }

private:
    iterator begin_impl() const {
        ptr_t root = get_root();
        if (!root) return end();

        std::string key;
        bool is_skip_eos;
        ptr_t node = nav_t::find_first_leaf(root, key, is_skip_eos);
        if (!node) return end();

        T* val_ptr = is_skip_eos ? node.get_skip_eos() : node.get_eos();
        if (!val_ptr) return end();

        return iterator(this, key, *val_ptr);
    }

    iterator next_after_impl(const std::string& current_key) const {
        ptr_t root = get_root();
        if (!root) return end();

        std::string next_key;
        bool is_skip_eos;
        ptr_t node = nav_t::find_next_leaf(root, current_key, next_key, is_skip_eos);
        if (!node) return end();

        T* val_ptr = is_skip_eos ? node.get_skip_eos() : node.get_eos();
        if (!val_ptr) return end();

        return iterator(this, next_key, *val_ptr);
    }

    template <typename U>
    std::pair<iterator, bool> insert_impl(const Key& key, U&& value) {
        std::string kb = traits::to_bytes(key);
        if constexpr (THREADED) {
            return insert_threaded(key, kb, std::forward<U>(value));
        } else {
            return insert_single(key, kb, std::forward<U>(value));
        }
    }

    template <typename U>
    std::pair<iterator, bool> insert_single(const Key& key, const std::string& kb, U&& value) {
        ptr_t root = get_root();
        auto result = insert_t::build_insert_path(builder_, &root_, root, kb, std::forward<U>(value));

        if (result.already_exists) {
            for (auto& n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            return {find(key), false};
        }

        if (!result.in_place) {
            set_root(result.new_subtree);
            for (auto& n : result.old_nodes) {
                builder_.deallocate_node(n);
            }
        }

        ++elem_count_;
        T val;
        if constexpr (std::is_rvalue_reference_v<U&&>) {
            val = std::forward<U>(value);
        } else {
            val = value;
        }
        return {iterator(this, kb, val), true};
    }

    template <typename U>
    std::pair<iterator, bool> insert_threaded(const Key& key, const std::string& kb, U&& value) {
        std::lock_guard<mutex_type> lock(write_mutex_);
        auto& slot = get_ebr_slot();
        auto guard = slot.get_guard();

        ptr_t root = get_root();
        auto result = insert_t::build_insert_path(builder_, &root_, root, kb, std::forward<U>(value));

        if (result.already_exists) {
            for (auto& n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            return {find(key), false};
        }

        if (result.in_place) {
            elem_count_.fetch_add(1, std::memory_order_relaxed);
            T val;
            if constexpr (std::is_rvalue_reference_v<U&&>) {
                val = std::forward<U>(value);
            } else {
                val = value;
            }
            return {iterator(this, kb, val), true};
        }

        set_root(result.new_subtree);
        elem_count_.fetch_add(1, std::memory_order_relaxed);

        for (auto& n : result.old_nodes) {
            retire_node(n);
        }
        ebr_global::instance().try_reclaim();

        T val;
        if constexpr (std::is_rvalue_reference_v<U&&>) {
            val = std::forward<U>(value);
        } else {
            val = value;
        }
        return {iterator(this, kb, val), true};
    }

    bool erase_impl(const Key& key) {
        std::string kb = traits::to_bytes(key);
        if constexpr (THREADED) {
            return erase_threaded(kb);
        } else {
            return erase_single(kb);
        }
    }

    bool erase_single(const std::string& kb) {
        ptr_t root = get_root();
        auto result = remove_t::build_remove_path(builder_, &root_, root, kb);

        if (!result.found) return false;

        if (result.subtree_deleted) {
            set_root(nullptr);
        } else if (!result.in_place) {
            set_root(result.new_subtree);
        }

        for (auto& n : result.old_nodes) {
            builder_.deallocate_node(n);
        }
        for (auto* v : result.old_values) {
            builder_.free_value(v);
        }

        --elem_count_;
        return true;
    }

    bool erase_threaded(const std::string& kb) {
        std::lock_guard<mutex_type> lock(write_mutex_);
        auto& slot = get_ebr_slot();
        auto guard = slot.get_guard();

        ptr_t root = get_root();
        auto result = remove_t::build_remove_path(builder_, &root_, root, kb);

        if (!result.found) {
            for (auto& n : result.new_nodes) {
                builder_.deallocate_node(n);
            }
            return false;
        }

        if (result.in_place) {
            elem_count_.fetch_sub(1, std::memory_order_relaxed);
            for (auto* v : result.old_values) {
                ebr_global::instance().retire(v, [](void* p) {
                    delete static_cast<T*>(p);
                });
            }
            return true;
        }

        if (result.subtree_deleted) {
            set_root(nullptr);
        } else {
            set_root(result.new_subtree);
        }
        elem_count_.fetch_sub(1, std::memory_order_relaxed);

        for (auto& n : result.old_nodes) {
            retire_node(n);
        }
        for (auto* v : result.old_values) {
            ebr_global::instance().retire(v, [](void* p) {
                delete static_cast<T*>(p);
            });
        }
        ebr_global::instance().try_reclaim();

        return true;
    }

    void clear_threaded() {
        ptr_t old_root;
        {
            std::lock_guard<mutex_type> lock(write_mutex_);
            old_root = root_.exchange(nullptr);
            elem_count_.store(0, std::memory_order_relaxed);
        }
        if (old_root) {
            ebr_global::instance().advance_epoch();
            ebr_global::instance().advance_epoch();
            ebr_global::instance().try_reclaim();
            builder_.free_subtree(old_root);
        }
    }
};

template <typename Key, typename T, bool THREADED, typename Allocator>
void swap(tktrie<Key, T, THREADED, Allocator>& a, tktrie<Key, T, THREADED, Allocator>& b) noexcept {
    a.swap(b);
}

}  // namespace gteitelbaum
