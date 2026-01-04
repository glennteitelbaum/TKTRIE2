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

// Key traits
template <typename Key> struct tktrie_traits;

template <>
struct tktrie_traits<std::string> {
    static std::string to_bytes(const std::string& k) { return k; }
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

// Forward declaration
template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie_iterator;

// Main trie class
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
    
private:
    atomic_ptr root_;
    std::conditional_t<THREADED, std::atomic<size_t>, size_t> size_{0};
    mutable mutex_t mutex_;
    builder_t builder_;
    
    static void node_deleter(void* ptr) {
        if (!ptr) return;
        ptr_t n = static_cast<ptr_t>(ptr);
        switch (n->type()) {
            case TYPE_EOS: delete n->as_eos(); break;
            case TYPE_SKIP: delete n->as_skip(); break;
            case TYPE_LIST: delete n->as_list(); break;
            case TYPE_FULL: delete n->as_full(); break;
        }
    }
    
    void retire_node(ptr_t n) {
        if (!n) return;
        if constexpr (THREADED) {
            ebr_global::instance().retire(n, node_deleter);
        } else {
            node_deleter(n);
        }
    }
    
    static size_t match_skip(std::string_view skip, std::string_view key) noexcept {
        size_t i = 0;
        while (i < skip.size() && i < key.size() && skip[i] == key[i]) ++i;
        return i;
    }
    
    static std::string_view get_skip(ptr_t n) noexcept {
        switch (n->type()) {
            case TYPE_SKIP: return n->as_skip()->skip;
            case TYPE_LIST: return n->as_list()->skip;
            case TYPE_FULL: return n->as_full()->skip;
            default: return {};
        }
    }
    
    static T* get_eos_ptr(ptr_t n) noexcept {
        if (n->is_leaf()) return nullptr;
        switch (n->type()) {
            case TYPE_EOS: return n->as_eos()->eos_ptr;
            case TYPE_SKIP: return n->as_skip()->eos_ptr;
            case TYPE_LIST: return n->as_list()->eos_ptr;
            case TYPE_FULL: return n->as_full()->eos_ptr;
            default: return nullptr;
        }
    }
    
    static void set_eos_ptr(ptr_t n, T* p) noexcept {
        switch (n->type()) {
            case TYPE_EOS: n->as_eos()->eos_ptr = p; break;
            case TYPE_SKIP: n->as_skip()->eos_ptr = p; break;
            case TYPE_LIST: n->as_list()->eos_ptr = p; break;
            case TYPE_FULL: n->as_full()->eos_ptr = p; break;
        }
    }
    
    // =========================================================================
    // READ
    // =========================================================================
    bool read_impl(ptr_t n, std::string_view key, T& out) const noexcept {
        while (n) {
            if (n->is_leaf()) return read_from_leaf(n, key, out);
            
            std::string_view skip = get_skip(n);
            size_t m = match_skip(skip, key);
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
    
    bool read_from_leaf(ptr_t leaf, std::string_view key, T& out) const noexcept {
        std::string_view skip = get_skip(leaf);
        size_t m = match_skip(skip, key);
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
    
    bool contains_impl(ptr_t n, std::string_view key) const noexcept {
        T dummy;
        return read_impl(n, key, dummy);
    }
    
    ptr_t find_child(ptr_t n, unsigned char c) const noexcept {
        if (n->is_list()) {
            int idx = n->as_list()->chars.find(c);
            return idx >= 0 ? n->as_list()->children[idx].load() : nullptr;
        }
        if (n->is_full() && n->as_full()->valid.test(c)) {
            return n->as_full()->children[c].load();
        }
        return nullptr;
    }
    
    // =========================================================================
    // INSERT (included from tktrie_insert.inc and tktrie_insert_spec.inc)
    // =========================================================================
#include "tktrie_insert.inc"
#include "tktrie_insert_spec.inc"
    
    // =========================================================================
    // ERASE (included from tktrie_erase.inc)
    // =========================================================================
#include "tktrie_erase.inc"
    
public:
    tktrie() = default;
    ~tktrie() { clear(); }
    
    tktrie(const tktrie& other) {
        ptr_t other_root = other.root_.load();
        if (other_root) root_.store(builder_.deep_copy(other_root));
        if constexpr (THREADED) size_.store(other.size_.load());
        else size_ = other.size_;
    }
    
    tktrie& operator=(const tktrie& other) {
        if (this != &other) {
            clear();
            ptr_t other_root = other.root_.load();
            if (other_root) root_.store(builder_.deep_copy(other_root));
            if constexpr (THREADED) size_.store(other.size_.load());
            else size_ = other.size_;
        }
        return *this;
    }
    
    tktrie(tktrie&& other) noexcept {
        root_.store(other.root_.load());
        other.root_.store(nullptr);
        if constexpr (THREADED) {
            size_.store(other.size_.exchange(0));
        } else {
            size_ = other.size_;
            other.size_ = 0;
        }
    }
    
    tktrie& operator=(tktrie&& other) noexcept {
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
    
    void clear() {
        ptr_t r = root_.load();
        root_.store(nullptr);
        if (r) builder_.dealloc_node(r);
        if constexpr (THREADED) size_.store(0);
        else size_ = 0;
    }
    
    size_t size() const noexcept {
        if constexpr (THREADED) return size_.load();
        else return size_;
    }
    
    bool empty() const noexcept { return size() == 0; }
    
    bool contains(const Key& key) const {
        std::string kb = traits::to_bytes(key);
        if constexpr (THREADED) {
            auto& slot = get_ebr_slot();
            auto guard = slot.get_guard();
            return contains_impl(root_.load(), kb);
        } else {
            return contains_impl(root_.load(), kb);
        }
    }
    
    std::pair<iterator, bool> insert(const std::pair<const Key, T>& kv) {
        return insert_locked(kv.first, traits::to_bytes(kv.first), kv.second);
    }
    
    bool erase(const Key& key) {
        return erase_locked(traits::to_bytes(key));
    }
    
    iterator find(const Key& key) const {
        std::string kb = traits::to_bytes(key);
        T value;
        if constexpr (THREADED) {
            auto& slot = get_ebr_slot();
            auto guard = slot.get_guard();
            if (read_impl(root_.load(), kb, value)) {
                return iterator(this, kb, value);
            }
        } else {
            if (read_impl(root_.load(), kb, value)) {
                return iterator(this, kb, value);
            }
        }
        return end();
    }
    
    iterator end() const noexcept { return iterator(); }
};

// Iterator
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
        if (!valid_ && !o.valid_) return true;
        return valid_ == o.valid_ && key_bytes_ == o.key_bytes_;
    }
    bool operator!=(const tktrie_iterator& o) const { return !(*this == o); }
};

// Type aliases
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
