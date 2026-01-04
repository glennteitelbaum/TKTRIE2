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
        // Just delete this node - for single node retirement
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
    
    // Get skip string from any node type
    static std::string_view get_skip(ptr_t n) noexcept {
        switch (n->type()) {
            case TYPE_SKIP: return n->as_skip()->skip;
            case TYPE_LIST: return n->as_list()->skip;
            case TYPE_FULL: return n->as_full()->skip;
            default: return {};
        }
    }
    
    // Get eos_ptr from interior node
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
    
    // Set eos_ptr on interior node
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
    // INSERT
    // =========================================================================
    struct insert_result {
        ptr_t new_node = nullptr;
        std::vector<ptr_t> old_nodes;
        bool inserted = false;
        bool in_place = false;
    };
    
    insert_result insert_impl(atomic_ptr* slot, ptr_t n, std::string_view key, const T& value) {
        insert_result res;
        
        if (!n) {
            res.new_node = create_leaf_for_key(key, value);
            res.inserted = true;
            return res;
        }
        
        if (n->is_leaf()) return insert_into_leaf(slot, n, key, value);
        return insert_into_interior(slot, n, key, value);
    }
    
    insert_result insert_into_leaf(atomic_ptr*, ptr_t leaf, std::string_view key, const T& value) {
        insert_result res;
        std::string_view leaf_skip = get_skip(leaf);
        
        if (leaf->is_eos()) {
            if (key.empty()) return res;  // Already exists
            return demote_leaf_eos(leaf, key, value);
        }
        
        if (leaf->is_skip()) {
            size_t m = match_skip(leaf_skip, key);
            if (m == leaf_skip.size() && m == key.size()) return res;  // Exists
            if (m < leaf_skip.size() && m < key.size()) return split_leaf_skip(leaf, key, value, m);
            if (m == key.size()) return prefix_leaf_skip(leaf, key, value, m);
            return extend_leaf_skip(leaf, key, value, m);
        }
        
        // LIST or FULL
        size_t m = match_skip(leaf_skip, key);
        if (m < leaf_skip.size() && m < key.size()) return split_leaf_list(leaf, key, value, m);
        if (m < leaf_skip.size()) return prefix_leaf_list(leaf, key, value, m);
        key.remove_prefix(m);
        
        if (key.empty()) return add_eos_to_leaf_list(leaf, value);
        if (key.size() == 1) {
            unsigned char c = static_cast<unsigned char>(key[0]);
            return add_char_to_leaf(leaf, c, value);
        }
        return demote_leaf_list(leaf, key, value);
    }
    
    insert_result insert_into_interior(atomic_ptr*, ptr_t n, std::string_view key, const T& value) {
        insert_result res;
        std::string_view skip = get_skip(n);
        
        size_t m = match_skip(skip, key);
        if (m < skip.size() && m < key.size()) return split_interior(n, key, value, m);
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
    
    atomic_ptr* get_child_slot(ptr_t n, unsigned char c) noexcept {
        if (n->is_list()) {
            int idx = n->as_list()->chars.find(c);
            return idx >= 0 ? &n->as_list()->children[idx] : nullptr;
        }
        if (n->is_full() && n->as_full()->valid.test(c)) {
            return &n->as_full()->children[c];
        }
        return nullptr;
    }
    
    ptr_t create_leaf_for_key(std::string_view key, const T& value) {
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
    
    // Demote leaf_eos: becomes interior with child
    insert_result demote_leaf_eos(ptr_t leaf, std::string_view key, const T& value) {
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
    
    // Split leaf_skip at divergence
    insert_result split_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m) {
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
    
    // Key is prefix of leaf_skip
    insert_result prefix_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m) {
        insert_result res;
        std::string_view old_skip = leaf->as_skip()->skip;
        
        ptr_t interior = builder_.make_interior_list(key);
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
    
    // leaf_skip is prefix of key - extend
    insert_result extend_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m) {
        insert_result res;
        std::string_view old_skip = leaf->as_skip()->skip;
        
        ptr_t interior = builder_.make_interior_list(old_skip);
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
    
    // Split leaf_list at divergence in skip
    insert_result split_leaf_list(ptr_t leaf, std::string_view key, const T& value, size_t m) {
        insert_result res;
        std::string_view old_skip = get_skip(leaf);
        
        std::string common(old_skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);
        
        ptr_t interior = builder_.make_interior_list(common);
        
        // Clone old leaf with shorter skip
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
    
    // Key ends in skip
    insert_result prefix_leaf_list(ptr_t leaf, std::string_view key, const T& value, size_t m) {
        insert_result res;
        std::string_view old_skip = get_skip(leaf);
        
        ptr_t interior = builder_.make_interior_list(key);
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
    
    ptr_t clone_leaf_with_skip(ptr_t leaf, std::string_view new_skip) {
        if (leaf->is_list()) {
            ptr_t n = builder_.make_leaf_list(new_skip);
            n->as_list()->chars = leaf->as_list()->chars;
            for (int i = 0; i < leaf->as_list()->chars.count(); ++i) {
                n->as_list()->leaf_values[i] = leaf->as_list()->leaf_values[i];
            }
            return n;
        }
        // FULL
        ptr_t n = builder_.make_leaf_full(new_skip);
        n->as_full()->valid = leaf->as_full()->valid;
        for (int c = 0; c < 256; ++c) {
            if (leaf->as_full()->valid.test(static_cast<unsigned char>(c))) {
                n->as_full()->leaf_values[c] = leaf->as_full()->leaf_values[c];
            }
        }
        return n;
    }
    
    // Add eos to leaf_list - demote to interior
    insert_result add_eos_to_leaf_list(ptr_t leaf, const T& value) {
        insert_result res;
        std::string_view leaf_skip = get_skip(leaf);
        
        ptr_t interior = builder_.make_interior_list(leaf_skip);
        interior->as_list()->eos_ptr = new T(value);
        
        // Each char in leaf becomes a leaf_eos child
        if (leaf->is_list()) {
            for (int i = 0; i < leaf->as_list()->chars.count(); ++i) {
                unsigned char c = leaf->as_list()->chars.char_at(i);
                ptr_t child = builder_.make_leaf_eos(leaf->as_list()->leaf_values[i]);
                interior->as_list()->chars.add(c);
                interior->as_list()->children[i].store(child);
            }
        } else {
            for (int c = 0; c < 256; ++c) {
                if (leaf->as_full()->valid.test(static_cast<unsigned char>(c))) {
                    ptr_t child = builder_.make_leaf_eos(leaf->as_full()->leaf_values[c]);
                    int idx = interior->as_list()->chars.add(static_cast<unsigned char>(c));
                    interior->as_list()->children[idx].store(child);
                }
            }
        }
        
        res.new_node = interior;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }
    
    // Add char to leaf
    insert_result add_char_to_leaf(ptr_t leaf, unsigned char c, const T& value) {
        insert_result res;
        
        if (leaf->is_list()) {
            int idx = leaf->as_list()->chars.find(c);
            if (idx >= 0) return res;  // Exists
            
            if (leaf->as_list()->chars.count() < LIST_MAX) {
                idx = leaf->as_list()->chars.add(c);
                leaf->as_list()->leaf_values[idx] = value;
                res.in_place = true;
                res.inserted = true;
                return res;
            }
            
            // Convert to FULL
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
        if (leaf->as_full()->valid.test(c)) return res;  // Exists
        leaf->as_full()->valid.template atomic_set<THREADED>(c);
        leaf->as_full()->leaf_values[c] = value;
        res.in_place = true;
        res.inserted = true;
        return res;
    }
    
    // Demote leaf_list - key is longer than list handles
    insert_result demote_leaf_list(ptr_t leaf, std::string_view key, const T& value) {
        insert_result res;
        std::string_view leaf_skip = get_skip(leaf);
        unsigned char first_c = static_cast<unsigned char>(key[0]);
        
        ptr_t interior = builder_.make_interior_list(leaf_skip);
        
        // Convert existing chars to leaf_eos children
        if (leaf->is_list()) {
            for (int i = 0; i < leaf->as_list()->chars.count(); ++i) {
                unsigned char c = leaf->as_list()->chars.char_at(i);
                ptr_t child = builder_.make_leaf_eos(leaf->as_list()->leaf_values[i]);
                interior->as_list()->chars.add(c);
                interior->as_list()->children[i].store(child);
            }
        } else {
            for (int c = 0; c < 256; ++c) {
                if (leaf->as_full()->valid.test(static_cast<unsigned char>(c))) {
                    ptr_t child = builder_.make_leaf_eos(leaf->as_full()->leaf_values[c]);
                    int idx = interior->as_list()->chars.add(static_cast<unsigned char>(c));
                    interior->as_list()->children[idx].store(child);
                }
            }
        }
        
        // Add new key
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
    
    // Split interior at divergence
    insert_result split_interior(ptr_t n, std::string_view key, const T& value, size_t m) {
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
    
    ptr_t clone_interior_with_skip(ptr_t n, std::string_view new_skip) {
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
    
    // Key is prefix of interior skip
    insert_result prefix_interior(ptr_t n, std::string_view key, const T& value, size_t m) {
        insert_result res;
        std::string_view old_skip = get_skip(n);
        
        ptr_t new_int = builder_.make_interior_list(key);
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
    
    // Set eos on interior
    insert_result set_interior_eos(ptr_t n, const T& value) {
        insert_result res;
        T* p = get_eos_ptr(n);
        if (p) return res;  // Exists
        
        set_eos_ptr(n, new T(value));
        res.in_place = true;
        res.inserted = true;
        return res;
    }
    
    // Add child to interior
    insert_result add_child_to_interior(ptr_t n, unsigned char c, std::string_view remaining, const T& value) {
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
    
    // =========================================================================
    // ERASE
    // =========================================================================
    struct erase_result {
        ptr_t new_node = nullptr;
        std::vector<ptr_t> old_nodes;
        bool erased = false;
        bool deleted_subtree = false;
    };
    
    erase_result erase_impl(atomic_ptr*, ptr_t n, std::string_view key) {
        erase_result res;
        if (!n) return res;
        if (n->is_leaf()) return erase_from_leaf(n, key);
        return erase_from_interior(n, key);
    }
    
    erase_result erase_from_leaf(ptr_t leaf, std::string_view key) {
        erase_result res;
        std::string_view skip = get_skip(leaf);
        size_t m = match_skip(skip, key);
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
            
            // In-place remove
            for (int i = idx; i < count - 1; ++i) {
                leaf->as_list()->leaf_values[i] = leaf->as_list()->leaf_values[i + 1];
            }
            leaf->as_list()->chars.remove_at(idx);
            res.erased = true;
            return res;
        }
        
        // FULL
        if (!leaf->as_full()->valid.test(c)) return res;
        leaf->as_full()->valid.template atomic_clear<THREADED>(c);
        res.erased = true;
        return res;
    }
    
    erase_result erase_from_interior(ptr_t n, std::string_view key) {
        erase_result res;
        std::string_view skip = get_skip(n);
        size_t m = match_skip(skip, key);
        if (m < skip.size()) return res;
        key.remove_prefix(m);
        
        if (key.empty()) {
            T* p = get_eos_ptr(n);
            if (!p) return res;
            delete p;
            set_eos_ptr(n, nullptr);
            res.erased = true;
            return try_collapse_interior(n);
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = find_child(n, c);
        if (!child) return res;
        
        auto child_res = erase_impl(get_child_slot(n, c), child, key.substr(1));
        if (!child_res.erased) return res;
        
        if (child_res.deleted_subtree) {
            return try_collapse_after_child_removal(n, c, child_res.old_nodes);
        }
        
        if (child_res.new_node) {
            get_child_slot(n, c)->store(child_res.new_node);
        }
        res.erased = true;
        res.old_nodes = std::move(child_res.old_nodes);
        return res;
    }
    
    erase_result try_collapse_interior(ptr_t n) {
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
        
        // Single child - collapse
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
    
    erase_result try_collapse_after_child_removal(ptr_t n, unsigned char removed_c, std::vector<ptr_t>& child_old) {
        erase_result res;
        res.old_nodes = std::move(child_old);
        res.erased = true;
        
        T* eos = get_eos_ptr(n);
        int remaining = n->child_count();
        
        if (n->is_list()) {
            int idx = n->as_list()->chars.find(removed_c);
            if (idx >= 0) remaining--;
        } else if (n->is_full()) {
            if (n->as_full()->valid.test(removed_c)) remaining--;
        }
        
        if (!eos && remaining == 0) {
            res.deleted_subtree = true;
            res.old_nodes.push_back(n);
            return res;
        }
        
        // In-place removal
        if (n->is_list()) {
            int idx = n->as_list()->chars.find(removed_c);
            if (idx >= 0) {
                int count = n->as_list()->chars.count();
                for (int i = idx; i < count - 1; ++i) {
                    n->as_list()->children[i].store(n->as_list()->children[i + 1].load());
                }
                n->as_list()->children[count - 1].store(nullptr);
                n->as_list()->chars.remove_at(idx);
            }
        } else if (n->is_full()) {
            n->as_full()->valid.template atomic_clear<THREADED>(removed_c);
            n->as_full()->children[removed_c].store(nullptr);
        }
        
        // Check for collapse
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
    
    erase_result collapse_single_child(ptr_t n, unsigned char c, ptr_t child, erase_result& res) {
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
            if (child->is_eos() || child->is_skip()) {
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
    
private:
    std::pair<iterator, bool> insert_locked(const Key& key, const std::string& kb, const T& value) {
        std::lock_guard<mutex_t> lock(mutex_);
        
        ptr_t root = root_.load();
        auto res = insert_impl(&root_, root, kb, value);
        
        if (!res.inserted) {
            for (auto* old : res.old_nodes) retire_node(old);
            return {find(key), false};
        }
        
        if (res.new_node) root_.store(res.new_node);
        for (auto* old : res.old_nodes) retire_node(old);
        
        if constexpr (THREADED) size_.fetch_add(1);
        else ++size_;
        
        return {iterator(this, kb, value), true};
    }
    
    bool erase_locked(const std::string& kb) {
        std::lock_guard<mutex_t> lock(mutex_);
        
        ptr_t root = root_.load();
        auto res = erase_impl(&root_, root, kb);
        
        if (!res.erased) return false;
        
        if (res.deleted_subtree) root_.store(nullptr);
        else if (res.new_node) root_.store(res.new_node);
        
        for (auto* old : res.old_nodes) retire_node(old);
        
        if constexpr (THREADED) size_.fetch_sub(1);
        else --size_;
        
        return true;
    }
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
