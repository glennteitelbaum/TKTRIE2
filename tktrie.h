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
    using node_t = node<T, THREADED, Allocator>;
    using ptr_t = node_t*;
    using atomic_ptr = atomic_node_ptr<T, THREADED, Allocator>;
    using builder_t = node_builder<T, THREADED, Allocator>;
    using iterator = tktrie_iterator<Key, T, THREADED, Allocator>;
    using mutex_t = std::conditional_t<THREADED, std::mutex, empty_mutex>;
    
private:
    atomic_ptr root_;
    std::conditional_t<THREADED, std::atomic<size_t>, size_t> size_{0};
    mutable mutex_t mutex_;
    builder_t builder_;
    
    // Static deleter for EBR
    static void node_deleter(void* ptr) {
        if (!ptr) return;
        ptr_t n = static_cast<ptr_t>(ptr);
        // Don't recurse - just delete this node, children retired separately
        delete n;
    }
    
    void retire_node(ptr_t n) {
        if (!n) return;
        if constexpr (THREADED) {
            ebr_global::instance().retire(n, node_deleter);
        } else {
            delete n;
        }
    }
    
    // Match skip prefix, return match length
    static size_t match_skip(std::string_view skip, std::string_view key) noexcept {
        size_t i = 0;
        while (i < skip.size() && i < key.size() && skip[i] == key[i]) ++i;
        return i;
    }
    
    // =========================================================================
    // READ - state-based traversal
    // =========================================================================
    bool read_impl(ptr_t n, std::string_view key, T& out) const noexcept {
        while (n) {
            if (n->is_leaf()) {
                return read_from_leaf(n, key, out);
            }
            // Interior node - consume skip if any
            if (n->is_skip() || n->is_list() || n->is_full()) {
                size_t m = match_skip(n->skip, key);
                if (m < n->skip.size()) return false;  // Key diverges
                key.remove_prefix(m);
            }
            // Check for EOS value (key fully consumed)
            if (key.empty()) {
                T* p;
                if constexpr (THREADED) p = n->eos_ptr.load(std::memory_order_acquire);
                else p = n->eos_ptr;
                if (p) { out = *p; return true; }
                return false;
            }
            // Follow child
            unsigned char c = static_cast<unsigned char>(key[0]);
            key.remove_prefix(1);
            auto* slot = n->find_child_slot(c);
            n = slot ? slot->load() : nullptr;
        }
        return false;
    }
    
    bool read_from_leaf(ptr_t leaf, std::string_view key, T& out) const noexcept {
        if (leaf->is_eos()) {
            if (!key.empty()) return false;
            out = leaf->values[0];
            return true;
        }
        if (leaf->is_skip()) {
            if (key != leaf->skip) return false;
            out = leaf->values[0];
            return true;
        }
        // LIST or FULL: skip + 1 char
        size_t m = match_skip(leaf->skip, key);
        if (m < leaf->skip.size()) return false;
        key.remove_prefix(m);
        if (key.size() != 1) return false;
        unsigned char c = static_cast<unsigned char>(key[0]);
        T* slot = leaf->find_value_slot(c);
        if (!slot) return false;
        out = *slot;
        return true;
    }
    
    // =========================================================================
    // CONTAINS
    // =========================================================================
    bool contains_impl(ptr_t n, std::string_view key) const noexcept {
        T dummy;
        return read_impl(n, key, dummy);
    }
    
    // =========================================================================
    // INSERT - state-based with LEAF transitions
    // =========================================================================
    // Returns: {new_node_to_store_in_slot, nodes_to_free, success}
    struct insert_result {
        ptr_t new_node = nullptr;
        std::vector<ptr_t> old_nodes;
        bool inserted = false;
        bool in_place = false;
    };
    
    insert_result insert_impl(atomic_ptr* slot, ptr_t n, std::string_view key, const T& value) {
        insert_result res;
        
        if (!n) {
            // Empty slot - create leaf for entire key
            res.new_node = create_leaf_for_key(key, value);
            res.inserted = true;
            return res;
        }
        
        if (n->is_leaf()) {
            return insert_into_leaf(slot, n, key, value);
        }
        
        // Interior node
        return insert_into_interior(slot, n, key, value);
    }
    
    insert_result insert_into_leaf(atomic_ptr* slot, ptr_t leaf, std::string_view key, const T& value) {
        insert_result res;
        
        // Get leaf's key coverage
        std::string_view leaf_skip = leaf->skip;
        
        if (leaf->is_eos()) {
            // Leaf covers empty key
            if (key.empty()) {
                // Already exists
                return res;
            }
            // Need to demote: leaf becomes interior, create new leaf child
            return demote_leaf_eos(leaf, key, value);
        }
        
        if (leaf->is_skip()) {
            // Leaf covers exactly leaf_skip
            size_t m = match_skip(leaf_skip, key);
            if (m == leaf_skip.size() && m == key.size()) {
                // Same key - already exists
                return res;
            }
            if (m < leaf_skip.size() && m < key.size()) {
                // Diverge - split
                return split_leaf_skip(leaf, key, value, m);
            }
            if (m == key.size()) {
                // Key is prefix of leaf_skip - insert interior, leaf becomes child
                return prefix_leaf_skip(leaf, key, value, m);
            }
            // leaf_skip is prefix of key - demote leaf, insert deeper
            return extend_leaf_skip(leaf, key, value, m);
        }
        
        // LIST or FULL: covers skip + 1 char
        size_t m = match_skip(leaf_skip, key);
        if (m < leaf_skip.size() && m < key.size()) {
            // Diverge in skip
            return split_leaf_list(leaf, key, value, m);
        }
        if (m < leaf_skip.size()) {
            // Key ends in skip - need interior before leaf
            return prefix_leaf_list(leaf, key, value, m);
        }
        key.remove_prefix(m);
        
        if (key.empty()) {
            // Key ends at skip - need interior with eos
            return add_eos_to_leaf_list(leaf, value);
        }
        
        if (key.size() == 1) {
            // Key matches leaf structure - add to list/full
            unsigned char c = static_cast<unsigned char>(key[0]);
            return add_char_to_leaf(leaf, c, value);
        }
        
        // Key is longer - demote leaf
        return demote_leaf_list(leaf, key, value);
    }
    
    insert_result insert_into_interior(atomic_ptr* slot, ptr_t n, std::string_view key, const T& value) {
        insert_result res;
        
        // Consume skip
        std::string_view node_skip;
        if (n->is_skip() || n->is_list() || n->is_full()) {
            node_skip = n->skip;
        }
        
        size_t m = match_skip(node_skip, key);
        if (m < node_skip.size() && m < key.size()) {
            // Diverge in skip - split interior
            return split_interior(n, key, value, m);
        }
        if (m < node_skip.size()) {
            // Key is prefix of skip - insert value at split point
            return prefix_interior(n, key, value, m);
        }
        key.remove_prefix(m);
        
        if (key.empty()) {
            // Set EOS value
            return set_interior_eos(n, value);
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        
        // Find or create child slot
        auto* child_slot = n->find_child_slot(c);
        if (child_slot) {
            ptr_t child = child_slot->load();
            if (child) {
                // Recurse into child
                auto child_res = insert_impl(child_slot, child, key, value);
                if (child_res.new_node && child_res.new_node != child) {
                    child_slot->store(child_res.new_node);
                }
                // Don't propagate new_node up - the parent node wasn't replaced
                insert_result res;
                res.inserted = child_res.inserted;
                res.in_place = child_res.in_place;
                res.old_nodes = std::move(child_res.old_nodes);
                return res;
            }
        }
        
        // Add new child
        return add_child_to_interior(n, c, key, value);
    }
    
    // Helper: create leaf node for given key
    ptr_t create_leaf_for_key(std::string_view key, const T& value) {
        if (key.empty()) {
            return builder_.make_leaf_eos(value);
        }
        if (key.size() == 1) {
            ptr_t leaf = builder_.make_leaf_list("");
            unsigned char c = static_cast<unsigned char>(key[0]);
            leaf->chars.add(c);
            leaf->values[0] = value;
            return leaf;
        }
        // Skip + 1 char
        ptr_t leaf = builder_.make_leaf_list(key.substr(0, key.size() - 1));
        unsigned char c = static_cast<unsigned char>(key.back());
        leaf->chars.add(c);
        leaf->values[0] = value;
        return leaf;
    }
    
    // Demote leaf_eos: becomes interior with child
    insert_result demote_leaf_eos(ptr_t leaf, std::string_view key, const T& value) {
        insert_result res;
        // Old leaf value becomes eos_ptr on new interior
        // New key gets a child leaf
        ptr_t interior = builder_.make_interior_list("");
        interior->eos_ptr = new T(leaf->values[0]);
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = create_leaf_for_key(key.substr(1), value);
        interior->chars.add(c);
        interior->children[0].store(child);
        
        res.new_node = interior;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }
    
    // Split leaf_skip at divergence
    insert_result split_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m) {
        insert_result res;
        std::string_view old_skip = leaf->skip;
        
        // Common prefix
        std::string common(old_skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);
        
        // Create interior with two children
        ptr_t interior = builder_.make_interior_list(common);
        
        // Old leaf with shorter skip
        ptr_t old_child = builder_.make_leaf_skip(old_skip.substr(m + 1), leaf->values[0]);
        
        // New leaf
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        
        interior->chars.add(old_c);
        interior->chars.add(new_c);
        interior->children[0].store(old_child);
        interior->children[1].store(new_child);
        
        res.new_node = interior;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }
    
    // Key is prefix of leaf_skip
    insert_result prefix_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m) {
        insert_result res;
        std::string_view old_skip = leaf->skip;
        
        // Create interior with eos for new key, child for old
        ptr_t interior = builder_.make_interior_list(key);
        interior->eos_ptr = new T(value);
        
        unsigned char c = static_cast<unsigned char>(old_skip[m]);
        ptr_t child = builder_.make_leaf_skip(old_skip.substr(m + 1), leaf->values[0]);
        interior->chars.add(c);
        interior->children[0].store(child);
        
        res.new_node = interior;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }
    
    // leaf_skip is prefix of key
    insert_result extend_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m) {
        insert_result res;
        std::string_view old_skip = leaf->skip;
        
        // Create interior with eos for old value, child for new
        ptr_t interior = builder_.make_interior_list(old_skip);
        interior->eos_ptr = new T(leaf->values[0]);
        
        unsigned char c = static_cast<unsigned char>(key[m]);
        ptr_t child = create_leaf_for_key(key.substr(m + 1), value);
        interior->chars.add(c);
        interior->children[0].store(child);
        
        res.new_node = interior;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }
    
    // Split leaf_list at divergence in skip
    insert_result split_leaf_list(ptr_t leaf, std::string_view key, const T& value, size_t m) {
        insert_result res;
        std::string_view old_skip = leaf->skip;
        
        std::string common(old_skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);
        
        ptr_t interior = builder_.make_interior_list(common);
        
        // Clone old leaf with shorter skip
        ptr_t old_child;
        if (leaf->is_list()) {
            old_child = builder_.make_leaf_list(old_skip.substr(m + 1));
            old_child->chars = leaf->chars;
            for (int i = 0; i < leaf->chars.count(); ++i) {
                old_child->values[i] = leaf->values[i];
            }
        } else {
            old_child = builder_.make_leaf_full(old_skip.substr(m + 1));
            old_child->valid = leaf->valid;
            for (int c = 0; c < 256; ++c) {
                if (leaf->valid.test(static_cast<unsigned char>(c))) {
                    old_child->values[c] = leaf->values[c];
                }
            }
        }
        
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        
        interior->chars.add(old_c);
        interior->chars.add(new_c);
        interior->children[0].store(old_child);
        interior->children[1].store(new_child);
        
        res.new_node = interior;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }
    
    // Key ends in leaf's skip
    insert_result prefix_leaf_list(ptr_t leaf, std::string_view key, const T& value, size_t m) {
        insert_result res;
        std::string_view old_skip = leaf->skip;
        
        ptr_t interior = builder_.make_interior_list(key);
        interior->eos_ptr = new T(value);
        
        unsigned char c = static_cast<unsigned char>(old_skip[m]);
        
        // Clone leaf with shorter skip
        ptr_t old_child;
        if (leaf->is_list()) {
            old_child = builder_.make_leaf_list(old_skip.substr(m + 1));
            old_child->chars = leaf->chars;
            for (int i = 0; i < leaf->chars.count(); ++i) {
                old_child->values[i] = leaf->values[i];
            }
        } else {
            old_child = builder_.make_leaf_full(old_skip.substr(m + 1));
            old_child->valid = leaf->valid;
            for (int c2 = 0; c2 < 256; ++c2) {
                if (leaf->valid.test(static_cast<unsigned char>(c2))) {
                    old_child->values[c2] = leaf->values[c2];
                }
            }
        }
        
        interior->chars.add(c);
        interior->children[0].store(old_child);
        
        res.new_node = interior;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }
    
    // Add eos to leaf_list - demote to interior
    insert_result add_eos_to_leaf_list(ptr_t leaf, const T& value) {
        insert_result res;
        
        ptr_t interior = builder_.make_interior_list(leaf->skip);
        interior->eos_ptr = new T(value);
        
        // Each char in leaf becomes a leaf_eos child
        if (leaf->is_list()) {
            for (int i = 0; i < leaf->chars.count(); ++i) {
                unsigned char c = leaf->chars.char_at(i);
                ptr_t child = builder_.make_leaf_eos(leaf->values[i]);
                interior->chars.add(c);
                interior->children[i].store(child);
            }
        } else {
            // Convert to LIST for simplicity (could keep FULL)
            for (int c = 0; c < 256; ++c) {
                if (leaf->valid.test(static_cast<unsigned char>(c))) {
                    ptr_t child = builder_.make_leaf_eos(leaf->values[c]);
                    int idx = interior->chars.add(static_cast<unsigned char>(c));
                    interior->children[idx].store(child);
                }
            }
        }
        
        res.new_node = interior;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }
    
    // Add char to leaf_list/full
    insert_result add_char_to_leaf(ptr_t leaf, unsigned char c, const T& value) {
        insert_result res;
        
        if (leaf->is_list()) {
            if (leaf->chars.find(c) >= 0) {
                // Already exists
                return res;
            }
            if (leaf->chars.count() < LIST_MAX) {
                // In-place add
                int idx = leaf->chars.add(c);
                leaf->values[idx] = value;
                res.in_place = true;
                res.inserted = true;
                return res;
            }
            // Convert to FULL
            ptr_t full = builder_.make_leaf_full(leaf->skip);
            for (int i = 0; i < leaf->chars.count(); ++i) {
                unsigned char ch = leaf->chars.char_at(i);
                full->valid.set(ch);
                full->values[ch] = leaf->values[i];
            }
            full->valid.set(c);
            full->values[c] = value;
            
            res.new_node = full;
            res.old_nodes.push_back(leaf);
            res.inserted = true;
            return res;
        }
        
        // FULL
        if (leaf->valid.test(c)) {
            return res;  // Already exists
        }
        leaf->valid.template atomic_set<THREADED>(c);
        leaf->values[c] = value;
        res.in_place = true;
        res.inserted = true;
        return res;
    }
    
    // Demote leaf_list - key is longer than skip+1
    insert_result demote_leaf_list(ptr_t leaf, std::string_view key, const T& value) {
        insert_result res;
        
        // key has had skip consumed, so key[0] is the char after skip
        unsigned char first_c = static_cast<unsigned char>(key[0]);
        
        ptr_t interior = builder_.make_interior_list(leaf->skip);
        
        // Each entry in leaf becomes a leaf_eos child
        if (leaf->is_list()) {
            for (int i = 0; i < leaf->chars.count(); ++i) {
                unsigned char c = leaf->chars.char_at(i);
                ptr_t child = builder_.make_leaf_eos(leaf->values[i]);
                interior->chars.add(c);
                interior->children[i].store(child);
            }
        } else {
            for (int c = 0; c < 256; ++c) {
                if (leaf->valid.test(static_cast<unsigned char>(c))) {
                    ptr_t child = builder_.make_leaf_eos(leaf->values[c]);
                    int idx = interior->chars.add(static_cast<unsigned char>(c));
                    interior->children[idx].store(child);
                }
            }
        }
        
        // Now add the new key
        int existing_idx = interior->chars.find(first_c);
        if (existing_idx >= 0) {
            // Need to extend existing child
            ptr_t child = interior->children[existing_idx].load();
            auto child_res = insert_impl(&interior->children[existing_idx], child, key.substr(1), value);
            if (child_res.new_node) {
                interior->children[existing_idx].store(child_res.new_node);
            }
            for (auto* old : child_res.old_nodes) {
                res.old_nodes.push_back(old);
            }
        } else {
            // Add new child
            ptr_t child = create_leaf_for_key(key.substr(1), value);
            int idx = interior->chars.add(first_c);
            interior->children[idx].store(child);
        }
        
        res.new_node = interior;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }
    
    // Split interior at divergence in skip
    insert_result split_interior(ptr_t n, std::string_view key, const T& value, size_t m) {
        insert_result res;
        std::string_view old_skip = n->skip;
        
        std::string common(old_skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);
        
        ptr_t new_int = builder_.make_interior_list(common);
        
        // Clone old node with shorter skip
        ptr_t old_child;
        if (n->is_list()) {
            old_child = builder_.make_interior_list(old_skip.substr(m + 1));
            old_child->chars = n->chars;
            old_child->take_eos_from(*n);
            for (int i = 0; i < n->chars.count(); ++i) {
                old_child->children[i].store(n->children[i].load());
                n->children[i].store(nullptr);
            }
        } else if (n->is_full()) {
            old_child = builder_.make_interior_full(old_skip.substr(m + 1));
            old_child->valid = n->valid;
            old_child->take_eos_from(*n);
            for (int c = 0; c < 256; ++c) {
                if (n->valid.test(static_cast<unsigned char>(c))) {
                    old_child->children[c].store(n->children[c].load());
                    n->children[c].store(nullptr);
                }
            }
        } else {
            old_child = builder_.make_interior_skip(old_skip.substr(m + 1));
            old_child->take_eos_from(*n);
        }
        
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        
        new_int->chars.add(old_c);
        new_int->chars.add(new_c);
        new_int->children[0].store(old_child);
        new_int->children[1].store(new_child);
        
        res.new_node = new_int;
        res.old_nodes.push_back(n);
        res.inserted = true;
        return res;
    }
    
    // Key is prefix of interior's skip
    insert_result prefix_interior(ptr_t n, std::string_view key, const T& value, size_t m) {
        insert_result res;
        std::string_view old_skip = n->skip;
        
        ptr_t new_int = builder_.make_interior_list(key);
        new_int->eos_ptr = new T(value);
        
        unsigned char c = static_cast<unsigned char>(old_skip[m]);
        
        // Clone with shorter skip
        ptr_t old_child;
        if (n->is_list()) {
            old_child = builder_.make_interior_list(old_skip.substr(m + 1));
            old_child->chars = n->chars;
            old_child->take_eos_from(*n);
            for (int i = 0; i < n->chars.count(); ++i) {
                old_child->children[i].store(n->children[i].load());
                n->children[i].store(nullptr);
            }
        } else if (n->is_full()) {
            old_child = builder_.make_interior_full(old_skip.substr(m + 1));
            old_child->valid = n->valid;
            old_child->take_eos_from(*n);
            for (int c2 = 0; c2 < 256; ++c2) {
                if (n->valid.test(static_cast<unsigned char>(c2))) {
                    old_child->children[c2].store(n->children[c2].load());
                    n->children[c2].store(nullptr);
                }
            }
        } else {
            old_child = builder_.make_interior_skip(old_skip.substr(m + 1));
            old_child->take_eos_from(*n);
        }
        
        new_int->chars.add(c);
        new_int->children[0].store(old_child);
        
        res.new_node = new_int;
        res.old_nodes.push_back(n);
        res.inserted = true;
        return res;
    }
    
    // Set eos on interior node
    insert_result set_interior_eos(ptr_t n, const T& value) {
        insert_result res;
        T* p;
        if constexpr (THREADED) p = n->eos_ptr.load(std::memory_order_acquire);
        else p = n->eos_ptr;
        if (p) return res;  // Already exists
        
        T* np = new T(value);
        if constexpr (THREADED) n->eos_ptr.store(np, std::memory_order_release);
        else n->eos_ptr = np;
        res.in_place = true;
        res.inserted = true;
        return res;
    }
    
    // Add child to interior node
    insert_result add_child_to_interior(ptr_t n, unsigned char c, std::string_view remaining, const T& value) {
        insert_result res;
        
        ptr_t child = create_leaf_for_key(remaining, value);
        
        if (n->is_list()) {
            if (n->chars.count() < LIST_MAX) {
                int idx = n->chars.add(c);
                n->children[idx].store(child);
                res.in_place = true;
                res.inserted = true;
                return res;
            }
            // Convert to FULL
            ptr_t full = builder_.make_interior_full(n->skip);
            full->take_eos_from(*n);
            for (int i = 0; i < n->chars.count(); ++i) {
                unsigned char ch = n->chars.char_at(i);
                full->valid.set(ch);
                full->children[ch].store(n->children[i].load());
                n->children[i].store(nullptr);
            }
            full->valid.set(c);
            full->children[c].store(child);
            
            res.new_node = full;
            res.old_nodes.push_back(n);
            res.inserted = true;
            return res;
        }
        
        if (n->is_full()) {
            n->valid.template atomic_set<THREADED>(c);
            n->children[c].store(child);
            res.in_place = true;
            res.inserted = true;
            return res;
        }
        
        // EOS or SKIP - convert to LIST
        ptr_t list = builder_.make_interior_list(n->skip);
        list->take_eos_from(*n);
        list->chars.add(c);
        list->children[0].store(child);
        
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
    
    erase_result erase_impl(atomic_ptr* slot, ptr_t n, std::string_view key) {
        erase_result res;
        if (!n) return res;
        
        if (n->is_leaf()) {
            return erase_from_leaf(n, key);
        }
        return erase_from_interior(n, key);
    }
    
    erase_result erase_from_leaf(ptr_t leaf, std::string_view key) {
        erase_result res;
        
        if (leaf->is_eos()) {
            if (!key.empty()) return res;
            res.deleted_subtree = true;
            res.old_nodes.push_back(leaf);
            res.erased = true;
            return res;
        }
        
        if (leaf->is_skip()) {
            if (key != leaf->skip) return res;
            res.deleted_subtree = true;
            res.old_nodes.push_back(leaf);
            res.erased = true;
            return res;
        }
        
        // LIST or FULL
        size_t m = match_skip(leaf->skip, key);
        if (m < leaf->skip.size()) return res;
        key.remove_prefix(m);
        if (key.size() != 1) return res;
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        
        if (leaf->is_list()) {
            int idx = leaf->chars.find(c);
            if (idx < 0) return res;
            
            if (leaf->chars.count() == 1) {
                res.deleted_subtree = true;
                res.old_nodes.push_back(leaf);
                res.erased = true;
                return res;
            }
            
            // In-place remove: shift values down, then remove char
            int count = leaf->chars.count();
            for (int i = idx; i < count - 1; ++i) {
                leaf->values[i] = leaf->values[i + 1];
            }
            leaf->chars.remove_at(idx);
            res.erased = true;
            return res;
        }
        
        // FULL
        if (!leaf->valid.test(c)) return res;
        
        if (leaf->valid.count() == 1) {
            res.deleted_subtree = true;
            res.old_nodes.push_back(leaf);
            res.erased = true;
            return res;
        }
        
        leaf->valid.template atomic_clear<THREADED>(c);
        res.erased = true;
        return res;
    }
    
    erase_result erase_from_interior(ptr_t n, std::string_view key) {
        erase_result res;
        
        std::string_view node_skip;
        if (n->is_skip() || n->is_list() || n->is_full()) {
            node_skip = n->skip;
        }
        
        size_t m = match_skip(node_skip, key);
        if (m < node_skip.size()) return res;
        key.remove_prefix(m);
        
        if (key.empty()) {
            // Erase eos value
            T* p;
            if constexpr (THREADED) p = n->eos_ptr.load(std::memory_order_acquire);
            else p = n->eos_ptr;
            if (!p) return res;
            
            delete p;
            if constexpr (THREADED) n->eos_ptr.store(nullptr, std::memory_order_release);
            else n->eos_ptr = nullptr;
            
            // Check if node can be collapsed
            res = try_collapse_interior(n);
            res.erased = true;
            return res;
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        
        auto* child_slot = n->find_child_slot(c);
        if (!child_slot) return res;
        
        ptr_t child = child_slot->load();
        if (!child) return res;
        
        auto child_res = erase_impl(child_slot, child, key);
        if (!child_res.erased) return child_res;
        
        if (child_res.deleted_subtree) {
            child_slot->store(nullptr);
            // Try to collapse
            res = try_collapse_after_child_removal(n, c, child_res.old_nodes);
            res.erased = true;
            return res;
        }
        
        if (child_res.new_node) {
            child_slot->store(child_res.new_node);
        }
        // Don't propagate new_node up - parent wasn't replaced
        res.erased = true;
        res.old_nodes = std::move(child_res.old_nodes);
        return res;
    }
    
    erase_result try_collapse_interior(ptr_t n) {
        erase_result res;
        
        T* eos;
        if constexpr (THREADED) eos = n->eos_ptr.load(std::memory_order_acquire);
        else eos = n->eos_ptr;
        if (eos) return res;  // Has EOS value, can't collapse
        
        // Check for single child (LIST[1] or FULL[1])
        bool can_collapse = false;
        unsigned char c = 0;
        ptr_t child = nullptr;
        
        if (n->is_list() && n->chars.count() == 1) {
            c = n->chars.char_at(0);
            child = n->children[0].load();
            can_collapse = (child != nullptr);
        } else if (n->is_full() && n->valid.count() == 1) {
            c = n->valid.first();
            child = n->children[c].load();
            can_collapse = (child != nullptr);
        }
        
        if (!can_collapse) return res;
        
        return collapse_single_child(n, c, child, res);
    }
    
    erase_result try_collapse_after_child_removal(ptr_t n, unsigned char removed_c, std::vector<ptr_t>& child_old) {
        erase_result res;
        res.old_nodes = std::move(child_old);
        
        // Check if node should be deleted or collapsed
        T* eos;
        if constexpr (THREADED) eos = n->eos_ptr.load(std::memory_order_acquire);
        else eos = n->eos_ptr;
        
        int remaining = n->child_count();
        if (n->is_list()) {
            int idx = n->chars.find(removed_c);
            if (idx >= 0) remaining--;
        } else if (n->is_full()) {
            if (n->valid.test(removed_c)) remaining--;
        }
        
        if (!eos && remaining == 0) {
            res.deleted_subtree = true;
            res.old_nodes.push_back(n);
            return res;
        }
        
        // First, do the in-place removal
        if (n->is_list()) {
            int idx = n->chars.find(removed_c);
            if (idx >= 0) {
                int count = n->chars.count();
                for (int i = idx; i < count - 1; ++i) {
                    n->children[i].store(n->children[i + 1].load());
                }
                n->children[count - 1].store(nullptr);
                n->chars.remove_at(idx);
            }
        } else if (n->is_full()) {
            n->valid.template atomic_clear<THREADED>(removed_c);
            n->children[removed_c].store(nullptr);
        }
        
        // Now check if LIST[1] or FULL[1] can collapse to SKIP
        bool can_collapse = false;
        unsigned char c = 0;
        ptr_t child = nullptr;
        
        if (n->is_list() && n->chars.count() == 1 && !eos) {
            c = n->chars.char_at(0);
            child = n->children[0].load();
            can_collapse = (child != nullptr);
        } else if (n->is_full() && !eos) {
            int cnt = n->valid.count();
            if (cnt == 1) {
                c = n->valid.first();
                child = n->children[c].load();
                can_collapse = (child != nullptr);
            }
        }
        
        if (can_collapse) {
            return collapse_single_child(n, c, child, res);
        }
        
        return res;
    }
    
    // Helper to collapse a node with single child into merged SKIP node
    erase_result collapse_single_child(ptr_t n, unsigned char c, ptr_t child, erase_result& res) {
        // Build new skip = parent_skip + c + child_skip
        std::string new_skip = n->skip;
        new_skip.push_back(static_cast<char>(c));
        new_skip.append(child->skip);
        
        // Clone child with extended skip
        ptr_t merged;
        if (child->is_leaf()) {
            if (child->is_eos()) {
                merged = builder_.make_leaf_skip(new_skip, child->values[0]);
            } else if (child->is_skip()) {
                merged = builder_.make_leaf_skip(new_skip, child->values[0]);
            } else if (child->is_list()) {
                merged = builder_.make_leaf_list(new_skip);
                merged->chars = child->chars;
                for (int i = 0; i < child->chars.count(); ++i) {
                    merged->values[i] = child->values[i];
                }
            } else { // FULL
                merged = builder_.make_leaf_full(new_skip);
                merged->valid = child->valid;
                for (int i = 0; i < 256; ++i) {
                    if (child->valid.test(static_cast<unsigned char>(i))) {
                        merged->values[i] = child->values[i];
                    }
                }
            }
        } else {
            // Interior child
            if (child->is_eos() || child->is_skip()) {
                merged = builder_.make_interior_skip(new_skip);
                merged->take_eos_from(*child);
            } else if (child->is_list()) {
                merged = builder_.make_interior_list(new_skip);
                merged->take_eos_from(*child);
                merged->chars = child->chars;
                for (int i = 0; i < child->chars.count(); ++i) {
                    merged->children[i].store(child->children[i].load());
                    child->children[i].store(nullptr);
                }
            } else { // FULL
                merged = builder_.make_interior_full(new_skip);
                merged->take_eos_from(*child);
                merged->valid = child->valid;
                for (int i = 0; i < 256; ++i) {
                    if (child->valid.test(static_cast<unsigned char>(i))) {
                        merged->children[i].store(child->children[i].load());
                        child->children[i].store(nullptr);
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
        root_.store(builder_.deep_copy(other.root_.load()));
        if constexpr (THREADED) size_.store(other.size_.load());
        else size_ = other.size_;
    }
    
    tktrie& operator=(const tktrie& other) {
        if (this != &other) {
            clear();
            root_.store(builder_.deep_copy(other.root_.load()));
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
    
    bool empty() const noexcept { return size() == 0; }
    size_t size() const noexcept {
        if constexpr (THREADED) return size_.load(std::memory_order_relaxed);
        else return size_;
    }
    
    void clear() {
        if constexpr (THREADED) {
            std::lock_guard<mutex_t> lock(mutex_);
            ptr_t r = root_.load();
            root_.store(nullptr);
            size_.store(0);
            if (r) {
                // Advance epochs twice and reclaim to ensure no readers
                ebr_global::instance().advance_epoch();
                ebr_global::instance().advance_epoch();
                ebr_global::instance().try_reclaim();
                builder_.dealloc_node(r);
            }
        } else {
            ptr_t r = root_.load();
            root_.store(nullptr);
            builder_.dealloc_node(r);
            size_ = 0;
        }
    }
    
    bool contains(const Key& key) const {
        std::string kb = traits::to_bytes(key);
        if constexpr (THREADED) {
            auto guard = get_ebr_slot().get_guard();
            return contains_impl(root_.load(), kb);
        } else {
            return contains_impl(root_.load(), kb);
        }
    }
    
    std::pair<iterator, bool> insert(const std::pair<const Key, T>& kv) {
        std::string kb = traits::to_bytes(kv.first);
        
        if constexpr (THREADED) {
            std::lock_guard<mutex_t> lock(mutex_);
            return insert_locked(kv.first, kb, kv.second);
        } else {
            return insert_locked(kv.first, kb, kv.second);
        }
    }
    
    bool erase(const Key& key) {
        std::string kb = traits::to_bytes(key);
        
        if constexpr (THREADED) {
            std::lock_guard<mutex_t> lock(mutex_);
            return erase_locked(kb);
        } else {
            return erase_locked(kb);
        }
    }
    
    iterator find(const Key& key) const {
        std::string kb = traits::to_bytes(key);
        T value;
        if constexpr (THREADED) {
            auto guard = get_ebr_slot().get_guard();
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
    
    iterator begin() const { return end(); }  // TODO: implement iteration
    iterator end() const { return iterator(); }
    
private:
    std::pair<iterator, bool> insert_locked(const Key& key, const std::string& kb, const T& value) {
        auto res = insert_impl(&root_, root_.load(), kb, value);
        if (res.new_node) {
            root_.store(res.new_node);
        }
        for (auto* old : res.old_nodes) {
            if constexpr (THREADED) {
                retire_node(old);
            } else {
                builder_.dealloc_node(old);
            }
        }
        if constexpr (THREADED) {
            ebr_global::instance().advance_epoch();
            ebr_global::instance().try_reclaim();
        }
        if (res.inserted) {
            if constexpr (THREADED) size_.fetch_add(1);
            else ++size_;
            return {iterator(this, kb, value), true};
        }
        return {find(key), false};
    }
    
    bool erase_locked(const std::string& kb) {
        auto res = erase_impl(&root_, root_.load(), kb);
        if (res.deleted_subtree) {
            root_.store(nullptr);
        } else if (res.new_node) {
            root_.store(res.new_node);
        }
        for (auto* old : res.old_nodes) {
            if constexpr (THREADED) {
                retire_node(old);
            } else {
                builder_.dealloc_node(old);
            }
        }
        if constexpr (THREADED) {
            ebr_global::instance().advance_epoch();
            ebr_global::instance().try_reclaim();
        }
        if (res.erased) {
            if constexpr (THREADED) size_.fetch_sub(1);
            else --size_;
            return true;
        }
        return false;
    }
};

// Iterator
template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie_iterator {
    using trie_t = tktrie<Key, T, THREADED, Allocator>;
    using traits = tktrie_traits<Key>;
    
    const trie_t* trie_ = nullptr;
    std::string key_bytes_;
    T value_;
    bool valid_ = false;
    
public:
    tktrie_iterator() = default;
    tktrie_iterator(const trie_t* t, const std::string& kb, const T& v)
        : trie_(t), key_bytes_(kb), value_(v), valid_(true) {}
    
    Key key() const { return traits::from_bytes(key_bytes_); }
    const T& value() const { return value_; }
    bool valid() const { return valid_; }
    explicit operator bool() const { return valid_; }
    
    std::pair<Key, T> operator*() const { return {key(), value_}; }
    
    bool operator==(const tktrie_iterator& o) const {
        if (!valid_ && !o.valid_) return true;
        if (valid_ != o.valid_) return false;
        return key_bytes_ == o.key_bytes_;
    }
    bool operator!=(const tktrie_iterator& o) const { return !(*this == o); }
};

// Convenience aliases
template <typename T, typename Allocator = std::allocator<uint64_t>>
using string_trie = tktrie<std::string, T, false, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using int32_trie = tktrie<int32_t, T, false, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using int64_trie = tktrie<int64_t, T, false, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_string_trie = tktrie<std::string, T, true, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_int32_trie = tktrie<int32_t, T, true, Allocator>;

}  // namespace gteitelbaum
