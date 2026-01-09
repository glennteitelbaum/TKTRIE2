#pragma once

// This file contains implementation details for tktrie (insert operations)
// It should only be included from tktrie_core.h

#include "tktrie_insert_helper.h"

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

// -----------------------------------------------------------------------------
// Insert operations
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::insert_impl(
    atomic_ptr* slot, ptr_t n, std::string_view key, const T& value) {
    insert_result res;

    if (!n || n->is_poisoned() || builder_t::is_sentinel(n)) {
        res.new_node = create_leaf_for_key(key, value);
        res.inserted = true;
        return res;
    }

    if (n->is_leaf()) return insert_into_leaf(slot, n, key, value);
    return insert_into_interior(slot, n, key, value);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::insert_into_leaf(
    atomic_ptr*, ptr_t leaf, std::string_view key, const T& value) {
    insert_result res;
    std::string_view leaf_skip = leaf->skip_str();

    if (leaf->is_skip()) {
        size_t m = match_skip_impl(leaf_skip, key);
        if ((m == leaf_skip.size()) & (m == key.size())) return res;
        if ((m < leaf_skip.size()) & (m < key.size())) return split_leaf_skip(leaf, key, value, m);
        if (m == key.size()) return prefix_leaf_skip(leaf, key, value, m);
        return extend_leaf_skip(leaf, key, value, m);
    }

    // BINARY, LIST, POP, or FULL leaf
    size_t m = match_skip_impl(leaf_skip, key);
    if ((m < leaf_skip.size()) & (m < key.size())) return split_leaf_multi(leaf, key, value, m);
    if (m < leaf_skip.size()) return prefix_leaf_multi(leaf, key, value, m);
    key.remove_prefix(m);

    if (key.empty()) return add_eos_to_leaf_multi(leaf, value);
    if (key.size() == 1) {
        unsigned char c = static_cast<unsigned char>(key[0]);
        return add_char_to_leaf(leaf, c, value);
    }
    return demote_leaf_multi(leaf, key, value);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::insert_into_interior(
    atomic_ptr*, ptr_t n, std::string_view key, const T& value) {
    insert_result res;
    std::string_view skip = n->skip_str();

    size_t m = match_skip_impl(skip, key);
    if ((m < skip.size()) & (m < key.size())) return split_interior(n, key, value, m);
    if (m < skip.size()) return prefix_interior(n, key, value, m);
    key.remove_prefix(m);

    if (key.empty()) return set_interior_eos(n, value);

    unsigned char c = static_cast<unsigned char>(key[0]);
    key.remove_prefix(1);

    ptr_t child = n->get_child(c);
    if (child && !builder_t::is_sentinel(child)) {
        atomic_ptr* child_slot = n->get_child_slot(c);
        auto child_res = insert_impl(child_slot, child, key, value);
        if (child_res.new_node && child_res.new_node != child) {
            if constexpr (THREADED) {
                child_slot->store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
            }
            child_slot->store(child_res.new_node);
        }
        res.inserted = child_res.inserted;
        res.in_place = child_res.in_place;
        res.old_nodes = std::move(child_res.old_nodes);
        return res;
    }

    return add_child_to_interior(n, c, key, value);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::create_leaf_for_key(std::string_view key, const T& value) {
    return builder_.make_leaf_skip(key, value);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::split_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->skip_str();

    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);

    // If both keys have exactly 1 char remaining, create a BINARY leaf
    if (old_skip.size() == m + 1 && key.size() == m + 1) {
        ptr_t binary = builder_.make_leaf_binary(common);
        T old_value;
        leaf->as_skip()->value.try_read(old_value);
        binary->template as_binary<true>()->add_entry(old_c, old_value);
        binary->template as_binary<true>()->add_entry(new_c, value);
        binary->template as_binary<true>()->update_capacity_flags();
        res.new_node = binary;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }

    // Otherwise create interior node with children
    ptr_t interior = builder_.make_interior_binary(common);
    T old_value;
    leaf->as_skip()->value.try_read(old_value);
    ptr_t old_child = builder_.make_leaf_skip(old_skip.substr(m + 1), old_value);
    ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
    interior->template as_binary<false>()->add_child(old_c, old_child);
    interior->template as_binary<false>()->add_child(new_c, new_child);
    interior->template as_binary<false>()->update_capacity_flags();

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->skip_str();

    T old_value;
    leaf->as_skip()->value.try_read(old_value);
    ptr_t child = builder_.make_leaf_skip(old_skip.substr(m + 1), old_value);

    // For FIXED_LEN==0: 1 child + EOS = 2 entries -> BINARY
    // For FIXED_LEN>0: 1 child = 1 entry -> still need to store, use BINARY with 1 child
    ptr_t interior = builder_.make_interior_binary(std::string(key));
    if constexpr (FIXED_LEN == 0) {
        interior->set_eos(value);
    }
    interior->template as_binary<false>()->add_child(static_cast<unsigned char>(old_skip[m]), child);
    interior->template as_binary<false>()->update_capacity_flags();

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::extend_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->skip_str();

    // For FIXED_LEN==0: EOS + 1 child = 2 entries -> BINARY
    // For FIXED_LEN>0: 1 child = 1 entry, use BINARY
    ptr_t interior = builder_.make_interior_binary(std::string(old_skip));
    if constexpr (FIXED_LEN == 0) {
        T old_value;
        leaf->as_skip()->value.try_read(old_value);
        interior->set_eos(old_value);
    }

    ptr_t child = create_leaf_for_key(key.substr(m + 1), value);
    interior->template as_binary<false>()->add_child(static_cast<unsigned char>(key[m]), child);
    interior->template as_binary<false>()->update_capacity_flags();

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::split_leaf_multi(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->skip_str();

    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);

    ptr_t interior = builder_.make_interior_binary(common);
    ptr_t old_child = clone_leaf_with_skip(leaf, old_skip.substr(m + 1));
    ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
    interior->template as_binary<false>()->add_child(old_c, old_child);
    interior->template as_binary<false>()->add_child(new_c, new_child);
    interior->template as_binary<false>()->update_capacity_flags();

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_leaf_multi(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->skip_str();

    // 1 child (+ EOS for FIXED_LEN==0) -> BINARY
    ptr_t interior = builder_.make_interior_binary(std::string(key));
    if constexpr (FIXED_LEN == 0) {
        interior->set_eos(value);
    }

    ptr_t old_child = clone_leaf_with_skip(leaf, old_skip.substr(m + 1));
    interior->template as_binary<false>()->add_child(static_cast<unsigned char>(old_skip[m]), old_child);
    interior->template as_binary<false>()->update_capacity_flags();

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::clone_leaf_with_skip(ptr_t leaf, std::string_view new_skip) {
    using ops = trie_ops<T, THREADED, Allocator, FIXED_LEN>;
    return ops::clone_leaf_with_skip(leaf, new_skip, builder_);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_eos_to_leaf_multi(ptr_t leaf, const T& value) {
    insert_result res;
    
    if constexpr (FIXED_LEN > 0) {
        return res;
    } else {
        using ops = trie_ops<T, THREADED, Allocator, FIXED_LEN>;
        
        // Create interior and add all leaf entries as SKIP children
        ptr_t interior = ops::leaf_to_interior(leaf, builder_);
        interior->set_eos(value);
        
        res.new_node = interior;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_char_to_leaf(
    ptr_t leaf, unsigned char c, const T& value) {
    using ops = trie_ops<T, THREADED, Allocator, FIXED_LEN>;
    auto helper_res = ops::template add_entry<false, true>(leaf, c, value, builder_, static_cast<void*>(nullptr));
    
    insert_result res;
    res.new_node = helper_res.new_node;
    if (helper_res.old_node) res.old_nodes.push_back(helper_res.old_node);
    res.inserted = helper_res.success;
    res.in_place = helper_res.in_place;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::demote_leaf_multi(
    ptr_t leaf, std::string_view key, const T& value) {
    insert_result res;
    unsigned char first_c = static_cast<unsigned char>(key[0]);
    bool existing = leaf->has_leaf_entry(first_c);
    
    using ops = trie_ops<T, THREADED, Allocator, FIXED_LEN>;
    
    // Create new child for the value we're inserting (if not matching existing)
    ptr_t new_child = existing ? nullptr : create_leaf_for_key(key.substr(1), value);
    
    // Convert leaf to interior, adding new child if needed
    ptr_t interior = ops::leaf_to_interior(leaf, builder_, 
                                            existing ? 0 : first_c, 
                                            new_child);
    
    if (existing) {
        // Recurse into the existing child
        ptr_t child = interior->get_child(first_c);
        atomic_ptr* child_slot = interior->get_child_slot(first_c);
        auto child_res = insert_impl(child_slot, child, key.substr(1), value);
        if (child_res.new_node) {
            child_slot->store(child_res.new_node);
        }
        for (auto* old : child_res.old_nodes) res.old_nodes.push_back(old);
    }
    
    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::split_interior(
    ptr_t n, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = n->skip_str();

    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);

    // 2 children -> BINARY
    ptr_t new_int = builder_.make_interior_binary(common);
    ptr_t old_child = clone_interior_with_skip(n, old_skip.substr(m + 1));
    ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
    new_int->template as_binary<false>()->add_child(old_c, old_child);
    new_int->template as_binary<false>()->add_child(new_c, new_child);
    new_int->template as_binary<false>()->update_capacity_flags();

    res.new_node = new_int;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::clone_interior_with_skip(ptr_t n, std::string_view new_skip) {
    using ops = trie_ops<T, THREADED, Allocator, FIXED_LEN>;
    return ops::clone_interior_with_skip(n, new_skip, builder_);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_interior(
    ptr_t n, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = n->skip_str();

    // 1 child + EOS (FIXED_LEN==0) = 2 entries -> BINARY
    ptr_t new_int = builder_.make_interior_binary(std::string(key));
    if constexpr (FIXED_LEN == 0) {
        new_int->set_eos(value);
    }

    ptr_t old_child = clone_interior_with_skip(n, old_skip.substr(m + 1));
    new_int->template as_binary<false>()->add_child(static_cast<unsigned char>(old_skip[m]), old_child);
    new_int->template as_binary<false>()->update_capacity_flags();

    res.new_node = new_int;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::set_interior_eos(ptr_t n, const T& value) {
    insert_result res;
    
    if constexpr (FIXED_LEN > 0) {
        return res;
    } else {
        if (n->has_eos()) return res;
        n->bump_version();
        n->set_eos(value);
        res.in_place = true;
        res.inserted = true;
        return res;
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_child_to_interior(
    ptr_t n, unsigned char c, std::string_view remaining, const T& value) {
    using ops = trie_ops<T, THREADED, Allocator, FIXED_LEN>;
    ptr_t child = create_leaf_for_key(remaining, value);
    auto helper_res = ops::template add_entry<false, false>(n, c, child, builder_, static_cast<void*>(nullptr));
    
    insert_result res;
    res.new_node = helper_res.new_node;
    if (helper_res.old_node) res.old_nodes.push_back(helper_res.old_node);
    res.inserted = helper_res.success;
    res.in_place = helper_res.in_place;
    return res;
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_insert_probe.h"
