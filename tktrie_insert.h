#pragma once

// This file contains implementation details for tktrie (insert operations)
// It should only be included from tktrie_core.h

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

    if (!n) {
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
    std::string_view leaf_skip = get_skip(leaf);

    if (leaf->is_eos()) {
        if (key.empty()) return res;
        return demote_leaf_eos(leaf, key, value);
    }

    if (leaf->is_skip()) {
        size_t m = match_skip_impl(leaf_skip, key);
        if ((m == leaf_skip.size()) & (m == key.size())) return res;
        if ((m < leaf_skip.size()) & (m < key.size())) return split_leaf_skip(leaf, key, value, m);
        if (m == key.size()) return prefix_leaf_skip(leaf, key, value, m);
        return extend_leaf_skip(leaf, key, value, m);
    }

    // LIST or FULL
    size_t m = match_skip_impl(leaf_skip, key);
    if ((m < leaf_skip.size()) & (m < key.size())) return split_leaf_list(leaf, key, value, m);
    if (m < leaf_skip.size()) return prefix_leaf_list(leaf, key, value, m);
    key.remove_prefix(m);

    if (key.empty()) return add_eos_to_leaf_list(leaf, value);
    if (key.size() == 1) {
        unsigned char c = static_cast<unsigned char>(key[0]);
        return add_char_to_leaf(leaf, c, value);
    }
    return demote_leaf_list(leaf, key, value);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::insert_into_interior(
    atomic_ptr*, ptr_t n, std::string_view key, const T& value) {
    insert_result res;
    std::string_view skip = get_skip(n);

    size_t m = match_skip_impl(skip, key);
    if ((m < skip.size()) & (m < key.size())) return split_interior(n, key, value, m);
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

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::create_leaf_for_key(std::string_view key, const T& value) {
    if (key.empty()) return builder_.make_leaf_eos(value);
    if (key.size() == 1) {
        ptr_t leaf = builder_.make_leaf_list("");
        unsigned char c = static_cast<unsigned char>(key[0]);
        leaf->as_list()->chars.add(c);
        leaf->as_list()->construct_leaf_value(0, value);
        return leaf;
    }
    ptr_t leaf = builder_.make_leaf_list(key.substr(0, key.size() - 1));
    unsigned char c = static_cast<unsigned char>(key.back());
    leaf->as_list()->chars.add(c);
    leaf->as_list()->construct_leaf_value(0, value);
    return leaf;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::demote_leaf_eos(
    ptr_t leaf, std::string_view key, const T& value) {
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

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::split_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
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

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->as_skip()->skip;

    ptr_t interior = builder_.make_interior_list(std::string(key));
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

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::extend_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->as_skip()->skip;

    ptr_t interior = builder_.make_interior_list(std::string(old_skip));
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

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::split_leaf_list(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = get_skip(leaf);

    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);

    ptr_t interior = builder_.make_interior_list(common);
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

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_leaf_list(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = get_skip(leaf);

    ptr_t interior = builder_.make_interior_list(std::string(key));
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

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::clone_leaf_with_skip(ptr_t leaf, std::string_view new_skip) {
    if (leaf->is_list()) {
        ptr_t n = builder_.make_leaf_list(new_skip);
        n->as_list()->chars = leaf->as_list()->chars;
        int cnt = leaf->as_list()->chars.count();
        for (int i = 0; i < cnt; ++i) {
            n->as_list()->construct_leaf_value(i, leaf->as_list()->leaf_values[i]);
        }
        return n;
    }
    // FULL
    ptr_t n = builder_.make_leaf_full(new_skip);
    n->as_full()->valid = leaf->as_full()->valid;
    leaf->as_full()->valid.for_each_set([leaf, n](unsigned char c) {
        n->as_full()->construct_leaf_value(c, leaf->as_full()->leaf_values[c]);
    });
    return n;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_eos_to_leaf_list(ptr_t leaf, const T& value) {
    insert_result res;
    std::string_view leaf_skip = get_skip(leaf);

    if (leaf->is_list()) {
        ptr_t interior = builder_.make_interior_list(leaf_skip);
        interior->as_list()->eos_ptr = new T(value);
        int cnt = leaf->as_list()->chars.count();
        for (int i = 0; i < cnt; ++i) {
            unsigned char c = leaf->as_list()->chars.char_at(i);
            ptr_t child = builder_.make_leaf_eos(leaf->as_list()->leaf_values[i]);
            interior->as_list()->chars.add(c);
            interior->as_list()->children[i].store(child);
        }
        res.new_node = interior;
    } else {
        ptr_t interior = builder_.make_interior_full(leaf_skip);
        interior->as_full()->eos_ptr = new T(value);
        leaf->as_full()->valid.for_each_set([this, leaf, interior](unsigned char c) {
            ptr_t child = builder_.make_leaf_eos(leaf->as_full()->leaf_values[c]);
            interior->as_full()->valid.set(c);
            interior->as_full()->children[c].store(child);
        });
        res.new_node = interior;
    }

    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_char_to_leaf(
    ptr_t leaf, unsigned char c, const T& value) {
    insert_result res;

    if (leaf->is_list()) {
        int idx = leaf->as_list()->chars.find(c);
        if (idx >= 0) return res;

        if (leaf->as_list()->chars.count() < LIST_MAX) {
            idx = leaf->as_list()->chars.add(c);
            leaf->as_list()->construct_leaf_value(idx, value);
            res.in_place = true;
            res.inserted = true;
            return res;
        }

        ptr_t full = builder_.make_leaf_full(leaf->as_list()->skip);
        for (int i = 0; i < leaf->as_list()->chars.count(); ++i) {
            unsigned char ch = leaf->as_list()->chars.char_at(i);
            full->as_full()->valid.set(ch);
            full->as_full()->construct_leaf_value(ch, leaf->as_list()->leaf_values[i]);
        }
        full->as_full()->valid.set(c);
        full->as_full()->construct_leaf_value(c, value);

        res.new_node = full;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }

    // FULL
    if (leaf->as_full()->valid.test(c)) return res;
    leaf->as_full()->valid.template atomic_set<THREADED>(c);
    leaf->as_full()->construct_leaf_value(c, value);
    res.in_place = true;
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::demote_leaf_list(
    ptr_t leaf, std::string_view key, const T& value) {
    insert_result res;
    std::string_view leaf_skip = get_skip(leaf);
    unsigned char first_c = static_cast<unsigned char>(key[0]);

    if (leaf->is_list()) {
        int leaf_count = leaf->as_list()->chars.count();
        int existing_idx = leaf->as_list()->chars.find(first_c);
        bool need_full = (existing_idx < 0) && (leaf_count >= LIST_MAX);
        
        if (need_full) {
            ptr_t interior = builder_.make_interior_full(leaf_skip);
            for (int i = 0; i < leaf_count; ++i) {
                unsigned char c = leaf->as_list()->chars.char_at(i);
                ptr_t child = builder_.make_leaf_eos(leaf->as_list()->leaf_values[i]);
                interior->as_full()->valid.set(c);
                interior->as_full()->children[c].store(child);
            }
            ptr_t child = create_leaf_for_key(key.substr(1), value);
            interior->as_full()->valid.set(first_c);
            interior->as_full()->children[first_c].store(child);
            res.new_node = interior;
        } else {
            ptr_t interior = builder_.make_interior_list(leaf_skip);
            for (int i = 0; i < leaf_count; ++i) {
                unsigned char c = leaf->as_list()->chars.char_at(i);
                ptr_t child = builder_.make_leaf_eos(leaf->as_list()->leaf_values[i]);
                interior->as_list()->chars.add(c);
                interior->as_list()->children[i].store(child);
            }

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
        }
    } else {
        ptr_t interior = builder_.make_interior_full(leaf_skip);
        leaf->as_full()->valid.for_each_set([this, leaf, interior](unsigned char c) {
            ptr_t child = builder_.make_leaf_eos(leaf->as_full()->leaf_values[c]);
            interior->as_full()->valid.set(c);
            interior->as_full()->children[c].store(child);
        });

        if (interior->as_full()->valid.test(first_c)) {
            ptr_t child = interior->as_full()->children[first_c].load();
            auto child_res = insert_impl(&interior->as_full()->children[first_c], child, key.substr(1), value);
            if (child_res.new_node) {
                interior->as_full()->children[first_c].store(child_res.new_node);
            }
            for (auto* old : child_res.old_nodes) res.old_nodes.push_back(old);
        } else {
            ptr_t child = create_leaf_for_key(key.substr(1), value);
            interior->as_full()->valid.set(first_c);
            interior->as_full()->children[first_c].store(child);
        }
        res.new_node = interior;
    }

    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::split_interior(
    ptr_t n, std::string_view key, const T& value, size_t m) {
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

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::clone_interior_with_skip(ptr_t n, std::string_view new_skip) {
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

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_interior(
    ptr_t n, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = get_skip(n);

    ptr_t new_int = builder_.make_interior_list(std::string(key));
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

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::set_interior_eos(ptr_t n, const T& value) {
    insert_result res;
    T* p = get_eos_ptr(n);
    if (p) return res;

    set_eos_ptr(n, new T(value));
    res.in_place = true;
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_child_to_interior(
    ptr_t n, unsigned char c, std::string_view remaining, const T& value) {
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

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_insert_probe.h"
