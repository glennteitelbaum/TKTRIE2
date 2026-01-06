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
    std::string_view leaf_skip = get_skip(leaf);

    if (leaf->is_skip()) {
        size_t m = match_skip_impl(leaf_skip, key);
        if ((m == leaf_skip.size()) & (m == key.size())) return res;
        if ((m < leaf_skip.size()) & (m < key.size())) return split_leaf_skip(leaf, key, value, m);
        if (m == key.size()) return prefix_leaf_skip(leaf, key, value, m);
        return extend_leaf_skip(leaf, key, value, m);
    }

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
    if (child && !builder_t::is_sentinel(child)) {
        atomic_ptr* child_slot = get_child_slot(n, c);
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
    std::string_view old_skip = leaf->as_skip()->skip.view();

    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);

    ptr_t interior = builder_.make_interior_list(common);
    T old_value;
    leaf->as_skip()->value.try_read(old_value);
    ptr_t old_child = builder_.make_leaf_skip(old_skip.substr(m + 1), old_value);
    ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
    interior->template as_list<false>()->add_two_children(old_c, old_child, new_c, new_child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->as_skip()->skip.view();

    ptr_t interior = builder_.make_interior_list(std::string(key));
    if constexpr (FIXED_LEN == 0) {
        interior->template as_list<false>()->eos.set(value);
    }

    T old_value;
    leaf->as_skip()->value.try_read(old_value);
    ptr_t child = builder_.make_leaf_skip(old_skip.substr(m + 1), old_value);
    interior->template as_list<false>()->add_child(static_cast<unsigned char>(old_skip[m]), child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::extend_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->as_skip()->skip.view();

    ptr_t interior = builder_.make_interior_list(std::string(old_skip));
    if constexpr (FIXED_LEN == 0) {
        T old_value;
        leaf->as_skip()->value.try_read(old_value);
        interior->template as_list<false>()->eos.set(old_value);
    }

    ptr_t child = create_leaf_for_key(key.substr(m + 1), value);
    interior->template as_list<false>()->add_child(static_cast<unsigned char>(key[m]), child);

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
    interior->template as_list<false>()->add_two_children(old_c, old_child, new_c, new_child);

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
    if constexpr (FIXED_LEN == 0) {
        interior->template as_list<false>()->eos.set(value);
    }

    ptr_t old_child = clone_leaf_with_skip(leaf, old_skip.substr(m + 1));
    interior->template as_list<false>()->add_child(static_cast<unsigned char>(old_skip[m]), old_child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::clone_leaf_with_skip(ptr_t leaf, std::string_view new_skip) {
    if (leaf->is_list()) {
        ptr_t n = builder_.make_leaf_list(new_skip);
        auto* src = leaf->template as_list<true>();
        auto* dst = n->template as_list<true>();
        dst->chars = src->chars;
        int cnt = src->chars.count();
        for (int i = 0; i < cnt; ++i) {
            dst->values[i].deep_copy_from(src->values[i]);
        }
        return n;
    }
    ptr_t n = builder_.make_leaf_full(new_skip);
    auto* src = leaf->template as_full<true>();
    auto* dst = n->template as_full<true>();
    dst->valid = src->valid;
    src->valid.for_each_set([src, dst](unsigned char c) {
        dst->values[c].deep_copy_from(src->values[c]);
    });
    return n;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_eos_to_leaf_list(ptr_t leaf, const T& value) {
    insert_result res;
    
    if constexpr (FIXED_LEN > 0) {
        // Fixed-length keys can't have EOS - this shouldn't happen
        return res;
    } else {
        std::string_view leaf_skip = get_skip(leaf);

        if (leaf->is_list()) [[likely]] {
            ptr_t interior = builder_.make_interior_list(leaf_skip);
            interior->template as_list<false>()->eos.set(value);
            auto* src = leaf->template as_list<true>();
            int cnt = src->chars.count();
            for (int i = 0; i < cnt; ++i) {
                unsigned char c = src->chars.char_at(i);
                T val;
                src->values[i].try_read(val);
                ptr_t child = builder_.make_leaf_skip("", val);
                interior->template as_list<false>()->add_child(c, child);
            }
            res.new_node = interior;
        } else {
            ptr_t interior = builder_.make_interior_full(leaf_skip);
            interior->template as_full<false>()->eos.set(value);
            auto* src = leaf->template as_full<true>();
            src->valid.for_each_set([this, src, interior](unsigned char c) {
                T val;
                src->values[c].try_read(val);
                ptr_t child = builder_.make_leaf_skip("", val);
                interior->template as_full<false>()->add_child(c, child);
            });
            res.new_node = interior;
        }

        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_char_to_leaf(
    ptr_t leaf, unsigned char c, const T& value) {
    insert_result res;

    if (leaf->is_list()) {
        auto* ln = leaf->template as_list<true>();
        if (ln->chars.find(c) >= 0) return res;

        if (ln->chars.count() < LIST_MAX) {
            ln->add_entry(c, value);
            res.in_place = true;
            res.inserted = true;
            return res;
        }

        ptr_t full = builder_.make_leaf_full(ln->skip.view());
        auto* fn = full->template as_full<true>();
        for (int i = 0; i < ln->chars.count(); ++i) {
            unsigned char ch = ln->chars.char_at(i);
            T val;
            ln->values[i].try_read(val);
            fn->add_entry(ch, val);
        }
        fn->add_entry(c, value);

        res.new_node = full;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }

    auto* fn = leaf->template as_full<true>();
    if (fn->valid.template atomic_test<THREADED>(c)) return res;
    fn->template add_entry_atomic<THREADED>(c, value);
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

    if (leaf->is_list()) [[likely]] {
        auto* src = leaf->template as_list<true>();
        int leaf_count = src->chars.count();
        int existing_idx = src->chars.find(first_c);
        bool need_full = (existing_idx < 0) && (leaf_count >= LIST_MAX);
        
        if (need_full) {
            ptr_t interior = builder_.make_interior_full(leaf_skip);
            auto* dst = interior->template as_full<false>();
            for (int i = 0; i < leaf_count; ++i) {
                unsigned char c = src->chars.char_at(i);
                T val;
                src->values[i].try_read(val);
                ptr_t child = builder_.make_leaf_skip("", val);
                dst->add_child(c, child);
            }
            ptr_t child = create_leaf_for_key(key.substr(1), value);
            dst->add_child(first_c, child);
            res.new_node = interior;
        } else {
            ptr_t interior = builder_.make_interior_list(leaf_skip);
            auto* dst = interior->template as_list<false>();
            for (int i = 0; i < leaf_count; ++i) {
                unsigned char c = src->chars.char_at(i);
                T val;
                src->values[i].try_read(val);
                ptr_t child = builder_.make_leaf_skip("", val);
                dst->add_child(c, child);
            }

            if (existing_idx >= 0) {
                ptr_t child = dst->children[existing_idx].load();
                auto child_res = insert_impl(&dst->children[existing_idx], child, key.substr(1), value);
                if (child_res.new_node) {
                    dst->children[existing_idx].store(child_res.new_node);
                }
                for (auto* old : child_res.old_nodes) res.old_nodes.push_back(old);
            } else {
                ptr_t child = create_leaf_for_key(key.substr(1), value);
                dst->add_child(first_c, child);
            }
            res.new_node = interior;
        }
    } else {
        auto* src = leaf->template as_full<true>();
        ptr_t interior = builder_.make_interior_full(leaf_skip);
        auto* dst = interior->template as_full<false>();
        src->valid.for_each_set([this, src, dst](unsigned char c) {
            T val;
            src->values[c].try_read(val);
            ptr_t child = builder_.make_leaf_skip("", val);
            dst->add_child(c, child);
        });

        if (dst->valid.template atomic_test<THREADED>(first_c)) {
            ptr_t child = dst->children[first_c].load();
            auto child_res = insert_impl(&dst->children[first_c], child, key.substr(1), value);
            if (child_res.new_node) {
                dst->children[first_c].store(child_res.new_node);
            }
            for (auto* old : child_res.old_nodes) res.old_nodes.push_back(old);
        } else {
            ptr_t child = create_leaf_for_key(key.substr(1), value);
            dst->add_child(first_c, child);
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
    new_int->template as_list<false>()->add_two_children(old_c, old_child, new_c, new_child);

    res.new_node = new_int;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::clone_interior_with_skip(ptr_t n, std::string_view new_skip) {
    if (n->is_list()) [[likely]] {
        ptr_t clone = builder_.make_interior_list(new_skip);
        n->template as_list<false>()->move_interior_to(clone->template as_list<false>());
        return clone;
    }
    ptr_t clone = builder_.make_interior_full(new_skip);
    n->template as_full<false>()->move_interior_to(clone->template as_full<false>());
    return clone;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_interior(
    ptr_t n, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = get_skip(n);

    ptr_t new_int = builder_.make_interior_list(std::string(key));
    if constexpr (FIXED_LEN == 0) {
        new_int->template as_list<false>()->eos.set(value);
    }

    ptr_t old_child = clone_interior_with_skip(n, old_skip.substr(m + 1));
    new_int->template as_list<false>()->add_child(static_cast<unsigned char>(old_skip[m]), old_child);

    res.new_node = new_int;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::set_interior_eos(ptr_t n, const T& value) {
    insert_result res;
    
    if constexpr (FIXED_LEN > 0) {
        // Fixed-length keys can't have EOS
        return res;
    } else {
        if (has_eos(n)) return res;
        set_eos(n, value);
        res.in_place = true;
        res.inserted = true;
        return res;
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_child_to_interior(
    ptr_t n, unsigned char c, std::string_view remaining, const T& value) {
    insert_result res;
    ptr_t child = create_leaf_for_key(remaining, value);

    if (n->is_list()) {
        auto* ln = n->template as_list<false>();
        if (ln->chars.count() < LIST_MAX) {
            ln->add_child(c, child);
            res.in_place = true;
            res.inserted = true;
            return res;
        }
        ptr_t full = builder_.make_interior_full(ln->skip.view());
        ln->move_interior_to_full(full->template as_full<false>());
        full->template as_full<false>()->add_child(c, child);

        res.new_node = full;
        res.old_nodes.push_back(n);
        res.inserted = true;
        return res;
    }

    if (n->is_full()) {
        n->template as_full<false>()->template add_child_atomic<THREADED>(c, child);
        res.in_place = true;
        res.inserted = true;
        return res;
    }

    // Fallback - shouldn't normally reach here
    ptr_t list = builder_.make_interior_list(get_skip(n));
    list->template as_list<false>()->add_child(c, child);

    res.new_node = list;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_insert_probe.h"
