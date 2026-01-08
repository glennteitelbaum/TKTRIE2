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
    std::string_view leaf_skip = leaf->skip_str();

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
    std::string_view old_skip = leaf->skip_str();

    ptr_t interior = builder_.make_interior_list(std::string(key));
    if constexpr (FIXED_LEN == 0) {
        interior->set_eos(value);
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
    std::string_view old_skip = leaf->skip_str();

    ptr_t interior = builder_.make_interior_list(std::string(old_skip));
    if constexpr (FIXED_LEN == 0) {
        T old_value;
        leaf->as_skip()->value.try_read(old_value);
        interior->set_eos(old_value);
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
    std::string_view old_skip = leaf->skip_str();

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
    std::string_view old_skip = leaf->skip_str();

    ptr_t interior = builder_.make_interior_list(std::string(key));
    if constexpr (FIXED_LEN == 0) {
        interior->set_eos(value);
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
        leaf->template as_list<true>()->copy_values_to(n->template as_list<true>());
        return n;
    }
    ptr_t n = builder_.make_leaf_full(new_skip);
    leaf->template as_full<true>()->copy_values_to(n->template as_full<true>());
    return n;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_eos_to_leaf_list(ptr_t leaf, const T& value) {
    insert_result res;
    
    if constexpr (FIXED_LEN > 0) {
        return res;
    } else {
        std::string_view leaf_skip = leaf->skip_str();

        if (leaf->is_list()) [[likely]] {
            auto* src = leaf->template as_list<true>();
            ptr_t interior = builder_.make_interior_list(leaf_skip);
            interior->set_eos(value);
            int cnt = src->count();
            for (int i = 0; i < cnt; ++i) {
                unsigned char c = src->chars.char_at(i);
                T val;
                src->values[i].try_read(val);
                ptr_t child = builder_.make_leaf_skip("", val);
                interior->template as_list<false>()->add_child(c, child);
            }
            res.new_node = interior;
        } else {
            auto* src = leaf->template as_full<true>();
            ptr_t interior = builder_.make_interior_full(leaf_skip);
            interior->set_eos(value);
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
        if (ln->has(c)) return res;

        if (ln->count() < LIST_MAX) {
            ln->add_value(c, value);
            res.in_place = true;
            res.inserted = true;
            return res;
        }

        ptr_t full = builder_.make_leaf_full(leaf->skip_str());
        auto* fn = full->template as_full<true>();
        [[assume(ln->count() == 7)]];  // Must be LIST_MAX to reach here
        for (int i = 0; i < ln->count(); ++i) {
            unsigned char ch = ln->chars.char_at(i);
            T val;
            ln->values[i].try_read(val);
            fn->add_value(ch, val);
        }
        fn->add_value(c, value);

        res.new_node = full;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }

    auto* fn = leaf->template as_full<true>();
    if (fn->has(c)) return res;
    fn->add_value_atomic(c, value);
    res.in_place = true;
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::demote_leaf_list(
    ptr_t leaf, std::string_view key, const T& value) {
    insert_result res;
    std::string_view leaf_skip = leaf->skip_str();
    unsigned char first_c = static_cast<unsigned char>(key[0]);

    if (leaf->is_list()) [[likely]] {
        auto* src = leaf->template as_list<true>();
        int leaf_count = src->count();
        [[assume(leaf_count >= 0 && leaf_count <= 7)]];
        int existing_idx = src->chars.find(first_c);
        bool need_pop = (existing_idx < 0) && (leaf_count >= LIST_MAX);
        
        if (need_pop) {
            // LIST at capacity -> convert to POP
            ptr_t interior = builder_.make_interior_pop(leaf_skip);
            auto* dst = interior->as_pop();
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

        if (dst->has(first_c)) {
            ptr_t child = dst->get_child(first_c);
            auto child_res = insert_impl(dst->get_child_slot(first_c), child, key.substr(1), value);
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
    std::string_view old_skip = n->skip_str();

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
    if (n->is_pop()) {
        ptr_t clone = builder_.make_interior_pop(new_skip);
        n->as_pop()->move_children_to(clone->as_pop());
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
    std::string_view old_skip = n->skip_str();

    ptr_t new_int = builder_.make_interior_list(std::string(key));
    if constexpr (FIXED_LEN == 0) {
        new_int->set_eos(value);
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
        return res;
    } else {
        if (n->has_eos()) return res;
        n->set_eos(value);
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
        if (ln->count() < LIST_MAX) {
            ln->add_child(c, child);
            res.in_place = true;
            res.inserted = true;
            return res;
        }
        // LIST at capacity -> convert to POP
        ptr_t pop = builder_.make_interior_pop(n->skip_str());
        auto* pn = pop->as_pop();
        // Copy children from list to pop
        for (int i = 0; i < ln->count(); ++i) {
            unsigned char ch = ln->chars.char_at(i);
            pn->add_child(ch, ln->children[i].load());
            ln->children[i].store(nullptr);
        }
        if constexpr (FIXED_LEN == 0) {
            // Move EOS if present
            T eos_val;
            if (ln->eos.try_read(eos_val)) {
                // POP doesn't have EOS - need FULL instead
                ptr_t full = builder_.make_interior_full(n->skip_str());
                auto* fn = full->template as_full<false>();
                pn->move_children_to_full(fn);
                fn->eos.set(eos_val);
                fn->add_child(c, child);
                builder_.delete_node(pop);
                res.new_node = full;
                res.old_nodes.push_back(n);
                res.inserted = true;
                return res;
            }
        }
        pn->add_child(c, child);
        res.new_node = pop;
        res.old_nodes.push_back(n);
        res.inserted = true;
        return res;
    }

    if (n->is_pop()) {
        auto* pn = n->as_pop();
        if (pn->count() < POP_MAX) {
            pn->add_child(c, child);
            res.in_place = true;
            res.inserted = true;
            return res;
        }
        // POP at capacity -> convert to FULL
        ptr_t full = builder_.make_interior_full(n->skip_str());
        pn->move_children_to_full(full->template as_full<false>());
        full->template as_full<false>()->add_child(c, child);
        res.new_node = full;
        res.old_nodes.push_back(n);
        res.inserted = true;
        return res;
    }

    if (n->is_full()) {
        n->template as_full<false>()->add_child_atomic(c, child);
        res.in_place = true;
        res.inserted = true;
        return res;
    }

    ptr_t list = builder_.make_interior_list(n->skip_str());
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
