#pragma once

// This file contains implementation details for tktrie (speculative insert probing and commit)
// It should only be included from tktrie_insert.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

// -----------------------------------------------------------------------------
// Speculative insert operations  
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::speculative_info TKTRIE_CLASS::probe_leaf_speculative(
    ptr_t n, std::string_view key, speculative_info& info) const noexcept {
    std::string_view skip = get_skip(n);
    size_t m = match_skip_impl(skip, key);

    if (n->is_eos()) {
        if (key.empty()) { info.op = spec_op::EXISTS; return info; }
        info.op = spec_op::DEMOTE_LEAF_EOS;
        info.target = n;
        info.target_version = n->version();
        info.remaining_key = std::string(key);
        return info;
    }

    if (n->is_skip()) {
        if ((m == skip.size()) & (m == key.size())) { info.op = spec_op::EXISTS; return info; }
        info.target = n;
        info.target_version = n->version();
        info.target_skip = std::string(skip);
        info.match_pos = m;

        if ((m < skip.size()) & (m < key.size())) { info.op = spec_op::SPLIT_LEAF_SKIP; }
        else if (m == key.size()) { info.op = spec_op::PREFIX_LEAF_SKIP; }
        else { info.op = spec_op::EXTEND_LEAF_SKIP; }
        info.remaining_key = std::string(key);
        return info;
    }

    // LIST or FULL leaf
    info.target = n;
    info.target_version = n->version();
    info.target_skip = std::string(skip);

    if ((m < skip.size()) & (m < key.size())) {
        info.op = spec_op::SPLIT_LEAF_LIST;
        info.match_pos = m;
        info.remaining_key = std::string(key);
        return info;
    }
    if (m < skip.size()) {
        info.op = spec_op::PREFIX_LEAF_LIST;
        info.match_pos = m;
        info.remaining_key = std::string(key);
        return info;
    }
    key.remove_prefix(m);
    info.remaining_key = std::string(key);

    if (key.empty()) { info.op = spec_op::ADD_EOS_LEAF_LIST; return info; }
    if (key.size() != 1) { info.op = spec_op::DEMOTE_LEAF_LIST; return info; }

    unsigned char c = static_cast<unsigned char>(key[0]);
    info.c = c;

    if (n->is_list()) {
        if (n->as_list()->chars.find(c) >= 0) { info.op = spec_op::EXISTS; return info; }
        info.op = (n->as_list()->chars.count() < LIST_MAX) ? spec_op::IN_PLACE_LEAF : spec_op::LIST_TO_FULL_LEAF;
        return info;
    }
    // FULL
    info.op = n->as_full()->valid.test(c) ? spec_op::EXISTS : spec_op::IN_PLACE_LEAF;
    return info;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::speculative_info TKTRIE_CLASS::probe_speculative(
    ptr_t n, std::string_view key) const noexcept {
    speculative_info info;
    info.remaining_key = std::string(key);

    if (!n) {
        info.op = spec_op::EMPTY_TREE;
        return info;
    }

    info.path[info.path_len++] = {n, n->version(), 0};

    // Loop only on interior nodes
    while (!n->is_leaf()) {
        std::string_view skip = get_skip(n);
        size_t m = match_skip_impl(skip, key);

        if ((m < skip.size()) & (m < key.size())) {
            info.op = spec_op::SPLIT_INTERIOR;
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            info.match_pos = m;
            info.remaining_key = std::string(key);
            return info;
        }
        if (m < skip.size()) {
            info.op = spec_op::PREFIX_INTERIOR;
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            info.match_pos = m;
            info.remaining_key = std::string(key);
            return info;
        }
        key.remove_prefix(m);

        if (key.empty()) {
            T* p = get_eos_ptr(n);
            if (p) { info.op = spec_op::EXISTS; return info; }
            info.op = spec_op::IN_PLACE_INTERIOR;
            info.target = n;
            info.target_version = n->version();
            info.is_eos = true;
            return info;
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = find_child(n, c);

        if (!child) {
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            info.c = c;
            info.remaining_key = std::string(key.substr(1));

            if ((n->is_list() && n->as_list()->chars.count() < LIST_MAX) | n->is_full()) {
                info.op = spec_op::IN_PLACE_INTERIOR;
            } else {
                info.op = spec_op::ADD_CHILD_CONVERT;
            }
            return info;
        }

        key.remove_prefix(1);
        n = child;
        if (info.path_len < speculative_info::MAX_PATH) {
            info.path[info.path_len++] = {n, n->version(), c};
        }
    }

    // n is now a leaf
    return probe_leaf_speculative(n, key, info);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::pre_alloc TKTRIE_CLASS::allocate_speculative(
    const speculative_info& info, const T& value) {
    pre_alloc alloc;
    std::string_view key = info.remaining_key;
    std::string_view skip = info.target_skip;
    size_t m = info.match_pos;

    switch (info.op) {
    case spec_op::EMPTY_TREE: {
        alloc.root_replacement = create_leaf_for_key(key, value);
        alloc.add(alloc.root_replacement);
        break;
    }
    case spec_op::DEMOTE_LEAF_EOS: {
        ptr_t interior = builder_.make_interior_list("");
        interior->as_list()->eos_ptr = new T();
        ptr_t child = create_leaf_for_key(key.substr(1), value);
        interior->as_list()->add_child(static_cast<unsigned char>(key[0]), child);
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::SPLIT_LEAF_SKIP: {
        std::string common(skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);

        ptr_t interior = builder_.make_interior_list(common);
        ptr_t old_child = builder_.make_leaf_skip(skip.substr(m + 1), T{});
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        interior->as_list()->add_two_children(old_c, old_child, new_c, new_child);

        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(old_child);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_LEAF_SKIP: {
        ptr_t interior = builder_.make_interior_list(std::string(key));
        interior->as_list()->eos_ptr = new T(value);
        ptr_t child = builder_.make_leaf_skip(skip.substr(m + 1), T{});
        interior->as_list()->add_child(static_cast<unsigned char>(skip[m]), child);

        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::EXTEND_LEAF_SKIP: {
        ptr_t interior = builder_.make_interior_list(std::string(skip));
        interior->as_list()->eos_ptr = new T();
        ptr_t child = create_leaf_for_key(key.substr(m + 1), value);
        interior->as_list()->add_child(static_cast<unsigned char>(key[m]), child);

        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::SPLIT_LEAF_LIST: {
        std::string common(skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);

        ptr_t interior = builder_.make_interior_list(common);
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        // Note: children[0] will be set during commit with cloned old_child
        interior->as_list()->chars.add(old_c);
        interior->as_list()->chars.add(new_c);
        interior->as_list()->children[1].store(new_child);

        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_LEAF_LIST: {
        ptr_t interior = builder_.make_interior_list(std::string(key));
        interior->as_list()->eos_ptr = new T(value);
        // Note: child[0] will be set during commit with cloned old_child
        interior->as_list()->chars.add(static_cast<unsigned char>(skip[m]));

        alloc.root_replacement = interior;
        alloc.add(interior);
        break;
    }
    case spec_op::LIST_TO_FULL_LEAF: {
        ptr_t full = builder_.make_leaf_full(std::string(skip));
        full->as_full()->valid.set(info.c);
        full->as_full()->construct_leaf_value(info.c, value);

        alloc.root_replacement = full;
        alloc.add(full);
        break;
    }
    case spec_op::IN_PLACE_INTERIOR: {
        if (info.is_eos) {
            alloc.in_place_eos = new T(value);
        } else {
            ptr_t child = create_leaf_for_key(info.remaining_key, value);
            alloc.root_replacement = child;
            alloc.add(child);
        }
        break;
    }
    case spec_op::ADD_EOS_LEAF_LIST:
    case spec_op::DEMOTE_LEAF_LIST:
    case spec_op::ADD_CHILD_CONVERT:
    case spec_op::SPLIT_INTERIOR:
    case spec_op::PREFIX_INTERIOR:
        break;
    default:
        break;
    }

    return alloc;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::validate_path(const speculative_info& info) const noexcept {
    for (int i = 0; i < info.path_len; ++i) {
        if (info.path[i].node->version() != info.path[i].version) {
            return false;
        }
    }
    if (info.target && (info.path_len == 0 || info.path[info.path_len-1].node != info.target)) {
        if (info.target->version() != info.target_version) {
            return false;
        }
    }
    return true;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::atomic_ptr* TKTRIE_CLASS::find_slot_for_commit(
    const speculative_info& info) noexcept {
    if (info.path_len <= 1) return &root_;
    ptr_t parent = info.path[info.path_len - 2].node;
    unsigned char edge = info.path[info.path_len - 1].edge;
    return get_child_slot(parent, edge);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::atomic_ptr* TKTRIE_CLASS::get_verified_slot(
    const speculative_info& info) noexcept {
    atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
    return (slot->load() == info.target) ? slot : nullptr;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::commit_to_slot(atomic_ptr* slot, ptr_t new_node, 
                                   const speculative_info& info) noexcept {
    if (info.path_len > 1) info.path[info.path_len - 2].node->bump_version();
    slot->store(new_node);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::commit_speculative(
    speculative_info& info, pre_alloc& alloc, const T& value) {
    switch (info.op) {
    case spec_op::EMPTY_TREE:
        if (root_.load() != nullptr) return false;
        root_.store(alloc.root_replacement);
        return true;

    case spec_op::DEMOTE_LEAF_EOS: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        *alloc.root_replacement->as_list()->eos_ptr = info.target->as_eos()->leaf_value;
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    case spec_op::SPLIT_LEAF_SKIP: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        ptr_t old_child = alloc.root_replacement->as_list()->children[0].load();
        old_child->as_skip()->leaf_value = info.target->as_skip()->leaf_value;
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    case spec_op::PREFIX_LEAF_SKIP: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        ptr_t child = alloc.root_replacement->as_list()->children[0].load();
        child->as_skip()->leaf_value = info.target->as_skip()->leaf_value;
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    case spec_op::EXTEND_LEAF_SKIP: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        *alloc.root_replacement->as_list()->eos_ptr = info.target->as_skip()->leaf_value;
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    case spec_op::SPLIT_LEAF_LIST:
    case spec_op::PREFIX_LEAF_LIST: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        std::string_view skip = info.target_skip;
        ptr_t old_child = clone_leaf_with_skip(info.target, skip.substr(info.match_pos + 1));
        alloc.root_replacement->as_list()->children[0].store(old_child);
        alloc.add(old_child);
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    case spec_op::LIST_TO_FULL_LEAF: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        auto* list = info.target->as_list();
        for (int i = 0; i < list->chars.count(); ++i) {
            unsigned char ch = list->chars.char_at(i);
            alloc.root_replacement->as_full()->valid.set(ch);
            alloc.root_replacement->as_full()->construct_leaf_value(ch, list->leaf_values[i]);
        }
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    case spec_op::IN_PLACE_LEAF: {
        ptr_t n = info.target;
        unsigned char c = info.c;
        if (n->version() != info.target_version) return false;
        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        if (!slot | (slot->load() != n)) return false;

        if (n->is_list()) {
            if (n->as_list()->chars.find(c) >= 0) return false;
            if (n->as_list()->chars.count() >= LIST_MAX) return false;
            n->bump_version();
            n->as_list()->add_leaf_entry(c, value);
            if (alloc.root_replacement) {
                delete alloc.root_replacement->as_eos();
                alloc.clear();
            }
            return true;
        }
        // FULL
        if (n->as_full()->valid.test(c)) return false;
        n->bump_version();
        n->as_full()->valid.template atomic_set<THREADED>(c);
        n->as_full()->construct_leaf_value(c, value);
        return true;
    }
    case spec_op::IN_PLACE_INTERIOR: {
        ptr_t n = info.target;
        if (n->version() != info.target_version) {
            if (alloc.in_place_eos) { delete alloc.in_place_eos; alloc.in_place_eos = nullptr; }
            return false;
        }
        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        if (!slot | (slot->load() != n)) {
            if (alloc.in_place_eos) { delete alloc.in_place_eos; alloc.in_place_eos = nullptr; }
            return false;
        }

        if (info.is_eos) {
            if (get_eos_ptr(n)) { delete alloc.in_place_eos; return false; }
            n->bump_version();
            set_eos_ptr(n, alloc.in_place_eos);
            alloc.in_place_eos = nullptr;
            return true;
        }

        unsigned char c = info.c;
        ptr_t child = alloc.root_replacement;

        if (n->is_list()) {
            if (n->as_list()->chars.find(c) >= 0) return false;
            if (n->as_list()->chars.count() >= LIST_MAX) return false;
            n->bump_version();
            n->as_list()->add_child(c, child);
            return true;
        }
        if (n->is_full()) {
            if (n->as_full()->valid.test(c)) return false;
            n->bump_version();
            n->as_full()->template add_child_atomic<THREADED>(c, child);
            return true;
        }
        return false;
    }
    case spec_op::ADD_EOS_LEAF_LIST:
    case spec_op::DEMOTE_LEAF_LIST:
    case spec_op::SPLIT_INTERIOR:
    case spec_op::PREFIX_INTERIOR:
    case spec_op::ADD_CHILD_CONVERT:
        return false;
    default:
        return false;
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::dealloc_speculation(pre_alloc& alloc) {
    if (alloc.in_place_eos) {
        delete alloc.in_place_eos;
        alloc.in_place_eos = nullptr;
    }
    for (int i = 0; i < alloc.count; ++i) {
        ptr_t n = alloc.nodes[i];
        if (!n) continue;
        if (!n->is_leaf()) {
            T* eos = get_eos_ptr(n);
            if (eos) delete eos;
        }
        builder_t::delete_node(n);
    }
    alloc.clear();
}

TKTRIE_TEMPLATE
std::pair<typename TKTRIE_CLASS::iterator, bool> TKTRIE_CLASS::insert_locked(
    const Key& key, std::string_view kb, const T& value) {
    if constexpr (!THREADED) {
        std::lock_guard<mutex_t> lock(mutex_);

        ptr_t root = root_.load();
        auto res = insert_impl(&root_, root, kb, value);

        if (!res.inserted) {
            for (auto* old : res.old_nodes) retire_node(old);
            return {find(key), false};
        }

        if (res.new_node) root_.store(res.new_node);
        for (auto* old : res.old_nodes) retire_node(old);
        size_.fetch_add(1);

        return {iterator(this, std::string(kb), value), true};
    } else {
        ebr_global::instance().try_reclaim();
        auto& slot = get_ebr_slot();
        
        while (true) {
            auto guard = slot.get_guard();
            speculative_info spec = probe_speculative(root_.load(), kb);

            if (spec.op == spec_op::EXISTS) {
                return {find(key), false};
            }

            if ((spec.op == spec_op::IN_PLACE_LEAF) | (spec.op == spec_op::IN_PLACE_INTERIOR)) {
                pre_alloc alloc = allocate_speculative(spec, value);
                {
                    std::lock_guard<mutex_t> lock(mutex_);
                    if (!validate_path(spec)) { dealloc_speculation(alloc); continue; }
                    if (commit_speculative(spec, alloc, value)) {
                        size_.fetch_add(1);
                        return {iterator(this, std::string(kb), value), true};
                    }
                }
                dealloc_speculation(alloc);
                continue;
            }

            pre_alloc alloc = allocate_speculative(spec, value);
            {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_path(spec)) { dealloc_speculation(alloc); continue; }
                if (alloc.root_replacement && commit_speculative(spec, alloc, value)) {
                    if (spec.target) retire_node(spec.target);
                    size_.fetch_add(1);
                    return {iterator(this, std::string(kb), value), true};
                }

                if ((spec.op == spec_op::ADD_EOS_LEAF_LIST) |
                    (spec.op == spec_op::DEMOTE_LEAF_LIST) |
                    (spec.op == spec_op::SPLIT_INTERIOR) |
                    (spec.op == spec_op::PREFIX_INTERIOR) |
                    (spec.op == spec_op::ADD_CHILD_CONVERT)) {

                    dealloc_speculation(alloc);
                    if (validate_path(spec)) {
                        ptr_t root = root_.load();
                        auto res = insert_impl(&root_, root, kb, value);
                        if (!res.inserted) {
                            for (auto* old : res.old_nodes) retire_node(old);
                            return {find(key), false};
                        }
                        if (res.new_node) root_.store(res.new_node);
                        for (auto* old : res.old_nodes) retire_node(old);
                        size_.fetch_add(1);
                        return {iterator(this, std::string(kb), value), true};
                    }
                }
            }
            dealloc_speculation(alloc);
        }
    }
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_erase_probe.h"
