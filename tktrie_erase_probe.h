#pragma once

// This file contains erase probing for concurrent operations
// It should only be included from tktrie_insert_probe.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

// -----------------------------------------------------------------------------
// Erase probing operations
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_spec_info TKTRIE_CLASS::probe_leaf_erase(
    ptr_t n, std::string_view key, erase_spec_info& info) const noexcept {
    if (n->is_poisoned()) {
        info.op = erase_op::NOT_FOUND;
        return info;
    }
    
    std::string_view skip = get_skip(n);
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) { info.op = erase_op::NOT_FOUND; return info; }
    key.remove_prefix(m);

    if (n->is_skip()) {
        info.op = erase_op::NOT_FOUND;
        return info;
    }

    if (key.size() != 1) { info.op = erase_op::NOT_FOUND; return info; }

    unsigned char c = static_cast<unsigned char>(key[0]);
    info.c = c;
    info.target = n;
    info.target_version = n->version();

    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        int idx = ln->chars.find(c);
        if (idx < 0) { info.op = erase_op::NOT_FOUND; return info; }
        if (ln->chars.count() <= 1) { info.op = erase_op::NOT_FOUND; return info; }
        info.op = erase_op::IN_PLACE_LEAF_LIST;
        return info;
    }

    auto* fn = n->template as_full<true>();
    if (!fn->valid.template atomic_test<THREADED>(c)) { info.op = erase_op::NOT_FOUND; return info; }
    info.op = erase_op::IN_PLACE_LEAF_FULL;
    return info;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_spec_info TKTRIE_CLASS::probe_erase(
    ptr_t n, std::string_view key) const noexcept {
    erase_spec_info info;

    if (!n || n->is_poisoned() || builder_t::is_sentinel(n)) {
        info.op = erase_op::NOT_FOUND;
        return info;
    }

    info.path[info.path_len++] = {n, n->version(), 0};

    while (!n->is_leaf()) {
        std::string_view skip = get_skip(n);
        size_t m = match_skip_impl(skip, key);
        if (m < skip.size()) { info.op = erase_op::NOT_FOUND; return info; }
        key.remove_prefix(m);

        if (key.empty()) { info.op = erase_op::NOT_FOUND; return info; }

        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = find_child(n, c);
        
        if (!child || builder_t::is_sentinel(child)) { 
            info.op = erase_op::NOT_FOUND; 
            return info; 
        }

        key.remove_prefix(1);
        n = child;
        
        if (n->is_poisoned()) {
            info.op = erase_op::NOT_FOUND;
            return info;
        }
        
        if (info.path_len < erase_spec_info::MAX_PATH) {
            info.path[info.path_len++] = {n, n->version(), c};
        }
    }

    return probe_leaf_erase(n, key, info);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_leaf_list_erase(ptr_t leaf, unsigned char c, uint64_t expected_version) {
    if (leaf->version() != expected_version) return false;
    auto* ln = leaf->template as_list<true>();
    int idx = ln->chars.find(c);
    if (idx < 0) return false;
    int count = ln->chars.count();
    if (count <= 1) return false;

    leaf->bump_version();
    ln->shift_values_down(idx);
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_leaf_full_erase(ptr_t leaf, unsigned char c, uint64_t expected_version) {
    if (leaf->version() != expected_version) return false;
    auto* fn = leaf->template as_full<true>();
    if (!fn->valid.template atomic_test<THREADED>(c)) return false;
    leaf->bump_version();
    fn->template remove_entry<THREADED>(c);
    return true;
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_erase.h"
