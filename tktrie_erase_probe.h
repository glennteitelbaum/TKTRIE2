#pragma once

// This file contains speculative erase probing and allocation
// It should only be included from tktrie_insert_probe.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

// -----------------------------------------------------------------------------
// Probe leaf for erase operation
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_spec_info TKTRIE_CLASS::probe_leaf_erase(
    ptr_t n, std::string_view key, erase_spec_info& info) const noexcept {
    if (n->is_poisoned()) {
        info.op = erase_op::NOT_FOUND;
        return info;
    }
    
    std::string_view skip = n->skip_str();
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) { info.op = erase_op::NOT_FOUND; return info; }
    key.remove_prefix(m);

    info.target = n;
    info.target_version = n->version();
    info.target_skip = std::string(skip);

    // SKIP leaf - delete entire node
    if (n->is_skip()) {
        if (!key.empty()) { info.op = erase_op::NOT_FOUND; return info; }
        info.op = erase_op::DELETE_SKIP_LEAF;
        return info;
    }

    // LIST or FULL leaf
    if (key.size() != 1) { info.op = erase_op::NOT_FOUND; return info; }

    unsigned char c = static_cast<unsigned char>(key[0]);
    info.c = c;

    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        if (!ln->has(c)) { info.op = erase_op::NOT_FOUND; return info; }
        if (ln->count() == 1) {
            info.op = erase_op::DELETE_LAST_LEAF_ENTRY;
        } else {
            info.op = erase_op::IN_PLACE_LEAF_LIST;
        }
        return info;
    }

    auto* fn = n->template as_full<true>();
    if (!fn->has(c)) { info.op = erase_op::NOT_FOUND; return info; }
    // FULL leaf always in-place (never becomes empty from one removal)
    info.op = erase_op::IN_PLACE_LEAF_FULL;
    return info;
}

// -----------------------------------------------------------------------------
// Probe interior for erase - handles EOS deletion
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_spec_info TKTRIE_CLASS::probe_interior_erase(
    ptr_t n, std::string_view key, erase_spec_info& info) const noexcept {
    info.target = n;
    info.target_version = n->version();
    info.target_skip = std::string(n->skip_str());
    
    if (key.empty()) {
        // Deleting EOS from interior
        if constexpr (FIXED_LEN > 0) {
            info.op = erase_op::NOT_FOUND;
            return info;
        }
        if (!n->has_eos()) {
            info.op = erase_op::NOT_FOUND;
            return info;
        }
        
        int child_cnt = n->child_count();
        if (child_cnt == 0) {
            info.op = erase_op::NOT_FOUND;  // Use slow path
            return info;
        }
        if (child_cnt == 1) {
            // Will collapse with single child
            unsigned char c = 0;
            ptr_t child = nullptr;
            if (n->is_list()) {
                auto* ln = n->template as_list<false>();
                c = ln->chars.char_at(0);
                child = ln->children[0].load();
            } else {
                auto* fn = n->template as_full<false>();
                c = fn->valid.first();
                child = fn->children[c].load();
            }
            if (child && !builder_t::is_sentinel(child) && !child->is_poisoned()) {
                info.collapse_child = child;
                info.collapse_char = c;
                info.child_skip = std::string(child->skip_str());
            }
        }
        info.op = erase_op::DELETE_EOS_INTERIOR;
        return info;
    }
    
    info.op = erase_op::NOT_FOUND;
    return info;
}

// -----------------------------------------------------------------------------
// Main probe dispatcher
// -----------------------------------------------------------------------------

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
        std::string_view skip = n->skip_str();
        size_t m = match_skip_impl(skip, key);
        if (m < skip.size()) { info.op = erase_op::NOT_FOUND; return info; }
        key.remove_prefix(m);

        if (key.empty()) {
            return probe_interior_erase(n, key, info);
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = n->get_child(c);
        
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

// -----------------------------------------------------------------------------
// Allocate replacement nodes for erase
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_pre_alloc TKTRIE_CLASS::allocate_erase_speculative(
    const erase_spec_info& info) {
    erase_pre_alloc alloc;
    
    switch (info.op) {
    case erase_op::DELETE_SKIP_LEAF:
    case erase_op::DELETE_LAST_LEAF_ENTRY:
    case erase_op::DELETE_CHILD_NO_COLLAPSE:
        break;
        
    case erase_op::DELETE_EOS_INTERIOR:
    case erase_op::DELETE_CHILD_COLLAPSE: {
        if (!info.collapse_child) break;
        
        std::string new_skip(info.target_skip);
        new_skip.push_back(static_cast<char>(info.collapse_char));
        new_skip.append(info.child_skip);
        
        ptr_t child = info.collapse_child;
        ptr_t merged = nullptr;
        
        if (child->is_leaf()) {
            if (child->is_skip()) {
                T val{};
                child->as_skip()->value.try_read(val);
                merged = builder_.make_leaf_skip(new_skip, val);
            } else if (child->is_list()) {
                merged = builder_.make_leaf_list(new_skip);
                child->template as_list<true>()->copy_values_to(merged->template as_list<true>());
            } else {
                merged = builder_.make_leaf_full(new_skip);
                child->template as_full<true>()->copy_values_to(merged->template as_full<true>());
            }
        } else {
            if (child->is_list()) {
                merged = builder_.make_interior_list(new_skip);
                child->template as_list<false>()->copy_interior_to(merged->template as_list<false>());
            } else {
                merged = builder_.make_interior_full(new_skip);
                child->template as_full<false>()->copy_interior_to(merged->template as_full<false>());
            }
        }
        
        if (merged) {
            merged->poison();
            alloc.replacement = merged;
            alloc.add(merged);
        }
        break;
    }
    
    case erase_op::NOT_FOUND:
    case erase_op::IN_PLACE_LEAF_LIST:
    case erase_op::IN_PLACE_LEAF_FULL:
        break;
    }
    
    return alloc;
}

// -----------------------------------------------------------------------------
// Validate erase path
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::validate_erase_path(const erase_spec_info& info) const noexcept {
    for (int i = 0; i < info.path_len; ++i) {
        if (info.path[i].node->is_poisoned()) return false;
        if (info.path[i].node->version() != info.path[i].version) return false;
    }
    if (info.target && (info.path_len == 0 || info.path[info.path_len-1].node != info.target)) {
        if (info.target->is_poisoned()) return false;
        if (info.target->version() != info.target_version) return false;
    }
    if (info.collapse_child && info.collapse_child->is_poisoned()) {
        return false;
    }
    return true;
}

// -----------------------------------------------------------------------------
// Commit erase speculation
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::commit_erase_speculative(
    erase_spec_info& info, erase_pre_alloc& alloc) {
    
    atomic_ptr* slot = nullptr;
    if (info.path_len <= 1) {
        slot = &root_;
    } else {
        ptr_t parent = info.path[info.path_len - 2].node;
        unsigned char edge = info.path[info.path_len - 1].edge;
        slot = parent->get_child_slot(edge);
    }
    
    switch (info.op) {
    case erase_op::DELETE_SKIP_LEAF:
    case erase_op::DELETE_LAST_LEAF_ENTRY: {
        if (!slot || slot->load() != info.target) return false;
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        if constexpr (THREADED) {
            slot->store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
        }
        slot->store(nullptr);
        return true;
    }
    
    case erase_op::DELETE_CHILD_NO_COLLAPSE: {
        ptr_t parent = info.target;
        if (parent->version() != info.target_version) return false;
        
        parent->bump_version();
        if (parent->is_list()) {
            parent->template as_list<false>()->remove_child(info.c);
        } else {
            parent->template as_full<false>()->remove_child(info.c);
        }
        return true;
    }
    
    case erase_op::DELETE_EOS_INTERIOR: {
        ptr_t target = info.target;
        if (target->version() != info.target_version) return false;
        
        if (alloc.replacement) {
            if (!slot || slot->load() != target) return false;
            for (int i = 0; i < alloc.count; ++i) {
                if (alloc.nodes[i]) alloc.nodes[i]->unpoison();
            }
            if (info.path_len > 1) {
                info.path[info.path_len - 2].node->bump_version();
            }
            if constexpr (THREADED) {
                slot->store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
            }
            slot->store(alloc.replacement);
        } else {
            target->bump_version();
            target->clear_eos();
        }
        return true;
    }
    
    case erase_op::DELETE_CHILD_COLLAPSE: {
        if (!alloc.replacement) return false;
        if (!slot || slot->load() != info.target) return false;
        
        for (int i = 0; i < alloc.count; ++i) {
            if (alloc.nodes[i]) alloc.nodes[i]->unpoison();
        }
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        if constexpr (THREADED) {
            slot->store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
        }
        slot->store(alloc.replacement);
        return true;
    }
    
    case erase_op::NOT_FOUND:
    case erase_op::IN_PLACE_LEAF_LIST:
    case erase_op::IN_PLACE_LEAF_FULL:
        return false;
    }
    return false;
}

// -----------------------------------------------------------------------------
// Dealloc erase speculation
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
void TKTRIE_CLASS::dealloc_erase_speculation(erase_pre_alloc& alloc) {
    for (int i = 0; i < alloc.count; ++i) {
        if (alloc.nodes[i]) {
            builder_.dealloc_node(alloc.nodes[i]);
            alloc.nodes[i] = nullptr;
        }
    }
    alloc.count = 0;
    alloc.replacement = nullptr;
}

// -----------------------------------------------------------------------------
// In-place erase handlers
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_leaf_list_erase(ptr_t leaf, unsigned char c, uint64_t expected_version) {
    if (leaf->version() != expected_version) return false;
    auto* ln = leaf->template as_list<true>();
    if (!ln->has(c)) return false;
    if (ln->count() <= 1) return false;

    leaf->bump_version();
    ln->remove_value(c);
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_leaf_full_erase(ptr_t leaf, unsigned char c, uint64_t expected_version) {
    if (leaf->version() != expected_version) return false;
    auto* fn = leaf->template as_full<true>();
    if (!fn->has(c)) return false;
    leaf->bump_version();
    fn->remove_value(c);
    return true;
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_erase.h"
