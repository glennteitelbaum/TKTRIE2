#pragma once

// This file contains speculative insert probing for concurrent operations
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
    if (n->is_poisoned()) {
        info.op = spec_op::EXISTS;
        return info;
    }
    
    std::string_view skip = get_skip(n);
    size_t m = match_skip_impl(skip, key);

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

    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        if (ln->chars.find(c) >= 0) { info.op = spec_op::EXISTS; return info; }
        info.op = (ln->chars.count() < LIST_MAX) ? spec_op::IN_PLACE_LEAF : spec_op::LIST_TO_FULL_LEAF;
        return info;
    }
    auto* fn = n->template as_full<true>();
    info.op = fn->valid.template atomic_test<THREADED>(c) ? spec_op::EXISTS : spec_op::IN_PLACE_LEAF;
    return info;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::speculative_info TKTRIE_CLASS::probe_speculative(
    ptr_t n, std::string_view key) const noexcept {
    speculative_info info;
    info.remaining_key = std::string(key);

    if (!n || builder_t::is_sentinel(n)) {
        info.op = spec_op::EMPTY_TREE;
        return info;
    }

    if (n->is_poisoned()) {
        info.op = spec_op::EXISTS;
        return info;
    }

    info.path[info.path_len++] = {n, n->version(), 0};

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
            if (has_eos(n)) { info.op = spec_op::EXISTS; return info; }
            info.op = spec_op::IN_PLACE_INTERIOR;
            info.target = n;
            info.target_version = n->version();
            info.is_eos = true;
            return info;
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = find_child(n, c);

        if (!child || builder_t::is_sentinel(child)) {
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            info.c = c;
            info.remaining_key = std::string(key.substr(1));

            if (n->is_list()) {
                info.op = (n->template as_list<false>()->chars.count() < LIST_MAX) 
                    ? spec_op::IN_PLACE_INTERIOR : spec_op::ADD_CHILD_CONVERT;
            } else {
                info.op = spec_op::IN_PLACE_INTERIOR;
            }
            return info;
        }

        key.remove_prefix(1);
        n = child;
        
        if (n->is_poisoned()) {
            info.op = spec_op::EXISTS;
            return info;
        }
        
        if (info.path_len < speculative_info::MAX_PATH) {
            info.path[info.path_len++] = {n, n->version(), c};
        }
    }

    return probe_leaf_speculative(n, key, info);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::pre_alloc TKTRIE_CLASS::allocate_speculative(
    const speculative_info& info, const T& value) {
    pre_alloc alloc;
    (void)info; (void)value;  // Simplified - just use slow path for now
    return alloc;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::validate_path(const speculative_info& info) const noexcept {
    for (int i = 0; i < info.path_len; ++i) {
        if (info.path[i].node->is_poisoned()) return false;
        if (info.path[i].node->version() != info.path[i].version) return false;
    }
    if (info.target && (info.path_len == 0 || info.path[info.path_len-1].node != info.target)) {
        if (info.target->is_poisoned()) return false;
        if (info.target->version() != info.target_version) return false;
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
    if constexpr (THREADED) {
        slot->store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
    }
    slot->store(new_node);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::commit_speculative(
    speculative_info& info, pre_alloc& alloc, const T& value) {
    (void)info; (void)alloc; (void)value;
    return false;  // Always use slow path for now
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::dealloc_speculation(pre_alloc& alloc) {
    if (alloc.in_place_eos) {
        delete alloc.in_place_eos;
        alloc.in_place_eos = nullptr;
    }
    for (int i = 0; i < alloc.count; ++i) {
        ptr_t n = alloc.nodes[i];
        if (n) builder_t::delete_node(n);
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
        maybe_reclaim();
        
        auto& slot = get_ebr_slot();
        
        while (true) {
            auto guard = slot.get_guard();
            speculative_info spec = probe_speculative(root_.load(), kb);

            if (spec.op == spec_op::EXISTS) {
                return {iterator(this, std::string(kb), value), false};
            }

            if (spec.op == spec_op::IN_PLACE_LEAF) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_path(spec)) continue;
                
                ptr_t n = spec.target;
                unsigned char c = spec.c;
                
                if (n->is_list()) {
                    auto* ln = n->template as_list<true>();
                    if (ln->chars.find(c) >= 0) continue;
                    if (ln->chars.count() >= LIST_MAX) goto slow_path;
                    n->bump_version();
                    ln->add_entry(c, value);
                } else {
                    auto* fn = n->template as_full<true>();
                    if (fn->valid.template atomic_test<THREADED>(c)) continue;
                    n->bump_version();
                    fn->template add_entry_atomic<THREADED>(c, value);
                }
                size_.fetch_add(1);
                return {iterator(this, std::string(kb), value), true};
            }

            if (spec.op == spec_op::IN_PLACE_INTERIOR) {
                if (spec.is_eos) {
                    if constexpr (FIXED_LEN > 0) {
                        goto slow_path;  // Fixed-length keys can't have EOS
                    } else {
                        std::lock_guard<mutex_t> lock(mutex_);
                        if (!validate_path(spec)) continue;
                        
                        ptr_t n = spec.target;
                        if (has_eos(n)) continue;
                        
                        n->bump_version();
                        set_eos(n, value);
                        size_.fetch_add(1);
                        return {iterator(this, std::string(kb), value), true};
                    }
                } else {
                    ptr_t child = create_leaf_for_key(spec.remaining_key, value);
                    std::lock_guard<mutex_t> lock(mutex_);
                    if (!validate_path(spec)) { builder_.dealloc_node(child); continue; }
                    
                    ptr_t n = spec.target;
                    unsigned char c = spec.c;
                    
                    if (n->is_list()) {
                        auto* ln = n->template as_list<false>();
                        if (ln->chars.find(c) >= 0) { builder_.dealloc_node(child); continue; }
                        if (ln->chars.count() >= LIST_MAX) {
                            builder_.dealloc_node(child);
                            goto slow_path;
                        }
                        n->bump_version();
                        ln->add_child(c, child);
                    } else if (n->is_full()) {
                        auto* fn = n->template as_full<false>();
                        if (fn->valid.template atomic_test<THREADED>(c)) { builder_.dealloc_node(child); continue; }
                        n->bump_version();
                        fn->template add_child_atomic<THREADED>(c, child);
                    } else {
                        builder_.dealloc_node(child);
                        goto slow_path;
                    }
                    size_.fetch_add(1);
                    return {iterator(this, std::string(kb), value), true};
                }
            }

            slow_path:
            {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_path(spec)) continue;
                
                ptr_t root = root_.load();
                auto res = insert_impl(&root_, root, kb, value);
                
                if (!res.inserted) {
                    for (auto* old : res.old_nodes) retire_node(old);
                    return {iterator(this, std::string(kb), value), false};
                }
                
                if (res.new_node) {
                    root_.store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
                    root_.store(res.new_node);
                }
                for (auto* old : res.old_nodes) retire_node(old);
                size_.fetch_add(1);
                return {iterator(this, std::string(kb), value), true};
            }
        }
    }
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_erase_probe.h"
