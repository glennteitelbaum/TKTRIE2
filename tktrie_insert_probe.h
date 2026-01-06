#pragma once

// This file contains speculative insert probing for concurrent operations
// It should only be included from tktrie_insert.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::speculative_info TKTRIE_CLASS::probe_leaf_speculative(
    ptr_t n, std::string_view key, speculative_info& info) const noexcept {
    if (n->is_poisoned()) {
        info.op = spec_op::EXISTS;
        return info;
    }
    
    std::string_view skip = n->skip_str();
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
        if (ln->has(c)) { info.op = spec_op::EXISTS; return info; }
        info.op = (ln->count() < LIST_MAX) ? spec_op::IN_PLACE_LEAF : spec_op::LIST_TO_FULL_LEAF;
        return info;
    }
    auto* fn = n->template as_full<true>();
    info.op = fn->has(c) ? spec_op::EXISTS : spec_op::IN_PLACE_LEAF;
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
        std::string_view skip = n->skip_str();
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
            if (n->has_eos(false)) { info.op = spec_op::EXISTS; return info; }
            info.op = spec_op::IN_PLACE_INTERIOR;
            info.target = n;
            info.target_version = n->version();
            info.is_eos = true;
            return info;
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = n->get_child(false, c);

        if (!child || builder_t::is_sentinel(child)) {
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            info.c = c;
            info.remaining_key = std::string(key.substr(1));

            if (n->is_list()) {
                info.op = (n->template as_list<false>()->count() < LIST_MAX) 
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
    const speculative_info& info, [[maybe_unused]] const T& value) {
    pre_alloc alloc;
    std::string_view key = info.remaining_key;
    std::string_view skip = info.target_skip;
    size_t m = info.match_pos;

    // Only allocate node shells - data filling happens inside lock after validation
    switch (info.op) {
    case spec_op::EMPTY_TREE: {
        alloc.root_replacement = create_leaf_for_key(key, value);
        alloc.add(alloc.root_replacement);
        break;
    }
    case spec_op::SPLIT_LEAF_SKIP: {
        std::string common(skip.substr(0, m));
        ptr_t interior = builder_.make_interior_list(common);
        ptr_t old_child = builder_.make_leaf_skip(skip.substr(m + 1), T{});  // Empty, filled later
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(old_child);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_LEAF_SKIP: {
        ptr_t interior = builder_.make_interior_list(std::string(key));
        ptr_t child = builder_.make_leaf_skip(skip.substr(m + 1), T{});  // Empty, filled later
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::EXTEND_LEAF_SKIP: {
        ptr_t interior = builder_.make_interior_list(std::string(skip));
        ptr_t child = create_leaf_for_key(key.substr(m + 1), value);
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::SPLIT_LEAF_LIST: {
        std::string common(skip.substr(0, m));
        ptr_t interior = builder_.make_interior_list(common);
        // Allocate same type as target
        ptr_t old_child;
        if (info.target->is_list()) {
            old_child = builder_.make_leaf_list(skip.substr(m + 1));
        } else {
            old_child = builder_.make_leaf_full(skip.substr(m + 1));
        }
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(old_child);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_LEAF_LIST: {
        ptr_t interior = builder_.make_interior_list(std::string(key));
        // Allocate same type as target
        ptr_t old_child;
        if (info.target->is_list()) {
            old_child = builder_.make_leaf_list(skip.substr(m + 1));
        } else {
            old_child = builder_.make_leaf_full(skip.substr(m + 1));
        }
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(old_child);
        break;
    }
    case spec_op::LIST_TO_FULL_LEAF: {
        ptr_t full = builder_.make_leaf_full(std::string(skip));
        alloc.root_replacement = full;
        alloc.add(full);
        break;
    }
    case spec_op::SPLIT_INTERIOR: {
        std::string common(skip.substr(0, m));
        ptr_t new_int = builder_.make_interior_list(common);
        // Allocate same type as target for old_child
        ptr_t old_child;
        if (info.target->is_list()) {
            old_child = builder_.make_interior_list(skip.substr(m + 1));
        } else {
            old_child = builder_.make_interior_full(skip.substr(m + 1));
        }
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        alloc.root_replacement = new_int;
        alloc.add(new_int);
        alloc.add(old_child);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_INTERIOR: {
        ptr_t new_int = builder_.make_interior_list(std::string(key));
        // Allocate same type as target for old_child
        ptr_t old_child;
        if (info.target->is_list()) {
            old_child = builder_.make_interior_list(skip.substr(m + 1));
        } else {
            old_child = builder_.make_interior_full(skip.substr(m + 1));
        }
        alloc.root_replacement = new_int;
        alloc.add(new_int);
        alloc.add(old_child);
        break;
    }
    case spec_op::ADD_CHILD_CONVERT: {
        ptr_t full = builder_.make_interior_full(std::string(skip));
        ptr_t child = create_leaf_for_key(info.remaining_key, value);
        alloc.root_replacement = full;
        alloc.add(full);
        alloc.add(child);
        break;
    }
    // These are handled differently (in-place or complex)
    case spec_op::EXISTS:
    case spec_op::IN_PLACE_LEAF:
    case spec_op::IN_PLACE_INTERIOR:
    case spec_op::ADD_EOS_LEAF_LIST:
    case spec_op::DEMOTE_LEAF_LIST:
        break;
    }

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
    return parent->get_child_slot(false, edge);
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
    
    std::string_view skip = info.target_skip;
    size_t m = info.match_pos;

    switch (info.op) {
    case spec_op::EMPTY_TREE:
        if (root_.load() != nullptr) return false;
        root_.store(alloc.root_replacement);
        return true;

    case spec_op::SPLIT_LEAF_SKIP: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        
        // Fill in data: interior with two children
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(info.remaining_key[m]);
        
        T old_value{};
        info.target->as_skip()->value.try_read(old_value);
        alloc.nodes[1]->as_skip()->value.set(old_value);  // old_child
        
        auto* interior = alloc.root_replacement->template as_list<false>();
        interior->add_two_children(old_c, alloc.nodes[1], new_c, alloc.nodes[2]);
        
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    
    case spec_op::PREFIX_LEAF_SKIP: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        
        T old_value{};
        info.target->as_skip()->value.try_read(old_value);
        alloc.nodes[1]->as_skip()->value.set(old_value);  // child
        
        auto* interior = alloc.root_replacement->template as_list<false>();
        if constexpr (FIXED_LEN == 0) {
            interior->set_eos(value);
        }
        interior->add_child(old_c, alloc.nodes[1]);
        
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    
    case spec_op::EXTEND_LEAF_SKIP: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        
        unsigned char new_c = static_cast<unsigned char>(info.remaining_key[m]);
        
        T old_value{};
        info.target->as_skip()->value.try_read(old_value);
        
        auto* interior = alloc.root_replacement->template as_list<false>();
        if constexpr (FIXED_LEN == 0) {
            interior->set_eos(old_value);
        }
        interior->add_child(new_c, alloc.nodes[1]);  // new child already has value
        
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    
    case spec_op::SPLIT_LEAF_LIST: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(info.remaining_key[m]);
        
        // Copy values to old_child (same type as target)
        if (info.target->is_list()) {
            auto* src = info.target->template as_list<true>();
            auto* dst = alloc.nodes[1]->template as_list<true>();
            src->copy_values_to(dst);
        } else {
            auto* src = info.target->template as_full<true>();
            auto* dst = alloc.nodes[1]->template as_full<true>();
            src->copy_values_to(dst);
        }
        
        auto* interior = alloc.root_replacement->template as_list<false>();
        interior->add_two_children(old_c, alloc.nodes[1], new_c, alloc.nodes[2]);
        
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    
    case spec_op::PREFIX_LEAF_LIST: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        
        // Copy values to old_child (same type as target)
        if (info.target->is_list()) {
            auto* src = info.target->template as_list<true>();
            auto* dst = alloc.nodes[1]->template as_list<true>();
            src->copy_values_to(dst);
        } else {
            auto* src = info.target->template as_full<true>();
            auto* dst = alloc.nodes[1]->template as_full<true>();
            src->copy_values_to(dst);
        }
        
        auto* interior = alloc.root_replacement->template as_list<false>();
        if constexpr (FIXED_LEN == 0) {
            interior->set_eos(value);
        }
        interior->add_child(old_c, alloc.nodes[1]);
        
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    
    case spec_op::LIST_TO_FULL_LEAF: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        
        // Copy existing values + add new
        auto* src = info.target->template as_list<true>();
        auto* dst = alloc.root_replacement->template as_full<true>();
        for (int i = 0; i < src->count(); ++i) {
            unsigned char ch = src->chars.char_at(i);
            T val{};
            src->values[i].try_read(val);
            dst->add_value(ch, val);
        }
        dst->add_value(info.c, value);
        
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    
    case spec_op::SPLIT_INTERIOR: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(info.remaining_key[m]);
        
        // Move children to old_child (same type as target)
        if (info.target->is_list()) {
            auto* src = info.target->template as_list<false>();
            auto* dst = alloc.nodes[1]->template as_list<false>();
            src->move_interior_to(dst);
        } else {
            auto* src = info.target->template as_full<false>();
            auto* dst = alloc.nodes[1]->template as_full<false>();
            src->move_interior_to(dst);
        }
        
        auto* interior = alloc.root_replacement->template as_list<false>();
        interior->add_two_children(old_c, alloc.nodes[1], new_c, alloc.nodes[2]);
        
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    
    case spec_op::PREFIX_INTERIOR: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        
        // Move children to old_child (same type as target)
        if (info.target->is_list()) {
            auto* src = info.target->template as_list<false>();
            auto* dst = alloc.nodes[1]->template as_list<false>();
            src->move_interior_to(dst);
        } else {
            auto* src = info.target->template as_full<false>();
            auto* dst = alloc.nodes[1]->template as_full<false>();
            src->move_interior_to(dst);
        }
        
        auto* interior = alloc.root_replacement->template as_list<false>();
        if constexpr (FIXED_LEN == 0) {
            interior->set_eos(value);
        }
        interior->add_child(old_c, alloc.nodes[1]);
        
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }
    
    case spec_op::ADD_CHILD_CONVERT: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        
        // Move children from list to full + add new child
        auto* src = info.target->template as_list<false>();
        auto* dst = alloc.root_replacement->template as_full<false>();
        src->move_interior_to_full(dst);
        dst->add_child(info.c, alloc.nodes[1]);  // new child
        
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }

    // These should not reach commit_speculative
    case spec_op::EXISTS:
    case spec_op::IN_PLACE_LEAF:
    case spec_op::IN_PLACE_INTERIOR:
    case spec_op::ADD_EOS_LEAF_LIST:
    case spec_op::DEMOTE_LEAF_LIST:
        return false;
    }
    return false;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::dealloc_speculation(pre_alloc& alloc) {
    for (int i = 0; i < alloc.count; ++i) {
        ptr_t n = alloc.nodes[i];
        if (n) builder_.dealloc_node(n);
    }
    alloc.count = 0;
    alloc.root_replacement = nullptr;
}

TKTRIE_TEMPLATE
std::pair<typename TKTRIE_CLASS::iterator, bool> TKTRIE_CLASS::insert_locked(
    const Key& key, std::string_view kb, const T& value, bool* retired_any) {
    if (retired_any) *retired_any = false;
    
    if constexpr (!THREADED) {
        std::lock_guard<mutex_t> lock(mutex_);

        ptr_t root = root_.load();
        auto res = insert_impl(&root_, root, kb, value);

        if (!res.inserted) {
            if (retired_any && !res.old_nodes.empty()) *retired_any = true;
            for (auto* old : res.old_nodes) retire_node(old);
            return {find(key), false};
        }

        if (res.new_node) root_.store(res.new_node);
        if (retired_any && !res.old_nodes.empty()) *retired_any = true;
        for (auto* old : res.old_nodes) retire_node(old);
        size_.fetch_add(1);

        return {iterator(this, kb, value), true};
    } else {
        maybe_reclaim();
        
        auto& slot = get_ebr_slot();
        constexpr int MAX_RETRIES = 7;
        
        for (int retry = 0; retry <= MAX_RETRIES; ++retry) {
            auto guard = slot.get_guard();
            speculative_info spec = probe_speculative(root_.load(), kb);
            
            get_retry_stats().speculative_attempts.fetch_add(1, std::memory_order_relaxed);

            if (spec.op == spec_op::EXISTS) {
                get_retry_stats().speculative_successes.fetch_add(1, std::memory_order_relaxed);
                if (retry < 8) get_retry_stats().retries[retry].fetch_add(1, std::memory_order_relaxed);
                return {iterator(this, kb, value), false};
            }

            // In-place leaf - no allocation, brief lock
            if (spec.op == spec_op::IN_PLACE_LEAF) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_path(spec)) continue;
                
                ptr_t n = spec.target;
                unsigned char c = spec.c;
                
                if (n->is_list()) {
                    auto* ln = n->template as_list<true>();
                    if (ln->has(c)) continue;
                    if (ln->count() >= LIST_MAX) {
                        // Need LIST_TO_FULL - re-probe will get it
                        continue;
                    }
                    n->bump_version();
                    ln->add_value(c, value);
                } else {
                    auto* fn = n->template as_full<true>();
                    if (fn->has(c)) continue;
                    n->bump_version();
                    fn->add_value_atomic(c, value);
                }
                size_.fetch_add(1);
                get_retry_stats().speculative_successes.fetch_add(1, std::memory_order_relaxed);
                if (retry < 8) get_retry_stats().retries[retry].fetch_add(1, std::memory_order_relaxed);
                return {iterator(this, kb, value), true};
            }

            // In-place interior - brief lock
            if (spec.op == spec_op::IN_PLACE_INTERIOR) {
                if (spec.is_eos) {
                    if constexpr (FIXED_LEN > 0) {
                        // Can't happen for fixed-length keys
                        continue;
                    } else {
                        std::lock_guard<mutex_t> lock(mutex_);
                        if (!validate_path(spec)) continue;
                        
                        ptr_t n = spec.target;
                        if (n->has_eos(false)) continue;
                        
                        n->bump_version();
                        n->set_eos(false, value);
                        size_.fetch_add(1);
                        get_retry_stats().speculative_successes.fetch_add(1, std::memory_order_relaxed);
                        if (retry < 8) get_retry_stats().retries[retry].fetch_add(1, std::memory_order_relaxed);
                        return {iterator(this, kb, value), true};
                    }
                } else {
                    // Adding child to interior - allocate child outside lock
                    ptr_t child = create_leaf_for_key(spec.remaining_key, value);
                    std::lock_guard<mutex_t> lock(mutex_);
                    if (!validate_path(spec)) { builder_.dealloc_node(child); continue; }
                    
                    ptr_t n = spec.target;
                    unsigned char c = spec.c;
                    
                    if (n->is_list()) {
                        auto* ln = n->template as_list<false>();
                        if (ln->has(c)) { builder_.dealloc_node(child); continue; }
                        if (ln->count() >= LIST_MAX) {
                            builder_.dealloc_node(child);
                            continue;  // Re-probe will get ADD_CHILD_CONVERT
                        }
                        n->bump_version();
                        ln->add_child(c, child);
                    } else if (n->is_full()) {
                        auto* fn = n->template as_full<false>();
                        if (fn->has(c)) { builder_.dealloc_node(child); continue; }
                        n->bump_version();
                        fn->add_child_atomic(c, child);
                    } else {
                        builder_.dealloc_node(child);
                        continue;
                    }
                    size_.fetch_add(1);
                    get_retry_stats().speculative_successes.fetch_add(1, std::memory_order_relaxed);
                    if (retry < 8) get_retry_stats().retries[retry].fetch_add(1, std::memory_order_relaxed);
                    return {iterator(this, kb, value), true};
                }
            }

            // Complex ops that need full speculative path (ADD_EOS_LEAF_LIST, DEMOTE_LEAF_LIST)
            // Fall through to allocation-based speculative or fallback
            if (spec.op == spec_op::ADD_EOS_LEAF_LIST || spec.op == spec_op::DEMOTE_LEAF_LIST) {
                // These are complex - use fallback
                if (retry == MAX_RETRIES) break;
                continue;
            }

            // Speculative path: allocate outside lock, then brief lock for commit
            pre_alloc alloc = allocate_speculative(spec, value);
            
            if (alloc.root_replacement) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_path(spec)) {
                    dealloc_speculation(alloc);
                    continue;
                }
                
                if (commit_speculative(spec, alloc, value)) {
                    // Retire old node
                    if (spec.target) {
                        retire_node(spec.target);
                        if (retired_any) *retired_any = true;
                    }
                    size_.fetch_add(1);
                    get_retry_stats().speculative_successes.fetch_add(1, std::memory_order_relaxed);
                    if (retry < 8) get_retry_stats().retries[retry].fetch_add(1, std::memory_order_relaxed);
                    return {iterator(this, kb, value), true};
                }
                dealloc_speculation(alloc);
                continue;
            }
        }
        
        // Fallback after MAX_RETRIES
        get_retry_stats().fallbacks.fetch_add(1, std::memory_order_relaxed);
        {
            std::lock_guard<mutex_t> lock(mutex_);
            
            ptr_t root = root_.load();
            auto res = insert_impl(&root_, root, kb, value);
            
            if (!res.inserted) {
                if (retired_any && !res.old_nodes.empty()) *retired_any = true;
                for (auto* old : res.old_nodes) retire_node(old);
                return {iterator(this, kb, value), false};
            }
            
            if (res.new_node) {
                root_.store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
                root_.store(res.new_node);
            }
            if (retired_any && !res.old_nodes.empty()) *retired_any = true;
            for (auto* old : res.old_nodes) retire_node(old);
            size_.fetch_add(1);
            return {iterator(this, kb, value), true};
        }
    }
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_erase_probe.h"
