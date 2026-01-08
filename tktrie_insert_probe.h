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
        info.op = spec_op::RETRY;  // Signal retry, not EXISTS
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

    // BINARY, LIST, POP, or FULL leaf
    info.target = n;
    info.target_version = n->version();
    info.target_skip = std::string(skip);

    if ((m < skip.size()) & (m < key.size())) {
        info.op = spec_op::SPLIT_LEAF_MULTI;
        info.match_pos = m;
        info.remaining_key = std::string(key);
        return info;
    }
    if (m < skip.size()) {
        info.op = spec_op::PREFIX_LEAF_MULTI;
        info.match_pos = m;
        info.remaining_key = std::string(key);
        return info;
    }
    key.remove_prefix(m);
    info.remaining_key = std::string(key);

    if (key.empty()) { info.op = spec_op::ADD_EOS_LEAF_MULTI; return info; }
    if (key.size() != 1) { info.op = spec_op::DEMOTE_LEAF_MULTI; return info; }

    unsigned char c = static_cast<unsigned char>(key[0]);
    info.c = c;

    if (n->is_binary()) {
        auto* bn = n->template as_binary<true>();
        if (bn->has(c)) { info.op = spec_op::EXISTS; return info; }
        info.op = (bn->count() < BINARY_MAX) ? spec_op::IN_PLACE_LEAF : spec_op::BINARY_TO_LIST_LEAF;
        return info;
    }
    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        if (ln->has(c)) { info.op = spec_op::EXISTS; return info; }
        info.op = (ln->count() < LIST_MAX) ? spec_op::IN_PLACE_LEAF : spec_op::LIST_TO_POP_LEAF;
        return info;
    }
    if (n->is_pop()) {
        auto* pn = n->template as_pop<true>();
        if (pn->has(c)) { info.op = spec_op::EXISTS; return info; }
        info.op = (pn->count() < POP_MAX) ? spec_op::IN_PLACE_LEAF : spec_op::POP_TO_FULL_LEAF;
        return info;
    }
    // FULL
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
        info.op = spec_op::RETRY;  // Signal retry, not EXISTS
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
            if (n->has_eos()) { info.op = spec_op::EXISTS; return info; }
            info.op = spec_op::IN_PLACE_INTERIOR;
            info.target = n;
            info.target_version = n->version();
            info.is_eos = true;
            return info;
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = n->get_child(c);

        if (!child || builder_t::is_sentinel(child)) {
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            info.c = c;
            info.remaining_key = std::string(key.substr(1));

            if (n->is_binary()) {
                auto* bn = n->template as_binary<false>();
                info.op = (bn->count() < BINARY_MAX) 
                    ? spec_op::IN_PLACE_INTERIOR : spec_op::BINARY_TO_LIST_INTERIOR;
            } else if (n->is_list()) {
                auto* ln = n->template as_list<false>();
                info.op = (ln->count() < LIST_MAX) 
                    ? spec_op::IN_PLACE_INTERIOR : spec_op::LIST_TO_POP_INTERIOR;
            } else if (n->is_pop()) {
                auto* pn = n->template as_pop<false>();
                info.op = (pn->count() < POP_MAX) 
                    ? spec_op::IN_PLACE_INTERIOR : spec_op::POP_TO_FULL_INTERIOR;
            } else {
                // FULL - always in-place
                info.op = spec_op::IN_PLACE_INTERIOR;
            }
            return info;
        }

        key.remove_prefix(1);
        n = child;
        
        if (n->is_poisoned()) {
            info.op = spec_op::RETRY;  // Signal retry, not EXISTS
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
    std::string_view key = info.remaining_key;
    std::string_view skip = info.target_skip;
    size_t m = info.match_pos;

    // Allocate AND fill data outside lock - speculative reads are validated later
    // All new nodes are poisoned so dealloc_node won't recurse into borrowed children
    switch (info.op) {
    case spec_op::EMPTY_TREE: {
        alloc.root_replacement = create_leaf_for_key(key, value);
        alloc.root_replacement->poison();
        alloc.add(alloc.root_replacement);
        break;
    }
    case spec_op::SPLIT_LEAF_SKIP: {
        T old_value{};
        info.target->as_skip()->value.try_read(old_value);
        
        std::string common(skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);

        ptr_t interior = builder_.make_interior_list(common);
        ptr_t old_child = builder_.make_leaf_skip(skip.substr(m + 1), old_value);
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        interior->template as_list<false>()->add_two_children(old_c, old_child, new_c, new_child);
        interior->template as_list<false>()->update_capacity_flags();

        interior->poison();
        old_child->poison();
        new_child->poison();
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(old_child);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_LEAF_SKIP: {
        T old_value{};
        info.target->as_skip()->value.try_read(old_value);
        
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        
        ptr_t interior = builder_.make_interior_list(std::string(key));
        if constexpr (FIXED_LEN == 0) {
            interior->set_eos(value);
        }
        ptr_t child = builder_.make_leaf_skip(skip.substr(m + 1), old_value);
        interior->template as_list<false>()->add_child(old_c, child);
        interior->template as_list<false>()->update_capacity_flags();

        interior->poison();
        child->poison();
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::EXTEND_LEAF_SKIP: {
        T old_value{};
        info.target->as_skip()->value.try_read(old_value);
        
        unsigned char new_c = static_cast<unsigned char>(key[m]);
        
        ptr_t interior = builder_.make_interior_list(std::string(skip));
        if constexpr (FIXED_LEN == 0) {
            interior->set_eos(old_value);
        }
        ptr_t child = create_leaf_for_key(key.substr(m + 1), value);
        interior->template as_list<false>()->add_child(new_c, child);
        interior->template as_list<false>()->update_capacity_flags();

        interior->poison();
        child->poison();
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::SPLIT_LEAF_MULTI: {
        std::string common(skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);

        ptr_t interior = builder_.make_interior_binary(common);
        ptr_t old_child;
        if (info.target->is_binary()) {
            old_child = builder_.make_leaf_binary(skip.substr(m + 1));
            info.target->template as_binary<true>()->copy_values_to(old_child->template as_binary<true>());
            old_child->template as_binary<true>()->update_capacity_flags();
        } else if (info.target->is_list()) {
            old_child = builder_.make_leaf_list(skip.substr(m + 1));
            info.target->template as_list<true>()->copy_values_to(old_child->template as_list<true>());
            old_child->template as_list<true>()->update_capacity_flags();
        } else if (info.target->is_pop()) {
            old_child = builder_.make_leaf_pop(skip.substr(m + 1));
            info.target->template as_pop<true>()->copy_values_to(old_child->template as_pop<true>());
            old_child->template as_pop<true>()->update_capacity_flags();
        } else {
            old_child = builder_.make_leaf_full(skip.substr(m + 1));
            info.target->template as_full<true>()->copy_values_to(old_child->template as_full<true>());
            old_child->template as_full<true>()->update_capacity_flags();
        }
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        interior->template as_binary<false>()->add_child(old_c, old_child);
        interior->template as_binary<false>()->add_child(new_c, new_child);
        interior->template as_binary<false>()->update_capacity_flags();

        interior->poison();
        old_child->poison();
        new_child->poison();
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(old_child);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_LEAF_MULTI: {
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        
        ptr_t interior = builder_.make_interior_binary(std::string(key));
        if constexpr (FIXED_LEN == 0) {
            interior->set_eos(value);
        }
        ptr_t old_child;
        if (info.target->is_binary()) {
            old_child = builder_.make_leaf_binary(skip.substr(m + 1));
            info.target->template as_binary<true>()->copy_values_to(old_child->template as_binary<true>());
            old_child->template as_binary<true>()->update_capacity_flags();
        } else if (info.target->is_list()) {
            old_child = builder_.make_leaf_list(skip.substr(m + 1));
            info.target->template as_list<true>()->copy_values_to(old_child->template as_list<true>());
            old_child->template as_list<true>()->update_capacity_flags();
        } else if (info.target->is_pop()) {
            old_child = builder_.make_leaf_pop(skip.substr(m + 1));
            info.target->template as_pop<true>()->copy_values_to(old_child->template as_pop<true>());
            old_child->template as_pop<true>()->update_capacity_flags();
        } else {
            old_child = builder_.make_leaf_full(skip.substr(m + 1));
            info.target->template as_full<true>()->copy_values_to(old_child->template as_full<true>());
            old_child->template as_full<true>()->update_capacity_flags();
        }
        interior->template as_binary<false>()->add_child(old_c, old_child);
        interior->template as_binary<false>()->update_capacity_flags();

        interior->poison();
        old_child->poison();
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(old_child);
        break;
    }
    case spec_op::BINARY_TO_LIST_LEAF: {
        ptr_t list = builder_.make_leaf_list(std::string(skip));
        auto* src = info.target->template as_binary<true>();
        auto* dst = list->template as_list<true>();
        for (int i = 0; i < src->count(); ++i) {
            T val{};
            src->values[i].try_read(val);
            dst->add_value(src->chars[i], val);
        }
        dst->add_value(info.c, value);
        dst->update_capacity_flags();
        
        list->poison();
        
        alloc.root_replacement = list;
        alloc.add(list);
        break;
    }
    case spec_op::LIST_TO_POP_LEAF: {
        ptr_t pop = builder_.make_leaf_pop(std::string(skip));
        auto* src = info.target->template as_list<true>();
        auto* dst = pop->template as_pop<true>();
        for (int i = 0; i < src->count(); ++i) {
            unsigned char ch = src->chars.char_at(i);
            T val{};
            src->values[i].try_read(val);
            dst->add_value(ch, val);
        }
        dst->add_value(info.c, value);
        dst->update_capacity_flags();
        
        pop->poison();
        
        alloc.root_replacement = pop;
        alloc.add(pop);
        break;
    }
    case spec_op::POP_TO_FULL_LEAF: {
        ptr_t full = builder_.make_leaf_full(std::string(skip));
        auto* src = info.target->template as_pop<true>();
        auto* dst = full->template as_full<true>();
        int slot = 0;
        src->valid.for_each_set([src, dst, &slot](unsigned char ch) {
            T val{};
            src->values[slot].try_read(val);
            dst->add_value(ch, val);
            ++slot;
        });
        dst->add_value(info.c, value);
        dst->update_capacity_flags();
        
        full->poison();
        
        alloc.root_replacement = full;
        alloc.add(full);
        break;
    }
    case spec_op::SPLIT_INTERIOR: {
        std::string common(skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);

        ptr_t new_int = builder_.make_interior_binary(common);
        ptr_t old_child;
        bool had_eos = info.target->has_eos();  // Capture EOS state before copy
        // Copy interior with borrowed children (poison prevents recursive delete)
        if (info.target->is_binary()) {
            old_child = builder_.make_interior_binary(skip.substr(m + 1));
            info.target->template as_binary<false>()->copy_interior_to(old_child->template as_binary<false>());
            old_child->template as_binary<false>()->update_capacity_flags();
        } else if (info.target->is_list()) {
            old_child = builder_.make_interior_list(skip.substr(m + 1));
            info.target->template as_list<false>()->copy_interior_to(old_child->template as_list<false>());
            old_child->template as_list<false>()->update_capacity_flags();
        } else if (info.target->is_pop()) {
            old_child = builder_.make_interior_pop(skip.substr(m + 1));
            info.target->template as_pop<false>()->copy_interior_to(old_child->template as_pop<false>());
            old_child->template as_pop<false>()->update_capacity_flags();
        } else {
            old_child = builder_.make_interior_full(skip.substr(m + 1));
            info.target->template as_full<false>()->copy_interior_to(old_child->template as_full<false>());
            old_child->template as_full<false>()->update_capacity_flags();
        }
        if constexpr (FIXED_LEN == 0) {
            if (had_eos) old_child->set_eos_flag();  // Preserve EOS flag
        }
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        new_int->template as_binary<false>()->add_child(old_c, old_child);
        new_int->template as_binary<false>()->add_child(new_c, new_child);
        new_int->template as_binary<false>()->update_capacity_flags();

        new_int->poison();
        old_child->poison();
        new_child->poison();
        
        alloc.root_replacement = new_int;
        alloc.add(new_int);
        alloc.add(old_child);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_INTERIOR: {
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        
        ptr_t new_int = builder_.make_interior_binary(std::string(key));
        if constexpr (FIXED_LEN == 0) {
            new_int->set_eos(value);
        }
        ptr_t old_child;
        bool had_eos = info.target->has_eos();  // Capture EOS state before copy
        // Copy interior with borrowed children (poison prevents recursive delete)
        if (info.target->is_binary()) {
            old_child = builder_.make_interior_binary(skip.substr(m + 1));
            info.target->template as_binary<false>()->copy_interior_to(old_child->template as_binary<false>());
            old_child->template as_binary<false>()->update_capacity_flags();
        } else if (info.target->is_list()) {
            old_child = builder_.make_interior_list(skip.substr(m + 1));
            info.target->template as_list<false>()->copy_interior_to(old_child->template as_list<false>());
            old_child->template as_list<false>()->update_capacity_flags();
        } else if (info.target->is_pop()) {
            old_child = builder_.make_interior_pop(skip.substr(m + 1));
            info.target->template as_pop<false>()->copy_interior_to(old_child->template as_pop<false>());
            old_child->template as_pop<false>()->update_capacity_flags();
        } else {
            old_child = builder_.make_interior_full(skip.substr(m + 1));
            info.target->template as_full<false>()->copy_interior_to(old_child->template as_full<false>());
            old_child->template as_full<false>()->update_capacity_flags();
        }
        if constexpr (FIXED_LEN == 0) {
            if (had_eos) old_child->set_eos_flag();  // Preserve EOS flag
        }
        new_int->template as_binary<false>()->add_child(old_c, old_child);
        new_int->template as_binary<false>()->update_capacity_flags();

        new_int->poison();
        old_child->poison();
        
        alloc.root_replacement = new_int;
        alloc.add(new_int);
        alloc.add(old_child);
        break;
    }
    case spec_op::BINARY_TO_LIST_INTERIOR: {
        // BINARY interior full, convert to LIST
        ptr_t list = builder_.make_interior_list(std::string(skip));
        auto* src = info.target->template as_binary<false>();
        auto* dst = list->template as_list<false>();
        for (int i = 0; i < src->count(); ++i) {
            dst->add_child(src->chars[i], src->children[i].load());
        }
        if constexpr (FIXED_LEN == 0) {
            T eos_val;
            if (src->eos.try_read(eos_val)) {
                dst->eos.set(eos_val);
                list->set_eos_flag();  // Update header flag
            }
        }
        ptr_t child = create_leaf_for_key(info.remaining_key, value);
        dst->add_child(info.c, child);
        dst->update_capacity_flags();
        
        list->poison();
        child->poison();
        
        alloc.root_replacement = list;
        alloc.add(list);
        alloc.add(child);
        break;
    }
    case spec_op::LIST_TO_POP_INTERIOR: {
        // LIST interior full, convert to POP
        ptr_t pop = builder_.make_interior_pop(std::string(skip));
        auto* src = info.target->template as_list<false>();
        auto* dst = pop->template as_pop<false>();
        for (int i = 0; i < src->count(); ++i) {
            unsigned char ch = src->chars.char_at(i);
            dst->add_child(ch, src->children[i].load());
        }
        if constexpr (FIXED_LEN == 0) {
            T eos_val;
            if (src->eos.try_read(eos_val)) {
                dst->eos.set(eos_val);
                pop->set_eos_flag();  // Update header flag
            }
        }
        ptr_t child = create_leaf_for_key(info.remaining_key, value);
        dst->add_child(info.c, child);
        dst->update_capacity_flags();
        
        pop->poison();
        child->poison();
        
        alloc.root_replacement = pop;
        alloc.add(pop);
        alloc.add(child);
        break;
    }
    case spec_op::POP_TO_FULL_INTERIOR: {
        // POP interior full, convert to FULL
        ptr_t full = builder_.make_interior_full(std::string(skip));
        info.target->template as_pop<false>()->copy_children_to_full(full->template as_full<false>());
        if constexpr (FIXED_LEN == 0) {
            T eos_val;
            if (info.target->template as_pop<false>()->eos.try_read(eos_val)) {
                full->template as_full<false>()->eos.set(eos_val);
                full->set_eos_flag();  // Update header flag
            }
        }
        ptr_t child = create_leaf_for_key(info.remaining_key, value);
        full->template as_full<false>()->add_child(info.c, child);
        full->template as_full<false>()->update_capacity_flags();
        
        full->poison();
        child->poison();
        
        alloc.root_replacement = full;
        alloc.add(full);
        alloc.add(child);
        break;
    }
    // These are handled differently (in-place or complex)
    case spec_op::EXISTS:
    case spec_op::RETRY:
    case spec_op::IN_PLACE_LEAF:
    case spec_op::IN_PLACE_INTERIOR:
    case spec_op::ADD_EOS_LEAF_MULTI:
    case spec_op::DEMOTE_LEAF_MULTI:
        break;
    }

    return alloc;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::validate_path(const speculative_info& info) const noexcept {
    [[assume(info.path_len >= 0 && info.path_len <= 64)]];
    // Version check is sufficient - poison() bumps version
    for (int i = 0; i < info.path_len; ++i) {
        if (info.path[i].node->version() != info.path[i].version) return false;
    }
    if (info.target && (info.path_len == 0 || info.path[info.path_len-1].node != info.target)) {
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
    return parent->get_child_slot(edge);
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
    speculative_info& info, pre_alloc& alloc, [[maybe_unused]] const T& value) {
    
    // All data already filled in allocate_speculative - just validate and swap
    // On success, unpoison all nodes so they become live
    switch (info.op) {
    case spec_op::EMPTY_TREE:
        if (root_.load() != nullptr) return false;
        // Unpoison before making visible
        [[assume(alloc.count >= 0 && alloc.count <= 8)]];
        for (int i = 0; i < alloc.count; ++i) {
            if (alloc.nodes[i]) alloc.nodes[i]->unpoison();
        }
        root_.store(alloc.root_replacement);
        return true;

    case spec_op::SPLIT_LEAF_SKIP:
    case spec_op::PREFIX_LEAF_SKIP:
    case spec_op::EXTEND_LEAF_SKIP:
    case spec_op::SPLIT_LEAF_MULTI:
    case spec_op::PREFIX_LEAF_MULTI:
    case spec_op::BINARY_TO_LIST_LEAF:
    case spec_op::LIST_TO_POP_LEAF:
    case spec_op::POP_TO_FULL_LEAF:
    case spec_op::SPLIT_INTERIOR:
    case spec_op::PREFIX_INTERIOR:
    case spec_op::BINARY_TO_LIST_INTERIOR:
    case spec_op::LIST_TO_POP_INTERIOR:
    case spec_op::POP_TO_FULL_INTERIOR: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        // Unpoison before making visible
        [[assume(alloc.count >= 0 && alloc.count <= 8)]];
        for (int i = 0; i < alloc.count; ++i) {
            if (alloc.nodes[i]) alloc.nodes[i]->unpoison();
        }
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }

    // These should not reach commit_speculative
    case spec_op::EXISTS:
    case spec_op::RETRY:
    case spec_op::IN_PLACE_LEAF:
    case spec_op::IN_PLACE_INTERIOR:
    case spec_op::ADD_EOS_LEAF_MULTI:
    case spec_op::DEMOTE_LEAF_MULTI:
        return false;
    }
    return false;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::dealloc_speculation(pre_alloc& alloc) {
    [[assume(alloc.count >= 0 && alloc.count <= 8)]];
    // Iterate all allocated nodes - dealloc_node handles poison (won't recurse into borrowed children)
    for (int i = 0; i < alloc.count; ++i) {
        if (alloc.nodes[i]) {
            builder_.dealloc_node(alloc.nodes[i]);
            alloc.nodes[i] = nullptr;
        }
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
        // Writers cleanup at 1x threshold
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED) {
            ebr_cleanup();
        }
        
        reader_enter();
        
        constexpr int MAX_RETRIES = 7;
        
        for (int retry = 0; retry <= MAX_RETRIES; ++retry) {
            speculative_info spec = probe_speculative(root_.load(), kb);
            
            stat_attempt();

            // RETRY means concurrent write detected - try again
            if (spec.op == spec_op::RETRY) {
                continue;
            }

            if (spec.op == spec_op::EXISTS) {
                stat_success(retry);
                reader_exit();
                return {iterator(this, kb, value), false};
            }

            // In-place leaf - no allocation, brief lock
            if (spec.op == spec_op::IN_PLACE_LEAF) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_path(spec)) continue;
                
                ptr_t n = spec.target;
                unsigned char c = spec.c;
                
                if (n->is_binary()) {
                    auto* bn = n->template as_binary<true>();
                    if (bn->has(c)) continue;
                    if (bn->count() >= BINARY_MAX) continue;  // Re-probe for conversion
                    epoch_.fetch_add(1, std::memory_order_release);
                    n->bump_version();
                    bn->add_entry(c, value);
                    bn->update_capacity_flags();
                } else if (n->is_list()) {
                    auto* ln = n->template as_list<true>();
                    if (ln->has(c)) continue;
                    if (ln->count() >= LIST_MAX) continue;  // Re-probe for conversion
                    epoch_.fetch_add(1, std::memory_order_release);
                    n->bump_version();
                    ln->add_value(c, value);
                    ln->update_capacity_flags();
                } else if (n->is_pop()) {
                    auto* pn = n->template as_pop<true>();
                    if (pn->has(c)) continue;
                    if (pn->count() >= POP_MAX) continue;  // Re-probe for conversion
                    epoch_.fetch_add(1, std::memory_order_release);
                    n->bump_version();
                    pn->add_value(c, value);
                    pn->update_capacity_flags();
                } else {
                    auto* fn = n->template as_full<true>();
                    if (fn->has(c)) continue;
                    epoch_.fetch_add(1, std::memory_order_release);
                    n->bump_version();
                    fn->add_value_atomic(c, value);
                    fn->update_capacity_flags();
                }
                size_.fetch_add(1);
                stat_success(retry);
                reader_exit();
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
                        if (n->has_eos()) continue;
                        
                        epoch_.fetch_add(1, std::memory_order_release);
                        n->bump_version();
                        n->set_eos(value);
                        size_.fetch_add(1);
                        stat_success(retry);
                        reader_exit();
                        return {iterator(this, kb, value), true};
                    }
                } else {
                    // Adding child to interior - allocate child outside lock
                    ptr_t child = create_leaf_for_key(spec.remaining_key, value);
                    std::lock_guard<mutex_t> lock(mutex_);
                    if (!validate_path(spec)) { builder_.dealloc_node(child); continue; }
                    
                    ptr_t n = spec.target;
                    unsigned char c = spec.c;
                    
                    if (n->is_binary()) {
                        auto* bn = n->template as_binary<false>();
                        if (bn->has(c)) { builder_.dealloc_node(child); continue; }
                        if (bn->count() >= BINARY_MAX) {
                            builder_.dealloc_node(child);
                            continue;  // Re-probe for conversion
                        }
                        epoch_.fetch_add(1, std::memory_order_release);
                        n->bump_version();
                        bn->add_child(c, child);
                        bn->update_capacity_flags();
                    } else if (n->is_list()) {
                        auto* ln = n->template as_list<false>();
                        if (ln->has(c)) { builder_.dealloc_node(child); continue; }
                        if (ln->count() >= LIST_MAX) {
                            builder_.dealloc_node(child);
                            continue;  // Re-probe for conversion
                        }
                        epoch_.fetch_add(1, std::memory_order_release);
                        n->bump_version();
                        ln->add_child(c, child);
                        ln->update_capacity_flags();
                    } else if (n->is_pop()) {
                        auto* pn = n->template as_pop<false>();
                        if (pn->has(c)) { builder_.dealloc_node(child); continue; }
                        if (pn->count() >= POP_MAX) {
                            builder_.dealloc_node(child);
                            continue;  // Re-probe for conversion
                        }
                        epoch_.fetch_add(1, std::memory_order_release);
                        n->bump_version();
                        pn->add_child(c, child);
                        pn->update_capacity_flags();
                    } else if (n->is_full()) {
                        auto* fn = n->template as_full<false>();
                        if (fn->has(c)) { builder_.dealloc_node(child); continue; }
                        epoch_.fetch_add(1, std::memory_order_release);
                        n->bump_version();
                        fn->add_child_atomic(c, child);
                        fn->update_capacity_flags();
                    } else {
                        builder_.dealloc_node(child);
                        continue;
                    }
                    size_.fetch_add(1);
                    stat_success(retry);
                    reader_exit();
                    return {iterator(this, kb, value), true};
                }
            }

            // Complex ops that need full speculative path (ADD_EOS_LEAF_MULTI, DEMOTE_LEAF_MULTI)
            // Fall through to allocation-based speculative or fallback
            if (spec.op == spec_op::ADD_EOS_LEAF_MULTI || spec.op == spec_op::DEMOTE_LEAF_MULTI) {
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
                    epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
                    // Retire old node
                    if (spec.target) {
                        retire_node(spec.target);
                        if (retired_any) *retired_any = true;
                    }
                    size_.fetch_add(1);
                    stat_success(retry);
                    reader_exit();
                    return {iterator(this, kb, value), true};
                }
                dealloc_speculation(alloc);
                continue;
            }
        }
        
        // Fallback after MAX_RETRIES
        stat_fallback();
        {
            std::lock_guard<mutex_t> lock(mutex_);
            
            ptr_t root = root_.load();
            auto res = insert_impl(&root_, root, kb, value);
            
            if (!res.inserted) {
                if (retired_any && !res.old_nodes.empty()) *retired_any = true;
                for (auto* old : res.old_nodes) retire_node(old);
                reader_exit();
                return {iterator(this, kb, value), false};
            }
            
            epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
            if (res.new_node) {
                root_.store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
                root_.store(res.new_node);
            }
            if (retired_any && !res.old_nodes.empty()) *retired_any = true;
            for (auto* old : res.old_nodes) retire_node(old);
            size_.fetch_add(1);
            reader_exit();
            return {iterator(this, kb, value), true};
        }
    }
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

#include "tktrie_erase_probe.h"
