#pragma once

// This file contains implementation details for tktrie (erase implementation)
// It should only be included from tktrie_erase_probe.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::erase_locked(std::string_view kb) {
    // Helper to apply erase result (used by both THREADED and non-THREADED paths)
    auto apply_erase_result = [this](erase_result& res) -> bool {
        if (!res.erased) return false;
        if (res.deleted_subtree) {
            if constexpr (THREADED) root_.store(get_retry_sentinel<T, THREADED, Allocator>());
            root_.store(nullptr);
        } else if (res.new_node) {
            if constexpr (THREADED) root_.store(get_retry_sentinel<T, THREADED, Allocator>());
            root_.store(res.new_node);
        }
        for (auto* old : res.old_nodes) retire_node(old);
        size_.fetch_sub(1);
        return true;
    };
    
    if constexpr (!THREADED) {
        std::lock_guard<mutex_t> lock(mutex_);
        auto res = erase_impl(&root_, root_.load(), kb);
        return apply_erase_result(res);
    } else {
        maybe_reclaim();
        
        auto& ebr_slot_ref = get_ebr_slot();
        
        while (true) {
            auto guard = ebr_slot_ref.get_guard();
            erase_spec_info info = probe_erase(root_.load(), kb);

            // Fast path: IN_PLACE operations - direct modification
            if (info.op == erase_op::IN_PLACE_LEAF_LIST) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (do_inplace_leaf_list_erase(info.target, info.c, info.target_version)) {
                    size_.fetch_sub(1);
                    return true;
                }
                continue;
            }
            
            if (info.op == erase_op::IN_PLACE_LEAF_FULL) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (do_inplace_leaf_full_erase(info.target, info.c, info.target_version)) {
                    size_.fetch_sub(1);
                    return true;
                }
                continue;
            }

            // Slow path: NOT_FOUND or structural changes needed
            {
                std::lock_guard<mutex_t> lock(mutex_);
                auto res = erase_impl(&root_, root_.load(), kb);
                return apply_erase_result(res);
            }
        }
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::erase_impl(
    atomic_ptr*, ptr_t n, std::string_view key) {
    erase_result res;
    if (!n || n->is_poisoned()) return res;
    if (n->is_leaf()) return erase_from_leaf(n, key);
    return erase_from_interior(n, key);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::erase_from_leaf(
    ptr_t leaf, std::string_view key) {
    erase_result res;
    std::string_view skip = get_skip(leaf);
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) return res;
    key.remove_prefix(m);

    if (leaf->is_skip()) {
        if (!key.empty()) return res;
        res.erased = true;
        res.deleted_subtree = true;
        res.old_nodes.push_back(leaf);
        return res;
    }

    // LIST or FULL leaf
    if (key.size() != 1) return res;
    unsigned char c = static_cast<unsigned char>(key[0]);

    if (leaf->is_list()) [[likely]] {
        int idx = leaf->as_list()->chars.find(c);
        if (idx < 0) return res;

        int count = leaf->as_list()->chars.count();
        if (count == 1) {
            res.erased = true;
            res.deleted_subtree = true;
            res.old_nodes.push_back(leaf);
            return res;
        }

        leaf->bump_version();
        leaf->as_list()->shift_leaf_values_down(idx);
        res.erased = true;
        return res;
    }

    // FULL
    if (!leaf->as_full()->valid.template atomic_test<THREADED>(c)) return res;
    leaf->bump_version();
    leaf->as_full()->template remove_leaf_entry<THREADED>(c);
    res.erased = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::erase_from_interior(
    ptr_t n, std::string_view key) {
    erase_result res;
    std::string_view skip = get_skip(n);
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) return res;
    key.remove_prefix(m);

    if (key.empty()) {
        T* p = get_eos_ptr(n);
        if (!p) return res;
        n->bump_version();
        delete p;
        set_eos_ptr(n, nullptr);
        res.erased = true;
        return try_collapse_interior(n);
    }

    unsigned char c = static_cast<unsigned char>(key[0]);
    ptr_t child = find_child(n, c);
    if (!child) return res;

    auto child_res = erase_impl(get_child_slot(n, c), child, key.substr(1));
    if (!child_res.erased) return res;

    if (child_res.deleted_subtree) {
        return try_collapse_after_child_removal(n, c, child_res);
    }

    if (child_res.new_node) {
        n->bump_version();
        // Set sentinel to block readers, then store new value
        if constexpr (THREADED) {
            get_child_slot(n, c)->store(get_retry_sentinel<T, THREADED, Allocator>());
        }
        get_child_slot(n, c)->store(child_res.new_node);
    }
    res.erased = true;
    res.old_nodes = std::move(child_res.old_nodes);
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::try_collapse_interior(ptr_t n) {
    erase_result res;
    res.erased = true;

    T* eos = get_eos_ptr(n);
    if (eos) return res;

    int child_cnt = n->child_count();
    if (child_cnt == 0) {
        res.deleted_subtree = true;
        res.old_nodes.push_back(n);
        return res;
    }
    if (child_cnt != 1) return res;

    unsigned char c = 0;
    ptr_t child = nullptr;
    if (n->is_list()) {
        c = n->as_list()->chars.char_at(0);
        child = n->as_list()->children[0].load();
    } else if (n->is_full()) {
        c = n->as_full()->valid.first();
        child = n->as_full()->children[c].load();
    }
    if (!child) return res;

    return collapse_single_child(n, c, child, res);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::try_collapse_after_child_removal(
    ptr_t n, unsigned char removed_c, erase_result& child_res) {
    erase_result res;
    res.old_nodes = std::move(child_res.old_nodes);
    res.erased = true;

    T* eos = get_eos_ptr(n);
    int remaining = n->child_count();

    if (n->is_list()) {
        int idx = n->as_list()->chars.find(removed_c);
        if (idx >= 0) remaining--;
    } else if (n->is_full()) {
        if (n->as_full()->valid.template atomic_test<THREADED>(removed_c)) remaining--;
    }

    if ((!eos) & (remaining == 0)) {
        res.deleted_subtree = true;
        res.old_nodes.push_back(n);
        return res;
    }

    if (n->is_list()) {
        int idx = n->as_list()->chars.find(removed_c);
        if (idx >= 0) {
            n->bump_version();
            n->as_list()->shift_children_down(idx);
        }
    } else if (n->is_full()) {
        n->bump_version();
        n->as_full()->template remove_child<THREADED>(removed_c);
    }

    bool can_collapse = false;
    unsigned char c = 0;
    ptr_t child = nullptr;

    if (n->is_list() && n->as_list()->chars.count() == 1 && !eos) {
        c = n->as_list()->chars.char_at(0);
        child = n->as_list()->children[0].load();
        can_collapse = (child != nullptr);
    } else if (n->is_full() && !eos) {
        int cnt = n->as_full()->valid.count();
        if (cnt == 1) {
            c = n->as_full()->valid.first();
            child = n->as_full()->children[c].load();
            can_collapse = (child != nullptr);
        }
    }

    if (can_collapse) {
        return collapse_single_child(n, c, child, res);
    }
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::collapse_single_child(
    ptr_t n, unsigned char c, ptr_t child, erase_result& res) {
    std::string new_skip(get_skip(n));
    new_skip.push_back(static_cast<char>(c));
    new_skip.append(get_skip(child));

    ptr_t merged;
    if (child->is_leaf()) {
        if (child->is_skip()) {
            merged = builder_.make_leaf_skip(new_skip, child->as_skip()->leaf_value);
        } else if (child->is_list()) [[likely]] {
            merged = builder_.make_leaf_list(new_skip);
            child->as_list()->copy_leaf_values_to(merged->as_list());
        } else {
            merged = builder_.make_leaf_full(new_skip);
            child->as_full()->copy_leaf_values_to(merged->as_full());
        }
    } else {
        // Interior: LIST or FULL only
        if (child->is_list()) [[likely]] {
            merged = builder_.make_interior_list(new_skip);
            child->as_list()->move_interior_to(merged->as_list());
        } else {
            merged = builder_.make_interior_full(new_skip);
            child->as_full()->move_interior_to(merged->as_full());
        }
    }

    res.new_node = merged;
    res.old_nodes.push_back(n);
    res.old_nodes.push_back(child);
    return res;
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum
