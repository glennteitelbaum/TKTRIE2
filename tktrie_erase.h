#pragma once

// This file contains erase implementation
// It should only be included from tktrie_erase_probe.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

TKTRIE_TEMPLATE
std::pair<bool, bool> TKTRIE_CLASS::erase_locked(std::string_view kb) {
    // Returns (erased, retired_any)
    auto apply_erase_result = [this](erase_result& res) -> std::pair<bool, bool> {
        if (!res.erased) return {false, false};
        if (res.deleted_subtree) {
            if constexpr (THREADED) root_.store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
            root_.store(nullptr);
        } else if (res.new_node) {
            if constexpr (THREADED) root_.store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
            root_.store(res.new_node);
        }
        bool retired_any = !res.old_nodes.empty();
        for (auto* old : res.old_nodes) retire_node(old);
        size_.fetch_sub(1);
        return {true, retired_any};
    };
    
    if constexpr (!THREADED) {
        std::lock_guard<mutex_t> lock(mutex_);
        auto res = erase_impl(&root_, root_.load(), kb);
        return apply_erase_result(res);
    } else {
        maybe_reclaim();
        
        auto& ebr_slot_ref = get_ebr_slot();
        static constexpr int MAX_RETRIES = 7;
        
        for (int retry = 0; retry <= MAX_RETRIES; ++retry) {
            auto guard = ebr_slot_ref.get_guard();
            erase_spec_info info = probe_erase(root_.load(), kb);

            if (info.op == erase_op::NOT_FOUND) {
                return {false, false};
            }

            // In-place operations - no allocation needed
            if (info.op == erase_op::IN_PLACE_LEAF_LIST) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_erase_path(info)) continue;
                if (do_inplace_leaf_list_erase(info.target, info.c, info.target_version)) {
                    size_.fetch_sub(1);
                    return {true, false};
                }
                continue;
            }
            
            if (info.op == erase_op::IN_PLACE_LEAF_FULL) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_erase_path(info)) continue;
                if (do_inplace_leaf_full_erase(info.target, info.c, info.target_version)) {
                    size_.fetch_sub(1);
                    return {true, false};
                }
                continue;
            }

            // Speculative path: allocate outside lock, brief lock for commit
            erase_pre_alloc alloc = allocate_erase_speculative(info);
            
            {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_erase_path(info)) {
                    dealloc_erase_speculation(alloc);
                    continue;
                }
                
                if (commit_erase_speculative(info, alloc)) {
                    // Retire old nodes
                    bool retired_any = false;
                    if (info.target) {
                        retire_node(info.target);
                        retired_any = true;
                    }
                    if (info.collapse_child) {
                        retire_node(info.collapse_child);
                        retired_any = true;
                    }
                    size_.fetch_sub(1);
                    return {true, retired_any};
                }
                dealloc_erase_speculation(alloc);
                continue;
            }
        }
        
        // Fallback after MAX_RETRIES
        {
            std::lock_guard<mutex_t> lock(mutex_);
            auto res = erase_impl(&root_, root_.load(), kb);
            return apply_erase_result(res);
        }
    }
}


TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::erase_impl(
    atomic_ptr*, ptr_t n, std::string_view key) {
    erase_result res;
    if (!n || n->is_poisoned() || builder_t::is_sentinel(n)) return res;
    if (n->is_leaf()) return erase_from_leaf(n, key);
    return erase_from_interior(n, key);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::erase_from_leaf(
    ptr_t leaf, std::string_view key) {
    erase_result res;
    std::string_view skip = leaf->skip_str();
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

    if (key.size() != 1) return res;
    unsigned char c = static_cast<unsigned char>(key[0]);

    if (leaf->is_list()) [[likely]] {
        auto* ln = leaf->template as_list<true>();
        if (!ln->has(c)) return res;

        if (ln->count() == 1) {
            res.erased = true;
            res.deleted_subtree = true;
            res.old_nodes.push_back(leaf);
            return res;
        }

        leaf->bump_version();
        ln->remove_value(c);
        res.erased = true;
        return res;
    }

    auto* fn = leaf->template as_full<true>();
    if (!fn->has(c)) return res;
    leaf->bump_version();
    fn->remove_value(c);
    res.erased = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::erase_from_interior(
    ptr_t n, std::string_view key) {
    erase_result res;
    std::string_view skip = n->skip_str();
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) return res;
    key.remove_prefix(m);

    if (key.empty()) {
        if constexpr (FIXED_LEN > 0) {
            return res;
        } else {
            if (!n->has_eos(false)) return res;
            n->bump_version();
            n->clear_eos(false);
            res.erased = true;
            return try_collapse_interior(n);
        }
    }

    unsigned char c = static_cast<unsigned char>(key[0]);
    ptr_t child = n->get_child(false, c);
    
    if (!child || builder_t::is_sentinel(child)) return res;

    auto child_res = erase_impl(n->get_child_slot(false, c), child, key.substr(1));
    if (!child_res.erased) return res;

    if (child_res.deleted_subtree) {
        return try_collapse_after_child_removal(n, c, child_res);
    }

    if (child_res.new_node) {
        n->bump_version();
        if constexpr (THREADED) {
            n->get_child_slot(false, c)->store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
        }
        n->get_child_slot(false, c)->store(child_res.new_node);
    }
    res.erased = true;
    res.old_nodes = std::move(child_res.old_nodes);
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::try_collapse_interior(ptr_t n) {
    erase_result res;
    res.erased = true;

    bool eos_exists = n->has_eos(false);
    if (eos_exists) return res;

    int child_cnt = n->entry_count(false);
    if (child_cnt == 0) {
        res.deleted_subtree = true;
        res.old_nodes.push_back(n);
        return res;
    }
    if (child_cnt != 1) return res;

    unsigned char c = 0;
    ptr_t child = nullptr;
    if (n->is_list()) {
        auto* ln = n->template as_list<false>();
        c = ln->chars.char_at(0);
        child = ln->children[0].load();
    } else if (n->is_full()) {
        auto* fn = n->template as_full<false>();
        c = fn->valid.first();
        child = fn->children[c].load();
    }
    if (!child || builder_t::is_sentinel(child)) return res;

    return collapse_single_child(n, c, child, res);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::try_collapse_after_child_removal(
    ptr_t n, unsigned char removed_c, erase_result& child_res) {
    erase_result res;
    res.old_nodes = std::move(child_res.old_nodes);
    res.erased = true;

    bool eos_exists = n->has_eos(false);
    int remaining = n->entry_count(false);

    if (n->is_list()) {
        auto* ln = n->template as_list<false>();
        if (ln->has(removed_c)) remaining--;
    } else if (n->is_full()) {
        auto* fn = n->template as_full<false>();
        if (fn->has(removed_c)) remaining--;
    }

    if ((!eos_exists) & (remaining == 0)) {
        res.deleted_subtree = true;
        res.old_nodes.push_back(n);
        return res;
    }

    if (n->is_list()) {
        auto* ln = n->template as_list<false>();
        if (ln->has(removed_c)) {
            n->bump_version();
            ln->remove_child(removed_c);
        }
    } else if (n->is_full()) {
        auto* fn = n->template as_full<false>();
        if (fn->has(removed_c)) {
            n->bump_version();
            fn->remove_child(removed_c);
        }
    }

    bool can_collapse = false;
    unsigned char c = 0;
    ptr_t child = nullptr;

    if (n->is_list()) {
        auto* ln = n->template as_list<false>();
        if (ln->count() == 1 && !eos_exists) {
            c = ln->chars.char_at(0);
            child = ln->children[0].load();
            can_collapse = (child != nullptr && !builder_t::is_sentinel(child));
        }
    } else if (n->is_full() && !eos_exists) {
        auto* fn = n->template as_full<false>();
        if (fn->count() == 1) {
            c = fn->valid.first();
            child = fn->children[c].load();
            can_collapse = (child != nullptr && !builder_t::is_sentinel(child));
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
    std::string new_skip(n->skip_str());
    new_skip.push_back(static_cast<char>(c));
    new_skip.append(child->skip_str());

    ptr_t merged;
    if (child->is_leaf()) {
        if (child->is_skip()) {
            T val;
            child->as_skip()->value.try_read(val);
            merged = builder_.make_leaf_skip(new_skip, val);
        } else if (child->is_list()) [[likely]] {
            merged = builder_.make_leaf_list(new_skip);
            child->template as_list<true>()->copy_values_to(merged->template as_list<true>());
        } else {
            merged = builder_.make_leaf_full(new_skip);
            child->template as_full<true>()->copy_values_to(merged->template as_full<true>());
        }
    } else {
        if (child->is_list()) [[likely]] {
            merged = builder_.make_interior_list(new_skip);
            child->template as_list<false>()->move_interior_to(merged->template as_list<false>());
        } else {
            merged = builder_.make_interior_full(new_skip);
            child->template as_full<false>()->move_interior_to(merged->template as_full<false>());
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
