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
        if constexpr (THREADED) {
            epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
        }
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
        // Writers cleanup at 1x threshold
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED) {
            ebr_cleanup();
        }
        
        size_t slot = reader_enter();
        
        static constexpr int MAX_RETRIES = 7;
        
        for (int retry = 0; retry <= MAX_RETRIES; ++retry) {
            erase_spec_info info = probe_erase(root_.load(), kb);

            if (info.op == erase_op::NOT_FOUND) {
                reader_exit(slot);
                return {false, false};
            }

            // In-place operations - no allocation needed
            if (info.op == erase_op::IN_PLACE_LEAF_LIST) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_erase_path(info)) continue;
                if (do_inplace_leaf_list_erase(info.target, info.c, info.target_version)) {
                    epoch_.fetch_add(1, std::memory_order_release);
                    size_.fetch_sub(1);
                    reader_exit(slot);
                    return {true, false};
                }
                continue;
            }
            
            if (info.op == erase_op::IN_PLACE_LEAF_POP) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_erase_path(info)) continue;
                if (do_inplace_leaf_pop_erase(info.target, info.c, info.target_version)) {
                    epoch_.fetch_add(1, std::memory_order_release);
                    size_.fetch_sub(1);
                    reader_exit(slot);
                    return {true, false};
                }
                continue;
            }
            
            if (info.op == erase_op::IN_PLACE_LEAF_FULL) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_erase_path(info)) continue;
                if (do_inplace_leaf_full_erase(info.target, info.c, info.target_version)) {
                    epoch_.fetch_add(1, std::memory_order_release);
                    size_.fetch_sub(1);
                    reader_exit(slot);
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
                    epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
                    
                    // Determine if target should be retired based on operation type
                    bool should_retire_target = false;
                    switch (info.op) {
                        case erase_op::DELETE_SKIP_LEAF:
                        case erase_op::DELETE_LAST_LEAF_ENTRY:
                        case erase_op::BINARY_TO_SKIP:
                        case erase_op::DELETE_CHILD_COLLAPSE:
                            // Node was removed/replaced
                            should_retire_target = true;
                            break;
                        case erase_op::DELETE_EOS_INTERIOR:
                            // Only retire if we replaced (collapse case), not in-place clear
                            should_retire_target = (alloc.replacement != nullptr);
                            break;
                        case erase_op::DELETE_CHILD_NO_COLLAPSE:
                            // In-place removal, target stays in tree
                            should_retire_target = false;
                            break;
                        default:
                            break;
                    }
                    
                    bool retired_any = false;
                    if (should_retire_target && info.target) {
                        retire_node(info.target);
                        retired_any = true;
                    }
                    if (info.collapse_child) {
                        // Collapse child is always retired when set
                        retire_node(info.collapse_child);
                        retired_any = true;
                    }
                    size_.fetch_sub(1);
                    reader_exit(slot);
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
            reader_exit(slot);
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

    // SKIP leaf - delete entire node
    if (leaf->is_skip()) {
        if (!key.empty()) return res;
        res.erased = true;
        res.deleted_subtree = true;
        res.old_nodes.push_back(leaf);
        return res;
    }

    // Multi-entry leaf (BINARY, LIST, POP, FULL)
    if (key.size() != 1) return res;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (!leaf->has_leaf_entry(c)) return res;
    
    int count = leaf->leaf_entry_count();
    
    // If only 1 entry, delete the subtree
    if (count == 1) {
        res.erased = true;
        res.deleted_subtree = true;
        res.old_nodes.push_back(leaf);
        return res;
    }
    
    using ops = trie_ops<T, THREADED, Allocator, FIXED_LEN>;
    
    // BINARY with 2 entries -> convert to SKIP
    if (leaf->is_binary() && count == 2) {
        auto helper_res = ops::template binary_to_skip<false>(leaf, c, builder_, static_cast<void*>(nullptr));
        res.new_node = helper_res.new_node;
        if (helper_res.old_node) res.old_nodes.push_back(helper_res.old_node);
        res.erased = helper_res.success;
        return res;
    }
    
    // Use helper for in-place remove or downgrade
    auto helper_res = ops::template remove_entry<false, true>(leaf, c, builder_, static_cast<void*>(nullptr));
    res.new_node = helper_res.new_node;
    if (helper_res.old_node) res.old_nodes.push_back(helper_res.old_node);
    res.erased = helper_res.success || helper_res.in_place;
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
            if (!n->has_eos()) return res;
            n->bump_version();
            n->clear_eos();
            res.erased = true;
            return try_collapse_interior(n);
        }
    }

    unsigned char c = static_cast<unsigned char>(key[0]);
    ptr_t child = n->get_child(c);
    
    if (!child || builder_t::is_sentinel(child)) return res;

    auto child_res = erase_impl(n->get_child_slot(c), child, key.substr(1));
    if (!child_res.erased) return res;

    if (child_res.deleted_subtree) {
        return try_collapse_after_child_removal(n, c, child_res);
    }

    if (child_res.new_node) {
        n->bump_version();
        if constexpr (THREADED) {
            n->get_child_slot(c)->store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
        }
        n->get_child_slot(c)->store(child_res.new_node);
    }
    res.erased = true;
    res.old_nodes = std::move(child_res.old_nodes);
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::try_collapse_interior(ptr_t n) {
    erase_result res;
    res.erased = true;

    bool eos_exists = n->has_eos();
    if (eos_exists) return res;

    int child_cnt = n->child_count();
    if (child_cnt == 0) {
        res.deleted_subtree = true;
        res.old_nodes.push_back(n);
        return res;
    }
    if (child_cnt != 1) return res;

    auto [c, child] = n->first_child_info();
    if (!child || builder_t::is_sentinel(child)) return res;

    return collapse_single_child(n, c, child, res);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::try_collapse_after_child_removal(
    ptr_t n, unsigned char removed_c, erase_result& child_res) {
    erase_result res;
    res.old_nodes = std::move(child_res.old_nodes);
    res.erased = true;

    bool eos_exists = n->has_eos();
    int remaining = n->child_count();
    int eos_count = eos_exists ? 1 : 0;

    // Count remaining children (excluding the one being removed)
    if (n->has_child(removed_c)) remaining--;

    int total_remaining = remaining + eos_count;

    // Delete if empty
    if (total_remaining == 0) {
        res.deleted_subtree = true;
        res.old_nodes.push_back(n);
        return res;
    }

    using ops = trie_ops<T, THREADED, Allocator, FIXED_LEN>;
    
    // Use helper to remove child (handles downgrade if needed)
    auto helper_res = ops::template remove_entry<false, false>(n, removed_c, builder_, static_cast<void*>(nullptr));
    if (helper_res.new_node) {
        res.new_node = helper_res.new_node;
        res.old_nodes.push_back(n);
        
        // Check if we can collapse the new node (after downgrade)
        if (helper_res.new_node->child_count() == 1 && !eos_exists) {
            auto [c, child] = helper_res.new_node->first_child_info();
            if (child && !builder_t::is_sentinel(child)) {
                return collapse_single_child(helper_res.new_node, c, child, res);
            }
        }
        return res;
    }

    // Check for collapse opportunity (1 child, no EOS) after in-place removal
    if (!eos_exists && remaining == 1) {
        auto [c, child] = n->first_child_info();
        if (child && !builder_t::is_sentinel(child)) {
            return collapse_single_child(n, c, child, res);
        }
    }
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::collapse_single_child(
    ptr_t n, unsigned char c, ptr_t child, erase_result& res) {
    std::string new_skip(n->skip_str());
    new_skip.push_back(static_cast<char>(c));
    new_skip.append(child->skip_str());

    using ops = trie_ops<T, THREADED, Allocator, FIXED_LEN>;
    
    ptr_t merged;
    if (child->is_leaf()) {
        if (child->is_skip()) {
            T val{};
            child->as_skip()->value.try_read(val);
            merged = builder_.make_leaf_skip(new_skip, val);
        } else {
            merged = ops::clone_leaf_with_skip(child, new_skip, builder_);
        }
    } else {
        merged = ops::clone_interior_with_skip(child, new_skip, builder_);
    }

    res.new_node = merged;
    res.old_nodes.push_back(n);
    res.old_nodes.push_back(child);
    return res;
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum
