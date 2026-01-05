#pragma once

// This file contains implementation details for tktrie (erase implementation)
// It should only be included from tktrie_erase_probe.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::erase_locked(std::string_view kb) {
    if constexpr (!THREADED) {
        std::lock_guard<mutex_t> lock(mutex_);

        ptr_t root = root_.load();
        auto res = erase_impl(&root_, root, kb);

        if (!res.erased) return false;

        if (res.deleted_subtree) root_.store(nullptr);
        else if (res.new_node) root_.store(res.new_node);

        for (auto* old : res.old_nodes) retire_node(old);
        --size_;

        return true;
    } else {
        // Try to reclaim old nodes BEFORE taking a guard
        ebr_global::instance().try_reclaim();
        
        auto& ebr_slot_ref = get_ebr_slot();
        
        while (true) {
            // Keep guard active for entire operation to protect probe results
            auto guard = ebr_slot_ref.get_guard();
            
            erase_spec_info info = probe_erase(root_.load(), kb);
            if (info.op != erase_op::NOT_FOUND) {
                capture_parent_collapse_info(info);
            }

            if (info.op == erase_op::NOT_FOUND) {
                // Re-probe under lock to be sure key doesn't exist
                // The initial probe might have seen stale data
                std::lock_guard<mutex_t> lock(mutex_);
                ptr_t root = root_.load();
                auto res = erase_impl(&root_, root, kb);
                if (!res.erased) {
                    return false;  // Confirmed: key doesn't exist
                }
                // Key actually existed - apply the erase result
                if (res.deleted_subtree) root_.store(nullptr);
                else if (res.new_node) root_.store(res.new_node);
                for (auto* old : res.old_nodes) retire_node(old);
                size_.fetch_sub(1);
                return true;
            }

            // =================================================================
            // IN_PLACE operations - simple modify in place
            // =================================================================
            if ((info.op == erase_op::IN_PLACE_LEAF_LIST) |
                (info.op == erase_op::IN_PLACE_LEAF_FULL) |
                (info.op == erase_op::IN_PLACE_INTERIOR_LIST) |
                (info.op == erase_op::IN_PLACE_INTERIOR_FULL)) {

                std::lock_guard<mutex_t> lock(mutex_);

                if (!validate_erase_path(info)) continue;

                bool success = false;
                switch (info.op) {
                    case erase_op::IN_PLACE_LEAF_LIST:
                        success = do_inplace_leaf_list_erase(info.target, info.c, info.target_version);
                        break;
                    case erase_op::IN_PLACE_LEAF_FULL:
                        success = do_inplace_leaf_full_erase(info.target, info.c, info.target_version);
                        break;
                    case erase_op::IN_PLACE_INTERIOR_LIST:
                        success = do_inplace_interior_list_erase(info.target, info.c, info.target_version);
                        break;
                    case erase_op::IN_PLACE_INTERIOR_FULL:
                        success = do_inplace_interior_full_erase(info.target, info.c, info.target_version);
                        break;
                    default:
                        break;
                }

                if (success) {
                    size_.fetch_sub(1);
                    return true;
                }
                continue;  // Version changed, retry
            }

            // =================================================================
            // DELETE_LEAF_* - remove leaf node from parent
            // =================================================================
            if ((info.op == erase_op::DELETE_LEAF_EOS) |
                (info.op == erase_op::DELETE_LEAF_SKIP) |
                (info.op == erase_op::DELETE_LAST_LEAF_LIST)) {

                // Pre-allocate merge node if parent collapse is possible
                erase_pre_alloc alloc = allocate_erase_speculative(info);

                std::lock_guard<mutex_t> lock(mutex_);

                if (!validate_erase_path(info)) {
                    dealloc_erase_speculation(alloc);
                    continue;
                }

                // Root deletion
                if (info.path_len <= 1) {
                    root_.store(nullptr);
                    retire_node(info.target);
                    size_.fetch_sub(1);
                    dealloc_erase_speculation(alloc);
                    return true;
                }

                ptr_t parent = info.path[info.path_len - 2].node;
                unsigned char edge = info.path[info.path_len - 1].edge;

                // Verify slot still points to target
                atomic_ptr* slot_ptr = get_child_slot(parent, edge);
                if (!slot_ptr || slot_ptr->load() != info.target) {
                    dealloc_erase_speculation(alloc);
                    continue;  // Slot changed, retry
                }

                // Check if we can do parent collapse
                if (alloc.parent_merged && info.parent_collapse_child) {
                    // Verify parent collapse child hasn't changed
                    if (info.parent_collapse_child->version() != info.parent_collapse_child_version) {
                        dealloc_erase_speculation(alloc);
                        continue;
                    }
                    
                    fill_collapse_node(alloc.parent_merged, info.parent_collapse_child);

                    if (info.path_len <= 2) {
                        root_.store(alloc.parent_merged);
                    } else {
                        ptr_t grandparent = info.path[info.path_len - 3].node;
                        unsigned char parent_edge = info.path[info.path_len - 2].edge;
                        atomic_ptr* gp_slot = get_child_slot(grandparent, parent_edge);
                        if (gp_slot) {
                            // CRITICAL: Bump version BEFORE storing new child
                            grandparent->bump_version();
                            gp_slot->store(alloc.parent_merged);
                        }
                    }
                    retire_node(info.target);
                    retire_node(parent);
                    retire_node(info.parent_collapse_child);
                    alloc.parent_merged = nullptr;
                    size_.fetch_sub(1);
                    dealloc_erase_speculation(alloc);
                    return true;
                }

                // Simple removal from parent
                bool removed = false;
                if (parent->is_list()) {
                    removed = do_inplace_interior_list_erase(parent, edge, info.path[info.path_len - 2].version);
                } else if (parent->is_full()) {
                    removed = do_inplace_interior_full_erase(parent, edge, info.path[info.path_len - 2].version);
                }

                if (!removed) {
                    dealloc_erase_speculation(alloc);
                    continue;  // Parent version changed, retry
                }

                retire_node(info.target);
                size_.fetch_sub(1);
                dealloc_erase_speculation(alloc);
                return true;
            }

            // =================================================================
            // DELETE_EOS_INTERIOR - remove eos value from interior node
            // =================================================================
            if (info.op == erase_op::DELETE_EOS_INTERIOR) {
                std::lock_guard<mutex_t> lock(mutex_);

                if (!validate_erase_path(info)) continue;

                // Re-verify eos_ptr exists
                T* p = get_eos_ptr(info.target);
                if (!p) continue;  // Already deleted, retry

                // CRITICAL: Bump version BEFORE modifying data
                info.target->bump_version();
                
                // Delete the eos value
                delete p;
                set_eos_ptr(info.target, nullptr);

                // Check if node needs cleanup (no children and no eos)
                int child_cnt = info.target->child_count();
                if (child_cnt == 0) {
                    // Node is now empty - remove from parent
                    if (info.path_len <= 1) {
                        root_.store(nullptr);
                        retire_node(info.target);
                    } else {
                        ptr_t parent = info.path[info.path_len - 2].node;
                        unsigned char edge = info.path[info.path_len - 1].edge;
                        
                        // Remove from parent
                        if (parent->is_list()) {
                            do_inplace_interior_list_erase(parent, edge, parent->version());
                        } else if (parent->is_full()) {
                            do_inplace_interior_full_erase(parent, edge, parent->version());
                        }
                        retire_node(info.target);
                    }
                }
                // If child_cnt > 0, node stays in tree (just without eos value)

                size_.fetch_sub(1);
                return true;
            }

            // =================================================================
            // COLLAPSE_AFTER_REMOVE - remove eos and collapse with single child
            // =================================================================
            if (info.op == erase_op::COLLAPSE_AFTER_REMOVE) {
                erase_pre_alloc alloc = allocate_erase_speculative(info);
                
                if (!alloc.merged) {
                    dealloc_erase_speculation(alloc);
                    // Fall back to erase_impl for complex cases
                    std::lock_guard<mutex_t> lock(mutex_);
                    ptr_t root = root_.load();
                    auto res = erase_impl(&root_, root, kb);
                    if (!res.erased) return false;
                    if (res.deleted_subtree) root_.store(nullptr);
                    else if (res.new_node) root_.store(res.new_node);
                    for (auto* old : res.old_nodes) retire_node(old);
                    size_.fetch_sub(1);
                    return true;
                }

                std::lock_guard<mutex_t> lock(mutex_);

                if (!validate_erase_path(info)) {
                    dealloc_erase_speculation(alloc);
                    continue;
                }

                // Verify collapse child hasn't changed
                if (info.collapse_child->version() != info.collapse_child_version) {
                    dealloc_erase_speculation(alloc);
                    continue;
                }

                // Re-verify eos exists BEFORE any modifications
                T* p = get_eos_ptr(info.target);
                if (!p) {
                    // Another thread already deleted it - retry to get correct result
                    dealloc_erase_speculation(alloc);
                    continue;
                }
                
                // CRITICAL: Bump target version BEFORE deleting eos
                info.target->bump_version();
                
                delete p;
                set_eos_ptr(info.target, nullptr);

                // Fill and install merged node
                fill_collapse_node(alloc.merged, info.collapse_child);

                if (info.path_len <= 1) {
                    root_.store(alloc.merged);
                } else {
                    ptr_t parent = info.path[info.path_len - 2].node;
                    unsigned char edge = info.path[info.path_len - 1].edge;
                    atomic_ptr* slot_ptr = get_child_slot(parent, edge);
                    if (slot_ptr) {
                        // CRITICAL: Bump version BEFORE storing new child
                        parent->bump_version();
                        slot_ptr->store(alloc.merged);
                    }
                }

                retire_node(info.target);
                retire_node(info.collapse_child);
                alloc.merged = nullptr;
                size_.fetch_sub(1);
                dealloc_erase_speculation(alloc);
                return true;
            }

            // =================================================================
            // Fallback - use erase_impl for any unhandled cases
            // =================================================================
            {
                std::lock_guard<mutex_t> lock(mutex_);
                ptr_t root = root_.load();
                auto res = erase_impl(&root_, root, kb);
                if (!res.erased) return false;
                if (res.deleted_subtree) root_.store(nullptr);
                else if (res.new_node) root_.store(res.new_node);
                for (auto* old : res.old_nodes) retire_node(old);
                size_.fetch_sub(1);
                return true;
            }
        }
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::erase_impl(
    atomic_ptr*, ptr_t n, std::string_view key) {
    erase_result res;
    if (!n) return res;
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

    if (leaf->is_eos()) {
        if (!key.empty()) return res;
        res.erased = true;
        res.deleted_subtree = true;
        res.old_nodes.push_back(leaf);
        return res;
    }

    if (leaf->is_skip()) {
        if (!key.empty()) return res;
        res.erased = true;
        res.deleted_subtree = true;
        res.old_nodes.push_back(leaf);
        return res;
    }

    if (key.size() != 1) return res;
    unsigned char c = static_cast<unsigned char>(key[0]);

    if (leaf->is_list()) {
        int idx = leaf->as_list()->chars.find(c);
        if (idx < 0) return res;

        int count = leaf->as_list()->chars.count();
        if (count == 1) {
            res.erased = true;
            res.deleted_subtree = true;
            res.old_nodes.push_back(leaf);
            return res;
        }

        // CRITICAL: Bump version BEFORE modifying data
        leaf->bump_version();
        
        for (int i = idx; i < count - 1; ++i) {
            leaf->as_list()->leaf_values[i] = leaf->as_list()->leaf_values[i + 1];
        }
        // Destroy the last element that's now extra
        leaf->as_list()->destroy_leaf_value(count - 1);
        leaf->as_list()->chars.remove_at(idx);
        res.erased = true;
        return res;
    }

    // FULL
    if (!leaf->as_full()->valid.test(c)) return res;
    
    // CRITICAL: Bump version BEFORE modifying data
    leaf->bump_version();
    
    leaf->as_full()->destroy_leaf_value(c);
    leaf->as_full()->valid.template atomic_clear<THREADED>(c);
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
        
        // CRITICAL: Bump version BEFORE modifying data
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
        // CRITICAL: Bump version BEFORE storing new node
        n->bump_version();
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
        if (n->as_full()->valid.test(removed_c)) remaining--;
    }

    if ((!eos) & (remaining == 0)) {
        res.deleted_subtree = true;
        res.old_nodes.push_back(n);
        return res;
    }

    if (n->is_list()) {
        int idx = n->as_list()->chars.find(removed_c);
        if (idx >= 0) {
            // CRITICAL: Bump version BEFORE modifying data
            n->bump_version();
            
            int count = n->as_list()->chars.count();
            for (int i = idx; i < count - 1; ++i) {
                n->as_list()->children[i].store(n->as_list()->children[i + 1].load());
            }
            n->as_list()->children[count - 1].store(nullptr);
            n->as_list()->chars.remove_at(idx);
        }
    } else if (n->is_full()) {
        // CRITICAL: Bump version BEFORE modifying data
        n->bump_version();
        
        n->as_full()->valid.template atomic_clear<THREADED>(removed_c);
        n->as_full()->children[removed_c].store(nullptr);
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
        if (child->is_eos()) {
            merged = builder_.make_leaf_skip(new_skip, child->as_eos()->leaf_value);
        } else if (child->is_skip()) {
            merged = builder_.make_leaf_skip(new_skip, child->as_skip()->leaf_value);
        } else if (child->is_list()) {
            merged = builder_.make_leaf_list(new_skip);
            merged->as_list()->chars = child->as_list()->chars;
            for (int i = 0; i < child->as_list()->chars.count(); ++i) {
                merged->as_list()->construct_leaf_value(i, child->as_list()->leaf_values[i]);
            }
        } else {
            merged = builder_.make_leaf_full(new_skip);
            merged->as_full()->valid = child->as_full()->valid;
            for (int i = 0; i < 256; ++i) {
                if (child->as_full()->valid.test(static_cast<unsigned char>(i))) {
                    merged->as_full()->construct_leaf_value(static_cast<unsigned char>(i), child->as_full()->leaf_values[i]);
                }
            }
        }
    } else {
        if (child->is_eos() | child->is_skip()) {
            merged = builder_.make_interior_skip(new_skip);
            merged->as_skip()->eos_ptr = get_eos_ptr(child);
            set_eos_ptr(child, nullptr);
        } else if (child->is_list()) {
            merged = builder_.make_interior_list(new_skip);
            set_eos_ptr(merged, get_eos_ptr(child));
            set_eos_ptr(child, nullptr);
            merged->as_list()->chars = child->as_list()->chars;
            for (int i = 0; i < child->as_list()->chars.count(); ++i) {
                merged->as_list()->children[i].store(child->as_list()->children[i].load());
                child->as_list()->children[i].store(nullptr);
            }
        } else {
            merged = builder_.make_interior_full(new_skip);
            set_eos_ptr(merged, get_eos_ptr(child));
            set_eos_ptr(child, nullptr);
            merged->as_full()->valid = child->as_full()->valid;
            for (int i = 0; i < 256; ++i) {
                if (child->as_full()->valid.test(static_cast<unsigned char>(i))) {
                    merged->as_full()->children[i].store(child->as_full()->children[i].load());
                    child->as_full()->children[i].store(nullptr);
                }
            }
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
