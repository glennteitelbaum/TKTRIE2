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
        
        reader_enter();
        
        static constexpr int MAX_RETRIES = 7;
        
        for (int retry = 0; retry <= MAX_RETRIES; ++retry) {
            erase_spec_info info = probe_erase(root_.load(), kb);

            if (info.op == erase_op::NOT_FOUND) {
                reader_exit();
                return {false, false};
            }

            // In-place operations - no allocation needed
            if (info.op == erase_op::IN_PLACE_LEAF_LIST) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_erase_path(info)) continue;
                if (do_inplace_leaf_list_erase(info.target, info.c, info.target_version)) {
                    epoch_.fetch_add(1, std::memory_order_release);
                    size_.fetch_sub(1);
                    reader_exit();
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
                    reader_exit();
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
                    reader_exit();
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
                    reader_exit();
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
            reader_exit();
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

    // Handle BINARY leaf
    if (leaf->is_binary()) {
        auto* bn = leaf->template as_binary<true>();
        if (!bn->has(c)) return res;
        
        if (bn->count() == 1) {
            res.erased = true;
            res.deleted_subtree = true;
            res.old_nodes.push_back(leaf);
            return res;
        }
        
        // Convert BINARY to SKIP when count goes from 2 to 1
        int idx = bn->find(c);
        unsigned char other_c = bn->chars[1 - idx];
        T other_val;
        bn->values[1 - idx].try_read(other_val);
        
        // Build new skip string: existing skip + other_c
        std::string new_skip_str(leaf->skip_str());
        new_skip_str.push_back(static_cast<char>(other_c));
        
        ptr_t new_skip = builder_.make_leaf_skip(new_skip_str, other_val);
        
        res.new_node = new_skip;
        res.old_nodes.push_back(leaf);
        res.erased = true;
        return res;
    }

    // Handle LIST leaf
    if (leaf->is_list()) [[likely]] {
        auto* ln = leaf->template as_list<true>();
        if (!ln->has(c)) return res;
        
        int count = ln->count();
        if (count == 1) {
            res.erased = true;
            res.deleted_subtree = true;
            res.old_nodes.push_back(leaf);
            return res;
        }
        
        // Downgrade LIST to BINARY when count goes from 3 to 2
        if (count == 3) {
            ptr_t bn = builder_.make_leaf_binary(leaf->skip_str());
            auto* new_bn = bn->template as_binary<true>();
            int src_idx = 0;
            for (int i = 0; i < 7; ++i) {
                unsigned char ch = ln->chars.char_at(i);
                if (i >= count) break;
                if (ch == c) continue;
                T val{};
                ln->values[i].try_read(val);
                new_bn->add_entry(ch, val);
                ++src_idx;
            }
            res.new_node = bn;
            res.old_nodes.push_back(leaf);
            res.erased = true;
            return res;
        }
        
        leaf->bump_version();
        ln->remove_value(c);
        res.erased = true;
        return res;
    }

    // Handle POP leaf
    if (leaf->is_pop()) {
        auto* pn = leaf->template as_pop<true>();
        if (!pn->has(c)) return res;
        
        int count = pn->count();
        // Downgrade to LIST when count goes from 8 to 7
        if (count == 8) {
            ptr_t ln = builder_.make_leaf_list(leaf->skip_str());
            auto* new_ln = ln->template as_list<true>();
            pn->valid.for_each_set([&](unsigned char ch) {
                if (ch == c) return;
                T val{};
                pn->read_value(ch, val);
                new_ln->add_value(ch, val);
            });
            res.new_node = ln;
            res.old_nodes.push_back(leaf);
            res.erased = true;
            return res;
        }
        
        leaf->bump_version();
        pn->remove_value(c);
        res.erased = true;
        return res;
    }

    // Handle FULL leaf
    auto* fn = leaf->template as_full<true>();
    if (!fn->has(c)) return res;
    
    int count = fn->count();
    // Downgrade to POP when count goes from 33 to 32
    if (count == 33) {
        ptr_t pn = builder_.make_leaf_pop(leaf->skip_str());
        auto* new_pn = pn->template as_pop<true>();
        fn->valid.for_each_set([&](unsigned char ch) {
            if (ch == c) return;
            T val{};
            fn->read_value(ch, val);
            new_pn->add_value(ch, val);
        });
        res.new_node = pn;
        res.old_nodes.push_back(leaf);
        res.erased = true;
        return res;
    }
    
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

    unsigned char c = 0;
    ptr_t child = nullptr;
    if (n->is_binary()) {
        auto* bn = n->template as_binary<false>();
        c = bn->first_char();
        child = bn->child_at_slot(0);
    } else if (n->is_list()) {
        auto* ln = n->template as_list<false>();
        c = ln->chars.char_at(0);
        child = ln->children[0].load();
    } else if (n->is_pop()) {
        auto* pn = n->template as_pop<false>();
        c = pn->first_char();
        child = pn->child_at_slot(0);
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

    bool eos_exists = n->has_eos();
    int remaining = n->child_count();
    int eos_count = eos_exists ? 1 : 0;

    // Count remaining children (excluding the one being removed)
    if (n->is_binary()) {
        auto* bn = n->template as_binary<false>();
        if (bn->has(removed_c)) remaining--;
    } else if (n->is_list()) {
        auto* ln = n->template as_list<false>();
        if (ln->has(removed_c)) remaining--;
    } else if (n->is_pop()) {
        auto* pn = n->template as_pop<false>();
        if (pn->has(removed_c)) remaining--;
    } else if (n->is_full()) {
        auto* fn = n->template as_full<false>();
        if (fn->has(removed_c)) remaining--;
    }

    int total_remaining = remaining + eos_count;

    // Delete if empty
    if (total_remaining == 0) {
        res.deleted_subtree = true;
        res.old_nodes.push_back(n);
        return res;
    }

    // Actually remove the child and potentially downgrade
    if (n->is_binary()) {
        auto* bn = n->template as_binary<false>();
        if (bn->has(removed_c)) {
            n->bump_version();
            bn->remove_child(bn->find(removed_c));
        }
    } else if (n->is_list()) {
        auto* ln = n->template as_list<false>();
        if (ln->has(removed_c)) {
            // Downgrade LIST to BINARY when remaining + eos = 2
            if (total_remaining == 2 && remaining > 0) {
                ptr_t new_bn = builder_.make_interior_binary(n->skip_str());
                auto* dest = new_bn->template as_binary<false>();
                for (int i = 0; i < ln->count(); ++i) {
                    unsigned char ch = ln->chars.char_at(i);
                    if (ch == removed_c) continue;
                    dest->add_child(ch, ln->children[i].load());
                    ln->children[i].store(nullptr);
                }
                if constexpr (FIXED_LEN == 0) {
                    if (eos_exists) {
                        T val{};
                        n->try_read_eos(val);
                        dest->eos.set(val);
                    }
                }
                res.new_node = new_bn;
                res.old_nodes.push_back(n);
                
                // Check if we can collapse the new BINARY node
                if (dest->count() == 1 && !eos_exists) {
                    unsigned char c = dest->first_char();
                    ptr_t child = dest->child_at_slot(0);
                    if (child && !builder_t::is_sentinel(child)) {
                        auto collapse_res = collapse_single_child(new_bn, c, child, res);
                        return collapse_res;
                    }
                }
                return res;
            }
            n->bump_version();
            ln->remove_child(removed_c);
        }
    } else if (n->is_pop()) {
        auto* pn = n->template as_pop<false>();
        if (pn->has(removed_c)) {
            // Downgrade POP to LIST when remaining + eos <= 7
            if (total_remaining <= 7) {
                ptr_t new_ln = builder_.make_interior_list(n->skip_str());
                auto* dest = new_ln->template as_list<false>();
                pn->valid.for_each_set([&](unsigned char ch) {
                    if (ch == removed_c) return;
                    int slot = pn->slot_for(ch);
                    dest->add_child(ch, pn->children[slot].load());
                    pn->children[slot].store(nullptr);
                });
                if constexpr (FIXED_LEN == 0) {
                    if (eos_exists) {
                        T val{};
                        n->try_read_eos(val);
                        dest->eos.set(val);
                    }
                }
                res.new_node = new_ln;
                res.old_nodes.push_back(n);
                return res;
            }
            n->bump_version();
            pn->remove_child(removed_c);
        }
    } else if (n->is_full()) {
        auto* fn = n->template as_full<false>();
        if (fn->has(removed_c)) {
            // Downgrade FULL to POP when remaining + eos <= 32
            if (total_remaining <= POP_MAX) {
                ptr_t new_pn = builder_.make_interior_pop(n->skip_str());
                auto* dest = new_pn->template as_pop<false>();
                fn->valid.for_each_set([&](unsigned char ch) {
                    if (ch == removed_c) return;
                    dest->add_child(ch, fn->children[ch].load());
                    fn->children[ch].store(nullptr);
                });
                if constexpr (FIXED_LEN == 0) {
                    if (eos_exists) {
                        T val{};
                        n->try_read_eos(val);
                        dest->eos.set(val);
                    }
                }
                res.new_node = new_pn;
                res.old_nodes.push_back(n);
                return res;
            }
            n->bump_version();
            fn->remove_child(removed_c);
        }
    }

    // Check for collapse opportunity (1 child, no EOS)
    bool can_collapse = false;
    unsigned char c = 0;
    ptr_t child = nullptr;

    if (!eos_exists && remaining == 1) {
        if (n->is_binary()) {
            auto* bn = n->template as_binary<false>();
            c = bn->first_char();
            child = bn->child_at_slot(0);
            can_collapse = (child != nullptr && !builder_t::is_sentinel(child));
        } else if (n->is_list()) {
            auto* ln = n->template as_list<false>();
            c = ln->chars.char_at(0);
            child = ln->children[0].load();
            can_collapse = (child != nullptr && !builder_t::is_sentinel(child));
        } else if (n->is_pop()) {
            auto* pn = n->template as_pop<false>();
            c = pn->first_char();
            child = pn->child_at_slot(0);
            can_collapse = (child != nullptr && !builder_t::is_sentinel(child));
        } else if (n->is_full()) {
            auto* fn = n->template as_full<false>();
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
            T val{};
            child->as_skip()->value.try_read(val);
            merged = builder_.make_leaf_skip(new_skip, val);
        } else if (child->is_binary()) {
            merged = builder_.make_leaf_binary(new_skip);
            child->template as_binary<true>()->copy_values_to(merged->template as_binary<true>());
        } else if (child->is_list()) [[likely]] {
            merged = builder_.make_leaf_list(new_skip);
            child->template as_list<true>()->copy_values_to(merged->template as_list<true>());
        } else if (child->is_pop()) {
            merged = builder_.make_leaf_pop(new_skip);
            child->template as_pop<true>()->copy_values_to(merged->template as_pop<true>());
        } else {
            merged = builder_.make_leaf_full(new_skip);
            child->template as_full<true>()->copy_values_to(merged->template as_full<true>());
        }
    } else {
        if (child->is_binary()) {
            merged = builder_.make_interior_binary(new_skip);
            child->template as_binary<false>()->move_children_to(merged->template as_binary<false>());
            if constexpr (FIXED_LEN == 0) {
                if (child->has_eos()) {
                    T val{};
                    child->try_read_eos(val);
                    merged->set_eos(val);
                }
            }
        } else if (child->is_list()) [[likely]] {
            merged = builder_.make_interior_list(new_skip);
            child->template as_list<false>()->move_interior_to(merged->template as_list<false>());
        } else if (child->is_pop()) {
            merged = builder_.make_interior_pop(new_skip);
            child->template as_pop<false>()->move_children_to(merged->template as_pop<false>());
            if constexpr (FIXED_LEN == 0) {
                if (child->has_eos()) {
                    T val{};
                    child->try_read_eos(val);
                    merged->set_eos(val);
                }
            }
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
