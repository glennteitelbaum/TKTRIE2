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
        size_.fetch_sub(1);
        return true;
    } else {
        ebr_global::instance().try_reclaim();
        auto& ebr_slot_ref = get_ebr_slot();
        
        while (true) {
            auto guard = ebr_slot_ref.get_guard();
            erase_spec_info info = probe_erase(root_.load(), kb);
            if (info.op != erase_op::NOT_FOUND) {
                capture_parent_collapse_info(info);
            }

            if (info.op == erase_op::NOT_FOUND) {
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

            // IN_PLACE operations
            if ((info.op == erase_op::IN_PLACE_LEAF_LIST) |
                (info.op == erase_op::IN_PLACE_LEAF_FULL) |
                (info.op == erase_op::IN_PLACE_INTERIOR_LIST) |
                (info.op == erase_op::IN_PLACE_INTERIOR_FULL)) {

                std::lock_guard<mutex_t> lock(mutex_);
                erase_spec_info locked_info = probe_erase(root_.load(), kb);
                
                if (locked_info.op == erase_op::NOT_FOUND) return false;
                
                if ((locked_info.op == erase_op::IN_PLACE_LEAF_LIST) |
                    (locked_info.op == erase_op::IN_PLACE_LEAF_FULL) |
                    (locked_info.op == erase_op::IN_PLACE_INTERIOR_LIST) |
                    (locked_info.op == erase_op::IN_PLACE_INTERIOR_FULL)) {
                    
                    bool success = false;
                    switch (locked_info.op) {
                        case erase_op::IN_PLACE_LEAF_LIST:
                            success = do_inplace_leaf_list_erase(locked_info.target, locked_info.c, locked_info.target_version);
                            break;
                        case erase_op::IN_PLACE_LEAF_FULL:
                            success = do_inplace_leaf_full_erase(locked_info.target, locked_info.c, locked_info.target_version);
                            break;
                        case erase_op::IN_PLACE_INTERIOR_LIST:
                            success = do_inplace_interior_list_erase(locked_info.target, locked_info.c, locked_info.target_version);
                            break;
                        case erase_op::IN_PLACE_INTERIOR_FULL:
                            success = do_inplace_interior_full_erase(locked_info.target, locked_info.c, locked_info.target_version);
                            break;
                        default: break;
                    }

                    if (success) { size_.fetch_sub(1); return true; }
                    continue;
                }
                
                ptr_t root = root_.load();
                auto res = erase_impl(&root_, root, kb);
                if (!res.erased) return false;
                if (res.deleted_subtree) root_.store(nullptr);
                else if (res.new_node) root_.store(res.new_node);
                for (auto* old : res.old_nodes) retire_node(old);
                size_.fetch_sub(1);
                return true;
            }

            // DELETE_LEAF_* operations
            if ((info.op == erase_op::DELETE_LEAF_EOS) |
                (info.op == erase_op::DELETE_LEAF_SKIP) |
                (info.op == erase_op::DELETE_LAST_LEAF_LIST)) {

                erase_pre_alloc alloc = allocate_erase_speculative(info);
                std::lock_guard<mutex_t> lock(mutex_);
                erase_spec_info locked_info = probe_erase(root_.load(), kb);
                
                if (locked_info.op == erase_op::NOT_FOUND) {
                    dealloc_erase_speculation(alloc);
                    return false;
                }
                
                dealloc_erase_speculation(alloc);
                ptr_t root = root_.load();
                auto res = erase_impl(&root_, root, kb);
                if (!res.erased) return false;
                if (res.deleted_subtree) root_.store(nullptr);
                else if (res.new_node) root_.store(res.new_node);
                for (auto* old : res.old_nodes) retire_node(old);
                size_.fetch_sub(1);
                return true;
            }

            // DELETE_EOS_INTERIOR or COLLAPSE_AFTER_REMOVE
            {
                std::lock_guard<mutex_t> lock(mutex_);
                erase_spec_info locked_info = probe_erase(root_.load(), kb);
                if (locked_info.op == erase_op::NOT_FOUND) return false;
                
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

    if (leaf->is_eos() | leaf->is_skip()) {
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

        leaf->bump_version();
        for (int i = idx; i < count - 1; ++i) {
            leaf->as_list()->leaf_values[i] = leaf->as_list()->leaf_values[i + 1];
        }
        leaf->as_list()->destroy_leaf_value(count - 1);
        leaf->as_list()->chars.remove_at(idx);
        res.erased = true;
        return res;
    }

    // FULL
    if (!leaf->as_full()->valid.test(c)) return res;
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
            n->bump_version();
            int count = n->as_list()->chars.count();
            for (int i = idx; i < count - 1; ++i) {
                n->as_list()->children[i].store(n->as_list()->children[i + 1].load());
            }
            n->as_list()->children[count - 1].store(nullptr);
            n->as_list()->chars.remove_at(idx);
        }
    } else if (n->is_full()) {
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
        if (child->is_eos() | child->is_skip()) {
            T& val = child->is_eos() ? child->as_eos()->leaf_value : child->as_skip()->leaf_value;
            merged = builder_.make_leaf_skip(new_skip, val);
        } else if (child->is_list()) {
            merged = builder_.make_leaf_list(new_skip);
            merged->as_list()->chars = child->as_list()->chars;
            for (int i = 0; i < child->as_list()->chars.count(); ++i) {
                merged->as_list()->construct_leaf_value(i, child->as_list()->leaf_values[i]);
            }
        } else {
            merged = builder_.make_leaf_full(new_skip);
            merged->as_full()->valid = child->as_full()->valid;
            child->as_full()->valid.for_each_set([child, merged](unsigned char ch) {
                merged->as_full()->construct_leaf_value(ch, child->as_full()->leaf_values[ch]);
            });
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
            child->as_full()->valid.for_each_set([child, merged](unsigned char ch) {
                merged->as_full()->children[ch].store(child->as_full()->children[ch].load());
                child->as_full()->children[ch].store(nullptr);
            });
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
