#pragma once

// This file contains implementation details for tktrie (erase probing and allocation)
// It should only be included from tktrie_insert_probe.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

// -----------------------------------------------------------------------------
// Erase operations
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_spec_info TKTRIE_CLASS::probe_erase(
    ptr_t n, std::string_view key) const noexcept {
    erase_spec_info info;

    if (!n) {
        info.op = erase_op::NOT_FOUND;
        return info;
    }

    info.path[info.path_len++] = {n, n->version(), 0};

    while (n) {
        if (n->is_leaf()) {
            std::string_view skip = get_skip(n);
            size_t m = match_skip_impl(skip, key);
            if (m < skip.size()) {
                info.op = erase_op::NOT_FOUND;
                return info;
            }
            key.remove_prefix(m);

            if (n->is_eos()) {
                if (!key.empty()) {
                    info.op = erase_op::NOT_FOUND;
                    return info;
                }
                info.op = erase_op::DELETE_LEAF_EOS;
                info.target = n;
                info.target_version = n->version();
                return info;
            }

            if (n->is_skip()) {
                if (!key.empty()) {
                    info.op = erase_op::NOT_FOUND;
                    return info;
                }
                info.op = erase_op::DELETE_LEAF_SKIP;
                info.target = n;
                info.target_version = n->version();
                return info;
            }

            if (key.size() != 1) {
                info.op = erase_op::NOT_FOUND;
                return info;
            }

            unsigned char c = static_cast<unsigned char>(key[0]);
            info.c = c;
            info.target = n;
            info.target_version = n->version();

            if (n->is_list()) {
                int idx = n->as_list()->chars.find(c);
                if (idx < 0) {
                    info.op = erase_op::NOT_FOUND;
                    return info;
                }
                if (n->as_list()->chars.count() == 1) {
                    info.op = erase_op::DELETE_LAST_LEAF_LIST;
                } else {
                    info.op = erase_op::IN_PLACE_LEAF_LIST;
                }
                return info;
            }

            if (!n->as_full()->valid.test(c)) {
                info.op = erase_op::NOT_FOUND;
                return info;
            }
            info.op = erase_op::IN_PLACE_LEAF_FULL;
            return info;
        }

        std::string_view skip = get_skip(n);
        size_t m = match_skip_impl(skip, key);
        if (m < skip.size()) {
            info.op = erase_op::NOT_FOUND;
            return info;
        }
        key.remove_prefix(m);

        if (key.empty()) {
            T* p = get_eos_ptr(n);
            if (!p) {
                info.op = erase_op::NOT_FOUND;
                return info;
            }
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);

            int child_cnt = n->child_count();
            if (child_cnt == 0) {
                info.op = erase_op::DELETE_EOS_INTERIOR;
                return info;
            }
            if (child_cnt == 1) {
                info.op = erase_op::COLLAPSE_AFTER_REMOVE;
                if (n->is_list()) {
                    info.collapse_char = n->as_list()->chars.char_at(0);
                    info.collapse_child = n->as_list()->children[0].load();
                } else if (n->is_full()) {
                    info.collapse_char = n->as_full()->valid.first();
                    info.collapse_child = n->as_full()->children[info.collapse_char].load();
                }
                if (info.collapse_child) {
                    info.collapse_child_version = info.collapse_child->version();
                    info.child_skip = std::string(get_skip(info.collapse_child));
                }
                return info;
            }
            info.op = erase_op::DELETE_EOS_INTERIOR;
            return info;
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = find_child(n, c);
        if (!child) {
            info.op = erase_op::NOT_FOUND;
            return info;
        }

        key.remove_prefix(1);
        n = child;
        if (info.path_len < erase_spec_info::MAX_PATH) {
            info.path[info.path_len++] = {n, n->version(), c};
        }
    }

    info.op = erase_op::NOT_FOUND;
    return info;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::capture_parent_collapse_info(erase_spec_info& info) const noexcept {
    if (info.path_len < 2) return;

    ptr_t parent = info.path[info.path_len - 2].node;
    unsigned char edge = info.path[info.path_len - 1].edge;

    info.parent = parent;
    info.parent_version = parent->version();
    info.parent_edge = edge;
    info.parent_skip = std::string(get_skip(parent));

    T* eos = get_eos_ptr(parent);
    if (eos) return;

    int remaining = parent->child_count() - 1;
    if (remaining != 1) return;

    if (parent->is_list()) {
        for (int i = 0; i < parent->as_list()->chars.count(); ++i) {
            unsigned char ch = parent->as_list()->chars.char_at(i);
            if (ch != edge) {
                info.parent_collapse_char = ch;
                info.parent_collapse_child = parent->as_list()->children[i].load();
                break;
            }
        }
    } else if (parent->is_full()) {
        for (int i = 0; i < 256; ++i) {
            unsigned char ch = static_cast<unsigned char>(i);
            if ((parent->as_full()->valid.test(ch)) & (ch != edge)) {
                info.parent_collapse_char = ch;
                info.parent_collapse_child = parent->as_full()->children[ch].load();
                break;
            }
        }
    }

    if (info.parent_collapse_child) {
        info.parent_collapse_child_version = info.parent_collapse_child->version();
        info.parent_child_skip = std::string(get_skip(info.parent_collapse_child));
    }
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::check_collapse_needed(
    ptr_t parent, unsigned char removed_c,
    unsigned char& collapse_c, ptr_t& collapse_child) const noexcept {
    T* eos = get_eos_ptr(parent);
    if (eos) return false;

    int remaining = parent->child_count();

    if (parent->is_list()) {
        int idx = parent->as_list()->chars.find(removed_c);
        if (idx >= 0) remaining--;

        if (remaining != 1) return false;

        for (int i = 0; i < parent->as_list()->chars.count(); ++i) {
            unsigned char ch = parent->as_list()->chars.char_at(i);
            if (ch != removed_c) {
                collapse_c = ch;
                collapse_child = parent->as_list()->children[i].load();
                return collapse_child != nullptr;
            }
        }
    } else if (parent->is_full()) {
        if (parent->as_full()->valid.test(removed_c)) remaining--;

        if (remaining != 1) return false;

        for (int i = 0; i < 256; ++i) {
            unsigned char ch = static_cast<unsigned char>(i);
            if ((parent->as_full()->valid.test(ch)) & (ch != removed_c)) {
                collapse_c = ch;
                collapse_child = parent->as_full()->children[ch].load();
                return collapse_child != nullptr;
            }
        }
    }
    return false;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::allocate_collapse_node(const erase_spec_info& info) {
    std::string new_skip = info.target_skip;
    new_skip.push_back(static_cast<char>(info.collapse_char));
    new_skip.append(info.child_skip);

    ptr_t child = info.collapse_child;
    if (!child) return nullptr;

    if (child->is_leaf()) {
        if (child->is_eos() | child->is_skip()) {
            return builder_.make_leaf_skip(new_skip, T{});
        } else if (child->is_list()) {
            return builder_.make_leaf_list(new_skip);
        } else {
            return builder_.make_leaf_full(new_skip);
        }
    } else {
        if (child->is_eos() | child->is_skip()) {
            return builder_.make_interior_skip(new_skip);
        } else if (child->is_list()) {
            return builder_.make_interior_list(new_skip);
        } else {
            return builder_.make_interior_full(new_skip);
        }
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::allocate_parent_collapse_node(const erase_spec_info& info) {
    if (!info.parent_collapse_child) return nullptr;

    std::string new_skip = info.parent_skip;
    new_skip.push_back(static_cast<char>(info.parent_collapse_char));
    new_skip.append(info.parent_child_skip);

    ptr_t child = info.parent_collapse_child;

    if (child->is_leaf()) {
        if (child->is_eos() | child->is_skip()) {
            return builder_.make_leaf_skip(new_skip, T{});
        } else if (child->is_list()) {
            return builder_.make_leaf_list(new_skip);
        } else {
            return builder_.make_leaf_full(new_skip);
        }
    } else {
        if (child->is_eos() | child->is_skip()) {
            return builder_.make_interior_skip(new_skip);
        } else if (child->is_list()) {
            return builder_.make_interior_list(new_skip);
        } else {
            return builder_.make_interior_full(new_skip);
        }
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_pre_alloc TKTRIE_CLASS::allocate_erase_speculative(
    const erase_spec_info& info) {
    erase_pre_alloc alloc;

    if (info.op == erase_op::COLLAPSE_AFTER_REMOVE) {
        alloc.merged = allocate_collapse_node(info);
    }

    if (info.parent_collapse_child) {
        alloc.parent_merged = allocate_parent_collapse_node(info);
    }

    return alloc;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::dealloc_erase_speculation(erase_pre_alloc& alloc) {
    if (alloc.merged) {
        switch (alloc.merged->type()) {
            case TYPE_EOS: delete alloc.merged->as_eos(); break;
            case TYPE_SKIP: delete alloc.merged->as_skip(); break;
            case TYPE_LIST: delete alloc.merged->as_list(); break;
            case TYPE_FULL: delete alloc.merged->as_full(); break;
        }
        alloc.merged = nullptr;
    }
    if (alloc.parent_merged) {
        switch (alloc.parent_merged->type()) {
            case TYPE_EOS: delete alloc.parent_merged->as_eos(); break;
            case TYPE_SKIP: delete alloc.parent_merged->as_skip(); break;
            case TYPE_LIST: delete alloc.parent_merged->as_list(); break;
            case TYPE_FULL: delete alloc.parent_merged->as_full(); break;
        }
        alloc.parent_merged = nullptr;
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::fill_collapse_node(ptr_t merged, ptr_t child) {
    if (child->is_leaf()) {
        if (child->is_eos()) {
            // merged was created with make_leaf_skip which already constructed leaf_value
            // Use assignment, not placement new
            merged->as_skip()->leaf_value = child->as_eos()->leaf_value;
        } else if (child->is_skip()) {
            merged->as_skip()->leaf_value = child->as_skip()->leaf_value;
        } else if (child->is_list()) {
            merged->as_list()->chars = child->as_list()->chars;
            for (int i = 0; i < child->as_list()->chars.count(); ++i) {
                merged->as_list()->construct_leaf_value(i, child->as_list()->leaf_values[i]);
            }
        } else {
            merged->as_full()->valid = child->as_full()->valid;
            for (int i = 0; i < 256; ++i) {
                if (child->as_full()->valid.test(static_cast<unsigned char>(i))) {
                    merged->as_full()->construct_leaf_value(static_cast<unsigned char>(i), child->as_full()->leaf_values[i]);
                }
            }
        }
    } else {
        if (child->is_eos() | child->is_skip()) {
            merged->as_skip()->eos_ptr = get_eos_ptr(child);
            set_eos_ptr(child, nullptr);
        } else if (child->is_list()) {
            // Use set_eos_ptr for atomic access when THREADED
            set_eos_ptr(merged, get_eos_ptr(child));
            set_eos_ptr(child, nullptr);
            merged->as_list()->chars = child->as_list()->chars;
            for (int i = 0; i < child->as_list()->chars.count(); ++i) {
                merged->as_list()->children[i].store(child->as_list()->children[i].load());
                child->as_list()->children[i].store(nullptr);
            }
        } else {
            // Use set_eos_ptr for atomic access when THREADED
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
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::validate_erase_path(const erase_spec_info& info) const noexcept {
    for (int i = 0; i < info.path_len; ++i) {
        if (info.path[i].node->version() != info.path[i].version) {
            return false;
        }
    }
    if (info.target && (info.path_len == 0 || info.path[info.path_len-1].node != info.target)) {
        if (info.target->version() != info.target_version) {
            return false;
        }
    }
    if (info.collapse_child) {
        if (info.collapse_child->version() != info.collapse_child_version) {
            return false;
        }
    }
    if (info.parent) {
        if (info.parent->version() != info.parent_version) {
            return false;
        }
    }
    if (info.parent_collapse_child) {
        if (info.parent_collapse_child->version() != info.parent_collapse_child_version) {
            return false;
        }
    }
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_leaf_list_erase(ptr_t leaf, unsigned char c, uint64_t expected_version) {
    // Re-verify version hasn't changed (TOCTOU protection)
    if (leaf->version() != expected_version) return false;
    
    int idx = leaf->as_list()->chars.find(c);
    if (idx < 0) return false;

    int count = leaf->as_list()->chars.count();
    if (count <= 1) return false;

    // CRITICAL: Bump version BEFORE modifying data
    leaf->bump_version();
    
    // Shift values down
    for (int i = idx; i < count - 1; ++i) {
        leaf->as_list()->leaf_values[i] = leaf->as_list()->leaf_values[i + 1];
    }
    // Destroy the last element that's now extra
    leaf->as_list()->destroy_leaf_value(count - 1);
    leaf->as_list()->chars.remove_at(idx);
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_leaf_full_erase(ptr_t leaf, unsigned char c, uint64_t expected_version) {
    // Re-verify version hasn't changed (TOCTOU protection)
    if (leaf->version() != expected_version) return false;
    
    if (!leaf->as_full()->valid.test(c)) return false;
    
    // CRITICAL: Bump version BEFORE modifying data
    leaf->bump_version();
    
    leaf->as_full()->destroy_leaf_value(c);
    leaf->as_full()->valid.template atomic_clear<THREADED>(c);
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_interior_list_erase(ptr_t n, unsigned char c, uint64_t expected_version) {
    // Re-verify version hasn't changed (TOCTOU protection)
    if (n->version() != expected_version) return false;
    
    int idx = n->as_list()->chars.find(c);
    if (idx < 0) return false;

    // CRITICAL: Bump version BEFORE modifying data to signal readers
    // This ensures any concurrent reader will see version change and retry
    n->bump_version();
    
    int count = n->as_list()->chars.count();
    for (int i = idx; i < count - 1; ++i) {
        n->as_list()->children[i].store(n->as_list()->children[i + 1].load());
    }
    n->as_list()->children[count - 1].store(nullptr);
    n->as_list()->chars.remove_at(idx);
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_interior_full_erase(ptr_t n, unsigned char c, uint64_t expected_version) {
    // Re-verify version hasn't changed (TOCTOU protection)
    if (n->version() != expected_version) return false;
    
    if (!n->as_full()->valid.test(c)) return false;
    
    // CRITICAL: Bump version BEFORE modifying data
    n->bump_version();
    
    n->as_full()->valid.template atomic_clear<THREADED>(c);
    n->as_full()->children[c].store(nullptr);
    return true;
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

// Include final erase implementation part
#include "tktrie_erase.h"
