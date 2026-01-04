// tktrie_erase.inc - Erase operations for tktrie
// This file is included inside the tktrie class body

// =========================================================================
// ERASE RESULT - use small fixed array instead of vector
// =========================================================================
struct erase_result {
    static constexpr int MAX_OLD = 8;
    ptr_t new_node = nullptr;
    ptr_t old_nodes[MAX_OLD] = {};
    int old_count = 0;
    bool erased = false;
    bool deleted_subtree = false;
    
    void add_old(ptr_t n) {
        if (n && old_count < MAX_OLD) old_nodes[old_count++] = n;
    }
};

// =========================================================================
// ERASE OPERATION TYPES
// =========================================================================
enum class erase_op {
    NOT_FOUND,
    DELETE_LEAF_EOS,
    DELETE_LEAF_SKIP,
    DELETE_LAST_LEAF_LIST,
    IN_PLACE_LEAF_LIST,
    IN_PLACE_LEAF_FULL,
    DELETE_EOS_INTERIOR,
    IN_PLACE_INTERIOR_LIST,
    IN_PLACE_INTERIOR_FULL,
    COLLAPSE_AFTER_REMOVE,
};

struct erase_spec_info {
    static constexpr int MAX_PATH = 64;
    path_entry path[MAX_PATH];
    int path_len = 0;
    
    erase_op op = erase_op::NOT_FOUND;
    ptr_t target = nullptr;
    uint64_t target_header = 0;
    unsigned char c = 0;
    
    // For collapse when removing eos from target
    ptr_t collapse_child = nullptr;
    uint64_t collapse_child_header = 0;
    unsigned char collapse_char = 0;
    std::string target_skip;
    std::string child_skip;
    bool child_is_leaf = false;
    uint64_t child_type = 0;
    
    // For parent collapse when deleting leaf/child
    ptr_t parent = nullptr;
    uint64_t parent_header = 0;
    unsigned char parent_edge = 0;
    ptr_t parent_collapse_child = nullptr;
    uint64_t parent_collapse_child_header = 0;
    unsigned char parent_collapse_char = 0;
    std::string parent_skip;
    std::string parent_child_skip;
    bool parent_child_is_leaf = false;
    uint64_t parent_child_type = 0;
    bool parent_needs_collapse = false;
};

// Pre-allocated nodes for speculative erase
struct erase_pre_alloc {
    ptr_t merged = nullptr;           // For COLLAPSE_AFTER_REMOVE
    ptr_t parent_merged = nullptr;    // For parent collapse after child removal
    T* deferred_eos = nullptr;        // eos_ptr to delete after lock
    
    void clear() {
        merged = nullptr;
        parent_merged = nullptr;
        deferred_eos = nullptr;
    }
};

// =========================================================================
// PROBE FOR ERASE
// =========================================================================
erase_spec_info probe_erase(ptr_t n, std::string_view key) const noexcept {
    erase_spec_info info;
    
    if (!n) {
        info.op = erase_op::NOT_FOUND;
        return info;
    }
    
    info.path[info.path_len++] = {n, n->header(), 0};
    
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
                info.target_header = n->header();
                capture_parent_collapse_info(info);
                return info;
            }
            
            if (n->is_skip()) {
                if (!key.empty()) {
                    info.op = erase_op::NOT_FOUND;
                    return info;
                }
                info.op = erase_op::DELETE_LEAF_SKIP;
                info.target = n;
                info.target_header = n->header();
                capture_parent_collapse_info(info);
                return info;
            }
            
            if (key.size() != 1) {
                info.op = erase_op::NOT_FOUND;
                return info;
            }
            
            unsigned char c = static_cast<unsigned char>(key[0]);
            info.c = c;
            info.target = n;
            info.target_header = n->header();
            
            if (n->is_list()) {
                int idx = n->as_list()->chars.find(c);
                if (idx < 0) {
                    info.op = erase_op::NOT_FOUND;
                    return info;
                }
                if (n->as_list()->chars.count() == 1) {
                    info.op = erase_op::DELETE_LAST_LEAF_LIST;
                    capture_parent_collapse_info(info);
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
            info.target_header = n->header();
            info.target_skip = std::string(skip);
            
            int child_cnt = n->child_count();
            if (child_cnt == 0) {
                info.op = erase_op::DELETE_EOS_INTERIOR;
                capture_parent_collapse_info(info);
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
                    info.collapse_child_header = info.collapse_child->header();
                    info.child_skip = std::string(get_skip(info.collapse_child));
                    info.child_is_leaf = info.collapse_child->is_leaf();
                    info.child_type = info.collapse_child->type();
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
            info.path[info.path_len++] = {n, n->header(), c};
        }
    }
    
    info.op = erase_op::NOT_FOUND;
    return info;
}

// Helper to capture parent collapse info for leaf deletion cases
void capture_parent_collapse_info(erase_spec_info& info) const noexcept {
    if (info.path_len <= 1) return;
    
    ptr_t parent = info.path[info.path_len - 2].node;
    unsigned char edge = info.path[info.path_len - 1].edge;
    
    info.parent = parent;
    info.parent_header = parent->header();
    info.parent_edge = edge;
    info.parent_skip = std::string(get_skip(parent));
    
    // Check if removing this child would cause parent collapse
    T* eos = get_eos_ptr(parent);
    if (eos) return;  // Has eos, won't collapse
    
    int remaining = parent->child_count();
    if (parent->is_list()) {
        int idx = parent->as_list()->chars.find(edge);
        if (idx >= 0) remaining--;
    } else if (parent->is_full()) {
        if (parent->as_full()->valid.test(edge)) remaining--;
    }
    
    if (remaining != 1) return;  // Won't collapse to single child
    
    info.parent_needs_collapse = true;
    
    // Find the remaining child
    if (parent->is_list()) {
        int cnt = parent->as_list()->chars.count();
        for (int i = 0; i < cnt; ++i) {
            unsigned char ch = parent->as_list()->chars.char_at(i);
            if (ch != edge) {
                info.parent_collapse_char = ch;
                info.parent_collapse_child = parent->as_list()->children[i].load();
                break;
            }
        }
    } else if (parent->is_full()) {
        bool found = false;
        parent->as_full()->valid.for_each_set([&](unsigned char ch) {
            if (!found && ch != edge) {
                info.parent_collapse_char = ch;
                info.parent_collapse_child = parent->as_full()->children[ch].load();
                found = true;
            }
        });
    }
    
    if (info.parent_collapse_child) {
        info.parent_collapse_child_header = info.parent_collapse_child->header();
        info.parent_child_skip = std::string(get_skip(info.parent_collapse_child));
        info.parent_child_is_leaf = info.parent_collapse_child->is_leaf();
        info.parent_child_type = info.parent_collapse_child->type();
    }
}

// =========================================================================
// CHECK IF REMOVAL CAUSES COLLAPSE
// =========================================================================
bool check_collapse_needed(ptr_t parent, unsigned char removed_c, 
                          unsigned char& collapse_c, ptr_t& collapse_child) const noexcept {
    T* eos = get_eos_ptr(parent);
    if (eos) return false;
    
    int remaining = parent->child_count();
    
    if (parent->is_list()) {
        int idx = parent->as_list()->chars.find(removed_c);
        if (idx >= 0) remaining--;
        
        if (remaining != 1) return false;
        
        int cnt = parent->as_list()->chars.count();
        for (int i = 0; i < cnt; ++i) {
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
        
        // Use Kernighan - find first that isn't removed_c
        bool found = false;
        parent->as_full()->valid.for_each_set([&](unsigned char ch) {
            if (!found && ch != removed_c) {
                collapse_c = ch;
                collapse_child = parent->as_full()->children[ch].load();
                found = true;
            }
        });
        return found && collapse_child != nullptr;
    }
    return false;
}

// =========================================================================
// ALLOCATE MERGE NODE SHELL (for COLLAPSE_AFTER_REMOVE)
// =========================================================================
ptr_t allocate_collapse_node(const erase_spec_info& info) {
    std::string new_skip = info.target_skip;
    new_skip.push_back(static_cast<char>(info.collapse_char));
    new_skip.append(info.child_skip);
    
    if (!info.collapse_child) return nullptr;
    
    if (info.child_is_leaf) {
        if (info.child_type == TYPE_EOS || info.child_type == TYPE_SKIP) {
            return builder_.make_leaf_skip(new_skip, T{});
        } else if (info.child_type == TYPE_LIST) {
            return builder_.make_leaf_list(new_skip);
        } else {
            return builder_.make_leaf_full(new_skip);
        }
    } else {
        if (info.child_type == TYPE_EOS || info.child_type == TYPE_SKIP) {
            return builder_.make_interior_skip(new_skip);
        } else if (info.child_type == TYPE_LIST) {
            return builder_.make_interior_list(new_skip);
        } else {
            return builder_.make_interior_full(new_skip);
        }
    }
}

// =========================================================================
// ALLOCATE PARENT MERGE NODE SHELL (for parent collapse after child removal)
// =========================================================================
ptr_t allocate_parent_collapse_node(const erase_spec_info& info) {
    if (!info.parent_needs_collapse || !info.parent_collapse_child) return nullptr;
    
    std::string new_skip = info.parent_skip;
    new_skip.push_back(static_cast<char>(info.parent_collapse_char));
    new_skip.append(info.parent_child_skip);
    
    if (info.parent_child_is_leaf) {
        if (info.parent_child_type == TYPE_EOS || info.parent_child_type == TYPE_SKIP) {
            return builder_.make_leaf_skip(new_skip, T{});
        } else if (info.parent_child_type == TYPE_LIST) {
            return builder_.make_leaf_list(new_skip);
        } else {
            return builder_.make_leaf_full(new_skip);
        }
    } else {
        if (info.parent_child_type == TYPE_EOS || info.parent_child_type == TYPE_SKIP) {
            return builder_.make_interior_skip(new_skip);
        } else if (info.parent_child_type == TYPE_LIST) {
            return builder_.make_interior_list(new_skip);
        } else {
            return builder_.make_interior_full(new_skip);
        }
    }
}

// =========================================================================
// ALLOCATE FOR SPECULATIVE ERASE (called outside lock)
// =========================================================================
erase_pre_alloc allocate_erase_speculative(const erase_spec_info& info) {
    erase_pre_alloc alloc;
    
    switch (info.op) {
    case erase_op::COLLAPSE_AFTER_REMOVE:
        alloc.merged = allocate_collapse_node(info);
        break;
        
    case erase_op::DELETE_LEAF_EOS:
    case erase_op::DELETE_LEAF_SKIP:
    case erase_op::DELETE_LAST_LEAF_LIST:
        if (info.parent_needs_collapse) {
            alloc.parent_merged = allocate_parent_collapse_node(info);
        }
        break;
        
    case erase_op::DELETE_EOS_INTERIOR:
        // May need parent collapse if child_cnt becomes 0 after eos removal
        // But we don't know child_cnt until under lock, so pre-allocate if path allows
        if (info.parent_needs_collapse) {
            alloc.parent_merged = allocate_parent_collapse_node(info);
        }
        break;
        
    default:
        break;
    }
    
    return alloc;
}

// =========================================================================
// DEALLOCATE FAILED SPECULATION (called outside lock)
// =========================================================================
void dealloc_erase_speculation(erase_pre_alloc& alloc) {
    if (alloc.merged) {
        switch (alloc.merged->type()) {
            case TYPE_EOS: delete alloc.merged->as_eos(); break;
            case TYPE_SKIP: delete alloc.merged->as_skip(); break;
            case TYPE_LIST: delete alloc.merged->as_list(); break;
            case TYPE_FULL: delete alloc.merged->as_full(); break;
        }
    }
    if (alloc.parent_merged) {
        switch (alloc.parent_merged->type()) {
            case TYPE_EOS: delete alloc.parent_merged->as_eos(); break;
            case TYPE_SKIP: delete alloc.parent_merged->as_skip(); break;
            case TYPE_LIST: delete alloc.parent_merged->as_list(); break;
            case TYPE_FULL: delete alloc.parent_merged->as_full(); break;
        }
    }
    // deferred_eos is deleted separately after lock release
    alloc.clear();
}

// =========================================================================
// FILL IN COLLAPSE NODE FROM CHILD
// =========================================================================
void fill_collapse_node(ptr_t merged, ptr_t child) {
    if (child->is_leaf()) {
        if (child->is_eos()) {
            merged->as_skip()->leaf_value = child->as_eos()->leaf_value;
        } else if (child->is_skip()) {
            merged->as_skip()->leaf_value = child->as_skip()->leaf_value;
        } else if (child->is_list()) {
            merged->as_list()->chars = child->as_list()->chars;
            int cnt = child->as_list()->chars.count();
            for (int i = 0; i < cnt; ++i) {
                merged->as_list()->leaf_values[i] = child->as_list()->leaf_values[i];
            }
        } else {
            merged->as_full()->valid = child->as_full()->valid;
            child->as_full()->valid.for_each_set([child, merged](unsigned char c) {
                merged->as_full()->leaf_values[c] = child->as_full()->leaf_values[c];
            });
        }
    } else {
        if (child->is_eos() || child->is_skip()) {
            merged->as_skip()->eos_ptr = get_eos_ptr(child);
            set_eos_ptr(child, nullptr);
        } else if (child->is_list()) {
            merged->as_list()->eos_ptr = child->as_list()->eos_ptr;
            child->as_list()->eos_ptr = nullptr;
            merged->as_list()->chars = child->as_list()->chars;
            int cnt = child->as_list()->chars.count();
            for (int i = 0; i < cnt; ++i) {
                merged->as_list()->children[i].store(child->as_list()->children[i].load());
                child->as_list()->children[i].store(nullptr);
            }
        } else {
            merged->as_full()->eos_ptr = child->as_full()->eos_ptr;
            child->as_full()->eos_ptr = nullptr;
            merged->as_full()->valid = child->as_full()->valid;
            child->as_full()->valid.for_each_set([child, merged](unsigned char c) {
                merged->as_full()->children[c].store(child->as_full()->children[c].load());
                child->as_full()->children[c].store(nullptr);
            });
        }
    }
}

// =========================================================================
// VALIDATE ERASE PATH - compare full headers
// =========================================================================
bool validate_erase_path(const erase_spec_info& info) const noexcept {
    for (int i = 0; i < info.path_len; ++i) {
        if (info.path[i].node->header() != info.path[i].header) {
            return false;
        }
    }
    if (info.target && (info.path_len == 0 || info.path[info.path_len-1].node != info.target)) {
        if (info.target->header() != info.target_header) {
            return false;
        }
    }
    if (info.collapse_child) {
        if (info.collapse_child->header() != info.collapse_child_header) {
            return false;
        }
    }
    if (info.parent) {
        if (info.parent->header() != info.parent_header) {
            return false;
        }
    }
    if (info.parent_collapse_child) {
        if (info.parent_collapse_child->header() != info.parent_collapse_child_header) {
            return false;
        }
    }
    return true;
}

// =========================================================================
// IN-PLACE ERASE OPERATIONS
// =========================================================================
bool do_inplace_leaf_list_erase(ptr_t leaf, unsigned char c) {
    int idx = leaf->as_list()->chars.find(c);
    if (idx < 0) return false;
    
    int count = leaf->as_list()->chars.count();
    if (count <= 1) return false;
    
    for (int i = idx; i < count - 1; ++i) {
        leaf->as_list()->leaf_values[i] = leaf->as_list()->leaf_values[i + 1];
    }
    leaf->as_list()->chars.remove_at(idx);
    leaf->bump_version();
    return true;
}

bool do_inplace_leaf_full_erase(ptr_t leaf, unsigned char c) {
    if (!leaf->as_full()->valid.test(c)) return false;
    leaf->as_full()->valid.template atomic_clear<THREADED>(c);
    leaf->bump_version();
    return true;
}

bool do_inplace_interior_list_erase(ptr_t n, unsigned char c) {
    int idx = n->as_list()->chars.find(c);
    if (idx < 0) return false;
    
    int count = n->as_list()->chars.count();
    for (int i = idx; i < count - 1; ++i) {
        n->as_list()->children[i].store(n->as_list()->children[i + 1].load());
    }
    n->as_list()->children[count - 1].store(nullptr);
    n->as_list()->chars.remove_at(idx);
    n->bump_version();
    return true;
}

bool do_inplace_interior_full_erase(ptr_t n, unsigned char c) {
    if (!n->as_full()->valid.test(c)) return false;
    n->as_full()->valid.template atomic_clear<THREADED>(c);
    n->as_full()->children[c].store(nullptr);
    n->bump_version();
    return true;
}

// =========================================================================
// ERASE LOCKED
// =========================================================================
bool erase_locked(std::string_view kb) {
    if constexpr (!THREADED) {
        std::lock_guard<mutex_t> lock(mutex_);
        
        ptr_t root = root_.load();
        auto res = erase_impl(&root_, root, kb);
        
        if (!res.erased) return false;
        
        if (res.deleted_subtree) root_.store(nullptr);
        else if (res.new_node) root_.store(res.new_node);
        
        for (int i = 0; i < res.old_count; ++i) retire_node(res.old_nodes[i]);
        --size_;
        
        return true;
    } else {
        while (true) {
            erase_spec_info info;
            {
                auto& slot = get_ebr_slot();
                auto guard = slot.get_guard();
                info = probe_erase(root_.load(), kb);
            }
            
            if (info.op == erase_op::NOT_FOUND) {
                return false;
            }
            
            // Pre-allocate everything outside lock
            erase_pre_alloc alloc = allocate_erase_speculative(info);
            
            // IN_PLACE operations - no allocation needed
            if (info.op == erase_op::IN_PLACE_LEAF_LIST ||
                info.op == erase_op::IN_PLACE_LEAF_FULL ||
                info.op == erase_op::IN_PLACE_INTERIOR_LIST ||
                info.op == erase_op::IN_PLACE_INTERIOR_FULL) {
                
                std::lock_guard<mutex_t> lock(mutex_);
                
                if (!validate_erase_path(info)) {
                    dealloc_erase_speculation(alloc);
                    continue;
                }
                
                bool success = false;
                switch (info.op) {
                    case erase_op::IN_PLACE_LEAF_LIST:
                        success = do_inplace_leaf_list_erase(info.target, info.c);
                        break;
                    case erase_op::IN_PLACE_LEAF_FULL:
                        success = do_inplace_leaf_full_erase(info.target, info.c);
                        break;
                    case erase_op::IN_PLACE_INTERIOR_LIST:
                        success = do_inplace_interior_list_erase(info.target, info.c);
                        break;
                    case erase_op::IN_PLACE_INTERIOR_FULL:
                        success = do_inplace_interior_full_erase(info.target, info.c);
                        break;
                    default:
                        break;
                }
                
                if (success) {
                    size_.fetch_sub(1);
                    dealloc_erase_speculation(alloc);
                    return true;
                }
                dealloc_erase_speculation(alloc);
                continue;
            }
            
            // DELETE_LEAF cases - may need parent collapse
            if (info.op == erase_op::DELETE_LEAF_EOS ||
                info.op == erase_op::DELETE_LEAF_SKIP ||
                info.op == erase_op::DELETE_LAST_LEAF_LIST) {
                
                T* deferred_eos = nullptr;
                ptr_t retired_target = nullptr;
                ptr_t retired_parent = nullptr;
                ptr_t retired_sibling = nullptr;
                bool success = false;
                
                {
                    std::lock_guard<mutex_t> lock(mutex_);
                    
                    if (!validate_erase_path(info)) {
                        dealloc_erase_speculation(alloc);
                        continue;
                    }
                    
                    if (info.path_len <= 1) {
                        // Deleting root
                        root_.store(nullptr);
                        retired_target = info.target;
                        success = true;
                    } else if (info.parent_needs_collapse && alloc.parent_merged) {
                        // Parent will collapse - use pre-allocated merged node
                        fill_collapse_node(alloc.parent_merged, info.parent_collapse_child);
                        
                        if (info.path_len <= 2) {
                            // Parent is root
                            root_.store(alloc.parent_merged);
                        } else {
                            ptr_t grandparent = info.path[info.path_len - 3].node;
                            unsigned char parent_edge = info.path[info.path_len - 2].edge;
                            atomic_ptr* slot = get_child_slot(grandparent, parent_edge);
                            if (slot) {
                                slot->store(alloc.parent_merged);
                                grandparent->bump_version();
                            }
                        }
                        
                        alloc.parent_merged = nullptr;  // Transferred ownership
                        retired_target = info.target;
                        retired_parent = info.parent;
                        retired_sibling = info.parent_collapse_child;
                        success = true;
                    } else {
                        // Simple removal from parent
                        ptr_t parent = info.path[info.path_len - 2].node;
                        unsigned char edge = info.path[info.path_len - 1].edge;
                        
                        if (parent->is_list()) {
                            do_inplace_interior_list_erase(parent, edge);
                        } else if (parent->is_full()) {
                            do_inplace_interior_full_erase(parent, edge);
                        }
                        
                        retired_target = info.target;
                        success = true;
                    }
                }
                
                // Outside lock: cleanup
                dealloc_erase_speculation(alloc);
                if (deferred_eos) delete deferred_eos;
                if (retired_target) retire_node(retired_target);
                if (retired_parent) retire_node(retired_parent);
                if (retired_sibling) retire_node(retired_sibling);
                
                if (success) {
                    size_.fetch_sub(1);
                    return true;
                }
                continue;
            }
            
            // DELETE_EOS_INTERIOR - remove eos, may cause collapse
            if (info.op == erase_op::DELETE_EOS_INTERIOR) {
                T* deferred_eos = nullptr;
                ptr_t retired_target = nullptr;
                ptr_t retired_parent = nullptr;
                ptr_t retired_sibling = nullptr;
                bool success = false;
                
                {
                    std::lock_guard<mutex_t> lock(mutex_);
                    
                    if (!validate_erase_path(info)) {
                        dealloc_erase_speculation(alloc);
                        continue;
                    }
                    
                    T* p = get_eos_ptr(info.target);
                    if (!p) {
                        dealloc_erase_speculation(alloc);
                        continue;
                    }
                    
                    // Defer eos deletion
                    deferred_eos = p;
                    set_eos_ptr(info.target, nullptr);
                    info.target->bump_version();
                    
                    int child_cnt = info.target->child_count();
                    
                    if (child_cnt == 0) {
                        // Target becomes empty - delete it
                        if (info.path_len <= 1) {
                            root_.store(nullptr);
                            retired_target = info.target;
                        } else if (info.parent_needs_collapse && alloc.parent_merged) {
                            // Parent collapses
                            fill_collapse_node(alloc.parent_merged, info.parent_collapse_child);
                            
                            if (info.path_len <= 2) {
                                root_.store(alloc.parent_merged);
                            } else {
                                ptr_t grandparent = info.path[info.path_len - 3].node;
                                unsigned char parent_edge = info.path[info.path_len - 2].edge;
                                atomic_ptr* slot = get_child_slot(grandparent, parent_edge);
                                if (slot) {
                                    slot->store(alloc.parent_merged);
                                    grandparent->bump_version();
                                }
                            }
                            alloc.parent_merged = nullptr;
                            retired_target = info.target;
                            retired_parent = info.parent;
                            retired_sibling = info.parent_collapse_child;
                        } else {
                            // Simple removal from parent
                            ptr_t parent = info.path[info.path_len - 2].node;
                            unsigned char edge = info.path[info.path_len - 1].edge;
                            
                            if (parent->is_list()) {
                                do_inplace_interior_list_erase(parent, edge);
                            } else if (parent->is_full()) {
                                do_inplace_interior_full_erase(parent, edge);
                            }
                            retired_target = info.target;
                        }
                    }
                    // child_cnt > 0: target stays, just eos removed (already done above)
                    success = true;
                }
                
                // Outside lock: cleanup
                dealloc_erase_speculation(alloc);
                if (deferred_eos) delete deferred_eos;
                if (retired_target) retire_node(retired_target);
                if (retired_parent) retire_node(retired_parent);
                if (retired_sibling) retire_node(retired_sibling);
                
                if (success) {
                    size_.fetch_sub(1);
                    return true;
                }
                continue;
            }
            
            // COLLAPSE_AFTER_REMOVE - target has eos + 1 child, removing eos causes collapse
            if (info.op == erase_op::COLLAPSE_AFTER_REMOVE) {
                if (!alloc.merged) {
                    // Allocation failed, retry
                    dealloc_erase_speculation(alloc);
                    continue;
                }
                
                T* deferred_eos = nullptr;
                ptr_t retired_target = nullptr;
                ptr_t retired_child = nullptr;
                bool success = false;
                
                {
                    std::lock_guard<mutex_t> lock(mutex_);
                    
                    if (!validate_erase_path(info)) {
                        dealloc_erase_speculation(alloc);
                        continue;
                    }
                    
                    T* p = get_eos_ptr(info.target);
                    if (p) {
                        deferred_eos = p;
                        set_eos_ptr(info.target, nullptr);
                    }
                    
                    // Fill the pre-allocated merged node
                    fill_collapse_node(alloc.merged, info.collapse_child);
                    
                    if (info.path_len <= 1) {
                        root_.store(alloc.merged);
                    } else {
                        ptr_t parent = info.path[info.path_len - 2].node;
                        unsigned char edge = info.path[info.path_len - 1].edge;
                        atomic_ptr* slot = get_child_slot(parent, edge);
                        if (slot) {
                            slot->store(alloc.merged);
                            parent->bump_version();
                        }
                    }
                    
                    alloc.merged = nullptr;  // Transferred ownership
                    retired_target = info.target;
                    retired_child = info.collapse_child;
                    success = true;
                }
                
                // Outside lock: cleanup
                dealloc_erase_speculation(alloc);
                if (deferred_eos) delete deferred_eos;
                if (retired_target) retire_node(retired_target);
                if (retired_child) retire_node(retired_child);
                
                if (success) {
                    size_.fetch_sub(1);
                    return true;
                }
                continue;
            }
            
            // Fallback - should not reach here normally
            dealloc_erase_speculation(alloc);
            continue;
        }
    }
}

// =========================================================================
// LEGACY ERASE IMPLEMENTATION
// =========================================================================
erase_result erase_impl(atomic_ptr*, ptr_t n, std::string_view key) {
    erase_result res;
    if (!n) return res;
    if (n->is_leaf()) return erase_from_leaf(n, key);
    return erase_from_interior(n, key);
}

erase_result erase_from_leaf(ptr_t leaf, std::string_view key) {
    erase_result res;
    std::string_view skip = get_skip(leaf);
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) return res;
    key.remove_prefix(m);
    
    if (leaf->is_eos()) {
        if (!key.empty()) return res;
        res.erased = true;
        res.deleted_subtree = true;
        res.add_old(leaf);
        return res;
    }
    
    if (leaf->is_skip()) {
        if (!key.empty()) return res;
        res.erased = true;
        res.deleted_subtree = true;
        res.add_old(leaf);
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
            res.add_old(leaf);
            return res;
        }
        
        for (int i = idx; i < count - 1; ++i) {
            leaf->as_list()->leaf_values[i] = leaf->as_list()->leaf_values[i + 1];
        }
        leaf->as_list()->chars.remove_at(idx);
        leaf->bump_version();
        res.erased = true;
        return res;
    }
    
    if (!leaf->as_full()->valid.test(c)) return res;
    leaf->as_full()->valid.template atomic_clear<THREADED>(c);
    leaf->bump_version();
    res.erased = true;
    return res;
}

erase_result erase_from_interior(ptr_t n, std::string_view key) {
    erase_result res;
    std::string_view skip = get_skip(n);
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) return res;
    key.remove_prefix(m);
    
    if (key.empty()) {
        T* p = get_eos_ptr(n);
        if (!p) return res;
        delete p;
        set_eos_ptr(n, nullptr);
        n->bump_version();
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
        get_child_slot(n, c)->store(child_res.new_node);
        n->bump_version();
    }
    res.erased = true;
    for (int i = 0; i < child_res.old_count; ++i) res.add_old(child_res.old_nodes[i]);
    return res;
}

erase_result try_collapse_interior(ptr_t n) {
    erase_result res;
    res.erased = true;
    
    T* eos = get_eos_ptr(n);
    if (eos) return res;
    
    int child_cnt = n->child_count();
    if (child_cnt == 0) {
        res.deleted_subtree = true;
        res.add_old(n);
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

erase_result try_collapse_after_child_removal(ptr_t n, unsigned char removed_c, erase_result& child_res) {
    erase_result res;
    for (int i = 0; i < child_res.old_count; ++i) res.add_old(child_res.old_nodes[i]);
    res.erased = true;
    
    T* eos = get_eos_ptr(n);
    int remaining = n->child_count();
    
    if (n->is_list()) {
        int idx = n->as_list()->chars.find(removed_c);
        if (idx >= 0) remaining--;
    } else if (n->is_full()) {
        if (n->as_full()->valid.test(removed_c)) remaining--;
    }
    
    if (!eos && remaining == 0) {
        res.deleted_subtree = true;
        res.add_old(n);
        return res;
    }
    
    if (n->is_list()) {
        int idx = n->as_list()->chars.find(removed_c);
        if (idx >= 0) {
            int count = n->as_list()->chars.count();
            for (int i = idx; i < count - 1; ++i) {
                n->as_list()->children[i].store(n->as_list()->children[i + 1].load());
            }
            n->as_list()->children[count - 1].store(nullptr);
            n->as_list()->chars.remove_at(idx);
            n->bump_version();
        }
    } else if (n->is_full()) {
        n->as_full()->valid.template atomic_clear<THREADED>(removed_c);
        n->as_full()->children[removed_c].store(nullptr);
        n->bump_version();
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

erase_result collapse_single_child(ptr_t n, unsigned char c, ptr_t child, erase_result& res) {
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
            int cnt = child->as_list()->chars.count();
            for (int i = 0; i < cnt; ++i) {
                merged->as_list()->leaf_values[i] = child->as_list()->leaf_values[i];
            }
        } else {
            merged = builder_.make_leaf_full(new_skip);
            merged->as_full()->valid = child->as_full()->valid;
            child->as_full()->valid.for_each_set([child, merged](unsigned char ch) {
                merged->as_full()->leaf_values[ch] = child->as_full()->leaf_values[ch];
            });
        }
    } else {
        if (child->is_eos() || child->is_skip()) {
            merged = builder_.make_interior_skip(new_skip);
            merged->as_skip()->eos_ptr = get_eos_ptr(child);
            set_eos_ptr(child, nullptr);
        } else if (child->is_list()) {
            merged = builder_.make_interior_list(new_skip);
            merged->as_list()->eos_ptr = child->as_list()->eos_ptr;
            child->as_list()->eos_ptr = nullptr;
            merged->as_list()->chars = child->as_list()->chars;
            int cnt = child->as_list()->chars.count();
            for (int i = 0; i < cnt; ++i) {
                merged->as_list()->children[i].store(child->as_list()->children[i].load());
                child->as_list()->children[i].store(nullptr);
            }
        } else {
            merged = builder_.make_interior_full(new_skip);
            merged->as_full()->eos_ptr = child->as_full()->eos_ptr;
            child->as_full()->eos_ptr = nullptr;
            merged->as_full()->valid = child->as_full()->valid;
            child->as_full()->valid.for_each_set([child, merged](unsigned char ch) {
                merged->as_full()->children[ch].store(child->as_full()->children[ch].load());
                child->as_full()->children[ch].store(nullptr);
            });
        }
    }
    
    res.new_node = merged;
    res.add_old(n);
    res.add_old(child);
    return res;
}
