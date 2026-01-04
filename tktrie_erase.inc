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
    
    ptr_t collapse_child = nullptr;
    uint64_t collapse_child_header = 0;
    unsigned char collapse_char = 0;
    std::string parent_skip;
    std::string child_skip;
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
            info.parent_skip = std::string(skip);
            
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
                    info.collapse_child_header = info.collapse_child->header();
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
            info.path[info.path_len++] = {n, n->header(), c};
        }
    }
    
    info.op = erase_op::NOT_FOUND;
    return info;
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
// ALLOCATE MERGED NODE FOR COLLAPSE
// =========================================================================
ptr_t allocate_collapse_node(const erase_spec_info& info) {
    std::string new_skip = info.parent_skip;
    new_skip.push_back(static_cast<char>(info.collapse_char));
    new_skip.append(info.child_skip);
    
    ptr_t child = info.collapse_child;
    if (!child) return nullptr;
    
    if (child->is_leaf()) {
        if (child->is_eos() || child->is_skip()) {
            return builder_.make_leaf_skip(new_skip, T{});
        } else if (child->is_list()) {
            return builder_.make_leaf_list(new_skip);
        } else {
            return builder_.make_leaf_full(new_skip);
        }
    } else {
        if (child->is_eos() || child->is_skip()) {
            return builder_.make_interior_skip(new_skip);
        } else if (child->is_list()) {
            return builder_.make_interior_list(new_skip);
        } else {
            return builder_.make_interior_full(new_skip);
        }
    }
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
            
            if (info.op == erase_op::IN_PLACE_LEAF_LIST ||
                info.op == erase_op::IN_PLACE_LEAF_FULL ||
                info.op == erase_op::IN_PLACE_INTERIOR_LIST ||
                info.op == erase_op::IN_PLACE_INTERIOR_FULL) {
                
                std::lock_guard<mutex_t> lock(mutex_);
                
                if (!validate_erase_path(info)) continue;
                
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
                    return true;
                }
                continue;
            }
            
            if (info.op == erase_op::DELETE_LEAF_EOS ||
                info.op == erase_op::DELETE_LEAF_SKIP ||
                info.op == erase_op::DELETE_LAST_LEAF_LIST) {
                
                std::lock_guard<mutex_t> lock(mutex_);
                
                if (!validate_erase_path(info)) continue;
                
                if (info.path_len <= 1) {
                    root_.store(nullptr);
                    retire_node(info.target);
                    size_.fetch_sub(1);
                    return true;
                }
                
                ptr_t parent = info.path[info.path_len - 2].node;
                unsigned char edge = info.path[info.path_len - 1].edge;
                
                unsigned char collapse_c = 0;
                ptr_t collapse_child = nullptr;
                bool needs_collapse = check_collapse_needed(parent, edge, collapse_c, collapse_child);
                
                if (needs_collapse) {
                    ptr_t root = root_.load();
                    auto res = erase_impl(&root_, root, kb);
                    if (!res.erased) return false;
                    
                    if (res.deleted_subtree) root_.store(nullptr);
                    else if (res.new_node) root_.store(res.new_node);
                    
                    for (int i = 0; i < res.old_count; ++i) retire_node(res.old_nodes[i]);
                    size_.fetch_sub(1);
                    return true;
                }
                
                if (parent->is_list()) {
                    do_inplace_interior_list_erase(parent, edge);
                } else if (parent->is_full()) {
                    do_inplace_interior_full_erase(parent, edge);
                }
                
                retire_node(info.target);
                size_.fetch_sub(1);
                return true;
            }
            
            if (info.op == erase_op::DELETE_EOS_INTERIOR) {
                std::lock_guard<mutex_t> lock(mutex_);
                
                if (!validate_erase_path(info)) continue;
                
                T* p = get_eos_ptr(info.target);
                if (!p) continue;
                
                delete p;
                set_eos_ptr(info.target, nullptr);
                info.target->bump_version();
                
                int child_cnt = info.target->child_count();
                if (child_cnt == 0) {
                    if (info.path_len <= 1) {
                        root_.store(nullptr);
                        retire_node(info.target);
                    } else {
                        ptr_t parent = info.path[info.path_len - 2].node;
                        unsigned char edge = info.path[info.path_len - 1].edge;
                        
                        unsigned char collapse_c = 0;
                        ptr_t collapse_child = nullptr;
                        if (check_collapse_needed(parent, edge, collapse_c, collapse_child)) {
                            ptr_t root = root_.load();
                            auto res = erase_impl(&root_, root, kb);
                            if (res.new_node) root_.store(res.new_node);
                            for (int i = 0; i < res.old_count; ++i) retire_node(res.old_nodes[i]);
                            size_.fetch_sub(1);
                            return true;
                        }
                        
                        if (parent->is_list()) {
                            do_inplace_interior_list_erase(parent, edge);
                        } else if (parent->is_full()) {
                            do_inplace_interior_full_erase(parent, edge);
                        }
                        retire_node(info.target);
                    }
                } else if (child_cnt == 1) {
                    ptr_t root = root_.load();
                    auto res = erase_impl(&root_, root, kb);
                    if (res.new_node) {
                        if (info.path_len <= 1) {
                            root_.store(res.new_node);
                        } else {
                            ptr_t parent = info.path[info.path_len - 2].node;
                            unsigned char edge = info.path[info.path_len - 1].edge;
                            atomic_ptr* slot = get_child_slot(parent, edge);
                            if (slot) slot->store(res.new_node);
                            parent->bump_version();
                        }
                    }
                    for (int i = 0; i < res.old_count; ++i) retire_node(res.old_nodes[i]);
                }
                
                size_.fetch_sub(1);
                return true;
            }
            
            if (info.op == erase_op::COLLAPSE_AFTER_REMOVE) {
                ptr_t merged = allocate_collapse_node(info);
                if (!merged) {
                    std::lock_guard<mutex_t> lock(mutex_);
                    ptr_t root = root_.load();
                    auto res = erase_impl(&root_, root, kb);
                    if (!res.erased) return false;
                    if (res.deleted_subtree) root_.store(nullptr);
                    else if (res.new_node) root_.store(res.new_node);
                    for (int i = 0; i < res.old_count; ++i) retire_node(res.old_nodes[i]);
                    size_.fetch_sub(1);
                    return true;
                }
                
                {
                    std::lock_guard<mutex_t> lock(mutex_);
                    
                    if (!validate_erase_path(info)) {
                        switch (merged->type()) {
                            case TYPE_EOS: delete merged->as_eos(); break;
                            case TYPE_SKIP: delete merged->as_skip(); break;
                            case TYPE_LIST: delete merged->as_list(); break;
                            case TYPE_FULL: delete merged->as_full(); break;
                        }
                        continue;
                    }
                    
                    T* p = get_eos_ptr(info.target);
                    if (p) {
                        delete p;
                        set_eos_ptr(info.target, nullptr);
                    }
                    
                    fill_collapse_node(merged, info.collapse_child);
                    
                    if (info.path_len <= 1) {
                        root_.store(merged);
                    } else {
                        ptr_t parent = info.path[info.path_len - 2].node;
                        unsigned char edge = info.path[info.path_len - 1].edge;
                        atomic_ptr* slot = get_child_slot(parent, edge);
                        if (slot) {
                            slot->store(merged);
                            parent->bump_version();
                        }
                    }
                    
                    retire_node(info.target);
                    retire_node(info.collapse_child);
                    size_.fetch_sub(1);
                    return true;
                }
            }
            
            {
                std::lock_guard<mutex_t> lock(mutex_);
                ptr_t root = root_.load();
                auto res = erase_impl(&root_, root, kb);
                if (!res.erased) return false;
                if (res.deleted_subtree) root_.store(nullptr);
                else if (res.new_node) root_.store(res.new_node);
                for (int i = 0; i < res.old_count; ++i) retire_node(res.old_nodes[i]);
                size_.fetch_sub(1);
                return true;
            }
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
