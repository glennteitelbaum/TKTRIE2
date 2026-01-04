// tktrie_insert_spec.inc - Speculative insert operations for tktrie
// This file is included inside the tktrie class body after tktrie_insert.inc

// =========================================================================
// SPECULATIVE INSERT
// =========================================================================

// Path entry for version tracking
struct path_entry {
    ptr_t node;
    uint64_t version;
    unsigned char edge;  // char used to reach next node (0 for root)
};

// Speculative operation types
enum class spec_op {
    EXISTS,              // Key already exists
    IN_PLACE_LEAF,       // Add char to leaf (has room)
    IN_PLACE_INTERIOR,   // Add child/eos to interior (has room)
    EMPTY_TREE,          // Tree is empty
    DEMOTE_LEAF_EOS,     // leaf_eos -> interior + child
    SPLIT_LEAF_SKIP,     // Divergence in leaf_skip
    PREFIX_LEAF_SKIP,    // Key is prefix of leaf_skip
    EXTEND_LEAF_SKIP,    // leaf_skip is prefix of key
    SPLIT_LEAF_LIST,     // Divergence in list/full leaf skip
    PREFIX_LEAF_LIST,    // Key ends in list/full leaf skip
    ADD_EOS_LEAF_LIST,   // Add eos to list/full leaf
    LIST_TO_FULL_LEAF,   // Leaf list->full + add char
    DEMOTE_LEAF_LIST,    // Key longer than skip+1 for list/full leaf
    SPLIT_INTERIOR,      // Divergence in interior skip
    PREFIX_INTERIOR,     // Key is prefix of interior skip
    ADD_CHILD_CONVERT,   // Add child, need to convert node type
};

struct speculative_info {
    static constexpr int MAX_PATH = 64;
    path_entry path[MAX_PATH];
    int path_len = 0;
    
    spec_op op = spec_op::EXISTS;
    ptr_t target = nullptr;
    uint64_t target_version = 0;
    unsigned char c = 0;
    bool is_eos = false;
    size_t match_pos = 0;
    
    // Captured strings for allocation (skip content may change)
    std::string target_skip;
    std::string remaining_key;
};

// Pre-allocated nodes for speculative insert
struct pre_alloc {
    ptr_t nodes[8] = {};
    int count = 0;
    ptr_t root_replacement = nullptr;  // What to store at insertion point
    
    void add(ptr_t n) { if (n) nodes[count++] = n; }
    void clear() {
        for (int i = 0; i < count; ++i) nodes[i] = nullptr;
        count = 0;
        root_replacement = nullptr;
    }
};

// Probe: capture path, versions, and classify operation
speculative_info probe_speculative(ptr_t n, std::string_view key) const noexcept {
    speculative_info info;
    info.remaining_key = std::string(key);
    
    if (!n) {
        info.op = spec_op::EMPTY_TREE;
        return info;
    }
    
    // Add root to path
    info.path[info.path_len++] = {n, n->version(), 0};
    
    while (n) {
        if (n->is_leaf()) {
            std::string_view skip = get_skip(n);
            size_t m = match_skip(skip, key);
            
            if (n->is_eos()) {
                if (key.empty()) {
                    info.op = spec_op::EXISTS;
                    return info;
                }
                info.op = spec_op::DEMOTE_LEAF_EOS;
                info.target = n;
                info.target_version = n->version();
                info.remaining_key = std::string(key);
                return info;
            }
            
            if (n->is_skip()) {
                if (m == skip.size() && m == key.size()) {
                    info.op = spec_op::EXISTS;
                    return info;
                }
                info.target = n;
                info.target_version = n->version();
                info.target_skip = std::string(skip);
                info.match_pos = m;
                
                if (m < skip.size() && m < key.size()) {
                    info.op = spec_op::SPLIT_LEAF_SKIP;
                    info.remaining_key = std::string(key);
                    return info;
                }
                if (m == key.size()) {
                    info.op = spec_op::PREFIX_LEAF_SKIP;
                    info.remaining_key = std::string(key);
                    return info;
                }
                info.op = spec_op::EXTEND_LEAF_SKIP;
                info.remaining_key = std::string(key);
                return info;
            }
            
            // LIST or FULL leaf
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            
            if (m < skip.size() && m < key.size()) {
                info.op = spec_op::SPLIT_LEAF_LIST;
                info.match_pos = m;
                info.remaining_key = std::string(key);
                return info;
            }
            if (m < skip.size()) {
                info.op = spec_op::PREFIX_LEAF_LIST;
                info.match_pos = m;
                info.remaining_key = std::string(key);
                return info;
            }
            key.remove_prefix(m);
            info.remaining_key = std::string(key);
            
            if (key.empty()) {
                info.op = spec_op::ADD_EOS_LEAF_LIST;
                return info;
            }
            if (key.size() != 1) {
                info.op = spec_op::DEMOTE_LEAF_LIST;
                return info;
            }
            
            unsigned char c = static_cast<unsigned char>(key[0]);
            info.c = c;
            
            if (n->is_list()) {
                if (n->as_list()->chars.find(c) >= 0) {
                    info.op = spec_op::EXISTS;
                    return info;
                }
                if (n->as_list()->chars.count() < LIST_MAX) {
                    info.op = spec_op::IN_PLACE_LEAF;
                    return info;
                }
                info.op = spec_op::LIST_TO_FULL_LEAF;
                return info;
            }
            // FULL
            if (n->as_full()->valid.test(c)) {
                info.op = spec_op::EXISTS;
                return info;
            }
            info.op = spec_op::IN_PLACE_LEAF;
            return info;
        }
        
        // Interior node
        std::string_view skip = get_skip(n);
        size_t m = match_skip(skip, key);
        
        if (m < skip.size() && m < key.size()) {
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
            T* p = get_eos_ptr(n);
            if (p) {
                info.op = spec_op::EXISTS;
                return info;
            }
            info.op = spec_op::IN_PLACE_INTERIOR;
            info.target = n;
            info.target_version = n->version();
            info.is_eos = true;
            return info;
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = find_child(n, c);
        
        if (!child) {
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            info.c = c;
            info.remaining_key = std::string(key.substr(1));
            
            if (n->is_list() && n->as_list()->chars.count() < LIST_MAX) {
                info.op = spec_op::IN_PLACE_INTERIOR;
                return info;
            }
            if (n->is_full()) {
                info.op = spec_op::IN_PLACE_INTERIOR;
                return info;
            }
            info.op = spec_op::ADD_CHILD_CONVERT;
            return info;
        }
        
        key.remove_prefix(1);
        n = child;
        if (info.path_len < speculative_info::MAX_PATH) {
            info.path[info.path_len++] = {n, n->version(), c};
        }
    }
    
    info.op = spec_op::EMPTY_TREE;
    return info;
}

// Allocate nodes for speculative operation (called outside lock)
pre_alloc allocate_speculative(const speculative_info& info, const T& value) {
    pre_alloc alloc;
    std::string_view key = info.remaining_key;
    std::string_view skip = info.target_skip;
    size_t m = info.match_pos;
    
    switch (info.op) {
    case spec_op::EMPTY_TREE: {
        alloc.root_replacement = create_leaf_for_key(key, value);
        alloc.add(alloc.root_replacement);
        break;
    }
    case spec_op::DEMOTE_LEAF_EOS: {
        ptr_t interior = builder_.make_interior_list("");
        interior->as_list()->eos_ptr = new T();  // placeholder, will copy
        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = create_leaf_for_key(key.substr(1), value);
        interior->as_list()->chars.add(c);
        interior->as_list()->children[0].store(child);
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::SPLIT_LEAF_SKIP: {
        std::string common(skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);
        
        ptr_t interior = builder_.make_interior_list(common);
        ptr_t old_child = builder_.make_leaf_skip(skip.substr(m + 1), T{});
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        
        interior->as_list()->chars.add(old_c);
        interior->as_list()->chars.add(new_c);
        interior->as_list()->children[0].store(old_child);
        interior->as_list()->children[1].store(new_child);
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(old_child);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_LEAF_SKIP: {
        ptr_t interior = builder_.make_interior_list(std::string(key));
        interior->as_list()->eos_ptr = new T(value);
        
        unsigned char c = static_cast<unsigned char>(skip[m]);
        ptr_t child = builder_.make_leaf_skip(skip.substr(m + 1), T{});
        interior->as_list()->chars.add(c);
        interior->as_list()->children[0].store(child);
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::EXTEND_LEAF_SKIP: {
        ptr_t interior = builder_.make_interior_list(std::string(skip));
        interior->as_list()->eos_ptr = new T();  // placeholder
        
        unsigned char c = static_cast<unsigned char>(key[m]);
        ptr_t child = create_leaf_for_key(key.substr(m + 1), value);
        interior->as_list()->chars.add(c);
        interior->as_list()->children[0].store(child);
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::SPLIT_LEAF_LIST: {
        std::string common(skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);
        
        ptr_t interior = builder_.make_interior_list(common);
        // Old child will be cloned from target during commit
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        
        interior->as_list()->chars.add(old_c);
        interior->as_list()->chars.add(new_c);
        interior->as_list()->children[1].store(new_child);
        // children[0] filled during commit
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_LEAF_LIST: {
        ptr_t interior = builder_.make_interior_list(std::string(key));
        interior->as_list()->eos_ptr = new T(value);
        
        unsigned char c = static_cast<unsigned char>(skip[m]);
        interior->as_list()->chars.add(c);
        // children[0] filled during commit (clone with new skip)
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        break;
    }
    case spec_op::LIST_TO_FULL_LEAF: {
        ptr_t full = builder_.make_leaf_full(std::string(skip));
        // Values copied from target during commit
        full->as_full()->valid.set(info.c);
        full->as_full()->leaf_values[info.c] = value;
        
        alloc.root_replacement = full;
        alloc.add(full);
        break;
    }
    // For complex operations, we allocate the structure during commit
    // since they depend on target node's current state
    case spec_op::ADD_EOS_LEAF_LIST:
    case spec_op::DEMOTE_LEAF_LIST:
    case spec_op::ADD_CHILD_CONVERT:
        // These need target node content, handle in commit
        break;
    case spec_op::SPLIT_INTERIOR:
    case spec_op::PREFIX_INTERIOR:
        // Similar - need to clone interior content
        break;
    default:
        break;
    }
    
    return alloc;
}

// Validate path versions (called under lock)
bool validate_path(const speculative_info& info) const noexcept {
    // Check each node in path still has same version
    for (int i = 0; i < info.path_len; ++i) {
        if (info.path[i].node->version() != info.path[i].version) {
            return false;
        }
    }
    // Check target if different from last path entry
    if (info.target && (info.path_len == 0 || info.path[info.path_len-1].node != info.target)) {
        if (info.target->version() != info.target_version) {
            return false;
        }
    }
    return true;
}

// Find the slot to update based on path
atomic_ptr* find_slot_for_commit(const speculative_info& info) noexcept {
    if (info.path_len <= 1) {
        return &root_;
    }
    // Parent is second-to-last in path
    ptr_t parent = info.path[info.path_len - 2].node;
    unsigned char edge = info.path[info.path_len - 1].edge;
    return get_child_slot(parent, edge);
}

// Commit speculative allocation (called under lock after validation)
bool commit_speculative(speculative_info& info, pre_alloc& alloc, const T& /*value*/) {
    switch (info.op) {
    case spec_op::EMPTY_TREE:
        root_.store(alloc.root_replacement);
        return true;
        
    case spec_op::DEMOTE_LEAF_EOS: {
        // Copy old value to eos_ptr
        ptr_t interior = alloc.root_replacement;
        *interior->as_list()->eos_ptr = info.target->as_eos()->leaf_value;
        
        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::SPLIT_LEAF_SKIP: {
        // Copy old value to old_child
        ptr_t interior = alloc.root_replacement;
        ptr_t old_child = interior->as_list()->children[0].load();
        old_child->as_skip()->leaf_value = info.target->as_skip()->leaf_value;
        
        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::PREFIX_LEAF_SKIP: {
        // Copy old value to child
        ptr_t interior = alloc.root_replacement;
        ptr_t child = interior->as_list()->children[0].load();
        child->as_skip()->leaf_value = info.target->as_skip()->leaf_value;
        
        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::EXTEND_LEAF_SKIP: {
        // Copy old value to eos_ptr
        ptr_t interior = alloc.root_replacement;
        *interior->as_list()->eos_ptr = info.target->as_skip()->leaf_value;
        
        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::SPLIT_LEAF_LIST: {
        ptr_t interior = alloc.root_replacement;
        std::string_view skip = info.target_skip;
        size_t m = info.match_pos;
        
        // Clone old leaf with shorter skip
        ptr_t old_child = clone_leaf_with_skip(info.target, skip.substr(m + 1));
        interior->as_list()->children[0].store(old_child);
        alloc.add(old_child);
        
        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::PREFIX_LEAF_LIST: {
        ptr_t interior = alloc.root_replacement;
        std::string_view skip = info.target_skip;
        size_t m = info.match_pos;
        
        ptr_t old_child = clone_leaf_with_skip(info.target, skip.substr(m + 1));
        interior->as_list()->children[0].store(old_child);
        alloc.add(old_child);
        
        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::LIST_TO_FULL_LEAF: {
        ptr_t full = alloc.root_replacement;
        // Copy existing values from list
        auto* list = info.target->as_list();
        for (int i = 0; i < list->chars.count(); ++i) {
            unsigned char ch = list->chars.char_at(i);
            full->as_full()->valid.set(ch);
            full->as_full()->leaf_values[ch] = list->leaf_values[i];
        }
        
        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(full);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    // For remaining complex cases, fall back to COW-style allocation under lock
    case spec_op::ADD_EOS_LEAF_LIST:
    case spec_op::DEMOTE_LEAF_LIST:
    case spec_op::SPLIT_INTERIOR:
    case spec_op::PREFIX_INTERIOR:
    case spec_op::ADD_CHILD_CONVERT:
        return false;  // Signal to use fallback
        
    default:
        return false;
    }
}

// Deallocate failed speculation (called outside lock)
// Note: We use direct delete, NOT recursive dealloc_node, because
// children are stored separately in the nodes array and would be double-freed
void dealloc_speculation(pre_alloc& alloc) {
    for (int i = 0; i < alloc.count; ++i) {
        ptr_t n = alloc.nodes[i];
        if (!n) continue;
        
        // Delete any eos_ptr we allocated
        if (!n->is_leaf()) {
            T* eos = get_eos_ptr(n);
            if (eos) delete eos;
        }
        
        // Direct delete based on type (no recursion into children)
        switch (n->type()) {
            case TYPE_EOS: delete n->as_eos(); break;
            case TYPE_SKIP: delete n->as_skip(); break;
            case TYPE_LIST: delete n->as_list(); break;
            case TYPE_FULL: delete n->as_full(); break;
        }
    }
    alloc.clear();
}

// Legacy probe_info for compatibility with in-place path
enum class probe_result { NOT_FOUND, EXISTS, IN_PLACE_LEAF, IN_PLACE_INTERIOR, NEEDS_COW };

struct probe_info {
    probe_result result = probe_result::NOT_FOUND;
    ptr_t target = nullptr;
    unsigned char c = 0;
    bool is_eos = false;
};

// Read-only traversal to check if in-place insert is possible
// Called under EBR guard, no mutex
probe_info probe_insert(ptr_t n, std::string_view key) const noexcept {
    probe_info info;
    
    while (n) {
        if (n->is_leaf()) {
            std::string_view skip = get_skip(n);
            size_t m = match_skip(skip, key);
            if (m < skip.size()) {
                info.result = probe_result::NEEDS_COW;
                return info;
            }
            key.remove_prefix(m);
            
            if (n->is_eos() || n->is_skip()) {
                info.result = key.empty() ? probe_result::EXISTS : probe_result::NEEDS_COW;
                return info;
            }
            
            // LIST or FULL leaf
            if (key.empty()) {
                info.result = probe_result::NEEDS_COW;  // Need to add EOS to leaf
                return info;
            }
            if (key.size() != 1) {
                info.result = probe_result::NEEDS_COW;  // Key too long
                return info;
            }
            
            unsigned char c = static_cast<unsigned char>(key[0]);
            if (n->is_list()) {
                if (n->as_list()->chars.find(c) >= 0) {
                    info.result = probe_result::EXISTS;
                    return info;
                }
                if (n->as_list()->chars.count() < LIST_MAX) {
                    info.result = probe_result::IN_PLACE_LEAF;
                    info.target = n;
                    info.c = c;
                    return info;
                }
                info.result = probe_result::NEEDS_COW;  // LIST -> FULL conversion
                return info;
            }
            // FULL leaf
            if (n->as_full()->valid.test(c)) {
                info.result = probe_result::EXISTS;
                return info;
            }
            info.result = probe_result::IN_PLACE_LEAF;
            info.target = n;
            info.c = c;
            return info;
        }
        
        // Interior node
        std::string_view skip = get_skip(n);
        size_t m = match_skip(skip, key);
        if (m < skip.size()) {
            info.result = probe_result::NEEDS_COW;
            return info;
        }
        key.remove_prefix(m);
        
        if (key.empty()) {
            T* p = get_eos_ptr(n);
            if (p) {
                info.result = probe_result::EXISTS;
                return info;
            }
            info.result = probe_result::IN_PLACE_INTERIOR;
            info.target = n;
            info.is_eos = true;
            return info;
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = find_child(n, c);
        
        if (!child) {
            // Need to add new child
            if (n->is_list() && n->as_list()->chars.count() < LIST_MAX) {
                info.result = probe_result::IN_PLACE_INTERIOR;
                info.target = n;
                info.c = c;
                return info;
            }
            if (n->is_full()) {
                info.result = probe_result::IN_PLACE_INTERIOR;
                info.target = n;
                info.c = c;
                return info;
            }
            info.result = probe_result::NEEDS_COW;
            return info;
        }
        
        key.remove_prefix(1);
        n = child;
    }
    
    // Empty tree
    info.result = probe_result::NEEDS_COW;
    return info;
}

// Try in-place insert under mutex, return true if succeeded
bool try_in_place_insert(const probe_info& info, std::string_view key, const T& value) {
    if (info.result == probe_result::IN_PLACE_LEAF) {
        ptr_t n = info.target;
        unsigned char c = info.c;
        
        if (n->is_list()) {
            // Re-verify conditions
            if (n->as_list()->chars.find(c) >= 0) return false;  // Now exists
            if (n->as_list()->chars.count() >= LIST_MAX) return false;  // Now full
            
            int idx = n->as_list()->chars.add(c);
            n->as_list()->leaf_values[idx] = value;
            n->bump_version();
            return true;
        }
        // FULL
        if (n->as_full()->valid.test(c)) return false;  // Now exists
        n->as_full()->valid.template atomic_set<THREADED>(c);
        n->as_full()->leaf_values[c] = value;
        n->bump_version();
        return true;
    }
    
    if (info.result == probe_result::IN_PLACE_INTERIOR) {
        ptr_t n = info.target;
        
        if (info.is_eos) {
            if (get_eos_ptr(n)) return false;  // Now has eos
            set_eos_ptr(n, new T(value));
            n->bump_version();
            return true;
        }
        
        unsigned char c = info.c;
        
        // Build the child node (allocation, but outside critical section would be better)
        // For now, do it here
        std::string_view remaining = key;
        // Skip to find remaining key after this node
        std::string_view skip = get_skip(n);
        remaining.remove_prefix(skip.size() + 1);  // skip + the char c
        
        ptr_t child = create_leaf_for_key(remaining, value);
        
        if (n->is_list()) {
            if (n->as_list()->chars.find(c) >= 0) {
                builder_.dealloc_node(child);
                return false;  // Child now exists
            }
            if (n->as_list()->chars.count() >= LIST_MAX) {
                builder_.dealloc_node(child);
                return false;  // List now full
            }
            int idx = n->as_list()->chars.add(c);
            n->as_list()->children[idx].store(child);
            n->bump_version();
            return true;
        }
        // FULL
        if (find_child(n, c)) {
            builder_.dealloc_node(child);
            return false;  // Child now exists
        }
        n->as_full()->valid.template atomic_set<THREADED>(c);
        n->as_full()->children[c].store(child);
        n->bump_version();
        return true;
    }
    
    return false;
}

std::pair<iterator, bool> insert_locked(const Key& key, std::string_view kb, const T& value) {
    if constexpr (!THREADED) {
        // Non-threaded: simple path
        std::lock_guard<mutex_t> lock(mutex_);
        
        ptr_t root = root_.load();
        auto res = insert_impl(&root_, root, kb, value);
        
        if (!res.inserted) {
            for (auto* old : res.old_nodes) retire_node(old);
            return {find(key), false};
        }
        
        if (res.new_node) root_.store(res.new_node);
        for (auto* old : res.old_nodes) retire_node(old);
        ++size_;
        
        return {iterator(this, std::string(kb), value), true};
    } else {
        // Threaded: speculative allocation path
        while (true) {
            speculative_info spec;
            {
                auto& slot = get_ebr_slot();
                auto guard = slot.get_guard();
                spec = probe_speculative(root_.load(), kb);
            }
            
            if (spec.op == spec_op::EXISTS) {
                return {find(key), false};
            }
            
            // In-place operations: short lock path
            if (spec.op == spec_op::IN_PLACE_LEAF || spec.op == spec_op::IN_PLACE_INTERIOR) {
                probe_info info;
                info.target = spec.target;
                info.c = spec.c;
                info.is_eos = spec.is_eos;
                info.result = (spec.op == spec_op::IN_PLACE_LEAF) 
                    ? probe_result::IN_PLACE_LEAF 
                    : probe_result::IN_PLACE_INTERIOR;
                
                std::lock_guard<mutex_t> lock(mutex_);
                
                // Validate target version
                if (spec.target && spec.target->version() != spec.target_version) {
                    continue;  // Retry
                }
                
                if (try_in_place_insert(info, kb, value)) {
                    size_.fetch_add(1);
                    return {iterator(this, std::string(kb), value), true};
                }
                continue;  // Retry
            }
            
            // Speculative allocation path
            pre_alloc alloc = allocate_speculative(spec, value);
            
            {
                std::lock_guard<mutex_t> lock(mutex_);
                
                // Validate path versions
                if (!validate_path(spec)) {
                    // Path changed, retry
                    // Note: unlock happens here, dealloc outside
                } else {
                    // Try to commit
                    if (alloc.root_replacement && commit_speculative(spec, alloc, value)) {
                        // Success! Retire old node
                        if (spec.target) {
                            retire_node(spec.target);
                        }
                        size_.fetch_add(1);
                        return {iterator(this, std::string(kb), value), true};
                    }
                    // Commit returned false - need fallback for complex ops
                    // For now, use full COW under lock for these cases
                    if (spec.op == spec_op::ADD_EOS_LEAF_LIST ||
                        spec.op == spec_op::DEMOTE_LEAF_LIST ||
                        spec.op == spec_op::SPLIT_INTERIOR ||
                        spec.op == spec_op::PREFIX_INTERIOR ||
                        spec.op == spec_op::ADD_CHILD_CONVERT) {
                        
                        // Dealloc any partial allocation first (non-recursive)
                        dealloc_speculation(alloc);
                        
                        // Re-validate and do COW
                        if (validate_path(spec)) {
                            ptr_t root = root_.load();
                            auto res = insert_impl(&root_, root, kb, value);
                            
                            if (!res.inserted) {
                                for (auto* old : res.old_nodes) retire_node(old);
                                return {find(key), false};
                            }
                            
                            if (res.new_node) root_.store(res.new_node);
                            for (auto* old : res.old_nodes) retire_node(old);
                            size_.fetch_add(1);
                            
                            return {iterator(this, std::string(kb), value), true};
                        }
                        // else retry
                    }
                }
            }
            
            // Dealloc failed speculation outside lock
            dealloc_speculation(alloc);
            // Loop to retry
        }
    }
}
