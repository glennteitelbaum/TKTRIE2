// tktrie_insert_spec.inc - Speculative insert operations for tktrie
// This file is included inside the tktrie class body after tktrie_insert.inc

// =========================================================================
// SPECULATIVE INSERT
// =========================================================================

// Path entry for version tracking - store full header for comparison
struct path_entry {
    ptr_t node;
    uint64_t header;  // Full header including flags - any change invalidates
    unsigned char edge;
};

// Speculative operation types
enum class spec_op {
    EXISTS,
    IN_PLACE_LEAF,
    IN_PLACE_INTERIOR,
    EMPTY_TREE,
    DEMOTE_LEAF_EOS,
    SPLIT_LEAF_SKIP,
    PREFIX_LEAF_SKIP,
    EXTEND_LEAF_SKIP,
    SPLIT_LEAF_LIST,
    PREFIX_LEAF_LIST,
    ADD_EOS_LEAF_LIST,
    LIST_TO_FULL_LEAF,
    DEMOTE_LEAF_LIST,
    SPLIT_INTERIOR,
    PREFIX_INTERIOR,
    ADD_CHILD_CONVERT,
};

struct speculative_info {
    static constexpr int MAX_PATH = 64;
    path_entry path[MAX_PATH];
    int path_len = 0;
    
    spec_op op = spec_op::EXISTS;
    ptr_t target = nullptr;
    uint64_t target_header = 0;  // Full header
    unsigned char c = 0;
    bool is_eos = false;
    size_t match_pos = 0;
    bool target_is_list = false;  // For clone pre-allocation
    
    std::string target_skip;
    std::string remaining_key;
};

// Pre-allocated nodes for speculative insert
struct pre_alloc {
    static constexpr int MAX_NODES = 8;
    ptr_t nodes[MAX_NODES] = {};
    int count = 0;
    ptr_t root_replacement = nullptr;
    
    // For in-place interior path
    ptr_t in_place_child = nullptr;
    T* in_place_eos = nullptr;
    
    // For clone cases (SPLIT_LEAF_LIST, PREFIX_LEAF_LIST)
    ptr_t clone_shell = nullptr;
    
    void add(ptr_t n) { if (n && count < MAX_NODES) nodes[count++] = n; }
    void clear() {
        for (int i = 0; i < count; ++i) nodes[i] = nullptr;
        count = 0;
        root_replacement = nullptr;
        in_place_child = nullptr;
        in_place_eos = nullptr;
        clone_shell = nullptr;
    }
};

// Probe: capture path, headers, and classify operation
speculative_info probe_speculative(ptr_t n, std::string_view key) const noexcept {
    speculative_info info;
    info.remaining_key = std::string(key);
    
    if (!n) {
        info.op = spec_op::EMPTY_TREE;
        return info;
    }
    
    info.path[info.path_len++] = {n, n->header(), 0};
    
    while (n) {
        if (n->is_leaf()) {
            std::string_view skip = get_skip(n);
            size_t m = match_skip_impl(skip, key);
            
            if (n->is_eos()) {
                if (key.empty()) {
                    info.op = spec_op::EXISTS;
                    return info;
                }
                info.op = spec_op::DEMOTE_LEAF_EOS;
                info.target = n;
                info.target_header = n->header();
                info.remaining_key = std::string(key);
                return info;
            }
            
            if (n->is_skip()) {
                if (m == skip.size() && m == key.size()) {
                    info.op = spec_op::EXISTS;
                    return info;
                }
                info.target = n;
                info.target_header = n->header();
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
            info.target_header = n->header();
            info.target_skip = std::string(skip);
            info.target_is_list = n->is_list();
            
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
            if (n->as_full()->valid.test(c)) {
                info.op = spec_op::EXISTS;
                return info;
            }
            info.op = spec_op::IN_PLACE_LEAF;
            return info;
        }
        
        // Interior node
        std::string_view skip = get_skip(n);
        size_t m = match_skip_impl(skip, key);
        
        if (m < skip.size() && m < key.size()) {
            info.op = spec_op::SPLIT_INTERIOR;
            info.target = n;
            info.target_header = n->header();
            info.target_skip = std::string(skip);
            info.match_pos = m;
            info.remaining_key = std::string(key);
            return info;
        }
        if (m < skip.size()) {
            info.op = spec_op::PREFIX_INTERIOR;
            info.target = n;
            info.target_header = n->header();
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
            info.target_header = n->header();
            info.is_eos = true;
            return info;
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = find_child(n, c);
        
        if (!child) {
            info.target = n;
            info.target_header = n->header();
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
            info.path[info.path_len++] = {n, n->header(), c};
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
        interior->as_list()->eos_ptr = new T();
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
        interior->as_list()->eos_ptr = new T();
        
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
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        
        // Pre-allocate clone shell for old leaf
        std::string_view clone_skip = skip.substr(m + 1);
        if (info.target_is_list) {
            alloc.clone_shell = builder_.make_leaf_list(clone_skip);
        } else {
            alloc.clone_shell = builder_.make_leaf_full(clone_skip);
        }
        
        interior->as_list()->chars.add(old_c);
        interior->as_list()->chars.add(new_c);
        interior->as_list()->children[0].store(alloc.clone_shell);
        interior->as_list()->children[1].store(new_child);
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(alloc.clone_shell);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_LEAF_LIST: {
        ptr_t interior = builder_.make_interior_list(std::string(key));
        interior->as_list()->eos_ptr = new T(value);
        
        unsigned char c = static_cast<unsigned char>(skip[m]);
        
        // Pre-allocate clone shell for old leaf
        std::string_view clone_skip = skip.substr(m + 1);
        if (info.target_is_list) {
            alloc.clone_shell = builder_.make_leaf_list(clone_skip);
        } else {
            alloc.clone_shell = builder_.make_leaf_full(clone_skip);
        }
        
        interior->as_list()->chars.add(c);
        interior->as_list()->children[0].store(alloc.clone_shell);
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(alloc.clone_shell);
        break;
    }
    case spec_op::LIST_TO_FULL_LEAF: {
        ptr_t full = builder_.make_leaf_full(std::string(skip));
        full->as_full()->valid.set(info.c);
        full->as_full()->leaf_values[info.c] = value;
        
        alloc.root_replacement = full;
        alloc.add(full);
        break;
    }
    case spec_op::ADD_EOS_LEAF_LIST:
    case spec_op::DEMOTE_LEAF_LIST:
    case spec_op::ADD_CHILD_CONVERT:
    case spec_op::SPLIT_INTERIOR:
    case spec_op::PREFIX_INTERIOR:
        break;
    case spec_op::IN_PLACE_LEAF:
        // No pre-allocation needed - just updating existing leaf values
        break;
    case spec_op::IN_PLACE_INTERIOR:
        // Pre-allocate eos or child outside lock
        if (info.is_eos) {
            alloc.in_place_eos = new T(value);
        } else {
            alloc.in_place_child = create_leaf_for_key(info.remaining_key, value);
        }
        break;
    default:
        break;
    }
    
    return alloc;
}

// Validate path - compare full headers (includes flags and version)
bool validate_path(const speculative_info& info) const noexcept {
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
    return true;
}

atomic_ptr* find_slot_for_commit(const speculative_info& info) noexcept {
    if (info.path_len <= 1) {
        return &root_;
    }
    ptr_t parent = info.path[info.path_len - 2].node;
    unsigned char edge = info.path[info.path_len - 1].edge;
    return get_child_slot(parent, edge);
}

bool commit_speculative(speculative_info& info, pre_alloc& alloc, const T& /*value*/) {
    switch (info.op) {
    case spec_op::EMPTY_TREE:
        root_.store(alloc.root_replacement);
        return true;
        
    case spec_op::DEMOTE_LEAF_EOS: {
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
        
        // Fill pre-allocated clone shell with content from target
        ptr_t clone = alloc.clone_shell;
        if (info.target_is_list) {
            clone->as_list()->chars = info.target->as_list()->chars;
            int cnt = info.target->as_list()->chars.count();
            for (int i = 0; i < cnt; ++i) {
                clone->as_list()->leaf_values[i] = info.target->as_list()->leaf_values[i];
            }
        } else {
            clone->as_full()->valid = info.target->as_full()->valid;
            info.target->as_full()->valid.for_each_set([&](unsigned char c) {
                clone->as_full()->leaf_values[c] = info.target->as_full()->leaf_values[c];
            });
        }
        
        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::PREFIX_LEAF_LIST: {
        ptr_t interior = alloc.root_replacement;
        
        // Fill pre-allocated clone shell with content from target
        ptr_t clone = alloc.clone_shell;
        if (info.target_is_list) {
            clone->as_list()->chars = info.target->as_list()->chars;
            int cnt = info.target->as_list()->chars.count();
            for (int i = 0; i < cnt; ++i) {
                clone->as_list()->leaf_values[i] = info.target->as_list()->leaf_values[i];
            }
        } else {
            clone->as_full()->valid = info.target->as_full()->valid;
            info.target->as_full()->valid.for_each_set([&](unsigned char c) {
                clone->as_full()->leaf_values[c] = info.target->as_full()->leaf_values[c];
            });
        }
        
        atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
        slot->store(interior);
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        return true;
    }
    case spec_op::LIST_TO_FULL_LEAF: {
        ptr_t full = alloc.root_replacement;
        auto* list = info.target->as_list();
        int cnt = list->chars.count();
        for (int i = 0; i < cnt; ++i) {
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
    case spec_op::ADD_EOS_LEAF_LIST:
    case spec_op::DEMOTE_LEAF_LIST:
    case spec_op::SPLIT_INTERIOR:
    case spec_op::PREFIX_INTERIOR:
    case spec_op::ADD_CHILD_CONVERT:
        return false;
        
    default:
        return false;
    }
}

void dealloc_speculation(pre_alloc& alloc) {
    // Clean up nodes array
    for (int i = 0; i < alloc.count; ++i) {
        ptr_t n = alloc.nodes[i];
        if (!n) continue;
        
        if (!n->is_leaf()) {
            T* eos = get_eos_ptr(n);
            if (eos) delete eos;
        }
        
        switch (n->type()) {
            case TYPE_EOS: delete n->as_eos(); break;
            case TYPE_SKIP: delete n->as_skip(); break;
            case TYPE_LIST: delete n->as_list(); break;
            case TYPE_FULL: delete n->as_full(); break;
        }
    }
    
    // Clean up in-place pre-allocations (not in nodes array)
    if (alloc.in_place_eos) {
        delete alloc.in_place_eos;
    }
    if (alloc.in_place_child) {
        builder_.dealloc_node(alloc.in_place_child);
    }
    
    alloc.clear();
}

std::pair<iterator, bool> insert_locked(const Key& key, std::string_view kb, const T& value) {
    if constexpr (!THREADED) {
        std::lock_guard<mutex_t> lock(mutex_);
        
        ptr_t root = root_.load();
        auto res = insert_impl(&root_, root, kb, value);
        
        if (!res.inserted) {
            for (int i = 0; i < res.old_count; ++i) retire_node(res.old_nodes[i]);
            return {find(key), false};
        }
        
        if (res.new_node) root_.store(res.new_node);
        for (int i = 0; i < res.old_count; ++i) retire_node(res.old_nodes[i]);
        ++size_;
        
        return {iterator(this, std::string(kb), value), true};
    } else {
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
            
            // All paths now go through allocate_speculative for pre-allocation
            pre_alloc alloc = allocate_speculative(spec, value);
            
            if (spec.op == spec_op::IN_PLACE_LEAF) {
                // IN_PLACE_LEAF: no allocation needed, just update value in existing leaf
                std::lock_guard<mutex_t> lock(mutex_);
                
                if (spec.target->header() != spec.target_header) {
                    dealloc_speculation(alloc);
                    continue;
                }
                
                ptr_t n = spec.target;
                unsigned char c = spec.c;
                
                if (n->is_list()) {
                    if (n->as_list()->chars.find(c) >= 0) {
                        dealloc_speculation(alloc);
                        continue;
                    }
                    if (n->as_list()->chars.count() >= LIST_MAX) {
                        dealloc_speculation(alloc);
                        continue;
                    }
                    int idx = n->as_list()->chars.add(c);
                    n->as_list()->leaf_values[idx] = value;
                    n->bump_version();
                    size_.fetch_add(1);
                    return {iterator(this, std::string(kb), value), true};
                }
                // FULL leaf
                if (n->as_full()->valid.test(c)) {
                    dealloc_speculation(alloc);
                    continue;
                }
                n->as_full()->valid.template atomic_set<THREADED>(c);
                n->as_full()->leaf_values[c] = value;
                n->bump_version();
                size_.fetch_add(1);
                return {iterator(this, std::string(kb), value), true};
            }
            
            if (spec.op == spec_op::IN_PLACE_INTERIOR) {
                // IN_PLACE_INTERIOR: use pre-allocated eos or child
                std::lock_guard<mutex_t> lock(mutex_);
                
                if (spec.target->header() != spec.target_header) {
                    dealloc_speculation(alloc);
                    continue;
                }
                
                ptr_t n = spec.target;
                
                if (spec.is_eos) {
                    if (get_eos_ptr(n)) {
                        dealloc_speculation(alloc);
                        continue;
                    }
                    // Use pre-allocated eos
                    set_eos_ptr(n, alloc.in_place_eos);
                    alloc.in_place_eos = nullptr;  // Transferred ownership
                    n->bump_version();
                    size_.fetch_add(1);
                    return {iterator(this, std::string(kb), value), true};
                }
                
                // Adding child
                unsigned char c = spec.c;
                
                if (n->is_list()) {
                    if (n->as_list()->chars.find(c) >= 0) {
                        dealloc_speculation(alloc);
                        continue;
                    }
                    if (n->as_list()->chars.count() >= LIST_MAX) {
                        dealloc_speculation(alloc);
                        continue;
                    }
                    int idx = n->as_list()->chars.add(c);
                    n->as_list()->children[idx].store(alloc.in_place_child);
                    alloc.in_place_child = nullptr;  // Transferred ownership
                    n->bump_version();
                    size_.fetch_add(1);
                    return {iterator(this, std::string(kb), value), true};
                }
                // FULL interior
                if (find_child(n, c)) {
                    dealloc_speculation(alloc);
                    continue;
                }
                n->as_full()->valid.template atomic_set<THREADED>(c);
                n->as_full()->children[c].store(alloc.in_place_child);
                alloc.in_place_child = nullptr;  // Transferred ownership
                n->bump_version();
                size_.fetch_add(1);
                return {iterator(this, std::string(kb), value), true};
            }
            
            {
                std::lock_guard<mutex_t> lock(mutex_);
                
                if (!validate_path(spec)) {
                } else {
                    if (alloc.root_replacement && commit_speculative(spec, alloc, value)) {
                        if (spec.target) {
                            retire_node(spec.target);
                        }
                        size_.fetch_add(1);
                        return {iterator(this, std::string(kb), value), true};
                    }
                    if (spec.op == spec_op::ADD_EOS_LEAF_LIST ||
                        spec.op == spec_op::DEMOTE_LEAF_LIST ||
                        spec.op == spec_op::SPLIT_INTERIOR ||
                        spec.op == spec_op::PREFIX_INTERIOR ||
                        spec.op == spec_op::ADD_CHILD_CONVERT) {
                        
                        dealloc_speculation(alloc);
                        
                        if (validate_path(spec)) {
                            ptr_t root = root_.load();
                            auto res = insert_impl(&root_, root, kb, value);
                            
                            if (!res.inserted) {
                                for (int i = 0; i < res.old_count; ++i) retire_node(res.old_nodes[i]);
                                return {find(key), false};
                            }
                            
                            if (res.new_node) root_.store(res.new_node);
                            for (int i = 0; i < res.old_count; ++i) retire_node(res.old_nodes[i]);
                            size_.fetch_add(1);
                            
                            return {iterator(this, std::string(kb), value), true};
                        }
                    }
                }
            }
            
            dealloc_speculation(alloc);
        }
    }
}
