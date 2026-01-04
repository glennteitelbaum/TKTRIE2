// tktrie_insert.inc - Insert operations for tktrie
// This file is included inside the tktrie class body

// =========================================================================
// INSERT RESULT
// =========================================================================
struct insert_result {
    ptr_t new_node = nullptr;
    std::vector<ptr_t> old_nodes;
    bool inserted = false;
    bool in_place = false;
};

// =========================================================================
// INSERT IMPLEMENTATION
// =========================================================================
insert_result insert_impl(atomic_ptr* slot, ptr_t n, std::string_view key, const T& value) {
    insert_result res;
    
    if (!n) {
        res.new_node = create_leaf_for_key(key, value);
        res.inserted = true;
        return res;
    }
    
    if (n->is_leaf()) return insert_into_leaf(slot, n, key, value);
    return insert_into_interior(slot, n, key, value);
}

insert_result insert_into_leaf(atomic_ptr*, ptr_t leaf, std::string_view key, const T& value) {
    insert_result res;
    std::string_view leaf_skip = get_skip(leaf);
    
    if (leaf->is_eos()) {
        if (key.empty()) return res;  // Already exists
        return demote_leaf_eos(leaf, key, value);
    }
    
    if (leaf->is_skip()) {
        size_t m = match_skip(leaf_skip, key);
        if (m == leaf_skip.size() && m == key.size()) return res;  // Exists
        if (m < leaf_skip.size() && m < key.size()) return split_leaf_skip(leaf, key, value, m);
        if (m == key.size()) return prefix_leaf_skip(leaf, key, value, m);
        return extend_leaf_skip(leaf, key, value, m);
    }
    
    // LIST or FULL
    size_t m = match_skip(leaf_skip, key);
    if (m < leaf_skip.size() && m < key.size()) return split_leaf_list(leaf, key, value, m);
    if (m < leaf_skip.size()) return prefix_leaf_list(leaf, key, value, m);
    key.remove_prefix(m);
    
    if (key.empty()) return add_eos_to_leaf_list(leaf, value);
    if (key.size() == 1) {
        unsigned char c = static_cast<unsigned char>(key[0]);
        return add_char_to_leaf(leaf, c, value);
    }
    return demote_leaf_list(leaf, key, value);
}

insert_result insert_into_interior(atomic_ptr*, ptr_t n, std::string_view key, const T& value) {
    insert_result res;
    std::string_view skip = get_skip(n);
    
    size_t m = match_skip(skip, key);
    if (m < skip.size() && m < key.size()) return split_interior(n, key, value, m);
    if (m < skip.size()) return prefix_interior(n, key, value, m);
    key.remove_prefix(m);
    
    if (key.empty()) return set_interior_eos(n, value);
    
    unsigned char c = static_cast<unsigned char>(key[0]);
    key.remove_prefix(1);
    
    ptr_t child = find_child(n, c);
    if (child) {
        atomic_ptr* child_slot = get_child_slot(n, c);
        auto child_res = insert_impl(child_slot, child, key, value);
        if (child_res.new_node && child_res.new_node != child) {
            child_slot->store(child_res.new_node);
        }
        res.inserted = child_res.inserted;
        res.in_place = child_res.in_place;
        res.old_nodes = std::move(child_res.old_nodes);
        return res;
    }
    
    return add_child_to_interior(n, c, key, value);
}

atomic_ptr* get_child_slot(ptr_t n, unsigned char c) noexcept {
    if (n->is_list()) {
        int idx = n->as_list()->chars.find(c);
        return idx >= 0 ? &n->as_list()->children[idx] : nullptr;
    }
    if (n->is_full() && n->as_full()->valid.test(c)) {
        return &n->as_full()->children[c];
    }
    return nullptr;
}

ptr_t create_leaf_for_key(std::string_view key, const T& value) {
    if (key.empty()) return builder_.make_leaf_eos(value);
    if (key.size() == 1) {
        ptr_t leaf = builder_.make_leaf_list("");
        unsigned char c = static_cast<unsigned char>(key[0]);
        leaf->as_list()->chars.add(c);
        leaf->as_list()->leaf_values[0] = value;
        return leaf;
    }
    ptr_t leaf = builder_.make_leaf_list(key.substr(0, key.size() - 1));
    unsigned char c = static_cast<unsigned char>(key.back());
    leaf->as_list()->chars.add(c);
    leaf->as_list()->leaf_values[0] = value;
    return leaf;
}

// Demote leaf_eos: becomes interior with child
insert_result demote_leaf_eos(ptr_t leaf, std::string_view key, const T& value) {
    insert_result res;
    ptr_t interior = builder_.make_interior_list("");
    interior->as_list()->eos_ptr = new T(leaf->as_eos()->leaf_value);
    
    unsigned char c = static_cast<unsigned char>(key[0]);
    ptr_t child = create_leaf_for_key(key.substr(1), value);
    interior->as_list()->chars.add(c);
    interior->as_list()->children[0].store(child);
    
    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

// Split leaf_skip at divergence
insert_result split_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->as_skip()->skip;
    
    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);
    
    ptr_t interior = builder_.make_interior_list(common);
    ptr_t old_child = builder_.make_leaf_skip(old_skip.substr(m + 1), leaf->as_skip()->leaf_value);
    ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
    
    interior->as_list()->chars.add(old_c);
    interior->as_list()->chars.add(new_c);
    interior->as_list()->children[0].store(old_child);
    interior->as_list()->children[1].store(new_child);
    
    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

// Key is prefix of leaf_skip
insert_result prefix_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->as_skip()->skip;
    
    ptr_t interior = builder_.make_interior_list(key);
    interior->as_list()->eos_ptr = new T(value);
    
    unsigned char c = static_cast<unsigned char>(old_skip[m]);
    ptr_t child = builder_.make_leaf_skip(old_skip.substr(m + 1), leaf->as_skip()->leaf_value);
    interior->as_list()->chars.add(c);
    interior->as_list()->children[0].store(child);
    
    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

// leaf_skip is prefix of key - extend
insert_result extend_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->as_skip()->skip;
    
    ptr_t interior = builder_.make_interior_list(old_skip);
    interior->as_list()->eos_ptr = new T(leaf->as_skip()->leaf_value);
    
    unsigned char c = static_cast<unsigned char>(key[m]);
    ptr_t child = create_leaf_for_key(key.substr(m + 1), value);
    interior->as_list()->chars.add(c);
    interior->as_list()->children[0].store(child);
    
    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

// Split leaf_list at divergence in skip
insert_result split_leaf_list(ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = get_skip(leaf);
    
    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);
    
    ptr_t interior = builder_.make_interior_list(common);
    
    // Clone old leaf with shorter skip
    ptr_t old_child = clone_leaf_with_skip(leaf, old_skip.substr(m + 1));
    ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
    
    interior->as_list()->chars.add(old_c);
    interior->as_list()->chars.add(new_c);
    interior->as_list()->children[0].store(old_child);
    interior->as_list()->children[1].store(new_child);
    
    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

// Key ends in skip
insert_result prefix_leaf_list(ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = get_skip(leaf);
    
    ptr_t interior = builder_.make_interior_list(key);
    interior->as_list()->eos_ptr = new T(value);
    
    unsigned char c = static_cast<unsigned char>(old_skip[m]);
    ptr_t old_child = clone_leaf_with_skip(leaf, old_skip.substr(m + 1));
    interior->as_list()->chars.add(c);
    interior->as_list()->children[0].store(old_child);
    
    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

ptr_t clone_leaf_with_skip(ptr_t leaf, std::string_view new_skip) {
    if (leaf->is_list()) {
        ptr_t n = builder_.make_leaf_list(new_skip);
        n->as_list()->chars = leaf->as_list()->chars;
        for (int i = 0; i < leaf->as_list()->chars.count(); ++i) {
            n->as_list()->leaf_values[i] = leaf->as_list()->leaf_values[i];
        }
        return n;
    }
    // FULL
    ptr_t n = builder_.make_leaf_full(new_skip);
    n->as_full()->valid = leaf->as_full()->valid;
    for (int c = 0; c < 256; ++c) {
        if (leaf->as_full()->valid.test(static_cast<unsigned char>(c))) {
            n->as_full()->leaf_values[c] = leaf->as_full()->leaf_values[c];
        }
    }
    return n;
}

// Add eos to leaf_list - demote to interior
insert_result add_eos_to_leaf_list(ptr_t leaf, const T& value) {
    insert_result res;
    std::string_view leaf_skip = get_skip(leaf);
    
    ptr_t interior = builder_.make_interior_list(leaf_skip);
    interior->as_list()->eos_ptr = new T(value);
    
    // Each char in leaf becomes a leaf_eos child
    if (leaf->is_list()) {
        for (int i = 0; i < leaf->as_list()->chars.count(); ++i) {
            unsigned char c = leaf->as_list()->chars.char_at(i);
            ptr_t child = builder_.make_leaf_eos(leaf->as_list()->leaf_values[i]);
            interior->as_list()->chars.add(c);
            interior->as_list()->children[i].store(child);
        }
    } else {
        for (int c = 0; c < 256; ++c) {
            if (leaf->as_full()->valid.test(static_cast<unsigned char>(c))) {
                ptr_t child = builder_.make_leaf_eos(leaf->as_full()->leaf_values[c]);
                int idx = interior->as_list()->chars.add(static_cast<unsigned char>(c));
                interior->as_list()->children[idx].store(child);
            }
        }
    }
    
    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

// Add char to leaf
insert_result add_char_to_leaf(ptr_t leaf, unsigned char c, const T& value) {
    insert_result res;
    
    if (leaf->is_list()) {
        int idx = leaf->as_list()->chars.find(c);
        if (idx >= 0) return res;  // Exists
        
        if (leaf->as_list()->chars.count() < LIST_MAX) {
            idx = leaf->as_list()->chars.add(c);
            leaf->as_list()->leaf_values[idx] = value;
            res.in_place = true;
            res.inserted = true;
            return res;
        }
        
        // Convert to FULL
        ptr_t full = builder_.make_leaf_full(leaf->as_list()->skip);
        for (int i = 0; i < leaf->as_list()->chars.count(); ++i) {
            unsigned char ch = leaf->as_list()->chars.char_at(i);
            full->as_full()->valid.set(ch);
            full->as_full()->leaf_values[ch] = leaf->as_list()->leaf_values[i];
        }
        full->as_full()->valid.set(c);
        full->as_full()->leaf_values[c] = value;
        
        res.new_node = full;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }
    
    // FULL
    if (leaf->as_full()->valid.test(c)) return res;  // Exists
    leaf->as_full()->valid.template atomic_set<THREADED>(c);
    leaf->as_full()->leaf_values[c] = value;
    res.in_place = true;
    res.inserted = true;
    return res;
}

// Demote leaf_list - key is longer than list handles
insert_result demote_leaf_list(ptr_t leaf, std::string_view key, const T& value) {
    insert_result res;
    std::string_view leaf_skip = get_skip(leaf);
    unsigned char first_c = static_cast<unsigned char>(key[0]);
    
    ptr_t interior = builder_.make_interior_list(leaf_skip);
    
    // Convert existing chars to leaf_eos children
    if (leaf->is_list()) {
        for (int i = 0; i < leaf->as_list()->chars.count(); ++i) {
            unsigned char c = leaf->as_list()->chars.char_at(i);
            ptr_t child = builder_.make_leaf_eos(leaf->as_list()->leaf_values[i]);
            interior->as_list()->chars.add(c);
            interior->as_list()->children[i].store(child);
        }
    } else {
        for (int c = 0; c < 256; ++c) {
            if (leaf->as_full()->valid.test(static_cast<unsigned char>(c))) {
                ptr_t child = builder_.make_leaf_eos(leaf->as_full()->leaf_values[c]);
                int idx = interior->as_list()->chars.add(static_cast<unsigned char>(c));
                interior->as_list()->children[idx].store(child);
            }
        }
    }
    
    // Add new key
    int existing_idx = interior->as_list()->chars.find(first_c);
    if (existing_idx >= 0) {
        ptr_t child = interior->as_list()->children[existing_idx].load();
        auto child_res = insert_impl(&interior->as_list()->children[existing_idx], child, key.substr(1), value);
        if (child_res.new_node) {
            interior->as_list()->children[existing_idx].store(child_res.new_node);
        }
        for (auto* old : child_res.old_nodes) res.old_nodes.push_back(old);
    } else {
        ptr_t child = create_leaf_for_key(key.substr(1), value);
        int idx = interior->as_list()->chars.add(first_c);
        interior->as_list()->children[idx].store(child);
    }
    
    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

// Split interior at divergence
insert_result split_interior(ptr_t n, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = get_skip(n);
    
    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);
    
    ptr_t new_int = builder_.make_interior_list(common);
    ptr_t old_child = clone_interior_with_skip(n, old_skip.substr(m + 1));
    ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
    
    new_int->as_list()->chars.add(old_c);
    new_int->as_list()->chars.add(new_c);
    new_int->as_list()->children[0].store(old_child);
    new_int->as_list()->children[1].store(new_child);
    
    res.new_node = new_int;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}

ptr_t clone_interior_with_skip(ptr_t n, std::string_view new_skip) {
    if (n->is_list()) {
        ptr_t clone = builder_.make_interior_list(new_skip);
        clone->as_list()->chars = n->as_list()->chars;
        clone->as_list()->eos_ptr = n->as_list()->eos_ptr;
        n->as_list()->eos_ptr = nullptr;
        for (int i = 0; i < n->as_list()->chars.count(); ++i) {
            clone->as_list()->children[i].store(n->as_list()->children[i].load());
            n->as_list()->children[i].store(nullptr);
        }
        return clone;
    }
    if (n->is_full()) {
        ptr_t clone = builder_.make_interior_full(new_skip);
        clone->as_full()->valid = n->as_full()->valid;
        clone->as_full()->eos_ptr = n->as_full()->eos_ptr;
        n->as_full()->eos_ptr = nullptr;
        for (int c = 0; c < 256; ++c) {
            if (n->as_full()->valid.test(static_cast<unsigned char>(c))) {
                clone->as_full()->children[c].store(n->as_full()->children[c].load());
                n->as_full()->children[c].store(nullptr);
            }
        }
        return clone;
    }
    // EOS or SKIP
    ptr_t clone = builder_.make_interior_skip(new_skip);
    clone->as_skip()->eos_ptr = get_eos_ptr(n);
    set_eos_ptr(n, nullptr);
    return clone;
}

// Key is prefix of interior skip
insert_result prefix_interior(ptr_t n, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = get_skip(n);
    
    ptr_t new_int = builder_.make_interior_list(key);
    new_int->as_list()->eos_ptr = new T(value);
    
    unsigned char c = static_cast<unsigned char>(old_skip[m]);
    ptr_t old_child = clone_interior_with_skip(n, old_skip.substr(m + 1));
    new_int->as_list()->chars.add(c);
    new_int->as_list()->children[0].store(old_child);
    
    res.new_node = new_int;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}

// Set eos on interior
insert_result set_interior_eos(ptr_t n, const T& value) {
    insert_result res;
    T* p = get_eos_ptr(n);
    if (p) return res;  // Exists
    
    set_eos_ptr(n, new T(value));
    res.in_place = true;
    res.inserted = true;
    return res;
}

// Add child to interior
insert_result add_child_to_interior(ptr_t n, unsigned char c, std::string_view remaining, const T& value) {
    insert_result res;
    ptr_t child = create_leaf_for_key(remaining, value);
    
    if (n->is_list()) {
        if (n->as_list()->chars.count() < LIST_MAX) {
            int idx = n->as_list()->chars.add(c);
            n->as_list()->children[idx].store(child);
            res.in_place = true;
            res.inserted = true;
            return res;
        }
        // Convert to FULL
        ptr_t full = builder_.make_interior_full(n->as_list()->skip);
        full->as_full()->eos_ptr = n->as_list()->eos_ptr;
        n->as_list()->eos_ptr = nullptr;
        for (int i = 0; i < n->as_list()->chars.count(); ++i) {
            unsigned char ch = n->as_list()->chars.char_at(i);
            full->as_full()->valid.set(ch);
            full->as_full()->children[ch].store(n->as_list()->children[i].load());
            n->as_list()->children[i].store(nullptr);
        }
        full->as_full()->valid.set(c);
        full->as_full()->children[c].store(child);
        
        res.new_node = full;
        res.old_nodes.push_back(n);
        res.inserted = true;
        return res;
    }
    
    if (n->is_full()) {
        n->as_full()->valid.template atomic_set<THREADED>(c);
        n->as_full()->children[c].store(child);
        res.in_place = true;
        res.inserted = true;
        return res;
    }
    
    // EOS or SKIP - convert to LIST
    ptr_t list = builder_.make_interior_list(get_skip(n));
    list->as_list()->eos_ptr = get_eos_ptr(n);
    set_eos_ptr(n, nullptr);
    list->as_list()->chars.add(c);
    list->as_list()->children[0].store(child);
    
    res.new_node = list;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}
