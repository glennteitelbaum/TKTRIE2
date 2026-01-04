#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_node.h"
#include "tktrie_help_nav.h"

namespace gteitelbaum {

template <bool THREADED>
struct insert_result {
    trie_node<int, THREADED, std::allocator<uint64_t>>* placeholder;  // Just for type
    
    void* new_subtree = nullptr;
    void* target_slot = nullptr;
    uint64_t expected_ptr = 0;
    std::vector<void*> new_nodes;
    std::vector<void*> old_nodes;
    std::vector<void*> old_values;  // T* to free on commit
    bool already_exists = false;
    bool in_place = false;

    insert_result() {
        new_nodes.reserve(8);
        old_nodes.reserve(8);
        old_values.reserve(4);
    }
};

template <typename T, bool THREADED, typename Allocator>
struct insert_helpers {
    using node_t = trie_node<T, THREADED, Allocator>;
    using builder_t = node_builder<T, THREADED, Allocator>;
    using nav_t = nav_helpers<T, THREADED, Allocator>;
    using result_t = insert_result<THREADED>;

    template <typename U>
    static result_t build_insert_path(builder_t& builder, node_t** root_slot, node_t* root,
                                       std::string_view key, U&& value) {
        result_t result;

        if (!root) {
            // Empty trie - create new root
            result.target_slot = root_slot;
            result.expected_ptr = 0;

            T* val_ptr = builder.alloc_value(std::forward<U>(value));
            node_t* new_node;

            if (key.empty()) {
                new_node = builder.build_eos(val_ptr);
            } else {
                new_node = builder.build_skip(std::string(key), nullptr, val_ptr);
            }

            result.new_nodes.push_back(new_node);
            result.new_subtree = new_node;
            return result;
        }

        return insert_into_node(builder, root_slot, root, key, std::forward<U>(value), result);
    }

private:
    template <typename U>
    static result_t& insert_into_node(builder_t& builder, node_t** parent_slot, node_t* node,
                                       std::string_view key, U&& value, result_t& result) {
        result.target_slot = parent_slot;
        result.expected_ptr = reinterpret_cast<uint64_t>(node);

        // EOS-only node
        if (node->is_eos()) {
            if (key.empty()) {
                // Set/replace EOS value
                T* old = node->get_eos();
                if (old) {
                    result.already_exists = true;
                    return result;
                }
                // In-place update
                T* val_ptr = builder.alloc_value(std::forward<U>(value));
                node->set_eos(val_ptr);
                result.in_place = true;
                return result;
            }
            // Need to convert EOS -> SKIP or LIST
            return convert_eos_add_key(builder, node, key, std::forward<U>(value), result);
        }

        // Node with skip
        const std::string& skip = node->get_skip();

        if (!skip.empty()) {
            size_t match = nav_t::match_skip(skip, key);

            if (match < skip.size() && match < key.size()) {
                // Divergence in skip - split
                return split_skip_diverge(builder, node, key, std::forward<U>(value), match, result);
            }

            if (match < skip.size()) {
                // Key is prefix of skip - split
                return split_skip_prefix(builder, node, key, std::forward<U>(value), match, result);
            }

            // Full match of skip
            key.remove_prefix(match);

            if (key.empty()) {
                // Set skip_eos
                T* old = node->get_skip_eos();
                if (old) {
                    result.already_exists = true;
                    return result;
                }
                // In-place update
                T* val_ptr = builder.alloc_value(std::forward<U>(value));
                node->set_skip_eos(val_ptr);
                result.in_place = true;
                return result;
            }
        } else {
            // Empty skip - check EOS
            if (key.empty()) {
                T* old = node->get_eos();
                if (old) {
                    result.already_exists = true;
                    return result;
                }
                T* val_ptr = builder.alloc_value(std::forward<U>(value));
                node->set_eos(val_ptr);
                result.in_place = true;
                return result;
            }
        }

        // Need to follow or add child
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);

        if (node->is_skip()) {
            // SKIP node with no children - convert to LIST
            return convert_skip_to_list_with_child(builder, node, c, key, std::forward<U>(value), result);
        }

        // LIST or FULL node
        node_t* child = node->find_child(c);

        if (child) {
            // Recurse into existing child
            if (node->is_list()) {
                int idx = node->list.chars.find(c);
                return insert_into_node(builder, 
                    reinterpret_cast<node_t**>(&node->list.children[idx].ptr_),
                    child, key, std::forward<U>(value), result);
            } else {
                return insert_into_node(builder,
                    reinterpret_cast<node_t**>(&node->full.children[c].ptr_),
                    child, key, std::forward<U>(value), result);
            }
        }

        // Add new child
        return add_child_to_node(builder, node, c, key, std::forward<U>(value), result);
    }

    template <typename U>
    static result_t& convert_eos_add_key(builder_t& builder, node_t* node,
                                          std::string_view key, U&& value, result_t& result) {
        T* eos_val = node->get_eos();
        T* val_ptr = builder.alloc_value(std::forward<U>(value));

        // Create child for the new key
        node_t* child;
        if (key.size() == 1) {
            child = builder.build_eos(val_ptr);
        } else {
            child = builder.build_skip(std::string(key.substr(1)), nullptr, val_ptr);
        }
        result.new_nodes.push_back(child);

        // Create LIST node with the child
        node_t* new_node = builder.build_list("", eos_val, nullptr);
        new_node->list.chars.add(static_cast<unsigned char>(key[0]));
        new_node->list.children[0].store(child);
        result.new_nodes.push_back(new_node);

        result.new_subtree = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    template <typename U>
    static result_t& convert_skip_to_list_with_child(builder_t& builder, node_t* node,
                                                      unsigned char c, std::string_view rest,
                                                      U&& value, result_t& result) {
        T* eos_val = node->get_eos();
        T* skip_eos_val = node->get_skip_eos();
        const std::string& skip = node->get_skip();

        T* val_ptr = builder.alloc_value(std::forward<U>(value));

        // Create child
        node_t* child;
        if (rest.empty()) {
            child = builder.build_eos(val_ptr);
        } else {
            child = builder.build_skip(std::string(rest), nullptr, val_ptr);
        }
        result.new_nodes.push_back(child);

        // Create LIST node
        node_t* new_node = builder.build_list(skip, eos_val, skip_eos_val);
        new_node->list.chars.add(c);
        new_node->list.children[0].store(child);
        result.new_nodes.push_back(new_node);

        result.new_subtree = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    template <typename U>
    static result_t& split_skip_diverge(builder_t& builder, node_t* node, std::string_view key,
                                         U&& value, size_t match, result_t& result) {
        const std::string& skip = node->get_skip();
        std::string common = skip.substr(0, match);
        unsigned char old_char = static_cast<unsigned char>(skip[match]);
        unsigned char new_char = static_cast<unsigned char>(key[match]);

        T* eos_val = node->get_eos();
        T* val_ptr = builder.alloc_value(std::forward<U>(value));

        // Create suffix node for old skip remainder
        node_t* old_suffix = clone_with_shorter_skip(builder, node, match + 1);
        result.new_nodes.push_back(old_suffix);

        // Create suffix node for new key remainder
        std::string_view new_rest = key.substr(match + 1);
        node_t* new_suffix;
        if (new_rest.empty()) {
            new_suffix = builder.build_eos(val_ptr);
        } else {
            new_suffix = builder.build_skip(std::string(new_rest), nullptr, val_ptr);
        }
        result.new_nodes.push_back(new_suffix);

        // Create branch node with two children
        node_t* branch = builder.build_list(common, eos_val, nullptr);
        
        // Add both children to list
        if (old_char < new_char) {
            branch->list.chars.add(old_char);
            branch->list.chars.add(new_char);
            branch->list.children[0].store(old_suffix);
            branch->list.children[1].store(new_suffix);
        } else {
            branch->list.chars.add(new_char);
            branch->list.chars.add(old_char);
            branch->list.children[0].store(new_suffix);
            branch->list.children[1].store(old_suffix);
        }
        result.new_nodes.push_back(branch);

        result.new_subtree = branch;
        result.old_nodes.push_back(node);
        return result;
    }

    template <typename U>
    static result_t& split_skip_prefix(builder_t& builder, node_t* node, std::string_view key,
                                        U&& value, size_t match, result_t& result) {
        const std::string& skip = node->get_skip();
        std::string prefix = skip.substr(0, match);
        unsigned char c = static_cast<unsigned char>(skip[match]);

        T* eos_val = node->get_eos();
        T* val_ptr = builder.alloc_value(std::forward<U>(value));

        // Clone existing node with shortened skip
        node_t* suffix = clone_with_shorter_skip(builder, node, match + 1);
        result.new_nodes.push_back(suffix);

        // Create new branch node
        node_t* new_node = builder.build_list(prefix, eos_val, val_ptr);
        new_node->list.chars.add(c);
        new_node->list.children[0].store(suffix);
        result.new_nodes.push_back(new_node);

        result.new_subtree = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    template <typename U>
    static result_t& add_child_to_node(builder_t& builder, node_t* node,
                                        unsigned char c, std::string_view rest,
                                        U&& value, result_t& result) {
        T* val_ptr = builder.alloc_value(std::forward<U>(value));

        // Create new child
        node_t* child;
        if (rest.empty()) {
            child = builder.build_eos(val_ptr);
        } else {
            child = builder.build_skip(std::string(rest), nullptr, val_ptr);
        }
        result.new_nodes.push_back(child);

        if (node->is_list()) {
            int count = node->list.chars.count();
            if (count < LIST_MAX) {
                // In-place add to LIST
                int idx = node->list.chars.add(c);
                node->list.children[idx].store(child);
                result.in_place = true;
                return result;
            }
            // Convert LIST -> FULL (COW)
            return convert_list_to_full_with_child(builder, node, c, child, result);
        }

        // FULL node - in-place add
        node->full.valid.template atomic_set<THREADED>(c);
        node->full.children[c].store(child);
        result.in_place = true;
        return result;
    }

    static result_t& convert_list_to_full_with_child(builder_t& builder, node_t* node,
                                                      unsigned char c, node_t* new_child,
                                                      result_t& result) {
        T* eos_val = node->get_eos();
        T* skip_eos_val = node->get_skip_eos();
        const std::string& skip = node->get_skip();

        node_t* full = builder.build_full(skip, eos_val, skip_eos_val);

        // Copy existing children
        int count = node->list.chars.count();
        for (int i = 0; i < count; ++i) {
            unsigned char ch = node->list.chars.char_at(i);
            full->full.valid.set(ch);
            full->full.children[ch].store(node->list.children[i].load());
        }

        // Add new child
        full->full.valid.set(c);
        full->full.children[c].store(new_child);

        result.new_nodes.push_back(full);
        result.new_subtree = full;
        result.old_nodes.push_back(node);
        return result;
    }

    static node_t* clone_with_shorter_skip(builder_t& builder, node_t* node, size_t skip_prefix_len) {
        const std::string& skip = node->get_skip();
        std::string new_skip = skip.substr(skip_prefix_len);
        T* skip_eos_val = node->get_skip_eos();

        switch (node->node_type()) {
            case NODE_SKIP:
                if (new_skip.empty()) {
                    return builder.build_eos(skip_eos_val);
                }
                return builder.build_skip(new_skip, nullptr, skip_eos_val);

            case NODE_LIST: {
                node_t* n;
                if (new_skip.empty() && node->list.chars.count() == 0) {
                    n = builder.build_eos(skip_eos_val);
                } else {
                    n = builder.build_list(new_skip, nullptr, skip_eos_val);
                    n->list.chars = node->list.chars;
                    for (int i = 0; i < node->list.chars.count(); ++i) {
                        n->list.children[i].store(node->list.children[i].load());
                    }
                }
                return n;
            }

            case NODE_FULL: {
                node_t* n = builder.build_full(new_skip, nullptr, skip_eos_val);
                n->full.valid = node->full.valid;
                for (int i = 0; i < 256; ++i) {
                    if (node->full.valid.test(static_cast<unsigned char>(i))) {
                        n->full.children[i].store(node->full.children[i].load());
                    }
                }
                return n;
            }

            default:
                return nullptr;
        }
    }
};

}  // namespace gteitelbaum
