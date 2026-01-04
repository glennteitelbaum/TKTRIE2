#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_node.h"
#include "tktrie_help_nav.h"

namespace gteitelbaum {

template <bool THREADED>
struct remove_result {
    void* new_subtree = nullptr;
    void* target_slot = nullptr;
    uint64_t expected_ptr = 0;
    std::vector<void*> new_nodes;
    std::vector<void*> old_nodes;
    std::vector<void*> old_values;  // T* to free on commit
    bool found = false;
    bool subtree_deleted = false;
    bool in_place = false;

    remove_result() {
        new_nodes.reserve(8);
        old_nodes.reserve(8);
        old_values.reserve(4);
    }
};

template <typename T, bool THREADED, typename Allocator>
struct remove_helpers {
    using node_t = trie_node<T, THREADED, Allocator>;
    using builder_t = node_builder<T, THREADED, Allocator>;
    using nav_t = nav_helpers<T, THREADED, Allocator>;
    using result_t = remove_result<THREADED>;

    static result_t build_remove_path(builder_t& builder, node_t** root_slot, node_t* root,
                                       std::string_view key) {
        result_t result;
        if (!root) return result;

        result.target_slot = root_slot;
        result.expected_ptr = reinterpret_cast<uint64_t>(root);

        return remove_from_node(builder, root_slot, root, key, result);
    }

private:
    static result_t& remove_from_node(builder_t& builder, node_t** parent_slot, node_t* node,
                                       std::string_view key, result_t& result) {
        result.target_slot = parent_slot;
        result.expected_ptr = reinterpret_cast<uint64_t>(node);

        // EOS-only node
        if (node->is_eos()) {
            if (key.empty()) {
                T* eos = node->get_eos();
                if (!eos) return result;  // Not found
                
                result.found = true;
                result.subtree_deleted = true;
                result.old_values.push_back(eos);
                result.old_nodes.push_back(node);
                return result;
            }
            return result;  // Key not found
        }

        // Node with skip
        const std::string& skip = node->get_skip();

        if (!skip.empty()) {
            size_t match = nav_t::match_skip(skip, key);
            if (match < skip.size()) return result;  // Key diverges - not found
            key.remove_prefix(match);

            if (key.empty()) {
                // Remove skip_eos
                T* skip_eos = node->get_skip_eos();
                if (!skip_eos) return result;

                result.found = true;
                result.old_values.push_back(skip_eos);

                // Check if node can be collapsed
                return remove_skip_eos(builder, node, result);
            }
        } else {
            if (key.empty()) {
                // Remove EOS
                T* eos = node->get_eos();
                if (!eos) return result;

                result.found = true;
                result.old_values.push_back(eos);

                return remove_eos(builder, node, result);
            }
        }

        // Need to follow child
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);

        node_t* child = node->find_child(c);
        if (!child) return result;

        // Recurse
        result_t child_result;
        if (node->is_list()) {
            int idx = node->list.chars.find(c);
            remove_from_node(builder,
                reinterpret_cast<node_t**>(&node->list.children[idx].ptr_),
                child, key, child_result);
        } else {
            remove_from_node(builder,
                reinterpret_cast<node_t**>(&node->full.children[c].ptr_),
                child, key, child_result);
        }

        if (!child_result.found) {
            result.found = false;
            return result;
        }

        // Merge results
        result.found = true;
        for (void* n : child_result.new_nodes) result.new_nodes.push_back(n);
        for (void* n : child_result.old_nodes) result.old_nodes.push_back(n);
        for (void* v : child_result.old_values) result.old_values.push_back(v);

        if (child_result.subtree_deleted) {
            return remove_child(builder, node, c, result);
        }

        if (child_result.in_place) {
            result.in_place = true;
            return result;
        }

        // Child was rebuilt - need to update our pointer
        return rebuild_with_new_child(builder, node, c, 
            static_cast<node_t*>(child_result.new_subtree), result);
    }

    static result_t& remove_eos(builder_t& builder, node_t* node, result_t& result) {
        // In-place clear
        node->set_eos(nullptr);

        // Check if node should be collapsed
        if (can_delete_node(node)) {
            result.subtree_deleted = true;
            result.old_nodes.push_back(node);
        } else {
            result.in_place = true;
            try_collapse(builder, node, result);
        }
        return result;
    }

    static result_t& remove_skip_eos(builder_t& builder, node_t* node, result_t& result) {
        // In-place clear
        node->set_skip_eos(nullptr);

        // Check if node should be collapsed
        if (can_delete_node(node)) {
            result.subtree_deleted = true;
            result.old_nodes.push_back(node);
        } else {
            result.in_place = true;
            try_collapse(builder, node, result);
        }
        return result;
    }

    static result_t& remove_child(builder_t& builder, node_t* node, unsigned char c, result_t& result) {
        if (node->is_list()) {
            int idx = node->list.chars.find(c);
            if (idx >= 0) {
                node->list.children[idx].store(nullptr);
                node->list.chars.remove_at(idx);
                
                // Shift remaining children
                int count = node->list.chars.count();
                for (int i = idx; i < count; ++i) {
                    node->list.children[i].store(node->list.children[i + 1].load());
                }
                node->list.children[count].store(nullptr);
            }

            if (can_delete_node(node)) {
                result.subtree_deleted = true;
                result.old_nodes.push_back(node);
            } else {
                result.in_place = true;
                try_collapse(builder, node, result);
            }
        } else if (node->is_full()) {
            node->full.valid.template atomic_clear<THREADED>(c);
            node->full.children[c].store(nullptr);

            int count = node->full.valid.count();
            
            if (can_delete_node(node)) {
                result.subtree_deleted = true;
                result.old_nodes.push_back(node);
            } else if (count <= LIST_MAX) {
                // Convert FULL -> LIST (COW)
                return convert_full_to_list(builder, node, result);
            } else {
                result.in_place = true;
            }
        }
        return result;
    }

    static result_t& rebuild_with_new_child(builder_t& builder, node_t* node, unsigned char c,
                                             node_t* new_child, result_t& result) {
        // Create new node with updated child
        T* eos_val = node->get_eos();
        T* skip_eos_val = node->get_skip_eos();
        const std::string& skip = node->get_skip();

        node_t* new_node;

        if (node->is_list()) {
            new_node = builder.build_list(skip, eos_val, skip_eos_val);
            new_node->list.chars = node->list.chars;
            int idx = node->list.chars.find(c);
            for (int i = 0; i < node->list.chars.count(); ++i) {
                if (i == idx) {
                    new_node->list.children[i].store(new_child);
                } else {
                    new_node->list.children[i].store(node->list.children[i].load());
                }
            }
        } else {
            new_node = builder.build_full(skip, eos_val, skip_eos_val);
            new_node->full.valid = node->full.valid;
            for (int i = 0; i < 256; ++i) {
                if (node->full.valid.test(static_cast<unsigned char>(i))) {
                    if (static_cast<unsigned char>(i) == c) {
                        new_node->full.children[i].store(new_child);
                    } else {
                        new_node->full.children[i].store(node->full.children[i].load());
                    }
                }
            }
        }

        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    static result_t& convert_full_to_list(builder_t& builder, node_t* node, result_t& result) {
        T* eos_val = node->get_eos();
        T* skip_eos_val = node->get_skip_eos();
        const std::string& skip = node->get_skip();

        node_t* list = builder.build_list(skip, eos_val, skip_eos_val);

        // Copy valid children
        for (int i = 0; i < 256; ++i) {
            if (node->full.valid.test(static_cast<unsigned char>(i))) {
                int idx = list->list.chars.add(static_cast<unsigned char>(i));
                list->list.children[idx].store(node->full.children[i].load());
            }
        }

        result.new_nodes.push_back(list);
        result.new_subtree = list;
        result.old_nodes.push_back(node);
        return result;
    }

    static bool can_delete_node(node_t* node) {
        // Node can be deleted if it has no EOS, no skip_eos, and no children
        if (node->get_eos()) return false;

        if (node->is_eos()) return true;

        if (node->get_skip_eos()) return false;

        return node->child_count() == 0;
    }

    static void try_collapse(builder_t& builder, node_t* node, result_t& result) {
        // Try to collapse single-child node into skip
        if (node->is_eos()) return;
        if (node->get_eos()) return;
        if (node->get_skip_eos()) return;
        if (node->child_count() != 1) return;

        // Get the single child
        unsigned char c = node->first_child_char();
        if (c == 255) return;

        node_t* child = node->find_child(c);
        if (!child) return;

        // Can only collapse if child is EOS or SKIP (no children)
        if (has_children(child->header())) return;

        // Build new skip by concatenating: node.skip + c + child.skip
        std::string new_skip = node->get_skip();
        new_skip.push_back(static_cast<char>(c));
        if (!child->is_eos()) {
            new_skip.append(child->get_skip());
        }

        T* child_val = child->is_eos() ? child->get_eos() : child->get_skip_eos();

        node_t* collapsed;
        if (new_skip.empty()) {
            collapsed = builder.build_eos(child_val);
        } else {
            collapsed = builder.build_skip(new_skip, nullptr, child_val);
        }

        // The in_place flag was set, need to change to COW
        result.in_place = false;
        result.new_nodes.push_back(collapsed);
        result.new_subtree = collapsed;
        result.old_nodes.push_back(node);
        result.old_nodes.push_back(child);
    }
};

}  // namespace gteitelbaum
