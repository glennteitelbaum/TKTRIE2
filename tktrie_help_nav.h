#pragma once

#include <string>
#include <string_view>

#include "tktrie_defines.h"
#include "tktrie_node.h"

namespace gteitelbaum {

template <typename T, bool THREADED, typename Allocator>
struct nav_helpers {
    using node_t = trie_node<T, THREADED, Allocator>;

    // Match skip prefix with key, returns number of matching characters
    static size_t match_skip(const std::string& skip, std::string_view key) noexcept {
        size_t i = 0;
        size_t n = std::min(skip.size(), key.size());
        while (i < n && skip[i] == key[i]) ++i;
        return i;
    }

    // Check if key exists in trie
    static bool contains(node_t* root, std::string_view key) noexcept {
        T val;
        return read(root, key, val);
    }

    // Read value for key, returns true if found
    static bool read(node_t* root, std::string_view key, T& out) noexcept {
        node_t* cur = root;

        while (cur) {
            // Handle EOS-only node specially
            if (cur->is_eos()) {
                if (key.empty()) {
                    T* eos = cur->get_eos();
                    if (eos) { out = *eos; return true; }
                }
                return false;
            }

            // All other node types have skip
            const std::string& skip = cur->get_skip();
            
            if (!skip.empty()) {
                size_t match = match_skip(skip, key);
                if (match < skip.size()) return false;  // Key diverges from skip
                key.remove_prefix(match);

                if (key.empty()) {
                    // Key ends at skip_eos
                    T* skip_eos = cur->get_skip_eos();
                    if (skip_eos) { out = *skip_eos; return true; }
                    return false;
                }
            } else {
                // Empty skip - check EOS
                if (key.empty()) {
                    T* eos = cur->get_eos();
                    if (eos) { out = *eos; return true; }
                    return false;
                }
            }

            // Need to follow child
            unsigned char c = static_cast<unsigned char>(key[0]);
            cur = cur->find_child(c);
            key.remove_prefix(1);
        }

        return false;
    }

    // Find first leaf (for begin iterator)
    // Returns: node containing the value, key_out is populated with full key
    // value_is_skip_eos: true if value is at skip_eos, false if at eos
    static node_t* find_first_leaf(node_t* node, std::string& key_out, bool& value_is_skip_eos) noexcept {
        value_is_skip_eos = false;
        if (!node) return nullptr;

        while (true) {
            // EOS-only node
            if (node->is_eos()) {
                if (node->get_eos()) {
                    value_is_skip_eos = false;
                    return node;
                }
                return nullptr;
            }

            // Check EOS first (before skip)
            if (node->get_eos()) {
                value_is_skip_eos = false;
                return node;
            }

            // Consume skip
            const std::string& skip = node->get_skip();
            key_out.append(skip);

            // Check skip_eos
            if (node->get_skip_eos()) {
                value_is_skip_eos = true;
                return node;
            }

            // Must have children if we got here
            if (!has_children(node->header())) return nullptr;

            // Find first child
            unsigned char c = node->first_child_char();
            if (c == 255) return nullptr;

            key_out.push_back(static_cast<char>(c));
            node = node->find_child(c);
            if (!node) return nullptr;
        }
    }

    // Find next leaf after given key (for iterator increment)
    static node_t* find_next_leaf(node_t* root, const std::string& current_key, 
                                   std::string& next_key_out, bool& value_is_skip_eos) noexcept {
        // This is complex - need to backtrack up the trie
        // For now, simplified implementation: traverse from root
        // TODO: Optimize with parent pointers or stack-based traversal
        
        value_is_skip_eos = false;
        if (!root) return nullptr;

        // Find path to current key, then find next
        struct frame {
            node_t* node;
            std::string prefix;
            int state;  // 0=check eos, 1=check skip_eos, 2=iterating children
            unsigned char last_child;
        };

        std::vector<frame> stack;
        stack.push_back({root, "", 0, 0});

        bool found_current = false;
        std::string built_key;

        while (!stack.empty()) {
            frame& f = stack.back();
            node_t* node = f.node;

            if (!node) {
                stack.pop_back();
                continue;
            }

            if (f.state == 0) {
                // Check EOS
                f.state = 1;
                if (node->is_eos()) {
                    if (f.prefix == current_key) {
                        found_current = true;
                    } else if (found_current && node->get_eos()) {
                        next_key_out = f.prefix;
                        value_is_skip_eos = false;
                        return node;
                    }
                    stack.pop_back();
                    continue;
                }
                
                if (node->get_eos()) {
                    if (f.prefix == current_key) {
                        found_current = true;
                    } else if (found_current) {
                        next_key_out = f.prefix;
                        value_is_skip_eos = false;
                        return node;
                    }
                }
            }

            if (f.state == 1) {
                // Check skip_eos
                f.state = 2;
                const std::string& skip = node->get_skip();
                std::string skip_prefix = f.prefix + skip;

                if (node->get_skip_eos()) {
                    if (skip_prefix == current_key) {
                        found_current = true;
                    } else if (found_current) {
                        next_key_out = skip_prefix;
                        value_is_skip_eos = true;
                        return node;
                    }
                }
                f.prefix = skip_prefix;
                f.last_child = 0;
            }

            if (f.state == 2) {
                // Iterate children
                if (!has_children(node->header())) {
                    stack.pop_back();
                    continue;
                }

                unsigned char c;
                if (f.last_child == 0) {
                    c = node->first_child_char();
                } else {
                    c = node->next_child_char(f.last_child);
                }

                if (c == 255) {
                    stack.pop_back();
                    continue;
                }

                f.last_child = c;
                node_t* child = node->find_child(c);
                if (child) {
                    stack.push_back({child, f.prefix + static_cast<char>(c), 0, 0});
                }
            }
        }

        return nullptr;
    }
};

}  // namespace gteitelbaum
