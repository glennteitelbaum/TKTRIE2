#pragma once

#include <string>
#include <string_view>

#include "tktrie_defines.h"
#include "tktrie_node.h"

namespace gteitelbaum {

template <typename T, bool THREADED, typename Allocator>
struct nav_helpers {
    using ptr_t = node_ptr<T, THREADED, Allocator>;

    // Match skip prefix with key, returns number of matching characters
    static size_t match_skip(const std::string& skip, std::string_view key) noexcept {
        size_t i = 0;
        size_t n = std::min(skip.size(), key.size());
        while (i < n && skip[i] == key[i]) ++i;
        return i;
    }

    // Check if key exists in trie
    static bool contains(ptr_t root, std::string_view key) noexcept {
        T val;
        return read(root, key, val);
    }

    // Read value for key, returns true if found
    static bool read(ptr_t cur, std::string_view key, T& out) noexcept {
        while (cur) {
            // 1. Check EOS (all nodes have it)
            if (key.empty()) {
                T* eos = cur.get_eos();
                if (eos) { out = *eos; return true; }
                return false;
            }

            // 2. If EOS-only node, no skip or children
            if (cur.is_eos()) {
                return false;
            }

            // 3. Check/consume SKIP (SKIP, LIST, FULL all have it)
            const std::string& skip = cur.get_skip();
            if (!skip.empty()) {
                size_t match = match_skip(skip, key);
                if (match < skip.size()) return false;  // Key diverges
                key.remove_prefix(match);

                if (key.empty()) {
                    T* skip_eos = cur.get_skip_eos();
                    if (skip_eos) { out = *skip_eos; return true; }
                    return false;
                }
            }

            // 4. If no children (SKIP-only), done
            if (!cur.is_list() && !cur.is_full()) {
                return false;
            }

            // 5. Follow child
            unsigned char c = static_cast<unsigned char>(key[0]);
            key.remove_prefix(1);

            if (cur.is_list()) {
                int idx = cur.list->chars.find(c);
                if (idx < 0) return false;
                cur = cur.list->children[idx].load();
            } else {
                // FULL
                if (!cur.full->valid.test(c)) return false;
                cur = cur.full->children[c].load();
            }
        }

        return false;
    }

    // Find first leaf (for begin iterator)
    // Returns: node containing the value, key_out is populated with full key
    // value_is_skip_eos: true if value is at skip_eos, false if at eos
    static ptr_t find_first_leaf(ptr_t node, std::string& key_out, bool& value_is_skip_eos) noexcept {
        value_is_skip_eos = false;
        if (!node) return nullptr;

        while (true) {
            // 1. Check EOS first (before consuming skip)
            if (node.get_eos()) {
                value_is_skip_eos = false;
                return node;
            }

            // 2. If EOS-only node, no value found
            if (node.is_eos()) {
                return nullptr;
            }

            // 3. Consume SKIP and check skip_eos
            const std::string& skip = node.get_skip();
            key_out.append(skip);

            if (node.get_skip_eos()) {
                value_is_skip_eos = true;
                return node;
            }

            // 4. If no children (SKIP-only), no value found
            if (!node.is_list() && !node.is_full()) {
                return nullptr;
            }

            // 5. Find first child
            unsigned char c;
            if (node.is_list()) {
                c = node.list->chars.smallest();
                if (c == 255) return nullptr;
                int idx = node.list->chars.find(c);
                key_out.push_back(static_cast<char>(c));
                node = node.list->children[idx].load();
            } else {
                // FULL
                c = node.full->valid.first_set();
                if (c == 255) return nullptr;
                key_out.push_back(static_cast<char>(c));
                node = node.full->children[c].load();
            }

            if (!node) return nullptr;
        }
    }

    // Find next leaf after given key (for iterator increment)
    static ptr_t find_next_leaf(ptr_t root, const std::string& current_key,
                                 std::string& next_key_out, bool& value_is_skip_eos) noexcept {
        value_is_skip_eos = false;
        if (!root) return nullptr;

        struct frame {
            ptr_t node;
            std::string prefix;
            int state;  // 0=check eos, 1=check skip_eos, 2=iterating children
            unsigned char last_child;
        };

        std::vector<frame> stack;
        stack.push_back({root, "", 0, 0});

        bool found_current = false;

        while (!stack.empty()) {
            frame& f = stack.back();
            ptr_t node = f.node;

            if (!node) {
                stack.pop_back();
                continue;
            }

            if (f.state == 0) {
                f.state = 1;
                if (node.is_eos()) {
                    if (f.prefix == current_key) {
                        found_current = true;
                    } else if (found_current && node.get_eos()) {
                        next_key_out = f.prefix;
                        value_is_skip_eos = false;
                        return node;
                    }
                    stack.pop_back();
                    continue;
                }

                if (node.get_eos()) {
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
                f.state = 2;
                const std::string& skip = node.get_skip();
                std::string skip_prefix = f.prefix + skip;

                if (node.get_skip_eos()) {
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
                if (!node.is_list() && !node.is_full()) {
                    stack.pop_back();
                    continue;
                }

                unsigned char c;
                if (f.last_child == 0) {
                    c = node.first_child_char();
                } else {
                    c = node.next_child_char(f.last_child);
                }

                if (c == 255) {
                    stack.pop_back();
                    continue;
                }

                f.last_child = c;
                ptr_t child = node.find_child(c);
                if (child) {
                    stack.push_back({child, f.prefix + static_cast<char>(c), 0, 0});
                }
            }
        }

        return nullptr;
    }
};

}  // namespace gteitelbaum
