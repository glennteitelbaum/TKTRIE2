#pragma once

#include <string>
#include <string_view>

#include "tktrie_defines.h"
#include "tktrie_node.h"

namespace gteitelbaum {

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct nav_helpers {
    static constexpr bool VAR_LEN = (FIXED_LEN == 0);

    using interior_ptr = node_ptr<T, THREADED, Allocator, false, VAR_LEN>;
    using leaf_ptr = node_ptr<T, THREADED, Allocator, !VAR_LEN, false>;
    using var_acc = var_len_accessors<T, THREADED, Allocator>;
    using int_acc = interior_accessors<T, THREADED, Allocator>;
    using leaf_acc = leaf_accessors<T, THREADED, Allocator>;

    static size_t match_skip(const std::string& skip, std::string_view key) noexcept {
        size_t i = 0;
        size_t n = std::min(skip.size(), key.size());
        while (i < n && skip[i] == key[i]) ++i;
        return i;
    }

    static bool contains(interior_ptr root, std::string_view key) noexcept {
        T val;
        return read(root, key, val);
    }

    static bool read(interior_ptr cur, std::string_view key, T& out) noexcept {
        if constexpr (VAR_LEN) {
            return read_var_len(cur, key, out);
        } else {
            return read_fixed_len(cur, key, out);
        }
    }

private:
    // VAR_LEN: check eos/skip_eos at every node
    static bool read_var_len(interior_ptr cur, std::string_view key, T& out) noexcept {
        while (cur) {
            // Check EOS
            if (key.empty()) {
                T* eos = var_acc::get_eos(cur);
                if (eos) { out = *eos; return true; }
                return false;
            }

            if (cur.is_eos()) return false;

            // Consume skip
            const std::string& skip = var_acc::get_skip(cur);
            if (!skip.empty()) {
                size_t match = match_skip(skip, key);
                if (match < skip.size()) return false;
                key.remove_prefix(match);

                if (key.empty()) {
                    T* skip_eos = var_acc::get_skip_eos(cur);
                    if (skip_eos) { out = *skip_eos; return true; }
                    return false;
                }
            }

            // Follow child
            if (!cur.is_list() && !cur.is_full()) return false;

            unsigned char c = static_cast<unsigned char>(key[0]);
            key.remove_prefix(1);
            cur = var_acc::find_child(cur, c);
        }
        return false;
    }

    // FIXED_LEN: no eos checks in interior loop
    static bool read_fixed_len(interior_ptr cur, std::string_view key, T& out) noexcept {
        size_t depth = 0;

        while (cur) {
            // Consume skip (interior has no values)
            if (!cur.is_eos()) {  // EOS type doesn't exist in FIXED_LEN interior
                const std::string& skip = int_acc::get_skip(cur);
                if (!skip.empty()) {
                    size_t match = match_skip(skip, key);
                    if (match < skip.size()) return false;
                    key.remove_prefix(match);
                    depth += match;
                }
            }

            // Check if at leaf level
            if (depth == FIXED_LEN - 1 && key.size() == 1) {
                unsigned char c = static_cast<unsigned char>(key[0]);
                return read_from_leaf_parent(cur, c, out);
            }

            if (depth >= FIXED_LEN) {
                // Should have hit leaf already
                return false;
            }

            // Follow child
            if (!cur.is_list() && !cur.is_full()) return false;

            unsigned char c = static_cast<unsigned char>(key[0]);
            key.remove_prefix(1);
            ++depth;

            cur = int_acc::find_child(cur, c);
        }
        return false;
    }

    // Read from leaf node via interior parent's child pointer
    static bool read_from_leaf_parent(interior_ptr parent, unsigned char c, T& out) noexcept {
        static_assert(!VAR_LEN);

        if (parent.is_list()) {
            int idx = parent.list->chars.find(c);
            if (idx < 0) return false;

            // Child is a leaf node
            leaf_ptr leaf;
            leaf.raw = parent.list->children[idx].load().raw;
            if (!leaf) return false;

            return read_leaf_value(leaf, out);
        }

        if (parent.is_full()) {
            if (!parent.full->valid.test(c)) return false;

            leaf_ptr leaf;
            leaf.raw = parent.full->children[c].load().raw;
            if (!leaf) return false;

            return read_leaf_value(leaf, out);
        }

        return false;
    }

    static bool read_leaf_value(leaf_ptr leaf, T& out) noexcept {
        static_assert(!VAR_LEN);

        if (leaf.is_eos()) {
            out = leaf.eos->value;
            return true;
        }
        if (leaf.is_skip()) {
            out = leaf.skip->value;
            return true;
        }
        // LIST/FULL at leaf shouldn't happen for single-value read
        return false;
    }

public:
    // Find first value (for begin iterator)
    static interior_ptr find_first_leaf(interior_ptr node, std::string& key_out, T& value_out) noexcept {
        if constexpr (VAR_LEN) {
            return find_first_var_len(node, key_out, value_out);
        } else {
            return find_first_fixed_len(node, key_out, value_out, 0);
        }
    }

private:
    static interior_ptr find_first_var_len(interior_ptr node, std::string& key_out, T& value_out) noexcept {
        if (!node) return nullptr;

        while (true) {
            // Check EOS
            T* eos = var_acc::get_eos(node);
            if (eos) {
                value_out = *eos;
                return node;
            }

            if (node.is_eos()) return nullptr;

            // Consume skip, check skip_eos
            const std::string& skip = var_acc::get_skip(node);
            key_out.append(skip);

            T* skip_eos = var_acc::get_skip_eos(node);
            if (skip_eos) {
                value_out = *skip_eos;
                return node;
            }

            // Find first child
            if (!node.is_list() && !node.is_full()) return nullptr;

            unsigned char c = var_acc::first_child_char(node);
            if (c == 255) return nullptr;

            key_out.push_back(static_cast<char>(c));
            node = var_acc::find_child(node, c);
            if (!node) return nullptr;
        }
    }

    static interior_ptr find_first_fixed_len(interior_ptr node, std::string& key_out, T& value_out, size_t depth) noexcept {
        if (!node) return nullptr;

        while (true) {
            // Consume skip
            if (!node.is_eos()) {
                const std::string& skip = int_acc::get_skip(node);
                key_out.append(skip);
                depth += skip.size();
            }

            // Find first child
            if (!node.is_list() && !node.is_full()) return nullptr;

            unsigned char c = int_acc::first_child_char(node);
            if (c == 255) return nullptr;

            key_out.push_back(static_cast<char>(c));
            ++depth;

            // Check if child is at leaf level
            if (depth == FIXED_LEN) {
                leaf_ptr leaf;
                if (node.is_list()) {
                    int idx = node.list->chars.find(c);
                    leaf.raw = node.list->children[idx].load().raw;
                } else {
                    leaf.raw = node.full->children[c].load().raw;
                }

                if (leaf && read_leaf_value(leaf, value_out)) {
                    return node;
                }
                return nullptr;
            }

            node = int_acc::find_child(node, c);
            if (!node) return nullptr;
        }
    }
};

}  // namespace gteitelbaum
