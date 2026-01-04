#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_node.h"
#include "tktrie_help_nav.h"

namespace gteitelbaum {

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct insert_result {
    static constexpr bool VAR_LEN = (FIXED_LEN == 0);
    using interior_ptr = node_ptr<T, THREADED, Allocator, false, VAR_LEN>;

    interior_ptr new_subtree;
    void* target_slot = nullptr;
    uint64_t expected_ptr = 0;
    std::vector<void*> new_nodes;
    std::vector<void*> old_nodes;
    std::vector<T*> old_values;  // Only used for VAR_LEN
    bool already_exists = false;
    bool in_place = false;

    insert_result() {
        new_nodes.reserve(8);
        old_nodes.reserve(8);
    }
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct insert_helpers {
    static constexpr bool VAR_LEN = (FIXED_LEN == 0);

    using interior_ptr = node_ptr<T, THREADED, Allocator, false, VAR_LEN>;
    using leaf_ptr = node_ptr<T, THREADED, Allocator, !VAR_LEN, false>;
    using builder_t = node_builder<T, THREADED, Allocator, FIXED_LEN>;
    using nav_t = nav_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using result_t = insert_result<T, THREADED, Allocator, FIXED_LEN>;
    using var_acc = var_len_accessors<T, THREADED, Allocator>;
    using int_acc = interior_accessors<T, THREADED, Allocator>;
    using atomic_ptr_t = atomic_node_ptr<T, THREADED, Allocator, false, VAR_LEN>;

    template <typename U>
    static result_t build_insert_path(builder_t& builder, atomic_ptr_t* root_slot, interior_ptr root,
                                       std::string_view key, U&& value) {
        result_t result;

        if (!root) {
            result.target_slot = root_slot;
            result.expected_ptr = 0;

            if constexpr (VAR_LEN) {
                T* val_ptr = builder.alloc_value(std::forward<U>(value));
                interior_ptr new_node;
                if (key.empty()) {
                    new_node = builder.build_interior_eos(val_ptr);
                } else {
                    new_node = builder.build_interior_skip(std::string(key), nullptr, val_ptr);
                }
                result.new_nodes.push_back(new_node.raw);
                result.new_subtree = new_node;
            } else {
                // FIXED_LEN: create skip to leaf
                if (key.size() == FIXED_LEN) {
                    // Single char -> leaf
                    auto leaf = builder.build_leaf_eos(std::forward<U>(value));
                    interior_ptr list = builder.build_interior_list(std::string(key.substr(0, FIXED_LEN - 1)));
                    list.list->chars.add(static_cast<unsigned char>(key.back()));
                    list.list->children[0].store(interior_ptr(leaf.raw));
                    result.new_nodes.push_back(leaf.raw);
                    result.new_nodes.push_back(list.raw);
                    result.new_subtree = list;
                } else {
                    // Should be exactly FIXED_LEN
                    KTRIE_DEBUG_ASSERT(false);
                }
            }
            return result;
        }

        if constexpr (VAR_LEN) {
            return insert_var_len(builder, root_slot, root, key, std::forward<U>(value), result);
        } else {
            return insert_fixed_len(builder, root_slot, root, key, std::forward<U>(value), 0, result);
        }
    }

private:
    // =========================================================================
    // VAR_LEN insert
    // =========================================================================

    template <typename U>
    static result_t& insert_var_len(builder_t& builder, atomic_ptr_t* parent_slot, interior_ptr node,
                                     std::string_view key, U&& value, result_t& result) {
        result.target_slot = parent_slot;
        result.expected_ptr = reinterpret_cast<uint64_t>(node.raw);

        // Check EOS
        if (key.empty()) {
            T* old = var_acc::get_eos(node);
            if (old) {
                result.already_exists = true;
                return result;
            }
            T* val_ptr = builder.alloc_value(std::forward<U>(value));
            var_acc::set_eos(node, val_ptr);
            result.in_place = true;
            return result;
        }

        if (node.is_eos()) {
            return convert_eos_var_len(builder, node, key, std::forward<U>(value), result);
        }

        // Consume skip
        const std::string& skip = var_acc::get_skip(node);
        if (!skip.empty()) {
            size_t match = nav_t::match_skip(skip, key);

            if (match < skip.size() && match < key.size()) {
                return split_skip_diverge_var_len(builder, node, key, std::forward<U>(value), match, result);
            }
            if (match < skip.size()) {
                return split_skip_prefix_var_len(builder, node, key, std::forward<U>(value), match, result);
            }

            key.remove_prefix(match);

            if (key.empty()) {
                T* old = var_acc::get_skip_eos(node);
                if (old) {
                    result.already_exists = true;
                    return result;
                }
                T* val_ptr = builder.alloc_value(std::forward<U>(value));
                var_acc::set_skip_eos(node, val_ptr);
                result.in_place = true;
                return result;
            }
        }

        // Follow or add child
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);

        if (!node.is_list() && !node.is_full()) {
            return convert_skip_to_list_var_len(builder, node, c, key, std::forward<U>(value), result);
        }

        if (node.is_list()) {
            int idx = node.list->chars.find(c);
            if (idx >= 0) {
                interior_ptr child = node.list->children[idx].load();
                if (child) {
                    return insert_var_len(builder, &node.list->children[idx], child, key, std::forward<U>(value), result);
                }
            }
            return add_child_to_list_var_len(builder, node, c, key, std::forward<U>(value), result);
        } else {
            if (node.full->valid.test(c)) {
                interior_ptr child = node.full->children[c].load();
                if (child) {
                    return insert_var_len(builder, &node.full->children[c], child, key, std::forward<U>(value), result);
                }
            }
            return add_child_to_full_var_len(builder, node, c, key, std::forward<U>(value), result);
        }
    }

    template <typename U>
    static result_t& convert_eos_var_len(builder_t& builder, interior_ptr node,
                                          std::string_view key, U&& value, result_t& result) {
        T* eos_val = var_acc::get_eos(node);
        T* val_ptr = builder.alloc_value(std::forward<U>(value));

        interior_ptr child;
        if (key.size() == 1) {
            child = builder.build_interior_eos(val_ptr);
        } else {
            child = builder.build_interior_skip(std::string(key.substr(1)), nullptr, val_ptr);
        }
        result.new_nodes.push_back(child.raw);

        interior_ptr new_node = builder.build_interior_list("", eos_val, nullptr);
        new_node.list->chars.add(static_cast<unsigned char>(key[0]));
        new_node.list->children[0].store(child);
        result.new_nodes.push_back(new_node.raw);

        result.new_subtree = new_node;
        result.old_nodes.push_back(node.raw);
        return result;
    }

    template <typename U>
    static result_t& split_skip_diverge_var_len(builder_t& builder, interior_ptr node, std::string_view key,
                                                 U&& value, size_t match, result_t& result) {
        const std::string& skip = var_acc::get_skip(node);
        std::string common = skip.substr(0, match);
        unsigned char old_char = static_cast<unsigned char>(skip[match]);
        unsigned char new_char = static_cast<unsigned char>(key[match]);

        T* eos_val = var_acc::get_eos(node);
        T* val_ptr = builder.alloc_value(std::forward<U>(value));

        interior_ptr old_suffix = clone_with_shorter_skip_var_len(builder, node, match + 1);
        result.new_nodes.push_back(old_suffix.raw);

        std::string_view new_rest = key.substr(match + 1);
        interior_ptr new_suffix;
        if (new_rest.empty()) {
            new_suffix = builder.build_interior_eos(val_ptr);
        } else {
            new_suffix = builder.build_interior_skip(std::string(new_rest), nullptr, val_ptr);
        }
        result.new_nodes.push_back(new_suffix.raw);

        interior_ptr branch = builder.build_interior_list(common, eos_val, nullptr);
        if (old_char < new_char) {
            branch.list->chars.add(old_char);
            branch.list->chars.add(new_char);
            branch.list->children[0].store(old_suffix);
            branch.list->children[1].store(new_suffix);
        } else {
            branch.list->chars.add(new_char);
            branch.list->chars.add(old_char);
            branch.list->children[0].store(new_suffix);
            branch.list->children[1].store(old_suffix);
        }
        result.new_nodes.push_back(branch.raw);

        result.new_subtree = branch;
        result.old_nodes.push_back(node.raw);
        return result;
    }

    template <typename U>
    static result_t& split_skip_prefix_var_len(builder_t& builder, interior_ptr node, std::string_view /*key*/,
                                                U&& value, size_t match, result_t& result) {
        const std::string& skip = var_acc::get_skip(node);
        std::string prefix = skip.substr(0, match);
        unsigned char c = static_cast<unsigned char>(skip[match]);

        T* eos_val = var_acc::get_eos(node);
        T* val_ptr = builder.alloc_value(std::forward<U>(value));

        interior_ptr suffix = clone_with_shorter_skip_var_len(builder, node, match + 1);
        result.new_nodes.push_back(suffix.raw);

        interior_ptr new_node = builder.build_interior_list(prefix, eos_val, val_ptr);
        new_node.list->chars.add(c);
        new_node.list->children[0].store(suffix);
        result.new_nodes.push_back(new_node.raw);

        result.new_subtree = new_node;
        result.old_nodes.push_back(node.raw);
        return result;
    }

    template <typename U>
    static result_t& convert_skip_to_list_var_len(builder_t& builder, interior_ptr node,
                                                   unsigned char c, std::string_view rest, U&& value, result_t& result) {
        T* eos_val = var_acc::get_eos(node);
        T* skip_eos_val = var_acc::get_skip_eos(node);
        const std::string& skip = var_acc::get_skip(node);

        T* val_ptr = builder.alloc_value(std::forward<U>(value));

        interior_ptr child;
        if (rest.empty()) {
            child = builder.build_interior_eos(val_ptr);
        } else {
            child = builder.build_interior_skip(std::string(rest), nullptr, val_ptr);
        }
        result.new_nodes.push_back(child.raw);

        interior_ptr new_node = builder.build_interior_list(skip, eos_val, skip_eos_val);
        new_node.list->chars.add(c);
        new_node.list->children[0].store(child);
        result.new_nodes.push_back(new_node.raw);

        result.new_subtree = new_node;
        result.old_nodes.push_back(node.raw);
        return result;
    }

    template <typename U>
    static result_t& add_child_to_list_var_len(builder_t& builder, interior_ptr node,
                                                unsigned char c, std::string_view rest, U&& value, result_t& result) {
        T* val_ptr = builder.alloc_value(std::forward<U>(value));

        interior_ptr child;
        if (rest.empty()) {
            child = builder.build_interior_eos(val_ptr);
        } else {
            child = builder.build_interior_skip(std::string(rest), nullptr, val_ptr);
        }
        result.new_nodes.push_back(child.raw);

        int count = node.list->chars.count();
        if (count < LIST_MAX) {
            int idx = node.list->chars.add(c);
            node.list->children[idx].store(child);
            result.in_place = true;
            return result;
        }
        return convert_list_to_full_var_len(builder, node, c, child, result);
    }

    template <typename U>
    static result_t& add_child_to_full_var_len(builder_t& builder, interior_ptr node,
                                                unsigned char c, std::string_view rest, U&& value, result_t& result) {
        T* val_ptr = builder.alloc_value(std::forward<U>(value));

        interior_ptr child;
        if (rest.empty()) {
            child = builder.build_interior_eos(val_ptr);
        } else {
            child = builder.build_interior_skip(std::string(rest), nullptr, val_ptr);
        }
        result.new_nodes.push_back(child.raw);

        node.full->valid.template atomic_set<THREADED>(c);
        node.full->children[c].store(child);
        result.in_place = true;
        return result;
    }

    static result_t& convert_list_to_full_var_len(builder_t& builder, interior_ptr node,
                                                   unsigned char c, interior_ptr new_child, result_t& result) {
        T* eos_val = var_acc::get_eos(node);
        T* skip_eos_val = var_acc::get_skip_eos(node);
        const std::string& skip = var_acc::get_skip(node);

        interior_ptr full = builder.build_interior_full(skip, eos_val, skip_eos_val);

        int count = node.list->chars.count();
        for (int i = 0; i < count; ++i) {
            unsigned char ch = node.list->chars.char_at(i);
            full.full->valid.set(ch);
            full.full->children[ch].store(node.list->children[i].load());
        }

        full.full->valid.set(c);
        full.full->children[c].store(new_child);

        result.new_nodes.push_back(full.raw);
        result.new_subtree = full;
        result.old_nodes.push_back(node.raw);
        return result;
    }

    static interior_ptr clone_with_shorter_skip_var_len(builder_t& builder, interior_ptr node, size_t skip_prefix_len) {
        const std::string& skip = var_acc::get_skip(node);
        std::string new_skip = skip.substr(skip_prefix_len);
        T* skip_eos_val = var_acc::get_skip_eos(node);

        if (node.is_skip()) {
            if (new_skip.empty()) {
                return builder.build_interior_eos(skip_eos_val);
            }
            return builder.build_interior_skip(new_skip, nullptr, skip_eos_val);
        }

        if (node.is_list()) {
            interior_ptr n = builder.build_interior_list(new_skip, nullptr, skip_eos_val);
            n.list->chars = node.list->chars;
            for (int i = 0; i < node.list->chars.count(); ++i) {
                n.list->children[i].store(node.list->children[i].load());
            }
            return n;
        }

        // FULL
        interior_ptr n = builder.build_interior_full(new_skip, nullptr, skip_eos_val);
        n.full->valid = node.full->valid;
        for (int i = 0; i < 256; ++i) {
            if (node.full->valid.test(static_cast<unsigned char>(i))) {
                n.full->children[i].store(node.full->children[i].load());
            }
        }
        return n;
    }

    // =========================================================================
    // FIXED_LEN insert (simplified - no EOS/skip_eos checks in interior)
    // =========================================================================

    template <typename U>
    static result_t& insert_fixed_len(builder_t& builder, atomic_ptr_t* parent_slot, interior_ptr node,
                                       std::string_view key, U&& value, size_t depth, result_t& result) {
        result.target_slot = parent_slot;
        result.expected_ptr = reinterpret_cast<uint64_t>(node.raw);

        KTRIE_DEBUG_ASSERT(key.size() == FIXED_LEN - depth);

        // Consume skip
        if (!node.is_eos()) {
            const std::string& skip = int_acc::get_skip(node);
            if (!skip.empty()) {
                size_t match = nav_t::match_skip(skip, key);

                if (match < skip.size() && match < key.size()) {
                    return split_skip_diverge_fixed_len(builder, node, key, std::forward<U>(value), match, depth, result);
                }
                if (match < skip.size()) {
                    return split_skip_prefix_fixed_len(builder, node, key, std::forward<U>(value), match, depth, result);
                }

                key.remove_prefix(match);
                depth += match;
            }
        }

        // At leaf level?
        if (depth == FIXED_LEN - 1) {
            KTRIE_DEBUG_ASSERT(key.size() == 1);
            unsigned char c = static_cast<unsigned char>(key[0]);
            return insert_into_leaf_parent(builder, node, c, std::forward<U>(value), result);
        }

        // Follow or add child
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        ++depth;

        if (!node.is_list() && !node.is_full()) {
            return convert_skip_to_list_fixed_len(builder, node, c, key, std::forward<U>(value), depth, result);
        }

        if (node.is_list()) {
            int idx = node.list->chars.find(c);
            if (idx >= 0) {
                interior_ptr child = node.list->children[idx].load();
                if (child) {
                    return insert_fixed_len(builder, &node.list->children[idx], child, key, std::forward<U>(value), depth, result);
                }
            }
            return add_child_to_list_fixed_len(builder, node, c, key, std::forward<U>(value), depth, result);
        } else {
            if (node.full->valid.test(c)) {
                interior_ptr child = node.full->children[c].load();
                if (child) {
                    return insert_fixed_len(builder, &node.full->children[c], child, key, std::forward<U>(value), depth, result);
                }
            }
            return add_child_to_full_fixed_len(builder, node, c, key, std::forward<U>(value), depth, result);
        }
    }

    template <typename U>
    static result_t& insert_into_leaf_parent(builder_t& builder, interior_ptr node,
                                              unsigned char c, U&& value, result_t& result) {
        static_assert(!VAR_LEN);

        if (node.is_list()) {
            int idx = node.list->chars.find(c);
            if (idx >= 0) {
                // Value already exists
                result.already_exists = true;
                return result;
            }

            int count = node.list->chars.count();
            if (count < LIST_MAX) {
                // In-place add
                leaf_ptr leaf = builder.build_leaf_eos(std::forward<U>(value));
                idx = node.list->chars.add(c);
                node.list->children[idx].store(interior_ptr(leaf.raw));
                result.new_nodes.push_back(leaf.raw);
                result.in_place = true;
                return result;
            }

            // Convert to FULL
            return convert_leaf_list_to_full(builder, node, c, std::forward<U>(value), result);
        }

        if (node.is_full()) {
            if (node.full->valid.test(c)) {
                result.already_exists = true;
                return result;
            }

            leaf_ptr leaf = builder.build_leaf_eos(std::forward<U>(value));
            node.full->valid.template atomic_set<THREADED>(c);
            node.full->children[c].store(interior_ptr(leaf.raw));
            result.new_nodes.push_back(leaf.raw);
            result.in_place = true;
            return result;
        }

        // Need to convert SKIP to LIST first
        const std::string& skip = int_acc::get_skip(node);
        interior_ptr list = builder.build_interior_list(skip);
        leaf_ptr leaf = builder.build_leaf_eos(std::forward<U>(value));
        list.list->chars.add(c);
        list.list->children[0].store(interior_ptr(leaf.raw));
        result.new_nodes.push_back(leaf.raw);
        result.new_nodes.push_back(list.raw);
        result.new_subtree = list;
        result.old_nodes.push_back(node.raw);
        return result;
    }

    template <typename U>
    static result_t& convert_leaf_list_to_full(builder_t& builder, interior_ptr node,
                                                unsigned char c, U&& value, result_t& result) {
        static_assert(!VAR_LEN);
        const std::string& skip = int_acc::get_skip(node);

        interior_ptr full = builder.build_interior_full(skip);

        int count = node.list->chars.count();
        for (int i = 0; i < count; ++i) {
            unsigned char ch = node.list->chars.char_at(i);
            full.full->valid.set(ch);
            full.full->children[ch].store(node.list->children[i].load());
        }

        leaf_ptr leaf = builder.build_leaf_eos(std::forward<U>(value));
        full.full->valid.set(c);
        full.full->children[c].store(interior_ptr(leaf.raw));

        result.new_nodes.push_back(leaf.raw);
        result.new_nodes.push_back(full.raw);
        result.new_subtree = full;
        result.old_nodes.push_back(node.raw);
        return result;
    }

    template <typename U>
    static result_t& split_skip_diverge_fixed_len(builder_t& builder, interior_ptr node, std::string_view key,
                                                   U&& value, size_t match, size_t depth, result_t& result) {
        static_assert(!VAR_LEN);
        const std::string& skip = int_acc::get_skip(node);
        std::string common = skip.substr(0, match);
        unsigned char old_char = static_cast<unsigned char>(skip[match]);
        unsigned char new_char = static_cast<unsigned char>(key[match]);

        interior_ptr old_suffix = clone_with_shorter_skip_fixed_len(builder, node, match + 1);
        result.new_nodes.push_back(old_suffix.raw);

        size_t new_depth = depth + match + 1;
        std::string_view new_rest = key.substr(match + 1);

        interior_ptr new_suffix;
        if (new_depth == FIXED_LEN) {
            // new_rest is empty, this is a leaf
            leaf_ptr leaf = builder.build_leaf_eos(std::forward<U>(value));
            new_suffix = interior_ptr(leaf.raw);
        } else if (new_depth == FIXED_LEN - 1 && new_rest.size() == 1) {
            // Next level is leaf
            leaf_ptr leaf = builder.build_leaf_eos(std::forward<U>(value));
            new_suffix = builder.build_interior_list("");
            new_suffix.list->chars.add(static_cast<unsigned char>(new_rest[0]));
            new_suffix.list->children[0].store(interior_ptr(leaf.raw));
            result.new_nodes.push_back(leaf.raw);
        } else {
            // Build chain: skip -> list -> leaf
            leaf_ptr leaf = builder.build_leaf_eos(std::forward<U>(value));
            interior_ptr list = builder.build_interior_list(std::string(new_rest.substr(0, new_rest.size() - 1)));
            list.list->chars.add(static_cast<unsigned char>(new_rest.back()));
            list.list->children[0].store(interior_ptr(leaf.raw));
            result.new_nodes.push_back(leaf.raw);
            new_suffix = list;
        }
        result.new_nodes.push_back(new_suffix.raw);

        interior_ptr branch = builder.build_interior_list(common);
        if (old_char < new_char) {
            branch.list->chars.add(old_char);
            branch.list->chars.add(new_char);
            branch.list->children[0].store(old_suffix);
            branch.list->children[1].store(new_suffix);
        } else {
            branch.list->chars.add(new_char);
            branch.list->chars.add(old_char);
            branch.list->children[0].store(new_suffix);
            branch.list->children[1].store(old_suffix);
        }
        result.new_nodes.push_back(branch.raw);

        result.new_subtree = branch;
        result.old_nodes.push_back(node.raw);
        return result;
    }

    template <typename U>
    static result_t& split_skip_prefix_fixed_len(builder_t& builder, interior_ptr node, std::string_view key,
                                                  U&& value, size_t match, size_t depth, result_t& result) {
        static_assert(!VAR_LEN);
        // Key is prefix of skip - doesn't happen in FIXED_LEN (all keys same length)
        KTRIE_DEBUG_ASSERT(false && "Key prefix of skip shouldn't happen in FIXED_LEN");
        (void)builder; (void)node; (void)key; (void)value; (void)match; (void)depth;
        return result;
    }

    template <typename U>
    static result_t& convert_skip_to_list_fixed_len(builder_t& builder, interior_ptr node,
                                                     unsigned char c, std::string_view rest, U&& value,
                                                     size_t depth, result_t& result) {
        static_assert(!VAR_LEN);
        const std::string& skip = int_acc::get_skip(node);

        interior_ptr list = builder.build_interior_list(skip);

        // Create child subtree
        interior_ptr child;
        if (depth == FIXED_LEN - 1) {
            // Child is leaf
            leaf_ptr leaf = builder.build_leaf_eos(std::forward<U>(value));
            child = interior_ptr(leaf.raw);
            result.new_nodes.push_back(leaf.raw);
        } else if (rest.empty()) {
            leaf_ptr leaf = builder.build_leaf_eos(std::forward<U>(value));
            child = interior_ptr(leaf.raw);
            result.new_nodes.push_back(leaf.raw);
        } else {
            // Build skip chain to leaf
            child = build_chain_to_leaf(builder, rest, std::forward<U>(value), depth, result);
        }

        list.list->chars.add(c);
        list.list->children[0].store(child);
        result.new_nodes.push_back(list.raw);

        result.new_subtree = list;
        result.old_nodes.push_back(node.raw);
        return result;
    }

    template <typename U>
    static result_t& add_child_to_list_fixed_len(builder_t& builder, interior_ptr node,
                                                  unsigned char c, std::string_view rest, U&& value,
                                                  size_t depth, result_t& result) {
        static_assert(!VAR_LEN);

        interior_ptr child;
        if (depth == FIXED_LEN - 1 || rest.empty()) {
            leaf_ptr leaf = builder.build_leaf_eos(std::forward<U>(value));
            child = interior_ptr(leaf.raw);
            result.new_nodes.push_back(leaf.raw);
        } else {
            child = build_chain_to_leaf(builder, rest, std::forward<U>(value), depth, result);
        }

        int count = node.list->chars.count();
        if (count < LIST_MAX) {
            int idx = node.list->chars.add(c);
            node.list->children[idx].store(child);
            result.in_place = true;
            return result;
        }

        // Convert to FULL
        const std::string& skip = int_acc::get_skip(node);
        interior_ptr full = builder.build_interior_full(skip);

        for (int i = 0; i < count; ++i) {
            unsigned char ch = node.list->chars.char_at(i);
            full.full->valid.set(ch);
            full.full->children[ch].store(node.list->children[i].load());
        }

        full.full->valid.set(c);
        full.full->children[c].store(child);

        result.new_nodes.push_back(full.raw);
        result.new_subtree = full;
        result.old_nodes.push_back(node.raw);
        return result;
    }

    template <typename U>
    static result_t& add_child_to_full_fixed_len(builder_t& builder, interior_ptr node,
                                                  unsigned char c, std::string_view rest, U&& value,
                                                  size_t depth, result_t& result) {
        static_assert(!VAR_LEN);

        interior_ptr child;
        if (depth == FIXED_LEN - 1 || rest.empty()) {
            leaf_ptr leaf = builder.build_leaf_eos(std::forward<U>(value));
            child = interior_ptr(leaf.raw);
            result.new_nodes.push_back(leaf.raw);
        } else {
            child = build_chain_to_leaf(builder, rest, std::forward<U>(value), depth, result);
        }

        node.full->valid.template atomic_set<THREADED>(c);
        node.full->children[c].store(child);
        result.in_place = true;
        return result;
    }

    template <typename U>
    static interior_ptr build_chain_to_leaf(builder_t& builder, std::string_view rest, U&& value,
                                             size_t /*depth*/, result_t& result) {
        static_assert(!VAR_LEN);

        // Build: list(skip=rest minus last char, char=last char) -> leaf(value)
        leaf_ptr leaf = builder.build_leaf_eos(std::forward<U>(value));
        result.new_nodes.push_back(leaf.raw);

        if (rest.size() == 1) {
            interior_ptr list = builder.build_interior_list("");
            list.list->chars.add(static_cast<unsigned char>(rest[0]));
            list.list->children[0].store(interior_ptr(leaf.raw));
            result.new_nodes.push_back(list.raw);
            return list;
        }

        // Put skip prefix in the list node itself
        interior_ptr list = builder.build_interior_list(std::string(rest.substr(0, rest.size() - 1)));
        list.list->chars.add(static_cast<unsigned char>(rest.back()));
        list.list->children[0].store(interior_ptr(leaf.raw));
        result.new_nodes.push_back(list.raw);
        return list;
    }

    static interior_ptr clone_with_shorter_skip_fixed_len(builder_t& builder, interior_ptr node, size_t skip_prefix_len) {
        static_assert(!VAR_LEN);
        const std::string& skip = int_acc::get_skip(node);
        std::string new_skip = skip.substr(skip_prefix_len);

        if (node.is_skip()) {
            return builder.build_interior_skip(new_skip);
        }

        if (node.is_list()) {
            interior_ptr n = builder.build_interior_list(new_skip);
            n.list->chars = node.list->chars;
            for (int i = 0; i < node.list->chars.count(); ++i) {
                n.list->children[i].store(node.list->children[i].load());
            }
            return n;
        }

        // FULL
        interior_ptr n = builder.build_interior_full(new_skip);
        n.full->valid = node.full->valid;
        for (int i = 0; i < 256; ++i) {
            if (node.full->valid.test(static_cast<unsigned char>(i))) {
                n.full->children[i].store(node.full->children[i].load());
            }
        }
        return n;
    }
};

}  // namespace gteitelbaum
