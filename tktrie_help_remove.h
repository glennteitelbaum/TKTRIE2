#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_node.h"
#include "tktrie_help_nav.h"

namespace gteitelbaum {

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct remove_result {
    static constexpr bool VAR_LEN = (FIXED_LEN == 0);
    using interior_ptr = node_ptr<T, THREADED, Allocator, false, VAR_LEN>;

    interior_ptr new_subtree;
    void* target_slot = nullptr;
    uint64_t expected_ptr = 0;
    std::vector<void*> new_nodes;
    std::vector<void*> old_nodes;
    std::vector<T*> old_values;
    bool found = false;
    bool subtree_deleted = false;
    bool in_place = false;

    remove_result() {
        new_nodes.reserve(8);
        old_nodes.reserve(8);
    }
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct remove_helpers {
    static constexpr bool VAR_LEN = (FIXED_LEN == 0);

    using interior_ptr = node_ptr<T, THREADED, Allocator, false, VAR_LEN>;
    using leaf_ptr = node_ptr<T, THREADED, Allocator, !VAR_LEN, false>;
    using builder_t = node_builder<T, THREADED, Allocator, FIXED_LEN>;
    using nav_t = nav_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using result_t = remove_result<T, THREADED, Allocator, FIXED_LEN>;
    using var_acc = var_len_accessors<T, THREADED, Allocator>;
    using int_acc = interior_accessors<T, THREADED, Allocator>;
    using atomic_ptr_t = atomic_node_ptr<T, THREADED, Allocator, false, VAR_LEN>;

    static result_t build_remove_path(builder_t& builder, atomic_ptr_t* root_slot, interior_ptr root,
                                       std::string_view key) {
        result_t result;
        if (!root) return result;

        result.target_slot = root_slot;
        result.expected_ptr = reinterpret_cast<uint64_t>(root.raw);

        if constexpr (VAR_LEN) {
            return remove_var_len(builder, root_slot, root, key, result);
        } else {
            return remove_fixed_len(builder, root_slot, root, key, 0, result);
        }
    }

private:
    // =========================================================================
    // VAR_LEN remove
    // =========================================================================

    static result_t& remove_var_len(builder_t& builder, atomic_ptr_t* parent_slot, interior_ptr node,
                                     std::string_view key, result_t& result) {
        result.target_slot = parent_slot;
        result.expected_ptr = reinterpret_cast<uint64_t>(node.raw);

        // Check EOS
        if (key.empty()) {
            T* eos = var_acc::get_eos(node);
            if (!eos) return result;

            result.found = true;
            result.old_values.push_back(eos);
            var_acc::set_eos(node, nullptr);

            if (can_delete_node_var_len(node)) {
                result.subtree_deleted = true;
                result.old_nodes.push_back(node.raw);
            } else {
                result.in_place = true;
                try_collapse_var_len(builder, node, result);
            }
            return result;
        }

        if (node.is_eos()) return result;

        // Consume skip
        const std::string& skip = var_acc::get_skip(node);
        if (!skip.empty()) {
            size_t match = nav_t::match_skip(skip, key);
            if (match < skip.size()) return result;
            key.remove_prefix(match);

            if (key.empty()) {
                T* skip_eos = var_acc::get_skip_eos(node);
                if (!skip_eos) return result;

                result.found = true;
                result.old_values.push_back(skip_eos);
                var_acc::set_skip_eos(node, nullptr);

                if (can_delete_node_var_len(node)) {
                    result.subtree_deleted = true;
                    result.old_nodes.push_back(node.raw);
                } else {
                    result.in_place = true;
                    try_collapse_var_len(builder, node, result);
                }
                return result;
            }
        }

        // Follow child
        if (!node.is_list() && !node.is_full()) return result;

        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);

        result_t child_result;
        if (node.is_list()) {
            int idx = node.list->chars.find(c);
            if (idx < 0) return result;
            interior_ptr child = node.list->children[idx].load();
            if (!child) return result;
            remove_var_len(builder, &node.list->children[idx], child, key, child_result);
        } else {
            if (!node.full->valid.test(c)) return result;
            interior_ptr child = node.full->children[c].load();
            if (!child) return result;
            remove_var_len(builder, &node.full->children[c], child, key, child_result);
        }

        if (!child_result.found) return result;

        // Merge results
        result.found = true;
        for (auto* n : child_result.new_nodes) result.new_nodes.push_back(n);
        for (auto* n : child_result.old_nodes) result.old_nodes.push_back(n);
        for (auto* v : child_result.old_values) result.old_values.push_back(v);

        if (child_result.subtree_deleted) {
            return remove_child_var_len(builder, node, c, result);
        }

        if (child_result.in_place) {
            result.in_place = true;
            return result;
        }

        return rebuild_with_new_child_var_len(builder, node, c, child_result.new_subtree, result);
    }

    static result_t& remove_child_var_len(builder_t& builder, interior_ptr node, unsigned char c, result_t& result) {
        if (node.is_list()) {
            int idx = node.list->chars.find(c);
            if (idx >= 0) {
                node.list->children[idx].store(nullptr);
                node.list->chars.remove_at(idx);

                int count = node.list->chars.count();
                for (int i = idx; i < count; ++i) {
                    node.list->children[i].store(node.list->children[i + 1].load());
                }
                node.list->children[count].store(nullptr);
            }

            if (can_delete_node_var_len(node)) {
                result.subtree_deleted = true;
                result.old_nodes.push_back(node.raw);
            } else {
                result.in_place = true;
                try_collapse_var_len(builder, node, result);
            }
        } else if (node.is_full()) {
            node.full->valid.template atomic_clear<THREADED>(c);
            node.full->children[c].store(nullptr);

            int count = node.full->valid.count();

            if (can_delete_node_var_len(node)) {
                result.subtree_deleted = true;
                result.old_nodes.push_back(node.raw);
            } else if (count <= LIST_MAX) {
                return convert_full_to_list_var_len(builder, node, result);
            } else {
                result.in_place = true;
            }
        }
        return result;
    }

    static result_t& rebuild_with_new_child_var_len(builder_t& builder, interior_ptr node, unsigned char c,
                                                     interior_ptr new_child, result_t& result) {
        T* eos_val = var_acc::get_eos(node);
        T* skip_eos_val = var_acc::get_skip_eos(node);
        const std::string& skip = var_acc::get_skip(node);

        interior_ptr new_node;

        if (node.is_list()) {
            new_node = builder.build_interior_list(skip, eos_val, skip_eos_val);
            new_node.list->chars = node.list->chars;
            int idx = node.list->chars.find(c);
            for (int i = 0; i < node.list->chars.count(); ++i) {
                if (i == idx) {
                    new_node.list->children[i].store(new_child);
                } else {
                    new_node.list->children[i].store(node.list->children[i].load());
                }
            }
        } else {
            new_node = builder.build_interior_full(skip, eos_val, skip_eos_val);
            new_node.full->valid = node.full->valid;
            for (int i = 0; i < 256; ++i) {
                if (node.full->valid.test(static_cast<unsigned char>(i))) {
                    if (static_cast<unsigned char>(i) == c) {
                        new_node.full->children[i].store(new_child);
                    } else {
                        new_node.full->children[i].store(node.full->children[i].load());
                    }
                }
            }
        }

        result.new_nodes.push_back(new_node.raw);
        result.new_subtree = new_node;
        result.old_nodes.push_back(node.raw);
        return result;
    }

    static result_t& convert_full_to_list_var_len(builder_t& builder, interior_ptr node, result_t& result) {
        T* eos_val = var_acc::get_eos(node);
        T* skip_eos_val = var_acc::get_skip_eos(node);
        const std::string& skip = var_acc::get_skip(node);

        interior_ptr list = builder.build_interior_list(skip, eos_val, skip_eos_val);

        for (int i = 0; i < 256; ++i) {
            if (node.full->valid.test(static_cast<unsigned char>(i))) {
                int idx = list.list->chars.add(static_cast<unsigned char>(i));
                list.list->children[idx].store(node.full->children[i].load());
            }
        }

        result.new_nodes.push_back(list.raw);
        result.new_subtree = list;
        result.old_nodes.push_back(node.raw);
        return result;
    }

    static bool can_delete_node_var_len(interior_ptr node) {
        if (var_acc::get_eos(node)) return false;
        if (node.is_eos()) return true;
        if (var_acc::get_skip_eos(node)) return false;
        return var_acc::child_count(node) == 0;
    }

    static void try_collapse_var_len(builder_t& builder, interior_ptr node, result_t& result) {
        if (node.is_eos()) return;
        if (var_acc::get_eos(node)) return;
        if (var_acc::get_skip_eos(node)) return;
        if (var_acc::child_count(node) != 1) return;

        unsigned char c = var_acc::first_child_char(node);
        if (c == 255) return;

        interior_ptr child = var_acc::find_child(node, c);
        if (!child) return;

        if (child.is_list() || child.is_full()) return;

        std::string new_skip = var_acc::get_skip(node);
        new_skip.push_back(static_cast<char>(c));
        if (!child.is_eos()) {
            new_skip.append(var_acc::get_skip(child));
        }

        T* child_val = child.is_eos() ? var_acc::get_eos(child) : var_acc::get_skip_eos(child);

        interior_ptr collapsed;
        if (new_skip.empty()) {
            collapsed = builder.build_interior_eos(child_val);
        } else {
            collapsed = builder.build_interior_skip(new_skip, nullptr, child_val);
        }

        result.in_place = false;
        result.new_nodes.push_back(collapsed.raw);
        result.new_subtree = collapsed;
        result.old_nodes.push_back(node.raw);
        result.old_nodes.push_back(child.raw);
    }

    // =========================================================================
    // FIXED_LEN remove (simplified)
    // =========================================================================

    static result_t& remove_fixed_len(builder_t& builder, atomic_ptr_t* parent_slot, interior_ptr node,
                                       std::string_view key, size_t depth, result_t& result) {
        result.target_slot = parent_slot;
        result.expected_ptr = reinterpret_cast<uint64_t>(node.raw);

        // Consume skip
        if (!node.is_eos()) {
            const std::string& skip = int_acc::get_skip(node);
            if (!skip.empty()) {
                size_t match = nav_t::match_skip(skip, key);
                if (match < skip.size()) return result;
                key.remove_prefix(match);
                depth += match;
            }
        }

        // At leaf level?
        if (depth == FIXED_LEN - 1) {
            KTRIE_DEBUG_ASSERT(key.size() == 1);
            unsigned char c = static_cast<unsigned char>(key[0]);
            return remove_from_leaf_parent(builder, node, c, result);
        }

        // Follow child
        if (!node.is_list() && !node.is_full()) return result;

        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        ++depth;

        result_t child_result;
        if (node.is_list()) {
            int idx = node.list->chars.find(c);
            if (idx < 0) return result;
            interior_ptr child = node.list->children[idx].load();
            if (!child) return result;
            remove_fixed_len(builder, &node.list->children[idx], child, key, depth, child_result);
        } else {
            if (!node.full->valid.test(c)) return result;
            interior_ptr child = node.full->children[c].load();
            if (!child) return result;
            remove_fixed_len(builder, &node.full->children[c], child, key, depth, child_result);
        }

        if (!child_result.found) return result;

        // Merge results
        result.found = true;
        for (auto* n : child_result.new_nodes) result.new_nodes.push_back(n);
        for (auto* n : child_result.old_nodes) result.old_nodes.push_back(n);

        if (child_result.subtree_deleted) {
            return remove_child_fixed_len(builder, node, c, result);
        }

        if (child_result.in_place) {
            result.in_place = true;
            return result;
        }

        return rebuild_with_new_child_fixed_len(builder, node, c, child_result.new_subtree, result);
    }

    static result_t& remove_from_leaf_parent(builder_t& builder, interior_ptr node, unsigned char c, result_t& result) {
        static_assert(!VAR_LEN);

        if (node.is_list()) {
            int idx = node.list->chars.find(c);
            if (idx < 0) return result;

            result.found = true;
            leaf_ptr leaf;
            leaf.raw = node.list->children[idx].load().raw;
            result.old_nodes.push_back(leaf.raw);

            node.list->children[idx].store(nullptr);
            node.list->chars.remove_at(idx);

            int count = node.list->chars.count();
            for (int i = idx; i < count; ++i) {
                node.list->children[i].store(node.list->children[i + 1].load());
            }
            node.list->children[count].store(nullptr);

            if (count == 0) {
                result.subtree_deleted = true;
                result.old_nodes.push_back(node.raw);
            } else {
                result.in_place = true;
            }
            return result;
        }

        if (node.is_full()) {
            if (!node.full->valid.test(c)) return result;

            result.found = true;
            leaf_ptr leaf;
            leaf.raw = node.full->children[c].load().raw;
            result.old_nodes.push_back(leaf.raw);

            node.full->valid.template atomic_clear<THREADED>(c);
            node.full->children[c].store(nullptr);

            int count = node.full->valid.count();

            if (count == 0) {
                result.subtree_deleted = true;
                result.old_nodes.push_back(node.raw);
            } else if (count <= LIST_MAX) {
                return convert_full_to_list_fixed_len(builder, node, result);
            } else {
                result.in_place = true;
            }
            return result;
        }

        return result;
    }

    static result_t& remove_child_fixed_len(builder_t& builder, interior_ptr node, unsigned char c, result_t& result) {
        static_assert(!VAR_LEN);

        if (node.is_list()) {
            int idx = node.list->chars.find(c);
            if (idx >= 0) {
                node.list->children[idx].store(nullptr);
                node.list->chars.remove_at(idx);

                int count = node.list->chars.count();
                for (int i = idx; i < count; ++i) {
                    node.list->children[i].store(node.list->children[i + 1].load());
                }
                node.list->children[count].store(nullptr);
            }

            if (node.list->chars.count() == 0) {
                result.subtree_deleted = true;
                result.old_nodes.push_back(node.raw);
            } else {
                result.in_place = true;
            }
        } else if (node.is_full()) {
            node.full->valid.template atomic_clear<THREADED>(c);
            node.full->children[c].store(nullptr);

            int count = node.full->valid.count();

            if (count == 0) {
                result.subtree_deleted = true;
                result.old_nodes.push_back(node.raw);
            } else if (count <= LIST_MAX) {
                return convert_full_to_list_fixed_len(builder, node, result);
            } else {
                result.in_place = true;
            }
        }
        return result;
    }

    static result_t& rebuild_with_new_child_fixed_len(builder_t& builder, interior_ptr node, unsigned char c,
                                                       interior_ptr new_child, result_t& result) {
        static_assert(!VAR_LEN);
        const std::string& skip = int_acc::get_skip(node);

        interior_ptr new_node;

        if (node.is_list()) {
            new_node = builder.build_interior_list(skip);
            new_node.list->chars = node.list->chars;
            int idx = node.list->chars.find(c);
            for (int i = 0; i < node.list->chars.count(); ++i) {
                if (i == idx) {
                    new_node.list->children[i].store(new_child);
                } else {
                    new_node.list->children[i].store(node.list->children[i].load());
                }
            }
        } else {
            new_node = builder.build_interior_full(skip);
            new_node.full->valid = node.full->valid;
            for (int i = 0; i < 256; ++i) {
                if (node.full->valid.test(static_cast<unsigned char>(i))) {
                    if (static_cast<unsigned char>(i) == c) {
                        new_node.full->children[i].store(new_child);
                    } else {
                        new_node.full->children[i].store(node.full->children[i].load());
                    }
                }
            }
        }

        result.new_nodes.push_back(new_node.raw);
        result.new_subtree = new_node;
        result.old_nodes.push_back(node.raw);
        return result;
    }

    static result_t& convert_full_to_list_fixed_len(builder_t& builder, interior_ptr node, result_t& result) {
        static_assert(!VAR_LEN);
        const std::string& skip = int_acc::get_skip(node);

        interior_ptr list = builder.build_interior_list(skip);

        for (int i = 0; i < 256; ++i) {
            if (node.full->valid.test(static_cast<unsigned char>(i))) {
                int idx = list.list->chars.add(static_cast<unsigned char>(i));
                list.list->children[idx].store(node.full->children[i].load());
            }
        }

        result.new_nodes.push_back(list.raw);
        result.new_subtree = list;
        result.old_nodes.push_back(node.raw);
        return result;
    }
};

}  // namespace gteitelbaum
