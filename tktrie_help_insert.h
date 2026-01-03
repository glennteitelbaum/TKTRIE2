#pragma once

#include <string_view>
#include <vector>
#include <utility>

#include "tktrie_defines.h"
#include "tktrie_help_common.h"

namespace gteitelbaum {

template <bool THREADED>
struct insert_result {
    slot_type_t<THREADED>* new_subtree = nullptr;
    slot_type_t<THREADED>* target_slot = nullptr;    // Slot to CAS (parent's child slot or root_slot)
    uint64_t expected_ptr = 0;
    std::vector<slot_type_t<THREADED>*> new_nodes;
    std::vector<slot_type_t<THREADED>*> old_nodes;
    bool already_exists = false;
    bool in_place = false;  // True if update was done atomically in-place

    insert_result() { new_nodes.reserve(16); old_nodes.reserve(16); }

    bool path_has_conflict() const noexcept {
        if (in_place) return false;
        if constexpr (THREADED) {
            if (target_slot) {
                uint64_t cur = load_slot<THREADED>(target_slot);
                if (cur != expected_ptr) return true;
            }
        }
        return false;
    }
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct insert_helpers : trie_helpers<T, THREADED, Allocator, FIXED_LEN> {
    using base = trie_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using slot_type = typename base::slot_type;
    using node_view_t = typename base::node_view_t;
    using node_builder_t = typename base::node_builder_t;
    using dataptr_t = typename base::dataptr_t;
    using result_t = insert_result<THREADED>;

    template <typename U>
    static result_t build_insert_path(node_builder_t& builder, slot_type* root_slot, slot_type* root,
                                       std::string_view key, U&& value, size_t depth = 0) {
        result_t result;

        if (!root) {
            // Empty trie - create new root, target root_slot
            result.target_slot = root_slot;
            result.expected_ptr = 0;
            slot_type* new_node;
            if (key.empty()) {
                new_node = builder.build_empty();
                node_view_t view(new_node);
                view.eos_data()->set(std::forward<U>(value));
            } else {
                new_node = builder.build_skip(key);
                node_view_t view(new_node);
                view.skip_eos_data()->set(std::forward<U>(value));
            }
            result.new_nodes.push_back(new_node);
            result.new_subtree = new_node;
            return result;
        }

        // Traverse with parent tracking
        return insert_with_parent(builder, root_slot, root, nullptr, key, std::forward<U>(value), depth, result);
    }

    // parent_slot: the slot in parent that points to `node`, or root_slot if node is root
    template <typename U>
    static result_t& insert_with_parent(node_builder_t& builder, slot_type* parent_slot, slot_type* node,
                                         slot_type* parent_node, std::string_view key, U&& value,
                                         size_t depth, result_t& result) {
        node_view_t view(node);

        if (view.has_skip()) {
            std::string_view skip = view.skip_chars();
            size_t match = base::match_skip(skip, key);

            if (match < skip.size() && match < key.size()) {
                // Diverge in skip - need to split this node
                result.target_slot = parent_slot;
                result.expected_ptr = reinterpret_cast<uint64_t>(node);
                return split_skip_diverge(builder, node, key, std::forward<U>(value), depth, match, result);
            } else if (match < skip.size()) {
                // Key is prefix of skip - split
                result.target_slot = parent_slot;
                result.expected_ptr = reinterpret_cast<uint64_t>(node);
                return split_skip_prefix(builder, node, key, std::forward<U>(value), depth, match, result);
            } else {
                key.remove_prefix(match);
                depth += match;
                if (key.empty()) {
                    // Set skip_eos
                    if (view.has_leaf()) {
                        if (view.leaf_has_eos()) {
                            dataptr_t* dp = view.has_skip() ? view.skip_eos_data() : view.eos_data();
                            if (dp->has_data()) { result.already_exists = true; return result; }
                        }
                    } else {
                        if (view.skip_eos_data()->has_data()) { result.already_exists = true; return result; }
                        // In-place update only safe for non-THREADED mode
                        if constexpr (!THREADED) {
                            view.skip_eos_data()->set(std::forward<U>(value));
                            result.in_place = true;
                            return result;
                        }
                    }
                    result.target_slot = parent_slot;
                    result.expected_ptr = reinterpret_cast<uint64_t>(node);
                    return set_skip_eos(builder, node, std::forward<U>(value), result);
                }
            }
        }

        if (key.empty()) {
            // Set EOS
            if (view.has_leaf()) {
                if (view.leaf_has_eos()) {
                    if (view.eos_data()->has_data()) { result.already_exists = true; return result; }
                }
            } else {
                if (view.eos_data()->has_data()) { result.already_exists = true; return result; }
                // In-place update only safe for non-THREADED mode
                if constexpr (!THREADED) {
                    view.eos_data()->set(std::forward<U>(value));
                    result.in_place = true;
                    return result;
                }
            }
            result.target_slot = parent_slot;
            result.expected_ptr = reinterpret_cast<uint64_t>(node);
            return set_eos(builder, node, std::forward<U>(value), result);
        }

        // Need to follow or add child
        unsigned char c = static_cast<unsigned char>(key[0]);
        slot_type* child_slot = view.find_child(c);

        if (child_slot) {
            if (view.has_leaf()) {
                // LEAF node: check if character actually exists
                if (key.size() == 1) {
                    bool exists = false;
                    if (view.has_full()) {
                        exists = view.leaf_full_test_bit(c);
                    } else if (view.has_list()) {
                        exists = true;  // find_child only returns slot if in list
                    } else if (view.has_pop()) {
                        exists = true;  // find_child only returns slot if in bitmap
                    }
                    if (exists) { result.already_exists = true; return result; }
                    // Character doesn't exist - add it
                    result.target_slot = parent_slot;
                    result.expected_ptr = reinterpret_cast<uint64_t>(node);
                    return add_child(builder, node, c, key.substr(1), std::forward<U>(value), depth, result);
                }
                KTRIE_DEBUG_ASSERT(false && "Key longer than FIXED_LEN at leaf");
                return result;
            }

            uint64_t child_ptr = load_slot<THREADED>(child_slot);
            if (child_ptr == 0) {
                // Empty slot (deleted child) - add child here
                result.target_slot = parent_slot;
                result.expected_ptr = reinterpret_cast<uint64_t>(node);
                return add_child(builder, node, c, key.substr(1), std::forward<U>(value), depth, result);
            }

            // Recurse into child - child_slot becomes new parent_slot
            slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
            return insert_with_parent(builder, child_slot, child, node, key.substr(1), std::forward<U>(value), depth + 1, result);
        } else {
            // Need to add new child
            result.target_slot = parent_slot;
            result.expected_ptr = reinterpret_cast<uint64_t>(node);
            return add_child(builder, node, c, key.substr(1), std::forward<U>(value), depth, result);
        }
    }

    template <typename U>
    static result_t& split_skip_diverge(node_builder_t& builder, slot_type* node, std::string_view key,
                                         U&& value, size_t depth, size_t match, result_t& result) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        std::string_view common = skip.substr(0, match);
        unsigned char old_char = static_cast<unsigned char>(skip[match]);
        unsigned char new_char = static_cast<unsigned char>(key[match]);

        slot_type* old_suffix = clone_with_shorter_skip(builder, node, match + 1);
        result.new_nodes.push_back(old_suffix);

        std::string_view new_suffix = key.substr(match + 1);
        slot_type* new_suffix_node;
        if (new_suffix.empty()) {
            new_suffix_node = builder.build_empty();
            node_view_t nv(new_suffix_node);
            nv.eos_data()->set(std::forward<U>(value));
        } else {
            new_suffix_node = builder.build_skip(new_suffix);
            node_view_t nv(new_suffix_node);
            nv.skip_eos_data()->set(std::forward<U>(value));
        }
        result.new_nodes.push_back(new_suffix_node);

        small_list lst(old_char, new_char);
        std::vector<uint64_t> children;
        if (old_char < new_char)
            children = {reinterpret_cast<uint64_t>(old_suffix), reinterpret_cast<uint64_t>(new_suffix_node)};
        else
            children = {reinterpret_cast<uint64_t>(new_suffix_node), reinterpret_cast<uint64_t>(old_suffix)};

        slot_type* branch = common.empty() ? builder.build_list(lst, children)
                                           : builder.build_skip_list(common, lst, children);

        node_view_t bv(branch);
        if (!view.has_leaf()) bv.eos_data()->deep_copy_from(*view.eos_data());

        result.new_nodes.push_back(branch);
        result.new_subtree = branch;
        result.old_nodes.push_back(node);
        (void)depth;
        return result;
    }

    template <typename U>
    static result_t& split_skip_prefix(node_builder_t& builder, slot_type* node, std::string_view /*key*/,
                                        U&& value, size_t /*depth*/, size_t match, result_t& result) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        std::string_view prefix = skip.substr(0, match);
        unsigned char c = static_cast<unsigned char>(skip[match]);

        slot_type* suffix_node = clone_with_shorter_skip(builder, node, match + 1);
        result.new_nodes.push_back(suffix_node);

        small_list lst;
        lst.add(c);
        std::vector<uint64_t> children = {reinterpret_cast<uint64_t>(suffix_node)};

        slot_type* new_node = prefix.empty() ? builder.build_list(lst, children)
                                             : builder.build_skip_list(prefix, lst, children);

        node_view_t nv(new_node);
        if (prefix.empty()) nv.eos_data()->set(std::forward<U>(value));
        else nv.skip_eos_data()->set(std::forward<U>(value));
        if (!prefix.empty() && !view.has_leaf()) nv.eos_data()->deep_copy_from(*view.eos_data());

        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    static slot_type* clone_with_shorter_skip(node_builder_t& builder, slot_type* node, size_t skip_prefix_len) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        std::string_view new_skip = skip.substr(skip_prefix_len);
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);

        slot_type* new_node;
        if (view.has_leaf()) {
            if (view.leaf_has_eos()) {
                // Terminal becomes terminal
                new_node = new_skip.empty() ? builder.build_leaf_terminal()
                                            : builder.build_leaf_skip_terminal(new_skip);
            } else {
                auto values = base::extract_leaf_values(view);
                auto [node_type, lst, bmp] = base::build_child_structure(chars);
                if (node_type == 2) {
                    std::vector<T> full(256);
                    popcount_bitmap valid;
                    for (size_t i = 0; i < chars.size(); ++i) { full[chars[i]] = values[i]; valid.set(chars[i]); }
                    new_node = new_skip.empty() ? builder.build_leaf_full(valid, full)
                                                : builder.build_leaf_skip_full(new_skip, valid, full);
                } else if (node_type == 1) {
                    new_node = new_skip.empty() ? builder.build_leaf_pop(bmp, values)
                                                : builder.build_leaf_skip_pop(new_skip, bmp, values);
                } else {
                    new_node = new_skip.empty() ? builder.build_leaf_list(lst, values)
                                                : builder.build_leaf_skip_list(new_skip, lst, values);
                }
            }
        } else {
            if (children.empty()) {
                new_node = new_skip.empty() ? builder.build_empty() : builder.build_skip(new_skip);
            } else {
                auto [node_type, lst, bmp] = base::build_child_structure(chars);
                if (node_type == 2) {
                    std::vector<uint64_t> full(256, 0);
                    for (size_t i = 0; i < chars.size(); ++i) full[chars[i]] = children[i];
                    new_node = new_skip.empty() ? builder.build_full(full) : builder.build_skip_full(new_skip, full);
                } else if (node_type == 1) {
                    new_node = new_skip.empty() ? builder.build_pop(bmp, children) : builder.build_skip_pop(new_skip, bmp, children);
                } else {
                    new_node = new_skip.empty() ? builder.build_list(lst, children) : builder.build_skip_list(new_skip, lst, children);
                }
            }

            node_view_t nv(new_node);
            // Copy skip_eos to appropriate place
            if (view.skip_eos_data()->has_data()) {
                if (new_skip.empty()) nv.eos_data()->deep_copy_from(*view.skip_eos_data());
                else nv.skip_eos_data()->deep_copy_from(*view.skip_eos_data());
            }
        }
        return new_node;
    }

    template <typename U>
    static result_t& set_eos(node_builder_t& builder, slot_type* node, U&& value, result_t& result) {
        node_view_t view(node);

        if (view.has_leaf()) {
            // LEAF: need to rebuild as terminal
            slot_type* new_node;
            if (view.has_skip()) {
                new_node = builder.build_leaf_skip_terminal(view.skip_chars());
                node_view_t nv(new_node);
                // EOS goes to skip_eos for LEAF|SKIP terminal? No, actually for LEAF terminal without skip, EOS at offset 1
                // With skip: LEAF|SKIP|LIST|POP => skip_eos is where data goes
                // For set_eos with key.empty() and !skip: data goes to eos_data
                // But if has_skip, we already handled in set_skip_eos...
                // set_eos is called when key is empty AFTER consuming skip. So skip_eos.
                // Wait no - set_eos is called when key is empty at the node, skip already consumed
                // If view has skip and key empty after skip -> that's set_skip_eos
                // set_eos: no skip or skip already consumed and key empty at node
                // For LEAF terminal: if no skip, eos_data. 
                nv.skip_eos_data()->set(std::forward<U>(value));
            } else {
                new_node = builder.build_leaf_terminal();
                node_view_t nv(new_node);
                nv.eos_data()->set(std::forward<U>(value));
            }
            result.new_nodes.push_back(new_node);
            result.new_subtree = new_node;
            result.old_nodes.push_back(node);
            return result;
        }

        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        auto [node_type, lst, bmp] = base::build_child_structure(chars);

        if (node_type == 2 && !view.has_full()) {
            std::vector<uint64_t> full(256, 0);
            for (size_t i = 0; i < chars.size(); ++i) full[chars[i]] = children[i];
            children = std::move(full);
        }

        slot_type* new_node = base::rebuild_node(builder, view, node_type, lst, bmp, children);
        node_view_t nv(new_node);
        nv.eos_data()->set(std::forward<U>(value));

        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    template <typename U>
    static result_t& set_skip_eos(node_builder_t& builder, slot_type* node, U&& value, result_t& result) {
        node_view_t view(node);
        KTRIE_DEBUG_ASSERT(view.has_skip());

        if (view.has_leaf()) {
            // Must be terminal or add terminal
            slot_type* new_node = builder.build_leaf_skip_terminal(view.skip_chars());
            node_view_t nv(new_node);
            nv.skip_eos_data()->set(std::forward<U>(value));
            result.new_nodes.push_back(new_node);
            result.new_subtree = new_node;
            result.old_nodes.push_back(node);
            return result;
        }

        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        auto [node_type, lst, bmp] = base::build_child_structure(chars);

        if (node_type == 2 && !view.has_full()) {
            std::vector<uint64_t> full(256, 0);
            for (size_t i = 0; i < chars.size(); ++i) full[chars[i]] = children[i];
            children = std::move(full);
        }

        slot_type* new_node = base::rebuild_node(builder, view, node_type, lst, bmp, children);
        node_view_t nv(new_node);
        nv.skip_eos_data()->set(std::forward<U>(value));

        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    template <typename U>
    static result_t& add_child(node_builder_t& builder, slot_type* node, unsigned char c,
                                std::string_view rest, U&& value, size_t depth, result_t& result) {
        node_view_t view(node);
        
        size_t effective_depth = depth + (view.has_skip() ? view.skip_length() : 0);
        bool at_leaf_depth = (FIXED_LEN > 0) && can_embed_leaf_v<T> && (effective_depth == FIXED_LEN - 1) && rest.empty();

        // OPTIMIZATION: In-place update for LEAF|FULL (only non-THREADED - no race on bitmap)
        if constexpr (!THREADED) {
            if (at_leaf_depth && view.has_leaf() && view.has_full()) {
                view.set_leaf_value(static_cast<int>(c), std::forward<U>(value));
                view.leaf_full_set_bit(c);
                result.in_place = true;
                return result;
            }
        }

        auto old_children = base::extract_children(view);
        auto old_chars = base::get_child_chars(view);

        std::vector<unsigned char> new_chars = old_chars;
        new_chars.push_back(c);
        auto [node_type, lst, bmp] = base::build_child_structure(new_chars);

        if (at_leaf_depth) {
            std::vector<T> values;
            if (view.has_leaf() && view.leaf_has_children()) {
                values = base::extract_leaf_values(view);
            } else {
                // Non-LEAF node: extract values from child nodes' eos/skip_eos data
                values.reserve(old_chars.size());
                for (size_t i = 0; i < old_children.size(); ++i) {
                    T val{};
                    if (old_children[i]) {
                        node_view_t child(reinterpret_cast<slot_type*>(old_children[i]));
                        if (!child.has_leaf()) {
                            if (child.has_skip() && child.skip_eos_data()->has_data()) {
                                child.skip_eos_data()->try_read(val);
                            } else if (child.eos_data()->has_data()) {
                                child.eos_data()->try_read(val);
                            }
                        }
                        // Add old child nodes to be deallocated
                        result.old_nodes.push_back(reinterpret_cast<slot_type*>(old_children[i]));
                    }
                    values.push_back(val);
                }
            }
            values.push_back(std::forward<U>(value));

            slot_type* new_node;
            bool has_skip = view.has_skip();
            std::string_view skip = has_skip ? view.skip_chars() : std::string_view{};

            if (node_type == 2) {
                // FULL: direct-indexed by char value
                std::vector<T> full(256);
                popcount_bitmap valid;
                for (size_t i = 0; i < new_chars.size() - 1; ++i) { full[new_chars[i]] = values[i]; valid.set(new_chars[i]); }
                full[c] = values.back();
                valid.set(c);
                new_node = has_skip ? builder.build_leaf_skip_full(skip, valid, full)
                                    : builder.build_leaf_full(valid, full);
            } else if (node_type == 1) {
                // POP: values in bitmap order (sorted by char value)
                std::vector<T> sorted_values(bmp.count());
                for (size_t i = 0; i < new_chars.size() - 1; ++i) {
                    int pos = bmp.index_of(new_chars[i]);
                    sorted_values[pos] = values[i];
                }
                int new_pos = bmp.index_of(c);
                sorted_values[new_pos] = values.back();
                new_node = has_skip ? builder.build_leaf_skip_pop(skip, bmp, sorted_values)
                                    : builder.build_leaf_pop(bmp, sorted_values);
            } else {
                // LIST: unsorted
                new_node = has_skip ? builder.build_leaf_skip_list(skip, lst, values)
                                    : builder.build_leaf_list(lst, values);
            }

            result.new_nodes.push_back(new_node);
            result.new_subtree = new_node;
            result.old_nodes.push_back(node);
            return result;
        }

        // Non-leaf: build child node
        slot_type* child_node;
        if (rest.empty()) {
            child_node = builder.build_empty();
            node_view_t cv(child_node);
            cv.eos_data()->set(std::forward<U>(value));
        } else {
            child_node = builder.build_skip(rest);
            node_view_t cv(child_node);
            cv.skip_eos_data()->set(std::forward<U>(value));
        }
        result.new_nodes.push_back(child_node);

        std::vector<uint64_t> new_children;
        if (node_type == 2) {
            // FULL: 256 direct-indexed slots
            new_children.resize(256, 0);
            if (view.has_full()) {
                for (int i = 0; i < 256; ++i) new_children[i] = old_children[i];
            } else {
                for (size_t i = 0; i < old_chars.size(); ++i) new_children[old_chars[i]] = old_children[i];
            }
            new_children[c] = reinterpret_cast<uint64_t>(child_node);
        } else if (node_type == 1) {
            // POP: children in bitmap order (sorted by char value)
            // Must reorder if coming from LIST (unsorted)
            new_children.resize(bmp.count());
            for (size_t i = 0; i < old_chars.size(); ++i) {
                int pos = bmp.index_of(old_chars[i]);
                new_children[pos] = old_children[i];
            }
            int new_pos = bmp.index_of(c);
            new_children[new_pos] = reinterpret_cast<uint64_t>(child_node);
        } else {
            // LIST: unsorted, just append
            new_children = old_children;
            new_children.push_back(reinterpret_cast<uint64_t>(child_node));
        }

        slot_type* new_parent = base::rebuild_node(builder, view, node_type, lst, bmp, new_children);
        result.new_nodes.push_back(new_parent);
        result.new_subtree = new_parent;
        result.old_nodes.push_back(node);
        return result;
    }
};

}  // namespace gteitelbaum
