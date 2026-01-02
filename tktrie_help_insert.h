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
    slot_type_t<THREADED>* target_slot = nullptr;
    uint64_t expected_ptr = 0;
    std::vector<slot_type_t<THREADED>*> new_nodes;
    std::vector<slot_type_t<THREADED>*> old_nodes;
    bool already_exists = false;
    
    insert_result() { new_nodes.reserve(16); old_nodes.reserve(16); }
    
    bool path_has_conflict() const noexcept {
        if constexpr (THREADED) {
            if (target_slot) {
                uint64_t current = load_slot<THREADED>(target_slot);
                if (current != expected_ptr) return true;
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
        result.target_slot = root_slot;
        result.expected_ptr = reinterpret_cast<uint64_t>(root);
        
        if (!root) {
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
        
        return insert_into_node(builder, root, key, std::forward<U>(value), depth, result);
    }

    template <typename U>
    static result_t& insert_into_node(node_builder_t& builder, slot_type* node, std::string_view key,
                                       U&& value, size_t depth, result_t& result) {
        node_view_t view(node);
        
        if (view.has_skip()) {
            std::string_view skip = view.skip_chars();
            size_t match = base::match_skip(skip, key);
            
            if (match < skip.size() && match < key.size()) {
                return split_skip_diverge(builder, node, key, std::forward<U>(value), depth, match, result);
            } else if (match < skip.size()) {
                return split_skip_prefix(builder, node, key, std::forward<U>(value), depth, match, result);
            } else {
                key.remove_prefix(match);
                depth += match;
                if (key.empty()) {
                    if (view.skip_eos_data()->has_data()) {
                        result.already_exists = true;
                        return result;
                    }
                    return set_skip_eos(builder, node, std::forward<U>(value), result);
                }
            }
        }
        
        if (key.empty()) {
            if (view.eos_data()->has_data()) {
                result.already_exists = true;
                return result;
            }
            return set_eos(builder, node, std::forward<U>(value), result);
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        slot_type* child_slot = view.find_child(c);
        
        if (child_slot) {
            if (view.has_leaf()) {
                if (key.size() == 1) {
                    result.already_exists = true;  // Leaf key exists
                    return result;
                }
                // Key too long - need to convert leaf to internal? 
                // This shouldn't happen with FIXED_LEN correctly used
                KTRIE_DEBUG_ASSERT(false && "Key longer than FIXED_LEN at leaf");
                return result;
            }
            
            uint64_t child_ptr = load_slot<THREADED>(child_slot);
            if (child_ptr == 0) {
                return add_child(builder, node, c, key.substr(1), std::forward<U>(value), depth, result);
            }
            
            slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
            insert_into_node(builder, child, key.substr(1), std::forward<U>(value), depth + 1, result);
            
            if (!result.already_exists && result.new_subtree) {
                return rebuild_with_new_child(builder, node, c, result);
            }
            return result;
        } else {
            return add_child(builder, node, c, key.substr(1), std::forward<U>(value), depth, result);
        }
    }

    static result_t& rebuild_with_new_child(node_builder_t& builder, slot_type* node, unsigned char c, result_t& result) {
        node_view_t view(node);
        auto chars = base::get_child_chars(view);
        auto children = base::extract_children(view);
        
        int idx = base::find_char_index(chars, c);
        if (idx >= 0) {
            if (view.has_full()) {
                children[c] = reinterpret_cast<uint64_t>(result.new_subtree);
            } else {
                children[idx] = reinterpret_cast<uint64_t>(result.new_subtree);
            }
        }
        
        auto [node_type, lst, bmp] = base::build_child_structure(chars);
        
        if (node_type == 2 && !view.has_full()) {
            // Expand to FULL
            std::vector<uint64_t> full_children(256, 0);
            for (size_t i = 0; i < chars.size(); ++i) full_children[chars[i]] = children[i];
            children = std::move(full_children);
        }
        
        slot_type* new_node = base::rebuild_node(builder, view, node_type, lst, bmp, children);
        result.new_nodes.push_back(new_node);
        result.old_nodes.push_back(node);
        result.new_subtree = new_node;
        return result;
    }

    template <typename U>
    static result_t& split_skip_diverge(node_builder_t& builder, slot_type* node, std::string_view key,
                                         U&& value, size_t depth, size_t match, result_t& result) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        std::string_view common = skip.substr(0, match);
        unsigned char old_char = static_cast<unsigned char>(skip[match]);
        unsigned char new_char = static_cast<unsigned char>(key[match]);
        
        // Suffix node for old path
        slot_type* old_suffix = clone_with_shorter_skip(builder, node, match + 1);
        result.new_nodes.push_back(old_suffix);
        
        // New key suffix node
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
        
        // Branch node
        small_list lst(old_char, new_char);
        std::vector<uint64_t> children;
        if (old_char < new_char) {
            children = {reinterpret_cast<uint64_t>(old_suffix), reinterpret_cast<uint64_t>(new_suffix_node)};
        } else {
            children = {reinterpret_cast<uint64_t>(new_suffix_node), reinterpret_cast<uint64_t>(old_suffix)};
        }
        
        slot_type* branch = common.empty() ? builder.build_list(lst, children) 
                                           : builder.build_skip_list(common, lst, children);
        
        // Copy EOS from original
        node_view_t bv(branch);
        bv.eos_data()->deep_copy_from(*view.eos_data());
        
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
        if (prefix.empty()) {
            nv.eos_data()->set(std::forward<U>(value));
        } else {
            nv.skip_eos_data()->set(std::forward<U>(value));
        }
        // Copy original EOS
        if (!prefix.empty()) nv.eos_data()->deep_copy_from(*view.eos_data());
        
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
        if (children.empty()) {
            new_node = new_skip.empty() ? builder.build_empty() : builder.build_skip(new_skip);
        } else {
            auto [node_type, lst, bmp] = base::build_child_structure(chars);
            if (node_type == 2) {
                std::vector<uint64_t> full_children(256, 0);
                for (size_t i = 0; i < chars.size(); ++i) full_children[chars[i]] = children[i];
                new_node = new_skip.empty() ? builder.build_full(full_children)
                                            : builder.build_skip_full(new_skip, full_children);
            } else if (node_type == 1) {
                new_node = new_skip.empty() ? builder.build_pop(bmp, children)
                                            : builder.build_skip_pop(new_skip, bmp, children);
            } else {
                new_node = new_skip.empty() ? builder.build_list(lst, children)
                                            : builder.build_skip_list(new_skip, lst, children);
            }
        }
        
        node_view_t nv(new_node);
        // Move skip_eos to eos if new_skip empty, else to skip_eos
        if (view.skip_eos_data()->has_data()) {
            if (new_skip.empty()) {
                nv.eos_data()->deep_copy_from(*view.skip_eos_data());
            } else {
                nv.skip_eos_data()->deep_copy_from(*view.skip_eos_data());
            }
        }
        
        return new_node;
    }

    template <typename U>
    static result_t& set_eos(node_builder_t& builder, slot_type* node, U&& value, result_t& result) {
        node_view_t view(node);
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        auto [node_type, lst, bmp] = base::build_child_structure(chars);
        
        if (node_type == 2 && !view.has_full()) {
            std::vector<uint64_t> full_children(256, 0);
            for (size_t i = 0; i < chars.size(); ++i) full_children[chars[i]] = children[i];
            children = std::move(full_children);
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
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        auto [node_type, lst, bmp] = base::build_child_structure(chars);
        
        if (node_type == 2 && !view.has_full()) {
            std::vector<uint64_t> full_children(256, 0);
            for (size_t i = 0; i < chars.size(); ++i) full_children[chars[i]] = children[i];
            children = std::move(full_children);
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
        auto old_children = base::extract_children(view);
        auto old_chars = base::get_child_chars(view);
        
        std::vector<unsigned char> new_chars = old_chars;
        new_chars.push_back(c);
        auto [node_type, lst, bmp] = base::build_child_structure(new_chars);
        
        // Check if this should be a LEAF node (FIXED_LEN and at leaf depth)
        bool make_leaf = (FIXED_LEN > 0) && can_embed_leaf_v<T> && 
                         (depth + (view.has_skip() ? view.skip_length() : 0) == FIXED_LEN - 1) && rest.empty();
        
        if (make_leaf) {
            // Build or expand LEAF node
            std::vector<T> values;
            if (view.has_leaf()) {
                values = base::extract_leaf_values(view);
            } else {
                // Converting from non-leaf - shouldn't have children with data
                values.resize(old_chars.size());
            }
            
            if (node_type == 2) {
                // LEAF|FULL
                std::vector<T> full_values(256);
                popcount_bitmap valid_bmp;
                for (size_t i = 0; i < old_chars.size(); ++i) {
                    full_values[old_chars[i]] = values[i];
                    valid_bmp.set(old_chars[i]);
                }
                full_values[c] = std::forward<U>(value);
                valid_bmp.set(c);
                
                slot_type* new_node = view.has_skip() 
                    ? builder.build_skip_leaf_full(view.skip_chars(), valid_bmp, full_values)
                    : builder.build_leaf_full(valid_bmp, full_values);
                node_view_t nv(new_node);
                nv.eos_data()->deep_copy_from(*view.eos_data());
                if (view.has_skip()) nv.skip_eos_data()->deep_copy_from(*view.skip_eos_data());
                
                result.new_nodes.push_back(new_node);
                result.new_subtree = new_node;
                result.old_nodes.push_back(node);
                return result;
            } else {
                values.push_back(std::forward<U>(value));
                slot_type* new_node;
                if (node_type == 1) {
                    new_node = view.has_skip() ? builder.build_skip_leaf_pop(view.skip_chars(), bmp, values)
                                               : builder.build_leaf_pop(bmp, values);
                } else {
                    new_node = view.has_skip() ? builder.build_skip_leaf_list(view.skip_chars(), lst, values)
                                               : builder.build_leaf_list(lst, values);
                }
                node_view_t nv(new_node);
                nv.eos_data()->deep_copy_from(*view.eos_data());
                if (view.has_skip()) nv.skip_eos_data()->deep_copy_from(*view.skip_eos_data());
                
                result.new_nodes.push_back(new_node);
                result.new_subtree = new_node;
                result.old_nodes.push_back(node);
                return result;
            }
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
            new_children.resize(256, 0);
            if (view.has_full()) {
                for (int i = 0; i < 256; ++i) new_children[i] = old_children[i];
            } else {
                for (size_t i = 0; i < old_chars.size(); ++i) new_children[old_chars[i]] = old_children[i];
            }
            new_children[c] = reinterpret_cast<uint64_t>(child_node);
        } else if (node_type == 1) {
            int pos = bmp.index_of(c);
            new_children = old_children;
            new_children.insert(new_children.begin() + pos, reinterpret_cast<uint64_t>(child_node));
        } else {
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
