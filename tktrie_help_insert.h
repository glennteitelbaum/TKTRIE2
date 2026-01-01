#pragma once

#include <string_view>
#include <vector>
#include <utility>

#include "tktrie_defines.h"
#include "tktrie_help_common.h"

namespace gteitelbaum {

/**
 * Insert operation results
 */
template <bool THREADED>
struct insert_result {
    slot_type_t<THREADED>* new_root = nullptr;
    slot_type_t<THREADED>* expected_root = nullptr;
    std::vector<slot_type_t<THREADED>*> new_nodes;
    std::vector<slot_type_t<THREADED>*> old_nodes;
    std::vector<path_step<THREADED>> path;
    bool already_exists = false;
    bool hit_write = false;
    bool hit_read = false;
};

/**
 * Insert helper functions
 */
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct insert_helpers : trie_helpers<T, THREADED, Allocator, FIXED_LEN> {
    using base = trie_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using slot_type = typename base::slot_type;
    using node_view_t = typename base::node_view_t;
    using node_builder_t = typename base::node_builder_t;
    using dataptr_t = typename base::dataptr_t;
    using result_t = insert_result<THREADED>;
    using path_step_t = typename base::path_step_t;

    /**
     * Build new path for insertion
     * Returns new nodes and nodes to delete
     */
    template <typename U>
    static result_t build_insert_path(node_builder_t& builder,
                                       slot_type* root,
                                       std::string_view key,
                                       U&& value,
                                       size_t depth = 0) {
        result_t result;
        result.new_root = nullptr;
        result.expected_root = root;  // Record root we're building against
        result.already_exists = false;
        result.hit_write = false;
        
        if (!root) {
            // Empty trie - create new root with data
            if (key.empty()) {
                result.new_root = builder.build_eos(std::forward<U>(value));
            } else {
                result.new_root = builder.build_skip_eos(key, std::forward<U>(value));
            }
            result.new_nodes.push_back(result.new_root);
            return result;
        }
        
        return insert_into_node(builder, root, key, std::forward<U>(value), depth, result);
    }

    template <typename U>
    static result_t& insert_into_node(node_builder_t& builder,
                                       slot_type* node,
                                       std::string_view key,
                                       U&& value,
                                       size_t depth,
                                       result_t& result) {
        node_view_t view(node);
        (void)view.flags();  // Used implicitly through view methods
        
        // Handle skip sequence
        if (view.has_skip()) {
            std::string_view skip = view.skip_chars();
            size_t match = base::match_skip(skip, key);
            
            if (match < skip.size() && match < key.size()) {
                // Key diverges within skip - need to split
                return split_skip_diverge(builder, node, key, std::forward<U>(value), 
                                          depth, match, result);
            } else if (match < skip.size()) {
                // Key is prefix of skip
                return split_skip_prefix(builder, node, key, std::forward<U>(value),
                                         depth, match, result);
            } else {
                // Skip fully matched
                key.remove_prefix(match);
                depth += match;
                
                if (key.empty()) {
                    // Key ends at skip_eos position
                    if (view.has_skip_eos()) {
                        result.already_exists = true;
                        return result;
                    }
                    // Add skip_eos to existing node
                    return add_skip_eos(builder, node, std::forward<U>(value), result);
                }
            }
        }
        
        // Key continues past skip (or no skip)
        if (key.empty()) {
            // Key ends at this node
            if (view.has_eos()) {
                result.already_exists = true;
                return result;
            }
            // Add EOS to existing node
            return add_eos(builder, node, std::forward<U>(value), result);
        }
        
        // Need to follow or create child
        unsigned char c = static_cast<unsigned char>(key[0]);
        slot_type* child_slot = view.find_child(c);
        
        if (child_slot) {
            // Child exists - recurse
            uint64_t child_ptr = load_slot<THREADED>(child_slot);
            
            if constexpr (THREADED) {
                if (child_ptr & WRITE_BIT) {
                    result.hit_write = true;
                    return result;
                }
                child_ptr &= PTR_MASK;
            }
            
            // NOTE: fixed_len leaf optimization disabled
            // All children are stored as pointers to nodes
            
            slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
            result_t child_result;
            child_result.already_exists = false;
            child_result.hit_write = false;
            
            insert_into_node(builder, child, key.substr(1), std::forward<U>(value),
                            depth + 1, child_result);
            
            if (child_result.already_exists || child_result.hit_write) {
                result.already_exists = child_result.already_exists;
                result.hit_write = child_result.hit_write;
                return result;
            }
            
            // Record path step: this node and the char we descended through
            // Child's path is already recorded, we add our step at the front (root to leaf order)
            result.path.push_back({node, c});
            for (auto& step : child_result.path) {
                result.path.push_back(step);
            }
            
            // Child was modified - need to clone this node with new child pointer
            return clone_with_new_child(builder, node, c, child_result.new_root, 
                                        child_result, result);
        } else {
            // No child - add new child (no path to record - new nodes don't need WRITE_BIT)
            return add_child(builder, node, c, key.substr(1), std::forward<U>(value),
                            depth, result);
        }
    }

    template <typename U>
    static result_t& split_skip_diverge(node_builder_t& builder,
                                         slot_type* node,
                                         std::string_view key,
                                         U&& value,
                                         size_t /*depth*/,
                                         size_t match,
                                         result_t& result) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        
        // Split: common_prefix -> branch(old_suffix, new_suffix)
        std::string_view common = skip.substr(0, match);
        unsigned char old_char = static_cast<unsigned char>(skip[match]);
        unsigned char new_char = static_cast<unsigned char>(key[match]);
        
        // Build node for old suffix (rest of original node)
        slot_type* old_suffix_node = clone_with_shorter_skip(builder, node, match + 1);
        result.new_nodes.push_back(old_suffix_node);
        
        // Build node for new key suffix
        std::string_view new_suffix = key.substr(match + 1);
        slot_type* new_suffix_node;
        if (new_suffix.empty()) {
            new_suffix_node = builder.build_eos(std::forward<U>(value));
        } else {
            new_suffix_node = builder.build_skip_eos(new_suffix, std::forward<U>(value));
        }
        result.new_nodes.push_back(new_suffix_node);
        
        // Build branch node
        small_list lst(old_char, new_char);
        std::vector<uint64_t> children;
        if (old_char < new_char) {
            children = {reinterpret_cast<uint64_t>(old_suffix_node),
                       reinterpret_cast<uint64_t>(new_suffix_node)};
        } else {
            children = {reinterpret_cast<uint64_t>(new_suffix_node),
                       reinterpret_cast<uint64_t>(old_suffix_node)};
        }
        
        slot_type* branch;
        if (common.empty()) {
            // Handle EOS from original node
            if (view.has_eos()) {
                T eos_val;
                view.eos_data()->try_read(eos_val);
                branch = builder.build_eos_list(std::move(eos_val), lst, children);
            } else {
                branch = builder.build_list(lst, children);
            }
        } else {
            // Handle EOS from original node
            if (view.has_eos()) {
                T eos_val;
                view.eos_data()->try_read(eos_val);
                branch = builder.build_skip_list(common, lst, children);
                // Need EOS before skip - this is complex, rebuild differently
                // For now, just use skip_list
            } else {
                branch = builder.build_skip_list(common, lst, children);
            }
        }
        result.new_nodes.push_back(branch);
        result.new_root = branch;
        result.old_nodes.push_back(node);
        
        return result;
    }

    template <typename U>
    static result_t& split_skip_prefix(node_builder_t& builder,
                                        slot_type* node,
                                        std::string_view /*key*/,
                                        U&& value,
                                        size_t /*depth*/,
                                        size_t match,
                                        result_t& result) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        
        // Key is prefix of skip
        // New node: EOS at key end, then skip continues
        // skip[match] becomes edge character, skip[match+1:] becomes new suffix skip
        
        // Build node for rest of skip (skip the edge char at position match)
        slot_type* suffix_node = clone_with_shorter_skip(builder, node, match + 1);
        result.new_nodes.push_back(suffix_node);
        
        // Build new root with key as skip_eos
        std::string_view prefix = skip.substr(0, match);
        if (prefix.empty()) {
            if (view.has_eos()) {
                // Original had EOS, we're adding at same position - shouldn't happen
                result.already_exists = true;
                builder.deallocate_node(suffix_node);
                result.new_nodes.pop_back();
                return result;
            }
            // New data at root, original becomes single child
            unsigned char c = static_cast<unsigned char>(skip[match]);
            small_list lst;
            lst.insert(0, c);
            std::vector<uint64_t> children = {reinterpret_cast<uint64_t>(suffix_node)};
            result.new_root = builder.build_eos_list(std::forward<U>(value), lst, children);
        } else {
            // Prefix skip, then data, then rest
            unsigned char c = static_cast<unsigned char>(skip[match]);
            small_list lst;
            lst.insert(0, c);
            std::vector<uint64_t> children = {reinterpret_cast<uint64_t>(suffix_node)};
            result.new_root = builder.build_skip_eos_list(prefix, std::forward<U>(value), 
                                                          lst, children);
        }
        result.new_nodes.push_back(result.new_root);
        result.old_nodes.push_back(node);
        
        return result;
    }

    static slot_type* clone_with_shorter_skip(node_builder_t& builder,
                                               slot_type* node,
                                               size_t skip_prefix_len) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        std::string_view new_skip = skip.substr(skip_prefix_len);
        
        // Extract existing children
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        bool has_eos = view.has_skip_eos();  // SKIP_EOS becomes EOS
        T eos_val;
        if (has_eos) {
            view.skip_eos_data()->try_read(eos_val);
        }
        
        if (new_skip.empty()) {
            // No more skip
            if (children.empty()) {
                if (has_eos) {
                    return builder.build_eos(std::move(eos_val));
                } else {
                    return builder.build_empty_root();
                }
            }
            
            auto [is_list, lst, bmp] = base::build_child_structure(chars);
            if (has_eos) {
                if (is_list) {
                    return builder.build_eos_list(std::move(eos_val), lst, children);
                } else {
                    return builder.build_eos_pop(std::move(eos_val), bmp, children);
                }
            } else {
                if (is_list) {
                    return builder.build_list(lst, children);
                } else {
                    return builder.build_pop(bmp, children);
                }
            }
        } else {
            // Still has skip
            if (children.empty()) {
                if (has_eos) {
                    return builder.build_skip_eos(new_skip, std::move(eos_val));
                } else {
                    // Skip without data or children - shouldn't happen in valid trie
                    return builder.build_empty_root();
                }
            }
            
            auto [is_list, lst, bmp] = base::build_child_structure(chars);
            if (has_eos) {
                if (is_list) {
                    return builder.build_skip_eos_list(new_skip, std::move(eos_val), lst, children);
                } else {
                    return builder.build_skip_eos_pop(new_skip, std::move(eos_val), bmp, children);
                }
            } else {
                if (is_list) {
                    return builder.build_skip_list(new_skip, lst, children);
                } else {
                    return builder.build_skip_pop(new_skip, bmp, children);
                }
            }
        }
    }

    template <typename U>
    static result_t& add_eos(node_builder_t& builder,
                              slot_type* node,
                              U&& value,
                              result_t& result) {
        // Clone node with EOS flag added
        node_view_t view(node);
        
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        slot_type* new_node;
        
        if (view.has_skip()) {
            std::string_view skip = view.skip_chars();
            if (view.has_skip_eos()) {
                T skip_eos_val;
                view.skip_eos_data()->try_read(skip_eos_val);
                
                if (children.empty()) {
                    new_node = builder.build_eos_skip_eos(std::forward<U>(value), skip, 
                                                          std::move(skip_eos_val));
                } else {
                    auto [is_list, lst, bmp] = base::build_child_structure(chars);
                    if (is_list) {
                        new_node = builder.build_eos_skip_eos_list(std::forward<U>(value), skip,
                                                                   std::move(skip_eos_val), lst, children);
                    } else {
                        new_node = builder.build_eos_skip_eos_pop(std::forward<U>(value), skip,
                                                                  std::move(skip_eos_val), bmp, children);
                    }
                }
            } else {
                // Has skip but no skip_eos
                // EOS should go before skip - need to restructure
                // This case is complex - key ends at node start but there's a skip
                // Shouldn't happen if we handle skip matching correctly above
                new_node = builder.build_eos(std::forward<U>(value));
            }
        } else {
            if (children.empty()) {
                new_node = builder.build_eos(std::forward<U>(value));
            } else {
                auto [is_list, lst, bmp] = base::build_child_structure(chars);
                if (is_list) {
                    new_node = builder.build_eos_list(std::forward<U>(value), lst, children);
                } else {
                    new_node = builder.build_eos_pop(std::forward<U>(value), bmp, children);
                }
            }
        }
        
        result.new_nodes.push_back(new_node);
        result.new_root = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    template <typename U>
    static result_t& add_skip_eos(node_builder_t& builder,
                                   slot_type* node,
                                   U&& value,
                                   result_t& result) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        
        slot_type* new_node;
        
        if (view.has_eos()) {
            T eos_val;
            view.eos_data()->try_read(eos_val);
            
            if (children.empty()) {
                new_node = builder.build_eos_skip_eos(std::move(eos_val), skip, 
                                                      std::forward<U>(value));
            } else {
                if (is_list) {
                    new_node = builder.build_eos_skip_eos_list(std::move(eos_val), skip,
                                                               std::forward<U>(value), lst, children);
                } else {
                    new_node = builder.build_eos_skip_eos_pop(std::move(eos_val), skip,
                                                              std::forward<U>(value), bmp, children);
                }
            }
        } else {
            if (children.empty()) {
                new_node = builder.build_skip_eos(skip, std::forward<U>(value));
            } else {
                if (is_list) {
                    new_node = builder.build_skip_eos_list(skip, std::forward<U>(value), lst, children);
                } else {
                    new_node = builder.build_skip_eos_pop(skip, std::forward<U>(value), bmp, children);
                }
            }
        }
        
        result.new_nodes.push_back(new_node);
        result.new_root = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    template <typename U>
    static result_t& add_child(node_builder_t& builder,
                                slot_type* node,
                                unsigned char c,
                                std::string_view rest,
                                U&& value,
                                size_t /*depth*/,
                                result_t& result) {
        // NOTE: fixed_len leaf optimization disabled for simplicity
        // All children are stored as pointers to nodes
        
        // Build new child node
        slot_type* child;
        if (rest.empty()) {
            child = builder.build_eos(std::forward<U>(value));
        } else {
            child = builder.build_skip_eos(rest, std::forward<U>(value));
        }
        result.new_nodes.push_back(child);
        
        // Clone parent with new child added
        node_view_t view(node);
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        // Insert new char and child
        int pos;
        bool is_list;
        small_list lst;
        popcount_bitmap bmp;
        
        if (view.has_list()) {
            lst = view.get_list();
            is_list = true;
            pos = base::insert_child_char(lst, bmp, is_list, c);
        } else if (view.has_pop()) {
            bmp = view.get_bitmap();
            is_list = false;
            pos = base::insert_child_char(lst, bmp, is_list, c);
        } else {
            // No children yet
            lst.insert(0, c);
            is_list = true;
            pos = 0;
        }
        
        children.insert(children.begin() + pos, reinterpret_cast<uint64_t>(child));
        chars.insert(chars.begin() + pos, c);
        
        // Rebuild parent
        slot_type* new_parent = base::rebuild_node(builder, view, is_list, lst, bmp, children);
        result.new_nodes.push_back(new_parent);
        result.new_root = new_parent;
        result.old_nodes.push_back(node);
        
        return result;
    }

    static result_t& clone_with_new_child(node_builder_t& builder,
                                           slot_type* node,
                                           unsigned char c,
                                           slot_type* new_child_node,
                                           result_t& child_result,
                                           result_t& result) {
        // Merge child result into result
        for (auto* n : child_result.new_nodes) {
            result.new_nodes.push_back(n);
        }
        for (auto* n : child_result.old_nodes) {
            result.old_nodes.push_back(n);
        }
        
        // Clone this node with updated child pointer
        node_view_t view(node);
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        // Find and update child
        int idx = -1;
        if (view.has_list()) {
            idx = view.get_list().offset(c) - 1;
        } else if (view.has_pop()) {
            view.get_bitmap().find(c, &idx);
        }
        
        if (idx >= 0) {
            children[idx] = reinterpret_cast<uint64_t>(new_child_node);
        }
        
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        slot_type* new_node = base::rebuild_node(builder, view, is_list, lst, bmp, children);
        
        result.new_nodes.push_back(new_node);
        result.new_root = new_node;
        result.old_nodes.push_back(node);
        
        return result;
    }
};

}  // namespace gteitelbaum
