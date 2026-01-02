#pragma once

#include <string_view>
#include <vector>
#include <utility>

#include "tktrie_defines.h"
#include "tktrie_help_common.h"

namespace gteitelbaum {

/**
 * Insert operation results
 * 
 * Atomic slot update approach:
 * - new_subtree: the newly built node/subtree to install
 * - target_slot: the single slot to atomically update (null = update root)
 * - expected_ptr: expected current value in target_slot for verification
 * - old_nodes: only the nodes being replaced (NOT ancestors - they stay in place)
 */
template <bool THREADED>
struct insert_result {
    slot_type_t<THREADED>* new_subtree = nullptr;   // What to install
    slot_type_t<THREADED>* target_slot = nullptr;   // Where to install (null = root)
    uint64_t expected_ptr = 0;                       // Expected value in target_slot
    std::vector<slot_type_t<THREADED>*> new_nodes;
    std::vector<slot_type_t<THREADED>*> old_nodes;   // Only replaced nodes, not ancestors
    bool already_exists = false;
    bool hit_write = false;
    bool hit_read = false;
    
    insert_result() {
        new_nodes.reserve(16);
        old_nodes.reserve(16);
    }
};

/**
 * Insert helper functions - atomic slot update approach
 * 
 * Key insight: We only replace the node where modification happens.
 * Ancestor nodes stay in place - we just atomically update their child slot.
 */
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct insert_helpers : trie_helpers<T, THREADED, Allocator, FIXED_LEN> {
    using base = trie_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using slot_type = typename base::slot_type;
    using node_view_t = typename base::node_view_t;
    using node_builder_t = typename base::node_builder_t;
    using dataptr_t = typename base::dataptr_t;
    using result_t = insert_result<THREADED>;

    /**
     * Build insert operation
     * @param builder Node builder
     * @param root Current root node
     * @param key Key to insert
     * @param value Value to insert
     * @return Result with new_subtree and target_slot to update
     */
    template <typename U>
    static result_t build_insert_path(node_builder_t& builder,
                                       slot_type* root,
                                       std::string_view key,
                                       U&& value,
                                       size_t depth = 0) {
        result_t result;
        
        if (!root) {
            // Empty trie - create new root
            // target_slot = null means "update root slot"
            if (key.empty()) {
                result.new_subtree = builder.build_eos(std::forward<U>(value));
            } else {
                result.new_subtree = builder.build_skip_eos(key, std::forward<U>(value));
            }
            result.new_nodes.push_back(result.new_subtree);
            result.target_slot = nullptr;  // Update root
            result.expected_ptr = 0;       // Root was null
            return result;
        }
        
        // Non-empty trie: target_slot = null means update root, expected_ptr = root
        result.expected_ptr = reinterpret_cast<uint64_t>(root);
        return insert_into_node(builder, root, nullptr, result.expected_ptr,
                                key, std::forward<U>(value), depth, result);
    }

    /**
     * Insert into a node
     * @param node Current node
     * @param parent_slot Slot pointing to this node (null = root slot)
     * @param parent_slot_value Current value in parent_slot
     * @param key Remaining key
     * @param value Value to insert
     * @param depth Current depth
     * @param result Output result
     */
    template <typename U>
    static result_t& insert_into_node(node_builder_t& builder,
                                       slot_type* node,
                                       slot_type* parent_slot,
                                       uint64_t parent_slot_value,
                                       std::string_view key,
                                       U&& value,
                                       size_t depth,
                                       result_t& result) {
        node_view_t view(node);
        
        // Handle skip sequence
        if (view.has_skip()) {
            std::string_view skip = view.skip_chars();
            size_t match = base::match_skip(skip, key);
            
            if (match < skip.size() && match < key.size()) {
                // Key diverges within skip - split this node
                return split_skip_diverge(builder, node, parent_slot, parent_slot_value,
                                          key, std::forward<U>(value), depth, match, result);
            } else if (match < skip.size()) {
                // Key is prefix of skip - split this node
                return split_skip_prefix(builder, node, parent_slot, parent_slot_value,
                                         key, std::forward<U>(value), depth, match, result);
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
                    // Add skip_eos - rebuild this node
                    return add_skip_eos(builder, node, parent_slot, parent_slot_value,
                                        std::forward<U>(value), result);
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
            // Add EOS - rebuild this node
            return add_eos(builder, node, parent_slot, parent_slot_value,
                           std::forward<U>(value), result);
        }
        
        // Need to follow or create child
        unsigned char c = static_cast<unsigned char>(key[0]);
        slot_type* child_slot = view.find_child(c);
        
        if (child_slot) {
            // Child exists - follow it
            uint64_t child_ptr = load_slot<THREADED>(child_slot);
            
            if constexpr (THREADED) {
                if (child_ptr & WRITE_BIT) {
                    result.hit_write = true;
                    return result;
                }
                if (child_ptr & READ_BIT) {
                    result.hit_read = true;
                    return result;
                }
                // Double-check slot unchanged
                uint64_t recheck = load_slot<THREADED>(child_slot);
                if (recheck != child_ptr) {
                    result.hit_write = true;
                    return result;
                }
            }
            
            uint64_t clean_ptr = child_ptr & PTR_MASK;
            
            // FIXED_LEN leaf optimization
            if constexpr (FIXED_LEN > 0 && !THREADED) {
                if (depth == FIXED_LEN - 1 && key.size() == 1) {
                    dataptr_t* dp = reinterpret_cast<dataptr_t*>(child_slot);
                    if (dp->has_data()) {
                        result.already_exists = true;
                        return result;
                    }
                    // Set data in existing slot - rebuild parent node
                    return set_leaf_data(builder, node, parent_slot, parent_slot_value,
                                         c, std::forward<U>(value), depth, result);
                }
            }
            
            // Recurse into child
            // Pass child_slot as the parent_slot for the child
            slot_type* child = reinterpret_cast<slot_type*>(clean_ptr);
            return insert_into_node(builder, child, child_slot, child_ptr,
                                    key.substr(1), std::forward<U>(value), depth + 1, result);
        } else {
            // No child exists - add new child (requires rebuilding this node)
            return add_child(builder, node, parent_slot, parent_slot_value,
                             c, key.substr(1), std::forward<U>(value), depth, result);
        }
    }

    // =========================================================================
    // Node modification operations - all rebuild the current node only
    // =========================================================================

    /**
     * Split node where key diverges within skip
     */
    template <typename U>
    static result_t& split_skip_diverge(node_builder_t& builder,
                                         slot_type* node,
                                         slot_type* parent_slot,
                                         uint64_t parent_slot_value,
                                         std::string_view key,
                                         U&& value,
                                         size_t depth,
                                         size_t match,
                                         result_t& result) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        
        std::string_view common = skip.substr(0, match);
        unsigned char old_char = static_cast<unsigned char>(skip[match]);
        unsigned char new_char = static_cast<unsigned char>(key[match]);
        
        // FIXED_LEN leaf optimization
        if constexpr (FIXED_LEN > 0 && !THREADED) {
            if (depth + match == FIXED_LEN - 1) {
                T old_val;
                if (view.has_skip_eos()) {
                    view.skip_eos_data()->try_read(old_val);
                }
                
                small_list lst(old_char, new_char);
                std::vector<uint64_t> children = {0, 0};
                
                slot_type* branch;
                if (common.empty()) {
                    if (view.has_eos()) {
                        T eos_val;
                        view.eos_data()->try_read(eos_val);
                        branch = builder.build_eos_list(std::move(eos_val), lst, children);
                    } else {
                        branch = builder.build_list(lst, children);
                    }
                } else {
                    if (view.has_eos()) {
                        T eos_val;
                        view.eos_data()->try_read(eos_val);
                        branch = builder.build_skip_list(common, lst, children);
                    } else {
                        branch = builder.build_skip_list(common, lst, children);
                    }
                }
                
                node_view_t branch_view(branch);
                int old_idx = lst.offset(old_char) - 1;
                int new_idx = lst.offset(new_char) - 1;
                
                dataptr_t* old_dp = reinterpret_cast<dataptr_t*>(&branch_view.child_ptrs()[old_idx]);
                new (old_dp) dataptr_t();
                old_dp->set(std::move(old_val));
                
                dataptr_t* new_dp = reinterpret_cast<dataptr_t*>(&branch_view.child_ptrs()[new_idx]);
                new (new_dp) dataptr_t();
                new_dp->set(std::forward<U>(value));
                
                result.new_nodes.push_back(branch);
                result.new_subtree = branch;
                result.target_slot = parent_slot;
                result.expected_ptr = parent_slot_value;
                result.old_nodes.push_back(node);
                return result;
            }
        }
        
        // Build node for old suffix
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
            if (view.has_eos()) {
                T eos_val;
                view.eos_data()->try_read(eos_val);
                branch = builder.build_eos_list(std::move(eos_val), lst, children);
            } else {
                branch = builder.build_list(lst, children);
            }
        } else {
            branch = builder.build_skip_list(common, lst, children);
        }
        result.new_nodes.push_back(branch);
        
        result.new_subtree = branch;
        result.target_slot = parent_slot;
        result.expected_ptr = parent_slot_value;
        result.old_nodes.push_back(node);
        return result;
    }

    /**
     * Split node where key is prefix of skip
     */
    template <typename U>
    static result_t& split_skip_prefix(node_builder_t& builder,
                                        slot_type* node,
                                        slot_type* parent_slot,
                                        uint64_t parent_slot_value,
                                        std::string_view /*key*/,
                                        U&& value,
                                        size_t /*depth*/,
                                        size_t match,
                                        result_t& result) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        
        // Build node for rest of skip
        slot_type* suffix_node = clone_with_shorter_skip(builder, node, match + 1);
        result.new_nodes.push_back(suffix_node);
        
        // Build new root with key as data point
        std::string_view prefix = skip.substr(0, match);
        unsigned char c = static_cast<unsigned char>(skip[match]);
        small_list lst;
        lst.insert(0, c);
        std::vector<uint64_t> children = {reinterpret_cast<uint64_t>(suffix_node)};
        
        slot_type* new_node;
        if (prefix.empty()) {
            if (view.has_eos()) {
                result.already_exists = true;
                builder.deallocate_node(suffix_node);
                result.new_nodes.pop_back();
                return result;
            }
            new_node = builder.build_eos_list(std::forward<U>(value), lst, children);
        } else {
            new_node = builder.build_skip_eos_list(prefix, std::forward<U>(value), lst, children);
        }
        result.new_nodes.push_back(new_node);
        
        result.new_subtree = new_node;
        result.target_slot = parent_slot;
        result.expected_ptr = parent_slot_value;
        result.old_nodes.push_back(node);
        return result;
    }

    /**
     * Clone node with shorter skip sequence
     */
    static slot_type* clone_with_shorter_skip(node_builder_t& builder,
                                               slot_type* node,
                                               size_t skip_prefix_len) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        std::string_view new_skip = skip.substr(skip_prefix_len);
        
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        bool has_eos = view.has_skip_eos();
        T eos_val;
        if (has_eos) {
            view.skip_eos_data()->try_read(eos_val);
        }
        
        if (new_skip.empty()) {
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
            if (children.empty()) {
                if (has_eos) {
                    return builder.build_skip_eos(new_skip, std::move(eos_val));
                } else {
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

    /**
     * Add EOS to existing node
     */
    template <typename U>
    static result_t& add_eos(node_builder_t& builder,
                              slot_type* node,
                              slot_type* parent_slot,
                              uint64_t parent_slot_value,
                              U&& value,
                              result_t& result) {
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
        result.new_subtree = new_node;
        result.target_slot = parent_slot;
        result.expected_ptr = parent_slot_value;
        result.old_nodes.push_back(node);
        return result;
    }

    /**
     * Add skip_eos to existing node
     */
    template <typename U>
    static result_t& add_skip_eos(node_builder_t& builder,
                                   slot_type* node,
                                   slot_type* parent_slot,
                                   uint64_t parent_slot_value,
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
        result.new_subtree = new_node;
        result.target_slot = parent_slot;
        result.expected_ptr = parent_slot_value;
        result.old_nodes.push_back(node);
        return result;
    }

    /**
     * Add new child to existing node
     */
    template <typename U>
    static result_t& add_child(node_builder_t& builder,
                                slot_type* node,
                                slot_type* parent_slot,
                                uint64_t parent_slot_value,
                                unsigned char c,
                                std::string_view rest,
                                U&& value,
                                size_t depth,
                                result_t& result) {
        node_view_t view(node);
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        // Determine child structure
        bool is_list;
        small_list lst;
        popcount_bitmap bmp;
        int pos;
        
        if (view.has_list()) {
            lst = view.get_list();
            is_list = true;
            pos = base::insert_child_char(lst, bmp, is_list, c);
        } else if (view.has_pop()) {
            bmp = view.get_bitmap();
            is_list = false;
            pos = base::insert_child_char(lst, bmp, is_list, c);
        } else {
            lst.insert(0, c);
            is_list = true;
            pos = 0;
        }
        
        // FIXED_LEN leaf optimization
        if constexpr (FIXED_LEN > 0 && !THREADED) {
            if (depth == FIXED_LEN - 1 && rest.empty()) {
                children.insert(children.begin() + pos, 0);
                chars.insert(chars.begin() + pos, c);
                
                slot_type* new_parent = base::rebuild_node(builder, view, is_list, lst, bmp, children);
                
                node_view_t new_view(new_parent);
                slot_type* new_child_slot = new_view.find_child(c);
                dataptr_t* dp = reinterpret_cast<dataptr_t*>(new_child_slot);
                new (dp) dataptr_t();
                dp->set(std::forward<U>(value));
                
                result.new_nodes.push_back(new_parent);
                result.new_subtree = new_parent;
                result.target_slot = parent_slot;
                result.expected_ptr = parent_slot_value;
                result.old_nodes.push_back(node);
                return result;
            }
        }
        
        // Build new child node
        slot_type* child;
        if (rest.empty()) {
            child = builder.build_eos(std::forward<U>(value));
        } else {
            child = builder.build_skip_eos(rest, std::forward<U>(value));
        }
        result.new_nodes.push_back(child);
        
        children.insert(children.begin() + pos, reinterpret_cast<uint64_t>(child));
        chars.insert(chars.begin() + pos, c);
        
        // Rebuild parent with new child
        slot_type* new_parent = base::rebuild_node(builder, view, is_list, lst, bmp, children);
        result.new_nodes.push_back(new_parent);
        
        result.new_subtree = new_parent;
        result.target_slot = parent_slot;
        result.expected_ptr = parent_slot_value;
        result.old_nodes.push_back(node);
        return result;
    }

    /**
     * Set data in leaf slot (FIXED_LEN non-threaded only)
     */
    template <typename U>
    static result_t& set_leaf_data(node_builder_t& builder,
                                    slot_type* node,
                                    slot_type* parent_slot,
                                    uint64_t parent_slot_value,
                                    unsigned char c,
                                    U&& value,
                                    size_t /*depth*/,
                                    result_t& result) {
        node_view_t view(node);
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        slot_type* new_node = base::rebuild_node(builder, view, is_list, lst, bmp, children);
        
        node_view_t new_view(new_node);
        slot_type* child_slot = new_view.find_child(c);
        dataptr_t* dp = reinterpret_cast<dataptr_t*>(child_slot);
        new (dp) dataptr_t();
        dp->set(std::forward<U>(value));
        
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.target_slot = parent_slot;
        result.expected_ptr = parent_slot_value;
        result.old_nodes.push_back(node);
        return result;
    }
};

}  // namespace gteitelbaum
