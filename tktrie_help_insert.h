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
 * THREADED writer protocol (COW + EBR + mutex):
 * 1. Traverse and build new subtree (EBR protects from freed memory)
 * 2. LOCK mutex
 * 3. Verify root_slot still has expected_ptr
 * 4. Store new_ptr to root_slot
 * 5. UNLOCK
 * 6. Retire old nodes to EBR
 */
template <bool THREADED>
struct insert_result {
    slot_type_t<THREADED>* new_subtree = nullptr;   // What to install
    slot_type_t<THREADED>* target_slot = nullptr;   // Always root_slot for THREADED
    uint64_t expected_ptr = 0;                       // Expected value in target_slot
    std::vector<slot_type_t<THREADED>*> new_nodes;
    std::vector<slot_type_t<THREADED>*> old_nodes;
    bool already_exists = false;
    
    insert_result() {
        new_nodes.reserve(16);
        old_nodes.reserve(16);
    }
    
    // Check if target_slot has been modified (another writer committed)
    bool path_has_conflict() const noexcept {
        if constexpr (THREADED) {
            if (target_slot) {
                uint64_t current = load_slot<THREADED>(target_slot);
                if (current != expected_ptr) {
                    return true;
                }
            }
        }
        return false;
    }
};

/**
 * Insert helper functions - COW approach
 * 
 * With COW + EBR:
 * - Traverse is safe even if another writer commits (EBR protects)
 * - Build new tree optimistically
 * - Verify expected_ptr inside lock to detect concurrent modifications
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
     * For THREADED: always targets root_slot, rebuilds entire path from root
     */
    template <typename U>
    static result_t build_insert_path(node_builder_t& builder,
                                       slot_type* root_slot,
                                       slot_type* root,
                                       std::string_view key,
                                       U&& value,
                                       size_t depth = 0) {
        result_t result;
        
        // Always target root_slot for THREADED mode
        result.target_slot = root_slot;
        result.expected_ptr = reinterpret_cast<uint64_t>(root);
        
        if (!root) {
            // Empty trie - create new root
            if (key.empty()) {
                result.new_subtree = builder.build_eos(std::forward<U>(value));
            } else {
                result.new_subtree = builder.build_skip_eos(key, std::forward<U>(value));
            }
            result.new_nodes.push_back(result.new_subtree);
            return result;
        }
        
        return insert_into_node(builder, root, key, std::forward<U>(value), depth, result);
    }

    /**
     * Insert into a node
     * For THREADED: rebuilds entire path (COW) for safe concurrent access
     */
    template <typename U>
    static result_t& insert_into_node(node_builder_t& builder,
                                       slot_type* node,
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
                split_skip_diverge(builder, node,
                                   key, std::forward<U>(value), depth, match, result);
                return result;
            } else if (match < skip.size()) {
                split_skip_prefix(builder, node,
                                  key, std::forward<U>(value), depth, match, result);
                return result;
            } else {
                key.remove_prefix(match);
                depth += match;
                
                if (key.empty()) {
                    if (view.has_skip_eos()) {
                        result.already_exists = true;
                        return result;
                    }
                    add_skip_eos(builder, node,
                                 std::forward<U>(value), result);
                    return result;
                }
            }
        }
        
        if (key.empty()) {
            if (view.has_eos()) {
                result.already_exists = true;
                return result;
            }
            add_eos(builder, node,
                    std::forward<U>(value), result);
            return result;
        }
        
        // Need to follow or create child
        unsigned char c = static_cast<unsigned char>(key[0]);
        slot_type* child_slot = view.find_child(c);
        
        if (child_slot) {
            // Child exists - follow it
            uint64_t child_ptr = load_slot<THREADED>(child_slot);
            
            // FIXED_LEN leaf optimization
            if constexpr (FIXED_LEN > 0 && !THREADED) {
                if (depth == FIXED_LEN - 1 && key.size() == 1) {
                    dataptr_t* dp = reinterpret_cast<dataptr_t*>(child_slot);
                    if (dp->has_data()) {
                        result.already_exists = true;
                        return result;
                    }
                    set_leaf_data(builder, node,
                                  c, std::forward<U>(value), depth, result);
                    return result;
                }
            }
            
            // Recurse into child
            slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
            insert_into_node(builder, child,
                             key.substr(1), std::forward<U>(value), depth + 1, result);
            
            // If child modification succeeded, rebuild this node to point to new child
            if (!result.already_exists && result.new_subtree) {
                return rebuild_with_new_child(builder, node, c, result);
            }
            return result;
        } else {
            add_child(builder, node,
                      c, key.substr(1), std::forward<U>(value), depth, result);
            return result;
        }
    }
    
    /**
     * Rebuild current node with new child subtree (COW propagation)
     */
    static result_t& rebuild_with_new_child(node_builder_t& builder,
                                             slot_type* node,
                                             unsigned char c,
                                             result_t& result) {
        node_view_t view(node);
        
        // Get current children
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        // Find and replace the modified child
        int idx = base::find_char_index(chars, c);
        if (idx >= 0) {
            children[idx] = reinterpret_cast<uint64_t>(result.new_subtree);
        }
        
        // Rebuild with new child pointer
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        slot_type* new_node = base::rebuild_node(builder, view, is_list, lst, bmp, children);
        
        result.new_nodes.push_back(new_node);
        result.old_nodes.push_back(node);
        result.new_subtree = new_node;
        
        return result;
    }

    // =========================================================================
    // Node modification operations
    // =========================================================================

    /**
     * Split node where key diverges within skip
     */
    template <typename U>
    static result_t& split_skip_diverge(node_builder_t& builder,
                                         slot_type* node,
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
                        branch = builder.build_eos_skip_list(std::move(eos_val), common, lst, children);
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
            if (view.has_eos()) {
                T eos_val;
                view.eos_data()->try_read(eos_val);
                branch = builder.build_eos_skip_list(std::move(eos_val), common, lst, children);
            } else {
                branch = builder.build_skip_list(common, lst, children);
            }
        }
        result.new_nodes.push_back(branch);
        
        result.new_subtree = branch;
        result.old_nodes.push_back(node);
        return result;
    }

    /**
     * Split node where key is prefix of skip
     */
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
        
        bool has_new_skip = !new_skip.empty();
        bool has_children = !children.empty();
        
        if (!has_children) {
            switch (mk_switch(has_new_skip, has_eos)) {
                case mk_switch(true, true):
                    return builder.build_skip_eos(new_skip, std::move(eos_val));
                case mk_switch(true, false):
                case mk_switch(false, false):
                    return builder.build_empty_root();
                case mk_switch(false, true):
                    return builder.build_eos(std::move(eos_val));
            }
            __builtin_unreachable();
        }
        
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        switch (mk_switch(has_new_skip, has_eos, is_list)) {
            case mk_switch(true, true, true):
                return builder.build_skip_eos_list(new_skip, std::move(eos_val), lst, children);
            case mk_switch(true, true, false):
                return builder.build_skip_eos_pop(new_skip, std::move(eos_val), bmp, children);
            case mk_switch(true, false, true):
                return builder.build_skip_list(new_skip, lst, children);
            case mk_switch(true, false, false):
                return builder.build_skip_pop(new_skip, bmp, children);
            case mk_switch(false, true, true):
                return builder.build_eos_list(std::move(eos_val), lst, children);
            case mk_switch(false, true, false):
                return builder.build_eos_pop(std::move(eos_val), bmp, children);
            case mk_switch(false, false, true):
                return builder.build_list(lst, children);
            case mk_switch(false, false, false):
                return builder.build_pop(bmp, children);
        }
        __builtin_unreachable();
    }

    /**
     * Add EOS to existing node
     */
    template <typename U>
    static result_t& add_eos(node_builder_t& builder,
                              slot_type* node,
                              U&& value,
                              result_t& result) {
        node_view_t view(node);
        uint64_t flags = view.flags();
        constexpr uint64_t MASK = FLAG_SKIP | FLAG_SKIP_EOS;
        
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        T skip_eos_val;
        if (flags & FLAG_SKIP_EOS) view.skip_eos_data()->try_read(skip_eos_val);
        std::string_view skip = (flags & FLAG_SKIP) ? view.skip_chars() : std::string_view{};
        
        slot_type* new_node;
        
        if (children.empty()) {
            switch (mk_flag_switch(flags, MASK)) {
                case mk_flag_switch(FLAG_SKIP | FLAG_SKIP_EOS, MASK):
                    new_node = builder.build_eos_skip_eos(std::forward<U>(value), skip, std::move(skip_eos_val));
                    break;
                case mk_flag_switch(FLAG_SKIP, MASK):
                    new_node = builder.build_eos_skip(std::forward<U>(value), skip);
                    break;
                case mk_flag_switch(0, MASK):
                    new_node = builder.build_eos(std::forward<U>(value));
                    break;
                case mk_flag_switch(FLAG_SKIP_EOS, MASK):
                default:
                    KTRIE_DEBUG_ASSERT(false && "Invalid flag combination");
                    __builtin_unreachable();
            }
        } else {
            auto [is_list, lst, bmp] = base::build_child_structure(chars);
            switch (mk_flag_switch(flags, MASK, is_list)) {
                case mk_flag_switch(FLAG_SKIP | FLAG_SKIP_EOS, MASK, true):
                    new_node = builder.build_eos_skip_eos_list(std::forward<U>(value), skip, std::move(skip_eos_val), lst, children);
                    break;
                case mk_flag_switch(FLAG_SKIP | FLAG_SKIP_EOS, MASK, false):
                    new_node = builder.build_eos_skip_eos_pop(std::forward<U>(value), skip, std::move(skip_eos_val), bmp, children);
                    break;
                case mk_flag_switch(FLAG_SKIP, MASK, true):
                    new_node = builder.build_eos_skip_list(std::forward<U>(value), skip, lst, children);
                    break;
                case mk_flag_switch(FLAG_SKIP, MASK, false):
                    new_node = builder.build_eos_skip_pop(std::forward<U>(value), skip, bmp, children);
                    break;
                case mk_flag_switch(0, MASK, true):
                    new_node = builder.build_eos_list(std::forward<U>(value), lst, children);
                    break;
                case mk_flag_switch(0, MASK, false):
                    new_node = builder.build_eos_pop(std::forward<U>(value), bmp, children);
                    break;
                case mk_flag_switch(FLAG_SKIP_EOS, MASK, true):
                case mk_flag_switch(FLAG_SKIP_EOS, MASK, false):
                default:
                    KTRIE_DEBUG_ASSERT(false && "Invalid flag combination");
                    __builtin_unreachable();
            }
        }
        
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    /**
     * Add skip_eos to existing node
     */
    template <typename U>
    static result_t& add_skip_eos(node_builder_t& builder,
                                   slot_type* node,
                                   U&& value,
                                   result_t& result) {
        node_view_t view(node);
        uint64_t flags = view.flags();
        constexpr uint64_t MASK = FLAG_EOS | FLAG_SKIP;
        std::string_view skip = view.skip_chars();
        
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        T eos_val;
        if (flags & FLAG_EOS) view.eos_data()->try_read(eos_val);
        
        slot_type* new_node;
        
        if (children.empty()) {
            switch (mk_flag_switch(flags, MASK)) {
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP, MASK):
                    new_node = builder.build_eos_skip_eos(std::move(eos_val), skip, std::forward<U>(value));
                    break;
                case mk_flag_switch(FLAG_SKIP, MASK):
                    new_node = builder.build_skip_eos(skip, std::forward<U>(value));
                    break;
                case mk_flag_switch(FLAG_EOS, MASK):
                case mk_flag_switch(0, MASK):
                default:
                    KTRIE_DEBUG_ASSERT(false && "add_skip_eos called on node without SKIP");
                    __builtin_unreachable();
            }
        } else {
            auto [is_list, lst, bmp] = base::build_child_structure(chars);
            switch (mk_flag_switch(flags, MASK, is_list)) {
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP, MASK, true):
                    new_node = builder.build_eos_skip_eos_list(std::move(eos_val), skip, std::forward<U>(value), lst, children);
                    break;
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP, MASK, false):
                    new_node = builder.build_eos_skip_eos_pop(std::move(eos_val), skip, std::forward<U>(value), bmp, children);
                    break;
                case mk_flag_switch(FLAG_SKIP, MASK, true):
                    new_node = builder.build_skip_eos_list(skip, std::forward<U>(value), lst, children);
                    break;
                case mk_flag_switch(FLAG_SKIP, MASK, false):
                    new_node = builder.build_skip_eos_pop(skip, std::forward<U>(value), bmp, children);
                    break;
                case mk_flag_switch(FLAG_EOS, MASK, true):
                case mk_flag_switch(FLAG_EOS, MASK, false):
                case mk_flag_switch(0, MASK, true):
                case mk_flag_switch(0, MASK, false):
                default:
                    KTRIE_DEBUG_ASSERT(false && "add_skip_eos called on node without SKIP");
                    __builtin_unreachable();
            }
        }
        
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    /**
     * Add new child to existing node
     */
    template <typename U>
    static result_t& add_child(node_builder_t& builder,
                                slot_type* node,
                                unsigned char c,
                                std::string_view rest,
                                U&& value,
                                size_t depth,
                                result_t& result) {
        node_view_t view(node);
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
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
        
        slot_type* new_parent = base::rebuild_node(builder, view, is_list, lst, bmp, children);
        result.new_nodes.push_back(new_parent);
        
        result.new_subtree = new_parent;
        result.old_nodes.push_back(node);
        return result;
    }

    /**
     * Set data in leaf slot (FIXED_LEN non-threaded only)
     */
    template <typename U>
    static result_t& set_leaf_data(node_builder_t& builder,
                                    slot_type* node,
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
        result.old_nodes.push_back(node);
        return result;
    }
};

}  // namespace gteitelbaum
