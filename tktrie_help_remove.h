#pragma once

#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_help_common.h"

namespace gteitelbaum {

/**
 * Remove operation results
 * 
 * THREADED writer protocol (same as insert):
 * 1. Traverse, check WRITE_BIT on each slot, record path
 * 2. Build new subtree optimistically
 * 3. LOCK mutex
 * 4. Re-verify path for WRITE_BIT (any set = abort)
 * 5. Store (new_ptr | WRITE_BIT) to target_slot
 * 6. UNLOCK
 * 7. Free old nodes
 * 8. Clear WRITE_BIT on target_slot
 */
template <bool THREADED>
struct remove_result {
    slot_type_t<THREADED>* new_subtree = nullptr;   // What to install (null = delete)
    slot_type_t<THREADED>* target_slot = nullptr;   // Where to install
    std::vector<slot_type_t<THREADED>*> traversal_path;  // Slots traversed (for re-verify)
    std::vector<slot_type_t<THREADED>*> new_nodes;
    std::vector<slot_type_t<THREADED>*> old_nodes;   // Only replaced nodes, not ancestors
    bool found = false;
    bool hit_write = false;
    bool subtree_deleted = false;                    // True if subtree should be removed
    
    remove_result() {
        new_nodes.reserve(16);
        old_nodes.reserve(16);
        if constexpr (THREADED) {
            traversal_path.reserve(32);
        }
    }
    
    // Check if any slot in path has WRITE_BIT set
    bool path_has_write_bit() const noexcept {
        if constexpr (THREADED) {
            for (auto* slot : traversal_path) {
                if (load_slot<THREADED>(slot) & WRITE_BIT) {
                    return true;
                }
            }
        }
        return false;
    }
};

/**
 * Remove helper functions - atomic slot update approach
 */
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct remove_helpers : trie_helpers<T, THREADED, Allocator, FIXED_LEN> {
    using base = trie_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using slot_type = typename base::slot_type;
    using node_view_t = typename base::node_view_t;
    using node_builder_t = typename base::node_builder_t;
    using dataptr_t = typename base::dataptr_t;
    using result_t = remove_result<THREADED>;

    /**
     * Build remove operation
     */
    static result_t build_remove_path(node_builder_t& builder, 
                                       slot_type* root_slot,
                                       slot_type* root,
                                       std::string_view key, size_t depth = 0) {
        result_t result;
        if (!root) return result;  // Key not found
        
        // Record root_slot in traversal path and check WRITE_BIT
        if constexpr (THREADED) {
            result.traversal_path.push_back(root_slot);
            uint64_t val = load_slot<THREADED>(root_slot);
            if (val & WRITE_BIT) {
                result.hit_write = true;
                return result;
            }
        }
        
        return remove_from_node(builder, root, root_slot, key, depth, result);
    }

private:
    /**
     * Remove from node
     */
    static result_t& remove_from_node(node_builder_t& builder, slot_type* node,
                                       slot_type* parent_slot,
                                       std::string_view key, size_t depth, result_t& result) {
        node_view_t view(node);
        
        if (view.has_skip()) {
            std::string_view skip = view.skip_chars();
            size_t match = base::match_skip(skip, key);
            if (match < skip.size()) return result;  // Key not found
            key.remove_prefix(match);
            depth += match;
            if (key.empty()) {
                if (!view.has_skip_eos()) return result;  // Key not found
                return remove_skip_eos(builder, node, parent_slot, result);
            }
        }
        
        if (key.empty()) {
            if (!view.has_eos()) return result;  // Key not found
            return remove_eos(builder, node, parent_slot, result);
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        slot_type* child_slot = view.find_child(c);
        if (!child_slot) return result;  // Key not found
        
        // Check WRITE_BIT and record in path
        uint64_t child_ptr = load_slot<THREADED>(child_slot);
        if constexpr (THREADED) {
            if (child_ptr & WRITE_BIT) {
                result.hit_write = true;
                return result;
            }
            result.traversal_path.push_back(child_slot);
        }
        uint64_t clean_ptr = child_ptr & PTR_MASK;
        
        // FIXED_LEN leaf optimization
        if constexpr (FIXED_LEN > 0 && !THREADED) {
            if (depth == FIXED_LEN - 1 && key.size() == 1) {
                dataptr_t* dp = reinterpret_cast<dataptr_t*>(child_slot);
                if (!dp->has_data()) return result;
                return remove_leaf_data(builder, node, parent_slot, c, result);
            }
        }
        
        // Recurse into child
        slot_type* child = reinterpret_cast<slot_type*>(clean_ptr);
        result_t child_result;
        if constexpr (THREADED) {
            // Copy traversal path to child for propagation back
            child_result.traversal_path = result.traversal_path;
        }
        remove_from_node(builder, child, child_slot, key.substr(1), depth + 1, child_result);
        
        if (!child_result.found || child_result.hit_write) {
            result.found = child_result.found;
            result.hit_write = child_result.hit_write;
            if constexpr (THREADED) {
                result.traversal_path = std::move(child_result.traversal_path);
            }
            return result;
        }
        
        // Child operation succeeded
        if (child_result.subtree_deleted) {
            // Child subtree should be removed - rebuild this node without that child
            if constexpr (THREADED) {
                result.traversal_path = std::move(child_result.traversal_path);
            }
            return remove_child(builder, node, parent_slot, c, child_result, result);
        } else {
            // Child was modified - just propagate the result up
            result.found = true;
            result.new_subtree = child_result.new_subtree;
            result.target_slot = child_result.target_slot;
            for (auto* n : child_result.new_nodes) result.new_nodes.push_back(n);
            for (auto* n : child_result.old_nodes) result.old_nodes.push_back(n);
            if constexpr (THREADED) {
                result.traversal_path = std::move(child_result.traversal_path);
            }
            return result;
        }
    }

    /**
     * Remove EOS from node
     */
    static result_t& remove_eos(node_builder_t& builder, slot_type* node,
                                 slot_type* parent_slot,
                                 result_t& result) {
        node_view_t view(node);
        result.found = true;
        uint64_t flags = view.flags();
        constexpr uint64_t MASK = FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS;
        
        bool has_children = view.child_count() > 0;
        
        // If no other data, delete this subtree
        if (!(flags & FLAG_SKIP_EOS) && !has_children) {
            result.subtree_deleted = true;
            result.target_slot = parent_slot;
            result.old_nodes.push_back(node);
            return result;
        }
        
        // SKIP without SKIP_EOS and without children is also useless
        if ((flags & FLAG_SKIP) && !(flags & FLAG_SKIP_EOS) && !has_children) {
            result.subtree_deleted = true;
            result.target_slot = parent_slot;
            result.old_nodes.push_back(node);
            return result;
        }
        
        // Rebuild node without EOS
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        T skip_eos_val;
        if (flags & FLAG_SKIP_EOS) view.skip_eos_data()->try_read(skip_eos_val);
        std::string_view skip = (flags & FLAG_SKIP) ? view.skip_chars() : std::string_view{};
        
        slot_type* new_node;
        
        if (has_children) {
            auto [is_list, lst, bmp] = base::build_child_structure(chars);
            switch (mk_flag_switch(flags, MASK, is_list)) {
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS, MASK, true):
                    new_node = builder.build_skip_eos_list(skip, std::move(skip_eos_val), lst, children);
                    break;
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS, MASK, false):
                    new_node = builder.build_skip_eos_pop(skip, std::move(skip_eos_val), bmp, children);
                    break;
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP, MASK, true):
                    new_node = builder.build_skip_list(skip, lst, children);
                    break;
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP, MASK, false):
                    new_node = builder.build_skip_pop(skip, bmp, children);
                    break;
                case mk_flag_switch(FLAG_EOS, MASK, true):
                    new_node = builder.build_list(lst, children);
                    break;
                case mk_flag_switch(FLAG_EOS, MASK, false):
                    new_node = builder.build_pop(bmp, children);
                    break;
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK, true):
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK, false):
                case mk_flag_switch(FLAG_SKIP | FLAG_SKIP_EOS, MASK, true):
                case mk_flag_switch(FLAG_SKIP | FLAG_SKIP_EOS, MASK, false):
                case mk_flag_switch(FLAG_SKIP, MASK, true):
                case mk_flag_switch(FLAG_SKIP, MASK, false):
                case mk_flag_switch(FLAG_SKIP_EOS, MASK, true):
                case mk_flag_switch(FLAG_SKIP_EOS, MASK, false):
                case mk_flag_switch(0, MASK, true):
                case mk_flag_switch(0, MASK, false):
                default:
                    KTRIE_DEBUG_ASSERT(false && "Invalid flag combination in remove_eos");
                    __builtin_unreachable();
            }
        } else {
            // No children - must have SKIP_EOS (checked above)
            new_node = builder.build_skip_eos(skip, std::move(skip_eos_val));
        }
        
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.target_slot = parent_slot;
        result.old_nodes.push_back(node);
        try_collapse(builder, result);
        return result;
    }

    /**
     * Remove skip_eos from node
     */
    static result_t& remove_skip_eos(node_builder_t& builder, slot_type* node,
                                      slot_type* parent_slot,
                                      result_t& result) {
        node_view_t view(node);
        result.found = true;
        uint64_t flags = view.flags();
        constexpr uint64_t MASK = FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS;
        
        bool has_children = view.child_count() > 0;
        std::string_view skip = view.skip_chars();
        
        if (!(flags & FLAG_EOS) && !has_children) {
            result.subtree_deleted = true;
            result.target_slot = parent_slot;
            result.old_nodes.push_back(node);
            return result;
        }
        
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        T eos_val;
        if (flags & FLAG_EOS) view.eos_data()->try_read(eos_val);
        
        slot_type* new_node;
        
        if (has_children) {
            auto [is_list, lst, bmp] = base::build_child_structure(chars);
            switch (mk_flag_switch(flags, MASK, is_list)) {
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS, MASK, true):
                    new_node = builder.build_eos_skip_list(std::move(eos_val), skip, lst, children);
                    break;
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS, MASK, false):
                    new_node = builder.build_eos_skip_pop(std::move(eos_val), skip, bmp, children);
                    break;
                case mk_flag_switch(FLAG_SKIP | FLAG_SKIP_EOS, MASK, true):
                    new_node = builder.build_skip_list(skip, lst, children);
                    break;
                case mk_flag_switch(FLAG_SKIP | FLAG_SKIP_EOS, MASK, false):
                    new_node = builder.build_skip_pop(skip, bmp, children);
                    break;
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP, MASK, true):
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP, MASK, false):
                case mk_flag_switch(FLAG_EOS, MASK, true):
                case mk_flag_switch(FLAG_EOS, MASK, false):
                case mk_flag_switch(FLAG_SKIP, MASK, true):
                case mk_flag_switch(FLAG_SKIP, MASK, false):
                case mk_flag_switch(0, MASK, true):
                case mk_flag_switch(0, MASK, false):
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK, true):
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK, false):
                case mk_flag_switch(FLAG_SKIP_EOS, MASK, true):
                case mk_flag_switch(FLAG_SKIP_EOS, MASK, false):
                default:
                    KTRIE_DEBUG_ASSERT(false && "Invalid flag combination in remove_skip_eos");
                    __builtin_unreachable();
            }
        } else {
            // No children - if has EOS, skip is pointless
            new_node = builder.build_eos(std::move(eos_val));
        }
        
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.target_slot = parent_slot;
        result.old_nodes.push_back(node);
        try_collapse(builder, result);
        return result;
    }

    /**
     * Remove child from node (child subtree was deleted)
     */
    static result_t& remove_child(node_builder_t& builder, slot_type* node,
                                   slot_type* parent_slot,
                                   unsigned char c, result_t& child_result, result_t& result) {
        for (auto* n : child_result.new_nodes) result.new_nodes.push_back(n);
        for (auto* n : child_result.old_nodes) result.old_nodes.push_back(n);
        result.found = true;
        
        node_view_t view(node);
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        int idx = base::find_char_index(chars, c);
        if (idx >= 0) {
            children.erase(children.begin() + idx);
            chars.erase(chars.begin() + idx);
        }
        
        bool has_eos = view.has_eos(), has_skip_eos = view.has_skip_eos();
        
        // If no other data/children, delete this subtree too
        if (children.empty() && !has_eos && !has_skip_eos) {
            result.subtree_deleted = true;
            result.target_slot = parent_slot;
            result.old_nodes.push_back(node);
            return result;
        }
        
        // Rebuild node without that child
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        slot_type* new_node = base::rebuild_node(builder, view, is_list, lst, bmp, children);
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.target_slot = parent_slot;
        result.old_nodes.push_back(node);
        try_collapse(builder, result);
        return result;
    }

    /**
     * Remove leaf data (FIXED_LEN non-threaded only)
     */
    static result_t& remove_leaf_data(node_builder_t& builder, slot_type* node,
                                       slot_type* parent_slot,
                                       unsigned char c, result_t& result) {
        result.found = true;
        node_view_t view(node);
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        int idx = base::find_char_index(chars, c);
        if (idx >= 0) {
            slot_type* child_slot = view.find_child(c);
            dataptr_t* dp = reinterpret_cast<dataptr_t*>(child_slot);
            dp->~dataptr_t();
            children.erase(children.begin() + idx);
            chars.erase(chars.begin() + idx);
        }
        
        if (children.empty()) {
            result.subtree_deleted = true;
            result.target_slot = parent_slot;
            result.old_nodes.push_back(node);
            return result;
        }
        
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        slot_type* new_node = base::rebuild_node(builder, view, is_list, lst, bmp, children);
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.target_slot = parent_slot;
        result.old_nodes.push_back(node);
        return result;
    }

    /**
     * Try to collapse single-child nodes into skip sequence
     */
    static void try_collapse(node_builder_t& builder, result_t& result) {
        if (!result.new_subtree) return;
        node_view_t view(result.new_subtree);
        if (view.has_eos() || view.has_skip_eos() || view.child_count() != 1) return;
        
        uint64_t child_ptr = view.get_child_ptr(0);
        if constexpr (THREADED) child_ptr &= PTR_MASK;
        slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
        if (!child) return;
        
        unsigned char c = view.has_list() ? view.get_list().char_at(0) : view.get_bitmap().nth_char(0);
        std::string new_skip;
        if (view.has_skip()) new_skip = std::string(view.skip_chars());
        new_skip.push_back(static_cast<char>(c));
        
        node_view_t child_view(child);
        uint64_t child_flags = child_view.flags();
        constexpr uint64_t MASK = FLAG_EOS | FLAG_SKIP_EOS;
        
        if (child_view.has_skip()) new_skip.append(child_view.skip_chars());
        
        auto children = base::extract_children(child_view);
        auto chars = base::get_child_chars(child_view);
        
        T eos_val, skip_eos_val;
        if (child_flags & FLAG_EOS) child_view.eos_data()->try_read(eos_val);
        if (child_flags & FLAG_SKIP_EOS) child_view.skip_eos_data()->try_read(skip_eos_val);
        
        slot_type* collapsed;
        if (children.empty()) {
            switch (mk_flag_switch(child_flags, MASK)) {
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK):
                    collapsed = builder.build_eos_skip_eos(std::move(eos_val), new_skip, std::move(skip_eos_val));
                    break;
                case mk_flag_switch(FLAG_EOS, MASK):
                    collapsed = builder.build_skip_eos(new_skip, std::move(eos_val));
                    break;
                case mk_flag_switch(FLAG_SKIP_EOS, MASK):
                    collapsed = builder.build_skip_eos(new_skip, std::move(skip_eos_val));
                    break;
                case mk_flag_switch(0, MASK):
                    return;  // Can't collapse empty node
            }
        } else {
            auto [is_list, lst, bmp] = base::build_child_structure(chars);
            switch (mk_flag_switch(child_flags, MASK, is_list)) {
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK, true):
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK, false):
                    return;  // Too complex to collapse
                case mk_flag_switch(FLAG_EOS, MASK, true):
                    collapsed = builder.build_skip_eos_list(new_skip, std::move(eos_val), lst, children);
                    break;
                case mk_flag_switch(FLAG_EOS, MASK, false):
                    collapsed = builder.build_skip_eos_pop(new_skip, std::move(eos_val), bmp, children);
                    break;
                case mk_flag_switch(FLAG_SKIP_EOS, MASK, true):
                    collapsed = builder.build_skip_eos_list(new_skip, std::move(skip_eos_val), lst, children);
                    break;
                case mk_flag_switch(FLAG_SKIP_EOS, MASK, false):
                    collapsed = builder.build_skip_eos_pop(new_skip, std::move(skip_eos_val), bmp, children);
                    break;
                case mk_flag_switch(0, MASK, true):
                    collapsed = builder.build_skip_list(new_skip, lst, children);
                    break;
                case mk_flag_switch(0, MASK, false):
                    collapsed = builder.build_skip_pop(new_skip, bmp, children);
                    break;
            }
        }
        
        // Replace new_subtree with collapsed version
        result.old_nodes.push_back(result.new_subtree);
        result.old_nodes.push_back(child);  // Child absorbed into collapsed
        result.new_subtree = collapsed;
        result.new_nodes.push_back(collapsed);
    }
};

}  // namespace gteitelbaum
