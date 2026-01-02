#pragma once

#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_help_common.h"

namespace gteitelbaum {

/**
 * Remove operation results
 * 
 * Atomic slot update approach (same as insert):
 * - new_subtree: the rebuilt node/subtree to install (or null if deleting)
 * - target_slot: the single slot to atomically update (null = update root)
 * - expected_ptr: expected current value in target_slot for verification
 * - old_nodes: only the nodes being replaced (NOT ancestors)
 * - read_locked_slots: slots where we set READ_BIT during traversal (THREADED only)
 */
template <bool THREADED>
struct remove_result {
    slot_type_t<THREADED>* new_subtree = nullptr;   // What to install (null = delete)
    slot_type_t<THREADED>* target_slot = nullptr;   // Where to install (null = root)
    uint64_t expected_ptr = 0;                       // Expected value in target_slot
    std::vector<slot_type_t<THREADED>*> new_nodes;
    std::vector<slot_type_t<THREADED>*> old_nodes;   // Only replaced nodes, not ancestors
    std::vector<slot_type_t<THREADED>*> read_locked_slots;  // Slots with READ_BIT set
    bool found = false;
    bool hit_write = false;
    bool hit_read = false;
    bool subtree_deleted = false;                    // True if subtree should be removed
    
    remove_result() {
        new_nodes.reserve(16);
        old_nodes.reserve(16);
        if constexpr (THREADED) {
            read_locked_slots.reserve(16);
        }
    }
    
    // Clear READ_BITs we set (call after commit or on abort)
    void clear_read_locks() noexcept {
        if constexpr (THREADED) {
            for (auto* slot : read_locked_slots) {
                fetch_and_slot<THREADED>(slot, ~READ_BIT);
            }
            read_locked_slots.clear();
        }
    }
};

/**
 * Remove helper functions - atomic slot update approach
 * 
 * THREADED writer protocol:
 * - Set READ_BIT on each slot we traverse through
 * - If we see READ_BIT or WRITE_BIT already set, abort (another writer is here)
 * - After commit, clear READ_BITs we set (except target_slot which is updated)
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
     * Try to acquire READ_BIT on a slot for writer traversal
     * Returns the clean pointer value, or sets hit_read/hit_write and returns 0
     */
    static uint64_t try_read_lock_slot(slot_type* slot, result_t& result) noexcept {
        if constexpr (THREADED) {
            // Try to set READ_BIT
            uint64_t old_val = fetch_or_slot<THREADED>(slot, READ_BIT);
            
            // If WRITE_BIT or READ_BIT was already set, another writer is here
            if (old_val & WRITE_BIT) {
                // Clear our READ_BIT and abort
                fetch_and_slot<THREADED>(slot, ~READ_BIT);
                result.hit_write = true;
                return 0;
            }
            if (old_val & READ_BIT) {
                // READ_BIT was already set by another writer
                // Clear our addition and abort
                fetch_and_slot<THREADED>(slot, ~READ_BIT);
                result.hit_read = true;
                return 0;
            }
            
            // Successfully acquired READ_BIT, track it
            result.read_locked_slots.push_back(slot);
            return old_val & PTR_MASK;
        } else {
            return load_slot<THREADED>(slot);
        }
    }

    /**
     * Build remove operation
     * @param builder Node builder
     * @param root_slot Pointer to root slot (for READ_BIT protection in THREADED mode)
     * @param root Current root node
     * @param key Key to remove
     * @param depth Current depth
     */
    static result_t build_remove_path(node_builder_t& builder, 
                                       slot_type* root_slot,
                                       slot_type* root,
                                       std::string_view key, size_t depth = 0) {
        result_t result;
        if (!root) return result;  // Key not found
        
        // For THREADED: acquire READ_BIT on root_slot before traversing
        if constexpr (THREADED) {
            uint64_t old_val = fetch_or_slot<THREADED>(root_slot, READ_BIT);
            if (old_val & WRITE_BIT) {
                fetch_and_slot<THREADED>(root_slot, ~READ_BIT);
                result.hit_write = true;
                return result;
            }
            if (old_val & READ_BIT) {
                fetch_and_slot<THREADED>(root_slot, ~READ_BIT);
                result.hit_read = true;
                return result;
            }
            result.read_locked_slots.push_back(root_slot);
        }
        
        result.expected_ptr = reinterpret_cast<uint64_t>(root);
        return remove_from_node(builder, root, nullptr, result.expected_ptr, key, depth, result);
    }

private:
    /**
     * Remove from node
     * @param node Current node
     * @param parent_slot Slot pointing to this node (null = root)
     * @param parent_slot_value Current value in parent_slot
     */
    static result_t& remove_from_node(node_builder_t& builder, slot_type* node,
                                       slot_type* parent_slot, uint64_t parent_slot_value,
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
                return remove_skip_eos(builder, node, parent_slot, parent_slot_value, result);
            }
        }
        
        if (key.empty()) {
            if (!view.has_eos()) return result;  // Key not found
            return remove_eos(builder, node, parent_slot, parent_slot_value, result);
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        slot_type* child_slot = view.find_child(c);
        if (!child_slot) return result;  // Key not found
        
        // Try to acquire READ_BIT for traversal
        uint64_t clean_ptr = try_read_lock_slot(child_slot, result);
        if (result.hit_write || result.hit_read) {
            return result;
        }
        
        // FIXED_LEN leaf optimization
        if constexpr (FIXED_LEN > 0 && !THREADED) {
            if (depth == FIXED_LEN - 1 && key.size() == 1) {
                dataptr_t* dp = reinterpret_cast<dataptr_t*>(child_slot);
                if (!dp->has_data()) return result;
                return remove_leaf_data(builder, node, parent_slot, parent_slot_value, c, result);
            }
        }
        
        // Recurse into child
        // Note: for THREADED, slot now has READ_BIT set, so expected value is (clean_ptr | READ_BIT)
        slot_type* child = reinterpret_cast<slot_type*>(clean_ptr);
        uint64_t child_slot_value = THREADED ? (clean_ptr | READ_BIT) : clean_ptr;
        result_t child_result;
        remove_from_node(builder, child, child_slot, child_slot_value, key.substr(1), depth + 1, child_result);
        
        if (!child_result.found || child_result.hit_write || child_result.hit_read) {
            result.found = child_result.found;
            result.hit_write = child_result.hit_write;
            result.hit_read = child_result.hit_read;
            // Propagate read locks for cleanup on abort
            if constexpr (THREADED) {
                for (auto* s : child_result.read_locked_slots) {
                    result.read_locked_slots.push_back(s);
                }
            }
            return result;
        }
        
        // Child operation succeeded
        if (child_result.subtree_deleted) {
            // Child subtree should be removed - rebuild this node without that child
            // Propagate read locks
            if constexpr (THREADED) {
                for (auto* s : child_result.read_locked_slots) {
                    result.read_locked_slots.push_back(s);
                }
            }
            return remove_child(builder, node, parent_slot, parent_slot_value, c, child_result, result);
        } else {
            // Child was modified - just propagate the result up
            // The target_slot is already set to child_slot by the child
            result.found = true;
            result.new_subtree = child_result.new_subtree;
            result.target_slot = child_result.target_slot;
            result.expected_ptr = child_result.expected_ptr;
            for (auto* n : child_result.new_nodes) result.new_nodes.push_back(n);
            for (auto* n : child_result.old_nodes) result.old_nodes.push_back(n);
            if constexpr (THREADED) {
                for (auto* s : child_result.read_locked_slots) {
                    result.read_locked_slots.push_back(s);
                }
            }
            return result;
        }
    }

    /**
     * Remove EOS from node
     */
    static result_t& remove_eos(node_builder_t& builder, slot_type* node,
                                 slot_type* parent_slot, uint64_t parent_slot_value,
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
            result.expected_ptr = parent_slot_value;
            result.old_nodes.push_back(node);
            return result;
        }
        
        // SKIP without SKIP_EOS and without children is also useless
        if ((flags & FLAG_SKIP) && !(flags & FLAG_SKIP_EOS) && !has_children) {
            result.subtree_deleted = true;
            result.target_slot = parent_slot;
            result.expected_ptr = parent_slot_value;
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
            // 3 flag bits + is_list = 16 combos
            // Valid: must have EOS, SKIP_EOS requires SKIP
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
                // Invalid: EOS | SKIP_EOS without SKIP
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK, true):
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK, false):
                // Invalid: no EOS (shouldn't call remove_eos)
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
        result.expected_ptr = parent_slot_value;
        result.old_nodes.push_back(node);
        try_collapse(builder, result);
        return result;
    }

    /**
     * Remove skip_eos from node
     */
    static result_t& remove_skip_eos(node_builder_t& builder, slot_type* node,
                                      slot_type* parent_slot, uint64_t parent_slot_value,
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
            result.expected_ptr = parent_slot_value;
            result.old_nodes.push_back(node);
            return result;
        }
        
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        T eos_val;
        if (flags & FLAG_EOS) view.eos_data()->try_read(eos_val);
        
        slot_type* new_node;
        
        if (has_children) {
            // 3 flag bits + is_list = 16 combos
            // Valid: must have SKIP_EOS (which requires SKIP)
            auto [is_list, lst, bmp] = base::build_child_structure(chars);
            switch (mk_flag_switch(flags, MASK, is_list)) {
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS, MASK, true):
                    // Keep the skip - children hang off the end of it
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
                // Invalid: no SKIP_EOS (shouldn't call remove_skip_eos)
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP, MASK, true):
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP, MASK, false):
                case mk_flag_switch(FLAG_EOS, MASK, true):
                case mk_flag_switch(FLAG_EOS, MASK, false):
                case mk_flag_switch(FLAG_SKIP, MASK, true):
                case mk_flag_switch(FLAG_SKIP, MASK, false):
                case mk_flag_switch(0, MASK, true):
                case mk_flag_switch(0, MASK, false):
                // Invalid flag combos (SKIP_EOS without SKIP)
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK, true):
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK, false):
                case mk_flag_switch(FLAG_SKIP_EOS, MASK, true):
                case mk_flag_switch(FLAG_SKIP_EOS, MASK, false):
                default:
                    KTRIE_DEBUG_ASSERT(false && "Invalid flag combination in remove_skip_eos");
                    __builtin_unreachable();
            }
        } else {
            // No children - if has EOS, skip is pointless; if no EOS, handled by early return
            new_node = builder.build_eos(std::move(eos_val));
        }
        
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.target_slot = parent_slot;
        result.expected_ptr = parent_slot_value;
        result.old_nodes.push_back(node);
        try_collapse(builder, result);
        return result;
    }

    /**
     * Remove child from node (child subtree was deleted)
     */
    static result_t& remove_child(node_builder_t& builder, slot_type* node,
                                   slot_type* parent_slot, uint64_t parent_slot_value,
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
            result.expected_ptr = parent_slot_value;
            result.old_nodes.push_back(node);
            return result;
        }
        
        // Rebuild node without that child
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        slot_type* new_node = base::rebuild_node(builder, view, is_list, lst, bmp, children);
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.target_slot = parent_slot;
        result.expected_ptr = parent_slot_value;
        result.old_nodes.push_back(node);
        try_collapse(builder, result);
        return result;
    }

    /**
     * Remove leaf data (FIXED_LEN non-threaded only)
     */
    static result_t& remove_leaf_data(node_builder_t& builder, slot_type* node,
                                       slot_type* parent_slot, uint64_t parent_slot_value,
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
            result.expected_ptr = parent_slot_value;
            result.old_nodes.push_back(node);
            return result;
        }
        
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        slot_type* new_node = base::rebuild_node(builder, view, is_list, lst, bmp, children);
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.target_slot = parent_slot;
        result.expected_ptr = parent_slot_value;
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
            // 2 flag bits = 4 combos, all valid
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
            // 2 flag bits + is_list = 8 combos, all valid
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
