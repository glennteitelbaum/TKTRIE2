#pragma once

#include <string_view>
#include <vector>
#include <array>

#include "tktrie_defines.h"
#include "tktrie_help_common.h"

namespace gteitelbaum {

/**
 * Navigation helper functions (READER operations)
 * 
 * With COW + EBR + atomic deletes:
 * - Readers are protected by EBR epoch - old nodes won't be freed while readers active
 * - Child pointers may be null (deleted) - check after loading
 * - FULL nodes: slot always exists, but value may be null
 * - LIST/POP nodes: slot exists only if char in structure, but value may be null
 */
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct nav_helpers : trie_helpers<T, THREADED, Allocator, FIXED_LEN> {
    using base = trie_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using slot_type = typename base::slot_type;
    using node_view_t = typename base::node_view_t;
    using dataptr_t = typename base::dataptr_t;

    /**
     * Find node for exact key match
     * Returns pointer to data slot, or nullptr if not found
     */
    static slot_type* find_data_slot(slot_type* root, std::string_view key, 
                                      size_t start_depth = 0) noexcept {
        slot_type* cur = root;
        size_t depth = start_depth;
        
        while (cur) {
            node_view_t view(cur);
            
            if (view.has_skip()) {
                std::string_view skip = view.skip_chars();
                size_t match = base::match_skip(skip, key);
                
                if (match < skip.size()) {
                    return nullptr;  // key diverges in skip
                }
                
                key.remove_prefix(match);
                depth += match;
                
                if (key.empty()) {
                    // Key ends at skip
                    if (view.has_skip_eos()) {
                        return reinterpret_cast<slot_type*>(view.skip_eos_data());
                    }
                    return nullptr;
                }
            }
            
            if (key.empty()) {
                if (view.has_eos()) {
                    return reinterpret_cast<slot_type*>(view.eos_data());
                }
                return nullptr;
            }
            
            // Find child
            unsigned char c = static_cast<unsigned char>(key[0]);
            slot_type* child_slot = view.find_child(c);
            
            // For LIST/POP: nullptr means char not in structure
            // For FULL: always returns a slot, but value may be null
            if (!child_slot) return nullptr;
            
            // FIXED_LEN leaf optimization: non-threaded stores dataptr inline at leaf depth
            if constexpr (FIXED_LEN > 0 && !THREADED) {
                if (depth == FIXED_LEN - 1 && key.size() == 1) {
                    return child_slot;
                }
            }
            
            // Load child pointer - may be null (deleted)
            uint64_t child_ptr = load_slot<THREADED>(child_slot);
            if (child_ptr == 0) return nullptr;  // Deleted child
            
            cur = reinterpret_cast<slot_type*>(child_ptr);
            
            key.remove_prefix(1);
            ++depth;
        }
        
        return nullptr;
    }

    /**
     * Check if key exists in trie
     */
    static bool contains(slot_type* root, std::string_view key) noexcept {
        slot_type* data_slot = find_data_slot(root, key);
        if (!data_slot) return false;
        
        dataptr_t* dp = reinterpret_cast<dataptr_t*>(data_slot);
        return dp->has_data();
    }

    /**
     * Read value at key
     */
    static bool read(slot_type* root, std::string_view key, T& out) noexcept {
        slot_type* data_slot = find_data_slot(root, key);
        if (!data_slot) return false;
        
        dataptr_t* dp = reinterpret_cast<dataptr_t*>(data_slot);
        return dp->try_read(out);
    }

    /**
     * Find first leaf in subtree (for iteration)
     * Skips null (deleted) children
     */
    static slot_type* find_first_leaf(slot_type* node, std::string& key_out, 
                                       size_t depth = 0) noexcept {
        if (!node) return nullptr;
        
        while (true) {
            node_view_t view(node);
            
            // Accumulate skip chars
            if (view.has_skip()) {
                key_out.append(view.skip_chars());
                depth += view.skip_length();
            }
            
            // Check for data - return first data pointer found
            if (view.has_eos()) {
                return reinterpret_cast<slot_type*>(view.eos_data());
            }
            if (view.has_skip_eos()) {
                return reinterpret_cast<slot_type*>(view.skip_eos_data());
            }
            
            // Find first non-null child
            slot_type* child_slot = nullptr;
            unsigned char c = 0;
            
            if (view.has_full()) {
                // FULL: scan 256 slots for first non-null
                for (int i = 0; i < 256; ++i) {
                    uint64_t ptr = load_slot<THREADED>(&view.child_ptrs()[i]);
                    if (ptr != 0) {
                        c = static_cast<unsigned char>(i);
                        child_slot = &view.child_ptrs()[i];
                        break;
                    }
                }
            } else if (view.has_list()) {
                // LIST: scan chars in order, find first non-null ptr
                auto [sorted, count] = view.get_list().sorted_chars();
                for (int i = 0; i < count; ++i) {
                    slot_type* slot = view.find_child(sorted[i]);
                    uint64_t ptr = load_slot<THREADED>(slot);
                    if (ptr != 0) {
                        c = sorted[i];
                        child_slot = slot;
                        break;
                    }
                }
            } else if (view.has_pop()) {
                // POP: iterate bitmap in order
                popcount_bitmap bmp = view.get_bitmap();
                for (int i = 0; i < bmp.count(); ++i) {
                    unsigned char ch = bmp.nth_char(i);
                    uint64_t ptr = view.get_child_ptr(i);
                    if (ptr != 0) {
                        c = ch;
                        child_slot = &view.child_ptrs()[i];
                        break;
                    }
                }
            }
            
            if (!child_slot) return nullptr;  // No live children
            
            key_out.push_back(static_cast<char>(c));
            
            // FIXED_LEN leaf optimization
            if constexpr (FIXED_LEN > 0 && !THREADED) {
                if (depth == FIXED_LEN - 1) {
                    return child_slot;
                }
            }
            
            // Load child pointer
            uint64_t child_ptr = load_slot<THREADED>(child_slot);
            node = reinterpret_cast<slot_type*>(child_ptr);
            
            ++depth;
        }
    }
};

}  // namespace gteitelbaum
