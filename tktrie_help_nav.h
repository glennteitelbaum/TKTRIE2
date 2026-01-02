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
 * Reader protocol for THREADED mode:
 * - Child pointers: if WRITE_BIT set → restart from root
 *   Then double-check slot unchanged after loading pointer
 * - Data pointers: handled by dataptr::try_read (spin on READ_BIT, CAS to set, copy, clear)
 */
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct nav_helpers : trie_helpers<T, THREADED, Allocator, FIXED_LEN> {
    using base = trie_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using slot_type = typename base::slot_type;
    using node_view_t = typename base::node_view_t;
    using dataptr_t = typename base::dataptr_t;

    /**
     * Safely load child pointer with double-check
     * Returns nullptr and sets hit_write if WRITE_BIT seen or slot changed
     */
    static slot_type* safe_load_child(slot_type* child_slot, bool& hit_write) noexcept {
        if constexpr (THREADED) {
            uint64_t val = load_slot<THREADED>(child_slot);
            
            // Check for WRITE_BIT (writer is modifying)
            if (val & WRITE_BIT) {
                hit_write = true;
                return nullptr;
            }
            
            slot_type* child = reinterpret_cast<slot_type*>(val & PTR_MASK);
            
            // Double-check slot hasn't changed (race protection)
            uint64_t val2 = load_slot<THREADED>(child_slot);
            if (val2 != val) {
                hit_write = true;
                return nullptr;
            }
            
            return child;
        } else {
            uint64_t val = load_slot<THREADED>(child_slot);
            return reinterpret_cast<slot_type*>(val);
        }
    }

    /**
     * Find node for exact key match
     * Sets hit_write if WRITE_BIT encountered (caller should retry)
     * Returns pointer to data slot, or nullptr if not found
     */
    static slot_type* find_data_slot(slot_type* root, std::string_view key, 
                                      bool& hit_write, size_t start_depth = 0) noexcept {
        hit_write = false;
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
            if (!child_slot) return nullptr;
            
            // FIXED_LEN leaf optimization: non-threaded stores dataptr inline at leaf depth
            if constexpr (FIXED_LEN > 0 && !THREADED) {
                if (depth == FIXED_LEN - 1 && key.size() == 1) {
                    return child_slot;
                }
            }
            
            // Safe load with double-check
            cur = safe_load_child(child_slot, hit_write);
            if (hit_write) return nullptr;
            
            key.remove_prefix(1);
            ++depth;
        }
        
        return nullptr;
    }

    /**
     * Check if key exists in trie
     */
    static bool contains(slot_type* root, std::string_view key, bool& hit_write) noexcept {
        slot_type* data_slot = find_data_slot(root, key, hit_write);
        if (!data_slot || hit_write) return false;
        
        if constexpr (THREADED) {
            dataptr_t* dp = reinterpret_cast<dataptr_t*>(data_slot);
            T dummy;
            if (!dp->try_read(dummy)) {
                hit_write = true;
                return false;
            }
            return true;
        } else {
            dataptr_t* dp = reinterpret_cast<dataptr_t*>(data_slot);
            return dp->has_data();
        }
    }

    /**
     * Read value at key
     */
    static bool read(slot_type* root, std::string_view key, T& out, bool& hit_write) noexcept {
        slot_type* data_slot = find_data_slot(root, key, hit_write);
        if (!data_slot || hit_write) return false;
        
        dataptr_t* dp = reinterpret_cast<dataptr_t*>(data_slot);
        if (!dp->try_read(out)) {
            if constexpr (THREADED) {
                hit_write = true;
            }
            return false;
        }
        return true;
    }

    /**
     * Find first leaf in subtree (for iteration)
     * Used by DELETE ALL - must handle WRITE_BIT same as other readers
     */
    static slot_type* find_first_leaf(slot_type* node, std::string& key_out, 
                                       bool& hit_write, size_t depth = 0) noexcept {
        hit_write = false;
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
            
            // No data here, go to first child
            if (view.child_count() == 0) {
                return nullptr;
            }
            
            unsigned char c;
            if (view.has_list()) {
                c = view.get_list().char_at(0);
            } else {
                c = view.get_bitmap().nth_char(0);
            }
            
            key_out.push_back(static_cast<char>(c));
            
            slot_type* child_slot = view.find_child(c);
            
            // FIXED_LEN leaf optimization
            if constexpr (FIXED_LEN > 0 && !THREADED) {
                if (depth == FIXED_LEN - 1) {
                    return child_slot;
                }
            }
            
            // Safe load with double-check
            node = safe_load_child(child_slot, hit_write);
            if (hit_write) return nullptr;
            
            ++depth;
        }
    }
};

}  // namespace gteitelbaum
