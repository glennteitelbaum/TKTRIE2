#pragma once

#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_node.h"

namespace gteitelbaum {

/**
 * Path entry for tracking traversal
 */
template <bool THREADED>
struct path_entry {
    slot_type_t<THREADED>* node;
    slot_type_t<THREADED>* child_slot;  // slot we followed (nullptr for leaf)
    uint32_t version;
    int child_idx;
};

/**
 * Common helper functions for trie operations
 */
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct trie_helpers {
    using slot_type = slot_type_t<THREADED>;
    using node_view_t = node_view<T, THREADED, Allocator, FIXED_LEN>;
    using node_builder_t = node_builder<T, THREADED, Allocator, FIXED_LEN>;
    using dataptr_t = dataptr<T, THREADED, Allocator>;
    using path_entry_t = path_entry<THREADED>;

    /**
     * Spin wait helper
     */
    static void spin() noexcept {
        cpu_pause();
    }

    /**
     * Check if we can have EOS/SKIP_EOS at given depth
     */
    static constexpr bool can_have_data(size_t depth) noexcept {
        if constexpr (FIXED_LEN == 0) {
            return true;
        } else {
            return depth >= FIXED_LEN;
        }
    }

    /**
     * Check if node at given depth is a leaf (children are dataptr not nodes)
     */
    static constexpr bool is_leaf_depth(size_t depth) noexcept {
        if constexpr (FIXED_LEN == 0) {
            return false;  // variable length never has fixed leaf depth
        } else {
            return depth == FIXED_LEN - 1;
        }
    }

    /**
     * Match skip sequence against remaining key
     * Returns number of matching characters
     */
    static size_t match_skip(std::string_view skip, std::string_view key) noexcept {
        size_t i = 0;
        while (i < skip.size() && i < key.size() && skip[i] == key[i]) {
            ++i;
        }
        return i;
    }

    /**
     * Extract child pointers from a node as vector
     */
    static std::vector<uint64_t> extract_children(node_view_t& view) {
        std::vector<uint64_t> children;
        int count = view.child_count();
        children.reserve(count);
        for (int i = 0; i < count; ++i) {
            children.push_back(view.get_child_ptr(i));
        }
        return children;
    }

    /**
     * Get all characters from node's child structure
     */
    static std::vector<unsigned char> get_child_chars(node_view_t& view) {
        std::vector<unsigned char> chars;
        if (view.has_list()) {
            small_list lst = view.get_list();
            chars.reserve(lst.count());
            for (int i = 0; i < lst.count(); ++i) {
                chars.push_back(lst.char_at(i));
            }
        } else if (view.has_pop()) {
            popcount_bitmap bmp = view.get_bitmap();
            chars.reserve(bmp.count());
            for (int i = 0; i < bmp.count(); ++i) {
                chars.push_back(bmp.nth_char(i));
            }
        }
        return chars;
    }

    /**
     * Build appropriate children structure based on count
     * Returns (is_list, small_list or empty, bitmap or empty)
     */
    static std::tuple<bool, small_list, popcount_bitmap> 
    build_child_structure(const std::vector<unsigned char>& chars) {
        if (chars.size() <= 7) {
            small_list lst;
            for (size_t i = 0; i < chars.size(); ++i) {
                lst.insert(static_cast<int>(i), chars[i]);
            }
            return {true, lst, popcount_bitmap()};
        } else {
            popcount_bitmap bmp;
            for (auto c : chars) {
                bmp.set(c);
            }
            return {false, small_list(), bmp};
        }
    }

    /**
     * Insert a character into child structure, returns new index
     */
    static int insert_child_char(small_list& lst, popcount_bitmap& bmp, 
                                  bool& is_list, unsigned char c) {
        if (is_list) {
            if (lst.count() < small_list::max_count) {
                return lst.insert(lst.count(), c);
            } else {
                // Convert list to bitmap
                for (int i = 0; i < lst.count(); ++i) {
                    bmp.set(lst.char_at(i));
                }
                is_list = false;
                return bmp.set(c);
            }
        } else {
            return bmp.set(c);
        }
    }
};

}  // namespace gteitelbaum
