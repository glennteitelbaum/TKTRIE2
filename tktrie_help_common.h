#pragma once

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_node.h"

namespace gteitelbaum {

/**
 * Common helper functions for trie operations
 */
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct trie_helpers {
    using slot_type = slot_type_t<THREADED>;
    using node_view_t = node_view<T, THREADED, Allocator, FIXED_LEN>;
    using node_builder_t = node_builder<T, THREADED, Allocator, FIXED_LEN>;
    using dataptr_t = dataptr<T, THREADED, Allocator>;

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
     * Find index of character in chars vector
     */
    static int find_char_index(const std::vector<unsigned char>& chars, unsigned char c) {
        for (size_t i = 0; i < chars.size(); ++i) {
            if (chars[i] == c) return static_cast<int>(i);
        }
        return -1;
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

    /**
     * Rebuild node with given children - shared by insert and remove helpers
     */
    static slot_type* rebuild_node(node_builder_t& builder,
                                    node_view_t& view,
                                    bool is_list,
                                    small_list& lst,
                                    popcount_bitmap& bmp,
                                    const std::vector<uint64_t>& children) {
        uint64_t flags = view.flags();
        constexpr uint64_t MASK = FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS;
        
        T eos_val, skip_eos_val;
        if (flags & FLAG_EOS) view.eos_data()->try_read(eos_val);
        if (flags & FLAG_SKIP_EOS) view.skip_eos_data()->try_read(skip_eos_val);
        std::string_view skip = (flags & FLAG_SKIP) ? view.skip_chars() : std::string_view{};
        
        if (children.empty()) {
            // 3 flag bits = 8 combos, but SKIP_EOS requires SKIP
            switch (mk_flag_switch(flags, MASK)) {
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS, MASK):
                    return builder.build_eos_skip_eos(std::move(eos_val), skip, std::move(skip_eos_val));
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP, MASK):
                    return builder.build_eos_skip(std::move(eos_val), skip);
                case mk_flag_switch(FLAG_EOS, MASK):
                    return builder.build_eos(std::move(eos_val));
                case mk_flag_switch(FLAG_SKIP | FLAG_SKIP_EOS, MASK):
                    return builder.build_skip_eos(skip, std::move(skip_eos_val));
                case mk_flag_switch(FLAG_SKIP, MASK):  // SKIP with no data - degenerate but handle it
                case mk_flag_switch(0, MASK):
                    return builder.build_empty_root();
                // Invalid: SKIP_EOS without SKIP
                case mk_flag_switch(FLAG_SKIP_EOS, MASK):
                case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK):
                default:
                    KTRIE_DEBUG_ASSERT(false && "Invalid flag combination");
                    __builtin_unreachable();
            }
        }
        
        // 3 flag bits + is_list = 16 combos, but SKIP_EOS requires SKIP
        switch (mk_flag_switch(flags, MASK, is_list)) {
            case mk_flag_switch(FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS, MASK, true):
                return builder.build_eos_skip_eos_list(std::move(eos_val), skip, std::move(skip_eos_val), lst, children);
            case mk_flag_switch(FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS, MASK, false):
                return builder.build_eos_skip_eos_pop(std::move(eos_val), skip, std::move(skip_eos_val), bmp, children);
            case mk_flag_switch(FLAG_EOS | FLAG_SKIP, MASK, true):
                return builder.build_eos_skip_list(std::move(eos_val), skip, lst, children);
            case mk_flag_switch(FLAG_EOS | FLAG_SKIP, MASK, false):
                return builder.build_eos_skip_pop(std::move(eos_val), skip, bmp, children);
            case mk_flag_switch(FLAG_SKIP | FLAG_SKIP_EOS, MASK, true):
                return builder.build_skip_eos_list(skip, std::move(skip_eos_val), lst, children);
            case mk_flag_switch(FLAG_SKIP | FLAG_SKIP_EOS, MASK, false):
                return builder.build_skip_eos_pop(skip, std::move(skip_eos_val), bmp, children);
            case mk_flag_switch(FLAG_SKIP, MASK, true):
                return builder.build_skip_list(skip, lst, children);
            case mk_flag_switch(FLAG_SKIP, MASK, false):
                return builder.build_skip_pop(skip, bmp, children);
            case mk_flag_switch(FLAG_EOS, MASK, true):
                return builder.build_eos_list(std::move(eos_val), lst, children);
            case mk_flag_switch(FLAG_EOS, MASK, false):
                return builder.build_eos_pop(std::move(eos_val), bmp, children);
            case mk_flag_switch(0, MASK, true):
                return builder.build_list(lst, children);
            case mk_flag_switch(0, MASK, false):
                return builder.build_pop(bmp, children);
            // Invalid: SKIP_EOS without SKIP
            case mk_flag_switch(FLAG_SKIP_EOS, MASK, true):
            case mk_flag_switch(FLAG_SKIP_EOS, MASK, false):
            case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK, true):
            case mk_flag_switch(FLAG_EOS | FLAG_SKIP_EOS, MASK, false):
            default:
                KTRIE_DEBUG_ASSERT(false && "Invalid flag combination");
                __builtin_unreachable();
        }
    }
};

// =============================================================================
// Debug utilities
// =============================================================================

template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie;

template <typename Key, typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct trie_debug {
    using slot_type = slot_type_t<THREADED>;
    using node_view_t = node_view<T, THREADED, Allocator, FIXED_LEN>;
    using dataptr_t = dataptr<T, THREADED, Allocator>;

    static std::string byte_to_string(unsigned char c) {
        if (c >= 32 && c < 127) return std::string("'") + static_cast<char>(c) + "'";
        std::ostringstream oss;
        oss << "0x" << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
        return oss.str();
    }

    static std::string string_to_printable(std::string_view s) {
        std::string result;
        for (unsigned char c : s) {
            if (c >= 32 && c < 127) result += static_cast<char>(c);
            else { std::ostringstream oss; oss << "\\x" << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c); result += oss.str(); }
        }
        return result;
    }

    static std::string flags_to_string(uint64_t flags) {
        std::string result;
        if (flags & FLAG_EOS) result += "EOS|";
        if (flags & FLAG_SKIP) result += "SKIP|";
        if (flags & FLAG_SKIP_EOS) result += "SKIP_EOS|";
        if (flags & FLAG_LIST) result += "LIST|";
        if (flags & FLAG_POP) result += "POP|";
        if (!result.empty()) result.pop_back();
        else result = "NONE";
        return result;
    }

    static void pretty_print_node(slot_type* node, std::ostream& os, int indent_level, const std::string& prefix, size_t depth) {
        if (!node) { os << std::string(indent_level * 2, ' ') << prefix << "(null)\n"; return; }
        node_view_t view(node);
        std::string indent(indent_level * 2, ' ');
        os << indent << prefix << "NODE[flags=" << flags_to_string(view.flags()) << " size=" << view.size() << " depth=" << depth << "]\n";
        if (view.has_eos()) { os << indent << "  EOS: "; T val; os << (view.eos_data()->try_read(val) ? "(has data)" : "(no data)") << "\n"; }
        if (view.has_skip()) {
            os << indent << "  SKIP[" << view.skip_length() << "]: \"" << string_to_printable(view.skip_chars()) << "\"\n";
            if (view.has_skip_eos()) { os << indent << "  SKIP_EOS: "; T val; os << (view.skip_eos_data()->try_read(val) ? "(has data)" : "(no data)") << "\n"; }
        }
        if (view.has_list()) {
            small_list lst = view.get_list();
            os << indent << "  LIST[" << lst.count() << "]: ";
            for (int i = 0; i < lst.count(); ++i) os << byte_to_string(lst.char_at(i)) << " ";
            os << "\n";
            for (int i = 0; i < lst.count(); ++i) {
                unsigned char c = lst.char_at(i);
                uint64_t child_ptr = view.get_child_ptr(i);
                std::string child_prefix = byte_to_string(c) + " -> ";
                if constexpr (FIXED_LEN > 0) { if (depth + view.skip_length() >= FIXED_LEN - 1) { os << indent << "    " << child_prefix << "(leaf)\n"; continue; } }
                pretty_print_node(reinterpret_cast<slot_type*>(child_ptr), os, indent_level + 2, child_prefix, depth + (view.has_skip() ? view.skip_length() : 0) + 1);
            }
        } else if (view.has_pop()) {
            popcount_bitmap bmp = view.get_bitmap();
            os << indent << "  POP[" << bmp.count() << " children]\n";
            for (int i = 0; i < bmp.count(); ++i) {
                unsigned char c = bmp.nth_char(i);
                uint64_t child_ptr = view.get_child_ptr(i);
                std::string child_prefix = byte_to_string(c) + " -> ";
                if constexpr (FIXED_LEN > 0) { if (depth + view.skip_length() >= FIXED_LEN - 1) { os << indent << "    " << child_prefix << "(leaf)\n"; continue; } }
                pretty_print_node(reinterpret_cast<slot_type*>(child_ptr), os, indent_level + 2, child_prefix, depth + (view.has_skip() ? view.skip_length() : 0) + 1);
            }
        }
    }

    static std::string validate_node(slot_type* node, size_t depth) {
        if (!node) return "";
        node_view_t view(node);
        uint64_t flags = view.flags();
        if ((flags & FLAG_LIST) && (flags & FLAG_POP)) return "LIST and POP both set";
        if ((flags & FLAG_SKIP_EOS) && !(flags & FLAG_SKIP)) return "SKIP_EOS without SKIP";
        if ((flags & FLAG_SKIP) && view.skip_length() == 0) return "SKIP with length 0";
        int num_children = view.child_count();
        for (int i = 0; i < num_children; ++i) {
            uint64_t child_ptr = view.get_child_ptr(i);
            if constexpr (FIXED_LEN > 0) { if (depth + (view.has_skip() ? view.skip_length() : 0) + 1 >= FIXED_LEN) continue; }
            slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
            if (child) { std::string err = validate_node(child, depth + (view.has_skip() ? view.skip_length() : 0) + 1); if (!err.empty()) return err; }
        }
        return "";
    }
};

template <typename Key, typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
inline void validate_trie_impl(slot_type_t<THREADED>* root) {
    if constexpr (!k_validate) return;
    using debug_t = trie_debug<Key, T, THREADED, Allocator, FIXED_LEN>;
    std::string err = debug_t::validate_node(root, 0);
    if (!err.empty()) KTRIE_DEBUG_ASSERT(false && "Trie validation failed");
}

}  // namespace gteitelbaum
