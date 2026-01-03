#pragma once

#include <iostream>
#include <sstream>
#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_node.h"

namespace gteitelbaum {

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct trie_helpers {
    using slot_type = slot_type_t<THREADED>;
    using node_view_t = node_view<T, THREADED, Allocator, FIXED_LEN>;
    using node_builder_t = node_builder<T, THREADED, Allocator, FIXED_LEN>;
    using dataptr_t = dataptr<T, THREADED, Allocator>;

    static size_t match_skip(std::string_view skip, std::string_view key) noexcept {
        size_t i = 0;
        while (i < skip.size() && i < key.size() && skip[i] == key[i]) ++i;
        return i;
    }

    static std::vector<uint64_t> extract_children(node_view_t& view) {
        std::vector<uint64_t> children;
        if (view.has_full()) {
            children.resize(256);
            for (int i = 0; i < 256; ++i) children[i] = view.get_child_ptr(i);
        } else if (view.leaf_has_children() || !view.has_leaf()) {
            int count = view.child_count();
            children.reserve(count);
            for (int i = 0; i < count; ++i) children.push_back(view.get_child_ptr(i));
        }
        return children;
    }

    static std::vector<T> extract_leaf_values(node_view_t& view) {
        KTRIE_DEBUG_ASSERT(view.has_leaf() && view.leaf_has_children());
        std::vector<T> values;
        if (view.has_full()) {
            values.resize(256);
            for (int i = 0; i < 256; ++i) values[i] = view.get_leaf_value(i);
        } else {
            int count = view.child_count();
            values.reserve(count);
            for (int i = 0; i < count; ++i) values.push_back(view.get_leaf_value(i));
        }
        return values;
    }

    static std::vector<unsigned char> get_child_chars(node_view_t& view) {
        std::vector<unsigned char> chars;
        if (view.leaf_has_eos()) return chars;
        if (view.has_full()) {
            if (view.has_leaf()) {
                popcount_bitmap bmp = view.get_leaf_full_bitmap();
                for (int i = 0; i < 256; ++i)
                    if (bmp.contains(static_cast<unsigned char>(i)))
                        chars.push_back(static_cast<unsigned char>(i));
            } else {
                for (int i = 0; i < 256; ++i)
                    if (view.get_child_ptr(i) != 0)
                        chars.push_back(static_cast<unsigned char>(i));
            }
        } else if (view.has_list()) {
            small_list lst = view.get_list();
            chars.reserve(lst.count());
            for (int i = 0; i < lst.count(); ++i) chars.push_back(lst.char_at(i));
        } else if (view.has_pop()) {
            popcount_bitmap bmp = view.get_bitmap();
            chars.reserve(bmp.count());
            for (int i = 0; i < bmp.count(); ++i) chars.push_back(bmp.nth_char(i));
        }
        return chars;
    }

    static std::tuple<int, small_list, popcount_bitmap>
    build_child_structure(const std::vector<unsigned char>& chars) {
        if (chars.size() <= static_cast<size_t>(LIST_MAX)) {
            small_list lst;
            for (auto c : chars) lst.add(c);
            return {0, lst, popcount_bitmap()};
        } else if (chars.size() <= static_cast<size_t>(FULL_THRESHOLD)) {
            popcount_bitmap bmp;
            for (auto c : chars) bmp.set(c);
            return {1, small_list(), bmp};
        } else {
            popcount_bitmap bmp;
            for (auto c : chars) bmp.set(c);
            return {2, small_list(), bmp};
        }
    }

    static int find_char_index(const std::vector<unsigned char>& chars, unsigned char c) {
        for (size_t i = 0; i < chars.size(); ++i)
            if (chars[i] == c) return static_cast<int>(i);
        return -1;
    }

    // Rebuild NON-LEAF node
    static slot_type* rebuild_node(node_builder_t& builder, node_view_t& view,
                                    int node_type, small_list& lst, popcount_bitmap& bmp,
                                    const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(!view.has_leaf());
        bool has_skip = view.has_skip();
        std::string_view skip = has_skip ? view.skip_chars() : std::string_view{};

        slot_type* new_node;
        if (children.empty()) {
            new_node = has_skip ? builder.build_skip(skip) : builder.build_empty();
        } else if (node_type == 2) {
            new_node = has_skip ? builder.build_skip_full(skip, children) : builder.build_full(children);
        } else if (node_type == 1) {
            new_node = has_skip ? builder.build_skip_pop(skip, bmp, children) : builder.build_pop(bmp, children);
        } else {
            new_node = has_skip ? builder.build_skip_list(skip, lst, children) : builder.build_list(lst, children);
        }

        node_view_t nv(new_node);
        nv.eos_data()->deep_copy_from(*view.eos_data());
        if (has_skip) nv.skip_eos_data()->deep_copy_from(*view.skip_eos_data());
        return new_node;
    }

    // Rebuild LEAF node with children
    static slot_type* rebuild_leaf_node(node_builder_t& builder, node_view_t& view,
                                         int node_type, small_list& lst, popcount_bitmap& bmp,
                                         const std::vector<T>& values) {
        KTRIE_DEBUG_ASSERT(view.has_leaf());
        bool has_skip = view.has_skip();
        std::string_view skip = has_skip ? view.skip_chars() : std::string_view{};

        if (values.empty()) {
            slot_type* new_node = has_skip ? builder.build_leaf_skip_terminal(skip) : builder.build_leaf_terminal();
            return new_node;
        }

        slot_type* new_node;
        if (node_type == 2) {
            auto chars = get_child_chars(view);
            std::vector<T> full_values(256);
            popcount_bitmap valid_bmp;
            for (size_t i = 0; i < chars.size() && i < values.size(); ++i) {
                full_values[chars[i]] = values[i];
                valid_bmp.set(chars[i]);
            }
            new_node = has_skip ? builder.build_leaf_skip_full(skip, valid_bmp, full_values)
                                : builder.build_leaf_full(valid_bmp, full_values);
        } else if (node_type == 1) {
            new_node = has_skip ? builder.build_leaf_skip_pop(skip, bmp, values)
                                : builder.build_leaf_pop(bmp, values);
        } else {
            new_node = has_skip ? builder.build_leaf_skip_list(skip, lst, values)
                                : builder.build_leaf_list(lst, values);
        }
        return new_node;
    }
};

template <typename Key, typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct trie_debug {
    using slot_type = slot_type_t<THREADED>;
    using node_view_t = node_view<T, THREADED, Allocator, FIXED_LEN>;

    static std::string flags_to_string(uint64_t f) {
        std::string r;
        if (f & FLAG_SKIP) r += "SKIP|";
        if (f & FLAG_LIST) r += "LIST|";
        if (f & FLAG_POP) r += "POP|";
        if (f & FLAG_FULL) r += "FULL|";
        if (f & FLAG_LEAF) r += "LEAF|";
        if (!r.empty()) r.pop_back();
        else r = "NONE";
        return r;
    }

    static std::string validate_node(slot_type* node, size_t) {
        if (!node) return "";
        node_view_t view(node);
        uint64_t f = view.flags();
        if (flags_has_list(f) && flags_has_pop(f) && !flags_has_full(f)) {
            // This is the EOS marker, valid
        } else {
            int cf = (flags_has_list(f) ? 1 : 0) + (flags_has_pop(f) ? 1 : 0);
            if (cf > 1) return "LIST and POP both set without being EOS marker";
        }
        if ((f & FLAG_LEAF) && FIXED_LEN == 0) return "LEAF flag on variable-length trie";
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
