#pragma once

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "tktrie_defines.h"
#include "tktrie_node.h"

namespace gteitelbaum {

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
        os << indent << prefix << "NODE[flags=" << flags_to_string(view.flags()) << " ver=" << view.version() << " size=" << view.size() << " depth=" << depth << "]\n";
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
                if constexpr (THREADED) child_ptr &= PTR_MASK;
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
                if constexpr (THREADED) child_ptr &= PTR_MASK;
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
            if constexpr (THREADED) child_ptr &= PTR_MASK;
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
