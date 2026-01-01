#pragma once

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "tktrie_defines.h"
#include "tktrie_node.h"

namespace gteitelbaum {

// Forward declaration
template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie;

/**
 * Debug utilities for tktrie
 */
template <typename Key, typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct trie_debug {
    using slot_type = slot_type_t<THREADED>;
    using node_view_t = node_view<T, THREADED, Allocator, FIXED_LEN>;
    using dataptr_t = dataptr<T, THREADED, Allocator>;

    /**
     * Convert byte to printable representation
     */
    static std::string byte_to_string(unsigned char c) {
        if (c >= 32 && c < 127) {
            return std::string("'") + static_cast<char>(c) + "'";
        } else {
            std::ostringstream oss;
            oss << "0x" << std::hex << std::setw(2) << std::setfill('0') 
                << static_cast<int>(c);
            return oss.str();
        }
    }

    /**
     * Convert string to printable representation
     */
    static std::string string_to_printable(std::string_view s) {
        std::string result;
        result.reserve(s.size() * 4);
        for (unsigned char c : s) {
            if (c >= 32 && c < 127) {
                result += static_cast<char>(c);
            } else {
                std::ostringstream oss;
                oss << "\\x" << std::hex << std::setw(2) << std::setfill('0')
                    << static_cast<int>(c);
                result += oss.str();
            }
        }
        return result;
    }

    /**
     * Get flags as string
     */
    static std::string flags_to_string(uint64_t flags) {
        std::string result;
        if (flags & FLAG_EOS) result += "EOS|";
        if (flags & FLAG_SKIP) result += "SKIP|";
        if (flags & FLAG_SKIP_EOS) result += "SKIP_EOS|";
        if (flags & FLAG_LIST) result += "LIST|";
        if (flags & FLAG_POP) result += "POP|";
        if (!result.empty()) result.pop_back();  // Remove trailing |
        else result = "NONE";
        return result;
    }

    /**
     * Pretty print a single node
     */
    static void pretty_print_node(slot_type* node, std::ostream& os,
                                   int indent_level, const std::string& prefix,
                                   size_t depth) {
        if (!node) {
            os << std::string(indent_level * 2, ' ') << prefix << "(null)\n";
            return;
        }

        node_view_t view(node);
        std::string indent(indent_level * 2, ' ');

        os << indent << prefix << "NODE[";
        os << " flags=" << flags_to_string(view.flags());
        os << " ver=" << view.version();
        os << " size=" << view.size();
        os << " depth=" << depth;
        os << " ]\n";

        // EOS data
        if (view.has_eos()) {
            os << indent << "  EOS: ";
            T val;
            if (view.eos_data()->try_read(val)) {
                os << "(has data)";
            } else {
                os << "(no data or locked)";
            }
            os << "\n";
        }

        // Skip sequence
        if (view.has_skip()) {
            os << indent << "  SKIP[" << view.skip_length() << "]: \""
               << string_to_printable(view.skip_chars()) << "\"\n";

            if (view.has_skip_eos()) {
                os << indent << "  SKIP_EOS: ";
                T val;
                if (view.skip_eos_data()->try_read(val)) {
                    os << "(has data)";
                } else {
                    os << "(no data or locked)";
                }
                os << "\n";
            }
        }

        // Children
        if (view.has_list()) {
            small_list lst = view.get_list();
            os << indent << "  LIST[" << lst.count() << "]: ";
            for (int i = 0; i < lst.count(); ++i) {
                os << byte_to_string(lst.char_at(i)) << " ";
            }
            os << "\n";

            // Recurse into children
            for (int i = 0; i < lst.count(); ++i) {
                unsigned char c = lst.char_at(i);
                uint64_t child_ptr = view.get_child_ptr(i);
                
                if constexpr (THREADED) {
                    child_ptr &= PTR_MASK;
                }

                std::string child_prefix = byte_to_string(c) + " -> ";
                
                // For fixed_len at leaf depth, children are dataptr
                if constexpr (FIXED_LEN > 0) {
                    if (depth + view.skip_length() >= FIXED_LEN - 1) {
                        os << indent << "    " << child_prefix << "(leaf dataptr)\n";
                        continue;
                    }
                }

                slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
                size_t child_depth = depth + (view.has_skip() ? view.skip_length() : 0) + 1;
                pretty_print_node(child, os, indent_level + 2, child_prefix, child_depth);
            }
        } else if (view.has_pop()) {
            popcount_bitmap bmp = view.get_bitmap();
            os << indent << "  POP[" << bmp.count() << " children]\n";

            for (int i = 0; i < bmp.count(); ++i) {
                unsigned char c = bmp.nth_char(i);
                uint64_t child_ptr = view.get_child_ptr(i);
                
                if constexpr (THREADED) {
                    child_ptr &= PTR_MASK;
                }

                std::string child_prefix = byte_to_string(c) + " -> ";
                
                if constexpr (FIXED_LEN > 0) {
                    if (depth + view.skip_length() >= FIXED_LEN - 1) {
                        os << indent << "    " << child_prefix << "(leaf dataptr)\n";
                        continue;
                    }
                }

                slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
                size_t child_depth = depth + (view.has_skip() ? view.skip_length() : 0) + 1;
                pretty_print_node(child, os, indent_level + 2, child_prefix, child_depth);
            }
        }
    }

    /**
     * Validate a single node
     * Returns error message or empty string if valid
     */
    static std::string validate_node(slot_type* node, size_t depth) {
        if (!node) return "";

        node_view_t view(node);
        uint64_t flags = view.flags();

        // Invariant 1: LIST and POP mutually exclusive
        if ((flags & FLAG_LIST) && (flags & FLAG_POP)) {
            return "Invariant 1 violated: LIST and POP both set";
        }

        // Invariant 2: SKIP_EOS requires SKIP
        if ((flags & FLAG_SKIP_EOS) && !(flags & FLAG_SKIP)) {
            return "Invariant 2 violated: SKIP_EOS without SKIP";
        }

        // Invariant 3: SKIP length > 0
        if ((flags & FLAG_SKIP) && view.skip_length() == 0) {
            return "Invariant 3 violated: SKIP with length 0";
        }

        // Invariant 4: LIST count 1 requires SKIP
        if ((flags & FLAG_LIST) && view.get_list().count() == 1 && !(flags & FLAG_SKIP)) {
            return "Invariant 4 violated: LIST count 1 without SKIP";
        }

        // Invariant 6: fixed_len EOS/SKIP_EOS restrictions
        if constexpr (FIXED_LEN > 0) {
            size_t effective_depth = depth + (view.has_skip() ? view.skip_length() : 0);
            
            if (effective_depth < FIXED_LEN) {
                if (flags & FLAG_EOS) {
                    return "Invariant 6 violated: EOS at non-leaf depth for fixed_len";
                }
                if (flags & FLAG_SKIP_EOS) {
                    // SKIP_EOS might be okay if skip brings us to leaf depth
                    if (depth + view.skip_length() < FIXED_LEN) {
                        return "Invariant 6 violated: SKIP_EOS at non-leaf depth for fixed_len";
                    }
                }
            }
        }

        // Recursively validate children
        int num_children = view.child_count();
        for (int i = 0; i < num_children; ++i) {
            uint64_t child_ptr = view.get_child_ptr(i);
            
            if constexpr (THREADED) {
                child_ptr &= PTR_MASK;
            }

            // For fixed_len at leaf depth, children are dataptr
            if constexpr (FIXED_LEN > 0) {
                size_t effective_depth = depth + (view.has_skip() ? view.skip_length() : 0) + 1;
                if (effective_depth >= FIXED_LEN) {
                    continue;  // Leaf dataptr, not a node
                }
            }

            slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
            if (child) {
                size_t child_depth = depth + (view.has_skip() ? view.skip_length() : 0) + 1;
                std::string err = validate_node(child, child_depth);
                if (!err.empty()) return err;
            }
        }

        return "";
    }
};

/**
 * Validation helper - called after modifications when k_validate is true
 */
template <typename Key, typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
inline void validate_trie_impl(slot_type_t<THREADED>* root) {
    if constexpr (!k_validate) return;

    using debug_t = trie_debug<Key, T, THREADED, Allocator, FIXED_LEN>;
    std::string err = debug_t::validate_node(root, 0);
    if (!err.empty()) {
        KTRIE_DEBUG_ASSERT(false && "Trie validation failed");
    }
}

}  // namespace gteitelbaum
