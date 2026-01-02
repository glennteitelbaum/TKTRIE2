#pragma once

#include <string_view>
#include <vector>
#include <array>

#include "tktrie_defines.h"
#include "tktrie_help_common.h"

namespace gteitelbaum {

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct nav_helpers : trie_helpers<T, THREADED, Allocator, FIXED_LEN> {
    using base = trie_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using slot_type = typename base::slot_type;
    using node_view_t = typename base::node_view_t;
    using dataptr_t = typename base::dataptr_t;

    // Find data slot for exact key match
    // Returns: for non-leaf, pointer to dataptr slot; for leaf, pointer to embedded T slot
    // is_leaf_out: set to true if result is embedded T (not dataptr)
    static slot_type* find_data_slot(slot_type* root, std::string_view key, bool& is_leaf_out) noexcept {
        is_leaf_out = false;
        slot_type* cur = root;
        
        while (cur) {
            node_view_t view(cur);
            
            if (view.has_skip()) {
                std::string_view skip = view.skip_chars();
                size_t match = base::match_skip(skip, key);
                
                if (match < skip.size()) return nullptr;
                key.remove_prefix(match);
                
                if (key.empty()) {
                    return reinterpret_cast<slot_type*>(view.skip_eos_data());
                }
            }
            
            if (key.empty()) {
                return reinterpret_cast<slot_type*>(view.eos_data());
            }
            
            unsigned char c = static_cast<unsigned char>(key[0]);
            slot_type* child_slot = view.find_child(c);
            if (!child_slot) return nullptr;
            
            if (view.has_leaf()) {
                // Leaf node - child_slot contains T directly
                if (key.size() == 1) {
                    is_leaf_out = true;
                    return child_slot;
                }
                return nullptr;  // Key too long for leaf
            }
            
            uint64_t child_ptr = load_slot<THREADED>(child_slot);
            if (child_ptr == 0) return nullptr;
            
            cur = reinterpret_cast<slot_type*>(child_ptr);
            key.remove_prefix(1);
        }
        
        return nullptr;
    }

    static bool contains(slot_type* root, std::string_view key) noexcept {
        bool is_leaf;
        slot_type* data_slot = find_data_slot(root, key, is_leaf);
        if (!data_slot) return false;
        
        if (is_leaf) return true;  // Leaf existence = key exists
        
        dataptr_t* dp = reinterpret_cast<dataptr_t*>(data_slot);
        return dp->has_data();
    }

    static bool read(slot_type* root, std::string_view key, T& out) noexcept {
        bool is_leaf;
        slot_type* data_slot = find_data_slot(root, key, is_leaf);
        if (!data_slot) return false;
        
        if (is_leaf) {
            static_assert(can_embed_leaf_v<T>, "T must be embeddable for LEAF");
            uint64_t raw = load_slot<THREADED>(data_slot);
            std::memcpy(&out, &raw, sizeof(T));
            return true;
        }
        
        dataptr_t* dp = reinterpret_cast<dataptr_t*>(data_slot);
        return dp->try_read(out);
    }

    static slot_type* find_first_leaf(slot_type* node, std::string& key_out) noexcept {
        if (!node) return nullptr;
        
        while (true) {
            node_view_t view(node);
            
            if (view.has_skip()) {
                key_out.append(view.skip_chars());
            }
            
            // Check EOS first
            if (view.eos_data()->has_data()) {
                return reinterpret_cast<slot_type*>(view.eos_data());
            }
            
            // Check SKIP_EOS
            if (view.has_skip() && view.skip_eos_data()->has_data()) {
                return reinterpret_cast<slot_type*>(view.skip_eos_data());
            }
            
            // Find first child
            slot_type* child_slot = nullptr;
            unsigned char c = 0;
            
            if (view.has_full()) {
                if (view.has_leaf()) {
                    popcount_bitmap bmp = view.get_leaf_full_bitmap();
                    if (bmp.count() > 0) {
                        c = bmp.nth_char(0);
                        child_slot = &view.child_ptrs()[c];
                    }
                } else {
                    for (int i = 0; i < 256; ++i) {
                        if (load_slot<THREADED>(&view.child_ptrs()[i]) != 0) {
                            c = static_cast<unsigned char>(i);
                            child_slot = &view.child_ptrs()[i];
                            break;
                        }
                    }
                }
            } else if (view.has_list()) {
                auto [sorted, count] = view.get_list().sorted_chars();
                for (int i = 0; i < count; ++i) {
                    slot_type* slot = view.find_child(sorted[i]);
                    if (view.has_leaf() || load_slot<THREADED>(slot) != 0) {
                        c = sorted[i];
                        child_slot = slot;
                        break;
                    }
                }
            } else if (view.has_pop()) {
                popcount_bitmap bmp = view.get_bitmap();
                for (int i = 0; i < bmp.count(); ++i) {
                    unsigned char ch = bmp.nth_char(i);
                    if (view.has_leaf() || view.get_child_ptr(i) != 0) {
                        c = ch;
                        child_slot = &view.child_ptrs()[i];
                        break;
                    }
                }
            }
            
            if (!child_slot) return nullptr;
            
            key_out.push_back(static_cast<char>(c));
            
            if (view.has_leaf()) {
                return child_slot;  // Return embedded T slot
            }
            
            uint64_t child_ptr = load_slot<THREADED>(child_slot);
            node = reinterpret_cast<slot_type*>(child_ptr);
        }
    }
};

}  // namespace gteitelbaum
