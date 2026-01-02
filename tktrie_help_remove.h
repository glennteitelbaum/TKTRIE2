#pragma once

#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_help_common.h"

namespace gteitelbaum {

template <bool THREADED>
struct remove_result {
    slot_type_t<THREADED>* new_subtree = nullptr;
    slot_type_t<THREADED>* target_slot = nullptr;
    uint64_t expected_ptr = 0;
    std::vector<slot_type_t<THREADED>*> new_nodes;
    std::vector<slot_type_t<THREADED>*> old_nodes;
    bool found = false;
    bool subtree_deleted = false;
    
    remove_result() { new_nodes.reserve(16); old_nodes.reserve(16); }
    
    bool path_has_conflict() const noexcept {
        if constexpr (THREADED) {
            if (target_slot) {
                uint64_t current = load_slot<THREADED>(target_slot);
                if (current != expected_ptr) return true;
            }
        }
        return false;
    }
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct remove_helpers : trie_helpers<T, THREADED, Allocator, FIXED_LEN> {
    using base = trie_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using slot_type = typename base::slot_type;
    using node_view_t = typename base::node_view_t;
    using node_builder_t = typename base::node_builder_t;
    using dataptr_t = typename base::dataptr_t;
    using result_t = remove_result<THREADED>;

    static result_t build_remove_path(node_builder_t& builder, slot_type* root_slot, slot_type* root,
                                       std::string_view key, size_t depth = 0) {
        result_t result;
        if (!root) return result;
        
        result.target_slot = root_slot;
        result.expected_ptr = reinterpret_cast<uint64_t>(root);
        
        return remove_from_node(builder, root, key, depth, result);
    }

private:
    static result_t& remove_from_node(node_builder_t& builder, slot_type* node,
                                       std::string_view key, size_t depth, result_t& result) {
        node_view_t view(node);
        
        if (view.has_skip()) {
            std::string_view skip = view.skip_chars();
            size_t match = base::match_skip(skip, key);
            if (match < skip.size()) return result;
            key.remove_prefix(match);
            depth += match;
            if (key.empty()) {
                if (!view.skip_eos_data()->has_data()) return result;
                return remove_skip_eos(builder, node, result);
            }
        }
        
        if (key.empty()) {
            if (!view.eos_data()->has_data()) return result;
            return remove_eos(builder, node, result);
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        slot_type* child_slot = view.find_child(c);
        if (!child_slot) return result;
        
        if (view.has_leaf()) {
            if (key.size() == 1) {
                return remove_leaf_child(builder, node, c, result);
            }
            return result;  // Key not found
        }
        
        uint64_t child_ptr = load_slot<THREADED>(child_slot);
        if (child_ptr == 0) return result;
        
        slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
        result_t child_result;
        remove_from_node(builder, child, key.substr(1), depth + 1, child_result);
        
        if (!child_result.found) {
            result.found = false;
            return result;
        }
        
        if (child_result.subtree_deleted) {
            return remove_child(builder, node, c, child_result, result);
        } else {
            result.found = true;
            for (auto* n : child_result.new_nodes) result.new_nodes.push_back(n);
            for (auto* n : child_result.old_nodes) result.old_nodes.push_back(n);
            return rebuild_with_new_child(builder, node, c, child_result.new_subtree, result);
        }
    }

    static result_t& rebuild_with_new_child(node_builder_t& builder, slot_type* node, unsigned char c,
                                             slot_type* new_child, result_t& result) {
        node_view_t view(node);
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        int idx = base::find_char_index(chars, c);
        if (idx >= 0) {
            if (view.has_full()) {
                children[c] = reinterpret_cast<uint64_t>(new_child);
            } else {
                children[idx] = reinterpret_cast<uint64_t>(new_child);
            }
        }
        
        auto [node_type, lst, bmp] = base::build_child_structure(chars);
        slot_type* new_node = base::rebuild_node(builder, view, node_type, lst, bmp, children);
        
        result.new_nodes.push_back(new_node);
        result.old_nodes.push_back(node);
        result.new_subtree = new_node;
        return result;
    }

    static result_t& remove_eos(node_builder_t& builder, slot_type* node, result_t& result) {
        node_view_t view(node);
        result.found = true;
        
        bool has_skip_eos = view.has_skip() && view.skip_eos_data()->has_data();
        bool has_children = view.live_child_count() > 0;
        
        if (!has_skip_eos && !has_children) {
            result.subtree_deleted = true;
            result.old_nodes.push_back(node);
            return result;
        }
        
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        auto [node_type, lst, bmp] = base::build_child_structure(chars);
        
        slot_type* new_node = base::rebuild_node(builder, view, node_type, lst, bmp, children);
        node_view_t nv(new_node);
        nv.eos_data()->clear();  // Remove EOS data
        
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.old_nodes.push_back(node);
        try_collapse(builder, result);
        return result;
    }

    static result_t& remove_skip_eos(node_builder_t& builder, slot_type* node, result_t& result) {
        node_view_t view(node);
        result.found = true;
        
        bool has_eos = view.eos_data()->has_data();
        bool has_children = view.live_child_count() > 0;
        
        if (!has_eos && !has_children) {
            result.subtree_deleted = true;
            result.old_nodes.push_back(node);
            return result;
        }
        
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        auto [node_type, lst, bmp] = base::build_child_structure(chars);
        
        slot_type* new_node = base::rebuild_node(builder, view, node_type, lst, bmp, children);
        node_view_t nv(new_node);
        if (nv.has_skip()) nv.skip_eos_data()->clear();
        
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.old_nodes.push_back(node);
        try_collapse(builder, result);
        return result;
    }

    static result_t& remove_child(node_builder_t& builder, slot_type* node, unsigned char c,
                                   result_t& child_result, result_t& result) {
        for (auto* n : child_result.new_nodes) result.new_nodes.push_back(n);
        for (auto* n : child_result.old_nodes) result.old_nodes.push_back(n);
        result.found = true;
        
        node_view_t view(node);
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        int idx = base::find_char_index(chars, c);
        if (view.has_full()) {
            children[c] = 0;
            chars.erase(std::remove(chars.begin(), chars.end(), c), chars.end());
        } else if (idx >= 0) {
            children.erase(children.begin() + idx);
            chars.erase(chars.begin() + idx);
        }
        
        bool has_eos = view.eos_data()->has_data();
        bool has_skip_eos = view.has_skip() && view.skip_eos_data()->has_data();
        
        if (chars.empty() && !has_eos && !has_skip_eos) {
            result.subtree_deleted = true;
            result.old_nodes.push_back(node);
            return result;
        }
        
        auto [node_type, lst, bmp] = base::build_child_structure(chars);
        
        // Possibly downgrade from FULL
        if (node_type != 2 && view.has_full()) {
            std::vector<uint64_t> new_children;
            for (auto ch : chars) new_children.push_back(children[ch]);
            children = std::move(new_children);
        }
        
        slot_type* new_node = base::rebuild_node(builder, view, node_type, lst, bmp, children);
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.old_nodes.push_back(node);
        try_collapse(builder, result);
        return result;
    }

    static result_t& remove_leaf_child(node_builder_t& builder, slot_type* node, unsigned char c, result_t& result) {
        result.found = true;
        node_view_t view(node);
        
        auto values = base::extract_leaf_values(view);
        auto chars = base::get_child_chars(view);
        
        int idx = base::find_char_index(chars, c);
        
        if (view.has_full()) {
            // LEAF|FULL: update validity bitmap
            popcount_bitmap valid_bmp = view.get_leaf_full_bitmap();
            valid_bmp.clear(c);
            chars.erase(std::remove(chars.begin(), chars.end(), c), chars.end());
            
            bool has_eos = view.eos_data()->has_data();
            bool has_skip_eos = view.has_skip() && view.skip_eos_data()->has_data();
            
            if (chars.empty() && !has_eos && !has_skip_eos) {
                result.subtree_deleted = true;
                result.old_nodes.push_back(node);
                return result;
            }
            
            // Check if we can downgrade from FULL
            if (chars.size() <= static_cast<size_t>(FULL_THRESHOLD)) {
                auto [node_type, lst, bmp] = base::build_child_structure(chars);
                std::vector<T> new_values;
                for (auto ch : chars) new_values.push_back(values[ch]);
                slot_type* new_node = base::rebuild_leaf_node(builder, view, node_type, lst, bmp, new_values);
                result.new_nodes.push_back(new_node);
                result.new_subtree = new_node;
                result.old_nodes.push_back(node);
                return result;
            }
            
            // Stay as LEAF|FULL
            slot_type* new_node = view.has_skip() 
                ? builder.build_skip_leaf_full(view.skip_chars(), valid_bmp, values)
                : builder.build_leaf_full(valid_bmp, values);
            node_view_t nv(new_node);
            nv.eos_data()->deep_copy_from(*view.eos_data());
            if (view.has_skip()) nv.skip_eos_data()->deep_copy_from(*view.skip_eos_data());
            
            result.new_nodes.push_back(new_node);
            result.new_subtree = new_node;
            result.old_nodes.push_back(node);
            return result;
        }
        
        // LIST or POP
        if (idx >= 0) {
            values.erase(values.begin() + idx);
            chars.erase(chars.begin() + idx);
        }
        
        bool has_eos = view.eos_data()->has_data();
        bool has_skip_eos = view.has_skip() && view.skip_eos_data()->has_data();
        
        if (chars.empty() && !has_eos && !has_skip_eos) {
            result.subtree_deleted = true;
            result.old_nodes.push_back(node);
            return result;
        }
        
        auto [node_type, lst, bmp] = base::build_child_structure(chars);
        slot_type* new_node = base::rebuild_leaf_node(builder, view, node_type, lst, bmp, values);
        result.new_nodes.push_back(new_node);
        result.new_subtree = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    static void try_collapse(node_builder_t& builder, result_t& result) {
        if (!result.new_subtree) return;
        node_view_t view(result.new_subtree);
        
        if (view.eos_data()->has_data()) return;
        if (view.has_skip() && view.skip_eos_data()->has_data()) return;
        if (view.live_child_count() != 1) return;
        if (view.has_leaf()) return;  // Don't collapse leaf nodes
        
        // Single child - could collapse
        auto chars = base::get_child_chars(view);
        if (chars.empty()) return;
        
        unsigned char c = chars[0];
        slot_type* child_slot = view.find_child(c);
        if (!child_slot) return;
        
        uint64_t child_ptr = load_slot<THREADED>(child_slot);
        if (child_ptr == 0) return;
        
        slot_type* child = reinterpret_cast<slot_type*>(child_ptr);
        node_view_t child_view(child);
        
        if (child_view.has_leaf()) return;  // Don't collapse into leaf
        
        // Build new skip = old_skip + c + child_skip
        std::string new_skip;
        if (view.has_skip()) new_skip = std::string(view.skip_chars());
        new_skip.push_back(static_cast<char>(c));
        if (child_view.has_skip()) new_skip.append(child_view.skip_chars());
        
        auto child_children = base::extract_children(child_view);
        auto child_chars = base::get_child_chars(child_view);
        
        slot_type* collapsed;
        if (child_children.empty()) {
            collapsed = builder.build_skip(new_skip);
            node_view_t cv(collapsed);
            // Child's EOS becomes our SKIP_EOS
            cv.skip_eos_data()->deep_copy_from(*child_view.eos_data());
        } else {
            auto [node_type, lst, bmp] = base::build_child_structure(child_chars);
            if (node_type == 2) {
                collapsed = builder.build_skip_full(new_skip, child_children);
            } else if (node_type == 1) {
                collapsed = builder.build_skip_pop(new_skip, bmp, child_children);
            } else {
                collapsed = builder.build_skip_list(new_skip, lst, child_children);
            }
            node_view_t cv(collapsed);
            cv.skip_eos_data()->deep_copy_from(*child_view.eos_data());
        }
        
        result.old_nodes.push_back(result.new_subtree);
        result.old_nodes.push_back(child);
        result.new_subtree = collapsed;
        result.new_nodes.push_back(collapsed);
    }
};

}  // namespace gteitelbaum
