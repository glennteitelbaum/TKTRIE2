#pragma once

#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_help_common.h"

namespace gteitelbaum {

template <bool THREADED>
struct remove_result {
    slot_type_t<THREADED>* new_root = nullptr;
    slot_type_t<THREADED>* expected_root = nullptr;
    std::vector<slot_type_t<THREADED>*> new_nodes;
    std::vector<slot_type_t<THREADED>*> old_nodes;
    std::vector<path_step<THREADED>> path;
    bool found = false;
    bool hit_write = false;
    bool hit_read = false;
    bool root_deleted = false;
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct remove_helpers : trie_helpers<T, THREADED, Allocator, FIXED_LEN> {
    using base = trie_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using slot_type = typename base::slot_type;
    using node_view_t = typename base::node_view_t;
    using node_builder_t = typename base::node_builder_t;
    using dataptr_t = typename base::dataptr_t;
    using result_t = remove_result<THREADED>;
    using path_step_t = typename base::path_step_t;

    static result_t build_remove_path(node_builder_t& builder, slot_type* root,
                                       std::string_view key, size_t depth = 0) {
        result_t result;
        result.expected_root = root;
        if (!root) return result;
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
                if (!view.has_skip_eos()) return result;
                return remove_skip_eos(builder, node, result);
            }
        }
        
        if (key.empty()) {
            if (!view.has_eos()) return result;
            return remove_eos(builder, node, result);
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        slot_type* child_slot = view.find_child(c);
        if (!child_slot) return result;
        
        uint64_t child_ptr = load_slot<THREADED>(child_slot);
        if constexpr (THREADED) {
            if (child_ptr & WRITE_BIT) { result.hit_write = true; return result; }
            if (child_ptr & READ_BIT) { result.hit_read = true; return result; }
        }
        
        uint64_t clean_ptr = child_ptr & PTR_MASK;
        
        // Double-check slot unchanged before dereferencing (race protection)
        if constexpr (THREADED) {
            uint64_t recheck = load_slot<THREADED>(child_slot);
            if (recheck != child_ptr) {
                result.hit_write = true;  // Slot changed, need restart
                return result;
            }
        }
        
        // FIXED_LEN leaf optimization: non-threaded stores dataptr inline at leaf depth
        if constexpr (FIXED_LEN > 0 && !THREADED) {
            if (depth == FIXED_LEN - 1 && key.size() == 1) {
                dataptr_t* dp = reinterpret_cast<dataptr_t*>(child_slot);
                if (!dp->has_data()) return result;
                return remove_leaf_data(builder, node, c, result);
            }
        }
        
        slot_type* child = reinterpret_cast<slot_type*>(clean_ptr);
        result_t child_result;
        child_result.hit_read = false;
        remove_from_node(builder, child, key.substr(1), depth + 1, child_result);
        
        if (!child_result.found || child_result.hit_write || child_result.hit_read) {
            result.found = child_result.found;
            result.hit_write = child_result.hit_write;
            result.hit_read = child_result.hit_read;
            return result;
        }
        
        // Record path step with slot and full expected pointer for verification
        result.path.push_back({node, child_slot, child_ptr, c});
        for (auto& step : child_result.path) result.path.push_back(step);
        
        if (child_result.root_deleted)
            return remove_child(builder, node, c, child_result, result);
        return clone_with_updated_child(builder, node, c, child_result.new_root, child_result, result);
    }

    static result_t& remove_eos(node_builder_t& builder, slot_type* node, result_t& result) {
        node_view_t view(node);
        result.found = true;
        
        bool has_skip = view.has_skip(), has_skip_eos = view.has_skip_eos();
        bool has_children = view.child_count() > 0;
        
        if (!has_skip_eos && !has_children) {
            result.root_deleted = true;
            result.old_nodes.push_back(node);
            return result;
        }
        
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        
        slot_type* new_node;
        if (has_skip && has_skip_eos) {
            T skip_eos_val; view.skip_eos_data()->try_read(skip_eos_val);
            std::string_view skip = view.skip_chars();
            if (has_children) {
                new_node = is_list ? builder.build_skip_eos_list(skip, std::move(skip_eos_val), lst, children)
                                   : builder.build_skip_eos_pop(skip, std::move(skip_eos_val), bmp, children);
            } else {
                new_node = builder.build_skip_eos(skip, std::move(skip_eos_val));
            }
        } else if (has_skip) {
            std::string_view skip = view.skip_chars();
            if (has_children) {
                new_node = is_list ? builder.build_skip_list(skip, lst, children)
                                   : builder.build_skip_pop(skip, bmp, children);
            } else {
                result.root_deleted = true; result.old_nodes.push_back(node); return result;
            }
        } else {
            if (has_children) {
                new_node = is_list ? builder.build_list(lst, children) : builder.build_pop(bmp, children);
            } else {
                result.root_deleted = true; result.old_nodes.push_back(node); return result;
            }
        }
        
        result.new_nodes.push_back(new_node);
        result.new_root = new_node;
        result.old_nodes.push_back(node);
        try_collapse(builder, result);
        return result;
    }

    static result_t& remove_skip_eos(node_builder_t& builder, slot_type* node, result_t& result) {
        node_view_t view(node);
        result.found = true;
        
        bool has_eos = view.has_eos(), has_children = view.child_count() > 0;
        std::string_view skip = view.skip_chars();
        
        if (!has_eos && !has_children) {
            result.root_deleted = true; result.old_nodes.push_back(node); return result;
        }
        
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        
        slot_type* new_node;
        if (has_eos) {
            T eos_val; view.eos_data()->try_read(eos_val);
            if (has_children) {
                new_node = is_list ? builder.build_eos_list(std::move(eos_val), lst, children)
                                   : builder.build_eos_pop(std::move(eos_val), bmp, children);
            } else {
                new_node = builder.build_eos(std::move(eos_val));
            }
        } else {
            if (has_children) {
                new_node = is_list ? builder.build_skip_list(skip, lst, children)
                                   : builder.build_skip_pop(skip, bmp, children);
            } else {
                result.root_deleted = true; result.old_nodes.push_back(node); return result;
            }
        }
        
        result.new_nodes.push_back(new_node);
        result.new_root = new_node;
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
        
        if (idx >= 0) { children.erase(children.begin() + idx); chars.erase(chars.begin() + idx); }
        
        bool has_eos = view.has_eos(), has_skip_eos = view.has_skip_eos();
        (void)view.has_skip();  // Used indirectly via base::rebuild_node
        if (children.empty() && !has_eos && !has_skip_eos) {
            result.root_deleted = true; result.old_nodes.push_back(node); return result;
        }
        
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        slot_type* new_node = base::rebuild_node(builder, view, is_list, lst, bmp, children);
        result.new_nodes.push_back(new_node);
        result.new_root = new_node;
        result.old_nodes.push_back(node);
        try_collapse(builder, result);
        return result;
    }

    static result_t& clone_with_updated_child(node_builder_t& builder, slot_type* node, unsigned char c,
                                               slot_type* new_child, result_t& child_result, result_t& result) {
        for (auto* n : child_result.new_nodes) result.new_nodes.push_back(n);
        for (auto* n : child_result.old_nodes) result.old_nodes.push_back(n);
        result.found = true;
        
        node_view_t view(node);
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        int idx = base::find_char_index(chars, c);
        
        if (idx >= 0) children[idx] = reinterpret_cast<uint64_t>(new_child);
        
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        slot_type* new_node = base::rebuild_node(builder, view, is_list, lst, bmp, children);
        result.new_nodes.push_back(new_node);
        result.new_root = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    static result_t& remove_leaf_data(node_builder_t& builder, slot_type* node, unsigned char c,
                                       result_t& result) {
        result.found = true;
        node_view_t view(node);
        auto children = base::extract_children(view);
        auto chars = base::get_child_chars(view);
        
        int idx = base::find_char_index(chars, c);
        
        // Destroy the dataptr before removing
        if (idx >= 0) {
            slot_type* child_slot = view.find_child(c);
            dataptr_t* dp = reinterpret_cast<dataptr_t*>(child_slot);
            dp->~dataptr_t();
            children.erase(children.begin() + idx);
            chars.erase(chars.begin() + idx);
        }
        
        if (children.empty()) { result.root_deleted = true; result.old_nodes.push_back(node); return result; }
        
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        slot_type* new_node = base::rebuild_node(builder, view, is_list, lst, bmp, children);
        result.new_nodes.push_back(new_node);
        result.new_root = new_node;
        result.old_nodes.push_back(node);
        return result;
    }

    static void try_collapse(node_builder_t& builder, result_t& result) {
        if (!result.new_root) return;
        node_view_t view(result.new_root);
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
        if (child_view.has_skip()) new_skip.append(child_view.skip_chars());
        
        auto children = base::extract_children(child_view);
        auto chars = base::get_child_chars(child_view);
        auto [is_list, lst, bmp] = base::build_child_structure(chars);
        
        bool child_has_eos = child_view.has_eos(), child_has_skip_eos = child_view.has_skip_eos();
        T eos_val, skip_eos_val;
        if (child_has_eos) child_view.eos_data()->try_read(eos_val);
        if (child_has_skip_eos) child_view.skip_eos_data()->try_read(skip_eos_val);
        
        slot_type* collapsed;
        if (children.empty()) {
            if (child_has_eos && child_has_skip_eos) collapsed = builder.build_eos_skip_eos(std::move(eos_val), new_skip, std::move(skip_eos_val));
            else if (child_has_eos) collapsed = builder.build_skip_eos(new_skip, std::move(eos_val));
            else if (child_has_skip_eos) collapsed = builder.build_skip_eos(new_skip, std::move(skip_eos_val));
            else return;
        } else {
            if (child_has_eos && child_has_skip_eos) return;
            else if (child_has_eos) collapsed = builder.build_skip_eos_list(new_skip, std::move(eos_val), lst, children);
            else if (child_has_skip_eos) collapsed = is_list ? builder.build_skip_eos_list(new_skip, std::move(skip_eos_val), lst, children)
                                                              : builder.build_skip_eos_pop(new_skip, std::move(skip_eos_val), bmp, children);
            else collapsed = is_list ? builder.build_skip_list(new_skip, lst, children) : builder.build_skip_pop(new_skip, bmp, children);
        }
        
        result.old_nodes.push_back(result.new_root);
        result.old_nodes.push_back(child);  // Child is absorbed into collapsed node
        result.new_root = collapsed;
        result.new_nodes.push_back(collapsed);
    }
};

}  // namespace gteitelbaum
