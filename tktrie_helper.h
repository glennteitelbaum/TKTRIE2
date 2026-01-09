#pragma once

// =============================================================================
// TKTRIE UNIFIED HELPERS
// =============================================================================

#include <cstddef>
#include <string_view>
#include <type_traits>

#include "tktrie_node.h"

namespace gteitelbaum {

template <typename Node>
constexpr int node_max_count() {
    if constexpr (requires { Node::MAX_ENTRIES; })
        return Node::MAX_ENTRIES;
    else
        return 256;
}

template <typename T, typename PtrT, bool IS_LEAF>
using entry_t = std::conditional_t<IS_LEAF, const T&, PtrT>;

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct trie_ops {
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = base_t*;
    using builder_t = node_builder<T, THREADED, Allocator, FIXED_LEN>;
    
    struct result {
        ptr_t new_node = nullptr;
        ptr_t old_node = nullptr;
        bool success = false;
        bool in_place = false;
    };
    
    template <typename SrcNode>
    static void copy_eos_to(SrcNode* src, ptr_t dst_base) {
        if constexpr (FIXED_LEN == 0) {
            T eos_val;
            if (src->eos().try_read(eos_val)) {
                if (dst_base->is_binary()) {
                    dst_base->template as_binary<false>()->eos().set(eos_val);
                } else if (dst_base->is_list()) {
                    dst_base->template as_list<false>()->eos().set(eos_val);
                } else if (dst_base->is_pop()) {
                    dst_base->template as_pop<false>()->eos().set(eos_val);
                } else if (dst_base->is_full()) {
                    dst_base->template as_full<false>()->eos().set(eos_val);
                }
                dst_base->set_eos_flag();
            }
        }
    }
    
    template <bool IS_LEAF, int SRC_MAX>
    static ptr_t make_upgraded(std::string_view skip, builder_t& builder) {
        if constexpr (SRC_MAX == BINARY_MAX) {
            return builder.template make_list<IS_LEAF>(skip);
        } else if constexpr (SRC_MAX == LIST_MAX) {
            return builder.template make_pop<IS_LEAF>(skip);
        } else if constexpr (SRC_MAX == POP_MAX) {
            return builder.template make_full<IS_LEAF>(skip);
        } else {
            return nullptr;
        }
    }
    
    template <bool IS_LEAF, int SRC_MAX>
    static ptr_t make_downgraded(std::string_view skip, builder_t& builder) {
        if constexpr (SRC_MAX == LIST_MAX) {
            return builder.template make_binary<IS_LEAF>(skip);
        } else if constexpr (SRC_MAX == POP_MAX) {
            return builder.template make_list<IS_LEAF>(skip);
        } else if constexpr (SRC_MAX == 256) {
            return builder.template make_pop<IS_LEAF>(skip);
        } else {
            return nullptr;
        }
    }
    
    template <bool IS_LEAF, typename SrcNode, typename DstNode>
    static void copy_entries(SrcNode* src, DstNode* dst) {
        int cnt = src->count();
        constexpr int MAX = node_max_count<SrcNode>();
        
        if constexpr (MAX == BINARY_MAX || MAX == LIST_MAX) {
            for (int i = 0; i < cnt; ++i) {
                unsigned char c = src->char_at(i);
                if constexpr (IS_LEAF) {
                    T val{};
                    src->value_at(i).try_read(val);
                    dst->add_entry(c, val);
                } else {
                    dst->add_entry(c, src->child_at_slot(i));
                }
            }
        } else {
            int slot = 0;
            src->valid().for_each_set([&](unsigned char c) {
                if constexpr (IS_LEAF) {
                    T val{};
                    if constexpr (MAX == POP_MAX) {
                        src->element_at_slot(slot).try_read(val);
                    } else {
                        src->read_value(c, val);
                    }
                    dst->add_entry(c, val);
                } else {
                    if constexpr (MAX == POP_MAX) {
                        dst->add_entry(c, src->child_at_slot(slot));
                    } else {
                        dst->add_entry(c, src->get_child(c));
                    }
                }
                ++slot;
            });
        }
    }
    
    template <bool IS_LEAF, typename SrcNode, typename DstNode>
    static void copy_entries_except(SrcNode* src, DstNode* dst, unsigned char skip_c) {
        int cnt = src->count();
        constexpr int MAX = node_max_count<SrcNode>();
        
        if constexpr (MAX == BINARY_MAX || MAX == LIST_MAX) {
            for (int i = 0; i < cnt; ++i) {
                unsigned char c = src->char_at(i);
                if (c == skip_c) continue;
                if constexpr (IS_LEAF) {
                    T val{};
                    src->value_at(i).try_read(val);
                    dst->add_entry(c, val);
                } else {
                    dst->add_entry(c, src->child_at_slot(i));
                }
            }
        } else {
            int slot = 0;
            src->valid().for_each_set([&](unsigned char c) {
                if (c != skip_c) {
                    if constexpr (IS_LEAF) {
                        T val{};
                        if constexpr (MAX == POP_MAX) {
                            src->element_at_slot(slot).try_read(val);
                        } else {
                            src->read_value(c, val);
                        }
                        dst->add_entry(c, val);
                    } else {
                        if constexpr (MAX == POP_MAX) {
                            dst->add_entry(c, src->child_at_slot(slot));
                        } else {
                            dst->add_entry(c, src->get_child(c));
                        }
                    }
                }
                ++slot;
            });
        }
    }
    
    template <bool SPECULATIVE, bool IS_LEAF, typename SrcNode, typename Alloc = void>
    static result upgrade(
        ptr_t src_base, SrcNode* src, unsigned char c,
        entry_t<T, ptr_t, IS_LEAF> entry,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        result res;
        constexpr int MAX = node_max_count<SrcNode>();
        
        ptr_t dst_base = make_upgraded<IS_LEAF, MAX>(src_base->skip_str(), builder);
        if (!dst_base) return res;
        
        if constexpr (MAX == BINARY_MAX) {
            auto* dst = dst_base->template as_list<IS_LEAF>();
            copy_entries<IS_LEAF>(src, dst);
            if constexpr (!IS_LEAF) copy_eos_to(src, dst_base);
            dst->add_entry(c, entry);
            dst->update_capacity_flags();
        } else if constexpr (MAX == LIST_MAX) {
            auto* dst = dst_base->template as_pop<IS_LEAF>();
            copy_entries<IS_LEAF>(src, dst);
            if constexpr (!IS_LEAF) copy_eos_to(src, dst_base);
            dst->add_entry(c, entry);
            dst->update_capacity_flags();
        } else if constexpr (MAX == POP_MAX) {
            auto* dst = dst_base->template as_full<IS_LEAF>();
            copy_entries<IS_LEAF>(src, dst);
            if constexpr (!IS_LEAF) copy_eos_to(src, dst_base);
            dst->add_entry(c, entry);
            dst->update_capacity_flags();
        }
        
        if constexpr (SPECULATIVE) {
            if constexpr (THREADED) dst_base->poison();
            alloc->root_replacement = dst_base;
            alloc->add(dst_base);
        } else {
            res.new_node = dst_base;
            res.old_node = src_base;
        }
        res.success = true;
        return res;
    }
    
    template <bool SPECULATIVE, bool IS_LEAF, typename Alloc = void>
    static result upgrade(
        ptr_t node, unsigned char c,
        entry_t<T, ptr_t, IS_LEAF> entry,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        uint64_t h = node->header();
        
        if ((h & (FLAG_BINARY | FLAG_LIST)) != 0) [[likely]] {
            if ((h & FLAG_BINARY) != 0) [[likely]] {
                return upgrade<SPECULATIVE, IS_LEAF>(node, node->template as_binary<IS_LEAF>(), c, entry, builder, alloc);
            } else {
                return upgrade<SPECULATIVE, IS_LEAF>(node, node->template as_list<IS_LEAF>(), c, entry, builder, alloc);
            }
        } else {
            if ((h & FLAG_POP) != 0) [[likely]] {
                return upgrade<SPECULATIVE, IS_LEAF>(node, node->template as_pop<IS_LEAF>(), c, entry, builder, alloc);
            } else {
                result res;
                return res;
            }
        }
    }
    
    template <bool SPECULATIVE, bool IS_LEAF, typename Alloc = void>
    static result add_entry(
        ptr_t node, unsigned char c,
        entry_t<T, ptr_t, IS_LEAF> entry,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        uint64_t h = node->header();
        
        if ((h & (FLAG_BINARY | FLAG_LIST)) != 0) [[likely]] {
            if ((h & FLAG_BINARY) != 0) [[likely]] {
                return add_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_binary<IS_LEAF>(), c, entry, builder, alloc);
            } else {
                return add_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_list<IS_LEAF>(), c, entry, builder, alloc);
            }
        } else {
            if ((h & FLAG_POP) != 0) [[likely]] {
                return add_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_pop<IS_LEAF>(), c, entry, builder, alloc);
            } else {
                return add_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_full<IS_LEAF>(), c, entry, builder, alloc);
            }
        }
    }
    
    template <bool SPECULATIVE, bool IS_LEAF, typename Node, typename Alloc>
    static result add_entry_typed(
        ptr_t node_base, Node* node, unsigned char c,
        entry_t<T, ptr_t, IS_LEAF> entry,
        builder_t& builder, [[maybe_unused]] Alloc* alloc)
    {
        result res;
        
        if (node->has(c)) return res;
        
        constexpr int MAX = node_max_count<Node>();
        
        if (node->count() < MAX) {
            node_base->bump_version();
            node->add_entry(c, entry);
            node->update_capacity_flags();
            res.in_place = true;
            res.success = true;
            return res;
        }
        
        return upgrade<SPECULATIVE, IS_LEAF>(node_base, node, c, entry, builder, alloc);
    }
    
    template <bool SPECULATIVE, bool IS_LEAF, typename SrcNode, typename Alloc = void>
    static result downgrade(
        ptr_t src_base, SrcNode* src, unsigned char removed_c,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        result res;
        constexpr int MAX = node_max_count<SrcNode>();
        
        ptr_t dst_base = make_downgraded<IS_LEAF, MAX>(src_base->skip_str(), builder);
        if (!dst_base) return res;
        
        if constexpr (MAX == LIST_MAX) {
            auto* dst = dst_base->template as_binary<IS_LEAF>();
            copy_entries_except<IS_LEAF>(src, dst, removed_c);
            if constexpr (!IS_LEAF) copy_eos_to(src, dst_base);
            dst->update_capacity_flags();
        } else if constexpr (MAX == POP_MAX) {
            auto* dst = dst_base->template as_list<IS_LEAF>();
            copy_entries_except<IS_LEAF>(src, dst, removed_c);
            if constexpr (!IS_LEAF) copy_eos_to(src, dst_base);
            dst->update_capacity_flags();
        } else if constexpr (MAX == 256) {
            auto* dst = dst_base->template as_pop<IS_LEAF>();
            copy_entries_except<IS_LEAF>(src, dst, removed_c);
            if constexpr (!IS_LEAF) copy_eos_to(src, dst_base);
            dst->update_capacity_flags();
        }
        
        if constexpr (SPECULATIVE) {
            if constexpr (THREADED) dst_base->poison();
            alloc->replacement = dst_base;
            alloc->add(dst_base);
        } else {
            res.new_node = dst_base;
            res.old_node = src_base;
        }
        res.success = true;
        return res;
    }
    
    template <bool SPECULATIVE, bool IS_LEAF, typename Alloc = void>
    static result remove_entry(
        ptr_t node, unsigned char c,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        uint64_t h = node->header();
        
        if ((h & (FLAG_BINARY | FLAG_LIST)) != 0) [[likely]] {
            if ((h & FLAG_BINARY) != 0) [[likely]] {
                return remove_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_binary<IS_LEAF>(), c, builder, alloc);
            } else {
                return remove_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_list<IS_LEAF>(), c, builder, alloc);
            }
        } else {
            if ((h & FLAG_POP) != 0) [[likely]] {
                return remove_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_pop<IS_LEAF>(), c, builder, alloc);
            } else {
                return remove_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_full<IS_LEAF>(), c, builder, alloc);
            }
        }
    }
    
    template <bool SPECULATIVE, bool IS_LEAF, typename Node, typename Alloc>
    static result remove_entry_typed(
        ptr_t node_base, Node* node, unsigned char c,
        builder_t& builder, [[maybe_unused]] Alloc* alloc)
    {
        result res;
        
        if (!node->has(c)) return res;
        
        constexpr int MAX = node_max_count<Node>();
        int cnt = node->count();
        
        constexpr int FLOOR = (MAX == BINARY_MAX) ? 1 : 
                              (MAX == LIST_MAX) ? LIST_MIN :
                              (MAX == POP_MAX) ? POP_MIN : FULL_MIN;
        
        if (cnt <= FLOOR) {
            if constexpr (MAX == BINARY_MAX) {
                return res;
            }
            return downgrade<SPECULATIVE, IS_LEAF>(node_base, node, c, builder, alloc);
        }
        
        node_base->bump_version();
        node->remove_entry(c);
        node->update_capacity_flags();
        res.in_place = true;
        res.success = true;
        return res;
    }
    
    static bool remove_leaf_inplace(ptr_t node, unsigned char c) noexcept {
        uint64_t h = node->header();
        
        if (h & FLAG_BINARY) {
            auto* bn = node->template as_binary<true>();
            if (!bn->has(c)) return false;
            node->bump_version();
            bn->remove_entry(c);
            bn->update_capacity_flags();
            return true;
        }
        if (h & FLAG_LIST) [[likely]] {
            auto* ln = node->template as_list<true>();
            if (!ln->has(c)) return false;
            node->bump_version();
            ln->remove_entry(c);
            ln->update_capacity_flags();
            return true;
        }
        if (h & FLAG_POP) {
            auto* pn = node->template as_pop<true>();
            if (!pn->has(c)) return false;
            node->bump_version();
            pn->remove_entry(c);
            pn->update_capacity_flags();
            return true;
        }
        auto* fn = node->template as_full<true>();
        if (!fn->has(c)) return false;
        node->bump_version();
        fn->remove_entry(c);
        fn->update_capacity_flags();
        return true;
    }
    
    static bool remove_child_inplace(ptr_t node, unsigned char c) noexcept {
        uint64_t h = node->header();
        
        if (h & FLAG_BINARY) {
            auto* bn = node->template as_binary<false>();
            if (!bn->has(c)) return false;
            node->bump_version();
            bn->remove_entry(c);
            bn->update_capacity_flags();
            return true;
        }
        if (h & FLAG_LIST) [[likely]] {
            auto* ln = node->template as_list<false>();
            if (!ln->has(c)) return false;
            node->bump_version();
            ln->remove_entry(c);
            ln->update_capacity_flags();
            return true;
        }
        if (h & FLAG_POP) {
            auto* pn = node->template as_pop<false>();
            if (!pn->has(c)) return false;
            node->bump_version();
            pn->remove_entry(c);
            pn->update_capacity_flags();
            return true;
        }
        auto* fn = node->template as_full<false>();
        if (!fn->has(c)) return false;
        node->bump_version();
        fn->remove_entry(c);
        fn->update_capacity_flags();
        return true;
    }
    
    template <bool SPECULATIVE, typename Alloc = void>
    static result binary_to_skip(
        ptr_t leaf, unsigned char removed_c,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        result res;
        auto* bn = leaf->template as_binary<true>();
        
        int idx = bn->find(removed_c);
        if (idx < 0) return res;
        
        int other_idx = 1 - idx;
        unsigned char other_c = bn->char_at(other_idx);
        T other_val{};
        bn->value_at(other_idx).try_read(other_val);
        
        std::string new_skip(leaf->skip_str());
        new_skip.push_back(static_cast<char>(other_c));
        
        ptr_t new_node = builder.make_leaf_skip(new_skip, other_val);
        
        if constexpr (SPECULATIVE) {
            if constexpr (THREADED) new_node->poison();
            alloc->replacement = new_node;
            alloc->add(new_node);
        } else {
            res.new_node = new_node;
            res.old_node = leaf;
        }
        res.success = true;
        return res;
    }
    
    static ptr_t clone_leaf_with_skip(ptr_t leaf, std::string_view new_skip, builder_t& builder) {
        if (leaf->is_binary()) [[likely]] {
            ptr_t n = builder.make_leaf_binary(new_skip);
            leaf->template as_binary<true>()->copy_values_to(n->template as_binary<true>());
            n->template as_binary<true>()->update_capacity_flags();
            return n;
        }
        if (leaf->is_list()) {
            ptr_t n = builder.make_leaf_list(new_skip);
            leaf->template as_list<true>()->copy_values_to(n->template as_list<true>());
            n->template as_list<true>()->update_capacity_flags();
            return n;
        }
        if (leaf->is_pop()) {
            ptr_t n = builder.make_leaf_pop(new_skip);
            leaf->template as_pop<true>()->copy_values_to(n->template as_pop<true>());
            n->template as_pop<true>()->update_capacity_flags();
            return n;
        }
        ptr_t n = builder.make_leaf_full(new_skip);
        leaf->template as_full<true>()->copy_values_to(n->template as_full<true>());
        n->template as_full<true>()->update_capacity_flags();
        return n;
    }
    
    static ptr_t clone_interior_with_skip(ptr_t node, std::string_view new_skip, builder_t& builder) {
        bool had_eos = node->has_eos();
        
        if (node->is_binary()) [[likely]] {
            ptr_t clone = builder.make_interior_binary(new_skip);
            if constexpr (FIXED_LEN == 0) {
                node->template as_binary<false>()->move_interior_to(clone->template as_binary<false>());
                if (had_eos) clone->set_eos_flag();
            } else {
                node->template as_binary<false>()->move_children_to(clone->template as_binary<false>());
            }
            clone->template as_binary<false>()->update_capacity_flags();
            return clone;
        }
        if (node->is_list()) {
            ptr_t clone = builder.make_interior_list(new_skip);
            node->template as_list<false>()->move_interior_to(clone->template as_list<false>());
            if constexpr (FIXED_LEN == 0) {
                if (had_eos) clone->set_eos_flag();
            }
            clone->template as_list<false>()->update_capacity_flags();
            return clone;
        }
        if (node->is_pop()) {
            ptr_t clone = builder.make_interior_pop(new_skip);
            if constexpr (FIXED_LEN == 0) {
                node->template as_pop<false>()->move_interior_to(clone->template as_pop<false>());
                if (had_eos) clone->set_eos_flag();
            } else {
                node->template as_pop<false>()->move_children_to(clone->template as_pop<false>());
            }
            clone->template as_pop<false>()->update_capacity_flags();
            return clone;
        }
        ptr_t clone = builder.make_interior_full(new_skip);
        node->template as_full<false>()->move_interior_to(clone->template as_full<false>());
        if constexpr (FIXED_LEN == 0) {
            if (had_eos) clone->set_eos_flag();
        }
        clone->template as_full<false>()->update_capacity_flags();
        return clone;
    }
    
    static ptr_t leaf_to_interior(ptr_t leaf, builder_t& builder, 
                                   unsigned char extra_c = 0, ptr_t extra_child = nullptr) {
        std::string_view leaf_skip = leaf->skip_str();
        int leaf_count = leaf->leaf_entry_count();
        bool need_extra = (extra_child != nullptr);
        int total = leaf_count + (need_extra ? 1 : 0);
        
        ptr_t interior;
        if (total <= BINARY_MAX) {
            interior = builder.make_interior_binary(leaf_skip);
        } else if (total <= LIST_MAX) {
            interior = builder.make_interior_list(leaf_skip);
        } else if (total <= POP_MAX) {
            interior = builder.make_interior_pop(leaf_skip);
        } else {
            interior = builder.make_interior_full(leaf_skip);
        }
        
        leaf->for_each_leaf_entry([&builder, &interior](unsigned char c, const T& val) {
            ptr_t child = builder.make_leaf_skip("", val);
            add_entry_to_interior(interior, c, child);
        });
        
        if (need_extra) {
            add_entry_to_interior(interior, extra_c, extra_child);
        }
        
        update_interior_capacity_flags(interior);
        return interior;
    }
    
private:
    static void add_entry_to_interior(ptr_t interior, unsigned char c, ptr_t child) {
        if (interior->is_binary()) {
            interior->template as_binary<false>()->add_entry(c, child);
        } else if (interior->is_list()) {
            interior->template as_list<false>()->add_entry(c, child);
        } else if (interior->is_pop()) {
            interior->template as_pop<false>()->add_entry(c, child);
        } else {
            interior->template as_full<false>()->add_entry(c, child);
        }
    }
    
    static void update_interior_capacity_flags(ptr_t interior) {
        if (interior->is_binary()) {
            interior->template as_binary<false>()->update_capacity_flags();
        } else if (interior->is_list()) {
            interior->template as_list<false>()->update_capacity_flags();
        } else if (interior->is_pop()) {
            interior->template as_pop<false>()->update_capacity_flags();
        } else {
            interior->template as_full<false>()->update_capacity_flags();
        }
    }
};

}  // namespace gteitelbaum
