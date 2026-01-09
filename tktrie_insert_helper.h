#pragma once

// =============================================================================
// TKTRIE UNIFIED HELPERS
// =============================================================================
//
// Consolidates duplication across multiple dimensions:
//   - SPECULATIVE vs NON-SPECULATIVE: Template<bool SPECULATIVE>
//   - IS_LEAF vs INTERIOR: Template<bool IS_LEAF> with if constexpr
//   - Node types: Hierarchical 2-level dispatch (better branch prediction)
//
// Hierarchical dispatch (2 branches max instead of 4):
//   if (BINARY|LIST) [[likely]]     <- most nodes are small fanout
//     if (BINARY) [[likely]]
//     else LIST
//   else (POP|FULL)
//     if (POP) [[likely]]
//     else FULL                     <- rare
//
// =============================================================================

#include <cstddef>
#include <string_view>
#include <type_traits>

#include "tktrie_node.h"

namespace gteitelbaum {

// =============================================================================
// NODE CAPACITY TRAITS - Compile-time capacity detection
// =============================================================================

template <typename Node>
constexpr int node_max_count() {
    if constexpr (requires { Node::MAX_CHILDREN; }) 
        return Node::MAX_CHILDREN;
    else if constexpr (requires { Node::MAX_ENTRIES; })
        return Node::MAX_ENTRIES;
    else
        return 256;  // FULL node
}

// =============================================================================
// ENTRY TYPE - Value for leaf, pointer for interior
// =============================================================================

template <typename T, typename PtrT, bool IS_LEAF>
using entry_t = std::conditional_t<IS_LEAF, const T&, PtrT>;

// =============================================================================
// UNIFIED INSERT/ERASE OPERATIONS
// =============================================================================

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
    
    // =========================================================================
    // COPY EOS - Only for interior nodes with FIXED_LEN == 0
    // =========================================================================
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
    
    // =========================================================================
    // MAKE UPGRADED NODE - Returns appropriate upgrade target
    // =========================================================================
    template <bool IS_LEAF, int SRC_MAX>
    static ptr_t make_upgraded(std::string_view skip, builder_t& builder) {
        if constexpr (SRC_MAX == BINARY_MAX) {
            if constexpr (IS_LEAF) return builder.make_leaf_list(skip);
            else return builder.make_interior_list(skip);
        } else if constexpr (SRC_MAX == LIST_MAX) {
            if constexpr (IS_LEAF) return builder.make_leaf_pop(skip);
            else return builder.make_interior_pop(skip);
        } else if constexpr (SRC_MAX == POP_MAX) {
            if constexpr (IS_LEAF) return builder.make_leaf_full(skip);
            else return builder.make_interior_full(skip);
        } else {
            return nullptr;  // FULL can't upgrade
        }
    }
    
    // =========================================================================
    // MAKE DOWNGRADED NODE - Returns appropriate downgrade target
    // =========================================================================
    template <bool IS_LEAF, int SRC_MAX>
    static ptr_t make_downgraded(std::string_view skip, builder_t& builder) {
        if constexpr (SRC_MAX == LIST_MAX) {
            if constexpr (IS_LEAF) return builder.make_leaf_binary(skip);
            else return builder.make_interior_binary(skip);
        } else if constexpr (SRC_MAX == POP_MAX) {
            if constexpr (IS_LEAF) return builder.make_leaf_list(skip);
            else return builder.make_interior_list(skip);
        } else if constexpr (SRC_MAX == 256) {
            if constexpr (IS_LEAF) return builder.make_leaf_pop(skip);
            else return builder.make_interior_pop(skip);
        } else {
            return nullptr;  // BINARY can't downgrade (becomes SKIP)
        }
    }
    
    // =========================================================================
    // ADD ENTRY TO NODE - Handles iteration differences
    // =========================================================================
    template <bool IS_LEAF, typename DstNode>
    static void add_entry_to(DstNode* dst, unsigned char c, entry_t<T, ptr_t, IS_LEAF> entry) {
        if constexpr (IS_LEAF) {
            dst->add_value(c, entry);
        } else {
            dst->add_child(c, entry);
        }
    }
    
    // =========================================================================
    // COPY ENTRIES - Unified for leaf values and interior children
    // Uses accessor methods: char_at(), value_at(), valid(), child_at_slot()
    // =========================================================================
    template <bool IS_LEAF, typename SrcNode, typename DstNode>
    static void copy_entries(SrcNode* src, DstNode* dst) {
        int cnt = src->count();
        constexpr int MAX = node_max_count<SrcNode>();
        
        if constexpr (MAX == BINARY_MAX || MAX == LIST_MAX) {
            // BINARY or LIST: indexed access via char_at()
            for (int i = 0; i < cnt; ++i) {
                unsigned char c = src->char_at(i);
                if constexpr (IS_LEAF) {
                    T val{};
                    src->value_at(i).try_read(val);
                    dst->add_value(c, val);
                } else {
                    dst->add_child(c, src->child_at_slot(i));
                }
            }
        } else {
            // POP or FULL: bitmap iteration via valid()
            int slot = 0;
            src->valid().for_each_set([&](unsigned char c) {
                if constexpr (IS_LEAF) {
                    T val{};
                    if constexpr (MAX == POP_MAX) {
                        // POP: indexed via slot
                        src->element_at_slot(slot).try_read(val);
                    } else {
                        // FULL: read by char
                        src->read_value(c, val);
                    }
                    dst->add_value(c, val);
                } else {
                    if constexpr (MAX == POP_MAX) {
                        // POP: indexed
                        dst->add_child(c, src->child_at_slot(slot));
                    } else {
                        // FULL: direct
                        dst->add_child(c, src->get_child(c));
                    }
                }
                ++slot;
            });
        }
    }
    
    // =========================================================================
    // COPY ENTRIES EXCEPT - For downgrade (skip one entry)
    // Uses accessor methods: char_at(), value_at(), valid(), child_at_slot()
    // =========================================================================
    template <bool IS_LEAF, typename SrcNode, typename DstNode>
    static void copy_entries_except(SrcNode* src, DstNode* dst, unsigned char skip_c) {
        int cnt = src->count();
        constexpr int MAX = node_max_count<SrcNode>();
        
        if constexpr (MAX == BINARY_MAX || MAX == LIST_MAX) {
            // BINARY or LIST
            for (int i = 0; i < cnt; ++i) {
                unsigned char c = src->char_at(i);
                if (c == skip_c) continue;
                if constexpr (IS_LEAF) {
                    T val{};
                    src->value_at(i).try_read(val);
                    dst->add_value(c, val);
                } else {
                    dst->add_child(c, src->child_at_slot(i));
                }
            }
        } else {
            // POP or FULL
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
                        dst->add_value(c, val);
                    } else {
                        if constexpr (MAX == POP_MAX) {
                            dst->add_child(c, src->child_at_slot(slot));
                        } else {
                            dst->add_child(c, src->get_child(c));
                        }
                    }
                }
                ++slot;
            });
        }
    }
    
    // =========================================================================
    // UPGRADE - Unified for leaf and interior, SPEC and NON-SPEC
    // BINARY→LIST, LIST→POP, POP→FULL
    // =========================================================================
    template <bool SPECULATIVE, bool IS_LEAF, typename SrcNode, typename Alloc = void>
    static result upgrade(
        ptr_t src_base, SrcNode* src, unsigned char c,
        entry_t<T, ptr_t, IS_LEAF> entry,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        result res;
        constexpr int MAX = node_max_count<SrcNode>();
        
        ptr_t dst_base = make_upgraded<IS_LEAF, MAX>(src_base->skip_str(), builder);
        if (!dst_base) return res;  // FULL can't upgrade
        
        // Copy existing entries and add new one - dispatch to correct dst type
        if constexpr (MAX == BINARY_MAX) {
            if constexpr (IS_LEAF) {
                auto* dst = dst_base->template as_list<true>();
                copy_entries<IS_LEAF>(src, dst);
                dst->add_value(c, entry);
                dst->update_capacity_flags();
            } else {
                auto* dst = dst_base->template as_list<false>();
                copy_entries<IS_LEAF>(src, dst);
                copy_eos_to(src, dst_base);
                dst->add_child(c, entry);
                dst->update_capacity_flags();
            }
        } else if constexpr (MAX == LIST_MAX) {
            if constexpr (IS_LEAF) {
                auto* dst = dst_base->template as_pop<true>();
                copy_entries<IS_LEAF>(src, dst);
                dst->add_value(c, entry);
                dst->update_capacity_flags();
            } else {
                auto* dst = dst_base->template as_pop<false>();
                copy_entries<IS_LEAF>(src, dst);
                copy_eos_to(src, dst_base);
                dst->add_child(c, entry);
                dst->update_capacity_flags();
            }
        } else if constexpr (MAX == POP_MAX) {
            if constexpr (IS_LEAF) {
                auto* dst = dst_base->template as_full<true>();
                copy_entries<IS_LEAF>(src, dst);
                dst->add_value(c, entry);
                dst->update_capacity_flags();
            } else {
                auto* dst = dst_base->template as_full<false>();
                copy_entries<IS_LEAF>(src, dst);
                copy_eos_to(src, dst_base);
                dst->add_child(c, entry);
                dst->update_capacity_flags();
            }
        }
        
        // Handle SPEC vs NON-SPEC
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
    
    // =========================================================================
    // UPGRADE WRAPPER - Dispatches to typed upgrade based on node type
    // =========================================================================
    template <bool SPECULATIVE, bool IS_LEAF, typename Alloc = void>
    static result upgrade(
        ptr_t node, unsigned char c,
        entry_t<T, ptr_t, IS_LEAF> entry,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        uint64_t h = node->header();
        
        // Hierarchical dispatch: 2 levels for better branch prediction
        if ((h & (FLAG_BINARY | FLAG_LIST)) != 0) [[likely]] {
            if ((h & FLAG_BINARY) != 0) [[likely]] {
                if constexpr (IS_LEAF)
                    return upgrade<SPECULATIVE, IS_LEAF>(node, node->template as_binary<true>(), c, entry, builder, alloc);
                else
                    return upgrade<SPECULATIVE, IS_LEAF>(node, node->template as_binary<false>(), c, entry, builder, alloc);
            } else {
                if constexpr (IS_LEAF)
                    return upgrade<SPECULATIVE, IS_LEAF>(node, node->template as_list<true>(), c, entry, builder, alloc);
                else
                    return upgrade<SPECULATIVE, IS_LEAF>(node, node->template as_list<false>(), c, entry, builder, alloc);
            }
        } else {
            if ((h & FLAG_POP) != 0) [[likely]] {
                if constexpr (IS_LEAF)
                    return upgrade<SPECULATIVE, IS_LEAF>(node, node->template as_pop<true>(), c, entry, builder, alloc);
                else
                    return upgrade<SPECULATIVE, IS_LEAF>(node, node->template as_pop<false>(), c, entry, builder, alloc);
            } else {
                // FULL can't upgrade - return failure
                result res;
                return res;
            }
        }
    }
    
    // =========================================================================
    // ADD ENTRY - Unified in-place or upgrade for leaf and interior
    // Uses hierarchical 2-level dispatch
    // =========================================================================
    template <bool SPECULATIVE, bool IS_LEAF, typename Alloc = void>
    static result add_entry(
        ptr_t node, unsigned char c,
        entry_t<T, ptr_t, IS_LEAF> entry,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        uint64_t h = node->header();
        
        // Hierarchical dispatch: 2 levels for better branch prediction
        if ((h & (FLAG_BINARY | FLAG_LIST)) != 0) [[likely]] {
            if ((h & FLAG_BINARY) != 0) [[likely]] {
                if constexpr (IS_LEAF)
                    return add_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_binary<true>(), c, entry, builder, alloc);
                else
                    return add_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_binary<false>(), c, entry, builder, alloc);
            } else {
                if constexpr (IS_LEAF)
                    return add_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_list<true>(), c, entry, builder, alloc);
                else
                    return add_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_list<false>(), c, entry, builder, alloc);
            }
        } else {
            if ((h & FLAG_POP) != 0) [[likely]] {
                if constexpr (IS_LEAF)
                    return add_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_pop<true>(), c, entry, builder, alloc);
                else
                    return add_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_pop<false>(), c, entry, builder, alloc);
            } else {
                if constexpr (IS_LEAF)
                    return add_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_full<true>(), c, entry, builder, alloc);
                else
                    return add_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_full<false>(), c, entry, builder, alloc);
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
        
        if (node->has(c)) return res;  // Already exists
        
        constexpr int MAX = node_max_count<Node>();
        
        // In-place if room
        if (node->count() < MAX) {
            node_base->bump_version();
            add_entry_to<IS_LEAF>(node, c, entry);
            node->update_capacity_flags();
            res.in_place = true;
            res.success = true;
            return res;
        }
        
        // Need upgrade
        return upgrade<SPECULATIVE, IS_LEAF>(node_base, node, c, entry, builder, alloc);
    }
    
    // =========================================================================
    // DOWNGRADE - LIST→BINARY, POP→LIST, FULL→POP (for erase)
    // =========================================================================
    template <bool SPECULATIVE, bool IS_LEAF, typename SrcNode, typename Alloc = void>
    static result downgrade(
        ptr_t src_base, SrcNode* src, unsigned char removed_c,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        result res;
        constexpr int MAX = node_max_count<SrcNode>();
        
        ptr_t dst_base = make_downgraded<IS_LEAF, MAX>(src_base->skip_str(), builder);
        if (!dst_base) return res;  // BINARY can't downgrade this way
        
        // Copy entries except removed one - dispatch to correct dst type
        if constexpr (MAX == LIST_MAX) {
            if constexpr (IS_LEAF) {
                auto* dst = dst_base->template as_binary<true>();
                copy_entries_except<IS_LEAF>(src, dst, removed_c);
                dst->update_capacity_flags();
            } else {
                auto* dst = dst_base->template as_binary<false>();
                copy_entries_except<IS_LEAF>(src, dst, removed_c);
                copy_eos_to(src, dst_base);
                dst->update_capacity_flags();
            }
        } else if constexpr (MAX == POP_MAX) {
            if constexpr (IS_LEAF) {
                auto* dst = dst_base->template as_list<true>();
                copy_entries_except<IS_LEAF>(src, dst, removed_c);
                dst->update_capacity_flags();
            } else {
                auto* dst = dst_base->template as_list<false>();
                copy_entries_except<IS_LEAF>(src, dst, removed_c);
                copy_eos_to(src, dst_base);
                dst->update_capacity_flags();
            }
        } else if constexpr (MAX == 256) {
            if constexpr (IS_LEAF) {
                auto* dst = dst_base->template as_pop<true>();
                copy_entries_except<IS_LEAF>(src, dst, removed_c);
                dst->update_capacity_flags();
            } else {
                auto* dst = dst_base->template as_pop<false>();
                copy_entries_except<IS_LEAF>(src, dst, removed_c);
                copy_eos_to(src, dst_base);
                dst->update_capacity_flags();
            }
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
    
    // =========================================================================
    // REMOVE ENTRY - Unified in-place or downgrade for leaf and interior
    // =========================================================================
    template <bool SPECULATIVE, bool IS_LEAF, typename Alloc = void>
    static result remove_entry(
        ptr_t node, unsigned char c,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        uint64_t h = node->header();
        
        if ((h & (FLAG_BINARY | FLAG_LIST)) != 0) [[likely]] {
            if ((h & FLAG_BINARY) != 0) [[likely]] {
                if constexpr (IS_LEAF)
                    return remove_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_binary<true>(), c, builder, alloc);
                else
                    return remove_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_binary<false>(), c, builder, alloc);
            } else {
                if constexpr (IS_LEAF)
                    return remove_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_list<true>(), c, builder, alloc);
                else
                    return remove_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_list<false>(), c, builder, alloc);
            }
        } else {
            if ((h & FLAG_POP) != 0) [[likely]] {
                if constexpr (IS_LEAF)
                    return remove_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_pop<true>(), c, builder, alloc);
                else
                    return remove_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_pop<false>(), c, builder, alloc);
            } else {
                if constexpr (IS_LEAF)
                    return remove_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_full<true>(), c, builder, alloc);
                else
                    return remove_entry_typed<SPECULATIVE, IS_LEAF>(node, node->template as_full<false>(), c, builder, alloc);
            }
        }
    }
    
    template <bool SPECULATIVE, bool IS_LEAF, typename Node, typename Alloc>
    static result remove_entry_typed(
        ptr_t node_base, Node* node, unsigned char c,
        builder_t& builder, [[maybe_unused]] Alloc* alloc)
    {
        result res;
        
        if (!node->has(c)) return res;  // Doesn't exist
        
        constexpr int MAX = node_max_count<Node>();
        int cnt = node->count();
        
        // Check if downgrade needed (at floor)
        constexpr int FLOOR = (MAX == BINARY_MAX) ? 1 : 
                              (MAX == LIST_MAX) ? LIST_MIN :
                              (MAX == POP_MAX) ? POP_MIN : FULL_MIN;
        
        if (cnt <= FLOOR) {
            // Need downgrade or special handling
            if constexpr (MAX == BINARY_MAX) {
                // BINARY with 1 entry after removal -> handled separately (to SKIP)
                return res;
            }
            return downgrade<SPECULATIVE, IS_LEAF>(node_base, node, c, builder, alloc);
        }
        
        // In-place removal
        node_base->bump_version();
        if constexpr (IS_LEAF) {
            if constexpr (MAX == BINARY_MAX) {
                // BINARY uses remove_entry(idx)
                int idx = node->find(c);
                node->remove_entry(idx);
            } else {
                // LIST, POP, FULL use remove_value(c)
                node->remove_value(c);
            }
        } else {
            if constexpr (MAX == BINARY_MAX) {
                // BINARY interior uses remove_child(idx)
                int idx = node->find(c);
                node->remove_child(idx);
            } else {
                // LIST, POP, FULL use remove_child(c)
                node->remove_child(c);
            }
        }
        node->update_capacity_flags();
        res.in_place = true;
        res.success = true;
        return res;
    }
    
    // =========================================================================
    // SPLIT SKIP LEAF - Key and skip diverge
    // =========================================================================
    template <bool SPECULATIVE, typename Alloc = void>
    static result split_skip_leaf(
        ptr_t leaf, std::string_view key, const T& value, size_t m,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        result res;
        std::string_view old_skip = leaf->skip_str();
        std::string common(old_skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);
        
        T old_value{};
        leaf->as_skip()->value.try_read(old_value);
        
        ptr_t interior = builder.make_interior_list(common);
        ptr_t old_child = builder.make_leaf_skip(old_skip.substr(m + 1), old_value);
        ptr_t new_child = builder.make_leaf_skip(key.substr(m + 1), value);
        
        interior->template as_list<false>()->add_two_children(old_c, old_child, new_c, new_child);
        interior->template as_list<false>()->update_capacity_flags();
        
        if constexpr (SPECULATIVE) {
            if constexpr (THREADED) {
                interior->poison();
                old_child->poison();
                new_child->poison();
            }
            alloc->root_replacement = interior;
            alloc->add(interior);
            alloc->add(old_child);
            alloc->add(new_child);
        } else {
            res.new_node = interior;
            res.old_node = leaf;
        }
        res.success = true;
        return res;
    }
    
    // =========================================================================
    // PREFIX SKIP LEAF - Key is prefix of skip
    // =========================================================================
    template <bool SPECULATIVE, typename Alloc = void>
    static result prefix_skip_leaf(
        ptr_t leaf, std::string_view key, const T& value, size_t m,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        result res;
        std::string_view old_skip = leaf->skip_str();
        unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
        
        T old_value{};
        leaf->as_skip()->value.try_read(old_value);
        
        ptr_t interior = builder.make_interior_list(std::string(key));
        if constexpr (FIXED_LEN == 0) {
            interior->set_eos(value);
        }
        ptr_t child = builder.make_leaf_skip(old_skip.substr(m + 1), old_value);
        
        interior->template as_list<false>()->add_child(old_c, child);
        interior->template as_list<false>()->update_capacity_flags();
        
        if constexpr (SPECULATIVE) {
            if constexpr (THREADED) {
                interior->poison();
                child->poison();
            }
            alloc->root_replacement = interior;
            alloc->add(interior);
            alloc->add(child);
        } else {
            res.new_node = interior;
            res.old_node = leaf;
        }
        res.success = true;
        return res;
    }
    
    // =========================================================================
    // EXTEND SKIP LEAF - Skip is prefix of key
    // =========================================================================
    template <bool SPECULATIVE, typename Alloc = void>
    static result extend_skip_leaf(
        ptr_t leaf, std::string_view key, const T& value, size_t m,
        builder_t& builder, [[maybe_unused]] Alloc* alloc = nullptr)
    {
        result res;
        std::string_view old_skip = leaf->skip_str();
        unsigned char new_c = static_cast<unsigned char>(key[m]);
        
        ptr_t interior = builder.make_interior_list(std::string(old_skip));
        if constexpr (FIXED_LEN == 0) {
            T old_value{};
            leaf->as_skip()->value.try_read(old_value);
            interior->set_eos(old_value);
        }
        ptr_t child = builder.make_leaf_skip(key.substr(m + 1), value);
        
        interior->template as_list<false>()->add_child(new_c, child);
        interior->template as_list<false>()->update_capacity_flags();
        
        if constexpr (SPECULATIVE) {
            if constexpr (THREADED) {
                interior->poison();
                child->poison();
            }
            alloc->root_replacement = interior;
            alloc->add(interior);
            alloc->add(child);
        } else {
            res.new_node = interior;
            res.old_node = leaf;
        }
        res.success = true;
        return res;
    }
    
    // =========================================================================
    // BINARY TO SKIP - Erase from BINARY(2) leaves SKIP(1)
    // =========================================================================
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
    
    // =========================================================================
    // Clone helpers - create node of same type with different skip
    // =========================================================================
    
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
    
    // Convert a leaf node to an interior, with each entry becoming a SKIP child
    // Optionally adds an extra child at extra_c with extra_child (if extra_child != nullptr)
    // Returns interior of same type as leaf, unless upgrade_type is set
    static ptr_t leaf_to_interior(ptr_t leaf, builder_t& builder, 
                                   unsigned char extra_c = 0, ptr_t extra_child = nullptr) {
        std::string_view leaf_skip = leaf->skip_str();
        int leaf_count = leaf->leaf_entry_count();
        bool need_extra = (extra_child != nullptr);
        int total = leaf_count + (need_extra ? 1 : 0);
        
        // Determine target type based on total count
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
        
        // Add all entries from leaf as SKIP children
        leaf->for_each_leaf_entry([&builder, &interior](unsigned char c, const T& val) {
            ptr_t child = builder.make_leaf_skip("", val);
            add_child_to_interior_impl(interior, c, child);
        });
        
        // Add extra child if provided
        if (need_extra) {
            add_child_to_interior_impl(interior, extra_c, extra_child);
        }
        
        update_interior_capacity_flags(interior);
        return interior;
    }
    
private:
    // Helper to add child to any interior type
    static void add_child_to_interior_impl(ptr_t interior, unsigned char c, ptr_t child) {
        if (interior->is_binary()) {
            interior->template as_binary<false>()->add_child(c, child);
        } else if (interior->is_list()) {
            interior->template as_list<false>()->add_child(c, child);
        } else if (interior->is_pop()) {
            interior->template as_pop<false>()->add_child(c, child);
        } else {
            interior->template as_full<false>()->add_child(c, child);
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
    
public:
};

}  // namespace gteitelbaum
