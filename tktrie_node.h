#pragma once

#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_dataptr.h"

namespace gteitelbaum {

/**
 * Node layouts (EOS slot always exists, SKIP_EOS slot exists if SKIP):
 * 
 * Base:           [header][eos]
 * +SKIP:          [header][eos][skip_len][skip_chars...][skip_eos]
 * +LIST:          [header][eos][lst][child_ptr x N]
 * +LEAF|LIST:     [header][eos][lst][T x N]
 * +POP:           [header][eos][bmp x4][child_ptr x N]
 * +LEAF|POP:      [header][eos][bmp x4][T x N]
 * +FULL:          [header][eos][child_ptr x256]
 * +LEAF|FULL:     [header][eos][valid_bmp x4][T x256]
 * +SKIP+LIST:     [header][eos][skip_len][skip_chars...][skip_eos][lst][child_ptr x N]
 * etc.
 */

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
class node_view {
public:
    using slot_type = slot_type_t<THREADED>;
    using dataptr_t = dataptr<T, THREADED, Allocator>;
    
private:
    slot_type* arr_;

    uint64_t header() const noexcept { return load_slot<THREADED>(&arr_[0]); }

public:
    explicit node_view(slot_type* arr) noexcept : arr_(arr) {}
    
    slot_type* raw() noexcept { return arr_; }
    const slot_type* raw() const noexcept { return arr_; }

    uint64_t flags() const noexcept { return get_flags(header()); }
    uint32_t size() const noexcept { return get_size(header()); }

    bool has_skip() const noexcept { return (flags() & FLAG_SKIP) != 0; }
    bool has_list() const noexcept { return (flags() & FLAG_LIST) != 0; }
    bool has_pop() const noexcept { return (flags() & FLAG_POP) != 0; }
    bool has_full() const noexcept { return (flags() & FLAG_FULL) != 0; }
    bool has_leaf() const noexcept { return (flags() & FLAG_LEAF) != 0; }

    void set_header(uint64_t h) noexcept { store_slot<THREADED>(&arr_[0], h); }

    // EOS is always at offset 1
    static constexpr size_t eos_offset() noexcept { return 1; }

    dataptr_t* eos_data() noexcept { return reinterpret_cast<dataptr_t*>(&arr_[eos_offset()]); }
    const dataptr_t* eos_data() const noexcept { return reinterpret_cast<const dataptr_t*>(&arr_[eos_offset()]); }

    // Skip section starts after EOS
    static constexpr size_t skip_len_offset() noexcept { return 2; }
    
    size_t skip_chars_offset() const noexcept { return skip_len_offset() + 1; }

    size_t skip_eos_offset() const noexcept {
        return skip_chars_offset() + bytes_to_words(skip_length());
    }

    size_t skip_length() const noexcept {
        if (!has_skip()) return 0;
        return static_cast<size_t>(load_slot<THREADED>(&arr_[skip_len_offset()]));
    }

    std::string_view skip_chars() const noexcept {
        if (!has_skip()) return {};
        size_t len = skip_length();
        const char* data = reinterpret_cast<const char*>(&arr_[skip_chars_offset()]);
        return std::string_view(data, len);
    }

    void set_skip_length(size_t len) noexcept {
        store_slot<THREADED>(&arr_[skip_len_offset()], static_cast<uint64_t>(len));
    }

    void set_skip_chars(std::string_view s) noexcept {
        char* data = reinterpret_cast<char*>(&arr_[skip_chars_offset()]);
        std::memcpy(data, s.data(), s.size());
        size_t total_bytes = bytes_to_words(s.size()) * 8;
        if (s.size() < total_bytes) std::memset(data + s.size(), 0, total_bytes - s.size());
    }

    dataptr_t* skip_eos_data() noexcept {
        KTRIE_DEBUG_ASSERT(has_skip());
        return reinterpret_cast<dataptr_t*>(&arr_[skip_eos_offset()]);
    }

    const dataptr_t* skip_eos_data() const noexcept {
        KTRIE_DEBUG_ASSERT(has_skip());
        return reinterpret_cast<const dataptr_t*>(&arr_[skip_eos_offset()]);
    }

    // Children header offset (after EOS, after skip section if present)
    size_t children_header_offset() const noexcept {
        size_t off = 2;  // header + eos
        if (has_skip()) {
            off += 1;  // skip_len
            off += bytes_to_words(skip_length());
            off += 1;  // skip_eos
        }
        return off;
    }

    // For LEAF|FULL: valid bitmap is at children_header_offset
    size_t leaf_full_bitmap_offset() const noexcept {
        KTRIE_DEBUG_ASSERT(has_leaf() && has_full());
        return children_header_offset();
    }

    popcount_bitmap get_leaf_full_bitmap() const noexcept {
        KTRIE_DEBUG_ASSERT(has_leaf() && has_full());
        size_t off = leaf_full_bitmap_offset();
        std::array<uint64_t, 4> arr;
        for (int i = 0; i < 4; ++i) arr[i] = load_slot<THREADED>(&arr_[off + i]);
        return popcount_bitmap::from_array(arr);
    }

    void set_leaf_full_bitmap(const popcount_bitmap& bmp) noexcept {
        KTRIE_DEBUG_ASSERT(has_leaf() && has_full());
        size_t off = leaf_full_bitmap_offset();
        auto arr = bmp.to_array();
        for (int i = 0; i < 4; ++i) store_slot<THREADED>(&arr_[off + i], arr[i]);
    }

    size_t child_ptrs_offset() const noexcept {
        size_t off = children_header_offset();
        if (has_full()) {
            if (has_leaf()) off += 4;  // valid bitmap for LEAF|FULL
            return off;
        }
        if (has_list()) off += 1;
        else if (has_pop()) off += 4;
        return off;
    }

    small_list get_list() const noexcept {
        KTRIE_DEBUG_ASSERT(has_list());
        return small_list::from_u64(load_slot<THREADED>(&arr_[children_header_offset()]));
    }

    void set_list(small_list lst) noexcept {
        KTRIE_DEBUG_ASSERT(has_list());
        store_slot<THREADED>(&arr_[children_header_offset()], lst.to_u64());
    }

    popcount_bitmap get_bitmap() const noexcept {
        KTRIE_DEBUG_ASSERT(has_pop());
        size_t off = children_header_offset();
        std::array<uint64_t, 4> arr;
        for (int i = 0; i < 4; ++i) arr[i] = load_slot<THREADED>(&arr_[off + i]);
        return popcount_bitmap::from_array(arr);
    }

    void set_bitmap(const popcount_bitmap& bmp) noexcept {
        KTRIE_DEBUG_ASSERT(has_pop());
        size_t off = children_header_offset();
        auto arr = bmp.to_array();
        for (int i = 0; i < 4; ++i) store_slot<THREADED>(&arr_[off + i], arr[i]);
    }

    slot_type* child_ptrs() noexcept { return &arr_[child_ptrs_offset()]; }
    const slot_type* child_ptrs() const noexcept { return &arr_[child_ptrs_offset()]; }

    // Child count based on structure (not validity for LEAF|FULL)
    int child_count() const noexcept {
        if (has_full()) return 256;
        if (has_list()) return get_list().count();
        if (has_pop()) return get_bitmap().count();
        return 0;
    }

    // Live child count (respects LEAF|FULL validity bitmap)
    int live_child_count() const noexcept {
        if (has_leaf() && has_full()) {
            return get_leaf_full_bitmap().count();
        }
        if (has_full()) {
            int count = 0;
            for (int i = 0; i < 256; ++i)
                if (load_slot<THREADED>(&child_ptrs()[i]) != 0) ++count;
            return count;
        }
        if (has_list()) {
            int count = 0;
            for (int i = 0; i < get_list().count(); ++i)
                if (load_slot<THREADED>(&child_ptrs()[i]) != 0) ++count;
            return count;
        }
        if (has_pop()) {
            int count = 0;
            for (int i = 0; i < get_bitmap().count(); ++i)
                if (load_slot<THREADED>(&child_ptrs()[i]) != 0) ++count;
            return count;
        }
        return 0;
    }

    // Find child slot - for LEAF|FULL checks validity bitmap
    slot_type* find_child(unsigned char c) noexcept {
        if (has_full()) {
            if (has_leaf()) {
                if (!get_leaf_full_bitmap().contains(c)) return nullptr;
            }
            return &child_ptrs()[c];
        }
        if (has_list()) {
            int off = get_list().offset(c);
            if (off == 0) return nullptr;
            return &child_ptrs()[off - 1];
        }
        if (has_pop()) {
            int idx;
            if (!get_bitmap().find(c, &idx)) return nullptr;
            return &child_ptrs()[idx];
        }
        return nullptr;
    }

    const slot_type* find_child(unsigned char c) const noexcept {
        return const_cast<node_view*>(this)->find_child(c);
    }

    uint64_t get_child_ptr(int idx) const noexcept {
        return load_slot<THREADED>(&child_ptrs()[idx]);
    }

    void set_child_ptr(int idx, uint64_t ptr) noexcept {
        store_slot<THREADED>(&child_ptrs()[idx], ptr);
    }

    // For LEAF nodes: get/set embedded value
    T get_leaf_value(int idx) const noexcept {
        KTRIE_DEBUG_ASSERT(has_leaf());
        static_assert(can_embed_leaf_v<T>, "T must be embeddable for LEAF");
        T val;
        uint64_t raw = load_slot<THREADED>(&child_ptrs()[idx]);
        std::memcpy(&val, &raw, sizeof(T));
        return val;
    }

    void set_leaf_value(int idx, const T& val) noexcept {
        KTRIE_DEBUG_ASSERT(has_leaf());
        static_assert(can_embed_leaf_v<T>, "T must be embeddable for LEAF");
        uint64_t raw = 0;
        std::memcpy(&raw, &val, sizeof(T));
        store_slot<THREADED>(&child_ptrs()[idx], raw);
    }
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
class node_builder {
public:
    using slot_type = slot_type_t<THREADED>;
    using dataptr_t = dataptr<T, THREADED, Allocator>;
    using node_view_t = node_view<T, THREADED, Allocator, FIXED_LEN>;
    
    using alloc_traits = std::allocator_traits<Allocator>;
    using slot_alloc_t = typename alloc_traits::template rebind_alloc<slot_type>;
    using slot_alloc_traits = std::allocator_traits<slot_alloc_t>;

private:
    slot_alloc_t alloc_;

    slot_type* allocate_node(size_t num_slots) {
        slot_type* node = slot_alloc_traits::allocate(alloc_, num_slots);
        for (size_t i = 0; i < num_slots; ++i) store_slot<THREADED>(&node[i], 0);
        return node;
    }

public:
    explicit node_builder(const Allocator& alloc = Allocator()) : alloc_(alloc) {}

    void deallocate_node(slot_type* node) noexcept {
        if (!node) return;
        node_view_t view(node);
        view.eos_data()->~dataptr_t();
        if (view.has_skip()) view.skip_eos_data()->~dataptr_t();
        slot_alloc_traits::deallocate(alloc_, node, view.size());
    }

    // Size calculation: header + eos + [skip section] + [children section]
    static size_t calc_size(bool has_skip, size_t skip_len, 
                            bool has_list, bool has_pop, bool has_full, bool has_leaf,
                            int child_count) noexcept {
        size_t sz = 2;  // header + eos
        if (has_skip) {
            sz += 1;  // skip_len
            sz += bytes_to_words(skip_len);
            sz += 1;  // skip_eos
        }
        if (has_full) {
            if (has_leaf) sz += 4;  // valid bitmap
            sz += 256;
        } else {
            if (has_list) sz += 1;
            else if (has_pop) sz += 4;
            sz += child_count;
        }
        return sz;
    }

    // Build empty node (just EOS slot, nullable)
    slot_type* build_empty() {
        size_t sz = calc_size(false, 0, false, false, false, false, 0);
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(0, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        return node;
    }

    // Build SKIP node (EOS + SKIP_EOS, both nullable)
    slot_type* build_skip(std::string_view skip) {
        size_t sz = calc_size(true, skip.size(), false, false, false, false, 0);
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_SKIP, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        new (view.skip_eos_data()) dataptr_t();
        return node;
    }

    // Build LIST node
    slot_type* build_list(small_list lst, const std::vector<uint64_t>& children) {
        size_t sz = calc_size(false, 0, true, false, false, false, lst.count());
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_LIST, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_list(lst);
        for (size_t i = 0; i < children.size(); ++i) view.set_child_ptr(static_cast<int>(i), children[i]);
        return node;
    }

    // Build LEAF|LIST node
    slot_type* build_leaf_list(small_list lst, const std::vector<T>& values) {
        static_assert(can_embed_leaf_v<T>, "T must be embeddable for LEAF");
        size_t sz = calc_size(false, 0, true, false, false, true, lst.count());
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_LEAF | FLAG_LIST, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_list(lst);
        for (size_t i = 0; i < values.size(); ++i) view.set_leaf_value(static_cast<int>(i), values[i]);
        return node;
    }

    // Build POP node
    slot_type* build_pop(popcount_bitmap bmp, const std::vector<uint64_t>& children) {
        size_t sz = calc_size(false, 0, false, true, false, false, bmp.count());
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_POP, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_bitmap(bmp);
        for (size_t i = 0; i < children.size(); ++i) view.set_child_ptr(static_cast<int>(i), children[i]);
        return node;
    }

    // Build LEAF|POP node
    slot_type* build_leaf_pop(popcount_bitmap bmp, const std::vector<T>& values) {
        static_assert(can_embed_leaf_v<T>, "T must be embeddable for LEAF");
        size_t sz = calc_size(false, 0, false, true, false, true, bmp.count());
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_LEAF | FLAG_POP, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_bitmap(bmp);
        for (size_t i = 0; i < values.size(); ++i) view.set_leaf_value(static_cast<int>(i), values[i]);
        return node;
    }

    // Build FULL node (256 children indexed by char, null = absent)
    slot_type* build_full(const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(children.size() == 256);
        size_t sz = calc_size(false, 0, false, false, true, false, 0);
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_FULL, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        for (int i = 0; i < 256; ++i) view.set_child_ptr(i, children[i]);
        return node;
    }

    // Build LEAF|FULL node (valid_bmp + 256 values)
    slot_type* build_leaf_full(popcount_bitmap valid_bmp, const std::vector<T>& values) {
        static_assert(can_embed_leaf_v<T>, "T must be embeddable for LEAF");
        KTRIE_DEBUG_ASSERT(values.size() == 256);
        size_t sz = calc_size(false, 0, false, false, true, true, 0);
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_LEAF | FLAG_FULL, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_leaf_full_bitmap(valid_bmp);
        for (int i = 0; i < 256; ++i) view.set_leaf_value(i, values[i]);
        return node;
    }

    // Build SKIP|LIST node
    slot_type* build_skip_list(std::string_view skip, small_list lst, const std::vector<uint64_t>& children) {
        size_t sz = calc_size(true, skip.size(), true, false, false, false, lst.count());
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_SKIP | FLAG_LIST, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        new (view.skip_eos_data()) dataptr_t();
        view.set_list(lst);
        for (size_t i = 0; i < children.size(); ++i) view.set_child_ptr(static_cast<int>(i), children[i]);
        return node;
    }

    // Build SKIP|LEAF|LIST node
    slot_type* build_skip_leaf_list(std::string_view skip, small_list lst, const std::vector<T>& values) {
        static_assert(can_embed_leaf_v<T>, "T must be embeddable for LEAF");
        size_t sz = calc_size(true, skip.size(), true, false, false, true, lst.count());
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_SKIP | FLAG_LEAF | FLAG_LIST, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        new (view.skip_eos_data()) dataptr_t();
        view.set_list(lst);
        for (size_t i = 0; i < values.size(); ++i) view.set_leaf_value(static_cast<int>(i), values[i]);
        return node;
    }

    // Build SKIP|POP node
    slot_type* build_skip_pop(std::string_view skip, popcount_bitmap bmp, const std::vector<uint64_t>& children) {
        size_t sz = calc_size(true, skip.size(), false, true, false, false, bmp.count());
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_SKIP | FLAG_POP, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        new (view.skip_eos_data()) dataptr_t();
        view.set_bitmap(bmp);
        for (size_t i = 0; i < children.size(); ++i) view.set_child_ptr(static_cast<int>(i), children[i]);
        return node;
    }

    // Build SKIP|LEAF|POP node
    slot_type* build_skip_leaf_pop(std::string_view skip, popcount_bitmap bmp, const std::vector<T>& values) {
        static_assert(can_embed_leaf_v<T>, "T must be embeddable for LEAF");
        size_t sz = calc_size(true, skip.size(), false, true, false, true, bmp.count());
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_SKIP | FLAG_LEAF | FLAG_POP, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        new (view.skip_eos_data()) dataptr_t();
        view.set_bitmap(bmp);
        for (size_t i = 0; i < values.size(); ++i) view.set_leaf_value(static_cast<int>(i), values[i]);
        return node;
    }

    // Build SKIP|FULL node
    slot_type* build_skip_full(std::string_view skip, const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(children.size() == 256);
        size_t sz = calc_size(true, skip.size(), false, false, true, false, 0);
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_SKIP | FLAG_FULL, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        new (view.skip_eos_data()) dataptr_t();
        for (int i = 0; i < 256; ++i) view.set_child_ptr(i, children[i]);
        return node;
    }

    // Build SKIP|LEAF|FULL node
    slot_type* build_skip_leaf_full(std::string_view skip, popcount_bitmap valid_bmp, const std::vector<T>& values) {
        static_assert(can_embed_leaf_v<T>, "T must be embeddable for LEAF");
        KTRIE_DEBUG_ASSERT(values.size() == 256);
        size_t sz = calc_size(true, skip.size(), false, false, true, true, 0);
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_SKIP | FLAG_LEAF | FLAG_FULL, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        new (view.skip_eos_data()) dataptr_t();
        view.set_leaf_full_bitmap(valid_bmp);
        for (int i = 0; i < 256; ++i) view.set_leaf_value(i, values[i]);
        return node;
    }

    // Deep copy
    slot_type* deep_copy(slot_type* src) {
        if (!src) return nullptr;
        node_view_t src_view(src);
        size_t sz = src_view.size();
        slot_type* dst = allocate_node(sz);
        
        store_slot<THREADED>(&dst[0], load_slot<THREADED>(&src[0]));
        node_view_t dst_view(dst);
        
        new (dst_view.eos_data()) dataptr_t();
        dst_view.eos_data()->deep_copy_from(*src_view.eos_data());
        
        if (src_view.has_skip()) {
            dst_view.set_skip_length(src_view.skip_length());
            dst_view.set_skip_chars(src_view.skip_chars());
            new (dst_view.skip_eos_data()) dataptr_t();
            dst_view.skip_eos_data()->deep_copy_from(*src_view.skip_eos_data());
        }
        
        if (src_view.has_list()) dst_view.set_list(src_view.get_list());
        else if (src_view.has_pop()) dst_view.set_bitmap(src_view.get_bitmap());
        else if (src_view.has_leaf() && src_view.has_full()) 
            dst_view.set_leaf_full_bitmap(src_view.get_leaf_full_bitmap());
        
        int num_children = src_view.child_count();
        if (src_view.has_leaf()) {
            for (int i = 0; i < num_children; ++i)
                dst_view.set_leaf_value(i, src_view.get_leaf_value(i));
        } else {
            for (int i = 0; i < num_children; ++i) {
                uint64_t child_ptr = src_view.get_child_ptr(i);
                if (child_ptr) {
                    slot_type* child_copy = deep_copy(reinterpret_cast<slot_type*>(child_ptr));
                    dst_view.set_child_ptr(i, reinterpret_cast<uint64_t>(child_copy));
                }
            }
        }
        return dst;
    }
};

}  // namespace gteitelbaum
