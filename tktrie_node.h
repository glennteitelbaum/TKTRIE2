#pragma once

#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_dataptr.h"

namespace gteitelbaum {

/**
 * Node layouts:
 * 
 * NON-LEAF (EOS/SKIP_EOS slots always exist):
 *   [header][eos]                                          - no children
 *   [header][eos][lst][ptr x N]                            - LIST
 *   [header][eos][bmp x4][ptr x N]                         - POP
 *   [header][eos][ptr x256]                                - FULL
 *   [header][eos][skip_len][chars...][skip_eos]            - SKIP, no children
 *   [header][eos][skip_len][chars...][skip_eos][lst][ptr x N]  - SKIP+LIST
 *   etc.
 * 
 * LEAF (EOS slot only if LIST|POP without FULL):
 *   [header][lst][T x N]                                   - LEAF|LIST
 *   [header][bmp x4][T x N]                                - LEAF|POP  
 *   [header][valid_bmp x4][T x256]                         - LEAF|FULL
 *   [header][eos]                                          - LEAF|LIST|POP (terminal)
 *   [header][skip_len][chars...][skip_eos]                 - LEAF|SKIP|LIST|POP (skip to terminal)
 *   [header][skip_len][chars...][lst][T x N]               - LEAF|SKIP|LIST
 *   [header][skip_len][chars...][bmp x4][T x N]            - LEAF|SKIP|POP
 *   [header][skip_len][chars...][valid_bmp x4][T x256]     - LEAF|SKIP|FULL
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

    bool has_skip() const noexcept { return flags_has_skip(flags()); }
    bool has_list() const noexcept { return flags_has_list(flags()); }
    bool has_pop() const noexcept { return flags_has_pop(flags()); }
    bool has_full() const noexcept { return flags_has_full(flags()); }
    bool has_leaf() const noexcept { return flags_has_leaf(flags()); }
    
    // For LEAF: LIST|POP without FULL = terminal with EOS
    bool leaf_has_eos() const noexcept { return flags_leaf_has_eos(flags()); }
    bool leaf_has_children() const noexcept { return flags_leaf_has_children(flags()); }

    void set_header(uint64_t h) noexcept { store_slot<THREADED>(&arr_[0], h); }

    // === NON-LEAF: EOS always at offset 1 ===
    dataptr_t* eos_data() noexcept {
        KTRIE_DEBUG_ASSERT(!has_leaf() || leaf_has_eos());
        return reinterpret_cast<dataptr_t*>(&arr_[1]);
    }
    const dataptr_t* eos_data() const noexcept {
        KTRIE_DEBUG_ASSERT(!has_leaf() || leaf_has_eos());
        return reinterpret_cast<const dataptr_t*>(&arr_[1]);
    }

    // === SKIP section ===
    size_t skip_len_offset() const noexcept {
        if (has_leaf()) return 1;  // LEAF: right after header
        return 2;  // NON-LEAF: after header + eos
    }

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
        size_t total = bytes_to_words(s.size()) * 8;
        if (s.size() < total) std::memset(data + s.size(), 0, total - s.size());
    }

    // For NON-LEAF with SKIP, or LEAF|SKIP|LIST|POP (terminal)
    dataptr_t* skip_eos_data() noexcept {
        KTRIE_DEBUG_ASSERT(has_skip());
        KTRIE_DEBUG_ASSERT(!has_leaf() || leaf_has_eos());
        return reinterpret_cast<dataptr_t*>(&arr_[skip_eos_offset()]);
    }
    const dataptr_t* skip_eos_data() const noexcept {
        KTRIE_DEBUG_ASSERT(has_skip());
        KTRIE_DEBUG_ASSERT(!has_leaf() || leaf_has_eos());
        return reinterpret_cast<const dataptr_t*>(&arr_[skip_eos_offset()]);
    }

    // === Children header offset ===
    size_t children_header_offset() const noexcept {
        if (has_leaf()) {
            if (leaf_has_eos()) return 0;  // No children
            size_t off = 1;  // header
            if (has_skip()) {
                off += 1 + bytes_to_words(skip_length());  // skip_len + chars
            }
            return off;
        } else {
            size_t off = 2;  // header + eos
            if (has_skip()) {
                off += 1 + bytes_to_words(skip_length()) + 1;  // skip_len + chars + skip_eos
            }
            return off;
        }
    }

    // For LEAF|FULL: validity bitmap at children_header_offset
    popcount_bitmap get_leaf_full_bitmap() const noexcept {
        KTRIE_DEBUG_ASSERT(has_leaf() && has_full());
        size_t off = children_header_offset();
        std::array<uint64_t, 4> arr;
        for (int i = 0; i < 4; ++i) arr[i] = load_slot<THREADED>(&arr_[off + i]);
        return popcount_bitmap::from_array(arr);
    }

    void set_leaf_full_bitmap(const popcount_bitmap& bmp) noexcept {
        KTRIE_DEBUG_ASSERT(has_leaf() && has_full());
        size_t off = children_header_offset();
        auto arr = bmp.to_array();
        for (int i = 0; i < 4; ++i) store_slot<THREADED>(&arr_[off + i], arr[i]);
    }

    // Atomic in-place operations for LEAF|FULL
    bool leaf_full_test_bit(unsigned char c) const noexcept {
        KTRIE_DEBUG_ASSERT(has_leaf() && has_full());
        size_t off = children_header_offset();
        int word = c >> 6;
        int bit = c & 63;
        uint64_t val = load_slot<THREADED>(&arr_[off + word]);
        return (val & (1ULL << bit)) != 0;
    }

    void leaf_full_set_bit(unsigned char c) noexcept {
        KTRIE_DEBUG_ASSERT(has_leaf() && has_full());
        size_t off = children_header_offset();
        int word = c >> 6;
        int bit = c & 63;
        if constexpr (THREADED) {
            arr_[off + word].fetch_or(1ULL << bit, std::memory_order_release);
        } else {
            uint64_t old = arr_[off + word];
            arr_[off + word] = old | (1ULL << bit);
        }
    }

    void leaf_full_clear_bit(unsigned char c) noexcept {
        KTRIE_DEBUG_ASSERT(has_leaf() && has_full());
        size_t off = children_header_offset();
        int word = c >> 6;
        int bit = c & 63;
        if constexpr (THREADED) {
            arr_[off + word].fetch_and(~(1ULL << bit), std::memory_order_release);
        } else {
            uint64_t old = arr_[off + word];
            arr_[off + word] = old & ~(1ULL << bit);
        }
    }

    size_t child_ptrs_offset() const noexcept {
        size_t off = children_header_offset();
        if (has_full()) {
            if (has_leaf()) off += 4;  // validity bitmap
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

    int child_count() const noexcept {
        if (leaf_has_eos()) return 0;
        if (has_full()) return 256;
        if (has_list()) return get_list().count();
        if (has_pop()) return get_bitmap().count();
        return 0;
    }

    int live_child_count() const noexcept {
        if (leaf_has_eos()) return 0;
        if (has_leaf() && has_full()) return get_leaf_full_bitmap().count();
        if (has_full()) {
            int c = 0;
            for (int i = 0; i < 256; ++i) if (load_slot<THREADED>(&child_ptrs()[i]) != 0) ++c;
            return c;
        }
        if (has_list()) {
            if (has_leaf()) return get_list().count();
            int c = 0;
            for (int i = 0; i < get_list().count(); ++i) if (load_slot<THREADED>(&child_ptrs()[i]) != 0) ++c;
            return c;
        }
        if (has_pop()) {
            if (has_leaf()) return get_bitmap().count();
            int c = 0;
            for (int i = 0; i < get_bitmap().count(); ++i) if (load_slot<THREADED>(&child_ptrs()[i]) != 0) ++c;
            return c;
        }
        return 0;
    }

    slot_type* find_child(unsigned char c) noexcept {
        if (leaf_has_eos()) return nullptr;
        if (has_full()) {
            if (has_leaf() && !get_leaf_full_bitmap().contains(c)) return nullptr;
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

    uint64_t get_child_ptr(int idx) const noexcept { return load_slot<THREADED>(&child_ptrs()[idx]); }
    void set_child_ptr(int idx, uint64_t ptr) noexcept { store_slot<THREADED>(&child_ptrs()[idx], ptr); }

    T get_leaf_value(int idx) const noexcept {
        KTRIE_DEBUG_ASSERT(has_leaf() && leaf_has_children());
        static_assert(can_embed_leaf_v<T>);
        T val{};
        uint64_t raw = load_slot<THREADED>(&child_ptrs()[idx]);
        std::memcpy(&val, &raw, sizeof(T));
        return val;
    }

    void set_leaf_value(int idx, const T& val) noexcept {
        KTRIE_DEBUG_ASSERT(has_leaf() && leaf_has_children());
        static_assert(can_embed_leaf_v<T>);
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
        if (!view.has_leaf()) {
            view.eos_data()->~dataptr_t();
            if (view.has_skip()) view.skip_eos_data()->~dataptr_t();
        } else if (view.leaf_has_eos()) {
            if (view.has_skip()) view.skip_eos_data()->~dataptr_t();
            else view.eos_data()->~dataptr_t();
        }
        slot_alloc_traits::deallocate(alloc_, node, view.size());
    }

    // =========================================================================
    // NON-LEAF builders (EOS slot always exists)
    // =========================================================================

    slot_type* build_empty() {
        size_t sz = 2;  // header + eos
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(0, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        return node;
    }

    slot_type* build_skip(std::string_view skip) {
        size_t sz = 2 + 1 + bytes_to_words(skip.size()) + 1;
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_SKIP, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        new (view.skip_eos_data()) dataptr_t();
        return node;
    }

    slot_type* build_list(small_list lst, const std::vector<uint64_t>& children) {
        size_t sz = 2 + 1 + lst.count();
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_LIST, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_list(lst);
        for (size_t i = 0; i < children.size(); ++i) view.set_child_ptr(static_cast<int>(i), children[i]);
        return node;
    }

    slot_type* build_pop(popcount_bitmap bmp, const std::vector<uint64_t>& children) {
        size_t sz = 2 + 4 + bmp.count();
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_POP, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_bitmap(bmp);
        for (size_t i = 0; i < children.size(); ++i) view.set_child_ptr(static_cast<int>(i), children[i]);
        return node;
    }

    slot_type* build_full(const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(children.size() == 256);
        size_t sz = 2 + 256;
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_FULL, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        for (int i = 0; i < 256; ++i) view.set_child_ptr(i, children[i]);
        return node;
    }

    slot_type* build_skip_list(std::string_view skip, small_list lst, const std::vector<uint64_t>& children) {
        size_t sz = 2 + 1 + bytes_to_words(skip.size()) + 1 + 1 + lst.count();
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

    slot_type* build_skip_pop(std::string_view skip, popcount_bitmap bmp, const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(children.size() == static_cast<size_t>(bmp.count()));
        size_t sz = 2 + 1 + bytes_to_words(skip.size()) + 1 + 4 + bmp.count();
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_SKIP | FLAG_POP, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        new (view.skip_eos_data()) dataptr_t();
        view.set_bitmap(bmp);
        for (int i = 0; i < bmp.count(); ++i) view.set_child_ptr(i, children[i]);
        return node;
    }

    slot_type* build_skip_full(std::string_view skip, const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(children.size() == 256);
        size_t sz = 2 + 1 + bytes_to_words(skip.size()) + 1 + 256;
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

    // =========================================================================
    // LEAF builders (EOS only if LIST|POP without FULL)
    // =========================================================================

    // LEAF terminal (no children)
    slot_type* build_leaf_terminal() {
        static_assert(can_embed_leaf_v<T>);
        size_t sz = 2;  // header + eos
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_LEAF | FLAG_LIST | FLAG_POP, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        return node;
    }

    // LEAF|SKIP terminal
    slot_type* build_leaf_skip_terminal(std::string_view skip) {
        static_assert(can_embed_leaf_v<T>);
        size_t sz = 1 + 1 + bytes_to_words(skip.size()) + 1;  // header + skip_len + chars + skip_eos
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_LEAF | FLAG_SKIP | FLAG_LIST | FLAG_POP, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        new (view.skip_eos_data()) dataptr_t();
        return node;
    }

    slot_type* build_leaf_list(small_list lst, const std::vector<T>& values) {
        static_assert(can_embed_leaf_v<T>);
        size_t sz = 1 + 1 + lst.count();
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_LEAF | FLAG_LIST, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        view.set_list(lst);
        for (size_t i = 0; i < values.size(); ++i) view.set_leaf_value(static_cast<int>(i), values[i]);
        return node;
    }

    slot_type* build_leaf_pop(popcount_bitmap bmp, const std::vector<T>& values) {
        static_assert(can_embed_leaf_v<T>);
        size_t sz = 1 + 4 + bmp.count();
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_LEAF | FLAG_POP, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        view.set_bitmap(bmp);
        for (size_t i = 0; i < values.size(); ++i) view.set_leaf_value(static_cast<int>(i), values[i]);
        return node;
    }

    slot_type* build_leaf_full(popcount_bitmap valid_bmp, const std::vector<T>& values) {
        static_assert(can_embed_leaf_v<T>);
        KTRIE_DEBUG_ASSERT(values.size() == 256);
        size_t sz = 1 + 4 + 256;
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_LEAF | FLAG_FULL, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        view.set_leaf_full_bitmap(valid_bmp);
        for (int i = 0; i < 256; ++i) view.set_leaf_value(i, values[i]);
        return node;
    }

    slot_type* build_leaf_skip_list(std::string_view skip, small_list lst, const std::vector<T>& values) {
        static_assert(can_embed_leaf_v<T>);
        size_t sz = 1 + 1 + bytes_to_words(skip.size()) + 1 + lst.count();
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_LEAF | FLAG_SKIP | FLAG_LIST, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        view.set_list(lst);
        for (size_t i = 0; i < values.size(); ++i) view.set_leaf_value(static_cast<int>(i), values[i]);
        return node;
    }

    slot_type* build_leaf_skip_pop(std::string_view skip, popcount_bitmap bmp, const std::vector<T>& values) {
        static_assert(can_embed_leaf_v<T>);
        size_t sz = 1 + 1 + bytes_to_words(skip.size()) + 4 + bmp.count();
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_LEAF | FLAG_SKIP | FLAG_POP, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        view.set_bitmap(bmp);
        for (size_t i = 0; i < values.size(); ++i) view.set_leaf_value(static_cast<int>(i), values[i]);
        return node;
    }

    slot_type* build_leaf_skip_full(std::string_view skip, popcount_bitmap valid_bmp, const std::vector<T>& values) {
        static_assert(can_embed_leaf_v<T>);
        KTRIE_DEBUG_ASSERT(values.size() == 256);
        size_t sz = 1 + 1 + bytes_to_words(skip.size()) + 4 + 256;
        slot_type* node = allocate_node(sz);
        store_slot<THREADED>(&node[0], make_header(FLAG_LEAF | FLAG_SKIP | FLAG_FULL, static_cast<uint32_t>(sz)));
        node_view_t view(node);
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        view.set_leaf_full_bitmap(valid_bmp);
        for (int i = 0; i < 256; ++i) view.set_leaf_value(i, values[i]);
        return node;
    }

    // =========================================================================
    // Deep copy
    // =========================================================================
    slot_type* deep_copy(slot_type* src) {
        if (!src) return nullptr;
        node_view_t sv(src);
        size_t sz = sv.size();
        slot_type* dst = allocate_node(sz);
        store_slot<THREADED>(&dst[0], load_slot<THREADED>(&src[0]));
        node_view_t dv(dst);

        if (!sv.has_leaf()) {
            new (dv.eos_data()) dataptr_t();
            dv.eos_data()->deep_copy_from(*sv.eos_data());
            if (sv.has_skip()) {
                dv.set_skip_length(sv.skip_length());
                dv.set_skip_chars(sv.skip_chars());
                new (dv.skip_eos_data()) dataptr_t();
                dv.skip_eos_data()->deep_copy_from(*sv.skip_eos_data());
            }
        } else if (sv.leaf_has_eos()) {
            if (sv.has_skip()) {
                dv.set_skip_length(sv.skip_length());
                dv.set_skip_chars(sv.skip_chars());
                new (dv.skip_eos_data()) dataptr_t();
                dv.skip_eos_data()->deep_copy_from(*sv.skip_eos_data());
            } else {
                new (dv.eos_data()) dataptr_t();
                dv.eos_data()->deep_copy_from(*sv.eos_data());
            }
        } else {
            if (sv.has_skip()) {
                dv.set_skip_length(sv.skip_length());
                dv.set_skip_chars(sv.skip_chars());
            }
        }

        if (sv.has_list()) dv.set_list(sv.get_list());
        else if (sv.has_pop()) dv.set_bitmap(sv.get_bitmap());
        else if (sv.has_leaf() && sv.has_full()) dv.set_leaf_full_bitmap(sv.get_leaf_full_bitmap());

        int nc = sv.child_count();
        if (sv.has_leaf() && sv.leaf_has_children()) {
            for (int i = 0; i < nc; ++i) dv.set_leaf_value(i, sv.get_leaf_value(i));
        } else if (!sv.leaf_has_eos()) {
            for (int i = 0; i < nc; ++i) {
                uint64_t cp = sv.get_child_ptr(i);
                if (cp) dv.set_child_ptr(i, reinterpret_cast<uint64_t>(deep_copy(reinterpret_cast<slot_type*>(cp))));
            }
        }
        return dst;
    }
};

}  // namespace gteitelbaum
