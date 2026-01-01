#pragma once

#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_dataptr.h"
#include "tktrie_small_list.h"
#include "tktrie_popcount.h"

namespace gteitelbaum {

/**
 * Non-owning view into a node array
 * Provides accessors based on flags
 */
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
class node_view {
public:
    using slot_type = slot_type_t<THREADED>;
    using dataptr_t = dataptr<T, THREADED, Allocator>;
    
private:
    slot_type* arr_;

    uint64_t header() const noexcept {
        return load_slot<THREADED>(&arr_[0]);
    }

public:
    explicit node_view(slot_type* arr) noexcept : arr_(arr) {}
    
    slot_type* raw() noexcept { return arr_; }
    const slot_type* raw() const noexcept { return arr_; }

    uint64_t flags() const noexcept { return get_flags(header()); }
    uint32_t version() const noexcept { return get_version(header()); }
    uint32_t size() const noexcept { return get_size(header()); }

    bool has_eos() const noexcept { return (flags() & FLAG_EOS) != 0; }
    bool has_skip() const noexcept { return (flags() & FLAG_SKIP) != 0; }
    bool has_skip_eos() const noexcept { return (flags() & FLAG_SKIP_EOS) != 0; }
    bool has_list() const noexcept { return (flags() & FLAG_LIST) != 0; }
    bool has_pop() const noexcept { return (flags() & FLAG_POP) != 0; }

    void set_header(uint64_t h) noexcept {
        store_slot<THREADED>(&arr_[0], h);
    }

    void increment_version() noexcept {
        uint64_t h = header();
        store_slot<THREADED>(&arr_[0], gteitelbaum::increment_version(h));
    }

    // Compute offset to various sections based on flags
    size_t eos_offset() const noexcept {
        // EOS data is right after header
        return 1;
    }

    size_t skip_len_offset() const noexcept {
        // Skip length comes after header and optional EOS
        size_t off = 1;
        if (has_eos()) off += 1;
        return off;
    }

    size_t skip_chars_offset() const noexcept {
        return skip_len_offset() + 1;
    }

    size_t skip_eos_offset() const noexcept {
        // Skip EOS comes after skip chars
        size_t off = skip_chars_offset();
        off += bytes_to_words(skip_length());
        return off;
    }

    size_t children_header_offset() const noexcept {
        size_t off = 1;
        if (has_eos()) off += 1;
        if (has_skip()) {
            off += 1;  // skip_len
            off += bytes_to_words(skip_length());
            if (has_skip_eos()) off += 1;
        }
        return off;
    }

    size_t child_ptrs_offset() const noexcept {
        size_t off = children_header_offset();
        if (has_list()) off += 1;  // small_list
        else if (has_pop()) off += 4;  // bitmap
        return off;
    }

    // Data accessors
    dataptr_t* eos_data() noexcept {
        KTRIE_DEBUG_ASSERT(has_eos());
        return reinterpret_cast<dataptr_t*>(&arr_[eos_offset()]);
    }

    const dataptr_t* eos_data() const noexcept {
        KTRIE_DEBUG_ASSERT(has_eos());
        return reinterpret_cast<const dataptr_t*>(&arr_[eos_offset()]);
    }

    dataptr_t* skip_eos_data() noexcept {
        KTRIE_DEBUG_ASSERT(has_skip_eos());
        return reinterpret_cast<dataptr_t*>(&arr_[skip_eos_offset()]);
    }

    const dataptr_t* skip_eos_data() const noexcept {
        KTRIE_DEBUG_ASSERT(has_skip_eos());
        return reinterpret_cast<const dataptr_t*>(&arr_[skip_eos_offset()]);
    }

    // Skip accessors
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
        KTRIE_DEBUG_ASSERT(has_skip());
        store_slot<THREADED>(&arr_[skip_len_offset()], static_cast<uint64_t>(len));
    }

    void set_skip_chars(std::string_view s) noexcept {
        KTRIE_DEBUG_ASSERT(has_skip());
        char* data = reinterpret_cast<char*>(&arr_[skip_chars_offset()]);
        std::memcpy(data, s.data(), s.size());
        // Zero-pad remaining bytes
        size_t words = bytes_to_words(s.size());
        size_t total_bytes = words * 8;
        if (s.size() < total_bytes) {
            std::memset(data + s.size(), 0, total_bytes - s.size());
        }
    }

    // Children accessors
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
        for (int i = 0; i < 4; ++i) {
            arr[i] = load_slot<THREADED>(&arr_[off + i]);
        }
        return popcount_bitmap::from_array(arr);
    }

    void set_bitmap(const popcount_bitmap& bmp) noexcept {
        KTRIE_DEBUG_ASSERT(has_pop());
        size_t off = children_header_offset();
        auto arr = bmp.to_array();
        for (int i = 0; i < 4; ++i) {
            store_slot<THREADED>(&arr_[off + i], arr[i]);
        }
    }

    slot_type* child_ptrs() noexcept {
        return &arr_[child_ptrs_offset()];
    }

    const slot_type* child_ptrs() const noexcept {
        return &arr_[child_ptrs_offset()];
    }

    int child_count() const noexcept {
        if (has_list()) return get_list().count();
        if (has_pop()) return get_bitmap().count();
        return 0;
    }

    /**
     * Find child slot for character
     * Returns nullptr if not found
     */
    slot_type* find_child(unsigned char c) noexcept {
        if (has_list()) {
            small_list lst = get_list();
            int off = lst.offset(c);
            if (off == 0) return nullptr;
            return &child_ptrs()[off - 1];
        } else if (has_pop()) {
            popcount_bitmap bmp = get_bitmap();
            int idx;
            if (!bmp.find(c, &idx)) return nullptr;
            return &child_ptrs()[idx];
        }
        return nullptr;
    }

    const slot_type* find_child(unsigned char c) const noexcept {
        return const_cast<node_view*>(this)->find_child(c);
    }

    /**
     * Get child pointer value (for leaf, this may be dataptr)
     */
    uint64_t get_child_ptr(int idx) const noexcept {
        return load_slot<THREADED>(&child_ptrs()[idx]);
    }

    /**
     * Get pointer to child slot (for waiting on READ_BIT)
     */
    slot_type* get_child_slot(int idx) noexcept {
        return &child_ptrs()[idx];
    }

    void set_child_ptr(int idx, uint64_t ptr) noexcept {
        store_slot<THREADED>(&child_ptrs()[idx], ptr);
    }
};

/**
 * Node builder - constructs new node arrays
 */
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
        // Zero initialize
        for (size_t i = 0; i < num_slots; ++i) {
            store_slot<THREADED>(&node[i], 0);
        }
        return node;
    }

public:
    explicit node_builder(const Allocator& alloc = Allocator()) 
        : alloc_(alloc) {}

    slot_alloc_t& allocator() noexcept { return alloc_; }
    const slot_alloc_t& allocator() const noexcept { return alloc_; }

    void deallocate_node(slot_type* node) noexcept {
        if (!node) return;
        node_view_t view(node);
        
        // Clean up dataptr objects
        if (view.has_eos()) {
            view.eos_data()->~dataptr_t();
        }
        if (view.has_skip_eos()) {
            view.skip_eos_data()->~dataptr_t();
        }
        
        size_t sz = view.size();
        slot_alloc_traits::deallocate(alloc_, node, sz);
    }

    /**
     * Calculate required size for a node with given properties
     */
    static size_t calc_size(bool has_eos, bool has_skip, size_t skip_len, 
                            bool has_skip_eos, bool has_list, bool has_pop,
                            int child_count) noexcept {
        size_t sz = 1;  // header
        if (has_eos) sz += 1;
        if (has_skip) {
            sz += 1;  // skip_len
            sz += bytes_to_words(skip_len);
            if (has_skip_eos) sz += 1;
        }
        if (has_list) sz += 1;
        else if (has_pop) sz += 4;
        sz += child_count;
        return sz;
    }

    /**
     * Build EOS-only node (leaf with data, no children)
     */
    template <typename U>
    slot_type* build_eos(U&& value) {
        size_t sz = calc_size(true, false, 0, false, false, false, 0);
        slot_type* node = allocate_node(sz);
        
        uint64_t header = make_header(FLAG_EOS, 0, static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.eos_data()->begin_write();
        view.eos_data()->set(std::forward<U>(value));
        view.eos_data()->end_write();
        
        return node;
    }

    /**
     * Build SKIP | SKIP_EOS node (path compression to terminal)
     */
    template <typename U>
    slot_type* build_skip_eos(std::string_view skip, U&& value) {
        KTRIE_DEBUG_ASSERT(!skip.empty());
        
        size_t sz = calc_size(false, true, skip.size(), true, false, false, 0);
        slot_type* node = allocate_node(sz);
        
        uint64_t header = make_header(FLAG_SKIP | FLAG_SKIP_EOS, 0, static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        
        node_view_t view(node);
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        
        new (view.skip_eos_data()) dataptr_t();
        view.skip_eos_data()->begin_write();
        view.skip_eos_data()->set(std::forward<U>(value));
        view.skip_eos_data()->end_write();
        
        return node;
    }

    /**
     * Build LIST node (1-7 children, no data)
     */
    slot_type* build_list(small_list lst, const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(lst.count() == static_cast<int>(children.size()));
        KTRIE_DEBUG_ASSERT(lst.count() >= 1 && lst.count() <= 7);
        
        size_t sz = calc_size(false, false, 0, false, true, false, lst.count());
        slot_type* node = allocate_node(sz);
        
        uint64_t header = make_header(FLAG_LIST, 0, static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        
        node_view_t view(node);
        view.set_list(lst);
        
        for (size_t i = 0; i < children.size(); ++i) {
            view.set_child_ptr(static_cast<int>(i), children[i]);
        }
        
        return node;
    }

    /**
     * Build POP node (8+ children, no data)
     */
    slot_type* build_pop(popcount_bitmap bmp, const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(bmp.count() == static_cast<int>(children.size()));
        KTRIE_DEBUG_ASSERT(bmp.count() >= 8);
        
        size_t sz = calc_size(false, false, 0, false, false, true, bmp.count());
        slot_type* node = allocate_node(sz);
        
        uint64_t header = make_header(FLAG_POP, 0, static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        
        node_view_t view(node);
        view.set_bitmap(bmp);
        
        for (size_t i = 0; i < children.size(); ++i) {
            view.set_child_ptr(static_cast<int>(i), children[i]);
        }
        
        return node;
    }

    /**
     * Build EOS | LIST node (data here + 1-7 children)
     */
    template <typename U>
    slot_type* build_eos_list(U&& value, small_list lst, const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(lst.count() == static_cast<int>(children.size()));
        
        size_t sz = calc_size(true, false, 0, false, true, false, lst.count());
        slot_type* node = allocate_node(sz);
        
        uint64_t header = make_header(FLAG_EOS | FLAG_LIST, 0, static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.eos_data()->begin_write();
        view.eos_data()->set(std::forward<U>(value));
        view.eos_data()->end_write();
        
        view.set_list(lst);
        
        for (size_t i = 0; i < children.size(); ++i) {
            view.set_child_ptr(static_cast<int>(i), children[i]);
        }
        
        return node;
    }

    /**
     * Build EOS | POP node (data here + 8+ children)
     */
    template <typename U>
    slot_type* build_eos_pop(U&& value, popcount_bitmap bmp, const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(bmp.count() == static_cast<int>(children.size()));
        
        size_t sz = calc_size(true, false, 0, false, false, true, bmp.count());
        slot_type* node = allocate_node(sz);
        
        uint64_t header = make_header(FLAG_EOS | FLAG_POP, 0, static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.eos_data()->begin_write();
        view.eos_data()->set(std::forward<U>(value));
        view.eos_data()->end_write();
        
        view.set_bitmap(bmp);
        
        for (size_t i = 0; i < children.size(); ++i) {
            view.set_child_ptr(static_cast<int>(i), children[i]);
        }
        
        return node;
    }

    /**
     * Build SKIP | LIST node (path compression to branch)
     */
    slot_type* build_skip_list(std::string_view skip, small_list lst, 
                               const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(!skip.empty());
        KTRIE_DEBUG_ASSERT(lst.count() == static_cast<int>(children.size()));
        
        size_t sz = calc_size(false, true, skip.size(), false, true, false, lst.count());
        slot_type* node = allocate_node(sz);
        
        uint64_t header = make_header(FLAG_SKIP | FLAG_LIST, 0, static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        
        node_view_t view(node);
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        view.set_list(lst);
        
        for (size_t i = 0; i < children.size(); ++i) {
            view.set_child_ptr(static_cast<int>(i), children[i]);
        }
        
        return node;
    }

    /**
     * Build SKIP | POP node (path compression to large branch)
     */
    slot_type* build_skip_pop(std::string_view skip, popcount_bitmap bmp,
                              const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(!skip.empty());
        KTRIE_DEBUG_ASSERT(bmp.count() == static_cast<int>(children.size()));
        
        size_t sz = calc_size(false, true, skip.size(), false, false, true, bmp.count());
        slot_type* node = allocate_node(sz);
        
        uint64_t header = make_header(FLAG_SKIP | FLAG_POP, 0, static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        
        node_view_t view(node);
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        view.set_bitmap(bmp);
        
        for (size_t i = 0; i < children.size(); ++i) {
            view.set_child_ptr(static_cast<int>(i), children[i]);
        }
        
        return node;
    }

    /**
     * Build SKIP | SKIP_EOS | LIST node
     */
    template <typename U>
    slot_type* build_skip_eos_list(std::string_view skip, U&& value, 
                                    small_list lst, const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(!skip.empty());
        KTRIE_DEBUG_ASSERT(lst.count() == static_cast<int>(children.size()));
        
        size_t sz = calc_size(false, true, skip.size(), true, true, false, lst.count());
        slot_type* node = allocate_node(sz);
        
        uint64_t header = make_header(FLAG_SKIP | FLAG_SKIP_EOS | FLAG_LIST, 0, 
                                      static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        
        node_view_t view(node);
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        
        new (view.skip_eos_data()) dataptr_t();
        view.skip_eos_data()->begin_write();
        view.skip_eos_data()->set(std::forward<U>(value));
        view.skip_eos_data()->end_write();
        
        view.set_list(lst);
        
        for (size_t i = 0; i < children.size(); ++i) {
            view.set_child_ptr(static_cast<int>(i), children[i]);
        }
        
        return node;
    }

    /**
     * Build SKIP | SKIP_EOS | POP node
     */
    template <typename U>
    slot_type* build_skip_eos_pop(std::string_view skip, U&& value,
                                   popcount_bitmap bmp, const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(!skip.empty());
        KTRIE_DEBUG_ASSERT(bmp.count() == static_cast<int>(children.size()));
        
        size_t sz = calc_size(false, true, skip.size(), true, false, true, bmp.count());
        slot_type* node = allocate_node(sz);
        
        uint64_t header = make_header(FLAG_SKIP | FLAG_SKIP_EOS | FLAG_POP, 0,
                                      static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        
        node_view_t view(node);
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        
        new (view.skip_eos_data()) dataptr_t();
        view.skip_eos_data()->begin_write();
        view.skip_eos_data()->set(std::forward<U>(value));
        view.skip_eos_data()->end_write();
        
        view.set_bitmap(bmp);
        
        for (size_t i = 0; i < children.size(); ++i) {
            view.set_child_ptr(static_cast<int>(i), children[i]);
        }
        
        return node;
    }

    /**
     * Build EOS | SKIP | SKIP_EOS node
     * (data here, then path compression to more data, no children)
     */
    template <typename U1, typename U2>
    slot_type* build_eos_skip_eos(U1&& eos_value, std::string_view skip, U2&& skip_eos_value) {
        KTRIE_DEBUG_ASSERT(!skip.empty());
        
        size_t sz = calc_size(true, true, skip.size(), true, false, false, 0);
        slot_type* node = allocate_node(sz);
        
        uint64_t header = make_header(FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS, 0,
                                      static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.eos_data()->begin_write();
        view.eos_data()->set(std::forward<U1>(eos_value));
        view.eos_data()->end_write();
        
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        
        new (view.skip_eos_data()) dataptr_t();
        view.skip_eos_data()->begin_write();
        view.skip_eos_data()->set(std::forward<U2>(skip_eos_value));
        view.skip_eos_data()->end_write();
        
        return node;
    }

    /**
     * Build EOS | SKIP | SKIP_EOS | LIST node
     */
    template <typename U1, typename U2>
    slot_type* build_eos_skip_eos_list(U1&& eos_value, std::string_view skip, U2&& skip_eos_value,
                                        small_list lst, const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(!skip.empty());
        
        size_t sz = calc_size(true, true, skip.size(), true, true, false, lst.count());
        slot_type* node = allocate_node(sz);
        
        uint64_t header = make_header(FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS | FLAG_LIST, 0,
                                      static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.eos_data()->begin_write();
        view.eos_data()->set(std::forward<U1>(eos_value));
        view.eos_data()->end_write();
        
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        
        new (view.skip_eos_data()) dataptr_t();
        view.skip_eos_data()->begin_write();
        view.skip_eos_data()->set(std::forward<U2>(skip_eos_value));
        view.skip_eos_data()->end_write();
        
        view.set_list(lst);
        
        for (size_t i = 0; i < children.size(); ++i) {
            view.set_child_ptr(static_cast<int>(i), children[i]);
        }
        
        return node;
    }

    /**
     * Build EOS | SKIP | SKIP_EOS | POP node
     */
    template <typename U1, typename U2>
    slot_type* build_eos_skip_eos_pop(U1&& eos_value, std::string_view skip, U2&& skip_eos_value,
                                       popcount_bitmap bmp, const std::vector<uint64_t>& children) {
        KTRIE_DEBUG_ASSERT(!skip.empty());
        
        size_t sz = calc_size(true, true, skip.size(), true, false, true, bmp.count());
        slot_type* node = allocate_node(sz);
        
        uint64_t header = make_header(FLAG_EOS | FLAG_SKIP | FLAG_SKIP_EOS | FLAG_POP, 0,
                                      static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        
        node_view_t view(node);
        new (view.eos_data()) dataptr_t();
        view.eos_data()->begin_write();
        view.eos_data()->set(std::forward<U1>(eos_value));
        view.eos_data()->end_write();
        
        view.set_skip_length(skip.size());
        view.set_skip_chars(skip);
        
        new (view.skip_eos_data()) dataptr_t();
        view.skip_eos_data()->begin_write();
        view.skip_eos_data()->set(std::forward<U2>(skip_eos_value));
        view.skip_eos_data()->end_write();
        
        view.set_bitmap(bmp);
        
        for (size_t i = 0; i < children.size(); ++i) {
            view.set_child_ptr(static_cast<int>(i), children[i]);
        }
        
        return node;
    }

    /**
     * Build empty root node
     */
    slot_type* build_empty_root() {
        size_t sz = 1;  // just header
        slot_type* node = allocate_node(sz);
        uint64_t header = make_header(0, 0, static_cast<uint32_t>(sz));
        store_slot<THREADED>(&node[0], header);
        return node;
    }

    /**
     * Deep copy a node (recursively copies children)
     */
    slot_type* deep_copy(slot_type* src) {
        if (!src) return nullptr;
        
        node_view_t src_view(src);
        size_t sz = src_view.size();
        
        slot_type* dst = allocate_node(sz);
        
        // Copy header
        store_slot<THREADED>(&dst[0], load_slot<THREADED>(&src[0]));
        
        node_view_t dst_view(dst);
        
        // Deep copy EOS data
        if (src_view.has_eos()) {
            new (dst_view.eos_data()) dataptr_t();
            dst_view.eos_data()->deep_copy_from(*src_view.eos_data());
        }
        
        // Copy skip
        if (src_view.has_skip()) {
            dst_view.set_skip_length(src_view.skip_length());
            dst_view.set_skip_chars(src_view.skip_chars());
            
            // Deep copy SKIP_EOS data
            if (src_view.has_skip_eos()) {
                new (dst_view.skip_eos_data()) dataptr_t();
                dst_view.skip_eos_data()->deep_copy_from(*src_view.skip_eos_data());
            }
        }
        
        // Copy children structure
        if (src_view.has_list()) {
            dst_view.set_list(src_view.get_list());
        } else if (src_view.has_pop()) {
            dst_view.set_bitmap(src_view.get_bitmap());
        }
        
        // Recursively copy children
        int num_children = src_view.child_count();
        for (int i = 0; i < num_children; ++i) {
            uint64_t child_ptr = src_view.get_child_ptr(i);
            if constexpr (FIXED_LEN > 0) {
                // At leaf level, children are dataptr, not nodes
                // This needs depth tracking - for now just copy pointer
                // The caller should handle leaf vs non-leaf distinction
                dst_view.set_child_ptr(i, child_ptr);
            } else {
                slot_type* child = reinterpret_cast<slot_type*>(child_ptr & PTR_MASK);
                if (child) {
                    slot_type* child_copy = deep_copy(child);
                    dst_view.set_child_ptr(i, reinterpret_cast<uint64_t>(child_copy));
                } else {
                    dst_view.set_child_ptr(i, 0);
                }
            }
        }
        
        return dst;
    }
};

}  // namespace gteitelbaum
