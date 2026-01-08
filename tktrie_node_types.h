#pragma once

// This file contains node type definitions (SKIP, BINARY, LIST, POP, FULL)
// It should only be included from tktrie_node.h

namespace gteitelbaum {

// =============================================================================
// SKIP_NODE - skip string + single value (always leaf)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct skip_node : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    
    data_t value;
    
    skip_node() = default;
    ~skip_node() = default;
};

// =============================================================================
// BINARY_NODE - LEAF specialization (stores 2 values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct binary_node<T, THREADED, Allocator, FIXED_LEN, true> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    
    static constexpr int MAX_ENTRIES = 2;
    
    unsigned char chars[2] = {};
    int count_ = 0;
    std::array<data_t, 2> values;
    
    binary_node() = default;
    ~binary_node() = default;
    
    int count() const noexcept { return count_; }
    bool has(unsigned char c) const noexcept { 
        return (count_ > 0 && chars[0] == c) || (count_ > 1 && chars[1] == c); 
    }
    
    int find(unsigned char c) const noexcept {
        if (count_ > 0 && chars[0] == c) return 0;
        if (count_ > 1 && chars[1] == c) return 1;
        return -1;
    }
    
    void add_entry(unsigned char c, const T& value) {
        chars[count_] = c;
        values[count_].set(value);
        ++count_;
    }
    
    void remove_entry(int idx) {
        if (idx == 0 && count_ == 2) {
            chars[0] = chars[1];
            values[0] = std::move(values[1]);
        }
        --count_;
    }
    
    void copy_values_to(binary_node* dest) const {
        dest->count_ = count_;
        for (int i = 0; i < count_; ++i) {
            dest->chars[i] = chars[i];
            dest->values[i].deep_copy_from(values[i]);
        }
    }
    
    void update_capacity_flags() noexcept {
        // BINARY: floor=1, ceil=2
        if (count_ <= BINARY_MIN) this->set_floor(); else this->clear_floor();
        if (count_ >= BINARY_MAX) this->set_ceil(); else this->clear_ceil();
    }
};

// =============================================================================
// BINARY_NODE - INTERIOR specialization, FIXED_LEN > 0 (no eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
    requires (FIXED_LEN > 0)
struct binary_node<T, THREADED, Allocator, FIXED_LEN, false> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    static constexpr int MAX_CHILDREN = 2;
    
    unsigned char chars[2] = {};
    int count_ = 0;
    std::array<atomic_ptr, 2> children;
    
    binary_node() = default;
    ~binary_node() = default;
    
    int count() const noexcept { return count_; }
    bool has(unsigned char c) const noexcept { 
        return (count_ > 0 && chars[0] == c) || (count_ > 1 && chars[1] == c); 
    }
    
    int find(unsigned char c) const noexcept {
        if (count_ > 0 && chars[0] == c) return 0;
        if (count_ > 1 && chars[1] == c) return 1;
        return -1;
    }
    
    ptr_t get_child(unsigned char c) const noexcept {
        int idx = find(c);
        return idx >= 0 ? children[idx].load() : nullptr;
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        int idx = find(c);
        return idx >= 0 ? &children[idx] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        chars[count_] = c;
        children[count_].store(child);
        ++count_;
    }
    
    void remove_child(int idx) {
        if (idx == 0 && count_ == 2) {
            chars[0] = chars[1];
            children[0].store(children[1].load());
        }
        children[count_ - 1].store(nullptr);
        --count_;
    }
    
    unsigned char first_char() const noexcept { return chars[0]; }
    ptr_t child_at_slot(int slot) const noexcept { return children[slot].load(); }
    
    void move_children_to(binary_node* dest) {
        dest->count_ = count_;
        for (int i = 0; i < count_; ++i) {
            dest->chars[i] = chars[i];
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
        count_ = 0;
    }
    
    void copy_children_to(binary_node* dest) const {
        dest->count_ = count_;
        for (int i = 0; i < count_; ++i) {
            dest->chars[i] = chars[i];
            dest->children[i].store(children[i].load());
        }
    }
    
    void copy_interior_to(binary_node* dest) const {
        copy_children_to(dest);
    }
    
    void update_capacity_flags() noexcept {
        // BINARY: floor=1, ceil=2
        if (count_ <= BINARY_MIN) this->set_floor(); else this->clear_floor();
        if (count_ >= BINARY_MAX) this->set_ceil(); else this->clear_ceil();
    }
};

// =============================================================================
// BINARY_NODE - INTERIOR specialization, FIXED_LEN == 0 (has eos)
// Entry count = child_count + (has_eos ? 1 : 0), supports 1-2 entries
// Examples: 1 child + EOS = 2, 2 children = 2, 1 child alone = 1
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct binary_node<T, THREADED, Allocator, 0, false> 
    : node_with_skip<T, THREADED, Allocator, 0> {
    using base_t = node_with_skip<T, THREADED, Allocator, 0>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using eos_data_t = typename base_t::eos_data_t;
    
    static constexpr int MAX_CHILDREN = 2;
    
    eos_data_t eos;
    unsigned char chars[2] = {};
    int count_ = 0;  // child count (0-2), entry count = count_ + eos.has_data()
    std::array<atomic_ptr, 2> children;
    
    binary_node() = default;
    ~binary_node() = default;
    
    int count() const noexcept { return count_; }  // child count
    int entry_count() const noexcept { return count_ + (eos.has_data() ? 1 : 0); }
    
    bool has(unsigned char c) const noexcept { 
        return (count_ > 0 && chars[0] == c) || (count_ > 1 && chars[1] == c); 
    }
    
    int find(unsigned char c) const noexcept {
        if (count_ > 0 && chars[0] == c) return 0;
        if (count_ > 1 && chars[1] == c) return 1;
        return -1;
    }
    
    ptr_t get_child(unsigned char c) const noexcept {
        int idx = find(c);
        return idx >= 0 ? children[idx].load() : nullptr;
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        int idx = find(c);
        return idx >= 0 ? &children[idx] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        chars[count_] = c;
        children[count_].store(child);
        ++count_;
    }
    
    void remove_child(int idx) {
        if (idx == 0 && count_ == 2) {
            chars[0] = chars[1];
            children[0].store(children[1].load());
        }
        children[count_ - 1].store(nullptr);
        --count_;
    }
    
    unsigned char first_char() const noexcept { return chars[0]; }
    ptr_t child_at_slot(int slot) const noexcept { return children[slot].load(); }
    
    void move_children_to(binary_node* dest) {
        dest->count_ = count_;
        for (int i = 0; i < count_; ++i) {
            dest->chars[i] = chars[i];
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
        count_ = 0;
    }
    
    void copy_children_to(binary_node* dest) const {
        dest->count_ = count_;
        for (int i = 0; i < count_; ++i) {
            dest->chars[i] = chars[i];
            dest->children[i].store(children[i].load());
        }
    }
    
    void move_interior_to(binary_node* dest) {
        dest->eos = std::move(eos);
        move_children_to(dest);
    }
    
    void copy_interior_to(binary_node* dest) const {
        dest->eos.deep_copy_from(eos);
        copy_children_to(dest);
    }
    
    void update_capacity_flags() noexcept {
        // BINARY: floor=1, ceil=2 (based on child count only, EOS doesn't count)
        if (count_ <= BINARY_MIN) this->set_floor(); else this->clear_floor();
        if (count_ >= BINARY_MAX) this->set_ceil(); else this->clear_ceil();
    }
};

// =============================================================================
// LIST_NODE - LEAF specialization (stores values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct list_node<T, THREADED, Allocator, FIXED_LEN, true> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    
    static constexpr int MAX_CHILDREN = 7;
    
    small_list<THREADED> chars;
    std::array<data_t, MAX_CHILDREN> values;
    
    list_node() = default;
    ~list_node() = default;
    
    // Unified interface
    int count() const noexcept { return chars.count(); }
    int find(unsigned char c) const noexcept { return chars.find(c); }
    bool has(unsigned char c) const noexcept { return chars.find(c) >= 0; }
    
    // Caller must verify find(c) >= 0 first
    bool read_value(int idx, T& out) const noexcept {
        [[assume(idx >= 0 && idx < 7)]];
        return values[idx].try_read(out);
    }
    
    void set_value(unsigned char c, const T& val) {
        int idx = chars.find(c);
        if (idx >= 0) {
            values[idx].set(val);
        } else {
            idx = chars.add(c);
            values[idx].set(val);
        }
    }
    
    int add_value(unsigned char c, const T& val) {
        int idx = chars.add(c);
        values[idx].set(val);
        return idx;
    }
    
    void remove_value(unsigned char c) {
        int idx = chars.find(c);
        if (idx < 0) return;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = idx; i < cnt - 1; ++i) {
            values[i] = std::move(values[i + 1]);
        }
        values[cnt - 1].clear();
        chars.remove_at(idx);
    }
    
    void copy_values_to(list_node* dest) const {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->values[i].deep_copy_from(values[i]);
        }
    }
    
    void update_capacity_flags() noexcept {
        // LIST: floor=3, ceil=7
        int cnt = chars.count();
        if (cnt <= LIST_MIN) this->set_floor(); else this->clear_floor();
        if (cnt >= LIST_MAX) this->set_ceil(); else this->clear_ceil();
    }
};

// =============================================================================
// LIST_NODE - INTERIOR specialization, FIXED_LEN > 0 (no eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct list_node<T, THREADED, Allocator, FIXED_LEN, false> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    static constexpr int MAX_CHILDREN = 7;
    
    small_list<THREADED> chars;
    std::array<atomic_ptr, MAX_CHILDREN> children;
    
    list_node() = default;
    ~list_node() = default;
    
    // Unified interface
    int count() const noexcept { return chars.count(); }
    bool has(unsigned char c) const noexcept { return chars.find(c) >= 0; }
    
    ptr_t get_child(unsigned char c) const noexcept {
        int idx = chars.find(c);
        return idx >= 0 ? children[idx].load() : nullptr;
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        int idx = chars.find(c);
        return idx >= 0 ? &children[idx] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        int idx = chars.add(c);
        children[idx].store(child);
    }
    
    void add_two_children(unsigned char c1, ptr_t child1, unsigned char c2, ptr_t child2) {
        chars.add(c1);
        chars.add(c2);
        children[0].store(child1);
        children[1].store(child2);
    }
    
    void remove_child(unsigned char c) {
        int idx = chars.find(c);
        if (idx < 0) return;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = idx; i < cnt - 1; ++i) {
            children[i].store(children[i + 1].load());
        }
        children[cnt - 1].store(nullptr);
        chars.remove_at(idx);
    }
    
    void move_children_to(list_node* dest) {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void copy_children_to(list_node* dest) const {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
        }
    }
    
    void move_interior_to(list_node* dest) {
        move_children_to(dest);
    }
    
    void copy_interior_to(list_node* dest) const {
        copy_children_to(dest);
    }
    
    void move_interior_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest);
    void copy_interior_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) const;
    
    void update_capacity_flags() noexcept {
        // LIST: floor=3, ceil=7
        int cnt = chars.count();
        if (cnt <= LIST_MIN) this->set_floor(); else this->clear_floor();
        if (cnt >= LIST_MAX) this->set_ceil(); else this->clear_ceil();
    }
};

// =============================================================================
// LIST_NODE - INTERIOR specialization, FIXED_LEN == 0 (has eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct list_node<T, THREADED, Allocator, 0, false> 
    : node_with_skip<T, THREADED, Allocator, 0> {
    using base_t = node_with_skip<T, THREADED, Allocator, 0>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using data_t = typename base_t::data_t;
    using eos_data_t = typename base_t::eos_data_t;
    
    static constexpr int MAX_CHILDREN = 7;
    
    eos_data_t eos;
    small_list<THREADED> chars;
    std::array<atomic_ptr, MAX_CHILDREN> children;
    
    list_node() = default;
    ~list_node() = default;
    
    // Unified interface
    int count() const noexcept { return chars.count(); }
    bool has(unsigned char c) const noexcept { return chars.find(c) >= 0; }
    
    ptr_t get_child(unsigned char c) const noexcept {
        int idx = chars.find(c);
        return idx >= 0 ? children[idx].load() : nullptr;
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        int idx = chars.find(c);
        return idx >= 0 ? &children[idx] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        int idx = chars.add(c);
        children[idx].store(child);
    }
    
    void add_two_children(unsigned char c1, ptr_t child1, unsigned char c2, ptr_t child2) {
        chars.add(c1);
        chars.add(c2);
        children[0].store(child1);
        children[1].store(child2);
    }
    
    void remove_child(unsigned char c) {
        int idx = chars.find(c);
        if (idx < 0) return;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = idx; i < cnt - 1; ++i) {
            children[i].store(children[i + 1].load());
        }
        children[cnt - 1].store(nullptr);
        chars.remove_at(idx);
    }
    
    void move_children_to(list_node* dest) {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void copy_children_to(list_node* dest) const {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
        }
    }
    
    void move_interior_to(list_node* dest) {
        dest->eos = std::move(eos);
        move_children_to(dest);
    }
    
    void copy_interior_to(list_node* dest) const {
        dest->eos.deep_copy_from(eos);
        copy_children_to(dest);
    }
    
    void move_interior_to_full(full_node<T, THREADED, Allocator, 0, false>* dest);
    void copy_interior_to_full(full_node<T, THREADED, Allocator, 0, false>* dest) const;
    
    void update_capacity_flags() noexcept {
        // LIST: floor=3, ceil=7 (based on child count only, EOS doesn't count)
        int cnt = chars.count();
        if (cnt <= LIST_MIN) this->set_floor(); else this->clear_floor();
        if (cnt >= LIST_MAX) this->set_ceil(); else this->clear_ceil();
    }
};

// =============================================================================
// POP_NODE - LEAF specialization (8-32 values using popcount indexing)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct pop_node<T, THREADED, Allocator, FIXED_LEN, true> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    
    static constexpr int MAX_ENTRIES = POP_MAX;  // 32
    
    bitmap256 valid;
    std::array<data_t, MAX_ENTRIES> values;
    
    pop_node() = default;
    ~pop_node() = default;
    
    // Get slot index for character c using popcount
    int slot_for(unsigned char c) const noexcept {
        int word = c >> 6;
        int bit = c & 63;
        int slot = 0;
        for (int w = 0; w < word; ++w) {
            slot += std::popcount(valid.word(w));
        }
        uint64_t mask = (1ULL << bit) - 1;
        slot += std::popcount(valid.word(word) & mask);
        return slot;
    }
    
    int count() const noexcept { return valid.count(); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }
    
    int find(unsigned char c) const noexcept {
        return valid.test(c) ? slot_for(c) : -1;
    }
    
    bool read_value(unsigned char c, T& out) const noexcept {
        if (!valid.test(c)) return false;
        return values[slot_for(c)].try_read(out);
    }
    
    void add_value(unsigned char c, const T& val) {
        int slot = slot_for(c);
        int cnt = count();
        // Shift values up to make room
        for (int i = cnt; i > slot; --i) {
            values[i] = std::move(values[i-1]);
        }
        values[slot].set(val);
        valid.set(c);
    }
    
    void remove_value(unsigned char c) {
        int slot = slot_for(c);
        valid.clear(c);
        int new_count = count();
        // Shift values down
        for (int i = slot; i < new_count; ++i) {
            values[i] = std::move(values[i+1]);
        }
        values[new_count].clear();
    }
    
    void copy_values_to(pop_node* dest) const {
        dest->valid = valid;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->values[i].deep_copy_from(values[i]);
        }
    }
    
    unsigned char first_char() const noexcept { return valid.first(); }
    
    void update_capacity_flags() noexcept {
        // POP: floor=8, ceil=32
        int cnt = count();
        if (cnt <= POP_MIN) this->set_floor(); else this->clear_floor();
        if (cnt >= POP_MAX) this->set_ceil(); else this->clear_ceil();
    }
};

// =============================================================================
// POP_NODE - INTERIOR specialization, FIXED_LEN > 0 (no eos)
// 8-32 children using popcount indexing
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
    requires (FIXED_LEN > 0)
struct pop_node<T, THREADED, Allocator, FIXED_LEN, false> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    static constexpr int MAX_CHILDREN = POP_MAX;  // 32
    
    bitmap256 valid;
    std::array<atomic_ptr, MAX_CHILDREN> children;
    
    pop_node() = default;
    ~pop_node() = default;
    
    // Get slot index for character c using popcount
    int slot_for(unsigned char c) const noexcept {
        int word = c >> 6;
        int bit = c & 63;
        int slot = 0;
        for (int w = 0; w < word; ++w) {
            slot += std::popcount(valid.word(w));
        }
        uint64_t mask = (1ULL << bit) - 1;
        slot += std::popcount(valid.word(word) & mask);
        return slot;
    }
    
    int count() const noexcept { return valid.count(); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }
    
    ptr_t get_child(unsigned char c) const noexcept {
        if (!valid.test(c)) return nullptr;
        return children[slot_for(c)].load();
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        if (!valid.test(c)) return nullptr;
        return &children[slot_for(c)];
    }
    
    void add_child(unsigned char c, ptr_t child) {
        int slot = slot_for(c);
        int cnt = count();
        for (int i = cnt; i > slot; --i) {
            children[i].store(children[i-1].load());
        }
        children[slot].store(child);
        valid.set(c);
    }
    
    void remove_child(unsigned char c) {
        int slot = slot_for(c);
        valid.clear(c);
        int new_count = count();
        for (int i = slot; i < new_count; ++i) {
            children[i].store(children[i+1].load());
        }
        children[new_count].store(nullptr);
    }
    
    void move_children_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) {
        int idx = 0;
        valid.for_each_set([&](unsigned char c) {
            dest->valid.set(c);
            dest->children[c].store(children[idx].load());
            children[idx].store(nullptr);
            ++idx;
        });
    }
    
    void copy_children_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) const {
        int idx = 0;
        valid.for_each_set([&](unsigned char c) {
            dest->valid.set(c);
            dest->children[c].store(children[idx].load());
            ++idx;
        });
    }
    
    void move_children_to(pop_node* dest) {
        dest->valid = valid;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void copy_children_to(pop_node* dest) const {
        dest->valid = valid;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
        }
    }
    
    void copy_interior_to(pop_node* dest) const {
        copy_children_to(dest);
    }
    
    unsigned char first_char() const noexcept { return valid.first(); }
    ptr_t child_at_slot(int slot) const noexcept { return children[slot].load(); }
    
    void update_capacity_flags() noexcept {
        // POP: floor=8, ceil=32
        int cnt = count();
        if (cnt <= POP_MIN) this->set_floor(); else this->clear_floor();
        if (cnt >= POP_MAX) this->set_ceil(); else this->clear_ceil();
    }
};

// =============================================================================
// POP_NODE - INTERIOR specialization, FIXED_LEN == 0 (has eos)
// Entry count = child_count + (has_eos ? 1 : 0), supports 8-32 entries
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct pop_node<T, THREADED, Allocator, 0, false> 
    : node_with_skip<T, THREADED, Allocator, 0> {
    using base_t = node_with_skip<T, THREADED, Allocator, 0>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using eos_data_t = typename base_t::eos_data_t;
    
    static constexpr int MAX_CHILDREN = POP_MAX;  // 32
    
    eos_data_t eos;
    bitmap256 valid;
    std::array<atomic_ptr, MAX_CHILDREN> children;
    
    pop_node() = default;
    ~pop_node() = default;
    
    int slot_for(unsigned char c) const noexcept {
        int word = c >> 6;
        int bit = c & 63;
        int slot = 0;
        for (int w = 0; w < word; ++w) {
            slot += std::popcount(valid.word(w));
        }
        uint64_t mask = (1ULL << bit) - 1;
        slot += std::popcount(valid.word(word) & mask);
        return slot;
    }
    
    int count() const noexcept { return valid.count(); }  // child count
    int entry_count() const noexcept { return count() + (eos.has_data() ? 1 : 0); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }
    
    ptr_t get_child(unsigned char c) const noexcept {
        if (!valid.test(c)) return nullptr;
        return children[slot_for(c)].load();
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        if (!valid.test(c)) return nullptr;
        return &children[slot_for(c)];
    }
    
    void add_child(unsigned char c, ptr_t child) {
        int slot = slot_for(c);
        int cnt = count();
        for (int i = cnt; i > slot; --i) {
            children[i].store(children[i-1].load());
        }
        children[slot].store(child);
        valid.set(c);
    }
    
    void remove_child(unsigned char c) {
        int slot = slot_for(c);
        valid.clear(c);
        int new_count = count();
        for (int i = slot; i < new_count; ++i) {
            children[i].store(children[i+1].load());
        }
        children[new_count].store(nullptr);
    }
    
    void move_children_to_full(full_node<T, THREADED, Allocator, 0, false>* dest) {
        int idx = 0;
        valid.for_each_set([&](unsigned char c) {
            dest->valid.set(c);
            dest->children[c].store(children[idx].load());
            children[idx].store(nullptr);
            ++idx;
        });
    }
    
    void copy_children_to_full(full_node<T, THREADED, Allocator, 0, false>* dest) const {
        int idx = 0;
        valid.for_each_set([&](unsigned char c) {
            dest->valid.set(c);
            dest->children[c].store(children[idx].load());
            ++idx;
        });
    }
    
    void move_interior_to(pop_node* dest) {
        dest->eos = std::move(eos);
        dest->valid = valid;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void copy_interior_to(pop_node* dest) const {
        dest->eos.deep_copy_from(eos);
        dest->valid = valid;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
        }
    }
    
    // Alias for code that doesn't need to move eos
    void move_children_to(pop_node* dest) {
        dest->valid = valid;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    unsigned char first_char() const noexcept { return valid.first(); }
    ptr_t child_at_slot(int slot) const noexcept { return children[slot].load(); }
    
    void update_capacity_flags() noexcept {
        // POP: floor=8, ceil=32 (based on child count only, EOS doesn't count)
        int cnt = count();
        if (cnt <= POP_MIN) this->set_floor(); else this->clear_floor();
        if (cnt >= POP_MAX) this->set_ceil(); else this->clear_ceil();
    }
};

// =============================================================================
// FULL_NODE - LEAF specialization (stores values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct full_node<T, THREADED, Allocator, FIXED_LEN, true> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    
    bitmap256 valid;
    std::array<data_t, 256> values;
    
    full_node() = default;
    ~full_node() = default;
    
    // Unified interface
    int count() const noexcept { return valid.count(); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }  // Non-atomic read
    
    // Caller must verify has(c) first
    bool read_value(unsigned char c, T& out) const noexcept {
        return values[c].try_read(out);
    }
    
    void set_value(unsigned char c, const T& val) {
        values[c].set(val);
        valid.template atomic_set<THREADED>(c);
    }
    
    void add_value(unsigned char c, const T& val) {
        values[c].set(val);
        valid.set(c);
    }
    
    void add_value_atomic(unsigned char c, const T& val) {
        values[c].set(val);
        valid.template atomic_set<THREADED>(c);
    }
    
    void remove_value(unsigned char c) {
        values[c].clear();
        valid.template atomic_clear<THREADED>(c);
    }
    
    void copy_values_to(full_node* dest) const {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->values[c].deep_copy_from(values[c]);
        });
    }
    
    void update_capacity_flags() noexcept {
        // FULL: floor=33, no ceil (256 is max)
        int cnt = count();
        if (cnt <= FULL_MIN) this->set_floor(); else this->clear_floor();
        this->clear_ceil();  // FULL never at ceiling
    }
};

// =============================================================================
// FULL_NODE - INTERIOR specialization, FIXED_LEN > 0 (no eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct full_node<T, THREADED, Allocator, FIXED_LEN, false> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    bitmap256 valid;
    std::array<atomic_ptr, 256> children;
    
    full_node() = default;
    ~full_node() = default;
    
    // Unified interface
    int count() const noexcept { return valid.count(); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }
    
    ptr_t get_child(unsigned char c) const noexcept {
        return children[c].load();
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        return valid.test(c) ? &children[c] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        children[c].store(child);
        valid.set(c);
    }
    
    void add_child_atomic(unsigned char c, ptr_t child) {
        children[c].store(child);
        valid.template atomic_set<THREADED>(c);
    }
    
    void remove_child(unsigned char c) {
        valid.template atomic_clear<THREADED>(c);
        children[c].store(nullptr);
    }
    
    void move_interior_to(full_node* dest) {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
            children[c].store(nullptr);
        });
    }
    
    void copy_interior_to(full_node* dest) const {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
        });
    }
    
    void update_capacity_flags() noexcept {
        // FULL: floor=33, no ceil
        int cnt = count();
        if (cnt <= FULL_MIN) this->set_floor(); else this->clear_floor();
        this->clear_ceil();  // FULL never at ceiling
    }
};

// =============================================================================
// FULL_NODE - INTERIOR specialization, FIXED_LEN == 0 (has eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct full_node<T, THREADED, Allocator, 0, false> 
    : node_with_skip<T, THREADED, Allocator, 0> {
    using base_t = node_with_skip<T, THREADED, Allocator, 0>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using data_t = typename base_t::data_t;
    using eos_data_t = typename base_t::eos_data_t;
    
    eos_data_t eos;
    bitmap256 valid;
    std::array<atomic_ptr, 256> children;
    
    full_node() = default;
    ~full_node() = default;
    
    // Unified interface
    int count() const noexcept { return valid.count(); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }
    
    ptr_t get_child(unsigned char c) const noexcept {
        return children[c].load();
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        return valid.test(c) ? &children[c] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        children[c].store(child);
        valid.set(c);
    }
    
    void add_child_atomic(unsigned char c, ptr_t child) {
        children[c].store(child);
        valid.template atomic_set<THREADED>(c);
    }
    
    void remove_child(unsigned char c) {
        valid.template atomic_clear<THREADED>(c);
        children[c].store(nullptr);
    }
    
    void move_interior_to(full_node* dest) {
        dest->eos = std::move(eos);
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
            children[c].store(nullptr);
        });
    }
    
    void copy_interior_to(full_node* dest) const {
        dest->eos.deep_copy_from(eos);
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
        });
    }
    
    void update_capacity_flags() noexcept {
        // FULL: floor=33, no ceil (based on child count only, EOS doesn't count)
        int cnt = count();
        if (cnt <= FULL_MIN) this->set_floor(); else this->clear_floor();
        this->clear_ceil();  // FULL never at ceiling
    }
};

// =============================================================================
// Out-of-line definitions for move_interior_to_full
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
void list_node<T, THREADED, Allocator, FIXED_LEN, false>::move_interior_to_full(
    full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) {
    int cnt = chars.count();
    [[assume(cnt >= 0 && cnt < 8)]];
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars.char_at(i);
        dest->valid.set(ch);
        dest->children[ch].store(children[i].load());
        children[i].store(nullptr);
    }
}

template <typename T, bool THREADED, typename Allocator>
void list_node<T, THREADED, Allocator, 0, false>::move_interior_to_full(
    full_node<T, THREADED, Allocator, 0, false>* dest) {
    dest->eos = std::move(eos);
    int cnt = chars.count();
    [[assume(cnt >= 0 && cnt < 8)]];
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars.char_at(i);
        dest->valid.set(ch);
        dest->children[ch].store(children[i].load());
        children[i].store(nullptr);
    }
}

// =============================================================================
// Out-of-line definitions for copy_interior_to_full
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
void list_node<T, THREADED, Allocator, FIXED_LEN, false>::copy_interior_to_full(
    full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) const {
    int cnt = chars.count();
    [[assume(cnt >= 0 && cnt < 8)]];
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars.char_at(i);
        dest->valid.set(ch);
        dest->children[ch].store(children[i].load());
    }
}

template <typename T, bool THREADED, typename Allocator>
void list_node<T, THREADED, Allocator, 0, false>::copy_interior_to_full(
    full_node<T, THREADED, Allocator, 0, false>* dest) const {
    dest->eos.deep_copy_from(eos);
    int cnt = chars.count();
    [[assume(cnt >= 0 && cnt < 8)]];
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars.char_at(i);
        dest->valid.set(ch);
        dest->children[ch].store(children[i].load());
    }
}

}  // namespace gteitelbaum

#include "tktrie_node_builder.h"
