#pragma once

// This file contains node type definitions (SKIP, BINARY, LIST, POP, FULL)
// It should only be included from tktrie_node.h

namespace gteitelbaum {

// Helper for optional EOS field
struct empty_eos {};

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
// BINARY_NODE - 1-2 entries, unified leaf/interior
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF>
class binary_node : public node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    
public:
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using data_t = typename base_t::data_t;
    using eos_data_t = typename base_t::eos_data_t;
    
    static constexpr int MAX_ENTRIES = 2;
    static constexpr bool HAS_EOS = !IS_LEAF && (FIXED_LEN == 0);
    
private:
    using element_t = std::conditional_t<IS_LEAF, data_t, atomic_ptr>;
    
    [[no_unique_address]] std::conditional_t<HAS_EOS, eos_data_t, empty_eos> eos_;
    unsigned char chars_[2] = {};
    int count_ = 0;
    std::array<element_t, 2> elements_;
    
public:
    binary_node() = default;
    ~binary_node() = default;
    
    // -------------------------------------------------------------------------
    // Common interface
    // -------------------------------------------------------------------------
    int count() const noexcept { return count_; }
    
    bool has(unsigned char c) const noexcept { 
        return (count_ > 0 && chars_[0] == c) || (count_ > 1 && chars_[1] == c); 
    }
    
    int find(unsigned char c) const noexcept {
        if (count_ > 0 && chars_[0] == c) return 0;
        if (count_ > 1 && chars_[1] == c) return 1;
        return -1;
    }
    
    unsigned char first_char() const noexcept { return chars_[0]; }
    
    void update_capacity_flags() noexcept {
        if (count_ <= BINARY_MIN) this->set_floor(); else this->clear_floor();
        if (count_ >= BINARY_MAX) this->set_ceil(); else this->clear_ceil();
    }
    
    // -------------------------------------------------------------------------
    // EOS interface (interior + FIXED_LEN==0 only)
    // -------------------------------------------------------------------------
    int entry_count() const noexcept requires HAS_EOS { 
        return count_ + (eos_.has_data() ? 1 : 0); 
    }
    
    eos_data_t& eos() noexcept requires HAS_EOS { return eos_; }
    const eos_data_t& eos() const noexcept requires HAS_EOS { return eos_; }
    
    // -------------------------------------------------------------------------
    // Leaf interface (values)
    // -------------------------------------------------------------------------
    bool read_value(int idx, T& out) const noexcept requires IS_LEAF {
        return elements_[idx].try_read(out);
    }
    
    void add_entry(unsigned char c, const T& value) requires IS_LEAF {
        chars_[count_] = c;
        elements_[count_].set(value);
        ++count_;
    }
    
    // Alias for unified interface
    void add_value(unsigned char c, const T& value) requires IS_LEAF {
        add_entry(c, value);
    }
    
    void remove_entry(int idx) requires IS_LEAF {
        if (idx == 0 && count_ == 2) {
            chars_[0] = chars_[1];
            elements_[0] = std::move(elements_[1]);
        }
        --count_;
    }
    
    void copy_values_to(binary_node* dest) const requires IS_LEAF {
        dest->count_ = count_;
        for (int i = 0; i < count_; ++i) {
            dest->chars_[i] = chars_[i];
            dest->elements_[i].deep_copy_from(elements_[i]);
        }
    }
    
    // For compatibility with existing code that accesses chars/values directly
    unsigned char char_at(int i) const noexcept { return chars_[i]; }
    data_t& value_at(int i) noexcept requires IS_LEAF { return elements_[i]; }
    const data_t& value_at(int i) const noexcept requires IS_LEAF { return elements_[i]; }
    
    // -------------------------------------------------------------------------
    // Interior interface (children)
    // -------------------------------------------------------------------------
    ptr_t get_child(unsigned char c) const noexcept requires (!IS_LEAF) {
        int idx = find(c);
        return idx >= 0 ? elements_[idx].load() : nullptr;
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept requires (!IS_LEAF) {
        int idx = find(c);
        return idx >= 0 ? &elements_[idx] : nullptr;
    }
    
    ptr_t child_at_slot(int slot) const noexcept requires (!IS_LEAF) { 
        return elements_[slot].load(); 
    }
    
    atomic_ptr* child_slot_at(int slot) noexcept requires (!IS_LEAF) {
        return &elements_[slot];
    }
    
    void add_child(unsigned char c, ptr_t child) requires (!IS_LEAF) {
        chars_[count_] = c;
        elements_[count_].store(child);
        ++count_;
    }
    
    void remove_child(int idx) requires (!IS_LEAF) {
        if (idx == 0 && count_ == 2) {
            chars_[0] = chars_[1];
            elements_[0] = elements_[1];
        }
        elements_[count_ - 1].store(nullptr);
        --count_;
    }
    
    void move_children_to(binary_node* dest) requires (!IS_LEAF) {
        dest->count_ = count_;
        for (int i = 0; i < count_; ++i) {
            dest->chars_[i] = chars_[i];
            dest->elements_[i] = elements_[i];
            elements_[i].store(nullptr);
        }
        count_ = 0;
    }
    
    void copy_children_to(binary_node* dest) const requires (!IS_LEAF) {
        dest->count_ = count_;
        for (int i = 0; i < count_; ++i) {
            dest->chars_[i] = chars_[i];
            dest->elements_[i] = elements_[i];
        }
    }
    
    void move_interior_to(binary_node* dest) requires (!IS_LEAF) {
        if constexpr (HAS_EOS) {
            dest->eos_ = std::move(eos_);
        }
        move_children_to(dest);
    }
    
    void copy_interior_to(binary_node* dest) const requires (!IS_LEAF) {
        if constexpr (HAS_EOS) {
            dest->eos_.deep_copy_from(eos_);
        }
        copy_children_to(dest);
    }
};

// =============================================================================
// LIST_NODE - 3-7 entries, unified leaf/interior
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF>
class list_node : public node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    
public:
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using data_t = typename base_t::data_t;
    using eos_data_t = typename base_t::eos_data_t;
    
    static constexpr int MAX_ENTRIES = 7;
    static constexpr bool HAS_EOS = !IS_LEAF && (FIXED_LEN == 0);
    
private:
    using element_t = std::conditional_t<IS_LEAF, data_t, atomic_ptr>;
    
    [[no_unique_address]] std::conditional_t<HAS_EOS, eos_data_t, empty_eos> eos_;
    small_list<THREADED> chars_;
    std::array<element_t, MAX_ENTRIES> elements_;
    
public:
    list_node() = default;
    ~list_node() = default;
    
    // -------------------------------------------------------------------------
    // Common interface
    // -------------------------------------------------------------------------
    int count() const noexcept { return chars_.count(); }
    bool has(unsigned char c) const noexcept { return chars_.find(c) >= 0; }
    int find(unsigned char c) const noexcept { return chars_.find(c); }
    unsigned char char_at(int i) const noexcept { return chars_.char_at(i); }
    
    // Access to chars for iteration
    const small_list<THREADED>& chars() const noexcept { return chars_; }
    
    void update_capacity_flags() noexcept {
        int cnt = chars_.count();
        if (cnt <= LIST_MIN) this->set_floor(); else this->clear_floor();
        if (cnt >= LIST_MAX) this->set_ceil(); else this->clear_ceil();
    }
    
    // -------------------------------------------------------------------------
    // EOS interface (interior + FIXED_LEN==0 only)
    // -------------------------------------------------------------------------
    int entry_count() const noexcept requires HAS_EOS { 
        return count() + (eos_.has_data() ? 1 : 0); 
    }
    
    eos_data_t& eos() noexcept requires HAS_EOS { return eos_; }
    const eos_data_t& eos() const noexcept requires HAS_EOS { return eos_; }
    
    // -------------------------------------------------------------------------
    // Leaf interface (values)
    // -------------------------------------------------------------------------
    bool read_value(int idx, T& out) const noexcept requires IS_LEAF {
        [[assume(idx >= 0 && idx < 7)]];
        return elements_[idx].try_read(out);
    }
    
    void set_value(unsigned char c, const T& val) requires IS_LEAF {
        int idx = chars_.find(c);
        if (idx >= 0) {
            elements_[idx].set(val);
        } else {
            idx = chars_.add(c);
            elements_[idx].set(val);
        }
    }
    
    int add_value(unsigned char c, const T& val) requires IS_LEAF {
        int idx = chars_.add(c);
        elements_[idx].set(val);
        return idx;
    }
    
    void remove_value(unsigned char c) requires IS_LEAF {
        int idx = chars_.find(c);
        if (idx < 0) return;
        int cnt = chars_.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = idx; i < cnt - 1; ++i) {
            elements_[i] = std::move(elements_[i + 1]);
        }
        elements_[cnt - 1].clear();
        chars_.remove_at(idx);
    }
    
    void copy_values_to(list_node* dest) const requires IS_LEAF {
        dest->chars_ = chars_;
        int cnt = chars_.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->elements_[i].deep_copy_from(elements_[i]);
        }
    }
    
    data_t& value_at(int i) noexcept requires IS_LEAF { return elements_[i]; }
    const data_t& value_at(int i) const noexcept requires IS_LEAF { return elements_[i]; }
    
    // -------------------------------------------------------------------------
    // Interior interface (children)
    // -------------------------------------------------------------------------
    ptr_t get_child(unsigned char c) const noexcept requires (!IS_LEAF) {
        int idx = chars_.find(c);
        return idx >= 0 ? elements_[idx].load() : nullptr;
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept requires (!IS_LEAF) {
        int idx = chars_.find(c);
        return idx >= 0 ? &elements_[idx] : nullptr;
    }
    
    ptr_t child_at_slot(int slot) const noexcept requires (!IS_LEAF) { 
        return elements_[slot].load(); 
    }
    
    atomic_ptr* child_slot_at(int slot) noexcept requires (!IS_LEAF) {
        return &elements_[slot];
    }
    
    void add_child(unsigned char c, ptr_t child) requires (!IS_LEAF) {
        int idx = chars_.add(c);
        elements_[idx].store(child);
    }
    
    void add_two_children(unsigned char c1, ptr_t child1, unsigned char c2, ptr_t child2) 
        requires (!IS_LEAF) {
        chars_.add(c1);
        chars_.add(c2);
        elements_[0].store(child1);
        elements_[1].store(child2);
    }
    
    void remove_child(unsigned char c) requires (!IS_LEAF) {
        int idx = chars_.find(c);
        if (idx < 0) return;
        int cnt = chars_.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = idx; i < cnt - 1; ++i) {
            elements_[i] = elements_[i + 1];
        }
        elements_[cnt - 1].store(nullptr);
        chars_.remove_at(idx);
    }
    
    void move_children_to(list_node* dest) requires (!IS_LEAF) {
        dest->chars_ = chars_;
        int cnt = chars_.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->elements_[i] = elements_[i];
            elements_[i].store(nullptr);
        }
    }
    
    void copy_children_to(list_node* dest) const requires (!IS_LEAF) {
        dest->chars_ = chars_;
        int cnt = chars_.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->elements_[i] = elements_[i];
        }
    }
    
    void move_interior_to(list_node* dest) requires (!IS_LEAF) {
        if constexpr (HAS_EOS) {
            dest->eos_ = std::move(eos_);
        }
        move_children_to(dest);
    }
    
    void copy_interior_to(list_node* dest) const requires (!IS_LEAF) {
        if constexpr (HAS_EOS) {
            dest->eos_.deep_copy_from(eos_);
        }
        copy_children_to(dest);
    }
    
    // Forward declarations for full_node interactions
    void move_interior_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) 
        requires (!IS_LEAF);
    void copy_interior_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) const 
        requires (!IS_LEAF);
};

// =============================================================================
// POP_NODE - 8-32 entries using popcount indexing, unified leaf/interior
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF>
class pop_node : public node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    
public:
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using data_t = typename base_t::data_t;
    using eos_data_t = typename base_t::eos_data_t;
    
    static constexpr int MAX_ENTRIES = POP_MAX;  // 32
    static constexpr bool HAS_EOS = !IS_LEAF && (FIXED_LEN == 0);
    
private:
    using element_t = std::conditional_t<IS_LEAF, data_t, atomic_ptr>;
    
    [[no_unique_address]] std::conditional_t<HAS_EOS, eos_data_t, empty_eos> eos_;
    bitmap256<THREADED> valid_;
    std::array<element_t, MAX_ENTRIES> elements_;
    
public:
    pop_node() = default;
    ~pop_node() = default;
    
    // -------------------------------------------------------------------------
    // Common interface
    // -------------------------------------------------------------------------
    int count() const noexcept { return valid_.count(); }
    bool has(unsigned char c) const noexcept { return valid_.test(c); }
    
    int find(unsigned char c) const noexcept {
        return valid_.test(c) ? valid_.slot_for(c) : -1;
    }
    
    unsigned char first_char() const noexcept { return valid_.first(); }
    
    // Direct slot access (for iteration)
    element_t& element_at_slot(int slot) noexcept { return elements_[slot]; }
    const element_t& element_at_slot(int slot) const noexcept { return elements_[slot]; }
    
    // Access to bitmap for iteration
    const bitmap256<THREADED>& valid() const noexcept { return valid_; }
    
    void update_capacity_flags() noexcept {
        int cnt = count();
        if (cnt <= POP_MIN) this->set_floor(); else this->clear_floor();
        if (cnt >= POP_MAX) this->set_ceil(); else this->clear_ceil();
    }
    
    // -------------------------------------------------------------------------
    // EOS interface (interior + FIXED_LEN==0 only)
    // -------------------------------------------------------------------------
    int entry_count() const noexcept requires HAS_EOS { 
        return count() + (eos_.has_data() ? 1 : 0); 
    }
    
    eos_data_t& eos() noexcept requires HAS_EOS { return eos_; }
    const eos_data_t& eos() const noexcept requires HAS_EOS { return eos_; }
    
    // -------------------------------------------------------------------------
    // Leaf interface (values)
    // -------------------------------------------------------------------------
    bool read_value(unsigned char c, T& out) const noexcept requires IS_LEAF {
        if (!valid_.test(c)) return false;
        return elements_[valid_.slot_for(c)].try_read(out);
    }
    
    void add_value(unsigned char c, const T& val) requires IS_LEAF {
        int slot = valid_.shift_up_for_insert(c, elements_, count());
        elements_[slot].set(val);
        valid_.set(c);
    }
    
    void remove_value(unsigned char c) requires IS_LEAF {
        valid_.shift_down_for_remove(c, elements_, [](auto& v) { v.clear(); });
    }
    
    void copy_values_to(pop_node* dest) const requires IS_LEAF {
        dest->valid_ = valid_;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->elements_[i].deep_copy_from(elements_[i]);
        }
    }
    
    // -------------------------------------------------------------------------
    // Interior interface (children)
    // -------------------------------------------------------------------------
    ptr_t get_child(unsigned char c) const noexcept requires (!IS_LEAF) {
        if (!valid_.test(c)) return nullptr;
        return elements_[valid_.slot_for(c)].load();
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept requires (!IS_LEAF) {
        if (!valid_.test(c)) return nullptr;
        return &elements_[valid_.slot_for(c)];
    }
    
    ptr_t child_at_slot(int slot) const noexcept requires (!IS_LEAF) { 
        return elements_[slot].load(); 
    }
    
    atomic_ptr* child_slot_at(int slot) noexcept requires (!IS_LEAF) {
        return &elements_[slot];
    }
    
    void add_child(unsigned char c, ptr_t child) requires (!IS_LEAF) {
        int slot = valid_.shift_up_for_insert(c, elements_, count());
        elements_[slot].store(child);
        valid_.set(c);
    }
    
    void remove_child(unsigned char c) requires (!IS_LEAF) {
        valid_.shift_down_for_remove(c, elements_, [](auto& p) { p.store(nullptr); });
    }
    
    void move_children_to(pop_node* dest) requires (!IS_LEAF) {
        dest->valid_ = valid_;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->elements_[i] = elements_[i];
            elements_[i].store(nullptr);
        }
    }
    
    void copy_children_to(pop_node* dest) const requires (!IS_LEAF) {
        dest->valid_ = valid_;
        int cnt = count();
        for (int i = 0; i < cnt; ++i) {
            dest->elements_[i] = elements_[i];
        }
    }
    
    void move_interior_to(pop_node* dest) requires (!IS_LEAF) {
        if constexpr (HAS_EOS) {
            dest->eos_ = std::move(eos_);
        }
        move_children_to(dest);
    }
    
    void copy_interior_to(pop_node* dest) const requires (!IS_LEAF) {
        if constexpr (HAS_EOS) {
            dest->eos_.deep_copy_from(eos_);
        }
        copy_children_to(dest);
    }
    
    // For conversion to FULL
    void move_children_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) 
        requires (!IS_LEAF);
    void copy_children_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) const 
        requires (!IS_LEAF);
};

// =============================================================================
// FULL_NODE - 33+ entries using direct indexing, unified leaf/interior
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF>
class full_node : public node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    
public:
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using data_t = typename base_t::data_t;
    using eos_data_t = typename base_t::eos_data_t;
    
    static constexpr bool HAS_EOS = !IS_LEAF && (FIXED_LEN == 0);
    
private:
    using element_t = std::conditional_t<IS_LEAF, data_t, atomic_ptr>;
    
    [[no_unique_address]] std::conditional_t<HAS_EOS, eos_data_t, empty_eos> eos_;
    bitmap256<THREADED> valid_;
    std::array<element_t, 256> elements_;
    
public:
    full_node() = default;
    ~full_node() = default;
    
    // -------------------------------------------------------------------------
    // Common interface
    // -------------------------------------------------------------------------
    int count() const noexcept { return valid_.count(); }
    bool has(unsigned char c) const noexcept { return valid_.test(c); }
    
    // Access to bitmap for iteration
    const bitmap256<THREADED>& valid() const noexcept { return valid_; }
    
    void update_capacity_flags() noexcept {
        int cnt = count();
        if (cnt <= FULL_MIN) this->set_floor(); else this->clear_floor();
        this->clear_ceil();  // FULL never at ceiling
    }
    
    // -------------------------------------------------------------------------
    // EOS interface (interior + FIXED_LEN==0 only)
    // -------------------------------------------------------------------------
    int entry_count() const noexcept requires HAS_EOS { 
        return count() + (eos_.has_data() ? 1 : 0); 
    }
    
    eos_data_t& eos() noexcept requires HAS_EOS { return eos_; }
    const eos_data_t& eos() const noexcept requires HAS_EOS { return eos_; }
    
    // -------------------------------------------------------------------------
    // Leaf interface (values)
    // -------------------------------------------------------------------------
    bool read_value(unsigned char c, T& out) const noexcept requires IS_LEAF {
        return elements_[c].try_read(out);
    }
    
    void set_value(unsigned char c, const T& val) requires IS_LEAF {
        elements_[c].set(val);
        valid_.template atomic_set<THREADED>(c);
    }
    
    void add_value(unsigned char c, const T& val) requires IS_LEAF {
        elements_[c].set(val);
        valid_.set(c);
    }
    
    void add_value_atomic(unsigned char c, const T& val) requires IS_LEAF {
        elements_[c].set(val);
        valid_.template atomic_set<THREADED>(c);
    }
    
    void remove_value(unsigned char c) requires IS_LEAF {
        elements_[c].clear();
        valid_.template atomic_clear<THREADED>(c);
    }
    
    void copy_values_to(full_node* dest) const requires IS_LEAF {
        dest->valid_ = valid_;
        valid_.for_each_set([this, dest](unsigned char c) {
            dest->elements_[c].deep_copy_from(elements_[c]);
        });
    }
    
    // -------------------------------------------------------------------------
    // Interior interface (children)
    // -------------------------------------------------------------------------
    ptr_t get_child(unsigned char c) const noexcept requires (!IS_LEAF) {
        return elements_[c].load();
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept requires (!IS_LEAF) {
        return valid_.test(c) ? &elements_[c] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) requires (!IS_LEAF) {
        elements_[c].store(child);
        valid_.set(c);
    }
    
    void add_child_atomic(unsigned char c, ptr_t child) requires (!IS_LEAF) {
        elements_[c].store(child);
        valid_.template atomic_set<THREADED>(c);
    }
    
    void remove_child(unsigned char c) requires (!IS_LEAF) {
        valid_.template atomic_clear<THREADED>(c);
        elements_[c].store(nullptr);
    }
    
    void move_interior_to(full_node* dest) requires (!IS_LEAF) {
        if constexpr (HAS_EOS) {
            dest->eos_ = std::move(eos_);
        }
        dest->valid_ = valid_;
        valid_.for_each_set([this, dest](unsigned char c) {
            dest->elements_[c] = elements_[c];
            elements_[c].store(nullptr);
        });
    }
    
    void copy_interior_to(full_node* dest) const requires (!IS_LEAF) {
        if constexpr (HAS_EOS) {
            dest->eos_.deep_copy_from(eos_);
        }
        dest->valid_ = valid_;
        valid_.for_each_set([this, dest](unsigned char c) {
            dest->elements_[c] = elements_[c];
        });
    }
};

// =============================================================================
// Out-of-line definitions for list_node -> full_node conversions
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF>
void list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>::move_interior_to_full(
    full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) requires (!IS_LEAF) {
    if constexpr (HAS_EOS) {
        dest->eos() = std::move(eos_);
    }
    int cnt = chars_.count();
    [[assume(cnt >= 0 && cnt < 8)]];
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars_.char_at(i);
        dest->valid().set(ch);
        dest->add_child(ch, elements_[i].load());
        elements_[i].store(nullptr);
    }
}

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF>
void list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>::copy_interior_to_full(
    full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) const requires (!IS_LEAF) {
    if constexpr (HAS_EOS) {
        dest->eos().deep_copy_from(eos_);
    }
    int cnt = chars_.count();
    [[assume(cnt >= 0 && cnt < 8)]];
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars_.char_at(i);
        dest->add_child(ch, elements_[i].load());
    }
}

// =============================================================================
// Out-of-line definitions for pop_node -> full_node conversions
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF>
void pop_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>::move_children_to_full(
    full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) requires (!IS_LEAF) {
    int idx = 0;
    valid_.for_each_set([&](unsigned char c) {
        dest->add_child(c, elements_[idx].load());
        elements_[idx].store(nullptr);
        ++idx;
    });
}

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF>
void pop_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>::copy_children_to_full(
    full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) const requires (!IS_LEAF) {
    int idx = 0;
    valid_.for_each_set([&](unsigned char c) {
        dest->add_child(c, elements_[idx].load());
        ++idx;
    });
}

}  // namespace gteitelbaum

#include "tktrie_node_builder.h"
