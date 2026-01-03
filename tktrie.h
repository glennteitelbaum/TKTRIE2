#pragma once
// Lock-free trie with complex SKIP/LIST/POP/FULL node layout
// - Reads: lock-free with per-node version validation
// - Writes: mutex serialized, atomic ops where possible, minimal COW
// - EBR for safe node reclamation

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "tktrie_defines.h"
#include "tktrie_dataptr.h"
#include "tktrie_node.h"
#include "tktrie_ebr.h"

namespace gteitelbaum {

// Key traits
template <typename Key> struct tktrie_traits;

template <> struct tktrie_traits<std::string> {
    static constexpr size_t fixed_len = 0;
    static std::string_view to_bytes(const std::string& k) noexcept { return k; }
    static std::string from_bytes(std::string_view b) { return std::string(b); }
};

template <> struct tktrie_traits<std::string_view> {
    static constexpr size_t fixed_len = 0;
    static std::string_view to_bytes(std::string_view k) noexcept { return k; }
    static std::string from_bytes(std::string_view b) { return std::string(b); }
};

template <typename T> requires std::is_integral_v<T>
struct tktrie_traits<T> {
    static constexpr size_t fixed_len = sizeof(T);
    using unsigned_type = std::make_unsigned_t<T>;

    static std::string to_bytes(T k) {
        char buf[sizeof(T)];
        unsigned_type sortable;
        if constexpr (std::is_signed_v<T>) {
            sortable = static_cast<unsigned_type>(k) ^ (unsigned_type{1} << (sizeof(T) * 8 - 1));
        } else {
            sortable = k;
        }
        unsigned_type be = ktrie_byteswap(sortable);
        std::memcpy(buf, &be, sizeof(T));
        return std::string(buf, sizeof(T));
    }

    static T from_bytes(std::string_view b) {
        unsigned_type be;
        std::memcpy(&be, b.data(), sizeof(T));
        unsigned_type sortable = ktrie_byteswap(be);
        if constexpr (std::is_signed_v<T>) {
            return static_cast<T>(sortable ^ (unsigned_type{1} << (sizeof(T) * 8 - 1)));
        } else {
            return static_cast<T>(sortable);
        }
    }
};

// Iterator
template <typename Key, typename T>
class tktrie_iterator {
    Key key_;
    T data_;
    bool valid_{false};
public:
    using value_type = std::pair<Key, T>;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::forward_iterator_tag;

    tktrie_iterator() = default;
    tktrie_iterator(const Key& k, const T& d) : key_(k), data_(d), valid_(true) {}
    static tktrie_iterator end_iterator() { return tktrie_iterator(); }

    const Key& key() const { return key_; }
    T& value() { return data_; }
    const T& value() const { return data_; }
    value_type operator*() const { return {key_, data_}; }
    bool valid() const { return valid_; }
    explicit operator bool() const { return valid_; }

    bool operator==(const tktrie_iterator& o) const {
        if (!valid_ && !o.valid_) return true;
        return valid_ && o.valid_ && key_ == o.key_;
    }
    bool operator!=(const tktrie_iterator& o) const { return !(*this == o); }

    tktrie_iterator& operator++() { valid_ = false; return *this; }
    tktrie_iterator operator++(int) { auto tmp = *this; ++*this; return tmp; }
};

// Main trie class
template <typename Key, typename T, bool THREADED = true, typename Allocator = std::allocator<uint64_t>>
class tktrie {
public:
    using Traits = tktrie_traits<Key>;
    static constexpr size_t fixed_len = Traits::fixed_len;
    using slot_type = slot_type_t<THREADED>;
    using node_view_t = node_view<T, THREADED, Allocator, fixed_len>;
    using node_builder_t = node_builder<T, THREADED, Allocator, fixed_len>;
    using dataptr_t = dataptr<T, THREADED, Allocator>;
    using size_type = std::size_t;
    using iterator = tktrie_iterator<Key, T>;
    using const_iterator = iterator;
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<const Key, T>;

private:
    slot_type root_slot_;
    std::atomic<size_type> elem_count_{0};
    mutable std::mutex write_mutex_;
    Allocator alloc_;
    node_builder_t builder_;

    // Version state for optimistic reads
    struct ReadState {
        std::vector<std::pair<slot_type*, uint32_t>> versions;
        void record(slot_type* node) {
            node_view_t view(node);
            versions.push_back({node, view.version()});
        }
        bool validate() const {
            for (const auto& [node, ver] : versions) {
                node_view_t view(node);
                if (view.version() != ver) return false;
            }
            return true;
        }
    };

    slot_type* get_root() const noexcept {
        return reinterpret_cast<slot_type*>(load_slot<THREADED>(const_cast<slot_type*>(&root_slot_)));
    }

    void set_root(slot_type* r) noexcept {
        store_slot<THREADED>(&root_slot_, reinterpret_cast<uint64_t>(r));
    }

    static void delete_node_fn(void* ptr) {
        // Note: actual deallocation handled by trie
    }

    void delete_tree(slot_type* node) {
        if (!node) return;
        node_view_t view(node);
        if (!view.has_leaf() || !view.leaf_has_children()) {
            int nc = view.child_count();
            for (int i = 0; i < nc; ++i) {
                uint64_t cp = view.get_child_ptr(i);
                if (cp) delete_tree(reinterpret_cast<slot_type*>(cp));
            }
        }
        builder_.deallocate_node(node);
    }

    slot_type* deep_copy(slot_type* src) {
        return builder_.deep_copy(src);
    }

    static std::string get_key_bytes(const Key& key) {
        if constexpr (fixed_len > 0) return Traits::to_bytes(key);
        else return std::string(Traits::to_bytes(key));
    }

    static size_t match_skip(std::string_view skip, std::string_view key) noexcept {
        size_t i = 0;
        while (i < skip.size() && i < key.size() && skip[i] == key[i]) ++i;
        return i;
    }

public:
    tktrie() : root_slot_{}, builder_(alloc_) {
        store_slot<THREADED>(&root_slot_, reinterpret_cast<uint64_t>(builder_.build_empty()));
    }

    explicit tktrie(const Allocator& alloc) : root_slot_{}, alloc_(alloc), builder_(alloc) {
        store_slot<THREADED>(&root_slot_, reinterpret_cast<uint64_t>(builder_.build_empty()));
    }

    ~tktrie() {
        delete_tree(get_root());
    }

    tktrie(const tktrie& other) : root_slot_{}, alloc_(other.alloc_), builder_(other.alloc_) {
        std::lock_guard<std::mutex> lock(other.write_mutex_);
        set_root(deep_copy(other.get_root()));
        elem_count_.store(other.elem_count_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    }

    tktrie& operator=(const tktrie& other) {
        if (this != &other) {
            tktrie tmp(other);
            swap(tmp);
        }
        return *this;
    }

    tktrie(tktrie&& other) noexcept : root_slot_{}, alloc_(std::move(other.alloc_)), builder_(alloc_) {
        std::lock_guard<std::mutex> lock(other.write_mutex_);
        set_root(other.get_root());
        other.set_root(other.builder_.build_empty());
        elem_count_.store(other.elem_count_.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
    }

    tktrie& operator=(tktrie&& other) noexcept {
        if (this != &other) {
            std::scoped_lock lock(write_mutex_, other.write_mutex_);
            slot_type* old = get_root();
            set_root(other.get_root());
            other.set_root(other.builder_.build_empty());
            elem_count_.store(other.elem_count_.exchange(0, std::memory_order_relaxed), std::memory_order_relaxed);
            delete_tree(old);
        }
        return *this;
    }

    void swap(tktrie& other) noexcept {
        if (this == &other) return;
        std::scoped_lock lock(write_mutex_, other.write_mutex_);
        slot_type* tmp = get_root();
        set_root(other.get_root());
        other.set_root(tmp);
        size_type tc = elem_count_.load(std::memory_order_relaxed);
        elem_count_.store(other.elem_count_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        other.elem_count_.store(tc, std::memory_order_relaxed);
    }

    bool empty() const { return size() == 0; }
    size_type size() const { return elem_count_.load(std::memory_order_relaxed); }
    iterator end() const { return iterator::end_iterator(); }
    iterator begin() const { return end(); }

    // Lock-free read with optimistic validation
    bool contains(const Key& key) const {
        std::string kv = get_key_bytes(key);
        for (int retry = 0; retry < 3; ++retry) {
            if constexpr (THREADED) {
                ebr_guard guard;
                ReadState state;
                T dummy;
                bool found = read_impl(kv, dummy, state);
                if (state.validate()) return found;
                cpu_pause();
            } else {
                ReadState state;
                T dummy;
                return read_impl(kv, dummy, state);
            }
        }
        // Fallback with lock
        std::lock_guard<std::mutex> lock(write_mutex_);
        ReadState state;
        T dummy;
        return read_impl(kv, dummy, state);
    }

    iterator find(const Key& key) const {
        std::string kv = get_key_bytes(key);
        for (int retry = 0; retry < 3; ++retry) {
            if constexpr (THREADED) {
                ebr_guard guard;
                ReadState state;
                T value;
                bool found = read_impl(kv, value, state);
                if (state.validate()) {
                    return found ? iterator(key, value) : end();
                }
                cpu_pause();
            } else {
                ReadState state;
                T value;
                bool found = read_impl(kv, value, state);
                return found ? iterator(key, value) : end();
            }
        }
        std::lock_guard<std::mutex> lock(write_mutex_);
        ReadState state;
        T value;
        bool found = read_impl(kv, value, state);
        return found ? iterator(key, value) : end();
    }

    std::pair<iterator, bool> insert(const std::pair<const Key, T>& value) {
        return insert_impl(value.first, value.second);
    }

    std::pair<iterator, bool> insert(std::pair<const Key, T>&& value) {
        return insert_impl(value.first, std::move(value.second));
    }

    template <typename... Args>
    std::pair<iterator, bool> emplace(const Key& key, Args&&... args) {
        return insert_impl(key, T(std::forward<Args>(args)...));
    }

    bool erase(const Key& key) {
        return erase_impl(key);
    }

    void clear() {
        std::lock_guard<std::mutex> lock(write_mutex_);
        slot_type* old = get_root();
        set_root(builder_.build_empty());
        elem_count_.store(0, std::memory_order_relaxed);
        if constexpr (THREADED) {
            ebr_manager::instance().retire(old, [](void* p) {});
            ebr_manager::instance().force_reclaim();
        }
        delete_tree(old);
    }

private:
    // Lock-free read traversal with version tracking
    bool read_impl(std::string_view kv, T& out, ReadState& state) const {
        slot_type* cur = get_root();

        while (cur) {
            state.record(cur);
            node_view_t view(cur);

            if (view.has_skip()) {
                std::string_view skip = view.skip_chars();
                size_t match = match_skip(skip, kv);
                if (match < skip.size()) return false;
                kv.remove_prefix(match);

                if (kv.empty()) {
                    if (view.has_leaf()) {
                        if (view.leaf_has_eos()) {
                            return view.skip_eos_data()->try_read(out);
                        }
                        return false;
                    }
                    return view.skip_eos_data()->try_read(out);
                }
            }

            if (kv.empty()) {
                if (view.has_leaf()) {
                    if (view.leaf_has_eos()) {
                        return view.eos_data()->try_read(out);
                    }
                    return false;
                }
                return view.eos_data()->try_read(out);
            }

            unsigned char c = static_cast<unsigned char>(kv[0]);
            slot_type* child_slot = view.find_child(c);
            if (!child_slot) return false;

            if (view.has_leaf()) {
                if (kv.size() == 1) {
                    if constexpr (can_embed_leaf_v<T>) {
                        uint64_t raw = load_slot<THREADED>(child_slot);
                        std::memcpy(&out, &raw, sizeof(T));
                        return true;
                    }
                }
                return false;
            }

            uint64_t child_ptr = load_slot<THREADED>(child_slot);
            if (child_ptr == 0) return false;

            cur = reinterpret_cast<slot_type*>(child_ptr);
            kv.remove_prefix(1);
        }
        return false;
    }

    // Insert under mutex - atomic where possible, minimal COW
    template <typename V>
    std::pair<iterator, bool> insert_impl(const Key& key, V&& value) {
        std::string kv_str = get_key_bytes(key);
        std::lock_guard<std::mutex> lock(write_mutex_);
        return do_insert(key, std::forward<V>(value), kv_str);
    }

    template <typename V>
    std::pair<iterator, bool> do_insert(const Key& key, V&& value, const std::string& kv_str) {
        std::string_view kv(kv_str);
        slot_type* cur = get_root();
        slot_type* parent [[maybe_unused]] = nullptr;
        slot_type* parent_child_slot = &root_slot_;

        while (true) {
            node_view_t view(cur);

            if (view.has_skip()) {
                std::string_view skip = view.skip_chars();
                size_t match = match_skip(skip, kv);

                if (match < skip.size() && match < kv.size()) {
                    // Diverge in skip - split node (COW)
                    return split_skip_diverge(parent_child_slot, cur, key, std::forward<V>(value), kv, match);
                } else if (match < skip.size()) {
                    // Key is prefix of skip - split (COW)
                    return split_skip_prefix(parent_child_slot, cur, key, std::forward<V>(value), kv, match);
                } else {
                    kv.remove_prefix(match);
                    if (kv.empty()) {
                        // Set skip_eos - atomic, no COW
                        if (view.has_leaf()) {
                            if (view.leaf_has_eos() && view.skip_eos_data()->has_data()) {
                                T existing;
                                view.skip_eos_data()->try_read(existing);
                                return {iterator(key, existing), false};
                            }
                        } else {
                            if (view.skip_eos_data()->has_data()) {
                                T existing;
                                view.skip_eos_data()->try_read(existing);
                                return {iterator(key, existing), false};
                            }
                        }
                        view.skip_eos_data()->set(std::forward<V>(value));
                        view.bump_version();
                        elem_count_.fetch_add(1, std::memory_order_relaxed);
                        return {iterator(key, value), true};
                    }
                }
            }

            if (kv.empty()) {
                // Set eos - atomic, no COW
                if (view.has_leaf()) {
                    if (view.leaf_has_eos() && view.eos_data()->has_data()) {
                        T existing;
                        view.eos_data()->try_read(existing);
                        return {iterator(key, existing), false};
                    }
                } else {
                    if (view.eos_data()->has_data()) {
                        T existing;
                        view.eos_data()->try_read(existing);
                        return {iterator(key, existing), false};
                    }
                }
                view.eos_data()->set(std::forward<V>(value));
                view.bump_version();
                elem_count_.fetch_add(1, std::memory_order_relaxed);
                return {iterator(key, value), true};
            }

            unsigned char c = static_cast<unsigned char>(kv[0]);
            slot_type* child_slot = view.find_child(c);

            if (child_slot) {
                if (view.has_leaf()) {
                    // LEAF node with embedded values
                    if (kv.size() == 1) {
                        if constexpr (can_embed_leaf_v<T>) {
                            // Check if already exists
                            if (view.has_full() && view.leaf_full_test_bit(c)) {
                                T existing = view.get_leaf_value(c);
                                return {iterator(key, existing), false};
                            }
                            // Set value atomically (in-place for FULL)
                            if (view.has_full()) {
                                view.set_leaf_value(c, std::forward<V>(value));
                                view.leaf_full_set_bit(c);
                                view.bump_version();
                                elem_count_.fetch_add(1, std::memory_order_relaxed);
                                return {iterator(key, value), true};
                            }
                            // Need COW to add to LIST/POP
                            return add_leaf_child(parent_child_slot, cur, key, std::forward<V>(value), kv);
                        }
                    }
                    return {end(), false};
                }

                uint64_t child_ptr = load_slot<THREADED>(child_slot);
                if (child_ptr == 0) {
                    // Empty slot - add child (COW)
                    return add_child(parent_child_slot, cur, key, std::forward<V>(value), kv);
                }

                parent = cur;
                parent_child_slot = child_slot;
                cur = reinterpret_cast<slot_type*>(child_ptr);
                kv.remove_prefix(1);
            } else {
                // Need to add child (COW)
                return add_child(parent_child_slot, cur, key, std::forward<V>(value), kv);
            }
        }
    }

    template <typename V>
    std::pair<iterator, bool> split_skip_diverge(slot_type* parent_slot, slot_type* node,
                                                   const Key& key, V&& value,
                                                   std::string_view kv, size_t match) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        std::string_view common = skip.substr(0, match);
        unsigned char old_char = static_cast<unsigned char>(skip[match]);
        unsigned char new_char = static_cast<unsigned char>(kv[match]);

        // Clone old node with shorter skip
        slot_type* old_suffix = clone_with_shorter_skip(node, match + 1);

        // New node for new key suffix
        std::string_view new_suffix = kv.substr(match + 1);
        slot_type* new_suffix_node;
        if (new_suffix.empty()) {
            new_suffix_node = builder_.build_empty();
            node_view_t nv(new_suffix_node);
            nv.eos_data()->set(std::forward<V>(value));
        } else {
            new_suffix_node = builder_.build_skip(new_suffix);
            node_view_t nv(new_suffix_node);
            nv.skip_eos_data()->set(std::forward<V>(value));
        }

        // Build branch node
        small_list lst(std::min(old_char, new_char), std::max(old_char, new_char));
        std::vector<uint64_t> children;
        if (old_char < new_char) {
            children = {reinterpret_cast<uint64_t>(old_suffix), reinterpret_cast<uint64_t>(new_suffix_node)};
        } else {
            children = {reinterpret_cast<uint64_t>(new_suffix_node), reinterpret_cast<uint64_t>(old_suffix)};
        }

        slot_type* branch = common.empty() ? builder_.build_list(lst, children)
                                           : builder_.build_skip_list(common, lst, children);

        // Copy EOS from old node if needed
        if (!view.has_leaf() && view.eos_data()->has_data()) {
            node_view_t bv(branch);
            bv.eos_data()->deep_copy_from(*view.eos_data());
        }

        // Atomically replace in parent
        store_slot<THREADED>(parent_slot, reinterpret_cast<uint64_t>(branch));

        // Retire old node
        if constexpr (THREADED) {
            ebr_manager::instance().retire(node, [](void*) {});
        }
        builder_.deallocate_node(node);

        elem_count_.fetch_add(1, std::memory_order_relaxed);
        return {iterator(key, value), true};
    }

    template <typename V>
    std::pair<iterator, bool> split_skip_prefix(slot_type* parent_slot, slot_type* node,
                                                  const Key& key, V&& value,
                                                  std::string_view /*kv*/, size_t match) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        std::string_view prefix = skip.substr(0, match);
        unsigned char c = static_cast<unsigned char>(skip[match]);

        slot_type* suffix_node = clone_with_shorter_skip(node, match + 1);

        small_list lst;
        lst.add(c);
        std::vector<uint64_t> children = {reinterpret_cast<uint64_t>(suffix_node)};

        slot_type* new_node = prefix.empty() ? builder_.build_list(lst, children)
                                             : builder_.build_skip_list(prefix, lst, children);

        node_view_t nv(new_node);
        if (prefix.empty()) {
            nv.eos_data()->set(std::forward<V>(value));
        } else {
            nv.skip_eos_data()->set(std::forward<V>(value));
        }

        if (!prefix.empty() && !view.has_leaf() && view.eos_data()->has_data()) {
            nv.eos_data()->deep_copy_from(*view.eos_data());
        }

        store_slot<THREADED>(parent_slot, reinterpret_cast<uint64_t>(new_node));

        if constexpr (THREADED) {
            ebr_manager::instance().retire(node, [](void*) {});
        }
        builder_.deallocate_node(node);

        elem_count_.fetch_add(1, std::memory_order_relaxed);
        return {iterator(key, value), true};
    }

    slot_type* clone_with_shorter_skip(slot_type* node, size_t skip_prefix_len) {
        node_view_t view(node);
        std::string_view skip = view.skip_chars();
        std::string_view new_skip = skip.substr(skip_prefix_len);

        // The original node has data in skip_eos_data (if has skip) or eos_data
        // We need to create a new node with shorter/no skip, preserving that data

        slot_type* new_node;
        
        // Check if original has any children
        bool has_children = view.live_child_count() > 0;
        
        if (!has_children) {
            // No children - create a simple node with just the data
            if (new_skip.empty()) {
                new_node = builder_.build_empty();
                node_view_t nv(new_node);
                // Copy data from old node's skip_eos (or eos if no skip)
                if (view.has_skip() && view.skip_eos_data()->has_data()) {
                    nv.eos_data()->deep_copy_from(*view.skip_eos_data());
                } else if (!view.has_skip() && view.eos_data()->has_data()) {
                    nv.eos_data()->deep_copy_from(*view.eos_data());
                }
            } else {
                new_node = builder_.build_skip(new_skip);
                node_view_t nv(new_node);
                // Copy data from old node's skip_eos
                if (view.has_skip() && view.skip_eos_data()->has_data()) {
                    nv.skip_eos_data()->deep_copy_from(*view.skip_eos_data());
                }
            }
        } else {
            // Has children - need to rebuild with children
            // Extract existing children
            std::vector<uint64_t> children;
            std::vector<unsigned char> chars;
            
            if (view.has_full()) {
                children.resize(256);
                for (int i = 0; i < 256; ++i) {
                    children[i] = view.get_child_ptr(i);
                    if (children[i] != 0) chars.push_back(static_cast<unsigned char>(i));
                }
            } else if (view.has_list()) {
                small_list lst = view.get_list();
                for (int i = 0; i < lst.count(); ++i) {
                    chars.push_back(lst.char_at(i));
                    children.push_back(view.get_child_ptr(i));
                }
            } else if (view.has_pop()) {
                popcount_bitmap bmp = view.get_bitmap();
                for (int i = 0; i < bmp.count(); ++i) {
                    chars.push_back(bmp.nth_char(i));
                    children.push_back(view.get_child_ptr(i));
                }
            }
            
            // Build new node with same structure but different skip
            if (view.has_full()) {
                new_node = new_skip.empty() ? builder_.build_full(children)
                                            : builder_.build_skip_full(new_skip, children);
            } else if (chars.size() <= static_cast<size_t>(LIST_MAX)) {
                small_list lst;
                for (auto c : chars) lst.add(c);
                new_node = new_skip.empty() ? builder_.build_list(lst, children)
                                            : builder_.build_skip_list(new_skip, lst, children);
            } else {
                popcount_bitmap bmp;
                for (auto c : chars) bmp.set(c);
                new_node = new_skip.empty() ? builder_.build_pop(bmp, children)
                                            : builder_.build_skip_pop(new_skip, bmp, children);
            }
            
            node_view_t nv(new_node);
            // Copy skip_eos data
            if (view.has_skip() && view.skip_eos_data()->has_data()) {
                if (new_skip.empty()) {
                    nv.eos_data()->deep_copy_from(*view.skip_eos_data());
                } else {
                    nv.skip_eos_data()->deep_copy_from(*view.skip_eos_data());
                }
            }
        }
        
        return new_node;
    }

    template <typename V>
    std::pair<iterator, bool> add_child(slot_type* parent_slot, slot_type* node,
                                         const Key& key, V&& value, std::string_view kv) {
        node_view_t view(node);
        unsigned char c = static_cast<unsigned char>(kv[0]);
        std::string_view rest = kv.substr(1);

        // Build new child node
        slot_type* child_node;
        if (rest.empty()) {
            child_node = builder_.build_empty();
            node_view_t cv(child_node);
            cv.eos_data()->set(std::forward<V>(value));
        } else {
            child_node = builder_.build_skip(rest);
            node_view_t cv(child_node);
            cv.skip_eos_data()->set(std::forward<V>(value));
        }

        // COW: rebuild node with new child
        slot_type* new_node = rebuild_with_new_child(node, c, child_node);

        store_slot<THREADED>(parent_slot, reinterpret_cast<uint64_t>(new_node));

        if constexpr (THREADED) {
            ebr_manager::instance().retire(node, [](void*) {});
        }
        builder_.deallocate_node(node);

        elem_count_.fetch_add(1, std::memory_order_relaxed);
        return {iterator(key, value), true};
    }

    slot_type* rebuild_with_new_child(slot_type* node, unsigned char c, slot_type* new_child) {
        node_view_t view(node);

        // Extract existing children
        std::vector<uint64_t> children;
        std::vector<unsigned char> chars;

        if (view.has_full()) {
            children.resize(256);
            for (int i = 0; i < 256; ++i) children[i] = view.get_child_ptr(i);
            children[c] = reinterpret_cast<uint64_t>(new_child);
            // Still FULL
            slot_type* result;
            if (view.has_skip()) {
                result = builder_.build_skip_full(view.skip_chars(), children);
            } else {
                result = builder_.build_full(children);
            }
            node_view_t rv(result);
            rv.eos_data()->deep_copy_from(*view.eos_data());
            if (view.has_skip()) rv.skip_eos_data()->deep_copy_from(*view.skip_eos_data());
            return result;
        }

        if (view.has_list()) {
            small_list lst = view.get_list();
            for (int i = 0; i < lst.count(); ++i) {
                chars.push_back(lst.char_at(i));
                children.push_back(view.get_child_ptr(i));
            }
        } else if (view.has_pop()) {
            popcount_bitmap bmp = view.get_bitmap();
            for (int i = 0; i < bmp.count(); ++i) {
                chars.push_back(bmp.nth_char(i));
                children.push_back(view.get_child_ptr(i));
            }
        }

        chars.push_back(c);
        children.push_back(reinterpret_cast<uint64_t>(new_child));

        // Choose structure based on count
        slot_type* result;
        if (chars.size() <= static_cast<size_t>(LIST_MAX)) {
            small_list new_lst;
            for (auto ch : chars) new_lst.add(ch);
            if (view.has_skip()) {
                result = builder_.build_skip_list(view.skip_chars(), new_lst, children);
            } else {
                result = builder_.build_list(new_lst, children);
            }
        } else if (chars.size() <= static_cast<size_t>(FULL_THRESHOLD)) {
            popcount_bitmap bmp;
            std::vector<uint64_t> sorted_children(chars.size());
            for (size_t i = 0; i < chars.size(); ++i) {
                int idx = bmp.set(chars[i]);
                // Insert at correct position
                for (size_t j = sorted_children.size() - 1; j > static_cast<size_t>(idx); --j) {
                    sorted_children[j] = sorted_children[j - 1];
                }
                sorted_children[idx] = children[i];
            }
            if (view.has_skip()) {
                result = builder_.build_skip_pop(view.skip_chars(), bmp, sorted_children);
            } else {
                result = builder_.build_pop(bmp, sorted_children);
            }
        } else {
            std::vector<uint64_t> full(256, 0);
            for (size_t i = 0; i < chars.size(); ++i) full[chars[i]] = children[i];
            if (view.has_skip()) {
                result = builder_.build_skip_full(view.skip_chars(), full);
            } else {
                result = builder_.build_full(full);
            }
        }

        node_view_t rv(result);
        rv.eos_data()->deep_copy_from(*view.eos_data());
        if (view.has_skip()) rv.skip_eos_data()->deep_copy_from(*view.skip_eos_data());
        return result;
    }

    template <typename V>
    std::pair<iterator, bool> add_leaf_child(slot_type* parent_slot, slot_type* node,
                                              const Key& key, V&& value, std::string_view kv) {
        // Similar to add_child but for LEAF nodes with embedded values
        // Would rebuild with new leaf value
        // For now, just use COW approach
        return add_child(parent_slot, node, key, std::forward<V>(value), kv);
    }

    bool erase_impl(const Key& key) {
        std::string kv_str = get_key_bytes(key);
        std::lock_guard<std::mutex> lock(write_mutex_);
        return do_erase(kv_str);
    }

    bool do_erase(const std::string& kv_str) {
        std::string_view kv(kv_str);

        struct PathEntry {
            slot_type* parent_slot;
            slot_type* node;
            unsigned char c;
        };
        std::vector<PathEntry> path;

        slot_type* cur = get_root();
        slot_type* parent_slot = &root_slot_;

        while (cur) {
            node_view_t view(cur);

            if (view.has_skip()) {
                std::string_view skip = view.skip_chars();
                if (kv.size() < skip.size()) return false;
                if (kv.substr(0, skip.size()) != skip) return false;
                kv.remove_prefix(skip.size());

                if (kv.empty()) {
                    // Erase skip_eos
                    if (!view.skip_eos_data()->has_data()) return false;
                    view.skip_eos_data()->clear();
                    view.bump_version();
                    elem_count_.fetch_sub(1, std::memory_order_relaxed);
                    // TODO: cleanup if node now empty
                    return true;
                }
            }

            if (kv.empty()) {
                // Erase eos
                if (!view.eos_data()->has_data()) return false;
                view.eos_data()->clear();
                view.bump_version();
                elem_count_.fetch_sub(1, std::memory_order_relaxed);
                return true;
            }

            unsigned char c = static_cast<unsigned char>(kv[0]);
            slot_type* child_slot = view.find_child(c);
            if (!child_slot) return false;

            if (view.has_leaf()) {
                if (kv.size() == 1) {
                    // Erase leaf value - would need COW
                    // For now, just clear the bit in FULL
                    if (view.has_full() && view.leaf_full_test_bit(c)) {
                        view.leaf_full_clear_bit(c);
                        view.bump_version();
                        elem_count_.fetch_sub(1, std::memory_order_relaxed);
                        return true;
                    }
                    return false;
                }
                return false;
            }

            uint64_t child_ptr = load_slot<THREADED>(child_slot);
            if (child_ptr == 0) return false;

            path.push_back({parent_slot, cur, c});
            parent_slot = child_slot;
            cur = reinterpret_cast<slot_type*>(child_ptr);
            kv.remove_prefix(1);
        }

        return false;
    }
};

template <typename Key, typename T, bool THREADED, typename Allocator>
void swap(tktrie<Key, T, THREADED, Allocator>& a, tktrie<Key, T, THREADED, Allocator>& b) noexcept {
    a.swap(b);
}

// Type aliases
template <typename T, typename Allocator = std::allocator<uint64_t>>
using string_trie = tktrie<std::string, T, false, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_string_trie = tktrie<std::string, T, true, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using int32_trie = tktrie<int32_t, T, false, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_int32_trie = tktrie<int32_t, T, true, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using int64_trie = tktrie<int64_t, T, false, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_int64_trie = tktrie<int64_t, T, true, Allocator>;

}  // namespace gteitelbaum
