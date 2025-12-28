#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <iterator>
#include <mutex>
#include <string>
#include <shared_mutex>
#include <vector>
#include <utility>
#include <functional>

// 256-bit bitmap for sparse child indexing
class alignas(32) pop_tp {
    using bits_t = uint64_t;
    std::array<bits_t, 4> bits{};

public:
    bool find_pop(char c, int* cnt) const {
        uint8_t v = static_cast<uint8_t>(c);
        const int word = v >> 6;
        const int bit = v & 63;
        const bits_t mask = 1ULL << bit;
        if (!(bits[word] & mask)) return false;
        int idx = std::popcount(bits[word] & (mask - 1));
        for (int i = 0; i < word; ++i) idx += std::popcount(bits[i]);
        *cnt = idx;
        return true;
    }

    int set_bit(char c) {
        uint8_t v = static_cast<uint8_t>(c);
        const int word = v >> 6;
        const int bit = v & 63;
        const bits_t mask = 1ULL << bit;
        int idx = std::popcount(bits[word] & (mask - 1));
        for (int i = 0; i < word; ++i) idx += std::popcount(bits[i]);
        bits[word] |= mask;
        return idx;
    }

    int clear_bit(char c) {
        uint8_t v = static_cast<uint8_t>(c);
        const int word = v >> 6;
        const int bit = v & 63;
        const bits_t mask = 1ULL << bit;
        int idx = std::popcount(bits[word] & (mask - 1));
        for (int i = 0; i < word; ++i) idx += std::popcount(bits[i]);
        bits[word] &= ~mask;
        return idx;
    }

    bool has(char c) const {
        uint8_t v = static_cast<uint8_t>(c);
        return bits[v >> 6] & (1ULL << (v & 63));
    }

    int count() const {
        int total = 0;
        for (const auto& w : bits) total += std::popcount(w);
        return total;
    }

    bool empty() const {
        for (const auto& w : bits) if (w != 0) return false;
        return true;
    }

    char char_at_index(int target_idx) const {
        int current_idx = 0;
        for (int word = 0; word < 4; ++word) {
            bits_t w = bits[word];
            while (w != 0) {
                int bit = std::countr_zero(w);
                if (current_idx == target_idx) return static_cast<char>((word << 6) | bit);
                ++current_idx;
                w &= w - 1;
            }
        }
        return '\0';
    }

    char first_char() const {
        for (int word = 0; word < 4; ++word) {
            if (bits[word] != 0) {
                int bit = std::countr_zero(bits[word]);
                return static_cast<char>((word << 6) | bit);
            }
        }
        return '\0';
    }

    char next_char(char c) const {
        uint8_t v = static_cast<uint8_t>(c);
        int word = v >> 6;
        int bit = v & 63;
        bits_t mask = ~((1ULL << (bit + 1)) - 1);
        bits_t remaining = bits[word] & mask;
        if (remaining != 0) return static_cast<char>((word << 6) | std::countr_zero(remaining));
        for (int w = word + 1; w < 4; ++w) {
            if (bits[w] != 0) return static_cast<char>((w << 6) | std::countr_zero(bits[w]));
        }
        return '\0';
    }
};

template <class T> class tktrie;

template <class T>
class node {
    friend class tktrie<T>;

    mutable std::shared_mutex mtx;
    std::atomic<uint64_t> version{0};
    pop_tp pop{};
    node* parent{nullptr};
    std::string skip{};
    std::vector<node*> children{};
    bool has_data{false};
    T data{};
    char parent_edge{'\0'};

public:
    node() = default;
    ~node() { for (auto* c : children) delete c; }
    node(const node&) = delete;
    node& operator=(const node&) = delete;

    bool has_value() const { return has_data; }
    T& get_data() { return data; }
    const T& get_data() const { return data; }
    const std::string& get_skip() const { return skip; }
    int child_count() const { return pop.count(); }
    bool is_leaf() const { return pop.empty(); }
    node* get_parent() const { return parent; }
    char get_parent_edge() const { return parent_edge; }
    uint64_t get_version() const { return version.load(std::memory_order_acquire); }

    node* get_child(char c) const {
        int idx;
        if (pop.find_pop(c, &idx)) return children[idx];
        return nullptr;
    }

    char first_child_char() const { return pop.first_char(); }
    char next_child_char(char c) const { return pop.next_char(c); }

    void read_lock() const { mtx.lock_shared(); }
    void read_unlock() const { mtx.unlock_shared(); }
    void write_lock() { mtx.lock(); }
    void write_unlock() { mtx.unlock(); }
    void bump_version() { version.fetch_add(1, std::memory_order_release); }
};

// Iterator
template <class T>
class tktrie_iterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::pair<std::string, T&>;
    using difference_type = std::ptrdiff_t;
    using reference = value_type;

private:
    node<T>* current{nullptr};
    std::string current_key;

    // Find leftmost node with data, starting from n with given prefix
    // If n has no data and no valid descendants, go up and try next sibling
    void find_next_data(node<T>* n, std::string prefix) {
        while (n) {
            current_key = prefix + n->get_skip();
            
            if (n->has_value()) {
                current = n;
                return;
            }
            
            // Try to descend to first child
            char fc = n->first_child_char();
            if (fc != '\0') {
                prefix = current_key + fc;
                n = n->get_child(fc);
                continue;
            }
            
            // No children and no data - go up and find next sibling
            while (n) {
                node<T>* p = n->get_parent();
                if (!p) {
                    current = nullptr;
                    return;
                }
                
                char edge = n->get_parent_edge();
                // Reconstruct parent's key
                size_t pkl = current_key.length() - n->get_skip().length() - 1;
                std::string pk = current_key.substr(0, pkl);
                
                char next = p->next_child_char(edge);
                if (next != '\0') {
                    // Found next sibling - try to descend into it
                    prefix = pk + next;
                    n = p->get_child(next);
                    break;  // Continue outer while loop
                }
                
                // No more siblings - go up further
                current_key = pk;
                n = p;
            }
        }
        current = nullptr;
    }

    void advance() {
        if (!current) return;
        node<T>* n = current;
        
        // First try to descend to first child
        char fc = n->first_child_char();
        if (fc != '\0') {
            find_next_data(n->get_child(fc), current_key + fc);
            return;
        }
        
        // No children - go up and find next sibling
        while (n) {
            node<T>* p = n->get_parent();
            if (!p) {
                current = nullptr;
                return;
            }
            
            char edge = n->get_parent_edge();
            size_t pkl = current_key.length() - n->get_skip().length() - 1;
            std::string pk = current_key.substr(0, pkl);
            
            char next = p->next_child_char(edge);
            if (next != '\0') {
                find_next_data(p->get_child(next), pk + next);
                return;
            }
            
            current_key = pk;
            n = p;
        }
        current = nullptr;
    }

public:
    tktrie_iterator() = default;
    tktrie_iterator(node<T>* root, bool is_end = false) {
        if (is_end || !root) { current = nullptr; return; }
        find_next_data(root, "");
    }
    reference operator*() const { return {current_key, current->get_data()}; }
    tktrie_iterator& operator++() { advance(); return *this; }
    tktrie_iterator operator++(int) { auto tmp = *this; advance(); return tmp; }
    bool operator==(const tktrie_iterator& o) const { return current == o.current; }
    bool operator!=(const tktrie_iterator& o) const { return current != o.current; }
    node<T>* get_node() const { return current; }
    const std::string& key() const { return current_key; }
};

// Const iterator
template <class T>
class tktrie_const_iterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::pair<std::string, const T&>;
    using difference_type = std::ptrdiff_t;
    using reference = value_type;

private:
    const node<T>* current{nullptr};
    std::string current_key;

    void find_next_data(const node<T>* n, std::string prefix) {
        while (n) {
            current_key = prefix + n->get_skip();
            
            if (n->has_value()) {
                current = n;
                return;
            }
            
            char fc = n->first_child_char();
            if (fc != '\0') {
                prefix = current_key + fc;
                n = n->get_child(fc);
                continue;
            }
            
            while (n) {
                const node<T>* p = n->get_parent();
                if (!p) {
                    current = nullptr;
                    return;
                }
                
                char edge = n->get_parent_edge();
                size_t pkl = current_key.length() - n->get_skip().length() - 1;
                std::string pk = current_key.substr(0, pkl);
                
                char next = p->next_child_char(edge);
                if (next != '\0') {
                    prefix = pk + next;
                    n = p->get_child(next);
                    break;
                }
                
                current_key = pk;
                n = p;
            }
        }
        current = nullptr;
    }

    void advance() {
        if (!current) return;
        const node<T>* n = current;
        
        char fc = n->first_child_char();
        if (fc != '\0') {
            find_next_data(n->get_child(fc), current_key + fc);
            return;
        }
        
        while (n) {
            const node<T>* p = n->get_parent();
            if (!p) {
                current = nullptr;
                return;
            }
            
            char edge = n->get_parent_edge();
            size_t pkl = current_key.length() - n->get_skip().length() - 1;
            std::string pk = current_key.substr(0, pkl);
            
            char next = p->next_child_char(edge);
            if (next != '\0') {
                find_next_data(p->get_child(next), pk + next);
                return;
            }
            
            current_key = pk;
            n = p;
        }
        current = nullptr;
    }

public:
    tktrie_const_iterator() = default;
    tktrie_const_iterator(const node<T>* root, bool is_end = false) {
        if (is_end || !root) { current = nullptr; return; }
        find_next_data(root, "");
    }
    reference operator*() const { return {current_key, current->get_data()}; }
    tktrie_const_iterator& operator++() { advance(); return *this; }
    tktrie_const_iterator operator++(int) { auto tmp = *this; advance(); return tmp; }
    bool operator==(const tktrie_const_iterator& o) const { return current == o.current; }
    bool operator!=(const tktrie_const_iterator& o) const { return current != o.current; }
    const node<T>* get_node() const { return current; }
    const std::string& key() const { return current_key; }
};

template <class T>
class tktrie {
    node<T> head;
    std::atomic<size_t> count{0};
    mutable std::shared_mutex global_mtx;

    static constexpr int MAX_RETRIES = 100;

public:
    using iterator = tktrie_iterator<T>;
    using const_iterator = tktrie_const_iterator<T>;

    tktrie() = default;
    ~tktrie() = default;
    tktrie(const tktrie&) = delete;
    tktrie& operator=(const tktrie&) = delete;
    tktrie(tktrie&&) = delete;
    tktrie& operator=(tktrie&&) = delete;

    // Find with hand-over-hand read locking
    node<T>* find(const std::string& key) {
        std::shared_lock glock(global_mtx);
        node<T>* cur = &head;
        cur->read_lock();
        size_t kpos = 0;

        while (true) {
            const std::string& skip = cur->get_skip();
            
            // Match skip
            if (!skip.empty()) {
                if (key.size() - kpos < skip.size()) { cur->read_unlock(); return nullptr; }
                for (size_t i = 0; i < skip.size(); ++i) {
                    if (key[kpos + i] != skip[i]) { cur->read_unlock(); return nullptr; }
                }
                kpos += skip.size();
            }

            if (kpos == key.size()) {
                bool has = cur->has_value();
                cur->read_unlock();
                return has ? cur : nullptr;
            }

            char c = key[kpos++];
            node<T>* child = cur->get_child(c);
            if (!child) { cur->read_unlock(); return nullptr; }

            child->read_lock();
            cur->read_unlock();
            cur = child;
        }
    }

    const node<T>* find(const std::string& key) const {
        return const_cast<tktrie*>(this)->find(key);
    }

    bool insert(const std::string& key, const T& value) {
        for (int retry = 0; retry < MAX_RETRIES; ++retry) {
            auto [success, was_new] = try_insert(key, value);
            if (success) {
                if (was_new) count.fetch_add(1, std::memory_order_relaxed);
                return was_new;
            }
        }
        return insert_fallback(key, value);
    }

    bool remove(const std::string& key) {
        for (int retry = 0; retry < MAX_RETRIES; ++retry) {
            auto [success, was_removed] = try_remove(key);
            if (success) {
                if (was_removed) count.fetch_sub(1, std::memory_order_relaxed);
                return was_removed;
            }
        }
        return remove_fallback(key);
    }

    bool contains(const std::string& key) const { return find(key) != nullptr; }
    size_t size() const { return count.load(std::memory_order_relaxed); }
    bool empty() const { return size() == 0; }

    void clear() {
        std::unique_lock glock(global_mtx);
        head.write_lock();
        for (auto* c : head.children) delete c;
        head.children.clear();
        head.pop = pop_tp{};
        head.skip.clear();
        head.has_data = false;
        head.data = T{};
        head.bump_version();
        head.write_unlock();
        count.store(0);
    }

    // Remove dead nodes and merge single-child paths
    // Takes exclusive lock on entire trie - call during low-traffic periods
    void compact() {
        std::unique_lock glock(global_mtx);
        compact_node(&head);
    }

private:
    // Recursively compact a subtree, returns true if this node should be deleted
    bool compact_node(node<T>* n) {
        n->write_lock();
        
        // First, recursively compact all children
        std::vector<char> children_to_remove;
        for (int i = 0; i < n->child_count(); ++i) {
            char c = n->pop.char_at_index(i);
            node<T>* child = n->get_child(c);
            if (child) {
                n->write_unlock();  // Unlock while recursing to avoid holding too many locks
                bool should_delete = compact_node(child);
                n->write_lock();
                
                if (should_delete) {
                    children_to_remove.push_back(c);
                }
            }
        }
        
        // Remove dead children
        for (char c : children_to_remove) {
            node<T>* child = n->get_child(c);
            if (child) {
                int idx;
                if (n->pop.find_pop(c, &idx)) {
                    n->pop.clear_bit(c);
                    n->children.erase(n->children.begin() + idx);
                    delete child;
                }
            }
        }
        
        // If this node has no data and no children, it should be deleted
        // (unless it's the head node)
        if (!n->has_data && n->pop.empty() && n->parent != nullptr) {
            n->bump_version();
            n->write_unlock();
            return true;
        }
        
        // If this node has no data and exactly one child, merge with that child
        // (Path compression optimization)
        if (!n->has_data && n->child_count() == 1 && n->parent != nullptr) {
            char c = n->pop.first_char();
            node<T>* child = n->get_child(c);
            
            if (child) {
                child->write_lock();
                
                // Merge: this node's skip + edge char + child's skip
                std::string merged_skip = n->skip + c + child->skip;
                
                // Take over child's properties
                n->skip = merged_skip;
                n->has_data = child->has_data;
                n->data = std::move(child->data);
                n->pop = child->pop;
                
                // Take over child's children
                std::vector<node<T>*> grandchildren = std::move(child->children);
                child->children.clear();  // Prevent destructor from deleting grandchildren
                child->pop = pop_tp{};
                
                n->children = std::move(grandchildren);
                
                // Update grandchildren's parent pointers
                for (auto* gc : n->children) {
                    if (gc) {
                        gc->write_lock();
                        gc->parent = n;
                        gc->write_unlock();
                    }
                }
                
                child->write_unlock();
                delete child;
                
                n->bump_version();
            }
        }
        
        n->bump_version();
        n->write_unlock();
        return false;
    }

public:
    iterator begin() { std::shared_lock g(global_mtx); return iterator(&head); }
    iterator end() { return iterator(nullptr, true); }
    const_iterator begin() const { std::shared_lock g(global_mtx); return const_iterator(&head); }
    const_iterator end() const { return const_iterator(nullptr, true); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend() const { return end(); }

    T& operator[](const std::string& key) {
        node<T>* n = find(key);
        if (n) return n->get_data();
        insert(key, T{});
        return find(key)->get_data();
    }

    std::vector<std::string> keys_with_prefix(const std::string& prefix) const {
        std::shared_lock glock(global_mtx);
        std::vector<std::string> result;
        node<T>* cur = const_cast<node<T>*>(&head);
        size_t kpos = 0;

        while (kpos < prefix.size()) {
            const std::string& skip = cur->get_skip();
            size_t common = 0;
            while (common < skip.size() && kpos + common < prefix.size() &&
                   skip[common] == prefix[kpos + common]) ++common;
            kpos += common;
            if (common < skip.size()) {
                if (kpos == prefix.size()) break;
                return result;
            }
            if (kpos == prefix.size()) break;
            char c = prefix[kpos++];
            node<T>* child = cur->get_child(c);
            if (!child) return result;
            cur = child;
        }

        std::function<void(const node<T>*, const std::string&)> collect =
            [&](const node<T>* n, const std::string& ksf) {
                std::string fk = ksf + n->get_skip();
                if (n->has_value()) result.push_back(fk);
                for (int i = 0; i < n->child_count(); ++i) {
                    char c = n->pop.char_at_index(i);
                    collect(n->children[i], fk + c);
                }
            };
        collect(cur, prefix.substr(0, prefix.size() - (prefix.size() - kpos)));
        return result;
    }

private:
    // Optimistic insert with hand-over-hand locking
    // Returns {completed, was_new}
    std::pair<bool, bool> try_insert(const std::string& key, const T& value) {
        std::shared_lock glock(global_mtx);

        node<T>* cur = &head;
        size_t kpos = 0;

        cur->read_lock();
        uint64_t cur_ver = cur->get_version();

        while (true) {
            const std::string& skip = cur->skip;

            // Find common prefix length
            size_t common = 0;
            while (common < skip.size() && kpos + common < key.size() &&
                   skip[common] == key[kpos + common]) ++common;

            // Case 1: Exact match - set data on this node
            if (kpos + common == key.size() && common == skip.size()) {
                cur->read_unlock();
                cur->write_lock();
                if (cur->get_version() != cur_ver) {
                    cur->write_unlock();
                    return {false, false};  // Retry
                }
                bool was_new = !cur->has_data;
                cur->has_data = true;
                cur->data = value;
                cur->bump_version();
                cur->write_unlock();
                return {true, was_new};
            }

            // Case 2: Key is prefix of skip - need to split node
            if (kpos + common == key.size()) {
                cur->read_unlock();
                cur->write_lock();
                if (cur->get_version() != cur_ver) {
                    cur->write_unlock();
                    return {false, false};
                }
                
                auto* child = new node<T>();
                child->skip = skip.substr(common + 1);
                child->has_data = cur->has_data;
                child->data = std::move(cur->data);
                child->children = std::move(cur->children);
                child->pop = cur->pop;
                child->parent = cur;
                child->parent_edge = skip[common];
                for (auto* gc : child->children) if (gc) gc->parent = child;

                cur->skip = skip.substr(0, common);
                cur->has_data = true;
                cur->data = value;
                cur->children.clear();
                cur->pop = pop_tp{};
                int idx = cur->pop.set_bit(child->parent_edge);
                cur->children.insert(cur->children.begin() + idx, child);
                cur->bump_version();
                cur->write_unlock();
                return {true, true};
            }

            // Case 3: Skip exhausted, continue to child or add new child
            if (common == skip.size()) {
                kpos += common;
                char c = key[kpos];
                node<T>* child = cur->get_child(c);

                if (child) {
                    // Hand-over-hand: lock child, then unlock parent
                    child->read_lock();
                    cur->read_unlock();
                    cur = child;
                    cur_ver = cur->get_version();
                    kpos++;
                    continue;
                }
                
                // No child exists - add new one
                cur->read_unlock();
                cur->write_lock();
                if (cur->get_version() != cur_ver) {
                    cur->write_unlock();
                    return {false, false};
                }
                // Re-check child doesn't exist (might have been added)
                if (cur->get_child(c)) {
                    cur->write_unlock();
                    return {false, false};
                }
                
                auto* newc = new node<T>();
                newc->skip = key.substr(kpos + 1);
                newc->has_data = true;
                newc->data = value;
                newc->parent = cur;
                newc->parent_edge = c;
                int idx = cur->pop.set_bit(c);
                cur->children.insert(cur->children.begin() + idx, newc);
                cur->bump_version();
                cur->write_unlock();
                return {true, true};
            }

            // Case 4: Divergence in middle of skip - need to split
            cur->read_unlock();
            cur->write_lock();
            if (cur->get_version() != cur_ver) {
                cur->write_unlock();
                return {false, false};
            }

            auto* old_child = new node<T>();
            old_child->skip = skip.substr(common + 1);
            old_child->has_data = cur->has_data;
            old_child->data = std::move(cur->data);
            old_child->children = std::move(cur->children);
            old_child->pop = cur->pop;
            old_child->parent = cur;
            old_child->parent_edge = skip[common];
            for (auto* gc : old_child->children) if (gc) gc->parent = old_child;

            auto* new_child = new node<T>();
            new_child->skip = key.substr(kpos + common + 1);
            new_child->has_data = true;
            new_child->data = value;
            new_child->parent = cur;
            new_child->parent_edge = key[kpos + common];

            cur->skip = skip.substr(0, common);
            cur->has_data = false;
            cur->data = T{};
            cur->children.clear();
            cur->pop = pop_tp{};
            int i1 = cur->pop.set_bit(old_child->parent_edge);
            cur->children.insert(cur->children.begin() + i1, old_child);
            int i2 = cur->pop.set_bit(new_child->parent_edge);
            cur->children.insert(cur->children.begin() + i2, new_child);
            cur->bump_version();
            cur->write_unlock();
            return {true, true};
        }
    }

    // Optimistic remove with hand-over-hand locking
    std::pair<bool, bool> try_remove(const std::string& key) {
        std::shared_lock glock(global_mtx);

        node<T>* cur = &head;
        size_t kpos = 0;

        cur->read_lock();
        uint64_t cur_ver = cur->get_version();

        while (true) {
            const std::string& skip = cur->skip;

            // Check skip matches
            if (!skip.empty()) {
                if (key.size() - kpos < skip.size()) {
                    cur->read_unlock();
                    return {true, false};  // Key not found
                }
                for (size_t i = 0; i < skip.size(); ++i) {
                    if (key[kpos + i] != skip[i]) {
                        cur->read_unlock();
                        return {true, false};
                    }
                }
                kpos += skip.size();
            }

            // Key exhausted - this is the node to remove from
            if (kpos == key.size()) {
                if (!cur->has_data) {
                    cur->read_unlock();
                    return {true, false};
                }
                
                cur->read_unlock();
                cur->write_lock();
                if (cur->get_version() != cur_ver) {
                    cur->write_unlock();
                    return {false, false};
                }
                if (!cur->has_data) {
                    cur->write_unlock();
                    return {true, false};  // Already removed
                }
                
                cur->has_data = false;
                cur->data = T{};
                cur->bump_version();
                cur->write_unlock();
                return {true, true};
            }

            // Descend to child
            char c = key[kpos];
            node<T>* child = cur->get_child(c);
            if (!child) {
                cur->read_unlock();
                return {true, false};
            }

            child->read_lock();
            cur->read_unlock();
            cur = child;
            cur_ver = cur->get_version();
            kpos++;
        }
    }

    // Fallback with global exclusive lock
    bool insert_fallback(const std::string& key, const T& value) {
        std::unique_lock glock(global_mtx);

        node<T>* cur = &head;
        size_t kpos = 0;

        while (true) {
            cur->write_lock();
            const std::string& skip = cur->skip;

            size_t common = 0;
            while (common < skip.size() && kpos + common < key.size() &&
                   skip[common] == key[kpos + common]) ++common;

            if (kpos + common == key.size() && common == skip.size()) {
                bool was_new = !cur->has_data;
                cur->has_data = true;
                cur->data = value;
                cur->bump_version();
                cur->write_unlock();
                if (was_new) count.fetch_add(1);
                return was_new;
            }

            if (kpos + common == key.size()) {
                auto* child = new node<T>();
                child->skip = skip.substr(common + 1);
                child->has_data = cur->has_data;
                child->data = std::move(cur->data);
                child->children = std::move(cur->children);
                child->pop = cur->pop;
                child->parent = cur;
                child->parent_edge = skip[common];
                for (auto* gc : child->children) if (gc) gc->parent = child;

                cur->skip = skip.substr(0, common);
                cur->has_data = true;
                cur->data = value;
                cur->children.clear();
                cur->pop = pop_tp{};
                int idx = cur->pop.set_bit(child->parent_edge);
                cur->children.insert(cur->children.begin() + idx, child);
                cur->bump_version();
                cur->write_unlock();
                count.fetch_add(1);
                return true;
            }

            if (common == skip.size()) {
                kpos += common;
                char c = key[kpos];
                node<T>* child = cur->get_child(c);
                if (child) {
                    cur->write_unlock();
                    cur = child;
                    kpos++;
                    continue;
                }
                auto* newc = new node<T>();
                newc->skip = key.substr(kpos + 1);
                newc->has_data = true;
                newc->data = value;
                newc->parent = cur;
                newc->parent_edge = c;
                int idx = cur->pop.set_bit(c);
                cur->children.insert(cur->children.begin() + idx, newc);
                cur->bump_version();
                cur->write_unlock();
                count.fetch_add(1);
                return true;
            }

            auto* old_child = new node<T>();
            old_child->skip = skip.substr(common + 1);
            old_child->has_data = cur->has_data;
            old_child->data = std::move(cur->data);
            old_child->children = std::move(cur->children);
            old_child->pop = cur->pop;
            old_child->parent = cur;
            old_child->parent_edge = skip[common];
            for (auto* gc : old_child->children) if (gc) gc->parent = old_child;

            auto* new_child = new node<T>();
            new_child->skip = key.substr(kpos + common + 1);
            new_child->has_data = true;
            new_child->data = value;
            new_child->parent = cur;
            new_child->parent_edge = key[kpos + common];

            cur->skip = skip.substr(0, common);
            cur->has_data = false;
            cur->data = T{};
            cur->children.clear();
            cur->pop = pop_tp{};
            int i1 = cur->pop.set_bit(old_child->parent_edge);
            cur->children.insert(cur->children.begin() + i1, old_child);
            int i2 = cur->pop.set_bit(new_child->parent_edge);
            cur->children.insert(cur->children.begin() + i2, new_child);
            cur->bump_version();
            cur->write_unlock();
            count.fetch_add(1);
            return true;
        }
    }

    bool remove_fallback(const std::string& key) {
        std::unique_lock glock(global_mtx);

        node<T>* cur = &head;
        size_t kpos = 0;

        while (true) {
            cur->write_lock();
            const std::string& skip = cur->skip;

            if (!skip.empty()) {
                if (key.size() - kpos < skip.size()) {
                    cur->write_unlock();
                    return false;
                }
                for (size_t i = 0; i < skip.size(); ++i) {
                    if (key[kpos + i] != skip[i]) {
                        cur->write_unlock();
                        return false;
                    }
                }
                kpos += skip.size();
            }

            if (kpos == key.size()) {
                if (!cur->has_data) {
                    cur->write_unlock();
                    return false;
                }
                cur->has_data = false;
                cur->data = T{};
                cur->bump_version();
                cur->write_unlock();
                count.fetch_sub(1);
                return true;
            }

            char c = key[kpos];
            node<T>* child = cur->get_child(c);
            cur->write_unlock();
            if (!child) return false;
            cur = child;
            kpos++;
        }
    }
};

using test_type = std::array<char, 24>;
inline auto tst_find(tktrie<test_type>& N, const std::string& key) { return N.find(key); }
