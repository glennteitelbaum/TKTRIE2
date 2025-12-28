#pragma once

#include <array>
#include <cstdint>
#include <iterator>
#include <mutex>
#include <string>
#include <shared_mutex>
#include <vector>
#include <stack>
#include <utility>
#include <functional>

// 256-bit bitmap for sparse child indexing
class alignas(32) pop_tp {
    using bits_t = uint64_t;
    std::array<bits_t, 4> bits{};

public:
    // Check if character exists and return its compressed index
    bool find_pop(char c, int* cnt) const {
        uint8_t v = static_cast<uint8_t>(c);
        const int word = v >> 6;
        const int bit = v & 63;
        const bits_t mask = 1ULL << bit;

        if (!(bits[word] & mask)) {
            return false;
        }

        int idx = std::popcount(bits[word] & (mask - 1));
        for (int i = 0; i < word; ++i) {
            idx += std::popcount(bits[i]);
        }

        *cnt = idx;
        return true;
    }

    // Set a bit and return the index where child should be inserted
    int set_bit(char c) {
        uint8_t v = static_cast<uint8_t>(c);
        const int word = v >> 6;
        const int bit = v & 63;
        const bits_t mask = 1ULL << bit;

        int idx = std::popcount(bits[word] & (mask - 1));
        for (int i = 0; i < word; ++i) {
            idx += std::popcount(bits[i]);
        }

        bits[word] |= mask;
        return idx;
    }

    // Clear a bit and return the index where child was
    int clear_bit(char c) {
        uint8_t v = static_cast<uint8_t>(c);
        const int word = v >> 6;
        const int bit = v & 63;
        const bits_t mask = 1ULL << bit;

        int idx = std::popcount(bits[word] & (mask - 1));
        for (int i = 0; i < word; ++i) {
            idx += std::popcount(bits[i]);
        }

        bits[word] &= ~mask;
        return idx;
    }

    bool has(char c) const {
        uint8_t v = static_cast<uint8_t>(c);
        const int word = v >> 6;
        const int bit = v & 63;
        return bits[word] & (1ULL << bit);
    }

    int count() const {
        int total = 0;
        for (const auto& w : bits) {
            total += std::popcount(w);
        }
        return total;
    }

    bool empty() const {
        for (const auto& w : bits) {
            if (w != 0) return false;
        }
        return true;
    }

    // Get the character at a given index (inverse of find_pop)
    char char_at_index(int target_idx) const {
        int current_idx = 0;
        for (int word = 0; word < 4; ++word) {
            bits_t w = bits[word];
            while (w != 0) {
                int bit = std::countr_zero(w);
                if (current_idx == target_idx) {
                    return static_cast<char>((word << 6) | bit);
                }
                ++current_idx;
                w &= w - 1;
            }
        }
        return '\0';
    }

    // Get first set character, or '\0' if empty
    char first_char() const {
        for (int word = 0; word < 4; ++word) {
            if (bits[word] != 0) {
                int bit = std::countr_zero(bits[word]);
                return static_cast<char>((word << 6) | bit);
            }
        }
        return '\0';
    }

    // Get next character after c, or '\0' if none
    char next_char(char c) const {
        uint8_t v = static_cast<uint8_t>(c);
        int word = v >> 6;
        int bit = v & 63;

        // Check remaining bits in current word (after current bit)
        bits_t mask = ~((1ULL << (bit + 1)) - 1);  // Bits above current
        bits_t remaining = bits[word] & mask;
        if (remaining != 0) {
            int next_bit = std::countr_zero(remaining);
            return static_cast<char>((word << 6) | next_bit);
        }

        // Check subsequent words
        for (int w = word + 1; w < 4; ++w) {
            if (bits[w] != 0) {
                int next_bit = std::countr_zero(bits[w]);
                return static_cast<char>((w << 6) | next_bit);
            }
        }
        return '\0';
    }
};

// Key wrapper for traversal
class key_tp {
    const std::string& key;
    size_t offset;

public:
    explicit key_tp(const std::string& orig) : key(orig), offset(0) {}

    bool match(const std::string& skip) const {
        if (offset + skip.size() > key.size()) {
            return false;
        }
        return skip == key.substr(offset, skip.size());
    }

    bool is_empty() const { return offset >= key.size(); }

    size_t size() const {
        return (offset < key.size()) ? key.size() - offset : 0;
    }

    void eat(size_t sz) { offset += sz; }

    char cur() {
        if (offset >= key.size()) return '\0';
        return key[offset++];
    }

    char peek() const {
        if (offset >= key.size()) return '\0';
        return key[offset];
    }

    std::string remaining() const {
        if (offset >= key.size()) return "";
        return key.substr(offset);
    }

    void reset() { offset = 0; }
};

template <class T>
class tktrie;

template <class T>
class node {
    friend class tktrie<T>;

    mutable std::shared_mutex mtx;
    pop_tp pop{};
    node* parent{nullptr};
    std::string skip{};
    std::vector<node*> children{};
    bool has_data{false};
    T data{};
    char parent_edge{'\0'};

public:
    node() = default;

    ~node() {
        for (auto* child : children) {
            delete child;
        }
    }

    node(const node&) = delete;
    node& operator=(const node&) = delete;

    // Accessors
    bool has_value() const { return has_data; }
    T& get_data() { return data; }
    const T& get_data() const { return data; }
    const std::string& get_skip() const { return skip; }
    int child_count() const { return pop.count(); }
    bool is_leaf() const { return pop.empty(); }
    node* get_parent() const { return parent; }
    char get_parent_edge() const { return parent_edge; }

    // Get child by character
    node* get_child(char c) const {
        int idx;
        if (pop.find_pop(c, &idx)) {
            return children[idx];
        }
        return nullptr;
    }

    // Get first child
    node* first_child() const {
        if (children.empty()) return nullptr;
        return children[0];
    }

    // Get first child character
    char first_child_char() const {
        return pop.first_char();
    }

    // Get next sibling character after c
    char next_child_char(char c) const {
        return pop.next_char(c);
    }

private:
    // Find traversal - returns next node or nullptr
    node* find_internal(key_tp& key) {
        std::shared_lock lock(mtx);

        if (!skip.empty()) {
            if (!key.match(skip)) {
                return nullptr;
            }
            key.eat(skip.size());
        }

        if (key.is_empty()) {
            return has_data ? this : nullptr;
        }

        int offset = 0;
        char c = key.cur();
        if (!pop.find_pop(c, &offset)) {
            return nullptr;
        }

        return children[offset];
    }

    // Insert - returns {node_ptr, was_new_entry}
    std::pair<node*, bool> insert_internal(key_tp& key, const T& value, 
                                            node* parent_node = nullptr, 
                                            char edge = '\0') {
        std::unique_lock lock(mtx);

        if (parent_node) {
            parent = parent_node;
            parent_edge = edge;
        }

        // Handle path compression
        if (!skip.empty()) {
            size_t match_len = 0;
            while (match_len < skip.size() && match_len < key.size() &&
                   skip[match_len] == key.peek()) {
                ++match_len;
                key.eat(1);
            }

            if (match_len < skip.size()) {
                // Split this node
                auto* split = new node();
                split->skip = skip.substr(match_len + 1);
                split->has_data = has_data;
                split->data = std::move(data);
                split->children = std::move(children);
                split->pop = pop;
                split->parent = this;
                split->parent_edge = skip[match_len];

                // Update grandchildren's parent
                for (auto* child : split->children) {
                    if (child) {
                        std::unique_lock child_lock(child->mtx);
                        child->parent = split;
                    }
                }

                // Reset this node
                has_data = false;
                data = T{};
                children.clear();
                pop = pop_tp{};

                char split_char = skip[match_len];
                int idx = pop.set_bit(split_char);
                children.insert(children.begin() + idx, split);

                skip = skip.substr(0, match_len);
            }
        }

        // Key exhausted - store data here
        if (key.is_empty()) {
            bool was_new = !has_data;
            has_data = true;
            data = value;
            return {this, was_new};
        }

        // Continue to child
        char c = key.cur();
        int offset = 0;

        if (pop.find_pop(c, &offset)) {
            node* child = children[offset];
            // Hold the lock while getting the child pointer, then release
            // The child cannot be deleted while we hold this lock
            lock.unlock();
            return child->insert_internal(key, value, this, c);
        } else {
            // Create new child with path compression
            auto* child = new node();
            child->parent = this;
            child->parent_edge = c;
            int idx = pop.set_bit(c);
            children.insert(children.begin() + idx, child);

            if (!key.is_empty()) {
                child->skip = key.remaining();
                key.eat(key.size());
            }

            child->has_data = true;
            child->data = value;
            return {child, true};
        }
    }

    // Remove - returns {should_remove_this_node, was_removed, should_compact}
    struct remove_result {
        bool should_remove;
        bool was_removed;
    };

    remove_result remove_internal(key_tp& key) {
        std::unique_lock lock(mtx);

        if (!skip.empty()) {
            if (!key.match(skip)) {
                return {false, false};
            }
            key.eat(skip.size());
        }

        if (key.is_empty()) {
            if (!has_data) {
                return {false, false};
            }

            has_data = false;
            data = T{};

            // Should remove if no data and no children
            return {!has_data && pop.empty(), true};
        }

        char c = key.cur();
        int offset = 0;

        if (!pop.find_pop(c, &offset)) {
            return {false, false};
        }

        node* child = children[offset];
        lock.unlock();

        auto result = child->remove_internal(key);

        if (result.was_removed) {
            lock.lock();

            // Re-verify the child still exists at expected position
            int current_offset;
            if (!pop.find_pop(c, &current_offset)) {
                // Child was removed by another thread
                return {!has_data && pop.empty(), true};
            }

            if (result.should_remove) {
                // Remove the empty child
                pop.clear_bit(c);
                children.erase(children.begin() + current_offset);
                delete child;
            }
            // Note: Compaction is disabled for thread safety
            // The trie will work correctly but may have suboptimal memory layout
            // after many insertions/deletions

            return {!has_data && pop.empty(), true};
        }

        return {false, false};
    }

    // Compact this node with its single child
    void try_compact() {
        if (has_data || pop.count() != 1 || parent == nullptr) {
            return;
        }

        char only_char = pop.first_char();
        node* only_child = children[0];

        std::unique_lock child_lock(only_child->mtx);

        // Merge: this node absorbs the child
        skip = skip + only_char + only_child->skip;
        has_data = only_child->has_data;
        data = std::move(only_child->data);
        pop = only_child->pop;
        
        // Take ownership of grandchildren
        std::vector<node*> grandchildren = std::move(only_child->children);
        only_child->children.clear();  // Prevent double-free
        children = std::move(grandchildren);

        // Update grandchildren's parent
        for (auto* gc : children) {
            if (gc) {
                std::unique_lock gc_lock(gc->mtx);
                gc->parent = this;
            }
        }

        child_lock.unlock();
        delete only_child;
    }

    // Clear data (for head node)
    void clear_data() {
        std::unique_lock lock(mtx);
        has_data = false;
        data = T{};
    }
};

// Forward iterator for trie - traverses in lexicographic order
template <class T>
class tktrie_iterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::pair<std::string, T&>;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using reference = value_type;

private:
    node<T>* current{nullptr};
    std::string current_key;

    // Find leftmost node with data starting from n
    void descend_to_first_data(node<T>* n, std::string prefix) {
        while (n) {
            // Build current key: prefix contains everything before this node
            // This node contributes its skip string
            current_key = prefix + n->get_skip();

            if (n->has_value()) {
                current = n;
                return;
            }

            // Go to first child
            char fc = n->first_child_char();
            if (fc == '\0') {
                current = nullptr;
                return;
            }

            // The child's prefix is current_key + the edge character
            prefix = current_key + fc;
            n = n->get_child(fc);
        }
        current = nullptr;
    }

    // Move to next node with data
    void advance() {
        if (!current) return;

        node<T>* n = current;

        // Try to go to first child
        char fc = n->first_child_char();
        if (fc != '\0') {
            std::string prefix = current_key + fc;
            descend_to_first_data(n->get_child(fc), prefix);
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
            
            // Reconstruct parent's key
            // Current key = parent_key + edge + n->skip
            size_t parent_key_len = current_key.length() - n->get_skip().length() - 1;
            std::string parent_key = current_key.substr(0, parent_key_len);

            // Find next sibling
            char next = p->next_child_char(edge);
            if (next != '\0') {
                std::string prefix = parent_key + next;
                descend_to_first_data(p->get_child(next), prefix);
                return;
            }

            // No more siblings - go up
            current_key = parent_key;
            n = p;
        }

        current = nullptr;
    }

public:
    tktrie_iterator() = default;

    tktrie_iterator(node<T>* root, bool is_end = false) {
        if (is_end || !root) {
            current = nullptr;
            return;
        }

        // Start from root, find first node with data
        if (root->has_value()) {
            current = root;
            current_key = root->get_skip();
        } else {
            descend_to_first_data(root, "");
        }
    }

    reference operator*() const {
        return {current_key, current->get_data()};
    }

    tktrie_iterator& operator++() {
        advance();
        return *this;
    }

    tktrie_iterator operator++(int) {
        tktrie_iterator tmp = *this;
        advance();
        return tmp;
    }

    bool operator==(const tktrie_iterator& other) const {
        return current == other.current;
    }

    bool operator!=(const tktrie_iterator& other) const {
        return current != other.current;
    }

    // Get current node
    node<T>* get_node() const { return current; }

    // Get current key
    const std::string& key() const { return current_key; }
};

// Const iterator
template <class T>
class tktrie_const_iterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::pair<std::string, const T&>;
    using difference_type = std::ptrdiff_t;
    using pointer = const value_type*;
    using reference = value_type;

private:
    const node<T>* current{nullptr};
    std::string current_key;

    void descend_to_first_data(const node<T>* n, std::string prefix) {
        while (n) {
            current_key = prefix + n->get_skip();

            if (n->has_value()) {
                current = n;
                return;
            }

            char fc = n->first_child_char();
            if (fc == '\0') {
                current = nullptr;
                return;
            }

            prefix = current_key + fc;
            n = n->get_child(fc);
        }
        current = nullptr;
    }

    void advance() {
        if (!current) return;

        const node<T>* n = current;

        char fc = n->first_child_char();
        if (fc != '\0') {
            std::string prefix = current_key + fc;
            descend_to_first_data(n->get_child(fc), prefix);
            return;
        }

        while (n) {
            const node<T>* p = n->get_parent();
            if (!p) {
                current = nullptr;
                return;
            }

            char edge = n->get_parent_edge();
            size_t parent_key_len = current_key.length() - n->get_skip().length() - 1;
            std::string parent_key = current_key.substr(0, parent_key_len);

            char next = p->next_child_char(edge);
            if (next != '\0') {
                std::string prefix = parent_key + next;
                descend_to_first_data(p->get_child(next), prefix);
                return;
            }

            current_key = parent_key;
            n = p;
        }

        current = nullptr;
    }

public:
    tktrie_const_iterator() = default;

    tktrie_const_iterator(const node<T>* root, bool is_end = false) {
        if (is_end || !root) {
            current = nullptr;
            return;
        }

        if (root->has_value()) {
            current = root;
            current_key = root->get_skip();
        } else {
            descend_to_first_data(root, "");
        }
    }

    reference operator*() const {
        return {current_key, current->get_data()};
    }

    tktrie_const_iterator& operator++() {
        advance();
        return *this;
    }

    tktrie_const_iterator operator++(int) {
        tktrie_const_iterator tmp = *this;
        advance();
        return tmp;
    }

    bool operator==(const tktrie_const_iterator& other) const {
        return current == other.current;
    }

    bool operator!=(const tktrie_const_iterator& other) const {
        return current != other.current;
    }

    const node<T>* get_node() const { return current; }
    const std::string& key() const { return current_key; }
};

template <class T>
class tktrie {
    node<T> head;
    mutable std::shared_mutex mtx;
    mutable std::mutex write_mtx;  // Serialize all write operations for safety
    size_t count{0};

public:
    using iterator = tktrie_iterator<T>;
    using const_iterator = tktrie_const_iterator<T>;

    tktrie() = default;
    ~tktrie() = default;

    tktrie(const tktrie&) = delete;
    tktrie& operator=(const tktrie&) = delete;
    tktrie(tktrie&&) = delete;
    tktrie& operator=(tktrie&&) = delete;

    // Find a key, returns nullptr if not found
    node<T>* find(const std::string& key) {
        std::shared_lock lock(mtx);  // Allow concurrent reads
        
        if (key.empty()) {
            return head.has_value() ? &head : nullptr;
        }

        key_tp cp(key);
        node<T>* run = &head;

        while (run) {
            node<T>* nxt = run->find_internal(cp);
            if (!nxt) return nullptr;
            if (nxt == run) return run;  // Found at this node (key exhausted with data)
            run = nxt;
            
            // If key was exhausted but find_internal returned a child,
            // we need to check that child's skip is empty
            if (cp.is_empty()) {
                // We've consumed all key characters including the edge to 'run'
                // Now check if 'run' matches (empty skip and has data)
                if (!run->get_skip().empty()) {
                    return nullptr;  // Child has skip that wasn't matched
                }
                return run->has_value() ? run : nullptr;
            }
        }

        return nullptr;
    }

    const node<T>* find(const std::string& key) const {
        return const_cast<tktrie*>(this)->find(key);
    }

    // Insert a key-value pair, returns true if new entry
    bool insert(const std::string& key, const T& value) {
        std::unique_lock wlock(write_mtx);  // Serialize writes
        std::unique_lock lock(mtx);  // Exclusive access
        
        key_tp cp(key);
        auto [result_node, was_new] = head.insert_internal(cp, value);

        if (was_new) {
            ++count;
        }

        return was_new;
    }

    // Insert or update, returns reference to stored value
    T& operator[](const std::string& key) {
        node<T>* n = find(key);
        if (n) return n->get_data();

        insert(key, T{});
        return find(key)->get_data();
    }

    // Remove a key, returns true if found and removed
    bool remove(const std::string& key) {
        std::unique_lock wlock(write_mtx);  // Serialize writes
        std::unique_lock lock(mtx);  // Exclusive access
        
        if (key.empty()) {
            if (head.has_value()) {
                head.clear_data();
                --count;
                return true;
            }
            return false;
        }

        key_tp cp(key);
        auto result = head.remove_internal(cp);

        if (result.was_removed) {
            --count;
        }

        return result.was_removed;
    }

    // Check if key exists
    bool contains(const std::string& key) const {
        return find(key) != nullptr;
    }

    size_t size() const {
        std::shared_lock lock(mtx);
        return count;
    }

    bool empty() const {
        std::shared_lock lock(mtx);
        return count == 0;
    }

    void clear() {
        std::unique_lock wlock(write_mtx);
        std::unique_lock lock(mtx);
        head.~node<T>();
        new (&head) node<T>();
        count = 0;
    }

    // Iterators
    iterator begin() { return iterator(&head); }
    iterator end() { return iterator(nullptr, true); }

    const_iterator begin() const { return const_iterator(&head); }
    const_iterator end() const { return const_iterator(nullptr, true); }

    const_iterator cbegin() const { return begin(); }
    const_iterator cend() const { return end(); }

    // Find all keys with given prefix
    std::vector<std::string> keys_with_prefix(const std::string& prefix) const {
        std::vector<std::string> result;

        if (prefix.empty()) {
            for (auto it = begin(); it != end(); ++it) {
                result.push_back(it.key());
            }
            return result;
        }

        // Navigate to prefix node
        key_tp cp(prefix);
        const node<T>* run = &head;

        while (run && !cp.is_empty()) {
            std::shared_lock lock(run->mtx);

            if (!run->get_skip().empty()) {
                const std::string& skip = run->get_skip();
                size_t match_len = 0;

                while (match_len < skip.size() && !cp.is_empty() &&
                       skip[match_len] == cp.peek()) {
                    cp.eat(1);
                    ++match_len;
                }

                if (match_len < skip.size()) {
                    // Check if skip is a prefix of remaining key or vice versa
                    if (cp.is_empty()) {
                        // Prefix ends in middle of skip - collect all under this node
                        break;
                    }
                    return result;  // Mismatch
                }
            }

            if (cp.is_empty()) break;

            char c = cp.cur();
            int offset;
            if (!run->pop.find_pop(c, &offset)) {
                return result;
            }
            run = run->children[offset];
        }

        // Collect all keys under 'run'
        if (run) {
            std::function<void(const node<T>*, const std::string&)> collect =
                [&](const node<T>* n, const std::string& key_so_far) {
                    std::string full_key = key_so_far + n->get_skip();

                    if (n->has_value()) {
                        result.push_back(full_key);
                    }

                    for (int i = 0; i < n->child_count(); ++i) {
                        char c = n->pop.char_at_index(i);
                        collect(n->children[i], full_key + c);
                    }
                };

            collect(run, prefix.substr(0, prefix.length() - cp.size()));
        }

        return result;
    }
};

// Type alias for backward compatibility
using test_type = std::array<char, 24>;

inline auto tst_find(tktrie<test_type>& N, const std::string& key) {
    return N.find(key);
}
