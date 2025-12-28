#include <array>
#include <cstdint>
#include <mutex>
#include <string>
#include <shared_mutex>
#include <vector>

class alignas(32) pop_tp {
    using bits_t = uint64_t;
    std::array<bits_t, 4> bits{};  // Zero-initialize

public:
    // Check if character exists and return its compressed index
    bool find_pop(char c, int* cnt) const {
        uint8_t v = static_cast<uint8_t>(c);
        const int word = v >> 6;      // v / 64 -- Which 64-bit word (0-3)
        const int bit = v & 63;       // v % 64 -- Which bit within word
        const bits_t mask = 1ULL << bit;

        // Check if the bit is actually set
        if (!(bits[word] & mask)) {
            return false;
        }

        // Count bits before this one in the same word
        int idx = std::popcount(bits[word] & (mask - 1));

        // Add counts from previous words
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

        // Calculate index before setting the bit
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

        // Calculate index before clearing the bit
        int idx = std::popcount(bits[word] & (mask - 1));
        for (int i = 0; i < word; ++i) {
            idx += std::popcount(bits[i]);
        }

        bits[word] &= ~mask;
        return idx;
    }

    // Check if a character exists in the bitmap
    bool has(char c) const {
        uint8_t v = static_cast<uint8_t>(c);
        const int word = v >> 6;
        const int bit = v & 63;
        return bits[word] & (1ULL << bit);
    }

    // Get total number of children
    int count() const {
        int total = 0;
        for (const auto& w : bits) {
            total += std::popcount(w);
        }
        return total;
    }

    // Check if empty (no bits set)
    bool empty() const {
        for (const auto& w : bits) {
            if (w != 0) return false;
        }
        return true;
    }

    // Get the character at a given index (inverse of find_pop)
    // Returns '\0' if index is out of range
    char char_at_index(int target_idx) const {
        int current_idx = 0;
        for (int word = 0; word < 4; ++word) {
            bits_t w = bits[word];
            while (w != 0) {
                int bit = std::countr_zero(w);  // Find lowest set bit
                if (current_idx == target_idx) {
                    return static_cast<char>((word << 6) | bit);
                }
                ++current_idx;
                w &= w - 1;  // Clear lowest set bit
            }
        }
        return '\0';
    }
};

class key_tp {
    const std::string& key;
    size_t offset;

public:
    key_tp(const std::string& orig) : key(orig), offset(0) {}

    bool match(const std::string& skip) const {
        if (offset + skip.size() > key.size()) {
            return false;  // Not enough characters left
        }
        return skip == key.substr(offset, skip.size());
    }

    bool is_empty() const { return offset >= key.size(); }

    size_t size() const {
        return (offset < key.size()) ? key.size() - offset : 0;
    }

    void eat(size_t sz) { offset += sz; }

    // Get current character and advance
    char cur() {
        if (offset >= key.size()) {
            return '\0';  // Safety check
        }
        return key[offset++];
    }

    // Peek at current character without advancing
    char peek() const {
        if (offset >= key.size()) {
            return '\0';
        }
        return key[offset];
    }

    // Get remaining substring
    std::string remaining() const {
        if (offset >= key.size()) {
            return "";
        }
        return key.substr(offset);
    }

    // Reset to beginning
    void reset() { offset = 0; }
};

template <class T>
class node {
    std::shared_mutex shared{};
    pop_tp pop{};  
    node* parent{nullptr};
    std::string skip{};        // Path compression
    std::vector<node*> nxt{};
    bool has_data{false};    
    T data{};
    char parent_edge{'\0'};    // The character on the edge from parent to this node

public:
    node() = default;
    
    ~node() {
        // Recursively delete all children
        for (auto* child : nxt) {
            delete child;
        }
        nxt.clear();
    }

    // Non-copyable
    node(const node&) = delete;
    node& operator=(const node&) = delete;

    // Find traversal - returns next node or nullptr if not found
    // Returns this if key fully matched and has data
    node* find_internal(key_tp& key) {
        std::shared_lock<std::shared_mutex> lock(shared);

        // Handle path compression (skip string)
        if (!skip.empty()) {
            if (!key.match(skip)) {
                return nullptr;  // Mismatch in skip string
            }
            key.eat(skip.size());
        }

        // If key is exhausted, check if this node has data
        if (key.is_empty()) {
            return has_data ? this : nullptr;
        }

        // Look up next character in bitmap
        int offset = 0;
        char c = key.cur();
        if (!pop.find_pop(c, &offset)) {
            return nullptr;  // Character not in trie
        }

        return nxt[offset];
    }

    // Insert a key-value pair, returns pointer to the node holding data
    node* insert_internal(key_tp& key, const T& value, node* parent_node = nullptr, char edge = '\0') {
        std::unique_lock<std::shared_mutex> lock(shared);

        // Set parent info if provided
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
                // Need to split this node
                // Create new node for the remainder of skip
                auto* split = new node();
                split->skip = skip.substr(match_len + 1);
                split->has_data = has_data;
                split->data = std::move(data);
                split->nxt = std::move(nxt);
                split->pop = pop;
                split->parent = this;
                split->parent_edge = skip[match_len];

                // Update children's parent pointers
                for (auto* child : split->nxt) {
                    if (child) {
                        std::unique_lock<std::shared_mutex> child_lock(child->shared);
                        child->parent = split;
                    }
                }

                // Reset this node
                has_data = false;
                data = T{};
                nxt.clear();
                pop = pop_tp{};

                // Add split node as child
                char split_char = skip[match_len];
                int idx = pop.set_bit(split_char);
                nxt.insert(nxt.begin() + idx, split);

                skip = skip.substr(0, match_len);
            }
        }

        // If key is exhausted, store data here
        if (key.is_empty()) {
            has_data = true;
            data = value;
            return this;
        }

        // Continue to child node
        char c = key.cur();
        int offset = 0;

        if (pop.find_pop(c, &offset)) {
            // Child exists, recurse (need to unlock first)
            node* child = nxt[offset];
            lock.unlock();
            return child->insert_internal(key, value, this, c);
        } else {
            // Create new child
            auto* child = new node();
            child->parent = this;
            child->parent_edge = c;
            int idx = pop.set_bit(c);
            nxt.insert(nxt.begin() + idx, child);

            // If more characters remain, use path compression
            if (!key.is_empty()) {
                child->skip = key.remaining();
                key.eat(key.size());
            }

            child->has_data = true;
            child->data = value;
            return child;
        }
    }

    // Remove data from this node and clean up if possible
    // Returns true if this node should be removed by parent (is now empty leaf)
    bool remove_internal(key_tp& key, bool* removed) {
        std::unique_lock<std::shared_mutex> lock(shared);

        // Handle path compression (skip string)
        if (!skip.empty()) {
            if (!key.match(skip)) {
                *removed = false;
                return false;  // Key not found
            }
            key.eat(skip.size());
        }

        // If key is exhausted, this is the node to remove data from
        if (key.is_empty()) {
            if (!has_data) {
                *removed = false;
                return false;  // Key not found
            }

            // Clear the data
            has_data = false;
            data = T{};
            *removed = true;

            // Return true if this node should be removed (no data AND no children)
            return !has_data && pop.empty();
        }

        // Continue to child node
        char c = key.cur();
        int offset = 0;

        if (!pop.find_pop(c, &offset)) {
            *removed = false;
            return false;  // Key not found
        }

        node* child = nxt[offset];
        lock.unlock();

        bool child_should_be_removed = child->remove_internal(key, removed);

        if (child_should_be_removed && *removed) {
            // Child should be removed - reacquire lock and clean up
            lock.lock();
            
            // Re-find the offset (structure may have changed)
            if (pop.find_pop(c, &offset)) {
                // Remove child from our structures
                int clear_idx = pop.clear_bit(c);
                nxt.erase(nxt.begin() + clear_idx);
                delete child;

                // Check if we should be removed too (no data AND no children)
                // But don't remove root
                return !has_data && pop.empty();
            }
        }

        return false;
    }

    bool has_value() const { return has_data; }
    T& get_data() { return data; }
    const T& get_data() const { return data; }

    // Get number of children
    int child_count() const { return pop.count(); }

    // Check if this is a leaf node (no children)
    bool is_leaf() const { return pop.empty(); }
};

template <class T>
class tktrie {
    node<T> head;
    std::shared_mutex shared;  // Protect count
    size_t count{0};

public:
    tktrie() = default;
    
    // Destructor - node destructor handles recursive cleanup
    ~tktrie() = default;

    // Non-copyable (due to mutex and internal pointers)
    tktrie(const tktrie&) = delete;
    tktrie& operator=(const tktrie&) = delete;

    // Move operations
    tktrie(tktrie&& other) noexcept = delete;  // Complex due to mutexes
    tktrie& operator=(tktrie&& other) noexcept = delete;

    // Find a key, returns nullptr if not found
    node<T>* find(const std::string& key) {
        if (key.empty()) {
            return head.has_value() ? &head : nullptr;
        }

        key_tp cp(key);
        node<T>* run = &head;

        while (run && !cp.is_empty()) {
            auto* nxt = run->find_internal(cp);
            if (!nxt) {
                return nullptr;  // Not found
            }
            if (nxt == run) {
                return run;  // Found (node returned itself)
            }
            run = nxt;
        }

        // Check if final node has data
        return (run && run->has_value()) ? run : nullptr;
    }

    // Insert a key-value pair
    void insert(const std::string& key, const T& value) {
        key_tp cp(key);

        node<T>* result = head.insert_internal(cp, value);
        if (result) {
            std::unique_lock<std::shared_mutex> lock(shared);
            ++count;
        }
    }

    // Remove a key, returns true if key was found and removed
    bool remove(const std::string& key) {
        if (key.empty()) {
            // Special case: removing data from head node
            std::unique_lock<std::shared_mutex> lock(shared);
            if (head.has_value()) {
                // Need to clear head's data directly
                // This is a bit awkward - we need internal access
                --count;
            }
            // Note: Can't actually clear head's data without friend access
            // For now, return false for empty key removal
            return false;
        }

        key_tp cp(key);
        bool removed = false;

        head.remove_internal(cp, &removed);

        if (removed) {
            std::unique_lock<std::shared_mutex> lock(shared);
            --count;
        }

        return removed;
    }

    // Get number of entries
    size_t size() {
        std::shared_lock<std::shared_mutex> lock(shared);
        return count;
    }

    // Check if empty
    bool empty() {
        std::shared_lock<std::shared_mutex> lock(shared);
        return count == 0;
    }

    // Clear all entries
    void clear() {
        std::unique_lock<std::shared_mutex> lock(shared);
        // Create a new empty head by moving/swapping
        // The old head's destructor will clean up children
        head.~node<T>();
        new (&head) node<T>();
        count = 0;
    }
};

using test_type = std::array<char, 24>;

auto tst_find(tktrie<test_type>& N, const std::string& key) {
    return N.find(key);
}
