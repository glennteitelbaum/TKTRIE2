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

        // FIX: Check if the bit is actually set
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

    // FIX: Corrected logic - empty when offset >= size
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

public:
    node() = default;
    ~node() {
        for (auto* child : nxt) {
            delete child;
        }
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
    node* insert_internal(key_tp& key, const T& value) {
        std::unique_lock<std::shared_mutex> lock(shared);

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
            return child->insert_internal(key, value);
        } else {
            // Create new child
            auto* child = new node();
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

    bool has_value() const { return has_data; }
    T& get_data() { return data; }
    const T& get_data() const { return data; }
};

template <class T>
class tktrie {
    node<T> head;
    std::shared_mutex shared;  // Protect count
    size_t count{0};

public:
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

    // Get number of entries
    size_t size() {
        std::shared_lock<std::shared_mutex> lock(shared);
        return count;
    }
};

using test_type = std::array<char, 24>;

auto tst_find(tktrie<test_type>& N, const std::string& key) {
    return N.find(key);
}
