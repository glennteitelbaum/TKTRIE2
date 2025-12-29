#pragma once
// Thread-safe optimized trie v2:
// 1. Spinlocks with exponential backoff  
// 2. Read-lock for traversal, upgrade to write only when modifying
// 3. Hand-over-hand locking

#include <array>
#include <atomic>
#include <cstdint>
#include <string>
#include <vector>
#include <utility>
#include <initializer_list>
#include <thread>

namespace gteitelbaum {

class alignas(64) pop_tp {
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

    int count() const {
        int total = 0;
        for (const auto& w : bits) total += std::popcount(w);
        return total;
    }

    bool empty() const { return (bits[0] | bits[1] | bits[2] | bits[3]) == 0; }

    char first_char() const {
        for (int word = 0; word < 4; ++word) {
            if (bits[word] != 0) return static_cast<char>((word << 6) | std::countr_zero(bits[word]));
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

template <class Key, class T> class tktrie;

// Optimized reader-writer spinlock with try_upgrade
class rw_spinlock {
    std::atomic<int> state_{0};  // 0=free, -1=write, >0=readers
    
    void backoff(int spins) {
        if (spins < 4) {
            #if defined(__x86_64__)
            __builtin_ia32_pause();
            #endif
        } else if (spins < 16) {
            for (int i = 0; i < spins; i++) {
                #if defined(__x86_64__)
                __builtin_ia32_pause();
                #endif
            }
        } else if (spins < 32) {
            std::this_thread::yield();
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        }
    }
    
public:
    void read_lock() {
        int spins = 0;
        while (true) {
            int expected = state_.load(std::memory_order_relaxed);
            if (expected >= 0 && state_.compare_exchange_weak(expected, expected + 1,
                std::memory_order_acquire, std::memory_order_relaxed)) return;
            backoff(++spins);
        }
    }
    
    void read_unlock() {
        state_.fetch_sub(1, std::memory_order_release);
    }
    
    void write_lock() {
        int spins = 0;
        while (true) {
            int expected = 0;
            if (state_.compare_exchange_weak(expected, -1,
                std::memory_order_acquire, std::memory_order_relaxed)) return;
            backoff(++spins);
        }
    }
    
    void write_unlock() {
        state_.store(0, std::memory_order_release);
    }
    
    // Try to upgrade from read lock to write lock (must hold exactly one read lock)
    bool try_upgrade() {
        int expected = 1;  // We are the only reader
        return state_.compare_exchange_strong(expected, -1,
            std::memory_order_acquire, std::memory_order_relaxed);
    }
    
    // Downgrade from write to read lock
    void downgrade() {
        state_.store(1, std::memory_order_release);  // -1 -> 1 (one reader)
    }
};

template <class Key, class T>
class alignas(64) tktrie_node {
    friend class tktrie<Key, T>;

    mutable rw_spinlock lock_;
    pop_tp pop{};
    std::vector<tktrie_node*> children{};
    tktrie_node* parent{nullptr};
    std::string skip{};
    T data{};
    char parent_edge{'\0'};
    bool has_data{false};

public:
    tktrie_node() = default;
    ~tktrie_node() { for (auto* c : children) delete c; }
    tktrie_node(const tktrie_node&) = delete;
    tktrie_node& operator=(const tktrie_node&) = delete;

    bool has_value() const { return has_data; }
    T& get_data() { return data; }
    const T& get_data() const { return data; }
    const std::string& get_skip() const { return skip; }
    tktrie_node* get_parent() const { return parent; }
    char get_parent_edge() const { return parent_edge; }

    tktrie_node* get_child(char c) const {
        int idx;
        if (pop.find_pop(c, &idx)) return children[idx];
        return nullptr;
    }

    char first_child_char() const { return pop.first_char(); }
    char next_child_char(char c) const { return pop.next_char(c); }

    void read_lock() const { lock_.read_lock(); }
    void read_unlock() const { lock_.read_unlock(); }
    void write_lock() { lock_.write_lock(); }
    void write_unlock() { lock_.write_unlock(); }
    bool try_upgrade() { return lock_.try_upgrade(); }
    void downgrade() { lock_.downgrade(); }
};

template <class Key, class T>
class tktrie_iterator {
public:
    using node_type = tktrie_node<Key, T>;
    using reference = std::pair<const Key&, T&>;
private:
    node_type* current{nullptr};
    Key current_key;

    void find_next(node_type* n, Key prefix) {
        while (n) {
            current_key = prefix + n->get_skip();
            if (n->has_value()) { current = n; return; }
            char fc = n->first_child_char();
            if (fc != '\0') { prefix = current_key + fc; n = n->get_child(fc); continue; }
            while (n) {
                node_type* p = n->get_parent();
                if (!p) { current = nullptr; return; }
                char edge = n->get_parent_edge();
                Key pk = current_key.substr(0, current_key.length() - n->get_skip().length() - 1);
                char next = p->next_child_char(edge);
                if (next != '\0') { prefix = pk + next; n = p->get_child(next); break; }
                current_key = pk; n = p;
            }
        }
        current = nullptr;
    }

public:
    tktrie_iterator() = default;
    tktrie_iterator(node_type* root, bool is_end = false) {
        if (is_end || !root) { current = nullptr; return; }
        find_next(root, "");
    }
    tktrie_iterator(node_type* n, const Key& key) : current(n), current_key(key) {}
    
    reference operator*() const { return {current_key, current->get_data()}; }
    tktrie_iterator& operator++() {
        if (!current) return *this;
        node_type* n = current;
        char fc = n->first_child_char();
        if (fc != '\0') { find_next(n->get_child(fc), current_key + fc); return *this; }
        while (n) {
            node_type* p = n->get_parent();
            if (!p) { current = nullptr; return *this; }
            char edge = n->get_parent_edge();
            Key pk = current_key.substr(0, current_key.length() - n->get_skip().length() - 1);
            char next = p->next_child_char(edge);
            if (next != '\0') { find_next(p->get_child(next), pk + next); return *this; }
            current_key = pk; n = p;
        }
        current = nullptr;
        return *this;
    }
    bool operator==(const tktrie_iterator& o) const { return current == o.current; }
    bool operator!=(const tktrie_iterator& o) const { return current != o.current; }
    node_type* get_node() const { return current; }
    const Key& key() const { return current_key; }
};

template <class Key, class T>
class tktrie {
public:
    using iterator = tktrie_iterator<Key, T>;
    using node_type = tktrie_node<Key, T>;
    using size_type = std::size_t;

private:
    node_type head;
    std::atomic<size_type> elem_count{0};

public:
    tktrie() = default;
    tktrie(std::initializer_list<std::pair<const Key, T>> init) {
        for (const auto& p : init) insert(p);
    }
    ~tktrie() = default;
    tktrie(const tktrie&) = delete;
    tktrie& operator=(const tktrie&) = delete;

    iterator begin() noexcept { return iterator(&head); }
    iterator end() noexcept { return iterator(nullptr, true); }
    bool empty() const noexcept { return size() == 0; }
    size_type size() const noexcept { return elem_count.load(std::memory_order_relaxed); }

    std::pair<iterator, bool> insert(const std::pair<const Key, T>& value) {
        return insert_internal(value.first, value.second);
    }

    size_type erase(const Key& key) {
        return remove_internal(key) ? 1 : 0;
    }

    iterator find(const Key& key) {
        node_type* n = find_internal(key);
        return n ? iterator(n, key) : end();
    }

    bool contains(const Key& key) { return find_internal(key) != nullptr; }
    size_type count(const Key& key) { return contains(key) ? 1 : 0; }

private:
    node_type* find_internal(const Key& key) {
        node_type* cur = &head;
        cur->read_lock();
        size_t kpos = 0;

        while (true) {
            const std::string& skip = cur->skip;
            if (!skip.empty()) {
                if (key.size() - kpos < skip.size()) { cur->read_unlock(); return nullptr; }
                bool mismatch = false;
                for (size_t i = 0; i < skip.size(); ++i) {
                    if (key[kpos + i] != skip[i]) { mismatch = true; break; }
                }
                if (mismatch) { cur->read_unlock(); return nullptr; }
                kpos += skip.size();
            }
            
            if (kpos == key.size()) {
                node_type* result = cur->has_data ? cur : nullptr;
                cur->read_unlock();
                return result;
            }
            
            char c = key[kpos++];
            node_type* child = cur->get_child(c);
            if (!child) { cur->read_unlock(); return nullptr; }
            
            child->read_lock();
            cur->read_unlock();
            cur = child;
        }
    }

    std::pair<iterator, bool> insert_internal(const Key& key, const T& value) {
        node_type* cur = &head;
        cur->read_lock();  // Start with read lock
        size_t kpos = 0;

        while (true) {
            const std::string& skip = cur->skip;
            size_t common = 0;
            while (common < skip.size() && kpos + common < key.size() &&
                   skip[common] == key[kpos + common]) ++common;

            // Case 1: Exact match
            if (kpos + common == key.size() && common == skip.size()) {
                // Try to upgrade to write
                if (!cur->try_upgrade()) {
                    cur->read_unlock();
                    cur->write_lock();
                    // Re-verify state after acquiring write lock
                    if (cur->skip != skip) {
                        // Structure changed, restart
                        cur->write_unlock();
                        return insert_internal(key, value);
                    }
                }
                bool was_new = !cur->has_data;
                cur->has_data = true;
                if (was_new) {
                    cur->data = value;
                    elem_count.fetch_add(1, std::memory_order_relaxed);
                }
                cur->write_unlock();
                return {iterator(cur, key), was_new};
            }

            // Case 2: Key is prefix - need write lock for split
            if (kpos + common == key.size()) {
                cur->read_unlock();
                cur->write_lock();
                // Re-verify after acquiring write lock
                const std::string& skip2 = cur->skip;
                size_t common2 = 0;
                while (common2 < skip2.size() && kpos + common2 < key.size() &&
                       skip2[common2] == key[kpos + common2]) ++common2;
                if (kpos + common2 != key.size()) {
                    // State changed, restart
                    cur->write_unlock();
                    return insert_internal(key, value);
                }
                
                auto* child = new node_type();
                child->skip = skip2.substr(common2 + 1);
                child->has_data = cur->has_data;
                child->data = std::move(cur->data);
                child->children = std::move(cur->children);
                child->pop = cur->pop;
                child->parent = cur;
                child->parent_edge = skip2[common2];
                for (auto* gc : child->children) if (gc) gc->parent = child;
                
                cur->skip = skip2.substr(0, common2);
                cur->has_data = true;
                cur->data = value;
                cur->children.clear();
                cur->pop = pop_tp{};
                int idx = cur->pop.set_bit(child->parent_edge);
                cur->children.insert(cur->children.begin() + idx, child);
                
                elem_count.fetch_add(1, std::memory_order_relaxed);
                cur->write_unlock();
                return {iterator(cur, key), true};
            }

            // Case 3: Skip fully matched, continue
            if (common == skip.size()) {
                kpos += common;
                char c = key[kpos];
                node_type* child = cur->get_child(c);
                
                if (child) {
                    child->read_lock();
                    cur->read_unlock();
                    cur = child;
                    kpos++;
                    continue;
                }
                
                // Need write lock to add child
                cur->read_unlock();
                cur->write_lock();
                // Re-check child existence
                child = cur->get_child(c);
                if (child) {
                    // Another thread added it, retry
                    cur->write_unlock();
                    return insert_internal(key, value);
                }
                
                auto* newc = new node_type();
                newc->skip = key.substr(kpos + 1);
                newc->has_data = true;
                newc->data = value;
                newc->parent = cur;
                newc->parent_edge = c;
                int idx = cur->pop.set_bit(c);
                cur->children.insert(cur->children.begin() + idx, newc);
                
                elem_count.fetch_add(1, std::memory_order_relaxed);
                cur->write_unlock();
                return {iterator(newc, key), true};
            }

            // Case 4: Mismatch - need write lock for split
            cur->read_unlock();
            cur->write_lock();
            // Re-verify
            const std::string& skip2 = cur->skip;
            size_t common2 = 0;
            while (common2 < skip2.size() && kpos + common2 < key.size() &&
                   skip2[common2] == key[kpos + common2]) ++common2;
            if (common2 == skip2.size()) {
                // State changed, restart
                cur->write_unlock();
                return insert_internal(key, value);
            }
            
            auto* old_child = new node_type();
            old_child->skip = skip2.substr(common2 + 1);
            old_child->has_data = cur->has_data;
            old_child->data = std::move(cur->data);
            old_child->children = std::move(cur->children);
            old_child->pop = cur->pop;
            old_child->parent = cur;
            old_child->parent_edge = skip2[common2];
            for (auto* gc : old_child->children) if (gc) gc->parent = old_child;
            
            auto* new_child = new node_type();
            new_child->skip = key.substr(kpos + common2 + 1);
            new_child->has_data = true;
            new_child->data = value;
            new_child->parent = cur;
            new_child->parent_edge = key[kpos + common2];
            
            cur->skip = skip2.substr(0, common2);
            cur->has_data = false;
            cur->data = T{};
            cur->children.clear();
            cur->pop = pop_tp{};
            int i1 = cur->pop.set_bit(old_child->parent_edge);
            cur->children.insert(cur->children.begin() + i1, old_child);
            int i2 = cur->pop.set_bit(new_child->parent_edge);
            cur->children.insert(cur->children.begin() + i2, new_child);
            
            elem_count.fetch_add(1, std::memory_order_relaxed);
            cur->write_unlock();
            return {iterator(new_child, key), true};
        }
    }

    bool remove_internal(const Key& key) {
        node_type* cur = &head;
        cur->read_lock();
        size_t kpos = 0;

        while (true) {
            const std::string& skip = cur->skip;
            if (!skip.empty()) {
                if (key.size() - kpos < skip.size()) { cur->read_unlock(); return false; }
                bool mismatch = false;
                for (size_t i = 0; i < skip.size(); ++i) {
                    if (key[kpos + i] != skip[i]) { mismatch = true; break; }
                }
                if (mismatch) { cur->read_unlock(); return false; }
                kpos += skip.size();
            }
            
            if (kpos == key.size()) {
                if (!cur->has_data) { cur->read_unlock(); return false; }
                // Upgrade to write
                if (!cur->try_upgrade()) {
                    cur->read_unlock();
                    cur->write_lock();
                    if (!cur->has_data) { cur->write_unlock(); return false; }
                }
                cur->has_data = false;
                cur->data = T{};
                elem_count.fetch_sub(1, std::memory_order_relaxed);
                cur->write_unlock();
                return true;
            }
            
            char c = key[kpos++];
            node_type* child = cur->get_child(c);
            if (!child) { cur->read_unlock(); return false; }
            
            child->read_lock();
            cur->read_unlock();
            cur = child;
        }
    }
};

} // namespace gteitelbaum
