#pragma once
// Thread-safe optimized trie v3:
// 1. Spinlocks with exponential backoff  
// 2. Read-lock for traversal, upgrade to write only when modifying
// 3. Hand-over-hand locking
// 4. string_view for traversal to avoid allocations

#include <array>
#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>
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
    
    bool try_upgrade() {
        int expected = 1;
        return state_.compare_exchange_strong(expected, -1,
            std::memory_order_acquire, std::memory_order_relaxed);
    }
    
    void downgrade() {
        state_.store(1, std::memory_order_release);
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
    std::string_view get_skip_view() const { return skip; }
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
            current_key = prefix + std::string(n->get_skip_view());
            if (n->has_value()) { current = n; return; }
            char fc = n->first_child_char();
            if (fc != '\0') { prefix = current_key + fc; n = n->get_child(fc); continue; }
            while (n) {
                node_type* p = n->get_parent();
                if (!p) { current = nullptr; return; }
                char edge = n->get_parent_edge();
                Key pk = current_key.substr(0, current_key.length() - n->get_skip_view().length() - 1);
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
            Key pk = current_key.substr(0, current_key.length() - n->get_skip_view().length() - 1);
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
    // Use string_view for traversal - no allocations
    node_type* find_internal(const Key& key) {
        std::string_view kv(key);
        node_type* cur = &head;
        cur->read_lock();

        while (true) {
            std::string_view skip = cur->get_skip_view();
            
            if (!skip.empty()) {
                if (kv.size() < skip.size()) { cur->read_unlock(); return nullptr; }
                if (kv.substr(0, skip.size()) != skip) { cur->read_unlock(); return nullptr; }
                kv.remove_prefix(skip.size());
            }
            
            if (kv.empty()) {
                node_type* result = cur->has_data ? cur : nullptr;
                cur->read_unlock();
                return result;
            }
            
            char c = kv[0];
            kv.remove_prefix(1);
            node_type* child = cur->get_child(c);
            if (!child) { cur->read_unlock(); return nullptr; }
            
            child->read_lock();
            cur->read_unlock();
            cur = child;
        }
    }

    std::pair<iterator, bool> insert_internal(const Key& key, const T& value) {
        std::string_view kv(key);
        node_type* cur = &head;
        cur->read_lock();
        size_t kpos = 0;  // Track position for creating substrings only when needed

        while (true) {
            std::string_view skip = cur->get_skip_view();
            size_t common = 0;
            size_t kv_len = kv.size();
            size_t skip_len = skip.size();
            
            while (common < skip_len && common < kv_len && skip[common] == kv[common]) 
                ++common;

            // Case 1: Exact match - kv fully consumed and skip fully matched
            if (common == kv_len && common == skip_len) {
                if (!cur->try_upgrade()) {
                    cur->read_unlock();
                    cur->write_lock();
                    // Re-verify
                    if (cur->skip.size() != skip_len) {
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

            // Case 2: Key is prefix of current node - split needed
            if (common == kv_len) {
                cur->read_unlock();
                cur->write_lock();
                
                // Re-verify after lock upgrade
                std::string_view skip2 = cur->get_skip_view();
                size_t common2 = 0;
                while (common2 < skip2.size() && common2 < kv.size() && 
                       skip2[common2] == kv[common2]) ++common2;
                       
                if (common2 != kv.size()) {
                    cur->write_unlock();
                    return insert_internal(key, value);
                }
                
                // Split: create child with remainder of skip
                auto* child = new node_type();
                child->skip = std::string(skip2.substr(common2 + 1));
                child->has_data = cur->has_data;
                child->data = std::move(cur->data);
                child->children = std::move(cur->children);
                child->pop = cur->pop;
                child->parent = cur;
                child->parent_edge = skip2[common2];
                for (auto* gc : child->children) if (gc) gc->parent = child;
                
                cur->skip = std::string(skip2.substr(0, common2));
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

            // Case 3: Skip fully matched, continue to child
            if (common == skip_len) {
                kpos += common;
                kv.remove_prefix(common);
                char c = kv[0];
                node_type* child = cur->get_child(c);
                
                if (child) {
                    child->read_lock();
                    cur->read_unlock();
                    cur = child;
                    kv.remove_prefix(1);
                    kpos++;
                    continue;
                }
                
                // Need write lock to add new child
                cur->read_unlock();
                cur->write_lock();
                child = cur->get_child(c);
                if (child) {
                    cur->write_unlock();
                    return insert_internal(key, value);
                }
                
                auto* newc = new node_type();
                newc->skip = std::string(kv.substr(1));
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

            // Case 4: Mismatch in skip - split node
            cur->read_unlock();
            cur->write_lock();
            
            // Re-verify
            std::string_view skip2 = cur->get_skip_view();
            size_t common2 = 0;
            while (common2 < skip2.size() && common2 < kv.size() && 
                   skip2[common2] == kv[common2]) ++common2;
                   
            if (common2 == skip2.size()) {
                cur->write_unlock();
                return insert_internal(key, value);
            }
            
            // Create node for old content
            auto* old_child = new node_type();
            old_child->skip = std::string(skip2.substr(common2 + 1));
            old_child->has_data = cur->has_data;
            old_child->data = std::move(cur->data);
            old_child->children = std::move(cur->children);
            old_child->pop = cur->pop;
            old_child->parent = cur;
            old_child->parent_edge = skip2[common2];
            for (auto* gc : old_child->children) if (gc) gc->parent = old_child;
            
            // Create node for new key
            auto* new_child = new node_type();
            new_child->skip = std::string(kv.substr(common2 + 1));
            new_child->has_data = true;
            new_child->data = value;
            new_child->parent = cur;
            new_child->parent_edge = kv[common2];
            
            cur->skip = std::string(skip2.substr(0, common2));
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
        std::string_view kv(key);
        node_type* cur = &head;
        cur->read_lock();

        while (true) {
            std::string_view skip = cur->get_skip_view();
            
            if (!skip.empty()) {
                if (kv.size() < skip.size()) { cur->read_unlock(); return false; }
                if (kv.substr(0, skip.size()) != skip) { cur->read_unlock(); return false; }
                kv.remove_prefix(skip.size());
            }
            
            if (kv.empty()) {
                if (!cur->has_data) { cur->read_unlock(); return false; }
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
            
            char c = kv[0];
            kv.remove_prefix(1);
            node_type* child = cur->get_child(c);
            if (!child) { cur->read_unlock(); return false; }
            
            child->read_lock();
            cur->read_unlock();
            cur = child;
        }
    }
};

} // namespace gteitelbaum
