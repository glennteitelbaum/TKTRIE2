#pragma once
// RCU-style trie: lock-free reads, copy-on-write for mutations
// Readers: just follow pointers, no atomics/locks
// Writers: copy path from root to modified node, atomic swap root

#include <array>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>
#include <thread>

namespace rcu {

// Simple deferred deletion using a retire list
// In production, use proper epoch-based reclamation
class RetireList {
    struct Retired {
        void* ptr;
        void (*deleter)(void*);
    };
    std::vector<Retired> list;
    std::mutex mtx;
    
public:
    template<typename T>
    void retire(T* ptr) {
        std::lock_guard<std::mutex> lock(mtx);
        list.push_back({ptr, [](void* p) { delete static_cast<T*>(p); }});
        
        // Don't delete anything during benchmark - let destructor handle it
        // This is safe but leaks memory
        // In production, use proper epoch-based reclamation
    }
    
    ~RetireList() {
        for (auto& r : list) {
            r.deleter(r.ptr);
        }
    }
};

// Bitmap for sparse children
class PopCount {
    uint64_t bits[4]{};

public:
    bool find(char c, int* idx) const {
        uint8_t v = static_cast<uint8_t>(c);
        int word = v >> 6;
        int bit = v & 63;
        uint64_t mask = 1ULL << bit;
        if (!(bits[word] & mask)) return false;
        int i = std::popcount(bits[word] & (mask - 1));
        for (int w = 0; w < word; ++w) i += std::popcount(bits[w]);
        *idx = i;
        return true;
    }

    int set(char c) {
        uint8_t v = static_cast<uint8_t>(c);
        int word = v >> 6;
        int bit = v & 63;
        uint64_t mask = 1ULL << bit;
        int idx = std::popcount(bits[word] & (mask - 1));
        for (int w = 0; w < word; ++w) idx += std::popcount(bits[w]);
        bits[word] |= mask;
        return idx;
    }
};

template <typename T>
struct Node {
    PopCount pop{};
    std::vector<Node*> children{};
    std::string skip{};
    T data{};
    bool has_data{false};

    Node() = default;
    
    // Shallow copy - copies child pointers, not children themselves
    Node(const Node& other) 
        : pop(other.pop)
        , children(other.children)
        , skip(other.skip)
        , data(other.data)
        , has_data(other.has_data) 
    {}

    Node* get_child(char c) const {
        int idx;
        if (pop.find(c, &idx)) return children[idx];
        return nullptr;
    }
};

template <typename Key, typename T>
class tktrie {
public:
    using node_type = Node<T>;
    using size_type = std::size_t;

private:
    std::atomic<node_type*> root_{new node_type()};
    std::atomic<size_type> elem_count_{0};
    RetireList retired_;
    std::mutex write_mutex_;

public:
    tktrie() = default;
    
    ~tktrie() {
        delete_tree(root_.load(std::memory_order_relaxed));
    }

    bool empty() const { return size() == 0; }
    size_type size() const { return elem_count_.load(std::memory_order_relaxed); }

    // Lock-free find - NO synchronization in hot path!
    node_type* find(const Key& key) const {
        std::string_view kv(key);
        node_type* cur = root_.load(std::memory_order_acquire);
        
        while (cur) {
            const std::string& skip = cur->skip;
            
            if (!skip.empty()) {
                if (kv.size() < skip.size()) return nullptr;
                if (kv.substr(0, skip.size()) != skip) return nullptr;
                kv.remove_prefix(skip.size());
            }
            
            if (kv.empty()) {
                return cur->has_data ? cur : nullptr;
            }
            
            char c = kv[0];
            kv.remove_prefix(1);
            cur = cur->get_child(c);
        }
        return nullptr;
    }

    bool contains(const Key& key) const {
        return find(key) != nullptr;
    }

    // Copy-on-write insert - copies entire path from root
    bool insert(const std::pair<const Key, T>& value) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        return insert_cow(value.first, value.second);
    }

    bool erase(const Key& key) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        return erase_cow(key);
    }

private:
    void delete_tree(node_type* n) {
        if (!n) return;
        for (auto* child : n->children) {
            delete_tree(child);
        }
        delete n;
    }

    // Copy entire path from root to target, apply modification
    bool insert_cow(const Key& key, const T& value) {
        node_type* old_root = root_.load(std::memory_order_relaxed);
        
        // Build new tree with path copied
        std::vector<node_type*> retired_nodes;
        auto [new_root, inserted] = copy_and_insert(old_root, key, 0, value, retired_nodes);
        
        if (new_root) {
            root_.store(new_root, std::memory_order_release);
            // Retire only the copied nodes, not the entire old tree
            for (auto* n : retired_nodes) {
                retired_.retire(n);
            }
            if (inserted) {
                elem_count_.fetch_add(1, std::memory_order_relaxed);
            }
            return inserted;
        }
        return false;
    }

    // Returns {new_node, was_inserted}
    std::pair<node_type*, bool> copy_and_insert(node_type* cur, const Key& key, 
                                                 size_t kpos, const T& value,
                                                 std::vector<node_type*>& retired) {
        if (!cur) {
            // Create new node for remaining key
            node_type* n = new node_type();
            n->skip = key.substr(kpos);
            n->has_data = true;
            n->data = value;
            return {n, true};
        }
        
        const std::string& skip = cur->skip;
        size_t common = 0;
        while (common < skip.size() && kpos + common < key.size() &&
               skip[common] == key[kpos + common]) {
            ++common;
        }

        // Case 1: Exact match
        if (kpos + common == key.size() && common == skip.size()) {
            if (cur->has_data) {
                return {nullptr, false};  // Already exists
            }
            node_type* n = new node_type(*cur);
            n->has_data = true;
            n->data = value;
            retired.push_back(cur);
            return {n, true};
        }

        // Case 2: Key is prefix of skip - split
        if (kpos + common == key.size()) {
            node_type* split = new node_type();
            split->skip = skip.substr(0, common);
            split->has_data = true;
            split->data = value;
            
            node_type* child = new node_type(*cur);
            child->skip = skip.substr(common + 1);
            
            char edge = skip[common];
            split->pop.set(edge);
            split->children.push_back(child);
            
            retired.push_back(cur);
            return {split, true};
        }

        // Case 3: Skip fully matched, continue to child
        if (common == skip.size()) {
            kpos += common;
            char c = key[kpos];
            node_type* child = cur->get_child(c);
            
            auto [new_child, inserted] = copy_and_insert(child, key, kpos + 1, value, retired);
            if (!new_child && !inserted) {
                return {nullptr, false};
            }
            
            node_type* n = new node_type(*cur);
            int idx;
            if (n->pop.find(c, &idx)) {
                n->children[idx] = new_child;
            } else {
                idx = n->pop.set(c);
                n->children.insert(n->children.begin() + idx, new_child);
            }
            retired.push_back(cur);
            return {n, inserted};
        }

        // Case 4: Mismatch - split
        node_type* split = new node_type();
        split->skip = skip.substr(0, common);
        
        // old_child gets the remainder AFTER the mismatch char
        node_type* old_child = new node_type(*cur);
        old_child->skip = (common + 1 < skip.size()) ? skip.substr(common + 1) : "";
        
        // new_child gets the remainder of key AFTER the mismatch char  
        node_type* new_child = new node_type();
        new_child->skip = (kpos + common + 1 < key.size()) ? key.substr(kpos + common + 1) : "";
        new_child->has_data = true;
        new_child->data = value;
        
        char old_edge = skip[common];
        char new_edge = key[kpos + common];
        
        // Must insert in sorted order by edge character
        if (old_edge < new_edge) {
            split->pop.set(old_edge);
            split->pop.set(new_edge);
            split->children.push_back(old_child);
            split->children.push_back(new_child);
        } else {
            split->pop.set(new_edge);
            split->pop.set(old_edge);
            split->children.push_back(new_child);
            split->children.push_back(old_child);
        }
        
        retired.push_back(cur);
        return {split, true};
    }

    bool erase_cow(const Key& key) {
        node_type* old_root = root_.load(std::memory_order_relaxed);
        
        std::vector<node_type*> retired_nodes;
        auto [new_root, erased] = copy_and_erase(old_root, key, 0, retired_nodes);
        
        if (erased) {
            root_.store(new_root ? new_root : new node_type(), std::memory_order_release);
            for (auto* n : retired_nodes) {
                retired_.retire(n);
            }
            elem_count_.fetch_sub(1, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    std::pair<node_type*, bool> copy_and_erase(node_type* cur, const Key& key, size_t kpos,
                                                std::vector<node_type*>& retired) {
        if (!cur) return {nullptr, false};
        
        const std::string& skip = cur->skip;
        
        if (!skip.empty()) {
            if (key.size() - kpos < skip.size()) return {nullptr, false};
            if (key.substr(kpos, skip.size()) != skip) return {nullptr, false};
            kpos += skip.size();
        }
        
        if (kpos == key.size()) {
            if (!cur->has_data) return {nullptr, false};
            
            node_type* n = new node_type(*cur);
            n->has_data = false;
            n->data = T{};
            retired.push_back(cur);
            return {n, true};
        }
        
        char c = key[kpos];
        node_type* child = cur->get_child(c);
        if (!child) return {nullptr, false};
        
        auto [new_child, erased] = copy_and_erase(child, key, kpos + 1, retired);
        if (!erased) return {nullptr, false};
        
        node_type* n = new node_type(*cur);
        int idx;
        n->pop.find(c, &idx);
        n->children[idx] = new_child;
        retired.push_back(cur);
        return {n, true};
    }
};

} // namespace rcu
