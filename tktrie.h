#pragma once
// Thread-safe trie: lock-free reads via COW, global write lock
// Any structural change copies the affected node

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace gteitelbaum {

class PopCount {
    uint64_t bits[4]{};
public:
    bool find(char c, int* idx) const {
        uint8_t v = static_cast<uint8_t>(c);
        int word = v >> 6, bit = v & 63;
        uint64_t mask = 1ULL << bit;
        if (!(bits[word] & mask)) return false;
        *idx = std::popcount(bits[word] & (mask - 1));
        for (int w = 0; w < word; ++w) *idx += std::popcount(bits[w]);
        return true;
    }
    int set(char c) {
        uint8_t v = static_cast<uint8_t>(c);
        int word = v >> 6, bit = v & 63;
        uint64_t mask = 1ULL << bit;
        int idx = std::popcount(bits[word] & (mask - 1));
        for (int w = 0; w < word; ++w) idx += std::popcount(bits[w]);
        bits[word] |= mask;
        return idx;
    }
    void clear(char c) {
        uint8_t v = static_cast<uint8_t>(c);
        bits[v >> 6] &= ~(1ULL << (v & 63));
    }
    int count() const {
        int n = 0;
        for (auto b : bits) n += std::popcount(b);
        return n;
    }
    char first_char() const {
        for (int w = 0; w < 4; w++) {
            if (bits[w]) return static_cast<char>((w << 6) | std::countr_zero(bits[w]));
        }
        return 0;
    }
};

class RetireList {
    struct Retired { void* ptr; void (*deleter)(void*); };
    std::vector<Retired> list;
    std::mutex mtx;
public:
    template<typename T> void retire(T* ptr) {
        std::lock_guard<std::mutex> lock(mtx);
        list.push_back({ptr, [](void* p) { delete static_cast<T*>(p); }});
    }
    ~RetireList() { for (auto& r : list) r.deleter(r.ptr); }
};

template <typename T>
struct Node {
    PopCount pop{};
    std::vector<Node*> children{};
    std::string skip{};
    T data{};
    bool has_data{false};

    Node() = default;
    Node(const Node& o) : pop(o.pop), children(o.children), skip(o.skip), data(o.data), has_data(o.has_data) {}
    
    Node* get_child(char c) const {
        int idx;
        if (pop.find(c, &idx)) {
            return __atomic_load_n(&children[idx], __ATOMIC_ACQUIRE);
        }
        return nullptr;
    }
    
    void set_child(int idx, Node* child) {
        __atomic_store_n(&children[idx], child, __ATOMIC_RELEASE);
    }
    
    int child_count() const { return pop.count(); }
};

template <typename Key, typename T>
class tktrie {
public:
    using node_type = Node<T>;
    using size_type = std::size_t;

private:
    node_type* root_;
    std::atomic<size_type> elem_count_{0};
    RetireList retired_;
    std::mutex write_mutex_;

    node_type* get_root() const { return __atomic_load_n(&root_, __ATOMIC_ACQUIRE); }
    void set_root(node_type* n) { __atomic_store_n(&root_, n, __ATOMIC_RELEASE); }

public:
    tktrie() : root_(new node_type()) {}
    ~tktrie() { delete_tree(root_); }

    bool empty() const { return size() == 0; }
    size_type size() const { return elem_count_.load(std::memory_order_relaxed); }

    // Lock-free read
    bool contains(const Key& key) const {
        std::string_view kv(key);
        node_type* cur = get_root();
        while (cur) {
            const std::string& skip = cur->skip;
            if (!skip.empty()) {
                if (kv.size() < skip.size() || kv.substr(0, skip.size()) != skip)
                    return false;
                kv.remove_prefix(skip.size());
            }
            if (kv.empty()) return cur->has_data;
            char c = kv[0];
            kv.remove_prefix(1);
            cur = cur->get_child(c);
        }
        return false;
    }

    bool insert(const std::pair<const Key, T>& value) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        return insert_impl(value.first, value.second);
    }

    bool erase(const Key& key) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        return erase_impl(key);
    }

private:
    void delete_tree(node_type* n) {
        if (!n) return;
        for (auto* c : n->children) delete_tree(c);
        delete n;
    }

    bool insert_impl(const Key& key, const T& value) {
        size_t kpos = 0;
        node_type** parent_child_ptr = nullptr;
        node_type* cur = get_root();
        bool at_root = true;
        
        while (true) {
            const std::string& skip = cur->skip;
            size_t common = 0;
            while (common < skip.size() && kpos + common < key.size() &&
                   skip[common] == key[kpos + common]) ++common;

            // Exact match - just set has_data (no structural change if already has children)
            if (kpos + common == key.size() && common == skip.size()) {
                if (cur->has_data) return false;
                // Copy node to set data
                node_type* n = new node_type(*cur);
                n->has_data = true;
                n->data = value;
                if (at_root) set_root(n);
                else __atomic_store_n(parent_child_ptr, n, __ATOMIC_RELEASE);
                retired_.retire(cur);
                elem_count_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }

            // Key is prefix - split
            if (kpos + common == key.size()) {
                node_type* split = new node_type();
                split->skip = skip.substr(0, common);
                split->has_data = true;
                split->data = value;
                node_type* child = new node_type(*cur);
                child->skip = skip.substr(common + 1);
                split->pop.set(skip[common]);
                split->children.push_back(child);
                if (at_root) set_root(split);
                else __atomic_store_n(parent_child_ptr, split, __ATOMIC_RELEASE);
                retired_.retire(cur);
                elem_count_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }

            // Skip fully matched - continue
            if (common == skip.size()) {
                kpos += common;
                char c = key[kpos];
                int idx;
                if (!cur->pop.find(c, &idx)) {
                    // Add new child - copy node
                    node_type* n = new node_type(*cur);
                    node_type* leaf = new node_type();
                    leaf->skip = key.substr(kpos + 1);
                    leaf->has_data = true;
                    leaf->data = value;
                    int new_idx = n->pop.set(c);
                    n->children.insert(n->children.begin() + new_idx, leaf);
                    if (at_root) set_root(n);
                    else __atomic_store_n(parent_child_ptr, n, __ATOMIC_RELEASE);
                    retired_.retire(cur);
                    elem_count_.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }
                parent_child_ptr = &cur->children[idx];
                cur = cur->children[idx];
                at_root = false;
                kpos++;
                continue;
            }

            // Mismatch - split
            node_type* split = new node_type();
            split->skip = skip.substr(0, common);
            node_type* old_child = new node_type(*cur);
            old_child->skip = skip.substr(common + 1);
            node_type* new_child = new node_type();
            new_child->skip = key.substr(kpos + common + 1);
            new_child->has_data = true;
            new_child->data = value;
            
            char old_edge = skip[common], new_edge = key[kpos + common];
            if (old_edge < new_edge) {
                split->pop.set(old_edge); split->pop.set(new_edge);
                split->children.push_back(old_child); split->children.push_back(new_child);
            } else {
                split->pop.set(new_edge); split->pop.set(old_edge);
                split->children.push_back(new_child); split->children.push_back(old_child);
            }
            if (at_root) set_root(split);
            else __atomic_store_n(parent_child_ptr, split, __ATOMIC_RELEASE);
            retired_.retire(cur);
            elem_count_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
    }

    bool erase_impl(const Key& key) {
        size_t kpos = 0;
        node_type** parent_child_ptr = nullptr;
        node_type* cur = get_root();
        bool at_root = true;

        while (true) {
            const std::string& skip = cur->skip;
            if (!skip.empty()) {
                if (key.size() - kpos < skip.size()) return false;
                if (key.substr(kpos, skip.size()) != skip) return false;
                kpos += skip.size();
            }
            if (kpos == key.size()) {
                if (!cur->has_data) return false;
                // Copy node to clear data
                node_type* n = new node_type(*cur);
                n->has_data = false;
                n->data = T{};
                if (at_root) set_root(n);
                else __atomic_store_n(parent_child_ptr, n, __ATOMIC_RELEASE);
                retired_.retire(cur);
                elem_count_.fetch_sub(1, std::memory_order_relaxed);
                return true;
            }
            char c = key[kpos];
            int idx;
            if (!cur->pop.find(c, &idx)) return false;
            parent_child_ptr = &cur->children[idx];
            cur = cur->children[idx];
            at_root = false;
            kpos++;
        }
    }
};

} // namespace gteitelbaum
