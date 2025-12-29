#pragma once
// RCU-style trie with full compaction
// Key insight: child pointers are already read atomically via __atomic_load_n
// So we can UPDATE them atomically too - no need to copy parent!

#include <array>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace gteitelbaum {

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
        if (pop.find(c, &idx)) return __atomic_load_n(&children[idx], __ATOMIC_ACQUIRE);
        return nullptr;
    }
    int child_count() const { return pop.count(); }
    
    // Atomically set child pointer (for in-place updates under write lock)
    void set_child(int idx, Node* child) {
        __atomic_store_n(&children[idx], child, __ATOMIC_RELEASE);
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
    ~tktrie() { delete_tree(root_.load(std::memory_order_relaxed)); }

    bool empty() const { return size() == 0; }
    size_type size() const { return elem_count_.load(std::memory_order_relaxed); }

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
            if (kv.empty()) return cur->has_data ? cur : nullptr;
            char c = kv[0];
            kv.remove_prefix(1);
            cur = cur->get_child(c);
        }
        return nullptr;
    }

    bool contains(const Key& key) const { return find(key) != nullptr; }

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
        node_type* cur = root_.load(std::memory_order_acquire);
        bool at_root = true;
        
        while (true) {
            const std::string& skip = cur->skip;
            size_t common = 0;
            while (common < skip.size() && kpos + common < key.size() &&
                   skip[common] == key[kpos + common]) ++common;

            if (kpos + common == key.size() && common == skip.size()) {
                if (cur->has_data) return false;
                node_type* n = new node_type(*cur);
                n->has_data = true;
                n->data = value;
                if (at_root) root_.store(n, std::memory_order_release);
                else __atomic_store_n(parent_child_ptr, n, __ATOMIC_RELEASE);
                retired_.retire(cur);
                elem_count_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }

            if (kpos + common == key.size()) {
                node_type* split = new node_type();
                split->skip = skip.substr(0, common);
                split->has_data = true;
                split->data = value;
                node_type* child = new node_type(*cur);
                child->skip = skip.substr(common + 1);
                split->pop.set(skip[common]);
                split->children.push_back(child);
                if (at_root) root_.store(split, std::memory_order_release);
                else __atomic_store_n(parent_child_ptr, split, __ATOMIC_RELEASE);
                retired_.retire(cur);
                elem_count_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }

            if (common == skip.size()) {
                kpos += common;
                char c = key[kpos];
                int idx;
                if (!cur->pop.find(c, &idx)) {
                    node_type* n = new node_type(*cur);
                    node_type* leaf = new node_type();
                    leaf->skip = key.substr(kpos + 1);
                    leaf->has_data = true;
                    leaf->data = value;
                    int new_idx = n->pop.set(c);
                    n->children.insert(n->children.begin() + new_idx, leaf);
                    if (at_root) root_.store(n, std::memory_order_release);
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

            node_type* split = new node_type();
            split->skip = skip.substr(0, common);
            node_type* old_child = new node_type(*cur);
            old_child->skip = (common + 1 < skip.size()) ? skip.substr(common + 1) : "";
            node_type* new_child = new node_type();
            new_child->skip = (kpos + common + 1 < key.size()) ? key.substr(kpos + common + 1) : "";
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
            if (at_root) root_.store(split, std::memory_order_release);
            else __atomic_store_n(parent_child_ptr, split, __ATOMIC_RELEASE);
            retired_.retire(cur);
            elem_count_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
    }

    // Erase result: what to do at this level
    enum class EraseAction { 
        NotFound,      // Key not found
        KeepNode,      // Node stays, just update child pointer
        ReplaceNode,   // Replace with new node
        RemoveNode     // Remove this node entirely
    };

    struct EraseResult {
        EraseAction action;
        node_type* replacement;  // For ReplaceNode
        node_type* to_retire;    // Node being retired (if any)
    };

    EraseResult erase_at(node_type* cur, const Key& key, size_t kpos) {
        const std::string& skip = cur->skip;
        
        if (!skip.empty()) {
            if (key.size() - kpos < skip.size()) return {EraseAction::NotFound, nullptr, nullptr};
            if (key.substr(kpos, skip.size()) != skip) return {EraseAction::NotFound, nullptr, nullptr};
            kpos += skip.size();
        }
        
        if (kpos == key.size()) {
            if (!cur->has_data) return {EraseAction::NotFound, nullptr, nullptr};
            
            int nchildren = cur->child_count();
            
            if (nchildren == 0) {
                return {EraseAction::RemoveNode, nullptr, cur};
            } else if (nchildren == 1) {
                char edge = cur->pop.first_char();
                node_type* child = cur->get_child(edge);
                node_type* merged = new node_type();
                merged->skip = cur->skip + std::string(1, edge) + child->skip;
                merged->has_data = child->has_data;
                merged->data = child->data;
                merged->pop = child->pop;
                merged->children = child->children;
                // Note: child is now "absorbed", retire it too
                retired_.retire(child);
                return {EraseAction::ReplaceNode, merged, cur};
            } else {
                node_type* n = new node_type(*cur);
                n->has_data = false;
                n->data = T{};
                return {EraseAction::ReplaceNode, n, cur};
            }
        }
        
        // Continue to child
        char c = key[kpos];
        int idx;
        if (!cur->pop.find(c, &idx)) return {EraseAction::NotFound, nullptr, nullptr};
        
        node_type* child = cur->children[idx];
        EraseResult child_result = erase_at(child, key, kpos + 1);
        
        if (child_result.action == EraseAction::NotFound) {
            return {EraseAction::NotFound, nullptr, nullptr};
        }
        
        if (child_result.to_retire) {
            retired_.retire(child_result.to_retire);
        }
        
        if (child_result.action == EraseAction::KeepNode) {
            // Child handled everything, we stay the same
            return {EraseAction::KeepNode, nullptr, nullptr};
        }
        
        if (child_result.action == EraseAction::ReplaceNode) {
            // Child was replaced - update our pointer (atomic, no copy needed!)
            cur->set_child(idx, child_result.replacement);
            return {EraseAction::KeepNode, nullptr, nullptr};
        }
        
        // child_result.action == EraseAction::RemoveNode
        // Child was removed - we need to update our structure
        int remaining = cur->child_count() - 1;
        
        if (remaining == 0 && !cur->has_data) {
            // We become empty too - remove
            return {EraseAction::RemoveNode, nullptr, cur};
        } else if (remaining == 1 && !cur->has_data) {
            // Merge with our remaining child
            char other_edge = 0;
            node_type* other_child = nullptr;
            for (size_t i = 0; i < cur->children.size(); i++) {
                if ((int)i != idx) {
                    other_child = cur->children[i];
                    int cnt = 0;
                    for (int w = 0; w < 4; w++) {
                        uint64_t bits = *(reinterpret_cast<const uint64_t*>(&cur->pop) + w);
                        while (bits) {
                            if (cnt == (int)i) {
                                other_edge = static_cast<char>((w << 6) | std::countr_zero(bits));
                                goto found;
                            }
                            bits &= bits - 1;
                            cnt++;
                        }
                    }
                    found:;
                    break;
                }
            }
            
            node_type* merged = new node_type();
            merged->skip = cur->skip + std::string(1, other_edge) + other_child->skip;
            merged->has_data = other_child->has_data;
            merged->data = other_child->data;
            merged->pop = other_child->pop;
            merged->children = other_child->children;
            retired_.retire(other_child);
            return {EraseAction::ReplaceNode, merged, cur};
        } else {
            // Remove child from our list (need to copy - structure changes)
            node_type* n = new node_type(*cur);
            n->pop.clear(c);
            n->children.erase(n->children.begin() + idx);
            return {EraseAction::ReplaceNode, n, cur};
        }
    }

    bool erase_impl(const Key& key) {
        node_type* root = root_.load(std::memory_order_acquire);
        EraseResult result = erase_at(root, key, 0);
        
        if (result.action == EraseAction::NotFound) return false;
        
        if (result.to_retire) {
            retired_.retire(result.to_retire);
        }
        
        if (result.action == EraseAction::ReplaceNode) {
            root_.store(result.replacement, std::memory_order_release);
        } else if (result.action == EraseAction::RemoveNode) {
            root_.store(new node_type(), std::memory_order_release);
        }
        // KeepNode means root already updated via atomic child pointer
        
        elem_count_.fetch_sub(1, std::memory_order_relaxed);
        return true;
    }
};

} // namespace gteitelbaum
