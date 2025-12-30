#pragma once
// Thread-safe trie with multi-level skip compression
// Proper COW: copy node + copy parent, update root atomically

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
    int count() const {
        int n = 0;
        for (auto b : bits) n += std::popcount(b);
        return n;
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
struct Segment {
    std::string skip;
    T data{};
    bool has_data{false};
    bool use_pop{false};
    
    Segment() = default;
    Segment(std::string s, T d, bool hd, bool up) 
        : skip(std::move(s)), data(std::move(d)), has_data(hd), use_pop(up) {}
};

template <typename T>
struct Node {
    PopCount pop{};
    std::vector<Node*> children{};
    std::vector<Segment<T>> segments{};

    Node() = default;
    Node(const Node& o) : pop(o.pop), children(o.children), segments(o.segments) {}
    
    Node* get_child(char c) const {
        int idx;
        if (pop.find(c, &idx)) {
            return __atomic_load_n(&children[idx], __ATOMIC_ACQUIRE);
        }
        return nullptr;
    }
    
    bool get_child_idx(char c, int* idx) const {
        return pop.find(c, idx);
    }
};

template <typename Key, typename T> class tktrie;

template <typename Key, typename T>
class tktrie_iterator {
public:
    using value_type = std::pair<Key, T>;
private:
    Key key_;
    T data_;
    bool valid_{false};
public:
    tktrie_iterator() = default;
    tktrie_iterator(const Key& k, const T& d) : key_(k), data_(d), valid_(true) {}
    static tktrie_iterator end_iterator() { return tktrie_iterator(); }
    const Key& key() const { return key_; }
    T& value() { return data_; }
    value_type operator*() const { return {key_, data_}; }
    bool operator==(const tktrie_iterator& o) const {
        if (!valid_ && !o.valid_) return true;
        if (!valid_ || !o.valid_) return false;
        return key_ == o.key_;
    }
    bool operator!=(const tktrie_iterator& o) const { return !(*this == o); }
    bool valid() const { return valid_; }
};

template <typename Key, typename T>
class tktrie {
public:
    using node_type = Node<T>;
    using size_type = std::size_t;
    using iterator = tktrie_iterator<Key, T>;

private:
    node_type* root_;
    std::atomic<size_type> elem_count_{0};
    RetireList retired_;
    mutable std::mutex write_mutex_;

    node_type* get_root() const { return __atomic_load_n(&root_, __ATOMIC_ACQUIRE); }
    void set_root(node_type* n) { __atomic_store_n(&root_, n, __ATOMIC_RELEASE); }

public:
    tktrie() : root_(new node_type()) {}
    ~tktrie() { delete_tree(root_); }

    bool empty() const { return size() == 0; }
    size_type size() const { return elem_count_.load(std::memory_order_relaxed); }

    bool contains(const Key& key) const {
        std::string_view kv(key);
        node_type* cur = get_root();
        
        while (cur) {
            bool branched = false;
            for (const auto& seg : cur->segments) {
                if (kv.size() < seg.skip.size() || kv.substr(0, seg.skip.size()) != seg.skip)
                    return false;
                kv.remove_prefix(seg.skip.size());
                
                if (kv.empty()) return seg.has_data;
                
                if (seg.use_pop) {
                    cur = cur->get_child(kv[0]);
                    kv.remove_prefix(1);
                    branched = true;
                    break;
                }
            }
            if (!branched) {
                if (kv.empty()) return false;
                cur = cur->get_child(kv[0]);
                kv.remove_prefix(1);
            }
        }
        return false;
    }

    iterator find(const Key& key) const {
        std::string_view kv(key);
        node_type* cur = get_root();
        
        while (cur) {
            bool branched = false;
            for (const auto& seg : cur->segments) {
                if (kv.size() < seg.skip.size() || kv.substr(0, seg.skip.size()) != seg.skip)
                    return end();
                kv.remove_prefix(seg.skip.size());
                
                if (kv.empty()) {
                    if (seg.has_data) return iterator(key, seg.data);
                    return end();
                }
                
                if (seg.use_pop) {
                    cur = cur->get_child(kv[0]);
                    kv.remove_prefix(1);
                    branched = true;
                    break;
                }
            }
            if (!branched) {
                if (kv.empty()) return end();
                cur = cur->get_child(kv[0]);
                kv.remove_prefix(1);
            }
        }
        return end();
    }

    iterator end() const { return iterator::end_iterator(); }

    std::pair<iterator, bool> insert(const std::pair<const Key, T>& value) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        bool inserted = insert_impl(value.first, value.second);
        return {iterator(value.first, value.second), inserted};
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

    // Commit: copy parent to point to new_node, update root
    void commit(node_type* parent, int child_idx, node_type* new_node, node_type* old_node) {
        if (!parent) {
            node_type* old_root = get_root();
            set_root(new_node);
            retired_.retire(old_root);
        } else {
            node_type* new_parent = new node_type(*parent);
            new_parent->children[child_idx] = new_node;
            retired_.retire(old_node);
            set_root(new_parent);
            retired_.retire(parent);
        }
    }

    bool insert_impl(const Key& key, const T& value) {
        std::string_view kv(key);
        node_type* parent = nullptr;
        int parent_idx = -1;
        node_type* cur = get_root();
        
        while (true) {
            for (size_t seg_idx = 0; seg_idx < cur->segments.size(); seg_idx++) {
                const auto& seg = cur->segments[seg_idx];
                
                size_t common = 0;
                while (common < seg.skip.size() && common < kv.size() &&
                       seg.skip[common] == kv[common]) ++common;
                
                if (common < seg.skip.size()) {
                    // Split segment
                    node_type* n = new node_type();
                    for (size_t i = 0; i < seg_idx; i++)
                        n->segments.push_back(cur->segments[i]);
                    
                    if (common == kv.size()) {
                        // Key ends at split
                        n->segments.push_back(Segment<T>(
                            seg.skip.substr(0, common), value, true, true));
                        
                        node_type* suffix = new node_type();
                        suffix->segments.push_back(Segment<T>(
                            seg.skip.substr(common + 1), seg.data, seg.has_data, seg.use_pop));
                        for (size_t i = seg_idx + 1; i < cur->segments.size(); i++)
                            suffix->segments.push_back(cur->segments[i]);
                        suffix->pop = cur->pop;
                        suffix->children = cur->children;
                        
                        n->pop.set(seg.skip[common]);
                        n->children.push_back(suffix);
                    } else {
                        // Both continue
                        n->segments.push_back(Segment<T>(
                            seg.skip.substr(0, common), T{}, false, true));
                        
                        node_type* old_suffix = new node_type();
                        old_suffix->segments.push_back(Segment<T>(
                            seg.skip.substr(common + 1), seg.data, seg.has_data, seg.use_pop));
                        for (size_t i = seg_idx + 1; i < cur->segments.size(); i++)
                            old_suffix->segments.push_back(cur->segments[i]);
                        old_suffix->pop = cur->pop;
                        old_suffix->children = cur->children;
                        
                        node_type* new_child = new node_type();
                        new_child->segments.push_back(Segment<T>(
                            std::string(kv.substr(common + 1)), value, true, false));
                        
                        char old_edge = seg.skip[common];
                        char new_edge = kv[common];
                        if (old_edge < new_edge) {
                            n->pop.set(old_edge); n->pop.set(new_edge);
                            n->children.push_back(old_suffix);
                            n->children.push_back(new_child);
                        } else {
                            n->pop.set(new_edge); n->pop.set(old_edge);
                            n->children.push_back(new_child);
                            n->children.push_back(old_suffix);
                        }
                    }
                    
                    commit(parent, parent_idx, n, cur);
                    elem_count_.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }
                
                kv.remove_prefix(common);
                
                if (kv.empty()) {
                    if (seg.has_data) return false;
                    node_type* n = new node_type(*cur);
                    n->segments[seg_idx].has_data = true;
                    n->segments[seg_idx].data = value;
                    commit(parent, parent_idx, n, cur);
                    elem_count_.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }
                
                if (seg.use_pop) {
                    char c = kv[0];
                    int idx;
                    if (cur->get_child_idx(c, &idx)) {
                        parent = cur;
                        parent_idx = idx;
                        cur = cur->children[idx];
                        kv.remove_prefix(1);
                        goto next_node;
                    }
                    
                    node_type* n = new node_type(*cur);
                    node_type* child = new node_type();
                    child->segments.push_back(Segment<T>(
                        std::string(kv.substr(1)), value, true, false));
                    int new_idx = n->pop.set(c);
                    n->children.insert(n->children.begin() + new_idx, child);
                    commit(parent, parent_idx, n, cur);
                    elem_count_.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }
            }
            
            // After all segments
            if (kv.empty()) return false;
            
            if (cur->pop.count() == 0) {
                node_type* n = new node_type(*cur);
                n->segments.push_back(Segment<T>(std::string(kv), value, true, false));
                commit(parent, parent_idx, n, cur);
                elem_count_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
            
            {
                char c = kv[0];
                int idx;
                if (cur->get_child_idx(c, &idx)) {
                    parent = cur;
                    parent_idx = idx;
                    cur = cur->children[idx];
                    kv.remove_prefix(1);
                } else {
                    node_type* n = new node_type(*cur);
                    node_type* child = new node_type();
                    child->segments.push_back(Segment<T>(
                        std::string(kv.substr(1)), value, true, false));
                    int new_idx = n->pop.set(c);
                    n->children.insert(n->children.begin() + new_idx, child);
                    commit(parent, parent_idx, n, cur);
                    elem_count_.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }
            }
            next_node:;
        }
    }

    bool erase_impl(const Key& key) {
        std::string_view kv(key);
        node_type* parent = nullptr;
        int parent_idx = -1;
        node_type* cur = get_root();

        while (cur) {
            bool branched = false;
            for (size_t seg_idx = 0; seg_idx < cur->segments.size(); seg_idx++) {
                const auto& seg = cur->segments[seg_idx];
                
                if (kv.size() < seg.skip.size() || kv.substr(0, seg.skip.size()) != seg.skip)
                    return false;
                
                kv.remove_prefix(seg.skip.size());
                
                if (kv.empty()) {
                    if (!seg.has_data) return false;
                    node_type* n = new node_type(*cur);
                    n->segments[seg_idx].has_data = false;
                    n->segments[seg_idx].data = T{};
                    commit(parent, parent_idx, n, cur);
                    elem_count_.fetch_sub(1, std::memory_order_relaxed);
                    return true;
                }
                
                if (seg.use_pop) {
                    char c = kv[0];
                    int idx;
                    if (!cur->get_child_idx(c, &idx)) return false;
                    parent = cur;
                    parent_idx = idx;
                    cur = cur->children[idx];
                    kv.remove_prefix(1);
                    branched = true;
                    break;
                }
            }
            
            if (!branched) {
                if (kv.empty()) return false;
                char c = kv[0];
                int idx;
                if (!cur->get_child_idx(c, &idx)) return false;
                parent = cur;
                parent_idx = idx;
                cur = cur->children[idx];
                kv.remove_prefix(1);
            }
        }
        return false;
    }
};

} // namespace gteitelbaum
