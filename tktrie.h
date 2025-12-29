#pragma once
// Thread-safe trie with configurable synchronization strategy
// Sync::READ  - RCU style: lock-free reads, COW mutations (read-heavy workloads)
// Sync::WRITE - per-node spinlocks with hand-over-hand locking (write-heavy workloads)

#include <array>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>
#include <thread>

namespace gteitelbaum {

enum class Sync { READ, WRITE };

// ============================================================================
// Common utilities
// ============================================================================

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

// ============================================================================
// Sync::READ implementation (RCU-style)
// ============================================================================

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
struct ReadNode {
    PopCount pop{};
    std::vector<ReadNode*> children{};
    std::string skip{};
    T data{};
    bool has_data{false};

    ReadNode() = default;
    ReadNode(const ReadNode& o) : pop(o.pop), children(o.children), skip(o.skip), data(o.data), has_data(o.has_data) {}
    
    ReadNode* get_child(char c) const {
        int idx;
        if (pop.find(c, &idx)) return __atomic_load_n(&children[idx], __ATOMIC_ACQUIRE);
        return nullptr;
    }
    int child_count() const { return pop.count(); }
    void set_child(int idx, ReadNode* child) {
        __atomic_store_n(&children[idx], child, __ATOMIC_RELEASE);
    }
};

template <typename Key, typename T>
class tktrie_read {
public:
    using node_type = ReadNode<T>;
    using size_type = std::size_t;

private:
    node_type* root_;
    std::atomic<size_type> elem_count_{0};
    RetireList retired_;
    std::mutex write_mutex_;

    node_type* get_root() const { return __atomic_load_n(&root_, __ATOMIC_ACQUIRE); }
    void set_root(node_type* n) { __atomic_store_n(&root_, n, __ATOMIC_RELEASE); }

public:
    tktrie_read() : root_(new node_type()) {}
    ~tktrie_read() { delete_tree(root_); }

    bool empty() const { return size() == 0; }
    size_type size() const { return elem_count_.load(std::memory_order_relaxed); }

    node_type* find(const Key& key) const {
        std::string_view kv(key);
        node_type* cur = get_root();
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
        std::lock_guard<std::mutex> lk(write_mutex_);
        return insert_impl(value.first, value.second);
    }

    bool erase(const Key& key) {
        std::lock_guard<std::mutex> lk(write_mutex_);
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

            if (kpos + common == key.size() && common == skip.size()) {
                if (cur->has_data) return false;
                node_type* n = new node_type(*cur);
                n->has_data = true;
                n->data = value;
                if (at_root) set_root(n);
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
                if (at_root) set_root(split);
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
            if (at_root) set_root(split);
            else __atomic_store_n(parent_child_ptr, split, __ATOMIC_RELEASE);
            retired_.retire(cur);
            elem_count_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
    }

    enum class EraseAction { NotFound, KeepNode, ReplaceNode, RemoveNode };
    struct EraseResult {
        EraseAction action;
        node_type* replacement;
        node_type* to_retire;
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
                retired_.retire(child);
                return {EraseAction::ReplaceNode, merged, cur};
            } else {
                node_type* n = new node_type(*cur);
                n->has_data = false;
                n->data = T{};
                return {EraseAction::ReplaceNode, n, cur};
            }
        }
        
        char c = key[kpos];
        int idx;
        if (!cur->pop.find(c, &idx)) return {EraseAction::NotFound, nullptr, nullptr};
        
        node_type* child = cur->children[idx];
        EraseResult child_result = erase_at(child, key, kpos + 1);
        
        if (child_result.action == EraseAction::NotFound) {
            return {EraseAction::NotFound, nullptr, nullptr};
        }
        
        if (child_result.to_retire) retired_.retire(child_result.to_retire);
        
        if (child_result.action == EraseAction::KeepNode) {
            return {EraseAction::KeepNode, nullptr, nullptr};
        }
        
        if (child_result.action == EraseAction::ReplaceNode) {
            cur->set_child(idx, child_result.replacement);
            return {EraseAction::KeepNode, nullptr, nullptr};
        }
        
        int remaining = cur->child_count() - 1;
        
        if (remaining == 0 && !cur->has_data) {
            return {EraseAction::RemoveNode, nullptr, cur};
        } else if (remaining == 1 && !cur->has_data) {
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
            node_type* n = new node_type(*cur);
            n->pop.clear(c);
            n->children.erase(n->children.begin() + idx);
            return {EraseAction::ReplaceNode, n, cur};
        }
    }

    bool erase_impl(const Key& key) {
        node_type* root = get_root();
        EraseResult result = erase_at(root, key, 0);
        
        if (result.action == EraseAction::NotFound) return false;
        
        if (result.to_retire) retired_.retire(result.to_retire);
        
        if (result.action == EraseAction::ReplaceNode) {
            set_root(result.replacement);
        } else if (result.action == EraseAction::RemoveNode) {
            set_root(new node_type());
        }
        
        elem_count_.fetch_sub(1, std::memory_order_relaxed);
        return true;
    }
};

// ============================================================================
// Sync::WRITE implementation (per-node spinlocks)
// ============================================================================

class RWSpinlock {
    std::atomic<int> state_{0};
    
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
    void read_unlock() { state_.fetch_sub(1, std::memory_order_release); }
    void write_lock() {
        int spins = 0;
        while (true) {
            int expected = 0;
            if (state_.compare_exchange_weak(expected, -1,
                std::memory_order_acquire, std::memory_order_relaxed)) return;
            backoff(++spins);
        }
    }
    void write_unlock() { state_.store(0, std::memory_order_release); }
    bool try_upgrade() {
        int expected = 1;
        return state_.compare_exchange_strong(expected, -1,
            std::memory_order_acquire, std::memory_order_relaxed);
    }
};

template <typename T>
struct alignas(64) WriteNode {
    mutable RWSpinlock lock_;
    PopCount pop{};
    std::vector<WriteNode*> children{};
    WriteNode* parent{nullptr};
    std::string skip{};
    T data{};
    char parent_edge{'\0'};
    bool has_data{false};

    WriteNode() = default;
    ~WriteNode() { for (auto* c : children) delete c; }
    WriteNode(const WriteNode&) = delete;
    WriteNode& operator=(const WriteNode&) = delete;

    WriteNode* get_child(char c) const {
        int idx;
        if (pop.find(c, &idx)) return children[idx];
        return nullptr;
    }
    
    std::string_view get_skip_view() const { return skip; }
    
    void read_lock() const { lock_.read_lock(); }
    void read_unlock() const { lock_.read_unlock(); }
    void write_lock() { lock_.write_lock(); }
    void write_unlock() { lock_.write_unlock(); }
    bool try_upgrade() { return lock_.try_upgrade(); }
};

template <typename Key, typename T>
class tktrie_write {
public:
    using node_type = WriteNode<T>;
    using size_type = std::size_t;

private:
    node_type head;
    std::atomic<size_type> elem_count{0};

public:
    tktrie_write() = default;
    ~tktrie_write() = default;

    bool empty() const { return size() == 0; }
    size_type size() const { return elem_count.load(std::memory_order_relaxed); }

    bool contains(const Key& key) { return find_internal(key) != nullptr; }

    bool insert(const std::pair<const Key, T>& value) {
        return insert_internal(value.first, value.second).second;
    }

    bool erase(const Key& key) {
        return remove_internal(key);
    }

private:
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

    std::pair<node_type*, bool> insert_internal(const Key& key, const T& value) {
        std::string_view kv(key);
        node_type* cur = &head;
        cur->read_lock();

        while (true) {
            std::string_view skip = cur->get_skip_view();
            size_t common = 0;
            size_t kv_len = kv.size();
            size_t skip_len = skip.size();
            
            while (common < skip_len && common < kv_len && skip[common] == kv[common]) 
                ++common;

            // Case 1: Exact match
            if (common == kv_len && common == skip_len) {
                if (!cur->try_upgrade()) {
                    cur->read_unlock();
                    cur->write_lock();
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
                return {cur, was_new};
            }

            // Case 2: Key is prefix - split
            if (common == kv_len) {
                std::string new_cur_skip = std::string(skip.substr(0, common));
                std::string child_skip = std::string(skip.substr(common + 1));
                char edge_char = skip[common];
                
                auto* child = new node_type();
                child->skip = std::move(child_skip);
                child->parent_edge = edge_char;
                child->has_data = true;
                child->data = value;
                
                PopCount new_cur_pop{};
                new_cur_pop.set(edge_char);
                
                cur->read_unlock();
                cur->write_lock();
                
                std::string_view skip2 = cur->get_skip_view();
                size_t common2 = 0;
                while (common2 < skip2.size() && common2 < kv.size() && 
                       skip2[common2] == kv[common2]) ++common2;
                       
                if (common2 != kv.size()) {
                    cur->write_unlock();
                    delete child;
                    return insert_internal(key, value);
                }
                
                std::swap(child->has_data, cur->has_data);
                std::swap(child->data, cur->data);
                child->children.swap(cur->children);
                std::swap(child->pop, cur->pop);
                child->parent = cur;
                for (auto* gc : child->children) if (gc) gc->parent = child;
                
                cur->skip = std::move(new_cur_skip);
                cur->children.clear();
                cur->children.push_back(child);
                cur->pop = new_cur_pop;
                
                elem_count.fetch_add(1, std::memory_order_relaxed);
                cur->write_unlock();
                return {cur, true};
            }

            // Case 3: Skip fully matched, continue
            if (common == skip_len) {
                kv.remove_prefix(common);
                char c = kv[0];
                node_type* child = cur->get_child(c);
                
                if (child) {
                    child->read_lock();
                    cur->read_unlock();
                    cur = child;
                    kv.remove_prefix(1);
                    continue;
                }
                
                auto* newc = new node_type();
                newc->skip = std::string(kv.substr(1));
                newc->has_data = true;
                newc->data = value;
                newc->parent = cur;
                newc->parent_edge = c;
                
                cur->read_unlock();
                cur->write_lock();
                child = cur->get_child(c);
                if (child) {
                    cur->write_unlock();
                    delete newc;
                    return insert_internal(key, value);
                }
                
                int idx = cur->pop.set(c);
                cur->children.insert(cur->children.begin() + idx, newc);
                
                elem_count.fetch_add(1, std::memory_order_relaxed);
                cur->write_unlock();
                return {newc, true};
            }

            // Case 4: Mismatch - split
            std::string old_child_skip = std::string(skip.substr(common + 1));
            std::string new_child_skip = std::string(kv.substr(common + 1));
            std::string new_cur_skip = std::string(skip.substr(0, common));
            char old_edge = skip[common];
            char new_edge = kv[common];
            
            auto* old_child = new node_type();
            old_child->skip = std::move(old_child_skip);
            old_child->parent_edge = old_edge;
            
            auto* new_child = new node_type();
            new_child->skip = std::move(new_child_skip);
            new_child->has_data = true;
            new_child->data = value;
            new_child->parent_edge = new_edge;
            
            PopCount new_cur_pop{};
            int i1 = new_cur_pop.set(old_edge);
            int i2 = new_cur_pop.set(new_edge);
            
            std::vector<node_type*> new_cur_children;
            new_cur_children.reserve(2);
            if (i1 <= i2) {
                new_cur_children.push_back(old_child);
                new_cur_children.push_back(new_child);
            } else {
                new_cur_children.push_back(new_child);
                new_cur_children.push_back(old_child);
            }
            
            cur->read_unlock();
            cur->write_lock();
            
            std::string_view skip2 = cur->get_skip_view();
            size_t common2 = 0;
            while (common2 < skip2.size() && common2 < kv.size() && 
                   skip2[common2] == kv[common2]) ++common2;
                   
            if (common2 == skip2.size()) {
                cur->write_unlock();
                delete old_child;
                delete new_child;
                return insert_internal(key, value);
            }
            
            std::swap(old_child->has_data, cur->has_data);
            std::swap(old_child->data, cur->data);
            old_child->children.swap(cur->children);
            std::swap(old_child->pop, cur->pop);
            old_child->parent = cur;
            new_child->parent = cur;
            for (auto* gc : old_child->children) if (gc) gc->parent = old_child;
            
            cur->skip = std::move(new_cur_skip);
            cur->children.swap(new_cur_children);
            cur->pop = new_cur_pop;
            
            elem_count.fetch_add(1, std::memory_order_relaxed);
            cur->write_unlock();
            return {new_child, true};
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
                
                T old_data = std::move(cur->data);
                cur->has_data = false;
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

// ============================================================================
// Unified tktrie template that selects implementation based on Sync
// ============================================================================

template <typename Key, typename T, Sync S = Sync::READ>
class tktrie : public std::conditional_t<S == Sync::READ, tktrie_read<Key, T>, tktrie_write<Key, T>> {
    using base = std::conditional_t<S == Sync::READ, tktrie_read<Key, T>, tktrie_write<Key, T>>;
public:
    using base::base;
};

} // namespace gteitelbaum
