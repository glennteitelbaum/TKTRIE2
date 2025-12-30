#pragma once
// Thread-safe trie with type-based specialization

#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace gteitelbaum {

template <typename U>
U do_byteswap(U inp) {
    if constexpr (std::endian::native == std::endian::big) return inp;
    if constexpr (sizeof(U) == 1) return inp;
    else if constexpr (sizeof(U) == 2) return static_cast<U>(__builtin_bswap16(static_cast<uint16_t>(inp)));
    else if constexpr (sizeof(U) == 4) return static_cast<U>(__builtin_bswap32(static_cast<uint32_t>(inp)));
    else if constexpr (sizeof(U) == 8) return static_cast<U>(__builtin_bswap64(static_cast<uint64_t>(inp)));
}

template <typename Key> struct tktrie_traits;

template <> struct tktrie_traits<std::string> {
    static constexpr size_t fixed_len = 0;
    static std::string_view to_bytes(const std::string& k) { return k; }
    static std::string from_bytes(std::string_view s) { return std::string(s); }
};

template <typename T> requires std::is_integral_v<T>
struct tktrie_traits<T> {
    static constexpr size_t fixed_len = sizeof(T);
    using unsigned_type = std::make_unsigned_t<T>;
    static std::string to_bytes(T k) {
        char buf[sizeof(T)];
        unsigned_type sortable;
        if constexpr (std::is_signed_v<T>) {
            sortable = static_cast<unsigned_type>(k) + (unsigned_type{1} << (sizeof(T) * 8 - 1));
        } else { sortable = k; }
        unsigned_type be = do_byteswap(sortable);
        std::memcpy(buf, &be, sizeof(T));
        return std::string(buf, sizeof(T));
    }
    static T from_bytes(std::string_view s) {
        unsigned_type be;
        std::memcpy(&be, s.data(), sizeof(T));
        unsigned_type sortable = do_byteswap(be);
        if constexpr (std::is_signed_v<T>) {
            return static_cast<T>(sortable - (unsigned_type{1} << (sizeof(T) * 8 - 1)));
        } else { return static_cast<T>(sortable); }
    }
};

class PopCount {
    uint64_t bits[4]{};
public:
    bool find(unsigned char c, int* idx) const {
        int word = c >> 6, bit = c & 63;
        uint64_t mask = 1ULL << bit;
        if (!(bits[word] & mask)) return false;
        *idx = std::popcount(bits[word] & (mask - 1));
        for (int w = 0; w < word; ++w) *idx += std::popcount(bits[w]);
        return true;
    }
    int set(unsigned char c) {
        int word = c >> 6, bit = c & 63;
        uint64_t mask = 1ULL << bit;
        int idx = std::popcount(bits[word] & (mask - 1));
        for (int w = 0; w < word; ++w) idx += std::popcount(bits[w]);
        bits[word] |= mask;
        return idx;
    }
    int count() const { int n = 0; for (auto b : bits) n += std::popcount(b); return n; }
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

template <typename Key, typename T> class tktrie;

template <typename Key, typename T>
class tktrie_iterator {
    Key key_; T data_; bool valid_{false};
public:
    using value_type = std::pair<Key, T>;
    tktrie_iterator() = default;
    tktrie_iterator(const Key& k, const T& d) : key_(k), data_(d), valid_(true) {}
    static tktrie_iterator end_iterator() { return tktrie_iterator(); }
    const Key& key() const { return key_; }
    T& value() { return data_; }
    value_type operator*() const { return {key_, data_}; }
    bool operator==(const tktrie_iterator& o) const {
        if (!valid_ && !o.valid_) return true;
        return valid_ && o.valid_ && key_ == o.key_;
    }
    bool operator!=(const tktrie_iterator& o) const { return !(*this == o); }
    bool valid() const { return valid_; }
};

template <typename T> struct SegmentVL {
    std::string skip; T data{}; bool has_data{false}; bool use_pop{false};
    SegmentVL() = default;
    SegmentVL(std::string s, T d, bool hd, bool up) : skip(std::move(s)), data(std::move(d)), has_data(hd), use_pop(up) {}
};

template <typename T> struct NodeVL {
    PopCount pop{}; std::vector<NodeVL*> children{}; std::vector<SegmentVL<T>> segments{};
    NodeVL() = default;
    NodeVL(const NodeVL& o) : pop(o.pop), children(o.children), segments(o.segments) {}
    NodeVL* get_child(unsigned char c) const { int idx; return pop.find(c, &idx) ? children[idx] : nullptr; }
    bool get_child_idx(unsigned char c, int* idx) const { return pop.find(c, idx); }
};

template <typename T> struct NodeFL {
    PopCount pop{}; std::vector<NodeFL*> children{}; std::string skip{}; T data{}; bool has_data{false};
    NodeFL() = default;
    NodeFL(const NodeFL& o) : pop(o.pop), children(o.children), skip(o.skip), data(o.data), has_data(o.has_data) {}
    NodeFL* get_child(unsigned char c) const { int idx; return pop.find(c, &idx) ? children[idx] : nullptr; }
    bool get_child_idx(unsigned char c, int* idx) const { return pop.find(c, idx); }
};

template <typename Key, typename T>
class tktrie {
public:
    using Traits = tktrie_traits<Key>;
    static constexpr bool is_fixed = (Traits::fixed_len > 0);
    using node_type = std::conditional_t<is_fixed, NodeFL<T>, NodeVL<T>>;
    using size_type = std::size_t;
    using iterator = tktrie_iterator<Key, T>;

private:
    node_type* root_;
    std::atomic<size_type> elem_count_{0};
    RetireList retired_;
    mutable std::mutex write_mutex_;
    struct PathEntry { node_type* node; int child_idx; };

    node_type* get_root() const { return __atomic_load_n(&root_, __ATOMIC_ACQUIRE); }
    void set_root(node_type* n) { __atomic_store_n(&root_, n, __ATOMIC_RELEASE); }

    void commit_path(std::vector<PathEntry>& path, node_type* new_node, node_type* old_node) {
        retired_.retire(old_node);
        node_type* child = new_node;
        for (int i = (int)path.size() - 1; i >= 0; i--) {
            node_type* new_parent = new node_type(*path[i].node);
            new_parent->children[path[i].child_idx] = child;
            retired_.retire(path[i].node);
            child = new_parent;
        }
        set_root(child);
    }

    void delete_tree(node_type* n) {
        if (!n) return;
        for (auto* c : n->children) delete_tree(c);
        delete n;
    }

public:
    tktrie() : root_(new node_type()) {}
    ~tktrie() { delete_tree(root_); }
    bool empty() const { return size() == 0; }
    size_type size() const { return elem_count_.load(std::memory_order_relaxed); }

    bool contains(const Key& key) const {
        if constexpr (is_fixed) return contains_fixed(key);
        else return contains_variable(key);
    }
    iterator find(const Key& key) const {
        if constexpr (is_fixed) return find_fixed(key);
        else return find_variable(key);
    }
    iterator end() const { return iterator::end_iterator(); }
    std::pair<iterator, bool> insert(const std::pair<const Key, T>& value) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        bool ins; if constexpr (is_fixed) ins = insert_fixed(value.first, value.second);
        else ins = insert_variable(value.first, value.second);
        return {iterator(value.first, value.second), ins};
    }
    bool erase(const Key& key) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        if constexpr (is_fixed) return erase_fixed(key);
        else return erase_variable(key);
    }

private:
    // ==================== VARIABLE-LENGTH ====================
    bool contains_variable(const Key& key) const {
        std::string_view kv = Traits::to_bytes(key);
        NodeVL<T>* cur = get_root();
        while (cur) {
            bool br = false;
            for (const auto& seg : cur->segments) {
                if (kv.size() < seg.skip.size() || kv.substr(0, seg.skip.size()) != seg.skip) return false;
                kv.remove_prefix(seg.skip.size());
                if (kv.empty()) return seg.has_data;
                if (seg.use_pop) { cur = cur->get_child((unsigned char)kv[0]); kv.remove_prefix(1); br = true; break; }
            }
            if (!br) { if (kv.empty()) return false; cur = cur->get_child((unsigned char)kv[0]); kv.remove_prefix(1); }
        }
        return false;
    }

    iterator find_variable(const Key& key) const {
        std::string_view kv = Traits::to_bytes(key);
        NodeVL<T>* cur = get_root();
        while (cur) {
            bool br = false;
            for (const auto& seg : cur->segments) {
                if (kv.size() < seg.skip.size() || kv.substr(0, seg.skip.size()) != seg.skip) return end();
                kv.remove_prefix(seg.skip.size());
                if (kv.empty()) return seg.has_data ? iterator(key, seg.data) : end();
                if (seg.use_pop) { cur = cur->get_child((unsigned char)kv[0]); kv.remove_prefix(1); br = true; break; }
            }
            if (!br) { if (kv.empty()) return end(); cur = cur->get_child((unsigned char)kv[0]); kv.remove_prefix(1); }
        }
        return end();
    }

    bool insert_variable(const Key& key, const T& value) {
        std::string_view kv = Traits::to_bytes(key);
        std::vector<PathEntry> path;
        NodeVL<T>* cur = get_root();
        while (true) {
            for (size_t si = 0; si < cur->segments.size(); si++) {
                const auto& seg = cur->segments[si];
                size_t common = 0;
                while (common < seg.skip.size() && common < kv.size() && seg.skip[common] == kv[common]) ++common;
                if (common < seg.skip.size()) {
                    NodeVL<T>* n = new NodeVL<T>();
                    for (size_t i = 0; i < si; i++) n->segments.push_back(cur->segments[i]);
                    if (common == kv.size()) {
                        n->segments.push_back(SegmentVL<T>(seg.skip.substr(0, common), value, true, true));
                        NodeVL<T>* suf = new NodeVL<T>();
                        suf->segments.push_back(SegmentVL<T>(seg.skip.substr(common + 1), seg.data, seg.has_data, seg.use_pop));
                        for (size_t i = si + 1; i < cur->segments.size(); i++) suf->segments.push_back(cur->segments[i]);
                        suf->pop = cur->pop; suf->children = cur->children;
                        n->pop.set((unsigned char)seg.skip[common]); n->children.push_back(suf);
                    } else {
                        n->segments.push_back(SegmentVL<T>(seg.skip.substr(0, common), T{}, false, true));
                        NodeVL<T>* os = new NodeVL<T>();
                        os->segments.push_back(SegmentVL<T>(seg.skip.substr(common + 1), seg.data, seg.has_data, seg.use_pop));
                        for (size_t i = si + 1; i < cur->segments.size(); i++) os->segments.push_back(cur->segments[i]);
                        os->pop = cur->pop; os->children = cur->children;
                        NodeVL<T>* nc = new NodeVL<T>();
                        nc->segments.push_back(SegmentVL<T>(std::string(kv.substr(common + 1)), value, true, false));
                        unsigned char oe = (unsigned char)seg.skip[common], ne = (unsigned char)kv[common];
                        if (oe < ne) { n->pop.set(oe); n->pop.set(ne); n->children.push_back(os); n->children.push_back(nc); }
                        else { n->pop.set(ne); n->pop.set(oe); n->children.push_back(nc); n->children.push_back(os); }
                    }
                    commit_path(path, n, cur);
                    elem_count_.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }
                kv.remove_prefix(common);
                if (kv.empty()) {
                    if (seg.has_data) return false;
                    NodeVL<T>* n = new NodeVL<T>(*cur);
                    n->segments[si].has_data = true; n->segments[si].data = value;
                    commit_path(path, n, cur);
                    elem_count_.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }
                if (seg.use_pop) {
                    unsigned char c = (unsigned char)kv[0]; int idx;
                    if (cur->get_child_idx(c, &idx)) { path.push_back({cur, idx}); cur = cur->children[idx]; kv.remove_prefix(1); goto next_vl; }
                    NodeVL<T>* n = new NodeVL<T>(*cur);
                    NodeVL<T>* ch = new NodeVL<T>();
                    ch->segments.push_back(SegmentVL<T>(std::string(kv.substr(1)), value, true, false));
                    int ni = n->pop.set(c); n->children.insert(n->children.begin() + ni, ch);
                    commit_path(path, n, cur);
                    elem_count_.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }
            }
            if (kv.empty()) return false;
            if (cur->pop.count() == 0) {
                NodeVL<T>* n = new NodeVL<T>(*cur);
                n->segments.push_back(SegmentVL<T>(std::string(kv), value, true, false));
                commit_path(path, n, cur);
                elem_count_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
            { unsigned char c = (unsigned char)kv[0]; int idx;
              if (cur->get_child_idx(c, &idx)) { path.push_back({cur, idx}); cur = cur->children[idx]; kv.remove_prefix(1); }
              else {
                  NodeVL<T>* n = new NodeVL<T>(*cur);
                  NodeVL<T>* ch = new NodeVL<T>();
                  ch->segments.push_back(SegmentVL<T>(std::string(kv.substr(1)), value, true, false));
                  int ni = n->pop.set(c); n->children.insert(n->children.begin() + ni, ch);
                  commit_path(path, n, cur);
                  elem_count_.fetch_add(1, std::memory_order_relaxed);
                  return true;
              }
            }
            next_vl:;
        }
    }

    bool erase_variable(const Key& key) {
        std::string_view kv = Traits::to_bytes(key);
        std::vector<PathEntry> path;
        NodeVL<T>* cur = get_root();
        while (cur) {
            bool br = false;
            for (size_t si = 0; si < cur->segments.size(); si++) {
                const auto& seg = cur->segments[si];
                if (kv.size() < seg.skip.size() || kv.substr(0, seg.skip.size()) != seg.skip) return false;
                kv.remove_prefix(seg.skip.size());
                if (kv.empty()) {
                    if (!seg.has_data) return false;
                    NodeVL<T>* n = new NodeVL<T>(*cur);
                    n->segments[si].has_data = false; n->segments[si].data = T{};
                    commit_path(path, n, cur);
                    elem_count_.fetch_sub(1, std::memory_order_relaxed);
                    return true;
                }
                if (seg.use_pop) {
                    unsigned char c = (unsigned char)kv[0]; int idx;
                    if (!cur->get_child_idx(c, &idx)) return false;
                    path.push_back({cur, idx}); cur = cur->children[idx]; kv.remove_prefix(1); br = true; break;
                }
            }
            if (!br) {
                if (kv.empty()) return false;
                unsigned char c = (unsigned char)kv[0]; int idx;
                if (!cur->get_child_idx(c, &idx)) return false;
                path.push_back({cur, idx}); cur = cur->children[idx]; kv.remove_prefix(1);
            }
        }
        return false;
    }

    // ==================== FIXED-LENGTH ====================
    bool contains_fixed(const Key& key) const {
        std::string kv = Traits::to_bytes(key);
        std::string_view kvv(kv);
        NodeFL<T>* cur = get_root();
        while (cur) {
            if (!cur->skip.empty()) {
                if (kvv.size() < cur->skip.size() || kvv.substr(0, cur->skip.size()) != cur->skip) return false;
                kvv.remove_prefix(cur->skip.size());
            }
            if (kvv.empty()) return cur->has_data;
            unsigned char c = (unsigned char)kvv[0]; int idx;
            if (!cur->pop.find(c, &idx)) return false;
            cur = cur->children[idx]; kvv.remove_prefix(1);
        }
        return false;
    }

    iterator find_fixed(const Key& key) const {
        std::string kv = Traits::to_bytes(key);
        std::string_view kvv(kv);
        NodeFL<T>* cur = get_root();
        while (cur) {
            if (!cur->skip.empty()) {
                if (kvv.size() < cur->skip.size() || kvv.substr(0, cur->skip.size()) != cur->skip) return end();
                kvv.remove_prefix(cur->skip.size());
            }
            if (kvv.empty()) return cur->has_data ? iterator(key, cur->data) : end();
            unsigned char c = (unsigned char)kvv[0]; int idx;
            if (!cur->pop.find(c, &idx)) return end();
            cur = cur->children[idx]; kvv.remove_prefix(1);
        }
        return end();
    }

    bool insert_fixed(const Key& key, const T& value) {
        std::string kv = Traits::to_bytes(key);
        std::string_view kvv(kv);
        std::vector<PathEntry> path;
        NodeFL<T>* cur = get_root();
        while (true) {
            size_t common = 0;
            while (common < cur->skip.size() && common < kvv.size() && cur->skip[common] == kvv[common]) ++common;
            if (common < cur->skip.size()) {
                NodeFL<T>* n = new NodeFL<T>();
                n->skip = cur->skip.substr(0, common);
                NodeFL<T>* os = new NodeFL<T>(*cur);
                os->skip = cur->skip.substr(common + 1);
                NodeFL<T>* nc = new NodeFL<T>();
                nc->skip = std::string(kvv.substr(common + 1));
                nc->has_data = true; nc->data = value;
                unsigned char oe = (unsigned char)cur->skip[common], ne = (unsigned char)kvv[common];
                if (oe < ne) { n->pop.set(oe); n->pop.set(ne); n->children.push_back(os); n->children.push_back(nc); }
                else { n->pop.set(ne); n->pop.set(oe); n->children.push_back(nc); n->children.push_back(os); }
                commit_path(path, n, cur);
                elem_count_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
            kvv.remove_prefix(common);
            if (kvv.empty()) {
                if (cur->has_data) return false;
                NodeFL<T>* n = new NodeFL<T>(*cur);
                n->has_data = true; n->data = value;
                commit_path(path, n, cur);
                elem_count_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
            unsigned char c = (unsigned char)kvv[0]; int idx;
            if (!cur->pop.find(c, &idx)) {
                NodeFL<T>* n = new NodeFL<T>(*cur);
                NodeFL<T>* ch = new NodeFL<T>();
                ch->skip = std::string(kvv.substr(1));
                ch->has_data = true; ch->data = value;
                int ni = n->pop.set(c); n->children.insert(n->children.begin() + ni, ch);
                commit_path(path, n, cur);
                elem_count_.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
            path.push_back({cur, idx});
            cur = cur->children[idx]; kvv.remove_prefix(1);
        }
    }

    bool erase_fixed(const Key& key) {
        std::string kv = Traits::to_bytes(key);
        std::string_view kvv(kv);
        std::vector<PathEntry> path;
        NodeFL<T>* cur = get_root();
        while (cur) {
            if (!cur->skip.empty()) {
                if (kvv.size() < cur->skip.size() || kvv.substr(0, cur->skip.size()) != cur->skip) return false;
                kvv.remove_prefix(cur->skip.size());
            }
            if (kvv.empty()) {
                if (!cur->has_data) return false;
                NodeFL<T>* n = new NodeFL<T>(*cur);
                n->has_data = false; n->data = T{};
                commit_path(path, n, cur);
                elem_count_.fetch_sub(1, std::memory_order_relaxed);
                return true;
            }
            unsigned char c = (unsigned char)kvv[0]; int idx;
            if (!cur->pop.find(c, &idx)) return false;
            path.push_back({cur, idx});
            cur = cur->children[idx]; kvv.remove_prefix(1);
        }
        return false;
    }
};

} // namespace gteitelbaum
