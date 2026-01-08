#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <map>
#include <unordered_map>
#include <random>
#include <algorithm>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <numeric>

#include "tktrie.h"

using namespace gteitelbaum;
using namespace std::chrono;

static constexpr size_t NUM_KEYS = 100000;
static constexpr int ITERATIONS = 5;

// =============================================================================
// Guarded containers for thread-safe comparison
// =============================================================================

template <typename K, typename V>
class guarded_map {
    std::map<K, V> map_;
    mutable std::shared_mutex mutex_;
public:
    bool insert(const K& k, const V& v) {
        std::unique_lock lock(mutex_);
        return map_.insert({k, v}).second;
    }
    bool erase(const K& k) {
        std::unique_lock lock(mutex_);
        return map_.erase(k) > 0;
    }
    bool find(const K& k) const {
        std::shared_lock lock(mutex_);
        return map_.find(k) != map_.end();
    }
    void clear() {
        std::unique_lock lock(mutex_);
        map_.clear();
    }
};

template <typename K, typename V>
class guarded_unordered_map {
    std::unordered_map<K, V> map_;
    mutable std::shared_mutex mutex_;
public:
    bool insert(const K& k, const V& v) {
        std::unique_lock lock(mutex_);
        return map_.insert({k, v}).second;
    }
    bool erase(const K& k) {
        std::unique_lock lock(mutex_);
        return map_.erase(k) > 0;
    }
    bool find(const K& k) const {
        std::shared_lock lock(mutex_);
        return map_.find(k) != map_.end();
    }
    void clear() {
        std::unique_lock lock(mutex_);
        map_.clear();
    }
};

// =============================================================================
// Key generation
// =============================================================================

std::vector<uint64_t> generate_sequential_keys(size_t n) {
    std::vector<uint64_t> keys(n);
    std::iota(keys.begin(), keys.end(), 0);
    return keys;
}

std::vector<uint64_t> generate_random_keys(size_t n, uint64_t seed = 12345) {
    std::vector<uint64_t> keys(n);
    std::mt19937_64 rng(seed);
    for (auto& k : keys) k = rng();
    return keys;
}

std::vector<uint64_t> generate_missing_keys(size_t n, uint64_t seed = 99999) {
    std::vector<uint64_t> keys(n);
    std::mt19937_64 rng(seed);
    for (auto& k : keys) k = rng() | 0x8000000000000000ULL; // High bit set = not in dataset
    return keys;
}

// =============================================================================
// Timing utilities
// =============================================================================

template <typename F>
double time_op_ns(F&& f, size_t ops) {
    auto start = high_resolution_clock::now();
    f();
    auto end = high_resolution_clock::now();
    return duration_cast<nanoseconds>(end - start).count() / static_cast<double>(ops);
}

double trimmed_mean(std::vector<double>& v) {
    std::sort(v.begin(), v.end());
    // Drop best and worst, average the rest
    double sum = 0;
    for (size_t i = 1; i < v.size() - 1; ++i) sum += v[i];
    return sum / (v.size() - 2);
}

// =============================================================================
// Result structure
// =============================================================================

struct BenchRow {
    double tktrie;
    double map;
    double umap;
    
    double map_vs() const { return map / tktrie; }
    double umap_vs() const { return umap / tktrie; }
};

// =============================================================================
// Single-threaded benchmarks (THREADED=false)
// =============================================================================

BenchRow bench_find_st(const std::vector<uint64_t>& keys) {
    BenchRow r;
    
    // Build containers
    int64_trie<int> trie;
    std::map<uint64_t, int> m;
    std::unordered_map<uint64_t, int> um;
    um.reserve(keys.size());
    
    for (auto k : keys) {
        trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
        m.insert({k, static_cast<int>(k)});
        um.insert({k, static_cast<int>(k)});
    }
    
    volatile size_t cnt = 0;
    r.tktrie = time_op_ns([&]() {
        size_t c = 0;
        for (auto k : keys) c += trie.find(static_cast<int64_t>(k)).valid();
        cnt = c;
    }, keys.size());
    
    r.map = time_op_ns([&]() {
        size_t c = 0;
        for (auto k : keys) c += (m.find(k) != m.end());
        cnt = c;
    }, keys.size());
    
    r.umap = time_op_ns([&]() {
        size_t c = 0;
        for (auto k : keys) c += (um.find(k) != um.end());
        cnt = c;
    }, keys.size());
    
    return r;
}

BenchRow bench_notfound_st(const std::vector<uint64_t>& keys, const std::vector<uint64_t>& missing) {
    BenchRow r;
    
    int64_trie<int> trie;
    std::map<uint64_t, int> m;
    std::unordered_map<uint64_t, int> um;
    um.reserve(keys.size());
    
    for (auto k : keys) {
        trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
        m.insert({k, static_cast<int>(k)});
        um.insert({k, static_cast<int>(k)});
    }
    
    volatile size_t cnt = 0;
    r.tktrie = time_op_ns([&]() {
        size_t c = 0;
        for (auto k : missing) c += trie.find(static_cast<int64_t>(k)).valid();
        cnt = c;
    }, missing.size());
    
    r.map = time_op_ns([&]() {
        size_t c = 0;
        for (auto k : missing) c += (m.find(k) != m.end());
        cnt = c;
    }, missing.size());
    
    r.umap = time_op_ns([&]() {
        size_t c = 0;
        for (auto k : missing) c += (um.find(k) != um.end());
        cnt = c;
    }, missing.size());
    
    return r;
}

BenchRow bench_insert_st(const std::vector<uint64_t>& keys) {
    BenchRow r;
    
    {
        int64_trie<int> trie;
        r.tktrie = time_op_ns([&]() {
            for (auto k : keys) trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
        }, keys.size());
    }
    {
        std::map<uint64_t, int> m;
        r.map = time_op_ns([&]() {
            for (auto k : keys) m.insert({k, static_cast<int>(k)});
        }, keys.size());
    }
    {
        std::unordered_map<uint64_t, int> um;
        um.reserve(keys.size());
        r.umap = time_op_ns([&]() {
            for (auto k : keys) um.insert({k, static_cast<int>(k)});
        }, keys.size());
    }
    
    return r;
}

BenchRow bench_erase_st(const std::vector<uint64_t>& keys) {
    BenchRow r;
    
    {
        int64_trie<int> trie;
        for (auto k : keys) trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
        r.tktrie = time_op_ns([&]() {
            for (auto k : keys) trie.erase(static_cast<int64_t>(k));
        }, keys.size());
    }
    {
        std::map<uint64_t, int> m;
        for (auto k : keys) m.insert({k, static_cast<int>(k)});
        r.map = time_op_ns([&]() {
            for (auto k : keys) m.erase(k);
        }, keys.size());
    }
    {
        std::unordered_map<uint64_t, int> um;
        um.reserve(keys.size());
        for (auto k : keys) um.insert({k, static_cast<int>(k)});
        r.umap = time_op_ns([&]() {
            for (auto k : keys) um.erase(k);
        }, keys.size());
    }
    
    return r;
}

// =============================================================================
// Multi-threaded benchmarks (THREADED=true)
// =============================================================================

BenchRow bench_find_mt(const std::vector<uint64_t>& keys, int num_threads) {
    BenchRow r;
    auto chunk = keys.size() / num_threads;
    
    // TKTRIE
    {
        concurrent_int64_trie<int> trie;
        for (auto k : keys) trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
        
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t b = t * chunk, e = (t == num_threads-1) ? keys.size() : (t+1)*chunk;
            threads.emplace_back([&, b, e]() {
                volatile size_t c = 0;
                for (size_t i = b; i < e; ++i) c += trie.find(static_cast<int64_t>(keys[i])).valid();
            });
        }
        for (auto& th : threads) th.join();
        r.tktrie = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() / static_cast<double>(keys.size());
    }
    
    // guarded<map>
    {
        guarded_map<uint64_t, int> gm;
        for (auto k : keys) gm.insert(k, static_cast<int>(k));
        
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t b = t * chunk, e = (t == num_threads-1) ? keys.size() : (t+1)*chunk;
            threads.emplace_back([&, b, e]() {
                volatile size_t c = 0;
                for (size_t i = b; i < e; ++i) c += gm.find(keys[i]);
            });
        }
        for (auto& th : threads) th.join();
        r.map = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() / static_cast<double>(keys.size());
    }
    
    // guarded<unordered_map>
    {
        guarded_unordered_map<uint64_t, int> gum;
        for (auto k : keys) gum.insert(k, static_cast<int>(k));
        
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t b = t * chunk, e = (t == num_threads-1) ? keys.size() : (t+1)*chunk;
            threads.emplace_back([&, b, e]() {
                volatile size_t c = 0;
                for (size_t i = b; i < e; ++i) c += gum.find(keys[i]);
            });
        }
        for (auto& th : threads) th.join();
        r.umap = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() / static_cast<double>(keys.size());
    }
    
    return r;
}

BenchRow bench_notfound_mt(const std::vector<uint64_t>& keys, const std::vector<uint64_t>& missing, int num_threads) {
    BenchRow r;
    auto chunk = missing.size() / num_threads;
    
    // TKTRIE
    {
        concurrent_int64_trie<int> trie;
        for (auto k : keys) trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
        
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t b = t * chunk, e = (t == num_threads-1) ? missing.size() : (t+1)*chunk;
            threads.emplace_back([&, b, e]() {
                volatile size_t c = 0;
                for (size_t i = b; i < e; ++i) c += trie.find(static_cast<int64_t>(missing[i])).valid();
            });
        }
        for (auto& th : threads) th.join();
        r.tktrie = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() / static_cast<double>(missing.size());
    }
    
    // guarded<map>
    {
        guarded_map<uint64_t, int> gm;
        for (auto k : keys) gm.insert(k, static_cast<int>(k));
        
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t b = t * chunk, e = (t == num_threads-1) ? missing.size() : (t+1)*chunk;
            threads.emplace_back([&, b, e]() {
                volatile size_t c = 0;
                for (size_t i = b; i < e; ++i) c += gm.find(missing[i]);
            });
        }
        for (auto& th : threads) th.join();
        r.map = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() / static_cast<double>(missing.size());
    }
    
    // guarded<unordered_map>
    {
        guarded_unordered_map<uint64_t, int> gum;
        for (auto k : keys) gum.insert(k, static_cast<int>(k));
        
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t b = t * chunk, e = (t == num_threads-1) ? missing.size() : (t+1)*chunk;
            threads.emplace_back([&, b, e]() {
                volatile size_t c = 0;
                for (size_t i = b; i < e; ++i) c += gum.find(missing[i]);
            });
        }
        for (auto& th : threads) th.join();
        r.umap = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() / static_cast<double>(missing.size());
    }
    
    return r;
}

// Contested find: readers vs 1 writer
template <typename Container, typename FindFn, typename WriteFn>
double bench_contested_generic(Container& c, const std::vector<uint64_t>& read_keys, 
                                FindFn find_fn, WriteFn write_fn, int num_readers) {
    std::atomic<bool> running{true};
    std::atomic<size_t> read_ops{0};
    
    std::vector<std::thread> readers;
    auto chunk = read_keys.size() / num_readers;
    
    // Start readers
    for (int t = 0; t < num_readers; ++t) {
        size_t b = t * chunk, e = (t == num_readers-1) ? read_keys.size() : (t+1)*chunk;
        readers.emplace_back([&, b, e]() {
            size_t ops = 0;
            while (running.load(std::memory_order_relaxed)) {
                for (size_t i = b; i < e && running.load(std::memory_order_relaxed); ++i) {
                    find_fn(c, read_keys[i]);
                    ++ops;
                }
            }
            read_ops.fetch_add(ops);
        });
    }
    
    // Writer thread
    std::thread writer([&]() {
        size_t idx = 0;
        while (running.load(std::memory_order_relaxed)) {
            write_fn(c, read_keys[idx % read_keys.size()]);
            idx++;
        }
    });
    
    auto start = high_resolution_clock::now();
    std::this_thread::sleep_for(milliseconds(100));
    running.store(false);
    
    for (auto& r : readers) r.join();
    writer.join();
    
    auto elapsed = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count();
    return read_ops > 0 ? elapsed / static_cast<double>(read_ops) : 0;
}

BenchRow bench_contested_find_mt(const std::vector<uint64_t>& keys, int num_threads) {
    BenchRow r;
    int num_readers = std::max(1, num_threads - 1);
    
    // TKTRIE
    {
        concurrent_int64_trie<int> trie;
        for (auto k : keys) trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
        r.tktrie = bench_contested_generic(trie, keys,
            [](auto& t, uint64_t k) { return t.find(static_cast<int64_t>(k)).valid(); },
            [](auto& t, uint64_t k) { t.insert({static_cast<int64_t>(k), 0}); t.erase(static_cast<int64_t>(k)); },
            num_readers);
    }
    
    // guarded<map>
    {
        guarded_map<uint64_t, int> gm;
        for (auto k : keys) gm.insert(k, static_cast<int>(k));
        r.map = bench_contested_generic(gm, keys,
            [](auto& m, uint64_t k) { return m.find(k); },
            [](auto& m, uint64_t k) { m.insert(k, 0); m.erase(k); },
            num_readers);
    }
    
    // guarded<unordered_map>
    {
        guarded_unordered_map<uint64_t, int> gum;
        for (auto k : keys) gum.insert(k, static_cast<int>(k));
        r.umap = bench_contested_generic(gum, keys,
            [](auto& m, uint64_t k) { return m.find(k); },
            [](auto& m, uint64_t k) { m.insert(k, 0); m.erase(k); },
            num_readers);
    }
    
    return r;
}

BenchRow bench_contested_notfound_mt(const std::vector<uint64_t>& keys, const std::vector<uint64_t>& missing, int num_threads) {
    BenchRow r;
    int num_readers = std::max(1, num_threads - 1);
    
    // TKTRIE
    {
        concurrent_int64_trie<int> trie;
        for (auto k : keys) trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
        r.tktrie = bench_contested_generic(trie, missing,
            [](auto& t, uint64_t k) { return t.find(static_cast<int64_t>(k)).valid(); },
            [&keys](auto& t, uint64_t) { 
                static size_t idx = 0;
                auto k = keys[idx++ % keys.size()];
                t.insert({static_cast<int64_t>(k), 0}); 
                t.erase(static_cast<int64_t>(k)); 
            },
            num_readers);
    }
    
    // guarded<map>
    {
        guarded_map<uint64_t, int> gm;
        for (auto k : keys) gm.insert(k, static_cast<int>(k));
        r.map = bench_contested_generic(gm, missing,
            [](auto& m, uint64_t k) { return m.find(k); },
            [&keys](auto& m, uint64_t) { 
                static size_t idx = 0;
                auto k = keys[idx++ % keys.size()];
                m.insert(k, 0); 
                m.erase(k); 
            },
            num_readers);
    }
    
    // guarded<unordered_map>
    {
        guarded_unordered_map<uint64_t, int> gum;
        for (auto k : keys) gum.insert(k, static_cast<int>(k));
        r.umap = bench_contested_generic(gum, missing,
            [](auto& m, uint64_t k) { return m.find(k); },
            [&keys](auto& m, uint64_t) { 
                static size_t idx = 0;
                auto k = keys[idx++ % keys.size()];
                m.insert(k, 0); 
                m.erase(k); 
            },
            num_readers);
    }
    
    return r;
}

BenchRow bench_insert_mt(const std::vector<uint64_t>& keys, int num_threads) {
    BenchRow r;
    auto chunk = keys.size() / num_threads;
    
    // TKTRIE
    {
        concurrent_int64_trie<int> trie;
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t b = t * chunk, e = (t == num_threads-1) ? keys.size() : (t+1)*chunk;
            threads.emplace_back([&, b, e]() {
                for (size_t i = b; i < e; ++i) 
                    trie.insert({static_cast<int64_t>(keys[i]), static_cast<int>(keys[i])});
            });
        }
        for (auto& th : threads) th.join();
        r.tktrie = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() / static_cast<double>(keys.size());
    }
    
    // guarded<map>
    {
        guarded_map<uint64_t, int> gm;
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t b = t * chunk, e = (t == num_threads-1) ? keys.size() : (t+1)*chunk;
            threads.emplace_back([&, b, e]() {
                for (size_t i = b; i < e; ++i) gm.insert(keys[i], static_cast<int>(keys[i]));
            });
        }
        for (auto& th : threads) th.join();
        r.map = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() / static_cast<double>(keys.size());
    }
    
    // guarded<unordered_map>
    {
        guarded_unordered_map<uint64_t, int> gum;
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t b = t * chunk, e = (t == num_threads-1) ? keys.size() : (t+1)*chunk;
            threads.emplace_back([&, b, e]() {
                for (size_t i = b; i < e; ++i) gum.insert(keys[i], static_cast<int>(keys[i]));
            });
        }
        for (auto& th : threads) th.join();
        r.umap = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() / static_cast<double>(keys.size());
    }
    
    return r;
}

BenchRow bench_erase_mt(const std::vector<uint64_t>& keys, int num_threads) {
    BenchRow r;
    auto chunk = keys.size() / num_threads;
    
    // TKTRIE
    {
        concurrent_int64_trie<int> trie;
        for (auto k : keys) trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
        
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t b = t * chunk, e = (t == num_threads-1) ? keys.size() : (t+1)*chunk;
            threads.emplace_back([&, b, e]() {
                for (size_t i = b; i < e; ++i) trie.erase(static_cast<int64_t>(keys[i]));
            });
        }
        for (auto& th : threads) th.join();
        r.tktrie = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() / static_cast<double>(keys.size());
    }
    
    // guarded<map>
    {
        guarded_map<uint64_t, int> gm;
        for (auto k : keys) gm.insert(k, static_cast<int>(k));
        
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t b = t * chunk, e = (t == num_threads-1) ? keys.size() : (t+1)*chunk;
            threads.emplace_back([&, b, e]() {
                for (size_t i = b; i < e; ++i) gm.erase(keys[i]);
            });
        }
        for (auto& th : threads) th.join();
        r.map = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() / static_cast<double>(keys.size());
    }
    
    // guarded<unordered_map>
    {
        guarded_unordered_map<uint64_t, int> gum;
        for (auto k : keys) gum.insert(k, static_cast<int>(k));
        
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t b = t * chunk, e = (t == num_threads-1) ? keys.size() : (t+1)*chunk;
            threads.emplace_back([&, b, e]() {
                for (size_t i = b; i < e; ++i) gum.erase(keys[i]);
            });
        }
        for (auto& th : threads) th.join();
        r.umap = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() / static_cast<double>(keys.size());
    }
    
    return r;
}

// =============================================================================
// Output helpers
// =============================================================================

void print_row(const std::string& op, const BenchRow& r) {
    std::cout << "| " << std::left << std::setw(20) << op << " | "
              << std::right << std::fixed << std::setprecision(1)
              << std::setw(8) << r.tktrie << " | "
              << std::setw(8) << r.map << " | "
              << std::setw(6) << r.map_vs() << "x | "
              << std::setw(8) << r.umap << " | "
              << std::setw(6) << r.umap_vs() << "x |\n";
}

BenchRow average_rows(std::vector<BenchRow>& rows) {
    std::vector<double> t, m, u;
    for (auto& r : rows) { t.push_back(r.tktrie); m.push_back(r.map); u.push_back(r.umap); }
    return {trimmed_mean(t), trimmed_mean(m), trimmed_mean(u)};
}

// =============================================================================
// Main
// =============================================================================

int main() {
    auto seq_keys = generate_sequential_keys(NUM_KEYS);
    auto rnd_keys = generate_random_keys(NUM_KEYS);
    auto missing = generate_missing_keys(NUM_KEYS);
    
    std::cout << "# TKTRIE Benchmark Results\n\n";
    std::cout << "- **Keys:** " << NUM_KEYS << " uint64\n";
    std::cout << "- **Iterations:** " << ITERATIONS << " (drop best/worst, average middle 3)\n";
    std::cout << "- **Times:** nanoseconds per operation (lower is better)\n";
    std::cout << "- **MAP vs / UMAP vs:** ratio vs TKTRIE (>1 = TKTRIE faster)\n\n";
    
    // =========================================================================
    // SINGLE-THREADED (THREADED=false)
    // =========================================================================
    
    std::cout << "## Single-Threaded (THREADED=false)\n\n";
    
    for (int key_type = 0; key_type < 2; ++key_type) {
        const auto& keys = (key_type == 0) ? seq_keys : rnd_keys;
        std::cout << "### " << (key_type == 0 ? "Sequential" : "Random") << " Keys\n\n";
        std::cout << "| Operation            | TKTRIE   | MAP      | MAP vs | UMAP     | UMAP vs |\n";
        std::cout << "|----------------------|----------|----------|--------|----------|---------|\n";
        
        std::vector<BenchRow> find_r, notfound_r, insert_r, erase_r;
        for (int i = 0; i < ITERATIONS; ++i) {
            find_r.push_back(bench_find_st(keys));
            notfound_r.push_back(bench_notfound_st(keys, missing));
            insert_r.push_back(bench_insert_st(keys));
            erase_r.push_back(bench_erase_st(keys));
        }
        
        print_row("FIND", average_rows(find_r));
        print_row("NOT-FOUND", average_rows(notfound_r));
        print_row("INSERT", average_rows(insert_r));
        print_row("ERASE", average_rows(erase_r));
        std::cout << "\n";
    }
    
    // =========================================================================
    // MULTI-THREADED (THREADED=true)
    // =========================================================================
    
    for (int threads : {1, 2, 3, 4}) {
        std::cout << "## " << threads << " Thread" << (threads > 1 ? "s" : "") << " (THREADED=true)\n\n";
        
        for (int key_type = 0; key_type < 2; ++key_type) {
            const auto& keys = (key_type == 0) ? seq_keys : rnd_keys;
            std::cout << "### " << (key_type == 0 ? "Sequential" : "Random") << " Keys\n\n";
            std::cout << "| Operation            | TKTRIE   | MAP      | MAP vs | UMAP     | UMAP vs |\n";
            std::cout << "|----------------------|----------|----------|--------|----------|---------|\n";
            
            std::vector<BenchRow> find_r, notfound_r, cfind_r, cnotfound_r, insert_r, erase_r;
            for (int i = 0; i < ITERATIONS; ++i) {
                find_r.push_back(bench_find_mt(keys, threads));
                notfound_r.push_back(bench_notfound_mt(keys, missing, threads));
                cfind_r.push_back(bench_contested_find_mt(keys, threads));
                cnotfound_r.push_back(bench_contested_notfound_mt(keys, missing, threads));
                insert_r.push_back(bench_insert_mt(keys, threads));
                erase_r.push_back(bench_erase_mt(keys, threads));
            }
            
            print_row("FIND", average_rows(find_r));
            print_row("NOT-FOUND", average_rows(notfound_r));
            print_row("FIND + 1 Writer", average_rows(cfind_r));
            print_row("NOT-FOUND + 1 Writer", average_rows(cnotfound_r));
            print_row("INSERT", average_rows(insert_r));
            print_row("ERASE", average_rows(erase_r));
            std::cout << "\n";
        }
    }
    
    return 0;
}
