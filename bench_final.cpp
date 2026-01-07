#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <map>
#include <unordered_map>
#include <set>
#include <random>
#include <algorithm>
#include <thread>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <numeric>
#include <functional>
#include <sstream>

#include "tktrie.h"

using namespace gteitelbaum;
using namespace std::chrono;

static constexpr size_t NUM_KEYS = 100000;
static constexpr int BENCH_RUNS = 5;  // Run 5 times, drop best/worst, avg remaining 3

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

// Generate keys that don't exist in the given set
std::vector<uint64_t> generate_missing_keys(const std::vector<uint64_t>& existing, size_t n, uint64_t seed = 99999) {
    std::set<uint64_t> existing_set(existing.begin(), existing.end());
    std::vector<uint64_t> missing;
    missing.reserve(n);
    std::mt19937_64 rng(seed);
    while (missing.size() < n) {
        uint64_t k = rng();
        if (existing_set.find(k) == existing_set.end()) {
            missing.push_back(k);
            existing_set.insert(k);
        }
    }
    return missing;
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

double avg_drop_extremes(std::vector<double>& v) {
    std::sort(v.begin(), v.end());
    // Drop best and worst, average middle 3
    double sum = 0;
    for (size_t i = 1; i < v.size() - 1; ++i) sum += v[i];
    return sum / (v.size() - 2);
}

// =============================================================================
// Single-threaded benchmarks (THREADED=false)
// =============================================================================

struct BenchResult {
    double find_ns = 0;
    double not_found_ns = 0;
    double insert_ns = 0;
    double erase_ns = 0;
};

BenchResult bench_tktrie_st(const std::vector<uint64_t>& keys, const std::vector<uint64_t>& missing) {
    BenchResult res;
    int64_trie<int> trie;
    
    // Populate
    for (auto k : keys) trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
    
    // FIND (all found)
    res.find_ns = time_op_ns([&]() {
        for (auto k : keys) { volatile bool b = trie.contains(static_cast<int64_t>(k)); (void)b; }
    }, keys.size());
    
    // NOT-FOUND
    res.not_found_ns = time_op_ns([&]() {
        for (auto k : missing) { volatile bool b = trie.contains(static_cast<int64_t>(k)); (void)b; }
    }, missing.size());
    
    // Clear and re-measure INSERT
    trie.clear();
    res.insert_ns = time_op_ns([&]() {
        for (auto k : keys) trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
    }, keys.size());
    
    // ERASE
    res.erase_ns = time_op_ns([&]() {
        for (auto k : keys) trie.erase(static_cast<int64_t>(k));
    }, keys.size());
    
    return res;
}

BenchResult bench_std_map_st(const std::vector<uint64_t>& keys, const std::vector<uint64_t>& missing) {
    BenchResult res;
    std::map<uint64_t, int> m;
    
    for (auto k : keys) m.insert({k, static_cast<int>(k)});
    
    res.find_ns = time_op_ns([&]() {
        for (auto k : keys) { volatile bool b = (m.find(k) != m.end()); (void)b; }
    }, keys.size());
    
    res.not_found_ns = time_op_ns([&]() {
        for (auto k : missing) { volatile bool b = (m.find(k) != m.end()); (void)b; }
    }, missing.size());
    
    m.clear();
    res.insert_ns = time_op_ns([&]() {
        for (auto k : keys) m.insert({k, static_cast<int>(k)});
    }, keys.size());
    
    res.erase_ns = time_op_ns([&]() {
        for (auto k : keys) m.erase(k);
    }, keys.size());
    
    return res;
}

BenchResult bench_std_umap_st(const std::vector<uint64_t>& keys, const std::vector<uint64_t>& missing) {
    BenchResult res;
    std::unordered_map<uint64_t, int> m;
    m.reserve(keys.size());
    
    for (auto k : keys) m.insert({k, static_cast<int>(k)});
    
    res.find_ns = time_op_ns([&]() {
        for (auto k : keys) { volatile bool b = (m.find(k) != m.end()); (void)b; }
    }, keys.size());
    
    res.not_found_ns = time_op_ns([&]() {
        for (auto k : missing) { volatile bool b = (m.find(k) != m.end()); (void)b; }
    }, missing.size());
    
    m.clear();
    m.reserve(keys.size());
    res.insert_ns = time_op_ns([&]() {
        for (auto k : keys) m.insert({k, static_cast<int>(k)});
    }, keys.size());
    
    res.erase_ns = time_op_ns([&]() {
        for (auto k : keys) m.erase(k);
    }, keys.size());
    
    return res;
}

// =============================================================================
// Multi-threaded benchmarks (THREADED=true)
// =============================================================================

struct MTBenchResult {
    double find_ns = 0;
    double not_found_ns = 0;
    double find_contended_ns = 0;
    double not_found_contended_ns = 0;
    double insert_ns = 0;
    double erase_ns = 0;
};

template <typename Container, typename InsertFn, typename FindFn, typename EraseFn, typename ClearFn>
MTBenchResult bench_mt_generic(
    const std::vector<uint64_t>& keys,
    const std::vector<uint64_t>& missing,
    int num_threads,
    InsertFn insert_fn,
    FindFn find_fn,
    EraseFn erase_fn,
    ClearFn clear_fn
) {
    MTBenchResult res;
    Container container;
    
    // Populate
    for (auto k : keys) insert_fn(container, k, static_cast<int>(k));
    
    auto chunk_size = keys.size() / num_threads;
    
    // FIND (no contention) - parallel readers
    {
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t begin = t * chunk_size;
            size_t end = (t == num_threads - 1) ? keys.size() : (t + 1) * chunk_size;
            threads.emplace_back([&, begin, end]() {
                for (size_t i = begin; i < end; ++i) {
                    volatile bool b = find_fn(container, keys[i]);
                    (void)b;
                }
            });
        }
        for (auto& th : threads) th.join();
        res.find_ns = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() 
                      / static_cast<double>(keys.size());
    }
    
    // NOT-FOUND (no contention)
    {
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t begin = t * chunk_size;
            size_t end = (t == num_threads - 1) ? missing.size() : (t + 1) * chunk_size;
            threads.emplace_back([&, begin, end]() {
                for (size_t i = begin; i < end; ++i) {
                    volatile bool b = find_fn(container, missing[i]);
                    (void)b;
                }
            });
        }
        for (auto& th : threads) th.join();
        res.not_found_ns = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() 
                          / static_cast<double>(missing.size());
    }
    
    // FIND + 1 Writer (contended)
    {
        std::atomic<bool> stop{false};
        std::atomic<size_t> write_ops{0};
        
        // Writer thread - continuously insert/erase from second half
        std::thread writer([&]() {
            size_t half = keys.size() / 2;
            while (!stop.load(std::memory_order_relaxed)) {
                for (size_t i = half; i < keys.size() && !stop.load(std::memory_order_relaxed); ++i) {
                    erase_fn(container, keys[i]);
                    insert_fn(container, keys[i], static_cast<int>(keys[i]));
                    write_ops.fetch_add(2, std::memory_order_relaxed);
                }
            }
        });
        
        // Reader threads - read from first half
        std::vector<std::thread> readers;
        size_t read_chunk = (keys.size() / 2) / num_threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t begin = t * read_chunk;
            size_t end = (t == num_threads - 1) ? keys.size() / 2 : (t + 1) * read_chunk;
            readers.emplace_back([&, begin, end]() {
                for (size_t i = begin; i < end; ++i) {
                    volatile bool b = find_fn(container, keys[i]);
                    (void)b;
                }
            });
        }
        for (auto& th : readers) th.join();
        res.find_contended_ns = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() 
                                / static_cast<double>(keys.size() / 2);
        
        stop.store(true);
        writer.join();
    }
    
    // Repopulate for NOT-FOUND + 1 Writer
    clear_fn(container);
    for (auto k : keys) insert_fn(container, k, static_cast<int>(k));
    
    // NOT-FOUND + 1 Writer (contended)
    {
        std::atomic<bool> stop{false};
        
        std::thread writer([&]() {
            size_t half = keys.size() / 2;
            while (!stop.load(std::memory_order_relaxed)) {
                for (size_t i = half; i < keys.size() && !stop.load(std::memory_order_relaxed); ++i) {
                    erase_fn(container, keys[i]);
                    insert_fn(container, keys[i], static_cast<int>(keys[i]));
                }
            }
        });
        
        std::vector<std::thread> readers;
        size_t read_chunk = missing.size() / num_threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t begin = t * read_chunk;
            size_t end = (t == num_threads - 1) ? missing.size() : (t + 1) * read_chunk;
            readers.emplace_back([&, begin, end]() {
                for (size_t i = begin; i < end; ++i) {
                    volatile bool b = find_fn(container, missing[i]);
                    (void)b;
                }
            });
        }
        for (auto& th : readers) th.join();
        res.not_found_contended_ns = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() 
                                     / static_cast<double>(missing.size());
        
        stop.store(true);
        writer.join();
    }
    
    // INSERT - parallel
    clear_fn(container);
    {
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t begin = t * chunk_size;
            size_t end = (t == num_threads - 1) ? keys.size() : (t + 1) * chunk_size;
            threads.emplace_back([&, begin, end]() {
                for (size_t i = begin; i < end; ++i) {
                    insert_fn(container, keys[i], static_cast<int>(keys[i]));
                }
            });
        }
        for (auto& th : threads) th.join();
        res.insert_ns = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() 
                       / static_cast<double>(keys.size());
    }
    
    // ERASE - parallel
    {
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t begin = t * chunk_size;
            size_t end = (t == num_threads - 1) ? keys.size() : (t + 1) * chunk_size;
            threads.emplace_back([&, begin, end]() {
                for (size_t i = begin; i < end; ++i) {
                    erase_fn(container, keys[i]);
                }
            });
        }
        for (auto& th : threads) th.join();
        res.erase_ns = duration_cast<nanoseconds>(high_resolution_clock::now() - start).count() 
                      / static_cast<double>(keys.size());
    }
    
    return res;
}

MTBenchResult bench_tktrie_mt(const std::vector<uint64_t>& keys, const std::vector<uint64_t>& missing, int num_threads) {
    using Trie = concurrent_int64_trie<int>;
    return bench_mt_generic<Trie>(
        keys, missing, num_threads,
        [](Trie& t, uint64_t k, int v) { return t.insert({static_cast<int64_t>(k), v}).second; },
        [](Trie& t, uint64_t k) { return t.contains(static_cast<int64_t>(k)); },
        [](Trie& t, uint64_t k) { return t.erase(static_cast<int64_t>(k)); },
        [](Trie& t) { t.clear(); }
    );
}

MTBenchResult bench_guarded_map_mt(const std::vector<uint64_t>& keys, const std::vector<uint64_t>& missing, int num_threads) {
    using Map = guarded_map<uint64_t, int>;
    return bench_mt_generic<Map>(
        keys, missing, num_threads,
        [](Map& m, uint64_t k, int v) { return m.insert(k, v); },
        [](Map& m, uint64_t k) { return m.find(k); },
        [](Map& m, uint64_t k) { return m.erase(k); },
        [](Map& m) { m.clear(); }
    );
}

MTBenchResult bench_guarded_umap_mt(const std::vector<uint64_t>& keys, const std::vector<uint64_t>& missing, int num_threads) {
    using Map = guarded_unordered_map<uint64_t, int>;
    return bench_mt_generic<Map>(
        keys, missing, num_threads,
        [](Map& m, uint64_t k, int v) { return m.insert(k, v); },
        [](Map& m, uint64_t k) { return m.find(k); },
        [](Map& m, uint64_t k) { return m.erase(k); },
        [](Map& m) { m.clear(); }
    );
}

// =============================================================================
// Output formatting
// =============================================================================

std::string fmt(double v) {
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(1) << v;
    return ss.str();
}

std::string fmt_ratio(double tktrie, double other) {
    if (tktrie <= 0) return "N/A";
    double ratio = other / tktrie;
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(2) << ratio << "x";
    return ss.str();
}

void print_st_table(const char* key_type, BenchResult& trie, BenchResult& map, BenchResult& umap) {
    std::cout << "\n### " << key_type << " Keys\n\n";
    std::cout << "| Operation | TKTRIE | MAP | MAP vs | UMAP | UMAP vs |\n";
    std::cout << "|-----------|--------|-----|--------|------|--------|\n";
    std::cout << "| FIND | " << fmt(trie.find_ns) << " | " << fmt(map.find_ns) << " | " 
              << fmt_ratio(trie.find_ns, map.find_ns) << " | " << fmt(umap.find_ns) << " | "
              << fmt_ratio(trie.find_ns, umap.find_ns) << " |\n";
    std::cout << "| NOT-FOUND | " << fmt(trie.not_found_ns) << " | " << fmt(map.not_found_ns) << " | "
              << fmt_ratio(trie.not_found_ns, map.not_found_ns) << " | " << fmt(umap.not_found_ns) << " | "
              << fmt_ratio(trie.not_found_ns, umap.not_found_ns) << " |\n";
    std::cout << "| INSERT | " << fmt(trie.insert_ns) << " | " << fmt(map.insert_ns) << " | "
              << fmt_ratio(trie.insert_ns, map.insert_ns) << " | " << fmt(umap.insert_ns) << " | "
              << fmt_ratio(trie.insert_ns, umap.insert_ns) << " |\n";
    std::cout << "| ERASE | " << fmt(trie.erase_ns) << " | " << fmt(map.erase_ns) << " | "
              << fmt_ratio(trie.erase_ns, map.erase_ns) << " | " << fmt(umap.erase_ns) << " | "
              << fmt_ratio(trie.erase_ns, umap.erase_ns) << " |\n";
}

void print_mt_table(const char* key_type, MTBenchResult& trie, MTBenchResult& map, MTBenchResult& umap) {
    std::cout << "\n### " << key_type << " Keys\n\n";
    std::cout << "| Operation | TKTRIE | MAP | MAP vs | UMAP | UMAP vs |\n";
    std::cout << "|-----------|--------|-----|--------|------|--------|\n";
    std::cout << "| FIND | " << fmt(trie.find_ns) << " | " << fmt(map.find_ns) << " | "
              << fmt_ratio(trie.find_ns, map.find_ns) << " | " << fmt(umap.find_ns) << " | "
              << fmt_ratio(trie.find_ns, umap.find_ns) << " |\n";
    std::cout << "| NOT-FOUND | " << fmt(trie.not_found_ns) << " | " << fmt(map.not_found_ns) << " | "
              << fmt_ratio(trie.not_found_ns, map.not_found_ns) << " | " << fmt(umap.not_found_ns) << " | "
              << fmt_ratio(trie.not_found_ns, umap.not_found_ns) << " |\n";
    std::cout << "| FIND+1Writer | " << fmt(trie.find_contended_ns) << " | " << fmt(map.find_contended_ns) << " | "
              << fmt_ratio(trie.find_contended_ns, map.find_contended_ns) << " | " << fmt(umap.find_contended_ns) << " | "
              << fmt_ratio(trie.find_contended_ns, umap.find_contended_ns) << " |\n";
    std::cout << "| NOT-FOUND+1Writer | " << fmt(trie.not_found_contended_ns) << " | " << fmt(map.not_found_contended_ns) << " | "
              << fmt_ratio(trie.not_found_contended_ns, map.not_found_contended_ns) << " | " << fmt(umap.not_found_contended_ns) << " | "
              << fmt_ratio(trie.not_found_contended_ns, umap.not_found_contended_ns) << " |\n";
    std::cout << "| INSERT | " << fmt(trie.insert_ns) << " | " << fmt(map.insert_ns) << " | "
              << fmt_ratio(trie.insert_ns, map.insert_ns) << " | " << fmt(umap.insert_ns) << " | "
              << fmt_ratio(trie.insert_ns, umap.insert_ns) << " |\n";
    std::cout << "| ERASE | " << fmt(trie.erase_ns) << " | " << fmt(map.erase_ns) << " | "
              << fmt_ratio(trie.erase_ns, map.erase_ns) << " | " << fmt(umap.erase_ns) << " | "
              << fmt_ratio(trie.erase_ns, umap.erase_ns) << " |\n";
}

// =============================================================================
// Main
// =============================================================================

int main() {
    std::cout << "# TKTRIE Benchmark Results\n\n";
    std::cout << "- **Keys**: " << NUM_KEYS << " uint64\n";
    std::cout << "- **Runs**: " << BENCH_RUNS << " (drop best/worst, average remaining 3)\n";
    std::cout << "- **Times**: nanoseconds per operation\n";
    std::cout << "- **\"vs\" columns**: ratio to TKTRIE (>1 means TKTRIE is faster)\n\n";
    
    auto rnd_keys = generate_random_keys(NUM_KEYS);
    auto seq_keys = generate_sequential_keys(NUM_KEYS);
    auto rnd_missing = generate_missing_keys(rnd_keys, NUM_KEYS);
    auto seq_missing = generate_missing_keys(seq_keys, NUM_KEYS);
    
    // =========================================================================
    // SINGLE-THREADED (THREADED=false)
    // =========================================================================
    
    std::cout << "## Single-Threaded (THREADED=false)\n";
    
    // Random keys
    {
        std::vector<BenchResult> trie_runs, map_runs, umap_runs;
        for (int r = 0; r < BENCH_RUNS; ++r) {
            trie_runs.push_back(bench_tktrie_st(rnd_keys, rnd_missing));
            map_runs.push_back(bench_std_map_st(rnd_keys, rnd_missing));
            umap_runs.push_back(bench_std_umap_st(rnd_keys, rnd_missing));
        }
        
        BenchResult trie_avg, map_avg, umap_avg;
        std::vector<double> v;
        
        #define AVG_FIELD(field) \
            v.clear(); for (auto& r : trie_runs) v.push_back(r.field); trie_avg.field = avg_drop_extremes(v); \
            v.clear(); for (auto& r : map_runs) v.push_back(r.field); map_avg.field = avg_drop_extremes(v); \
            v.clear(); for (auto& r : umap_runs) v.push_back(r.field); umap_avg.field = avg_drop_extremes(v);
        
        AVG_FIELD(find_ns);
        AVG_FIELD(not_found_ns);
        AVG_FIELD(insert_ns);
        AVG_FIELD(erase_ns);
        #undef AVG_FIELD
        
        print_st_table("Random", trie_avg, map_avg, umap_avg);
    }
    
    // Sequential keys
    {
        std::vector<BenchResult> trie_runs, map_runs, umap_runs;
        for (int r = 0; r < BENCH_RUNS; ++r) {
            trie_runs.push_back(bench_tktrie_st(seq_keys, seq_missing));
            map_runs.push_back(bench_std_map_st(seq_keys, seq_missing));
            umap_runs.push_back(bench_std_umap_st(seq_keys, seq_missing));
        }
        
        BenchResult trie_avg, map_avg, umap_avg;
        std::vector<double> v;
        
        #define AVG_FIELD(field) \
            v.clear(); for (auto& r : trie_runs) v.push_back(r.field); trie_avg.field = avg_drop_extremes(v); \
            v.clear(); for (auto& r : map_runs) v.push_back(r.field); map_avg.field = avg_drop_extremes(v); \
            v.clear(); for (auto& r : umap_runs) v.push_back(r.field); umap_avg.field = avg_drop_extremes(v);
        
        AVG_FIELD(find_ns);
        AVG_FIELD(not_found_ns);
        AVG_FIELD(insert_ns);
        AVG_FIELD(erase_ns);
        #undef AVG_FIELD
        
        print_st_table("Sequential", trie_avg, map_avg, umap_avg);
    }
    
    // =========================================================================
    // MULTI-THREADED (THREADED=true)
    // =========================================================================
    
    std::vector<int> thread_counts = {1, 2, 3, 4};
    
    for (int num_threads : thread_counts) {
        std::cout << "\n## " << num_threads << " Thread" << (num_threads > 1 ? "s" : "") 
                  << " (THREADED=true)\n";
        
        // Random keys
        {
            std::vector<MTBenchResult> trie_runs, map_runs, umap_runs;
            for (int r = 0; r < BENCH_RUNS; ++r) {
                trie_runs.push_back(bench_tktrie_mt(rnd_keys, rnd_missing, num_threads));
                map_runs.push_back(bench_guarded_map_mt(rnd_keys, rnd_missing, num_threads));
                umap_runs.push_back(bench_guarded_umap_mt(rnd_keys, rnd_missing, num_threads));
            }
            
            MTBenchResult trie_avg, map_avg, umap_avg;
            std::vector<double> v;
            
            #define AVG_FIELD(field) \
                v.clear(); for (auto& r : trie_runs) v.push_back(r.field); trie_avg.field = avg_drop_extremes(v); \
                v.clear(); for (auto& r : map_runs) v.push_back(r.field); map_avg.field = avg_drop_extremes(v); \
                v.clear(); for (auto& r : umap_runs) v.push_back(r.field); umap_avg.field = avg_drop_extremes(v);
            
            AVG_FIELD(find_ns);
            AVG_FIELD(not_found_ns);
            AVG_FIELD(find_contended_ns);
            AVG_FIELD(not_found_contended_ns);
            AVG_FIELD(insert_ns);
            AVG_FIELD(erase_ns);
            #undef AVG_FIELD
            
            print_mt_table("Random", trie_avg, map_avg, umap_avg);
        }
        
        // Sequential keys
        {
            std::vector<MTBenchResult> trie_runs, map_runs, umap_runs;
            for (int r = 0; r < BENCH_RUNS; ++r) {
                trie_runs.push_back(bench_tktrie_mt(seq_keys, seq_missing, num_threads));
                map_runs.push_back(bench_guarded_map_mt(seq_keys, seq_missing, num_threads));
                umap_runs.push_back(bench_guarded_umap_mt(seq_keys, seq_missing, num_threads));
            }
            
            MTBenchResult trie_avg, map_avg, umap_avg;
            std::vector<double> v;
            
            #define AVG_FIELD(field) \
                v.clear(); for (auto& r : trie_runs) v.push_back(r.field); trie_avg.field = avg_drop_extremes(v); \
                v.clear(); for (auto& r : map_runs) v.push_back(r.field); map_avg.field = avg_drop_extremes(v); \
                v.clear(); for (auto& r : umap_runs) v.push_back(r.field); umap_avg.field = avg_drop_extremes(v);
            
            AVG_FIELD(find_ns);
            AVG_FIELD(not_found_ns);
            AVG_FIELD(find_contended_ns);
            AVG_FIELD(not_found_contended_ns);
            AVG_FIELD(insert_ns);
            AVG_FIELD(erase_ns);
            #undef AVG_FIELD
            
            print_mt_table("Sequential", trie_avg, map_avg, umap_avg);
        }
    }
    
    return 0;
}
