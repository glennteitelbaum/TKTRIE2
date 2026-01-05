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
#include "tktrie_core.h"
#include "tktrie_insert.h"
#include "tktrie_erase_probe.h"
#include "tktrie_erase.h"

using namespace gteitelbaum;
using namespace std::chrono;

static constexpr size_t NUM_KEYS = 100000;
static constexpr int BENCH_ITERATIONS = 3;

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
    size_t size() const {
        std::shared_lock lock(mutex_);
        return map_.size();
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
    size_t size() const {
        std::shared_lock lock(mutex_);
        return map_.size();
    }
};

// =============================================================================
// Timing utilities
// =============================================================================

struct BenchResult {
    double insert_ns;
    double find_ns;
    double erase_ns;
};

template <typename F>
double time_op_ns(F&& f, size_t ops) {
    auto start = high_resolution_clock::now();
    f();
    auto end = high_resolution_clock::now();
    return duration_cast<nanoseconds>(end - start).count() / static_cast<double>(ops);
}

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

// =============================================================================
// Single-threaded benchmarks
// =============================================================================

BenchResult bench_tktrie_st(const std::vector<uint64_t>& keys) {
    BenchResult res{};
    int64_trie<int> trie;
    
    // Insert
    res.insert_ns = time_op_ns([&]() {
        for (auto k : keys) trie.insert({static_cast<int64_t>(k), static_cast<int>(k)});
    }, keys.size());
    
    // Find
    volatile size_t found = 0;
    res.find_ns = time_op_ns([&]() {
        size_t cnt = 0;
        for (auto k : keys) cnt += trie.find(static_cast<int64_t>(k)).valid();
        found = cnt;
    }, keys.size());
    (void)found;
    
    // Erase
    res.erase_ns = time_op_ns([&]() {
        for (auto k : keys) trie.erase(static_cast<int64_t>(k));
    }, keys.size());
    
    return res;
}

BenchResult bench_std_map(const std::vector<uint64_t>& keys) {
    BenchResult res{};
    std::map<uint64_t, int> m;
    
    res.insert_ns = time_op_ns([&]() {
        for (auto k : keys) m.insert({k, static_cast<int>(k)});
    }, keys.size());
    
    volatile size_t found = 0;
    res.find_ns = time_op_ns([&]() {
        size_t cnt = 0;
        for (auto k : keys) cnt += (m.find(k) != m.end());
        found = cnt;
    }, keys.size());
    (void)found;
    
    res.erase_ns = time_op_ns([&]() {
        for (auto k : keys) m.erase(k);
    }, keys.size());
    
    return res;
}

BenchResult bench_std_unordered_map(const std::vector<uint64_t>& keys) {
    BenchResult res{};
    std::unordered_map<uint64_t, int> m;
    m.reserve(keys.size());
    
    res.insert_ns = time_op_ns([&]() {
        for (auto k : keys) m.insert({k, static_cast<int>(k)});
    }, keys.size());
    
    volatile size_t found = 0;
    res.find_ns = time_op_ns([&]() {
        size_t cnt = 0;
        for (auto k : keys) cnt += (m.find(k) != m.end());
        found = cnt;
    }, keys.size());
    (void)found;
    
    res.erase_ns = time_op_ns([&]() {
        for (auto k : keys) m.erase(k);
    }, keys.size());
    
    return res;
}

// =============================================================================
// Multi-threaded benchmarks - parallel operations
// =============================================================================

struct MTBenchResult {
    double insert_ns;
    double find_ns;
    double erase_ns;
    double read_with_write_ns;  // Read perf while writers active
    double write_with_read_ns;  // Write perf while readers active
};

template <typename Container>
MTBenchResult bench_mt_generic(
    const std::vector<uint64_t>& keys,
    int num_threads,
    std::function<void(Container&)> clear_fn,
    std::function<bool(Container&, uint64_t, int)> insert_fn,
    std::function<bool(Container&, uint64_t)> find_fn,
    std::function<bool(Container&, uint64_t)> erase_fn
) {
    MTBenchResult res{};
    Container container;
    
    auto chunk_size = keys.size() / num_threads;
    
    // Parallel insert
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
        auto elapsed = high_resolution_clock::now() - start;
        res.insert_ns = duration_cast<nanoseconds>(elapsed).count() / static_cast<double>(keys.size());
    }
    
    // Parallel find (container is populated)
    {
        std::vector<std::thread> threads;
        auto start = high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            size_t begin = t * chunk_size;
            size_t end = (t == num_threads - 1) ? keys.size() : (t + 1) * chunk_size;
            threads.emplace_back([&, begin, end]() {
                for (size_t i = begin; i < end; ++i) {
                    find_fn(container, keys[i]);
                }
            });
        }
        for (auto& th : threads) th.join();
        auto elapsed = high_resolution_clock::now() - start;
        res.find_ns = duration_cast<nanoseconds>(elapsed).count() / static_cast<double>(keys.size());
    }
    
    // Parallel erase
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
        auto elapsed = high_resolution_clock::now() - start;
        res.erase_ns = duration_cast<nanoseconds>(elapsed).count() / static_cast<double>(keys.size());
    }
    
    // Read with concurrent writes
    // Setup: populate half the keys
    clear_fn(container);
    for (size_t i = 0; i < keys.size() / 2; ++i) {
        insert_fn(container, keys[i], static_cast<int>(keys[i]));
    }
    
    {
        std::atomic<bool> running{true};
        std::atomic<size_t> read_count{0};
        std::atomic<size_t> write_count{0};
        
        // Writer threads (half of threads, min 1)
        int writer_threads = std::max(1, num_threads / 2);
        int reader_threads = std::max(1, num_threads - writer_threads);
        
        std::vector<std::thread> threads;
        
        // Start writers - they insert/erase the second half
        for (int t = 0; t < writer_threads; ++t) {
            threads.emplace_back([&, t]() {
                size_t chunk = (keys.size() / 2) / writer_threads;
                size_t start_idx = keys.size() / 2 + t * chunk;
                size_t end_idx = (t == writer_threads - 1) ? keys.size() : start_idx + chunk;
                size_t local_count = 0;
                while (running.load(std::memory_order_relaxed)) {
                    for (size_t i = start_idx; i < end_idx && running.load(std::memory_order_relaxed); ++i) {
                        insert_fn(container, keys[i], static_cast<int>(keys[i]));
                        local_count++;
                    }
                    for (size_t i = start_idx; i < end_idx && running.load(std::memory_order_relaxed); ++i) {
                        erase_fn(container, keys[i]);
                        local_count++;
                    }
                }
                write_count.fetch_add(local_count);
            });
        }
        
        // Start readers - they read the first half
        auto start = high_resolution_clock::now();
        for (int t = 0; t < reader_threads; ++t) {
            threads.emplace_back([&, t]() {
                size_t chunk = (keys.size() / 2) / reader_threads;
                size_t start_idx = t * chunk;
                size_t end_idx = (t == reader_threads - 1) ? keys.size() / 2 : start_idx + chunk;
                size_t local_count = 0;
                while (running.load(std::memory_order_relaxed)) {
                    for (size_t i = start_idx; i < end_idx && running.load(std::memory_order_relaxed); ++i) {
                        find_fn(container, keys[i]);
                        local_count++;
                    }
                }
                read_count.fetch_add(local_count);
            });
        }
        
        // Run for 100ms
        std::this_thread::sleep_for(milliseconds(100));
        running.store(false);
        
        for (auto& th : threads) th.join();
        auto elapsed = high_resolution_clock::now() - start;
        
        double elapsed_ns = duration_cast<nanoseconds>(elapsed).count();
        res.read_with_write_ns = read_count > 0 ? elapsed_ns / read_count : 0;
        res.write_with_read_ns = write_count > 0 ? elapsed_ns / write_count : 0;
    }
    
    return res;
}

MTBenchResult bench_tktrie_mt(const std::vector<uint64_t>& keys, int num_threads) {
    using Trie = concurrent_int64_trie<int>;
    return bench_mt_generic<Trie>(
        keys, num_threads,
        [](Trie& t) { t.clear(); },
        [](Trie& t, uint64_t k, int v) { return t.insert({static_cast<int64_t>(k), v}).second; },
        [](Trie& t, uint64_t k) { return t.find(static_cast<int64_t>(k)).valid(); },
        [](Trie& t, uint64_t k) { return t.erase(static_cast<int64_t>(k)); }
    );
}

MTBenchResult bench_guarded_map_mt(const std::vector<uint64_t>& keys, int num_threads) {
    using Map = guarded_map<uint64_t, int>;
    return bench_mt_generic<Map>(
        keys, num_threads,
        [](Map& m) { m.clear(); },
        [](Map& m, uint64_t k, int v) { return m.insert(k, v); },
        [](Map& m, uint64_t k) { return m.find(k); },
        [](Map& m, uint64_t k) { return m.erase(k); }
    );
}

MTBenchResult bench_guarded_unordered_map_mt(const std::vector<uint64_t>& keys, int num_threads) {
    using Map = guarded_unordered_map<uint64_t, int>;
    return bench_mt_generic<Map>(
        keys, num_threads,
        [](Map& m) { m.clear(); },
        [](Map& m, uint64_t k, int v) { return m.insert(k, v); },
        [](Map& m, uint64_t k) { return m.find(k); },
        [](Map& m, uint64_t k) { return m.erase(k); }
    );
}

// =============================================================================
// Reporting
// =============================================================================

void print_header() {
    std::cout << std::left << std::setw(25) << "Container"
              << std::right << std::setw(12) << "Insert(ns)"
              << std::setw(12) << "Find(ns)"
              << std::setw(12) << "Erase(ns)"
              << "\n";
    std::cout << std::string(61, '-') << "\n";
}

void print_row(const std::string& name, const BenchResult& r) {
    std::cout << std::left << std::setw(25) << name
              << std::right << std::fixed << std::setprecision(1)
              << std::setw(12) << r.insert_ns
              << std::setw(12) << r.find_ns
              << std::setw(12) << r.erase_ns
              << "\n";
}

void print_mt_header() {
    std::cout << std::left << std::setw(30) << "Container"
              << std::right << std::setw(10) << "Insert"
              << std::setw(10) << "Find"
              << std::setw(10) << "Erase"
              << std::setw(12) << "ReadW/Wrt"
              << std::setw(12) << "WrtW/Read"
              << "\n";
    std::cout << std::string(84, '-') << "\n";
}

void print_mt_row(const std::string& name, const MTBenchResult& r) {
    std::cout << std::left << std::setw(30) << name
              << std::right << std::fixed << std::setprecision(1)
              << std::setw(10) << r.insert_ns
              << std::setw(10) << r.find_ns
              << std::setw(10) << r.erase_ns
              << std::setw(12) << r.read_with_write_ns
              << std::setw(12) << r.write_with_read_ns
              << "\n";
}

BenchResult average_results(const std::vector<BenchResult>& results) {
    BenchResult avg{};
    for (const auto& r : results) {
        avg.insert_ns += r.insert_ns;
        avg.find_ns += r.find_ns;
        avg.erase_ns += r.erase_ns;
    }
    avg.insert_ns /= results.size();
    avg.find_ns /= results.size();
    avg.erase_ns /= results.size();
    return avg;
}

MTBenchResult average_mt_results(const std::vector<MTBenchResult>& results) {
    MTBenchResult avg{};
    for (const auto& r : results) {
        avg.insert_ns += r.insert_ns;
        avg.find_ns += r.find_ns;
        avg.erase_ns += r.erase_ns;
        avg.read_with_write_ns += r.read_with_write_ns;
        avg.write_with_read_ns += r.write_with_read_ns;
    }
    avg.insert_ns /= results.size();
    avg.find_ns /= results.size();
    avg.erase_ns /= results.size();
    avg.read_with_write_ns /= results.size();
    avg.write_with_read_ns /= results.size();
    return avg;
}

// =============================================================================
// Main
// =============================================================================

int main() {
    std::cout << "=============================================================================\n";
    std::cout << "                    TKTRIE BENCHMARK - " << NUM_KEYS << " uint64 keys\n";
    std::cout << "=============================================================================\n\n";
    
    auto seq_keys = generate_sequential_keys(NUM_KEYS);
    auto rnd_keys = generate_random_keys(NUM_KEYS);
    
    // Shuffle for find/erase to avoid predictable patterns
    auto seq_keys_shuffled = seq_keys;
    auto rnd_keys_shuffled = rnd_keys;
    std::mt19937_64 rng(54321);
    std::shuffle(seq_keys_shuffled.begin(), seq_keys_shuffled.end(), rng);
    std::shuffle(rnd_keys_shuffled.begin(), rnd_keys_shuffled.end(), rng);
    
    // =========================================================================
    // SINGLE-THREADED BENCHMARKS
    // =========================================================================
    
    std::cout << "=== SINGLE-THREADED (THREADED=false) ===\n\n";
    
    // --- Sequential keys ---
    std::cout << "--- SEQUENTIAL KEYS ---\n";
    print_header();
    
    {
        std::vector<BenchResult> trie_results, map_results, umap_results;
        for (int i = 0; i < BENCH_ITERATIONS; ++i) {
            trie_results.push_back(bench_tktrie_st(seq_keys));
            map_results.push_back(bench_std_map(seq_keys));
            umap_results.push_back(bench_std_unordered_map(seq_keys));
        }
        print_row("tktrie", average_results(trie_results));
        print_row("std::map", average_results(map_results));
        print_row("std::unordered_map", average_results(umap_results));
    }
    std::cout << "\n";
    
    // --- Random keys ---
    std::cout << "--- RANDOM KEYS ---\n";
    print_header();
    
    {
        std::vector<BenchResult> trie_results, map_results, umap_results;
        for (int i = 0; i < BENCH_ITERATIONS; ++i) {
            trie_results.push_back(bench_tktrie_st(rnd_keys));
            map_results.push_back(bench_std_map(rnd_keys));
            umap_results.push_back(bench_std_unordered_map(rnd_keys));
        }
        print_row("tktrie", average_results(trie_results));
        print_row("std::map", average_results(map_results));
        print_row("std::unordered_map", average_results(umap_results));
    }
    std::cout << "\n";
    
    // =========================================================================
    // MULTI-THREADED BENCHMARKS
    // =========================================================================
    
    std::cout << "=== MULTI-THREADED (THREADED=true) ===\n";
    std::cout << "(ReadW/Wrt = read ns/op while writers active)\n";
    std::cout << "(WrtW/Read = write ns/op while readers active)\n\n";
    
    std::vector<int> thread_counts = {1, 2, 4};
    
    for (int threads : thread_counts) {
        std::cout << "--- SEQUENTIAL KEYS, " << threads << " THREAD(S) ---\n";
        print_mt_header();
        
        {
            std::vector<MTBenchResult> trie_results, map_results, umap_results;
            for (int i = 0; i < BENCH_ITERATIONS; ++i) {
                trie_results.push_back(bench_tktrie_mt(seq_keys, threads));
                map_results.push_back(bench_guarded_map_mt(seq_keys, threads));
                umap_results.push_back(bench_guarded_unordered_map_mt(seq_keys, threads));
            }
            print_mt_row("concurrent_tktrie", average_mt_results(trie_results));
            print_mt_row("guarded<std::map>", average_mt_results(map_results));
            print_mt_row("guarded<std::unordered_map>", average_mt_results(umap_results));
        }
        std::cout << "\n";
        
        std::cout << "--- RANDOM KEYS, " << threads << " THREAD(S) ---\n";
        print_mt_header();
        
        {
            std::vector<MTBenchResult> trie_results, map_results, umap_results;
            for (int i = 0; i < BENCH_ITERATIONS; ++i) {
                trie_results.push_back(bench_tktrie_mt(rnd_keys, threads));
                map_results.push_back(bench_guarded_map_mt(rnd_keys, threads));
                umap_results.push_back(bench_guarded_unordered_map_mt(rnd_keys, threads));
            }
            print_mt_row("concurrent_tktrie", average_mt_results(trie_results));
            print_mt_row("guarded<std::map>", average_mt_results(map_results));
            print_mt_row("guarded<std::unordered_map>", average_mt_results(umap_results));
        }
        std::cout << "\n";
    }
    
    // =========================================================================
    // SUMMARY TABLE
    // =========================================================================
    
    std::cout << "=============================================================================\n";
    std::cout << "                              SUMMARY TABLE\n";
    std::cout << "=============================================================================\n\n";
    
    std::cout << "All times in nanoseconds per operation (lower is better)\n\n";
    
    std::cout << std::left << std::setw(35) << "Scenario"
              << std::right << std::setw(12) << "tktrie"
              << std::setw(12) << "std::map"
              << std::setw(15) << "unordered_map"
              << "\n";
    std::cout << std::string(74, '=') << "\n";
    
    // Run final summary benchmarks
    auto sum_trie_seq = bench_tktrie_st(seq_keys);
    auto sum_map_seq = bench_std_map(seq_keys);
    auto sum_umap_seq = bench_std_unordered_map(seq_keys);
    
    auto sum_trie_rnd = bench_tktrie_st(rnd_keys);
    auto sum_map_rnd = bench_std_map(rnd_keys);
    auto sum_umap_rnd = bench_std_unordered_map(rnd_keys);
    
    std::cout << std::fixed << std::setprecision(1);
    
    std::cout << std::left << std::setw(35) << "ST Sequential Insert"
              << std::right << std::setw(12) << sum_trie_seq.insert_ns
              << std::setw(12) << sum_map_seq.insert_ns
              << std::setw(15) << sum_umap_seq.insert_ns << "\n";
    std::cout << std::left << std::setw(35) << "ST Sequential Find"
              << std::right << std::setw(12) << sum_trie_seq.find_ns
              << std::setw(12) << sum_map_seq.find_ns
              << std::setw(15) << sum_umap_seq.find_ns << "\n";
    std::cout << std::left << std::setw(35) << "ST Sequential Erase"
              << std::right << std::setw(12) << sum_trie_seq.erase_ns
              << std::setw(12) << sum_map_seq.erase_ns
              << std::setw(15) << sum_umap_seq.erase_ns << "\n";
    
    std::cout << std::string(74, '-') << "\n";
    
    std::cout << std::left << std::setw(35) << "ST Random Insert"
              << std::right << std::setw(12) << sum_trie_rnd.insert_ns
              << std::setw(12) << sum_map_rnd.insert_ns
              << std::setw(15) << sum_umap_rnd.insert_ns << "\n";
    std::cout << std::left << std::setw(35) << "ST Random Find"
              << std::right << std::setw(12) << sum_trie_rnd.find_ns
              << std::setw(12) << sum_map_rnd.find_ns
              << std::setw(15) << sum_umap_rnd.find_ns << "\n";
    std::cout << std::left << std::setw(35) << "ST Random Erase"
              << std::right << std::setw(12) << sum_trie_rnd.erase_ns
              << std::setw(12) << sum_map_rnd.erase_ns
              << std::setw(15) << sum_umap_rnd.erase_ns << "\n";
    
    std::cout << std::string(74, '=') << "\n\n";
    
    // MT summary for 4 threads
    auto mt_trie_seq = bench_tktrie_mt(seq_keys, 4);
    auto mt_map_seq = bench_guarded_map_mt(seq_keys, 4);
    auto mt_umap_seq = bench_guarded_unordered_map_mt(seq_keys, 4);
    
    auto mt_trie_rnd = bench_tktrie_mt(rnd_keys, 4);
    auto mt_map_rnd = bench_guarded_map_mt(rnd_keys, 4);
    auto mt_umap_rnd = bench_guarded_unordered_map_mt(rnd_keys, 4);
    
    std::cout << std::left << std::setw(35) << "Scenario (4 threads)"
              << std::right << std::setw(12) << "tktrie"
              << std::setw(12) << "g<map>"
              << std::setw(15) << "g<umap>"
              << "\n";
    std::cout << std::string(74, '=') << "\n";
    
    std::cout << std::left << std::setw(35) << "MT Sequential Insert"
              << std::right << std::setw(12) << mt_trie_seq.insert_ns
              << std::setw(12) << mt_map_seq.insert_ns
              << std::setw(15) << mt_umap_seq.insert_ns << "\n";
    std::cout << std::left << std::setw(35) << "MT Sequential Find"
              << std::right << std::setw(12) << mt_trie_seq.find_ns
              << std::setw(12) << mt_map_seq.find_ns
              << std::setw(15) << mt_umap_seq.find_ns << "\n";
    std::cout << std::left << std::setw(35) << "MT Sequential Erase"
              << std::right << std::setw(12) << mt_trie_seq.erase_ns
              << std::setw(12) << mt_map_seq.erase_ns
              << std::setw(15) << mt_umap_seq.erase_ns << "\n";
    std::cout << std::left << std::setw(35) << "MT Seq Read+Write contention"
              << std::right << std::setw(12) << mt_trie_seq.read_with_write_ns
              << std::setw(12) << mt_map_seq.read_with_write_ns
              << std::setw(15) << mt_umap_seq.read_with_write_ns << "\n";
    
    std::cout << std::string(74, '-') << "\n";
    
    std::cout << std::left << std::setw(35) << "MT Random Insert"
              << std::right << std::setw(12) << mt_trie_rnd.insert_ns
              << std::setw(12) << mt_map_rnd.insert_ns
              << std::setw(15) << mt_umap_rnd.insert_ns << "\n";
    std::cout << std::left << std::setw(35) << "MT Random Find"
              << std::right << std::setw(12) << mt_trie_rnd.find_ns
              << std::setw(12) << mt_map_rnd.find_ns
              << std::setw(15) << mt_umap_rnd.find_ns << "\n";
    std::cout << std::left << std::setw(35) << "MT Random Erase"
              << std::right << std::setw(12) << mt_trie_rnd.erase_ns
              << std::setw(12) << mt_map_rnd.erase_ns
              << std::setw(15) << mt_umap_rnd.erase_ns << "\n";
    std::cout << std::left << std::setw(35) << "MT Rnd Read+Write contention"
              << std::right << std::setw(12) << mt_trie_rnd.read_with_write_ns
              << std::setw(12) << mt_map_rnd.read_with_write_ns
              << std::setw(15) << mt_umap_rnd.read_with_write_ns << "\n";
    
    std::cout << std::string(74, '=') << "\n";
    
    return 0;
}
