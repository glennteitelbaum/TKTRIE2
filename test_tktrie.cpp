#include <chrono>
#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <atomic>
#include <random>
#include <map>
#include <unordered_map>
#include <shared_mutex>
#include <algorithm>
#include "tktrie.h"

const std::vector<std::string> STRING_KEYS = {
    "the", "be", "to", "of", "and", "a", "in", "that", "have", "I",
    "it", "for", "not", "on", "with", "he", "as", "you", "do", "at",
    "this", "but", "his", "by", "from", "they", "we", "say", "her", "she",
    "or", "an", "will", "my", "one", "all", "would", "there", "their", "what",
    "so", "up", "out", "if", "about", "who", "get", "which", "go", "me",
    "when", "make", "can", "like", "time", "no", "just", "him", "know", "take",
    "people", "into", "year", "your", "good", "some", "could", "them", "see", "other",
    "than", "then", "now", "look", "only", "come", "its", "over", "think", "also",
    "back", "after", "use", "two", "how", "our", "work", "first", "well", "way",
    "even", "new", "want", "because", "any", "these", "give", "day", "most", "us",
    "is", "was", "are", "been", "has", "had", "were", "said", "each", "made",
    "does", "did", "got", "may", "part", "find", "long", "down", "many", "before",
    "must", "through", "much", "where", "should", "very", "might", "being", "such", "more",
    "those", "never", "still", "world", "last", "own", "public", "while", "next", "less",
    "both", "life", "under", "same", "right", "here", "state", "place", "high", "every"
};

std::vector<uint64_t> generate_uint64_keys(size_t count) {
    std::vector<uint64_t> keys;
    keys.reserve(count);
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<uint64_t> dist(
        std::numeric_limits<uint64_t>::min(),
        std::numeric_limits<uint64_t>::max()
    );
    for (size_t i = 0; i < count; i++) {
        keys.push_back(dist(rng));
    }
    return keys;
}

std::vector<int64_t> generate_int64_keys(size_t count) {
    std::vector<int64_t> keys;
    keys.reserve(count);
    std::mt19937_64 rng(42);  // Same seed = same bit patterns
    std::uniform_int_distribution<uint64_t> dist(
        std::numeric_limits<uint64_t>::min(),
        std::numeric_limits<uint64_t>::max()
    );
    for (size_t i = 0; i < count; i++) {
        // Reinterpret same bits as int64_t for identical distribution
        uint64_t bits = dist(rng);
        keys.push_back(static_cast<int64_t>(bits));
    }
    return keys;
}

const std::vector<uint64_t> UINT64_KEYS = generate_uint64_keys(10000);
const std::vector<int64_t> INT64_KEYS = generate_int64_keys(10000);

template<typename M>
class guarded_map {
    M data;
    mutable std::shared_mutex mtx;
public:
    bool contains(const typename M::key_type& key) const {
        std::shared_lock lock(mtx);
        return data.find(key) != data.end();
    }
    bool insert(const std::pair<typename M::key_type, typename M::mapped_type>& kv) {
        std::unique_lock lock(mtx);
        return data.insert(kv).second;
    }
    bool erase(const typename M::key_type& key) {
        std::unique_lock lock(mtx);
        return data.erase(key) > 0;
    }
};

template<typename K, typename V> using locked_map = guarded_map<std::map<K, V>>;
template<typename K, typename V> using locked_umap = guarded_map<std::unordered_map<K, V>>;

std::atomic<bool> stop{false};

template<typename Container, typename Keys>
double bench_find(Container& c, const Keys& keys, int threads, int ms) {
    std::atomic<long long> ops{0};
    stop = false;
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; t++) {
        workers.emplace_back([&]() {
            long long local = 0;
            while (!stop) { for (const auto& k : keys) { c.contains(k); local++; } }
            ops += local;
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop = true;
    for (auto& w : workers) w.join();
    return ops * 1000.0 / ms;
}

template<typename Container, typename Keys, typename V>
double bench_insert(const Keys& keys, int threads, int ms) {
    // Warmup: one full iteration per thread to prime allocator
    std::vector<std::thread> warmup_threads;
    for (int t = 0; t < threads; t++) {
        warmup_threads.emplace_back([&]() {
            Container c;
            for (const auto& k : keys) c.insert({k, V{}});
        });
    }
    for (auto& w : warmup_threads) w.join();
    
    std::atomic<long long> ops{0};
    stop = false;
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; t++) {
        workers.emplace_back([&, t]() {
            long long local = 0;
            while (!stop) {
                Container c;  // Fresh empty tree each iteration
                for (const auto& k : keys) { 
                    c.insert({k, (V)(t*10000 + local)}); 
                    local++; 
                }
            }
            ops += local;
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop = true;
    for (auto& w : workers) w.join();
    return ops * 1000.0 / ms;
}

template<typename Container, typename Keys, typename V>
double bench_erase(const Keys& keys, int threads, int ms) {
    // Warmup
    std::vector<std::thread> warmup_threads;
    for (int t = 0; t < threads; t++) {
        warmup_threads.emplace_back([&]() {
            Container c;
            for (size_t i = 0; i < keys.size(); i++) c.insert({keys[i], (V)i});
            for (const auto& k : keys) c.erase(k);
        });
    }
    for (auto& w : warmup_threads) w.join();
    
    std::atomic<long long> ops{0};
    stop = false;
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; t++) {
        workers.emplace_back([&, t]() {
            long long local = 0;
            while (!stop) {
                Container c;  // Fresh tree
                for (size_t i = 0; i < keys.size(); i++) c.insert({keys[i], (V)i});  // Populate
                for (const auto& k : keys) { 
                    c.erase(k); 
                    local++; 
                }
            }
            ops += local;
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop = true;
    for (auto& w : workers) w.join();
    return ops * 1000.0 / ms;
}

template<typename Container, typename Keys, typename V>
double bench_mixed_find(const Keys& keys, int find_threads, int write_threads, int ms) {
    Container c;
    for (size_t i = 0; i < keys.size(); i++) c.insert({keys[i], (V)i});
    std::atomic<long long> find_ops{0};
    stop = false;
    std::vector<std::thread> workers;
    for (int t = 0; t < find_threads; t++) {
        workers.emplace_back([&]() {
            long long local = 0;
            while (!stop) { for (const auto& k : keys) { c.contains(k); local++; } }
            find_ops += local;
        });
    }
    for (int t = 0; t < write_threads; t++) {
        workers.emplace_back([&, t]() {
            int i = 0;
            while (!stop) {
                for (size_t j = 0; j < keys.size(); j++) {
                    if (j % 2 == 0) c.insert({keys[j], (V)(t*10000 + i++)});
                    else c.erase(keys[j]);
                }
            }
        });
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    stop = true;
    for (auto& w : workers) w.join();
    return find_ops * 1000.0 / ms;
}

void test_signed_ordering() {
    std::cout << "## Signed Integer Ordering Test\n\n";
    gteitelbaum::tktrie<int64_t, std::string> t;
    
    std::vector<int64_t> vals = {-1000000, -100, -1, 0, 1, 100, 1000000};
    for (auto v : vals) {
        t.insert({v, "val_" + std::to_string(v)});
    }
    
    std::cout << "Inserted: -1000000, -100, -1, 0, 1, 100, 1000000\n\n";
    std::cout << "Lookup test:\n";
    for (auto v : vals) {
        auto it = t.find(v);
        std::cout << "  find(" << v << ") = " << (it.valid() ? it.value() : "NOT FOUND") << "\n";
    }
    
    // Verify ordering by checking that all are found
    bool all_found = true;
    for (auto v : vals) {
        if (!t.contains(v)) all_found = false;
    }
    std::cout << "\nAll values found: " << (all_found ? "YES" : "NO") << "\n\n";
}

template<typename Keys>
void run_benchmark(const std::string& name, const Keys& keys, int ms) {
    using K = typename Keys::value_type;
    
    std::cout << "## " << name << "\n\n";
    std::cout << "Keys: " << keys.size() << "\n\n";
    
    std::cout << "### FIND\n\n";
    std::cout << "| Threads | tktrie | std::map | std::unordered_map | tktrie/map | tktrie/umap |\n";
    std::cout << "|---------|--------|----------|-------------------|------------|-------------|\n";
    
    for (int threads : {1, 2, 4, 8}) {
        gteitelbaum::tktrie<K, int> trie;
        locked_map<K, int> lm;
        locked_umap<K, int> lu;
        for (size_t i = 0; i < keys.size(); i++) {
            trie.insert({keys[i], (int)i});
            lm.insert({keys[i], (int)i});
            lu.insert({keys[i], (int)i});
        }
        double tr = bench_find(trie, keys, threads, ms);
        double m = bench_find(lm, keys, threads, ms);
        double u = bench_find(lu, keys, threads, ms);
        printf("| %d | %.2fM | %.2fM | %.2fM | %.2fx | %.2fx |\n", 
               threads, tr/1e6, m/1e6, u/1e6, tr/m, tr/u);
    }
    
    std::cout << "\n### INSERT\n\n";
    std::cout << "| Threads | tktrie | std::map | std::unordered_map | tktrie/map | tktrie/umap |\n";
    std::cout << "|---------|--------|----------|-------------------|------------|-------------|\n";
    
    for (int threads : {1, 2, 4, 8}) {
        double tr = bench_insert<gteitelbaum::tktrie<K, int>, Keys, int>(keys, threads, ms);
        double m = bench_insert<locked_map<K, int>, Keys, int>(keys, threads, ms);
        double u = bench_insert<locked_umap<K, int>, Keys, int>(keys, threads, ms);
        printf("| %d | %.2fM | %.2fM | %.2fM | %.2fx | %.2fx |\n", 
               threads, tr/1e6, m/1e6, u/1e6, tr/m, tr/u);
    }
    
    std::cout << "\n### ERASE\n\n";
    std::cout << "| Threads | tktrie | std::map | std::unordered_map | tktrie/map | tktrie/umap |\n";
    std::cout << "|---------|--------|----------|-------------------|------------|-------------|\n";
    
    for (int threads : {1, 2, 4, 8}) {
        double tr = bench_erase<gteitelbaum::tktrie<K, int>, Keys, int>(keys, threads, ms);
        double m = bench_erase<locked_map<K, int>, Keys, int>(keys, threads, ms);
        double u = bench_erase<locked_umap<K, int>, Keys, int>(keys, threads, ms);
        printf("| %d | %.2fM | %.2fM | %.2fM | %.2fx | %.2fx |\n", 
               threads, tr/1e6, m/1e6, u/1e6, tr/m, tr/u);
    }
    
    std::cout << "\n### FIND with Concurrent Writers\n\n";
    std::cout << "| Readers | Writers | tktrie | std::map | std::unordered_map | tktrie/map | tktrie/umap |\n";
    std::cout << "|---------|---------|--------|----------|-------------------|------------|-------------|\n";
    
    for (auto [f, w] : std::vector<std::pair<int,int>>{{4,0}, {4,2}, {8,0}, {8,4}}) {
        double tr = bench_mixed_find<gteitelbaum::tktrie<K, int>, Keys, int>(keys, f, w, ms);
        double m = bench_mixed_find<locked_map<K, int>, Keys, int>(keys, f, w, ms);
        double u = bench_mixed_find<locked_umap<K, int>, Keys, int>(keys, f, w, ms);
        printf("| %d | %d | %.2fM | %.2fM | %.2fM | %.2fx | %.2fx |\n", 
               f, w, tr/1e6, m/1e6, u/1e6, tr/m, tr/u);
    }
    std::cout << "\n";
}

int main() {
    constexpr int MS = 500;
    
    std::cout << "# tktrie Benchmark Results\n\n";
    std::cout << "- Variable-length keys (string): multi-segment compression\n";
    std::cout << "- Fixed-length keys (integers): single-skip with variant leaves\n";
    std::cout << "- Duration: " << MS << "ms per test\n\n";
    
    test_signed_ordering();
    
    run_benchmark("String Keys (std::string)", STRING_KEYS, MS);
    run_benchmark("Unsigned Integer Keys (uint64_t)", UINT64_KEYS, MS);
    run_benchmark("Signed Integer Keys (int64_t)", INT64_KEYS, MS);
    
    return 0;
}
