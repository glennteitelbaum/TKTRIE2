/**
 * @file test_threaded.cpp
 * @brief Test threaded trie operations
 */

#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <cassert>
#include <chrono>

#include "tktrie.h"

using namespace gteitelbaum;

// Basic single-threaded test of threaded trie
void test_basic_threaded() {
    std::cout << "=== Basic Threaded Trie Test ===\n";
    
    concurrent_string_trie<int> trie;
    
    trie.insert({"hello", 1});
    trie.insert({"world", 2});
    trie.insert({"hell", 3});
    
    std::cout << "Size: " << trie.size() << "\n";
    
    assert(trie.contains("hello"));
    assert(trie.contains("world"));
    assert(trie.contains("hell"));
    assert(!trie.contains("xyz"));
    
    auto it = trie.find("hello");
    assert(it != trie.end());
    assert(it.value() == 1);
    
    std::cout << "Basic threaded test: PASS\n\n";
}

// Test concurrent reads
void test_concurrent_reads() {
    std::cout << "=== Concurrent Reads Test ===\n";
    
    concurrent_string_trie<int> trie;
    
    // Pre-populate
    for (int i = 0; i < 100; ++i) {
        trie.insert({"key" + std::to_string(i), i});
    }
    
    std::atomic<int> success_count{0};
    std::atomic<int> total_reads{0};
    
    auto reader = [&](int thread_id) {
        for (int i = 0; i < 1000; ++i) {
            int key_idx = (thread_id * 7 + i) % 100;
            std::string key = "key" + std::to_string(key_idx);
            
            if (trie.contains(key)) {
                auto it = trie.find(key);
                if (it != trie.end() && it.value() == key_idx) {
                    success_count.fetch_add(1);
                }
            }
            total_reads.fetch_add(1);
        }
    };
    
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back(reader, t);
    }
    
    for (auto& th : threads) {
        th.join();
    }
    
    std::cout << "Total reads: " << total_reads.load() << "\n";
    std::cout << "Successful reads: " << success_count.load() << "\n";
    assert(success_count.load() == total_reads.load());
    
    std::cout << "Concurrent reads test: PASS\n\n";
}

// Test concurrent writes
void test_concurrent_writes() {
    std::cout << "=== Concurrent Writes Test ===\n";
    
    concurrent_string_trie<int> trie;
    
    const int num_threads = 4;
    const int keys_per_thread = 250;
    
    auto writer = [&](int thread_id) {
        for (int i = 0; i < keys_per_thread; ++i) {
            std::string key = "t" + std::to_string(thread_id) + "_k" + std::to_string(i);
            int value = thread_id * 1000 + i;
            trie.insert({key, value});
        }
    };
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back(writer, t);
    }
    
    for (auto& th : threads) {
        th.join();
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    std::cout << "Inserted " << (num_threads * keys_per_thread) << " keys in " << ms << "ms\n";
    std::cout << "Trie size: " << trie.size() << "\n";
    
    assert(trie.size() == num_threads * keys_per_thread);
    
    // Verify all keys present
    int found = 0;
    for (int t = 0; t < num_threads; ++t) {
        for (int i = 0; i < keys_per_thread; ++i) {
            std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
            if (trie.contains(key)) {
                auto it = trie.find(key);
                if (it != trie.end() && it.value() == t * 1000 + i) {
                    ++found;
                }
            }
        }
    }
    
    std::cout << "Verified: " << found << "/" << (num_threads * keys_per_thread) << "\n";
    assert(found == num_threads * keys_per_thread);
    
    std::cout << "Concurrent writes test: PASS\n\n";
}

// Test mixed reads and writes
void test_mixed_concurrent() {
    std::cout << "=== Mixed Concurrent Test ===\n";
    
    concurrent_string_trie<int> trie;
    
    // Pre-populate some keys
    for (int i = 0; i < 50; ++i) {
        trie.insert({"init" + std::to_string(i), i});
    }
    
    std::atomic<bool> done{false};
    std::atomic<int> reads{0};
    std::atomic<int> writes{0};
    
    // Writer thread
    auto writer = [&]() {
        for (int i = 0; i < 500; ++i) {
            trie.insert({"new" + std::to_string(i), i + 1000});
            writes.fetch_add(1);
        }
    };
    
    // Reader thread
    auto reader = [&]() {
        while (!done.load()) {
            for (int i = 0; i < 50; ++i) {
                trie.contains("init" + std::to_string(i));
                reads.fetch_add(1);
            }
        }
    };
    
    std::vector<std::thread> readers;
    for (int i = 0; i < 3; ++i) {
        readers.emplace_back(reader);
    }
    
    std::thread write_thread(writer);
    write_thread.join();
    
    done.store(true);
    for (auto& th : readers) {
        th.join();
    }
    
    std::cout << "Writes: " << writes.load() << "\n";
    std::cout << "Reads: " << reads.load() << "\n";
    std::cout << "Final size: " << trie.size() << "\n";
    
    assert(trie.size() == 550);  // 50 init + 500 new
    
    std::cout << "Mixed concurrent test: PASS\n\n";
}

// Test concurrent erase
void test_concurrent_erase() {
    std::cout << "=== Concurrent Erase Test ===\n";
    
    concurrent_string_trie<int> trie;
    
    // Pre-populate
    for (int i = 0; i < 200; ++i) {
        trie.insert({"key" + std::to_string(i), i});
    }
    
    std::cout << "Initial size: " << trie.size() << "\n";
    
    // Half threads insert, half erase
    auto inserter = [&](int id) {
        for (int i = 0; i < 100; ++i) {
            std::string key = "new" + std::to_string(id) + "_" + std::to_string(i);
            trie.insert({key, id * 1000 + i});
        }
    };
    
    auto eraser = [&](int start) {
        for (int i = start; i < start + 50; ++i) {
            trie.erase("key" + std::to_string(i));
        }
    };
    
    std::vector<std::thread> threads;
    threads.emplace_back(inserter, 0);
    threads.emplace_back(inserter, 1);
    threads.emplace_back(eraser, 0);
    threads.emplace_back(eraser, 100);
    
    for (auto& th : threads) {
        th.join();
    }
    
    std::cout << "Final size: " << trie.size() << "\n";
    // 200 - 100 erased + 200 inserted = 300
    assert(trie.size() == 300);
    
    std::cout << "Concurrent erase test: PASS\n\n";
}

int main() {
    try {
        test_basic_threaded();
        test_concurrent_reads();
        test_concurrent_writes();
        test_mixed_concurrent();
        test_concurrent_erase();
        
        std::cout << "ALL THREADED TESTS PASSED!\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
        return 1;
    }
}
