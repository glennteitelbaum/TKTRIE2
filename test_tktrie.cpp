#include <iostream>
#include <cassert>
#include <string>
#include <vector>
#include <thread>
#include <random>

#include "tktrie.h"
#include "tktrie_core.h"
#include "tktrie_insert.h"
#include "tktrie_erase_probe.h"
#include "tktrie_erase.h"

using namespace gteitelbaum;

void test_basic_string_trie() {
    std::cout << "Testing basic string trie operations...\n";
    
    string_trie<int> trie;
    
    // Test empty
    assert(trie.empty());
    assert(trie.size() == 0);
    
    // Test insert
    auto [it1, inserted1] = trie.insert({"hello", 1});
    assert(inserted1);
    assert(trie.size() == 1);
    
    auto [it2, inserted2] = trie.insert({"world", 2});
    assert(inserted2);
    assert(trie.size() == 2);
    
    // Test duplicate insert
    auto [it3, inserted3] = trie.insert({"hello", 3});
    assert(!inserted3);
    assert(trie.size() == 2);
    
    // Test contains
    assert(trie.contains("hello"));
    assert(trie.contains("world"));
    assert(!trie.contains("foo"));
    
    // Test find
    auto found = trie.find("hello");
    assert(found.valid());
    assert(found.value() == 1);
    
    auto not_found = trie.find("bar");
    assert(!not_found.valid());
    
    // Test erase
    assert(trie.erase("hello"));
    assert(!trie.contains("hello"));
    assert(trie.size() == 1);
    
    assert(!trie.erase("nonexistent"));
    assert(trie.size() == 1);
    
    std::cout << "  PASSED\n";
}

void test_prefix_operations() {
    std::cout << "Testing prefix operations...\n";
    
    string_trie<int> trie;
    
    // Insert keys with common prefixes
    trie.insert({"abc", 1});
    trie.insert({"abcd", 2});
    trie.insert({"abcde", 3});
    trie.insert({"ab", 4});
    trie.insert({"a", 5});
    
    assert(trie.size() == 5);
    
    // Verify all present
    assert(trie.find("a").value() == 5);
    assert(trie.find("ab").value() == 4);
    assert(trie.find("abc").value() == 1);
    assert(trie.find("abcd").value() == 2);
    assert(trie.find("abcde").value() == 3);
    
    // Erase middle
    assert(trie.erase("abc"));
    assert(!trie.contains("abc"));
    assert(trie.contains("ab"));
    assert(trie.contains("abcd"));
    
    std::cout << "  PASSED\n";
}

void test_many_keys() {
    std::cout << "Testing many keys...\n";
    
    string_trie<int> trie;
    
    // Insert many keys to trigger list->full conversion
    for (int i = 0; i < 100; ++i) {
        std::string key = "key" + std::to_string(i);
        trie.insert({key, i});
    }
    
    assert(trie.size() == 100);
    
    // Verify all present
    for (int i = 0; i < 100; ++i) {
        std::string key = "key" + std::to_string(i);
        assert(trie.contains(key));
        auto it = trie.find(key);
        assert(it.valid());
        assert(it.value() == i);
    }
    
    // Erase half
    for (int i = 0; i < 50; ++i) {
        std::string key = "key" + std::to_string(i);
        assert(trie.erase(key));
    }
    
    assert(trie.size() == 50);
    
    // Verify correct keys remain
    for (int i = 0; i < 100; ++i) {
        std::string key = "key" + std::to_string(i);
        if (i < 50) {
            assert(!trie.contains(key));
        } else {
            assert(trie.contains(key));
        }
    }
    
    std::cout << "  PASSED\n";
}

void test_int_trie() {
    std::cout << "Testing integer key trie...\n";
    
    int32_trie<std::string> trie;
    
    trie.insert({42, "forty-two"});
    trie.insert({-1, "negative one"});
    trie.insert({0, "zero"});
    trie.insert({INT32_MAX, "max"});
    trie.insert({INT32_MIN, "min"});
    
    assert(trie.size() == 5);
    assert(trie.find(42).value() == "forty-two");
    assert(trie.find(-1).value() == "negative one");
    assert(trie.find(0).value() == "zero");
    assert(trie.find(INT32_MAX).value() == "max");
    assert(trie.find(INT32_MIN).value() == "min");
    
    std::cout << "  PASSED\n";
}

void test_copy_move() {
    std::cout << "Testing copy and move...\n";
    
    string_trie<int> trie1;
    trie1.insert({"a", 1});
    trie1.insert({"b", 2});
    
    // Copy constructor
    string_trie<int> trie2(trie1);
    assert(trie2.size() == 2);
    assert(trie2.find("a").value() == 1);
    
    // Copy assignment
    string_trie<int> trie3;
    trie3 = trie1;
    assert(trie3.size() == 2);
    
    // Move constructor
    string_trie<int> trie4(std::move(trie2));
    assert(trie4.size() == 2);
    assert(trie2.empty());
    
    // Move assignment
    string_trie<int> trie5;
    trie5 = std::move(trie3);
    assert(trie5.size() == 2);
    assert(trie3.empty());
    
    std::cout << "  PASSED\n";
}

void test_concurrent_basic() {
    std::cout << "Testing concurrent trie basic operations...\n";
    
    concurrent_string_trie<int> trie;
    
    trie.insert({"test", 1});
    assert(trie.contains("test"));
    assert(trie.find("test").value() == 1);
    assert(trie.erase("test"));
    assert(!trie.contains("test"));
    
    std::cout << "  PASSED\n";
}

void test_concurrent_multithread() {
    std::cout << "Testing concurrent trie with multiple threads...\n";
    
    concurrent_string_trie<int> trie;
    const int num_threads = 4;
    const int ops_per_thread = 100;
    
    std::vector<std::thread> threads;
    
    // Parallel inserts
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&trie, t, ops_per_thread]() {
            for (int i = 0; i < ops_per_thread; ++i) {
                std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                trie.insert({key, t * 1000 + i});
            }
        });
    }
    
    for (auto& t : threads) t.join();
    threads.clear();
    
    assert(trie.size() == num_threads * ops_per_thread);
    
    // Parallel reads
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&trie, t, ops_per_thread]() {
            for (int i = 0; i < ops_per_thread; ++i) {
                std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                assert(trie.contains(key));
                auto it = trie.find(key);
                assert(it.valid());
                assert(it.value() == t * 1000 + i);
            }
        });
    }
    
    for (auto& t : threads) t.join();
    threads.clear();
    
    // Parallel erases
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&trie, t, ops_per_thread]() {
            for (int i = 0; i < ops_per_thread; ++i) {
                std::string key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                trie.erase(key);
            }
        });
    }
    
    for (auto& t : threads) t.join();
    
    assert(trie.empty());
    
    std::cout << "  PASSED\n";
}

int main() {
    std::cout << "=== TKTRIE TEST SUITE ===\n\n";
    
    test_basic_string_trie();
    test_prefix_operations();
    test_many_keys();
    test_int_trie();
    test_copy_move();
    test_concurrent_basic();
    test_concurrent_multithread();
    
    std::cout << "\n=== ALL TESTS PASSED ===\n";
    return 0;
}
