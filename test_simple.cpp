#include <iostream>
#include <cassert>
#include "tktrie.h"

using namespace gteitelbaum;

void test_string_trie() {
    std::cout << "=== String Trie Tests ===\n";
    string_trie<int> trie;
    
    // Basic insert/find
    trie.insert({"apple", 1});
    trie.insert({"application", 2});
    trie.insert({"apply", 3});
    trie.insert({"app", 4});
    trie.insert({"banana", 5});
    
    assert(trie.size() == 5);
    assert(trie.contains("apple"));
    assert(trie.contains("application"));
    assert(trie.contains("apply"));
    assert(trie.contains("app"));
    assert(trie.contains("banana"));
    assert(!trie.contains("ap"));
    assert(!trie.contains("appl"));
    assert(!trie.contains("xyz"));
    
    std::cout << "Basic insert/find: PASS\n";
    
    // Find and get value
    auto it = trie.find("apple");
    assert(it != trie.end());
    assert(it.value() == 1);
    
    it = trie.find("application");
    assert(it != trie.end());
    assert(it.value() == 2);
    
    std::cout << "Find with value: PASS\n";
    
    // Erase
    assert(trie.erase("apple"));
    assert(!trie.contains("apple"));
    assert(trie.contains("app"));
    assert(trie.contains("application"));
    assert(trie.size() == 4);
    
    assert(!trie.erase("nonexistent"));
    
    std::cout << "Erase: PASS\n";
    
    // Duplicate insert
    auto [it2, inserted] = trie.insert({"app", 999});
    assert(!inserted);  // Already exists
    assert(trie.size() == 4);
    
    std::cout << "Duplicate insert: PASS\n";
    
    // Empty string
    trie.insert({"", 0});
    assert(trie.contains(""));
    assert(trie.size() == 5);
    
    std::cout << "Empty string: PASS\n";
    
    // Clear
    trie.clear();
    assert(trie.empty());
    assert(trie.size() == 0);
    
    std::cout << "Clear: PASS\n";
    
    std::cout << "All string trie tests PASSED!\n\n";
}

void test_integer_trie() {
    std::cout << "=== Integer Trie Tests ===\n";
    int64_trie<std::string> trie;
    
    trie.insert({100, "hundred"});
    trie.insert({-50, "neg fifty"});
    trie.insert({0, "zero"});
    trie.insert({1000000, "million"});
    trie.insert({-1000000, "neg million"});
    trie.insert({INT64_MAX, "max"});
    trie.insert({INT64_MIN, "min"});
    
    assert(trie.size() == 7);
    assert(trie.contains(100));
    assert(trie.contains(-50));
    assert(trie.contains(0));
    assert(trie.contains(INT64_MAX));
    assert(trie.contains(INT64_MIN));
    assert(!trie.contains(42));
    
    auto it = trie.find(-50);
    assert(it != trie.end());
    assert(it.value() == "neg fifty");
    
    std::cout << "Integer trie: PASS\n\n";
}

void test_large_data() {
    std::cerr << "=== Large Data Test ===\n";
    string_trie<int> trie;
    
    // Insert 1000 keys
    for (int i = 0; i < 1000; ++i) {
        trie.insert({"key" + std::to_string(i), i});
    }
    
    std::cerr << "Inserted 1000 keys, size=" << trie.size() << "\n";
    
    // Verify all present
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key" + std::to_string(i);
        if (!trie.contains(key)) {
            std::cerr << "FAIL: key '" << key << "' not found\n";
            return;
        }
        auto it = trie.find(key);
        if (it == trie.end() || it.value() != i) {
            std::cerr << "FAIL: value mismatch for '" << key << "'\n";
            return;
        }
    }
    
    std::cerr << "Large data (1000 keys): PASS\n\n";
}

int main() {
    test_string_trie();
    test_integer_trie();
    test_large_data();
    
    std::cout << "ALL TESTS PASSED!\n";
    return 0;
}
