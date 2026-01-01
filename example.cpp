/**
 * @file example.cpp
 * @brief Example usage of tktrie
 * 
 * Compile with:
 *   g++ -std=c++20 -O2 -o example example.cpp
 */

#include <iostream>
#include <string>

#include "tktrie.h"

using namespace gteitelbaum;

void example_string_trie() {
    std::cout << "=== String Trie Example ===\n";
    
    // Create a non-threaded string trie
    string_trie<int> trie;
    
    // Insert some values
    trie.insert({"apple", 1});
    trie.insert({"application", 2});
    trie.insert({"apply", 3});
    trie.insert({"banana", 4});
    trie.insert({"band", 5});
    trie.insert({"bandana", 6});
    
    std::cout << "Size: " << trie.size() << "\n";
    
    // Lookup
    if (auto it = trie.find("apple"); it != trie.end()) {
        std::cout << "Found 'apple' = " << it.value() << "\n";
    }
    
    if (auto it = trie.find("app"); it != trie.end()) {
        std::cout << "Found 'app' = " << it.value() << "\n";
    } else {
        std::cout << "'app' not found\n";
    }
    
    // Contains
    std::cout << "Contains 'banana': " << (trie.contains("banana") ? "yes" : "no") << "\n";
    std::cout << "Contains 'cherry': " << (trie.contains("cherry") ? "yes" : "no") << "\n";
    
    // Erase
    trie.erase("apple");
    std::cout << "After erasing 'apple', contains: " 
              << (trie.contains("apple") ? "yes" : "no") << "\n";
    std::cout << "Size after erase: " << trie.size() << "\n";
    
    // Pretty print (for debugging)
    std::cout << "\nTree structure:\n";
    trie.pretty_print(std::cout);
    std::cout << "\n";
}

void example_integer_trie() {
    std::cout << "=== Integer Trie Example ===\n";
    
    // Create a trie with 64-bit integer keys
    int64_trie<std::string> trie;
    
    // Insert values
    trie.insert({100, "one hundred"});
    trie.insert({-50, "negative fifty"});
    trie.insert({0, "zero"});
    trie.insert({1000000, "one million"});
    trie.insert({-1000000, "negative one million"});
    
    std::cout << "Size: " << trie.size() << "\n";
    
    // Lookup
    for (int64_t key : {100LL, -50LL, 0LL, 42LL}) {
        if (auto it = trie.find(key); it != trie.end()) {
            std::cout << "Found " << key << " = \"" << it.value() << "\"\n";
        } else {
            std::cout << key << " not found\n";
        }
    }
    
    std::cout << "\n";
}

void example_emplace() {
    std::cout << "=== Emplace Example ===\n";
    
    // Trie storing complex objects
    struct Data {
        int x, y;
        std::string name;
        
        Data() : x(0), y(0) {}
        Data(int x_, int y_, std::string n) : x(x_), y(y_), name(std::move(n)) {}
    };
    
    string_trie<Data> trie;
    
    // Emplace constructs in-place
    trie.emplace("point1", 10, 20, "first");
    trie.emplace("point2", 30, 40, "second");
    
    if (auto it = trie.find("point1"); it != trie.end()) {
        std::cout << "point1: x=" << it.value().x 
                  << ", y=" << it.value().y 
                  << ", name=" << it.value().name << "\n";
    }
    
    std::cout << "\n";
}

int main() {
    example_string_trie();
    example_integer_trie();
    example_emplace();
    
    std::cout << "All examples completed.\n";
    return 0;
}
