#include "tktrie.h"
#include <iostream>
#include <cassert>
#include <set>
#include <algorithm>

void test_basic_insert_find() {
    std::cout << "Testing basic insert and find...\n";
    tktrie<int> trie;

    trie.insert("hello", 1);
    trie.insert("hell", 2);
    trie.insert("helicopter", 3);
    trie.insert("help", 4);
    trie.insert("world", 5);

    assert(trie.size() == 5);
    assert(trie.find("hello") != nullptr);
    assert(trie.find("hello")->get_data() == 1);
    assert(trie.find("hell")->get_data() == 2);
    assert(trie.find("helicopter")->get_data() == 3);
    assert(trie.find("help")->get_data() == 4);
    assert(trie.find("world")->get_data() == 5);
    assert(trie.find("hel") == nullptr);
    assert(trie.find("notfound") == nullptr);
    std::cout << "  PASSED\n";
}

void test_insert_returns_correct_value() {
    std::cout << "Testing insert return value (new vs overwrite)...\n";
    tktrie<int> trie;

    // First inserts should return true (new entry)
    assert(trie.insert("hello", 1) == true);
    assert(trie.insert("world", 2) == true);
    assert(trie.size() == 2);

    // Overwrite should return false
    assert(trie.insert("hello", 100) == false);
    assert(trie.size() == 2);  // Size unchanged!
    assert(trie.find("hello")->get_data() == 100);  // Value updated

    // Another new insert
    assert(trie.insert("foo", 3) == true);
    assert(trie.size() == 3);
    std::cout << "  PASSED\n";
}

void test_remove_basic() {
    std::cout << "Testing basic remove...\n";
    tktrie<int> trie;

    trie.insert("hello", 1);
    trie.insert("hell", 2);
    trie.insert("helicopter", 3);
    trie.insert("help", 4);
    trie.insert("world", 5);

    // Remove a leaf node
    assert(trie.remove("helicopter") == true);
    assert(trie.find("helicopter") == nullptr);
    assert(trie.size() == 4);

    // Other entries still exist
    assert(trie.find("hello") != nullptr);
    assert(trie.find("hell") != nullptr);
    assert(trie.find("help") != nullptr);
    std::cout << "  PASSED\n";
}

void test_remove_nonexistent() {
    std::cout << "Testing remove non-existent...\n";
    tktrie<int> trie;

    trie.insert("hello", 1);
    trie.insert("hell", 2);

    assert(trie.remove("notfound") == false);
    assert(trie.remove("hel") == false);  // Prefix exists but no data
    assert(trie.size() == 2);
    std::cout << "  PASSED\n";
}

void test_remove_with_compaction() {
    std::cout << "Testing remove with compaction...\n";
    tktrie<int> trie;

    trie.insert("hello", 1);
    trie.insert("hell", 2);
    trie.insert("help", 3);

    trie.remove("hell");
    assert(trie.find("hell") == nullptr);
    assert(trie.find("hello") != nullptr);
    assert(trie.find("help") != nullptr);
    assert(trie.size() == 2);
    std::cout << "  PASSED\n";
}

void test_clear() {
    std::cout << "Testing clear...\n";
    tktrie<int> trie;

    trie.insert("hello", 1);
    trie.insert("world", 2);
    trie.insert("foo", 3);

    trie.clear();
    assert(trie.size() == 0);
    assert(trie.empty());
    assert(trie.find("hello") == nullptr);
    assert(trie.find("world") == nullptr);
    std::cout << "  PASSED\n";
}

void test_reinsert_after_clear() {
    std::cout << "Testing re-insert after clear...\n";
    tktrie<int> trie;

    trie.insert("hello", 1);
    trie.clear();
    trie.insert("new", 100);

    assert(trie.size() == 1);
    assert(trie.find("new") != nullptr);
    assert(trie.find("new")->get_data() == 100);
    std::cout << "  PASSED\n";
}

void test_destructor() {
    std::cout << "Testing destructor...\n";
    {
        tktrie<std::string> temp_trie;
        temp_trie.insert("one", "value1");
        temp_trie.insert("two", "value2");
        temp_trie.insert("three", "value3");
        temp_trie.insert("onesie", "value4");
    }
    std::cout << "  PASSED\n";
}

void test_path_compression_edge_cases() {
    std::cout << "Testing path compression edge cases...\n";
    {
        tktrie<int> t;
        t.insert("abcdefghij", 1);
        t.insert("abcdef", 2);
        t.insert("abcdefghijklmnop", 3);

        assert(t.find("abcdefghij")->get_data() == 1);
        assert(t.find("abcdef")->get_data() == 2);
        assert(t.find("abcdefghijklmnop")->get_data() == 3);

        t.remove("abcdefghij");
        assert(t.find("abcdefghij") == nullptr);
        assert(t.find("abcdef")->get_data() == 2);
        assert(t.find("abcdefghijklmnop")->get_data() == 3);

        t.remove("abcdef");
        t.remove("abcdefghijklmnop");
        assert(t.empty());
    }
    std::cout << "  PASSED\n";
}

void test_many_insertions_deletions() {
    std::cout << "Testing many insertions and deletions...\n";
    {
        tktrie<int> t;
        std::vector<std::string> keys = {
            "a", "ab", "abc", "abcd", "abcde",
            "b", "ba", "bac", "bad",
            "test", "testing", "tested", "tester",
            "x", "xy", "xyz", "xyzzy"
        };

        for (size_t i = 0; i < keys.size(); ++i) {
            t.insert(keys[i], static_cast<int>(i));
        }
        assert(t.size() == keys.size());

        for (size_t i = 0; i < keys.size(); ++i) {
            auto* node = t.find(keys[i]);
            assert(node != nullptr);
            assert(node->get_data() == static_cast<int>(i));
        }

        for (size_t i = 0; i < keys.size(); i += 2) {
            assert(t.remove(keys[i]));
        }

        for (size_t i = 0; i < keys.size(); ++i) {
            if (i % 2 == 0) {
                assert(t.find(keys[i]) == nullptr);
            } else {
                assert(t.find(keys[i]) != nullptr);
            }
        }

        t.clear();
        assert(t.empty());
        for (const auto& k : keys) {
            assert(t.find(k) == nullptr);
        }
    }
    std::cout << "  PASSED\n";
}

void test_iterator_basic() {
    std::cout << "Testing basic iteration...\n";
    tktrie<int> trie;

    trie.insert("cat", 1);
    trie.insert("car", 2);
    trie.insert("card", 3);
    trie.insert("care", 4);
    trie.insert("careful", 5);
    trie.insert("dog", 6);

    std::vector<std::string> keys;
    std::vector<int> values;

    for (auto it = trie.begin(); it != trie.end(); ++it) {
        auto [key, value] = *it;
        keys.push_back(key);
        values.push_back(value);
    }

    // Should be in lexicographic order
    assert(keys.size() == 6);
    std::vector<std::string> expected_keys = {"car", "card", "care", "careful", "cat", "dog"};
    assert(keys == expected_keys);
    std::cout << "  PASSED\n";
}

void test_iterator_range_for() {
    std::cout << "Testing range-for iteration...\n";
    tktrie<int> trie;

    trie.insert("apple", 1);
    trie.insert("application", 2);
    trie.insert("banana", 3);

    int count = 0;
    for (auto [key, value] : trie) {
        ++count;
        assert(!key.empty());
    }
    assert(count == 3);
    std::cout << "  PASSED\n";
}

void test_iterator_empty_trie() {
    std::cout << "Testing iteration on empty trie...\n";
    tktrie<int> trie;

    int count = 0;
    for (auto it = trie.begin(); it != trie.end(); ++it) {
        ++count;
    }
    assert(count == 0);
    assert(trie.begin() == trie.end());
    std::cout << "  PASSED\n";
}

void test_iterator_single_element() {
    std::cout << "Testing iteration with single element...\n";
    tktrie<int> trie;
    trie.insert("only", 42);

    std::vector<std::string> keys;
    for (auto [key, value] : trie) {
        keys.push_back(key);
        assert(value == 42);
    }
    assert(keys.size() == 1);
    assert(keys[0] == "only");
    std::cout << "  PASSED\n";
}

void test_iterator_modification() {
    std::cout << "Testing value modification through iterator...\n";
    tktrie<int> trie;

    trie.insert("one", 1);
    trie.insert("two", 2);
    trie.insert("three", 3);

    for (auto it = trie.begin(); it != trie.end(); ++it) {
        auto [key, value] = *it;
        value *= 10;  // Modify through reference
    }

    assert(trie.find("one")->get_data() == 10);
    assert(trie.find("two")->get_data() == 20);
    assert(trie.find("three")->get_data() == 30);
    std::cout << "  PASSED\n";
}

void test_const_iterator() {
    std::cout << "Testing const iteration...\n";
    tktrie<int> trie;

    trie.insert("a", 1);
    trie.insert("b", 2);
    trie.insert("c", 3);

    const tktrie<int>& const_trie = trie;

    int sum = 0;
    for (auto it = const_trie.begin(); it != const_trie.end(); ++it) {
        auto [key, value] = *it;
        sum += value;
    }
    assert(sum == 6);
    std::cout << "  PASSED\n";
}

void test_iterator_after_modifications() {
    std::cout << "Testing iteration after insertions and deletions...\n";
    tktrie<int> trie;

    trie.insert("aaa", 1);
    trie.insert("aab", 2);
    trie.insert("aba", 3);
    trie.insert("abb", 4);
    trie.insert("baa", 5);

    trie.remove("aab");
    trie.remove("aba");

    std::set<std::string> remaining;
    for (auto [key, value] : trie) {
        remaining.insert(key);
    }

    assert(remaining.size() == 3);
    assert(remaining.count("aaa") == 1);
    assert(remaining.count("abb") == 1);
    assert(remaining.count("baa") == 1);
    std::cout << "  PASSED\n";
}

void test_contains() {
    std::cout << "Testing contains...\n";
    tktrie<int> trie;

    trie.insert("hello", 1);
    trie.insert("world", 2);

    assert(trie.contains("hello") == true);
    assert(trie.contains("world") == true);
    assert(trie.contains("hel") == false);
    assert(trie.contains("foo") == false);
    std::cout << "  PASSED\n";
}

void test_subscript_operator() {
    std::cout << "Testing subscript operator...\n";
    tktrie<int> trie;

    trie["hello"] = 42;
    assert(trie.find("hello")->get_data() == 42);

    trie["hello"] = 100;
    assert(trie.find("hello")->get_data() == 100);

    // Access non-existent creates default
    int val = trie["new_key"];
    assert(val == 0);  // Default int
    assert(trie.contains("new_key"));
    std::cout << "  PASSED\n";
}

void test_keys_with_prefix() {
    std::cout << "Testing keys_with_prefix...\n";
    tktrie<int> trie;

    trie.insert("car", 1);
    trie.insert("card", 2);
    trie.insert("care", 3);
    trie.insert("careful", 4);
    trie.insert("cars", 5);
    trie.insert("cat", 6);
    trie.insert("dog", 7);

    auto car_keys = trie.keys_with_prefix("car");
    std::sort(car_keys.begin(), car_keys.end());

    assert(car_keys.size() == 5);
    std::vector<std::string> expected = {"car", "card", "care", "careful", "cars"};
    assert(car_keys == expected);

    auto ca_keys = trie.keys_with_prefix("ca");
    assert(ca_keys.size() == 6);  // car, card, care, careful, cars, cat

    auto dog_keys = trie.keys_with_prefix("dog");
    assert(dog_keys.size() == 1);

    auto empty_keys = trie.keys_with_prefix("xyz");
    assert(empty_keys.empty());

    auto all_keys = trie.keys_with_prefix("");
    assert(all_keys.size() == 7);
    std::cout << "  PASSED\n";
}

void test_empty_string_key() {
    std::cout << "Testing empty string key...\n";
    tktrie<int> trie;

    trie.insert("", 999);
    assert(trie.find("") != nullptr);
    assert(trie.find("")->get_data() == 999);
    assert(trie.size() == 1);

    trie.insert("hello", 1);
    assert(trie.size() == 2);

    assert(trie.remove("") == true);
    assert(trie.find("") == nullptr);
    assert(trie.size() == 1);
    assert(trie.find("hello") != nullptr);
    std::cout << "  PASSED\n";
}

void test_special_characters() {
    std::cout << "Testing special characters in keys...\n";
    tktrie<int> trie;

    trie.insert("hello\nworld", 1);
    trie.insert("hello\tworld", 2);
    trie.insert("hello world", 3);
    trie.insert("\x00test", 4);  // Null character (won't work with std::string)
    trie.insert("\xff\xfe", 5);  // High bytes

    assert(trie.find("hello\nworld")->get_data() == 1);
    assert(trie.find("hello\tworld")->get_data() == 2);
    assert(trie.find("hello world")->get_data() == 3);
    assert(trie.find("\xff\xfe")->get_data() == 5);
    std::cout << "  PASSED\n";
}

void test_long_keys() {
    std::cout << "Testing long keys...\n";
    tktrie<int> trie;

    std::string long_key(1000, 'a');
    std::string long_key2 = long_key + "b";

    trie.insert(long_key, 1);
    trie.insert(long_key2, 2);

    assert(trie.find(long_key)->get_data() == 1);
    assert(trie.find(long_key2)->get_data() == 2);
    assert(trie.size() == 2);
    std::cout << "  PASSED\n";
}

void test_iterator_ordering_comprehensive() {
    std::cout << "Testing comprehensive iterator ordering...\n";
    tktrie<int> trie;

    // Insert in random order
    std::vector<std::string> keys = {
        "zebra", "apple", "app", "application", "apply",
        "banana", "band", "bandana", "zoo", "zoom"
    };

    for (size_t i = 0; i < keys.size(); ++i) {
        trie.insert(keys[i], static_cast<int>(i));
    }

    // Collect via iteration
    std::vector<std::string> iterated;
    for (auto [key, value] : trie) {
        iterated.push_back(key);
    }

    // Should be sorted
    std::vector<std::string> sorted_keys = keys;
    std::sort(sorted_keys.begin(), sorted_keys.end());

    assert(iterated == sorted_keys);
    std::cout << "  PASSED\n";
}

void test_parent_pointers_valid() {
    std::cout << "Testing parent pointer validity...\n";
    tktrie<int> trie;

    trie.insert("a", 1);
    trie.insert("ab", 2);
    trie.insert("abc", 3);
    trie.insert("abd", 4);

    // Traverse using iterators and verify we can go back up
    for (auto it = trie.begin(); it != trie.end(); ++it) {
        auto* n = it.get_node();
        std::string reconstructed;

        // Walk up to root
        while (n->get_parent() != nullptr) {
            reconstructed = n->get_parent_edge() + n->get_skip() + reconstructed;
            n = n->get_parent();
        }
        reconstructed = n->get_skip() + reconstructed;

        assert(reconstructed == it.key());
    }
    std::cout << "  PASSED\n";
}

int main() {
    std::cout << "=== tktrie Test Suite ===\n\n";

    test_basic_insert_find();
    test_insert_returns_correct_value();
    test_remove_basic();
    test_remove_nonexistent();
    test_remove_with_compaction();
    test_clear();
    test_reinsert_after_clear();
    test_destructor();
    test_path_compression_edge_cases();
    test_many_insertions_deletions();

    std::cout << "\n--- Iterator Tests ---\n";
    test_iterator_basic();
    test_iterator_range_for();
    test_iterator_empty_trie();
    test_iterator_single_element();
    test_iterator_modification();
    test_const_iterator();
    test_iterator_after_modifications();

    std::cout << "\n--- Additional API Tests ---\n";
    test_contains();
    test_subscript_operator();
    test_keys_with_prefix();

    std::cout << "\n--- Edge Case Tests ---\n";
    test_empty_string_key();
    test_special_characters();
    test_long_keys();
    test_iterator_ordering_comprehensive();
    test_parent_pointers_valid();

    std::cout << "\n=== All tests passed! ===\n";
    return 0;
}
