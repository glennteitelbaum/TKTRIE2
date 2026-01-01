/**
 * @file tktrie.h
 * @brief Thread-safe trie with optimistic locking and SWAR optimizations
 * 
 * A high-performance trie implementation featuring:
 * - Lock-free reads with optimistic concurrency control
 * - SWAR (SIMD Within A Register) operations for compact child lookups
 * - Path compression for memory efficiency
 * - Support for both string and fixed-length integral keys
 * - Configurable thread safety
 * 
 * @author gteitelbaum
 * 
 * Basic Usage:
 * @code
 *     #include "tktrie.h"
 *     using namespace gteitelbaum;
 * 
 *     // String keys, non-threaded
 *     tktrie<std::string, int> trie;
 *     trie.insert({"hello", 1});
 *     trie.insert({"world", 2});
 *     
 *     if (auto it = trie.find("hello"); it != trie.end()) {
 *         std::cout << it.value() << std::endl;
 *     }
 * 
 *     // Integer keys, threaded
 *     tktrie<int64_t, std::string, true> concurrent_trie;
 *     concurrent_trie.insert({42, "answer"});
 * @endcode
 * 
 * Template Parameters:
 * - Key: Key type (std::string, std::string_view, or integral types)
 * - T: Mapped value type
 * - THREADED: Enable thread-safe operations (default: false)
 * - Allocator: Allocator for node storage (default: std::allocator<uint64_t>)
 * 
 * Thread Safety:
 * When THREADED=true:
 * - All read operations (contains, find, iterate) are lock-free
 * - Write operations use a single mutex for serialization
 * - Readers use optimistic concurrency with retry on conflict
 * - No reader blocking; writers wait for in-progress reads
 * 
 * Memory Layout:
 * Nodes are stored as contiguous arrays of uint64_t values.
 * Small branch points (1-7 children) use a compact sorted list with SWAR lookup.
 * Large branch points (8+ children) use a 256-bit bitmap with popcount indexing.
 * Path compression stores common prefixes inline to reduce node count.
 * 
 * Validation:
 * Define KTRIE_VALIDATE=1 at compile time to enable runtime invariant checks.
 * This adds overhead but catches structural errors during development.
 */

#pragma once

// Core definitions and utilities
#include "tktrie_defines.h"

// Key type traits
#include "tktrie_traits.h"

// Data pointer management
#include "tktrie_dataptr.h"

// SWAR structures
#include "tktrie_small_list.h"
#include "tktrie_popcount.h"

// Node layout and construction
#include "tktrie_node.h"

// Helper functions
#include "tktrie_help_common.h"
#include "tktrie_help_nav.h"
#include "tktrie_help_insert.h"
#include "tktrie_help_remove.h"

// Iterator
#include "tktrie_iterator.h"

// Debug utilities
#include "tktrie_debug.h"

// Main implementation
#include "tktrie_impl.h"

namespace gteitelbaum {

// Convenience type aliases

/// Non-threaded string trie
template <typename T, typename Allocator = std::allocator<uint64_t>>
using string_trie = tktrie<std::string, T, false, Allocator>;

/// Thread-safe string trie
template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_string_trie = tktrie<std::string, T, true, Allocator>;

/// Non-threaded integer trie (32-bit keys)
template <typename T, typename Allocator = std::allocator<uint64_t>>
using int32_trie = tktrie<int32_t, T, false, Allocator>;

/// Thread-safe integer trie (32-bit keys)
template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_int32_trie = tktrie<int32_t, T, true, Allocator>;

/// Non-threaded integer trie (64-bit keys)
template <typename T, typename Allocator = std::allocator<uint64_t>>
using int64_trie = tktrie<int64_t, T, false, Allocator>;

/// Thread-safe integer trie (64-bit keys)
template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_int64_trie = tktrie<int64_t, T, true, Allocator>;

}  // namespace gteitelbaum
