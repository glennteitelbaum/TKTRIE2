# TKTRIE

A high-performance concurrent trie (radix tree) optimized for integer and string keys.

## Features

- **Lock-free reads**: Readers never block, even during concurrent writes
- **Per-trie isolation**: Each trie has independent epoch tracking - no global contention
- **Adaptive node types**: Five node types (SKIP, BINARY, LIST, POP, FULL) minimize memory usage
- **Skip compression**: Patricia-style path compression reduces tree depth
- **Integer key support**: Native support for int32/int64 keys with sort-preserving encoding
- **C++20**: Modern C++ with concepts, constexpr, and standard library features

## Quick Start

```cpp
#include "tktrie.h"

using namespace gteitelbaum;

// Single-threaded integer trie
int64_trie<std::string> trie;
trie.insert({42, "hello"});
trie.insert({100, "world"});

auto it = trie.find(42);
if (it.valid()) {
    std::cout << it.value() << std::endl;  // "hello"
}

// Concurrent integer trie (thread-safe)
concurrent_int64_trie<int> ctrie;
// Safe to call from multiple threads:
ctrie.insert({1, 100});
ctrie.find(1);
ctrie.erase(1);
```

## Type Aliases

| Alias | Description |
|-------|-------------|
| `string_trie<T>` | Single-threaded string key trie |
| `concurrent_string_trie<T>` | Thread-safe string key trie |
| `int32_trie<T>` | Single-threaded int32 key trie |
| `concurrent_int32_trie<T>` | Thread-safe int32 key trie |
| `int64_trie<T>` | Single-threaded int64 key trie |
| `concurrent_int64_trie<T>` | Thread-safe int64 key trie |

## Configuration Defines

### User-Configurable

| Define | Effect |
|--------|--------|
| `NDEBUG` | Disables debug assertions (standard C++) |
| `TKTRIE_INSTRUMENT_RETRIES` | Enables retry statistics collection for debugging concurrent behavior |

### Internal Constants

These are defined in `tktrie_defines.h` and control node sizing:

| Constant | Value | Description |
|----------|-------|-------------|
| `BINARY_MAX` | 2 | Maximum entries in BINARY node |
| `LIST_MAX` | 7 | Maximum entries in LIST node |
| `POP_MAX` | 32 | Maximum entries in POP node |

### Header Flags (Internal)

| Flag | Bit | Description |
|------|-----|-------------|
| `FLAG_LEAF` | 63 | Node is a leaf (stores values) |
| `FLAG_SKIP` | 62 | SKIP node type (1 entry, leaf only) |
| `FLAG_BINARY` | 61 | BINARY node type (2 entries) |
| `FLAG_LIST` | 60 | LIST node type (3-7 entries) |
| `FLAG_POP` | 59 | POP node type (8-32 entries) |
| `FLAG_POISON` | 58 | Node is retired (pending deletion) |

## Header File Hierarchy

```
tktrie.h                    ← Main include (use this)
├── tktrie_defines.h        ← Constants, utilities, small_list, bitmap256
├── tktrie_node.h           ← Node types (skip, binary, list, pop, full)
│   ├── tktrie_defines.h
│   └── tktrie_dataptr.h    ← Compressed data pointer for fixed-length keys
├── tktrie_ebr.h            ← Epoch-based reclamation utilities
└── tktrie_core.h           ← Core implementation (find, contains, clear)
    └── tktrie_insert.h     ← Insert implementation
        └── tktrie_insert_probe.h  ← Speculative insert probing
            └── tktrie_erase_probe.h   ← Speculative erase probing
                └── tktrie_erase.h     ← Erase implementation
```

### File Descriptions

| File | Lines | Description |
|------|-------|-------------|
| `tktrie.h` | ~490 | Main header - class declaration, type aliases, iterator |
| `tktrie_defines.h` | ~280 | Constants, `small_list`, `bitmap256`, endian utilities |
| `tktrie_node.h` | ~1400 | All 5 node types with leaf/interior specializations |
| `tktrie_dataptr.h` | ~110 | Compressed pointer for fixed-length key optimization |
| `tktrie_ebr.h` | ~35 | Thread slot hashing for EBR |
| `tktrie_core.h` | ~620 | Read operations, EBR cleanup, public API |
| `tktrie_insert.h` | ~350 | Insert logic and node splitting |
| `tktrie_insert_probe.h` | ~340 | Lock-free insert probing |
| `tktrie_erase_probe.h` | ~100 | Lock-free erase probing |
| `tktrie_erase.h` | ~200 | Erase logic and node collapse |

## Template Parameters

```cpp
template <typename Key, typename T, bool THREADED = false, typename Allocator = std::allocator<uint64_t>>
class tktrie;
```

| Parameter | Description |
|-----------|-------------|
| `Key` | Key type (`std::string`, `int32_t`, `int64_t`, or any integral type) |
| `T` | Value type |
| `THREADED` | `false` = single-threaded, `true` = concurrent with lock-free reads |
| `Allocator` | Allocator type (default: `std::allocator<uint64_t>`) |

## Performance

Under concurrent workloads with active writers, TKTRIE provides:

- **2-6x faster reads** than mutex-guarded `std::map` or `std::unordered_map`
- **Near-linear read scaling** with thread count (lock-free)
- **No reader blocking** regardless of write activity

For single-threaded workloads, `std::unordered_map` is typically faster due to O(1) hash lookups.

## Documentation

- [**Concepts**](./concepts.md) - Core concepts: tries, skip compression, sentinels, EBR, poison bits
- [**Architecture**](./architecture.md) - Detailed technical guide: node types, memory layouts, operation flows
- [**Comparison**](./comparison.md) - Benchmark results vs `std::map` and `std::unordered_map`

## Requirements

- C++20 compiler (GCC 10+, Clang 12+, MSVC 19.29+)
- Standard library with `<bit>`, `<concepts>`, `<atomic>`

## License

[Your license here]

## Author

G. Teitelbaum
