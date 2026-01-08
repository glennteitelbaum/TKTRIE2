# TKTRIE Architecture

A detailed technical guide to the internal architecture of TKTRIE, a concurrent trie optimized for integer and string keys.

---

## Table of Contents

1. [Overview](#overview)
2. [Node Architecture](#node-architecture)
3. [Header Format](#header-format)
4. [Node Types in Detail](#node-types-in-detail)
5. [Key Encoding](#key-encoding)
6. [Skip Compression](#skip-compression)
7. [Memory Layouts](#memory-layouts)
8. [Operation Flows](#operation-flows)
9. [Concurrency Model](#concurrency-model)
10. [Epoch-Based Reclamation](#epoch-based-reclamation)
11. [Pseudocode Reference](#pseudocode-reference)

---

## Overview

TKTRIE is a radix trie with three key innovations:

1. **Skip Compression**: Chains of single-child nodes collapse into skip strings
2. **Adaptive Node Types**: SKIP → LIST → FULL based on child count
3. **Lock-Free Reads**: Readers never block, even during concurrent writes

```
┌───────────────────────────────────────────────────────────────┐
│                       TKTRIE STRUCTURE                        │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  Root ──► [Interior Node] ──► [Interior Node] ──► [Leaf]     │
│                 │                    │                        │
│                 ▼                    ▼                        │
│           [Leaf Node]          [Leaf Node]                    │
│                                                               │
│  • Interior nodes: LIST (1-7 children) or FULL (8-256)       │
│  • Leaf nodes: SKIP (single value) or LIST/FULL (multi)      │
│  • Skip strings compress common prefixes                      │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

---

## Node Architecture

### Node Type Hierarchy

```
                           node_base<T>
                                │
        ┌───────────┬───────────┼───────────┬───────────┐
        │           │           │           │           │
    skip_node   binary_node  list_node   pop_node   full_node
   (LEAF only)  (LEAF/INT)  (LEAF/INT)  (LEAF/INT)  (LEAF/INT)
     1 entry    2 entries   3-7 entries 8-32 entries 33-256 entries
```

All nodes share a common `node_base` with a 64-bit header. The header determines:
- Whether the node is a leaf or interior
- Which concrete type (SKIP, BINARY, LIST, POP, FULL)
- Poison status (for safe reclamation)
- Version number (for optimistic concurrency)

### Node Type Selection

| Entry Count | Leaf Type | Interior Type | Size (Leaf/Int) |
|-------------|-----------|---------------|-----------------|
| 1 (single value) | SKIP | N/A | 24 B |
| 2 | BINARY | BINARY | 32 B / 40 B |
| 3-7 | LIST | LIST | 56 B / 80 B |
| 8-32 | POP | POP | 176 B / 304 B |
| 33-256 | FULL | FULL | 1 KB / 2 KB |

---

## Header Format

The 64-bit header packs type flags and version into a single atomic word:

```
┌────────────────────────────────────────────────────────────────────────┐
│                         64-bit Header Layout                           │
├──────┬──────┬────────┬──────┬──────┬──────┬─────────────────────────────┤
│  63  │  62  │   61   │  60  │  59  │  58  │  57                      0  │
├──────┼──────┼────────┼──────┼──────┼──────┼─────────────────────────────┤
│ LEAF │ SKIP │ BINARY │ LIST │ POP  │ POIS │      VERSION (58 bits)      │
└──────┴──────┴────────┴──────┴──────┴──────┴─────────────────────────────┘

LEAF (bit 63):   1 = leaf node, 0 = interior node
SKIP (bit 62):   1 = skip_node type (always leaf)
BINARY (bit 61): 1 = binary_node type (2 entries)
LIST (bit 60):   1 = list_node type (3-7 entries)
POP (bit 59):    1 = pop_node type (8-32 entries)
POIS (bit 58):   1 = poisoned (pending deletion)
VERSION:         58-bit counter, incremented on modification
```

### Flag Combinations

| LEAF | SKIP | BINARY | LIST | POP | Node Type | Description |
|------|------|--------|------|-----|-----------|-------------|
| 1 | 1 | 0 | 0 | 0 | skip_node (leaf) | Single key-value pair |
| 1 | 0 | 1 | 0 | 0 | binary_node (leaf) | 2 values indexed by char |
| 1 | 0 | 0 | 1 | 0 | list_node (leaf) | 3-7 values indexed by char |
| 1 | 0 | 0 | 0 | 1 | pop_node (leaf) | 8-32 values, popcount indexed |
| 1 | 0 | 0 | 0 | 0 | full_node (leaf) | 33-256 values, direct indexed |
| 0 | 0 | 1 | 0 | 0 | binary_node (interior) | 2 child pointers |
| 0 | 0 | 0 | 1 | 0 | list_node (interior) | 3-7 child pointers |
| 0 | 0 | 0 | 0 | 1 | pop_node (interior) | 8-32 child pointers |
| 0 | 0 | 0 | 0 | 0 | full_node (interior) | 33-256 child pointers |

Note: SKIP nodes are always leaves (cannot have children).

### Header Operations

```cpp
// Constants
static constexpr uint64_t FLAG_LEAF   = 1ULL << 63;
static constexpr uint64_t FLAG_SKIP   = 1ULL << 62;
static constexpr uint64_t FLAG_BINARY = 1ULL << 61;
static constexpr uint64_t FLAG_LIST   = 1ULL << 60;
static constexpr uint64_t FLAG_POP    = 1ULL << 59;
static constexpr uint64_t FLAG_POISON = 1ULL << 58;
static constexpr uint64_t VERSION_MASK = (1ULL << 58) - 1;

// Extract version
uint64_t get_version(uint64_t h) { return h & VERSION_MASK; }

// Bump version (preserves all flags including poison)
uint64_t bump_version(uint64_t h) {
    uint64_t flags = h & ~VERSION_MASK;
    uint64_t ver = (h & VERSION_MASK) + 1;
    return flags | (ver & VERSION_MASK);
}

// Poison also bumps version (critical for validation)
void poison() {
    uint64_t h = header_.load();
    header_.store(bump_version(h) | FLAG_POISON);
}
```

---

## Node Types in Detail

### skip_node (Leaf Only, 1 entry)

Stores a single key-value pair. The "skip" string holds remaining key bytes.

```cpp
struct skip_node : node_base {
    T           leaf_value;   // The stored value
    std::string skip;         // Remaining key bytes (or fixed array for integer keys)
};
```

**Memory Layout (24 bytes for int keys):**
```
┌─────────────────────────────────────────────────┐
│                   skip_node                     │
├──────────────┬────────────────┬─────────────────┤
│    header    │   leaf_value   │   skip_bytes    │
│   (8 bytes)  │   (sizeof T)   │   (8 bytes)     │
└──────────────┴────────────────┴─────────────────┘
```

### binary_node (Leaf or Interior, 2 entries)

Stores exactly 2 entries with minimal overhead.

```cpp
struct binary_node : node_base {
    T*          eos_ptr;      // End-of-string value (interior only, string keys)
    unsigned char chars[2];   // The two characters
    union {
        std::array<T, 2>          leaf_values;  // If leaf
        std::array<atomic_ptr, 2> children;     // If interior
    };
    // skip stored inline or via fixed array
};
```

**Memory Layout (32 bytes leaf, 40 bytes interior for int keys):**
```
┌───────────────────────────────────────────────────────────────┐
│                      binary_node (leaf)                       │
├──────────────┬────────────────┬───────────────┬───────────────┤
│    header    │   skip_bytes   │  chars[0,1]   │  values[0,1]  │
│   (8 bytes)  │   (8 bytes)    │   (2 bytes)   │  (2*sizeof T) │
└──────────────┴────────────────┴───────────────┴───────────────┘
```

### list_node (Leaf or Interior, 3-7 entries)

Stores 3-7 entries, with characters packed into a 64-bit word.

```cpp
struct list_node : node_base {
    T*          eos_ptr;      // End-of-string value (interior only)
    small_list  chars;        // Packed list of 3-7 chars
    union {
        std::array<T, 7>          leaf_values;  // If leaf
        std::array<atomic_ptr, 7> children;     // If interior
    };
    // skip stored inline or via fixed array
};
```

**small_list Layout (8 bytes):**
```
┌────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┐
│ count  │ char_6 │ char_5 │ char_4 │ char_3 │ char_2 │ char_1 │ char_0 │
│ 8 bits │ 8 bits │ 8 bits │ 8 bits │ 8 bits │ 8 bits │ 8 bits │ 8 bits │
└────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┘
```

**Memory Layout (56 bytes leaf, 80 bytes interior for int keys):**
```
┌───────────────────────────────────────────────────────────────────────┐
│                        list_node (leaf)                               │
├──────────────┬────────────────┬───────────────┬────────────────────────┤
│    header    │   skip_bytes   │  small_list   │     values[0..6]       │
│   (8 bytes)  │   (8 bytes)    │   (8 bytes)   │     (7 * sizeof T)     │
└──────────────┴────────────────┴───────────────┴────────────────────────┘
```

### pop_node (Leaf or Interior, 8-32 entries)

Uses popcount indexing for efficient sparse storage.

```cpp
struct pop_node : node_base {
    T*          eos_ptr;      // End-of-string value (interior only)
    uint32_t    valid;        // 32-bit bitmap of valid entries
    union {
        std::array<T, 32>          leaf_values;  // Sparse, indexed via popcount
        std::array<atomic_ptr, 32> children;     // Sparse, indexed via popcount
    };
    // skip stored inline or via fixed array
};
```

**Popcount Indexing:**
```
valid = 0b00001001_00100001  // chars 0, 5, 8, 11 are valid

To find index for char c:
  if (valid & (1 << c)) {
      index = popcount(valid & ((1 << c) - 1));  // Count bits below c
  }

Example: char 8 → popcount(0b00100001) = 2 → values[2]
```

**Memory Layout (176 bytes leaf, 304 bytes interior for int keys):**
```
┌───────────────────────────────────────────────────────────────────────────┐
│                          pop_node (leaf)                                  │
├──────────────┬────────────────┬───────────────┬────────────────────────────┤
│    header    │   skip_bytes   │  valid (32b)  │       values[0..31]        │
│   (8 bytes)  │   (8 bytes)    │   (4 bytes)   │      (32 * sizeof T)       │
└──────────────┴────────────────┴───────────────┴────────────────────────────┘
```

### full_node (Leaf or Interior, 33-256 entries)

Direct array indexing for maximum branching factor.

```cpp
struct full_node : node_base {
    T*          eos_ptr;      // End-of-string value (interior only)
    bitmap256   valid;        // Which indices are valid
    union {
        std::array<T, 256>          leaf_values;  // Direct indexed by char
        std::array<atomic_ptr, 256> children;     // Direct indexed by char
    };
    // skip stored inline or via fixed array
};
```

**bitmap256 Layout (32 bytes):**
```
┌───────────────────────────────────────────────────────────────┐
│                     bitmap256 (256 bits)                      │
├───────────────┬───────────────┬───────────────┬───────────────┤
│   bits_[0]    │   bits_[1]    │   bits_[2]    │   bits_[3]    │
│  chars 0-63   │ chars 64-127  │ chars 128-191 │ chars 192-255 │
│   (64 bits)   │   (64 bits)   │   (64 bits)   │   (64 bits)   │
└───────────────┴───────────────┴───────────────┴───────────────┘
```

**Memory Layout (1072 bytes leaf, 2096 bytes interior for int keys):**
```
┌───────────────────────────────────────────────────────────────────────────┐
│                          full_node (leaf)                                 │
├──────────────┬────────────────┬───────────────┬────────────────────────────┤
│    header    │   skip_bytes   │  bitmap256    │       values[0..255]       │
│   (8 bytes)  │   (8 bytes)    │  (32 bytes)   │     (256 * sizeof T)       │
└──────────────┴────────────────┴───────────────┴────────────────────────────┘
```

---

## Key Encoding

### String Keys

Strings are used directly as byte sequences:

```cpp
template<>
struct tktrie_traits<std::string> {
    static std::string_view to_bytes(const std::string& k) { return k; }
    static std::string from_bytes(std::string_view b) { return std::string(b); }
};
```

### Integer Keys

Integers are encoded as big-endian bytes with sign bit flipped (for correct ordering):

```cpp
template<typename T> requires std::is_integral_v<T>
struct tktrie_traits<T> {
    static std::string to_bytes(T k) {
        using U = std::make_unsigned_t<T>;
        U sortable;
        if constexpr (std::is_signed_v<T>)
            sortable = static_cast<U>(k) ^ (U{1} << (sizeof(T)*8 - 1));
        else
            sortable = k;
        U be = to_big_endian(sortable);
        char buf[sizeof(T)];
        std::memcpy(buf, &be, sizeof(T));
        return std::string(buf, sizeof(T));
    }
};
```

**Examples (int32_t → bytes):**
```
INT32_MIN (-2147483648) → 0x00 0x00 0x00 0x00
         -1             → 0x7F 0xFF 0xFF 0xFF
          0             → 0x80 0x00 0x00 0x00
          1             → 0x80 0x00 0x00 0x01
INT32_MAX (2147483647)  → 0xFF 0xFF 0xFF 0xFF
```

This encoding ensures lexicographic byte order matches numeric order.

---

## Skip Compression

Skip compression collapses chains of single-child nodes:

### Without Skip Compression

```
Inserting "hello":

    [root]
       │'h'
       ▼
    [node]
       │'e'
       ▼
    [node]
       │'l'
       ▼
    [node]
       │'l'
       ▼
    [node]
       │'o'
       ▼
    [leaf: value]

Depth: 5 nodes
```

### With Skip Compression

```
Inserting "hello":

    [root]
       │
       ▼
    [skip_node]
    skip: "hello"
    value: ...

Depth: 1 node
```

### Skip Split Example

```
Initial: skip_node with skip="hello", value=1

Insert "help" with value=2:

    [interior LIST]
    skip: "hel"
    children:
      'l' → [skip_node: skip="o", value=1]
      'p' → [skip_node: skip="", value=2]
```

**Visualization:**
```
Before:                          After:
                                 
[skip:"hello",v=1]      →      [interior, skip:"hel"]
                                      │
                           ┌──────────┴──────────┐
                          'l'                   'p'
                           │                     │
                           ▼                     ▼
                 [skip:"o",v=1]          [skip:"",v=2]
```

---

## Memory Layouts

### Example: Trie with keys 100, 150, 200 (int64)

```
Keys as bytes (big-endian with sign flip):
  100 → 0x80 0x00 0x00 0x00 0x00 0x00 0x00 0x64
  150 → 0x80 0x00 0x00 0x00 0x00 0x00 0x00 0x96
  200 → 0x80 0x00 0x00 0x00 0x00 0x00 0x00 0xC8

Tree structure:

                   [root: interior LIST]
                   skip: "\x80\x00\x00\x00\x00\x00\x00"
                   (common 7-byte prefix)
                             │
                   ┌─────────┼─────────┐
                 0x64      0x96      0xC8
                   │         │         │
                   ▼         ▼         ▼
           [skip:""]  [skip:""]  [skip:""]
           v=100      v=150      v=200
```

### Example: Trie with keys "cat", "car", "card"

```
                   [root: interior LIST]
                   skip: "ca"
                             │
                   ┌─────────┴─────────┐
                  'r'                 't'
                   │                   │
                   ▼                   ▼
       [interior LIST]           [skip:""]
       skip: ""                  v="cat"
       eos_ptr: → v="car"
             │
            'd'
             │
             ▼
       [skip:""]
       v="card"
```

---

## Operation Flows

### Find Operation

```
┌───────────────────────────────────────────────────────────────┐
│                          FIND FLOW                            │
└───────────────────────────────────────────────────────────────┘

  Input: key = "card"
  
  ┌────────────────┐
  │  Start at root │
  └───────┬────────┘
          │
          ▼
  ┌────────────────┐       No       ┌────────────────┐
  │  Node null?    │───────────────►│ Return: false  │
  └───────┬────────┘                └────────────────┘
          │ Yes, continue
          ▼
  ┌────────────────┐
  │  Match skip    │  key="card", skip="ca"
  │    prefix      │  match=2, consume "ca"
  └───────┬────────┘  remaining key="rd"
          │
          ▼
  ┌────────────────┐       No       ┌────────────────┐
  │  Full match?   │───────────────►│ Return: false  │
  │ (m==skip.len)  │                └────────────────┘
  └───────┬────────┘
          │ Yes
          ▼
  ┌────────────────┐       Yes      ┌────────────────┐
  │  Key empty?    │───────────────►│  Return eos    │
  └───────┬────────┘                │    value       │
          │ No                      └────────────────┘
          ▼
  ┌────────────────┐
  │ Consume char   │  c='r', remaining="d"
  │  c = key[0]    │
  └───────┬────────┘
          │
          ▼
  ┌────────────────┐
  │  Find child    │  Look up children['r']
  │    for 'c'     │
  └───────┬────────┘
          │
          ▼
  ┌────────────────┐       No       ┌────────────────┐
  │ Child found?   │───────────────►│ Return: false  │
  └───────┬────────┘                └────────────────┘
          │ Yes
          ▼
  ┌────────────────┐
  │  Recurse with  │
  │  child, key-1  │───► (back to top with key="d")
  └────────────────┘
```

### Insert Operation

```
┌───────────────────────────────────────────────────────────────┐
│                         INSERT FLOW                           │
└───────────────────────────────────────────────────────────────┘

  Input: key, value
  
  ┌────────────────┐
  │  Start at root │
  └───────┬────────┘
          │
          ▼
  ┌────────────────┐       Yes      ┌────────────────┐
  │  Node null?    │───────────────►│  Create new    │
  │                │                │   skip_node    │
  └───────┬────────┘                └────────────────┘
          │ No
          ▼
  ┌────────────────┐
  │  Match skip    │
  │    prefix      │
  └───────┬────────┘
          │
     ┌────┴────┬──────────┬──────────┐
     │         │          │          │
     ▼         ▼          ▼          ▼
   FULL    PARTIAL     PREFIX    DIVERGE
   MATCH    MATCH      MATCH     (split)
     │         │          │          │
     ▼         ▼          ▼          ▼
┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
│Continue│ │  N/A   │ │New int │ │ Split  │
│to child│ │ (leaf) │ │w/ eos  │ │  node  │
└────────┘ └────────┘ └────────┘ └────────┘

SPLIT EXAMPLE:
  
  Before: skip_node(skip="hello", value=1)
  Insert: key="help", value=2
  
  After:
    interior_list(skip="hel")
        ├─'l'→ skip_node(skip="o", value=1)
        └─'p'→ skip_node(skip="", value=2)
```

### Insert Decision Tree

```
┌───────────────────────────────────────────────────────────────┐
│                    INSERT DECISION TREE                       │
└───────────────────────────────────────────────────────────────┘

Is node null?
├─ YES → Create skip_node(key, value)
└─ NO → Match skip prefix
         │
         ├─ match < skip.len AND match < key.len
         │   └─ SPLIT: Create interior with two children
         │
         ├─ match < skip.len (key is prefix of skip)
         │   └─ PREFIX: Create interior with eos + old as child
         │
         ├─ match == skip.len AND key exhausted
         │   ├─ LEAF: Already exists → return false
         │   └─ INTERIOR: Set eos_ptr → return true
         │
         └─ match == skip.len AND key remains
             │
             ├─ LEAF node:
             │   ├─ key.len == 1: Add to leaf (LIST→FULL if needed)
             │   └─ key.len > 1: Demote to interior + recurse
             │
             └─ INTERIOR node:
                 ├─ Child exists: Recurse into child
                 └─ No child: Add new skip_node child
```

### Erase Operation

```
┌───────────────────────────────────────────────────────────────┐
│                          ERASE FLOW                           │
└───────────────────────────────────────────────────────────────┘

  Input: key
  
  ┌────────────────┐
  │  Navigate to   │
  │    target      │
  └───────┬────────┘
          │
          ▼
  ┌────────────────┐       No       ┌────────────────┐
  │    Found?      │───────────────►│ Return: false  │
  └───────┬────────┘                └────────────────┘
          │ Yes
          ▼
  ┌────────────────┐
  │ Remove entry   │
  │ (value/child)  │
  └───────┬────────┘
          │
          ▼
  ┌───────────────────────────────────────────┐
  │              COLLAPSE CHECK               │
  │                                           │
  │  After removal, check if node should      │
  │  collapse with its single remaining       │
  │  child (skip compression)                 │
  │                                           │
  │  Collapse if:                             │
  │  • No eos_ptr AND exactly 1 child         │
  │                                           │
  │  New skip = old_skip + edge + child_skip  │
  └─────────────────────┬─────────────────────┘
                        │
                        ▼
               ┌────────────────┐
               │ Return: true   │
               └────────────────┘

COLLAPSE EXAMPLE:

  Before erasing "car" from {car, card}:
  
    [interior, skip="car"]
    eos_ptr → "car" value
        └─'d'→ [skip:"", value="card"]
  
  After erasing "car":
  
    [skip_node, skip="card", value="card"]
    
    (Interior collapsed with single child)
```

---

## Concurrency Model

### Thread Safety Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                     CONCURRENCY MODEL                         │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  READERS (lock-free)              WRITERS (mutex-protected)   │
│  ┌───────────────────┐            ┌───────────────────┐       │
│  │ 1. Enter EBR      │            │ 1. Acquire mutex  │       │
│  │ 2. Read root      │            │ 2. Modify tree    │       │
│  │ 3. Traverse       │            │ 3. Poison old     │       │
│  │ 4. Validate ver   │            │ 4. Retire nodes   │       │
│  │ 5. Exit EBR       │            │ 5. Release mutex  │       │
│  └───────────────────┘            └───────────────────┘       │
│                                                               │
│  Multiple readers            Only one writer at a time        │
│  Never block                 May block waiting for mutex      │
│  O(depth) operations         O(depth) operations              │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

### Version Numbers

Every modification bumps the node's version:

```
┌───────────────────────────────────────────────────────────────┐
│                  VERSION-BASED VALIDATION                     │
└───────────────────────────────────────────────────────────────┘

Reader:                              Writer:
                                     
1. Record path with versions         
   path = [(node1, v1),              
           (node2, v2),              1. Acquire mutex
           (node3, v3)]              2. Modify node2
                                        node2.bump_version()
2. Read value                        3. Release mutex
                                     
3. Validate path:                    
   for (node, ver) in path:          
     if node.version() != ver:       
       RETRY  ◄────────────────────── Version mismatch!
   SUCCESS                           
```

### Poison Flag

Poison marks nodes as logically deleted:

```cpp
void retire_node(ptr_t n) {
    n->poison();           // Sets FLAG_POISON AND bumps version
    ebr_retire(n, epoch);  // Add to retired list
    advance_epoch();       // Allow future reclamation
}
```

**Key insight**: `poison()` bumps version, so validation only needs to check version:

```cpp
bool validate_path(const path& p) {
    for (auto& [node, ver] : p) {
        // Single check catches both modification AND poison
        if (node->version() != ver) return false;
    }
    return true;
}
```

### RETRY Sentinel

When replacing a node, a sentinel is briefly installed to cause concurrent readers to retry:

```cpp
// Writer replacing child:
child_slot->store(RETRY_SENTINEL);  // Readers see sentinel, retry
child_slot->store(new_node);        // Normal operation resumes
```

The RETRY sentinel is a static self-referential node:
- All 256 children point back to itself
- Poisoned flag is set
- Any traversal will hit poison check and retry

```
┌───────────────────────────────────────────────────────────────┐
│                       RETRY SENTINEL                          │
├───────────────────────────────────────────────────────────────┤
│                                                               │
│  [full_node: POISONED]                                        │
│  children[0..255] ──┐                                         │
│        ▲            │                                         │
│        └────────────┘  (self-referential)                     │
│                                                               │
│  If reader follows any child → loops back                     │
│  Poison check catches it → reader retries                     │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

---

## Epoch-Based Reclamation

### The Problem

```
Writer:                          Reader:

1. old = slot.load()             1. node = slot.load()  // Gets old
2. slot.store(new)               
3. delete old  ◄─── DANGER! ────►2. node->value  // USE AFTER FREE!
```

### EBR Solution

TKTRIE uses **fully per-trie EBR** - no global state:

```
┌───────────────────────────────────────────────────────────────┐
│                     PER-TRIE EBR ARCHITECTURE                 │
└───────────────────────────────────────────────────────────────┘

                       ┌─────────────────────────────────────┐
                       │            TKTRIE INSTANCE          │
                       │                                     │
                       │  epoch_: 42                         │ ◄── Per-trie epoch
                       │                                     │
                       │  reader_epochs_[16]:                │ ◄── Per-trie slots
                       │    [0]: 42 (active)                 │     (64-byte aligned)
                       │    [1]: 0  (inactive)               │
                       │    [2]: 41 (active)                 │
                       │    ...                              │
                       │                                     │
                       │  retired_head_ ──► [entry1]         │ ◄── Per-trie retired list
                       │                      ↓              │
                       │                    [entry2]         │
                       │                      ↓              │
                       │                    [entry3]         │
                       │                                     │
                       └─────────────────────────────────────┘

SAFE TO DELETE: retired_epoch + 2 ≤ min(active reader epochs)

  Current epoch: 42
  Active readers: slot[0]=42, slot[2]=41
  Min active: 41
  
  entry1 (epoch=39): 39 + 2 = 41 ≤ 41? YES → DELETE
  entry2 (epoch=40): 40 + 2 = 42 ≤ 41? NO  → Wait
  entry3 (epoch=41): 41 + 2 = 43 ≤ 41? NO  → Wait
```

### Per-Trie State

Each trie instance contains:

```cpp
// Epoch counter - bumped on writes, used for read validation AND EBR
alignas(64) std::atomic<uint64_t> epoch_{1};

// 16 reader slots, each 64-byte aligned (1KB total per trie)
struct alignas(64) PaddedReaderSlot {
    std::atomic<uint64_t> epoch{0};  // 0 = inactive
};
std::array<PaddedReaderSlot, 16> reader_epochs_;

// Retired node list with external wrapper entries
std::atomic<retire_entry*> retired_head_{nullptr};
```

### Reader Slot Assignment

Threads hash their ID to a slot index:

```cpp
void reader_enter() const noexcept {
    size_t slot = thread_slot_hash(16);  // Hash thread ID to 0-15
    uint64_t e = epoch_.load();
    reader_epochs_[slot].epoch.store(e);
}

void reader_exit() const noexcept {
    size_t slot = thread_slot_hash(16);
    reader_epochs_[slot].epoch.store(0);  // 0 = inactive
}
```

### Computing Min Reader Epoch

```cpp
uint64_t min_reader_epoch() const noexcept {
    uint64_t current = epoch_.load();
    uint64_t min_e = current;
    
    for (auto& slot : reader_epochs_) {
        uint64_t e = slot.epoch.load();
        if (e != 0 && e < min_e) {  // Active && older
            min_e = e;
        }
    }
    return min_e;
}
```

### Retired Entry Structure

When a node is retired, a small wrapper is allocated:

```cpp
struct retire_entry {
    ptr_t node;           // The retired node
    uint64_t epoch;       // Epoch when retired  
    retire_entry* next;   // Linked list
};
```

This adds 24 bytes per retired node but keeps node sizes small (no embedded retire fields).

---

## Pseudocode Reference

### Find (Epoch Validation)

```
FUNCTION find(key) -> (found, value):
    key_bytes = encode(key)
    
    IF THREADED:
        reader_enter()  // Register with global EBR
        
    LOOP max_retries:
        epoch_before = trie.epoch_.load()  // Snapshot epoch
        node = root
        
        WHILE node != null:
            IF node.is_poisoned():
                BREAK  // Retry from beginning
            
            // Match skip
            skip = node.skip
            m = match_prefix(skip, key_bytes)
            
            IF m < skip.length:
                // Key not found (skip doesn't match)
                epoch_after = trie.epoch_.load()
                IF epoch_before == epoch_after:
                    reader_exit()
                    RETURN (false, null)
                BREAK  // Epoch changed, retry
            
            key_bytes = key_bytes[m:]  // Consume matched prefix
            
            IF key_bytes.empty():
                // End of key - check for value
                IF node.is_leaf():
                    IF node.is_skip():
                        value = node.leaf_value
                        epoch_after = trie.epoch_.load()
                        IF epoch_before == epoch_after:
                            reader_exit()
                            RETURN (true, value)
                    ELSE:
                        // Multi-value leaf, no match at empty key
                        epoch_after = trie.epoch_.load()
                        IF epoch_before == epoch_after:
                            reader_exit()
                            RETURN (false, null)
                ELSE:
                    // Interior node - check eos_ptr
                    value = node.eos_ptr
                    epoch_after = trie.epoch_.load()
                    IF epoch_before == epoch_after:
                        reader_exit()
                        IF value != null:
                            RETURN (true, *value)
                        RETURN (false, null)
                BREAK
            
            IF node.is_leaf():
                // Must consume exactly 1 more char in leaf
                IF key_bytes.length != 1:
                    epoch_after = trie.epoch_.load()
                    IF epoch_before == epoch_after:
                        reader_exit()
                        RETURN (false, null)
                    BREAK
                
                c = key_bytes[0]
                IF node.has_entry(c):
                    value = node.get_value(c)
                    epoch_after = trie.epoch_.load()
                    IF epoch_before == epoch_after:
                        reader_exit()
                        RETURN (true, value)
                ELSE:
                    epoch_after = trie.epoch_.load()
                    IF epoch_before == epoch_after:
                        reader_exit()
                        RETURN (false, null)
                BREAK
            
            // Interior node - descend
            c = key_bytes[0]
            key_bytes = key_bytes[1:]
            node = node.get_child(c)
    
    reader_exit()
    RETURN (false, null)  // Max retries exceeded
```

**Key insight**: Epoch validation is O(1) - just compare two atomic loads. No need to record path or validate each node's version individually.

### Insert

```
FUNCTION insert(key, value) -> (iterator, inserted):
    key_bytes = encode(key)
    
    IF THREADED:
        LOOP:
            reader_enter()
            spec = probe_speculative(root, key_bytes)
            reader_exit()
            
            IF spec.op == EXISTS:
                RETURN (find(key), false)
            
            // Fast path: in-place modification
            IF spec.op == IN_PLACE_LEAF OR spec.op == IN_PLACE_INTERIOR:
                mutex.lock()
                IF validate_path(spec.path):
                    perform_in_place_insert(spec, value)
                    size++
                    mutex.unlock()
                    RETURN (iterator(key, value), true)
                mutex.unlock()
                CONTINUE  // Retry
            
            // Slow path: structural change
            mutex.lock()
            IF validate_path(spec.path):
                result = insert_impl(root, key_bytes, value)
                IF result.new_node:
                    root = result.new_node
                retire_old_nodes(result.old_nodes)
                size++
                mutex.unlock()
                RETURN (iterator(key, value), true)
            mutex.unlock()
            // Retry
    ELSE:
        // Non-threaded: simple locked insert
        result = insert_impl(root, key_bytes, value)
        IF result.inserted:
            IF result.new_node:
                root = result.new_node
            size++
            RETURN (iterator(key, value), true)
        RETURN (find(key), false)
```

### Erase

```
FUNCTION erase(key) -> bool:
    key_bytes = encode(key)
    
    IF THREADED:
        LOOP:
            reader_enter()
            spec = probe_erase(root, key_bytes)
            reader_exit()
            
            IF spec.op == NOT_FOUND:
                // Might be real not-found, or retry needed
                mutex.lock()
                result = erase_impl(root, key_bytes)
                apply_erase_result(result)
                mutex.unlock()
                RETURN result.erased
            
            // Fast path: in-place removal
            IF spec.op == IN_PLACE_LEAF_LIST OR spec.op == IN_PLACE_LEAF_FULL:
                mutex.lock()
                IF validate_and_remove(spec):
                    size--
                    mutex.unlock()
                    RETURN true
                mutex.unlock()
                CONTINUE  // Retry
    ELSE:
        result = erase_impl(root, key_bytes)
        IF result.erased:
            apply_erase_result(result)
            size--
        RETURN result.erased
```

### Validate Path

Used during write operations to ensure speculative probing is still valid:

```
FUNCTION validate_path(path) -> bool:
    // Check all recorded versions still match
    // (poison bumps version, so this catches poison too)
    FOR (node, recorded_version) IN path:
        IF node.version() != recorded_version:
            RETURN false
    RETURN true
```

**Note**: Read operations use faster epoch validation (O(1) vs O(path length)).

### Retire Node

```
FUNCTION retire_node(node):
    IF node == null OR node == RETRY_SENTINEL:
        RETURN
    
    node.poison()              // Bumps version + sets poison flag
    current_epoch = epoch_.load()  // Per-trie epoch
    
    // Allocate wrapper and add to per-trie retired list
    entry = new retire_entry(node, current_epoch)
    entry.next = retired_head_.exchange(entry)  // Lock-free push
    retired_count_.fetch_add(1)
    
    epoch_.fetch_add(1)        // Advance per-trie epoch
```

### Try Reclaim (ebr_cleanup)

```
FUNCTION ebr_cleanup():
    // Find minimum epoch held by any active reader IN THIS TRIE
    min_epoch = min_reader_epoch()  // Scans per-trie reader_epochs_[]
    
    // Grab lock for cleanup (writes only, readers don't take this)
    mutex.lock()
    
    // Walk retired list, free what's safe
    prev = null
    curr = retired_head_
    
    WHILE curr != null:
        IF curr.epoch + 2 <= min_epoch:
            // Safe to delete
            delete curr.node
            // Remove from list
            IF prev == null:
                retired_head_ = curr.next
            ELSE:
                prev.next = curr.next
            delete curr
            retired_count_.fetch_sub(1)
        ELSE:
            prev = curr
        curr = curr.next
    
    mutex.unlock()
```

---

## Summary

TKTRIE achieves high concurrent read performance through:

1. **Lock-free reads**: Epoch validation enables optimistic traversal with O(1) consistency check
2. **Per-trie EBR**: Each trie has its own epoch, reader slots, and retired list - no global state
3. **Skip compression**: Shallow trees (avg depth ~3) minimize traversal cost
4. **Five adaptive node types**: SKIP(1) → BINARY(2) → LIST(3-7) → POP(8-32) → FULL(33-256) minimize memory
5. **Poison bits**: Writers mark nodes dead before retiring, readers detect and retry

The architecture trades write complexity for read performance, making it ideal for read-heavy concurrent workloads where reads can be 2-8x faster than mutex-guarded alternatives.
