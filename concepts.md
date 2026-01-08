# TKTRIE Concepts

This document explains the core concepts behind tktrie, a high-performance concurrent trie implementation.

## Table of Contents

1. [Standard Tries](#standard-tries)
2. [How TKTRIE Differs](#how-tktrie-differs)
3. [Sentinels](#sentinels)
4. [Epoch-Based Reclamation (EBR)](#epoch-based-reclamation-ebr)
5. [Poison Bits](#poison-bits)

---

## Standard Tries

A **trie** (pronounced "try", from re**trie**val) is a tree data structure for storing strings or sequences where each edge represents one element of the sequence.

### Basic Structure

```
Insert: "cat", "car", "card", "dog"

        (root)
        /    \
       c      d
       |      |
       a      o
      / \     |
     t   r    g
         |
         d
```

Each path from root to leaf spells out a key. Lookup is O(k) where k is key length, regardless of how many keys are stored.

### Properties of Standard Tries

- **One character per edge**: Each edge represents exactly one byte/character
- **Shared prefixes**: Keys with common prefixes share nodes (e.g., "cat" and "car" share "ca")
- **O(k) operations**: Insert, lookup, and delete are all proportional to key length
- **Memory overhead**: One node per character can be expensive

### Limitations

1. **Memory inefficiency**: Long keys with no shared prefixes create deep chains
2. **Cache unfriendly**: Deep trees cause many cache misses
3. **Node overhead**: Each node needs pointers, metadata, etc.

---

## How TKTRIE Differs

TKTRIE addresses standard trie limitations through several optimizations:

### 1. Skip Compression (Patricia Trie Style)

Instead of one character per edge, TKTRIE stores entire substrings in nodes:

```
Standard trie for "application":       TKTRIE with skip compression:
    a                                      "application" -> value
    |
    p
    |
    p                                  (Single node with skip="application")
    |
    l
    ... (11 nodes total)
```

Each node has a **skip string** that must match before descending. This dramatically reduces depth.

### 2. Three Node Types

TKTRIE uses three node types optimized for different branching factors:

| Type | Children | Use Case |
|------|----------|----------|
| **SKIP** | 0 | Leaf-only, stores single key suffix + value |
| **LIST** | 1-7 | Small branching factor, packed in 64-bit word |
| **FULL** | 8-256 | Large branching factor, 256-entry array + bitmap |

```cpp
// LIST node: children packed in single atomic uint64
// Layout: [count:8][char6:8][char5:8]...[char0:8]
small_list chars;            // Which characters have children
std::array<ptr, 7> children; // The child pointers

// FULL node: direct indexing by character
bitmap256 valid;             // Which slots are occupied
std::array<ptr, 256> children;
```

### 3. Leaf vs Interior Distinction

Nodes are tagged as **leaf** or **interior**:

- **Leaf nodes**: Store actual values, no child pointers
- **Interior nodes**: Store child pointers, optional end-of-string (EOS) value

The same LIST/FULL structure is reused:
- Leaf LIST: `chars` + `values[7]` array
- Interior LIST: `chars` + `children[7]` array + optional `eos_ptr`

### 4. Adaptive Promotion

Nodes automatically convert when needed:
- LIST with 7 children + new insert → promotes to FULL
- FULL with few children after delete → may demote to LIST

### 5. Integer Key Support

TKTRIE natively supports integer keys via big-endian encoding:

```cpp
// int64_t key "12345" becomes 8-byte string "\x00\x00\x00\x00\x00\x003\x09"
// Preserves sort order: key1 < key2 implies bytes(key1) < bytes(key2)
```

### Resulting Tree Depth

For 100K random uint64 keys:
- **Max depth**: ~4 levels
- **Average depth**: ~3 levels
- **Average skip length**: ~4 bytes

This is dramatically shallower than a standard trie (which would be 8 levels for 8-byte keys).

---

## Sentinels

A **sentinel** is a special marker value used to signal exceptional conditions without crashing. TKTRIE uses a single sentinel for concurrent node replacement.

### The Concurrent Replacement Problem

In concurrent programming, a reader might:
1. Load a pointer to node A
2. Writer replaces A with B, frees A
3. Reader dereferences stale pointer → **use-after-free crash**

### Traditional Solutions

1. **Locking**: Readers take shared locks (slow, contention)
2. **RCU**: Readers enter critical sections (Linux kernel style)
3. **Hazard pointers**: Readers publish what they're accessing (complex)

### TKTRIE's RETRY Sentinel

We use a **self-referential sentinel node** that is safe to dereference:

```cpp
template <typename T, bool THREADED, typename Allocator>
node_base<T, THREADED, Allocator>* get_retry_sentinel() noexcept {
    static full_node<T, THREADED, Allocator> sentinel;
    static bool initialized = []() {
        sentinel.set_header(SENTINEL_HEADER);  // FLAG_POISON set
        auto* self = static_cast<node_base*>(&sentinel);
        for (int i = 0; i < 256; ++i) {
            sentinel.children[i].store(self);  // All point to self
            sentinel.valid.set(static_cast<unsigned char>(i));
        }
        return true;
    }();
    return &sentinel;
}
```

The RETRY sentinel has `FLAG_POISON` set, marking it as "logically deleted."

### How It Works

1. **Writer wants to replace node A with node B**:
   ```cpp
   slot->store(get_retry_sentinel());  // Briefly install sentinel
   retire_node(old_node);              // Mark old node for deletion
   slot->store(new_node);              // Install actual new node
   ```

2. **Reader sees sentinel**:
   - Sentinel is poisoned → `is_poisoned()` returns true
   - Reader knows to retry from root
   - Even if reader follows children, they loop back to sentinel
   - Eventually hits poison check, triggers retry

3. **Properties**:
   - Never crashes (sentinel is always valid memory)
   - Never infinite loops (poison check breaks out)
   - No locks needed for readers
   - Writers briefly "block" readers via retry

### Self-Referential Safety

If a reader accidentally follows children of the RETRY sentinel:

```
Reader at RETRY sentinel
  → get_child('a') returns RETRY sentinel (self)
  → get_child('b') returns RETRY sentinel (self)
  → Eventually hits is_poisoned() check → retries from root
```

The self-reference creates a **safe infinite loop** that's broken by poison checks. This is safer than nullptr (which would crash) or a dangling pointer (use-after-free).

### Sentinel Header

```cpp
static constexpr uint64_t FLAG_POISON = 1ULL << 60;
static constexpr uint64_t SENTINEL_HEADER = FLAG_POISON;  // Interior FULL + poisoned
```

---

## Epoch-Based Reclamation (EBR)

**Epoch-Based Reclamation** is a memory management technique for lock-free data structures that solves the reclamation problem: when can we safely free a node that readers might still be accessing?

### The Reclamation Problem

```
Time 0: Reader R starts traversing, sees node A
Time 1: Writer W removes node A, wants to free it
Time 2: Reader R still has pointer to A
Time 3: Writer frees A
Time 4: Reader R dereferences A → CRASH (use-after-free)
```

### TKTRIE's Per-Trie EBR

TKTRIE uses a per-trie epoch and reader tracking:

```cpp
// Per-trie state (in tktrie class)
alignas(64) std::atomic<uint64_t> epoch_{1};  // Bumped on writes

// Per-trie retired node tracking
struct retired_entry {
    ptr_t node;
    uint64_t epoch;
    retired_entry* next;
};
std::atomic<retired_entry*> retired_head_{nullptr};
```

### Global Thread Slots

Reader tracking uses global EBR slots shared across all tries:

```cpp
class ebr_slot {
    std::atomic<uint64_t> epoch_{0};   // Current epoch reader holds
    std::atomic<bool> active_{false};   // Is reader active?
    std::atomic<bool> valid_{true};     // Is slot still valid?
};

class ebr_global {
    std::vector<ebr_slot*> slots_;     // All registered slots
    // ...
};
```

### Reader Protocol

```cpp
void reader_enter() noexcept {
    // Get thread-local slot
    ebr_slot& slot = get_ebr_slot();
    // Record current global epoch
    uint64_t e = ebr_slot::global_epoch().load();
    slot.epoch_.store(e);
    slot.active_.store(true);
}

void reader_exit() noexcept {
    ebr_slot& slot = get_ebr_slot();
    slot.active_.store(false);  // Mark inactive
}
```

### Finding Safe Epoch

```cpp
uint64_t compute_safe_epoch() {
    uint64_t current = ebr_slot::global_epoch().load();
    uint64_t safe = current;
    
    for (auto* slot : slots_) {
        if (slot->is_valid() && slot->is_active()) {
            uint64_t e = slot->epoch();
            if (e < safe) safe = e;  // Oldest active reader
        }
    }
    return safe;
}

// Node retired at epoch E can be freed when safe_epoch > E + grace_period
```

### Epoch Validation (Fast Path)

For reads, TKTRIE uses **epoch validation** instead of path validation:

```cpp
bool contains(const Key& key) const {
    reader_enter();
    
    for (int attempts = 0; attempts < 10; ++attempts) {
        uint64_t epoch_before = epoch_.load();  // Snapshot epoch
        
        ptr_t root = root_.load();
        if (!root) { reader_exit(); return false; }
        if (root->is_poisoned()) continue;  // Retry
        
        bool found = read_impl(root, key);
        
        uint64_t epoch_after = epoch_.load();
        if (epoch_before == epoch_after) {  // No concurrent writes
            reader_exit();
            return found;
        }
        // Epoch changed → retry to ensure consistent read
    }
    
    reader_exit();
    return fallback_locked_read();  // Too many retries
}
```

This is faster than path validation because:
- Only 2 atomic loads (epoch before/after) vs N loads (one per node)
- No path recording overhead during traversal

### Benefits of Per-Trie EBR

| Aspect | Global EBR | Per-Trie EBR |
|--------|------------|--------------|
| Epoch counter | Single global | One per trie |
| Reader interference | Cross-trie | Isolated |
| Memory overhead | O(threads) | O(tries × slots) |
| Scalability | Limited | Per-trie |

---

## Poison Bits

**Poison bits** mark nodes as logically deleted, enabling safe lock-free reads.

### The Problem

Between a writer deciding to delete a node and actually freeing it, readers might:
1. Already have a pointer to the node
2. Be mid-traversal through the node
3. Be about to read values from the node

### Solution: Poison Flag

Every node header has a poison bit:

```cpp
// Header layout: [LEAF:1][SKIP:1][LIST:1][POISON:1][VERSION:60]
static constexpr uint64_t FLAG_POISON = 1ULL << 60;

bool is_poisoned() const noexcept {
    return (header_.load() & FLAG_POISON) != 0;
}

void poison() noexcept {
    // Bump version AND set poison - version check catches both
    uint64_t h = header_.load();
    header_.store(bump_version(h) | FLAG_POISON);
}
```

### Writer Protocol

```cpp
void retire_node(ptr_t n) {
    n->poison();           // 1. Mark as dead FIRST
    ebr_retire(n, epoch);  // 2. Add to retired list
    epoch_.fetch_add(1);   // 3. Bump epoch
}
```

### Why Poison Before Retire?

The order is critical:

```
CORRECT:                          INCORRECT:
1. poison()                       1. retire()
2. retire()                       2. poison()
3. Reader sees poison → retry     3. Reader might not see poison
                                     before node is freed!
```

### Interaction with Sentinel

The sentinel is **permanently poisoned**:

```cpp
static constexpr uint64_t SENTINEL_HEADER = FLAG_POISON;
```

This means:
- `is_poisoned()` catches both retired nodes AND sentinel
- Readers don't need separate sentinel checks
- Same code path handles both cases

### Version Numbers and Poison

The header also contains a 60-bit version number:

```cpp
// Bump version preserving flags (including poison)
inline constexpr uint64_t bump_version(uint64_t h) noexcept {
    uint64_t flags = h & FLAGS_MASK;  // Preserve LEAF, SKIP, LIST, POISON
    uint64_t ver = (h & VERSION_MASK) + 1;
    return flags | (ver & VERSION_MASK);
}
```

**Key insight**: `poison()` bumps the version:

```cpp
void poison() noexcept {
    uint64_t h = header_.load();
    header_.store(bump_version(h) | FLAG_POISON);  // Version changes!
}
```

This means a **single version check** catches both:
- In-place modifications (child added, value changed)
- Node retirement (poison was set)

---

## Putting It All Together

A complete read operation:

```cpp
bool contains(const Key& key) const {
    reader_enter();  // Register with global EBR
    
    for (int attempts = 0; attempts < 10; ++attempts) {
        uint64_t epoch_before = epoch_.load();  // Per-trie epoch
        
        ptr_t n = root_.load();
        if (!n) { reader_exit(); return false; }
        if (n->is_poisoned()) continue;  // Sentinel or retired → retry
        
        bool found = traverse(n, key);  // May hit poison mid-traversal
        
        uint64_t epoch_after = epoch_.load();
        if (epoch_before == epoch_after) {  // No concurrent writes
            reader_exit();
            return found;
        }
        // Epoch changed → writer was active, retry to be safe
    }
    
    reader_exit();
    return fallback_locked_read();  // Too many retries
}
```

This achieves:
- **Lock-free reads**: No mutexes on the read path
- **Safe memory reclamation**: EBR prevents use-after-free
- **Progress guarantee**: Sentinel ensures no infinite loops
- **Per-trie isolation**: Operations on trie A don't affect trie B
- **Efficient validation**: Epoch check is O(1), not O(path length)
