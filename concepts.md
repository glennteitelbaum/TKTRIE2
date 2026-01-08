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
small_list<THREADED> chars;  // Which characters have children
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
- **Max depth**: 4 levels
- **Average depth**: 2.79 levels
- **Average skip length**: 4.01 bytes

This is dramatically shallower than a standard trie (which would be 8 levels for 8-byte keys).

---

## Sentinels

A **sentinel** is a special marker value used to signal exceptional conditions without null checks.

### The Problem

In concurrent programming, a reader might see:
1. A valid pointer to node A
2. Writer deletes node A, installs node B
3. Reader dereferences stale pointer → **use-after-free**

### Traditional Solutions

1. **Locking**: Readers take shared locks (slow, contention)
2. **RCU**: Readers enter critical sections (Linux kernel style)
3. **Hazard pointers**: Readers publish what they're accessing (complex)

### TKTRIE's Sentinel Approach

We use a **self-referential sentinel node** that is safe to dereference:

```cpp
// Static sentinel - one per template instantiation
template <typename T, bool THREADED, typename Allocator>
node_base<T, THREADED, Allocator>* get_retry_sentinel() noexcept {
    static full_node<T, THREADED, Allocator> sentinel;
    static bool initialized = []() {
        sentinel.set_header(SENTINEL_HEADER);  // Marked as poisoned
        // All 256 children point back to sentinel itself
        for (int i = 0; i < 256; ++i) {
            sentinel.children[i].store(&sentinel);
        }
        return true;
    }();
    return &sentinel;
}
```

### How It Works

1. **Writer wants to replace node A with node B**:
   ```cpp
   slot->store(get_retry_sentinel());  // Briefly install sentinel
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

### Standard EBR (Global Epoch)

Traditional EBR uses a **global epoch counter**:

1. **Global epoch**: Single counter incremented by writers
2. **Reader registration**: Each reader records current epoch when starting
3. **Grace period**: Node retired at epoch E can be freed when all readers have advanced past E

```
Global Epoch: 5

Reader Slots:
  Thread 1: epoch=5 (active)
  Thread 2: epoch=4 (active)  ← oldest
  Thread 3: epoch=0 (inactive)

Safe to free: nodes retired at epoch ≤ 2 (oldest - 2)
```

### Problems with Global EBR

1. **Global contention**: All tries share one epoch counter
2. **Cross-trie interference**: Slow reader in trie A blocks reclamation in trie B
3. **Registration overhead**: Thread-local storage lookup on every operation

### TKTRIE's Per-Trie EBR

Each trie has its own epoch and reader tracking:

```cpp
// Per-trie state (in tktrie class)
alignas(64) std::atomic<uint64_t> epoch_{1};  // Bumped on writes

// Cache-line padded reader slots (16 slots, 1KB total)
struct alignas(64) PaddedReaderSlot {
    std::atomic<uint64_t> epoch{0};  // 0 = inactive
};
std::array<PaddedReaderSlot, 16> reader_epochs_;

// Retired node list (lock-free MPSC queue)
std::atomic<ptr_t> retired_head_{nullptr};
std::atomic<size_t> retired_count_{0};
```

### Reader Protocol

```cpp
void reader_enter() const noexcept {
    size_t slot = thread_slot_hash(16);  // Hash thread ID to slot
    uint64_t e = epoch_.load(std::memory_order_acquire);
    reader_epochs_[slot].epoch.store(e, std::memory_order_release);
}

void reader_exit() const noexcept {
    size_t slot = thread_slot_hash(16);
    reader_epochs_[slot].epoch.store(0, std::memory_order_release);  // Mark inactive
}
```

### Finding Safe Epoch

```cpp
uint64_t min_reader_epoch() const noexcept {
    uint64_t current = epoch_.load();
    uint64_t min_e = current;
    for (auto& slot : reader_epochs_) {
        uint64_t e = slot.epoch.load();
        if (e != 0 && e < min_e) {  // Active reader with older epoch
            min_e = e;
        }
    }
    return min_e;
}

// Node retired at epoch E can be freed when min_reader_epoch() > E + grace_period
```

### Slot Collisions

With 16 slots and potentially many threads, collisions are possible:

```
Thread 1 (slot 3): epoch=10
Thread 2 (slot 3): epoch=12  ← overwrites Thread 1's registration
```

**This is safe**: We see the newer epoch (12), which is conservative. We might delay reclamation slightly, but never free too early.

### Benefits of Per-Trie EBR

| Aspect | Global EBR | Per-Trie EBR |
|--------|------------|--------------|
| Epoch counter | Single global | One per trie |
| Reader interference | Cross-trie | Isolated |
| Memory overhead | O(threads) | O(tries × 16) |
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
    header_.store(header_.load() | FLAG_POISON);
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

### Reader Protocol

```cpp
bool read_impl(ptr_t n, std::string_view key) {
    while (true) {
        if (n->is_poisoned()) return false;  // Retry from root
        
        // ... traverse to child ...
        n = n->get_child(c);
        
        if (!n || n->is_poisoned()) return false;  // Check again
    }
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

Versions enable optimistic concurrency:
1. Reader records version before traversal
2. Reader records version after traversal  
3. If versions match and node not poisoned → result is valid
4. Otherwise → retry

---

## Putting It All Together

A complete read operation:

```cpp
bool contains(const Key& key) const {
    reader_enter();  // Register with this trie's EBR
    
    for (int attempts = 0; attempts < 10; ++attempts) {
        uint64_t epoch_before = epoch_.load();
        
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
