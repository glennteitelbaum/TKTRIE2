# Array-Based Trie Design Document

## Overview

A thread-safe trie implementation where nodes are represented as contiguous arrays of `uint64_t` (or `std::atomic<uint64_t>` when threaded). This design prioritizes cache locality, SWAR (SIMD Within A Register) operations, and clean separation between threaded and non-threaded variants.

### Template Parameters

```cpp
template <typename Key, typename Value, bool THREADED = false, typename Allocator = std::allocator<uint64_t>>
class tktrie;
```

| Parameter | Description |
|-----------|-------------|
| `Key` | Key type (string or integral types supported via traits) |
| `Value` | Stored value type. `nullptr` values are NOT allowed |
| `THREADED` | Enable thread-safe operations with lock-free reads |
| `Allocator` | Allocator for node arrays (default: `std::allocator<uint64_t>`) |

---

## Node Array Storage

```cpp
// Non-threaded: plain arrays
uint64_t* node;

// Threaded: atomic arrays
std::atomic<uint64_t>* node;
```

All slot accesses in threaded mode use appropriate memory ordering.

---

## Node Array Layout

Each node is a contiguous array. The layout is determined by flags in the header.

### Header (Always First Element)

```
┌─────────────┬────────────────────────┬─────────────────────────────┐
│   flags     │        version         │            size             │
│   5 bits    │        32 bits         │          27 bits            │
└─────────────┴────────────────────────┴─────────────────────────────┘
     MSB                                                          LSB
```

**Bit Layout:**
- Bits 63-59: Flags (5 bits)
- Bits 58-27: Version (32 bits) - used for optimistic concurrency when THREADED=true
- Bits 26-0: Size (27 bits) - number of uint64_t elements in this node array

**Size Capacity:** 27 bits allows up to 134,217,727 elements per node.

### Flags

| Flag | Bit | Description |
|------|-----|-------------|
| `EOS` | 63 | End-of-string: node contains data for a complete key |
| `SKIP` | 62 | Node has path compression (skip sequence) |
| `SKIP_EOS` | 61 | Skip sequence terminates with data |
| `LIST` | 60 | Children stored as small sorted list (1-7 children) |
| `POP` | 59 | Children stored as popcount bitmap (8+ children) |

**Flag Constants:**
```cpp
static constexpr uint64_t FLAG_EOS      = 1ULL << 63;
static constexpr uint64_t FLAG_SKIP     = 1ULL << 62;
static constexpr uint64_t FLAG_SKIP_EOS = 1ULL << 61;
static constexpr uint64_t FLAG_LIST     = 1ULL << 60;
static constexpr uint64_t FLAG_POP      = 1ULL << 59;

static constexpr uint64_t VERSION_MASK  = 0x07FFFFFF'F8000000ULL;  // bits 58-27
static constexpr uint64_t SIZE_MASK     = 0x00000000'07FFFFFFULL;  // bits 26-0
static constexpr int VERSION_SHIFT      = 27;
```

### Invariants

1. `LIST` and `POP` are mutually exclusive (never both set)
2. `SKIP_EOS` requires `SKIP` to be set
3. `SKIP` with length 0 is invalid (clear the flag instead)
4. `LIST` with count 1 is invalid UNLESS `SKIP` is also set in the node
5. `nullptr` values are not allowed
6. **fixed_len > 0 specific:**
   - `EOS` and `SKIP_EOS` can ONLY occur at leaf depth (depth == fixed_len)
   - At leaf depth, `EOS` and `SKIP_EOS` are mutually exclusive
   - Internal nodes (depth < fixed_len) never have `EOS` or `SKIP_EOS` set

---

## fixed_len > 0 Optimization

When `Key` is a fixed-length type (e.g., integers), additional optimizations apply:

### Leaf Node Structure

At leaf depth, instead of `Node*` child pointers, nodes store `dataptr` directly:

```
Internal Node (depth < fixed_len):
┌──────────────────────────────────────────────────────────────────┐
│ [0] HEADER: LIST | version | size                                │
├──────────────────────────────────────────────────────────────────┤
│ [1] small_list                                                   │
├──────────────────────────────────────────────────────────────────┤
│ [2] child_ptr[0] (Node*)                                         │
│ [3] child_ptr[1] (Node*)                                         │
│ ...                                                              │
└──────────────────────────────────────────────────────────────────┘

Leaf Node (depth == fixed_len):
┌──────────────────────────────────────────────────────────────────┐
│ [0] HEADER: EOS | LIST | version | size                          │
├──────────────────────────────────────────────────────────────────┤
│ [1] small_list                                                   │
├──────────────────────────────────────────────────────────────────┤
│ [2] dataptr[0] (Value*)                                          │
│ [3] dataptr[1] (Value*)                                          │
│ ...                                                              │
└──────────────────────────────────────────────────────────────────┘
```

### Path Arrays

For fixed-length keys, path tracking uses stack allocation:

```cpp
// fixed_len == 0 (variable length keys)
std::vector<PathEntry> path;

// fixed_len > 0 (fixed length keys)
std::array<PathEntry, fixed_len> path;
```

---

## Grammar / Node Layouts

The flags determine the exact layout of the node array. Below are all valid configurations:

### Layout 1: EOS Only (Leaf with data, no children)

```
Flags: EOS
┌──────────────────────────────────────────────────────────────────┐
│ [0] HEADER: EOS | version | size=2                               │
├──────────────────────────────────────────────────────────────────┤
│ [1] dataptr (Value*)                                             │
└──────────────────────────────────────────────────────────────────┘
```

### Layout 2: SKIP | SKIP_EOS (Path compression to terminal)

```
Flags: SKIP | SKIP_EOS
┌──────────────────────────────────────────────────────────────────┐
│ [0] HEADER: SKIP | SKIP_EOS | version | size                     │
├──────────────────────────────────────────────────────────────────┤
│ [1] skip_byte_length (uint64_t)                                  │
├──────────────────────────────────────────────────────────────────┤
│ [2] skip_chars[0-7]                                              │
│ [3] skip_chars[8-15]                                             │
│ ... (ceil(skip_byte_length / 8) words total)                     │
├──────────────────────────────────────────────────────────────────┤
│ [N] dataptr (Value*) for SKIP_EOS                                │
└──────────────────────────────────────────────────────────────────┘
```

### Layout 3: EOS | SKIP | SKIP_EOS (Data here, then path compression to more data)

```
Flags: EOS | SKIP | SKIP_EOS
┌──────────────────────────────────────────────────────────────────┐
│ [0] HEADER: EOS | SKIP | SKIP_EOS | version | size               │
├──────────────────────────────────────────────────────────────────┤
│ [1] dataptr (Value*) for EOS                                     │
├──────────────────────────────────────────────────────────────────┤
│ [2] skip_byte_length                                             │
│ [3..M] skip_chars                                                │
├──────────────────────────────────────────────────────────────────┤
│ [M+1] dataptr (Value*) for SKIP_EOS                              │
└──────────────────────────────────────────────────────────────────┘
```

**Use Case:** Keys "cat" and "catalog" in same node with no "cats" branch.
**Note:** This layout only valid when fixed_len == 0.

### Layout 4: LIST (1-7 children, no data here)

```
Flags: LIST
┌──────────────────────────────────────────────────────────────────┐
│ [0] HEADER: LIST | version | size                                │
├──────────────────────────────────────────────────────────────────┤
│ [1] small_list: sorted_chars[0-6] | count                        │
├──────────────────────────────────────────────────────────────────┤
│ [2] child_ptr[0] (Node* or dataptr if leaf)                      │
│ [3] child_ptr[1]                                                 │
│ ... (count child pointers)                                       │
└──────────────────────────────────────────────────────────────────┘
```

**Note:** LIST with count==1 requires SKIP to also be set (otherwise extend skip).

### Layout 5: POP (8+ children, no data here)

```
Flags: POP
┌──────────────────────────────────────────────────────────────────┐
│ [0] HEADER: POP | version | size                                 │
├──────────────────────────────────────────────────────────────────┤
│ [1] bitmap[0] (chars 0-63)                                       │
│ [2] bitmap[1] (chars 64-127)                                     │
│ [3] bitmap[2] (chars 128-191)                                    │
│ [4] bitmap[3] (chars 192-255)                                    │
├──────────────────────────────────────────────────────────────────┤
│ [5] child_ptr[0] (Node* or dataptr if leaf)                      │
│ [6] child_ptr[1]                                                 │
│ ... (popcount(bitmap) child pointers)                            │
└──────────────────────────────────────────────────────────────────┘
```

### Layout 6: EOS | LIST (Data here + 1-7 children)

```
Flags: EOS | LIST
┌──────────────────────────────────────────────────────────────────┐
│ [0] HEADER: EOS | LIST | version | size                          │
├──────────────────────────────────────────────────────────────────┤
│ [1] dataptr (Value*)                                             │
├──────────────────────────────────────────────────────────────────┤
│ [2] small_list                                                   │
├──────────────────────────────────────────────────────────────────┤
│ [3] child_ptr[0]                                                 │
│ ... (count child pointers)                                       │
└──────────────────────────────────────────────────────────────────┘
```

**Note for fixed_len > 0:** Only valid at leaf depth. Children are dataptr, not Node*.

### Layout 7: EOS | POP (Data here + 8+ children)

```
Flags: EOS | POP
┌──────────────────────────────────────────────────────────────────┐
│ [0] HEADER: EOS | POP | version | size                           │
├──────────────────────────────────────────────────────────────────┤
│ [1] dataptr (Value*)                                             │
├──────────────────────────────────────────────────────────────────┤
│ [2-5] bitmap[0-3]                                                │
├──────────────────────────────────────────────────────────────────┤
│ [6] child_ptr[0]                                                 │
│ ...                                                              │
└──────────────────────────────────────────────────────────────────┘
```

### Layout 8: SKIP | LIST (Path compression to branch point)

```
Flags: SKIP | LIST
┌──────────────────────────────────────────────────────────────────┐
│ [0] HEADER: SKIP | LIST | version | size                         │
├──────────────────────────────────────────────────────────────────┤
│ [1] skip_byte_length                                             │
│ [2..M] skip_chars                                                │
├──────────────────────────────────────────────────────────────────┤
│ [M+1] small_list                                                 │
├──────────────────────────────────────────────────────────────────┤
│ [M+2] child_ptr[0]                                               │
│ ...                                                              │
└──────────────────────────────────────────────────────────────────┘
```

### Layout 9: SKIP | POP (Path compression to large branch)

```
Flags: SKIP | POP
(Similar structure with bitmap instead of small_list)
```

### Layout 10: SKIP | SKIP_EOS | LIST

```
Flags: SKIP | SKIP_EOS | LIST
┌──────────────────────────────────────────────────────────────────┐
│ [0] HEADER                                                       │
├──────────────────────────────────────────────────────────────────┤
│ [1] skip_byte_length                                             │
│ [2..M] skip_chars                                                │
├──────────────────────────────────────────────────────────────────┤
│ [M+1] dataptr (Value*) for SKIP_EOS                              │
├──────────────────────────────────────────────────────────────────┤
│ [M+2] small_list                                                 │
├──────────────────────────────────────────────────────────────────┤
│ [M+3] child_ptr[0]                                               │
│ ...                                                              │
└──────────────────────────────────────────────────────────────────┘
```

### Layout 11: SKIP | SKIP_EOS | POP

(Similar with bitmap)

### Layout 12: EOS | SKIP | SKIP_EOS | LIST

```
Flags: EOS | SKIP | SKIP_EOS | LIST
┌──────────────────────────────────────────────────────────────────┐
│ [0] HEADER                                                       │
├──────────────────────────────────────────────────────────────────┤
│ [1] dataptr for EOS                                              │
├──────────────────────────────────────────────────────────────────┤
│ [2] skip_byte_length                                             │
│ [3..M] skip_chars                                                │
├──────────────────────────────────────────────────────────────────┤
│ [M+1] dataptr for SKIP_EOS                                       │
├──────────────────────────────────────────────────────────────────┤
│ [M+2] small_list                                                 │
├──────────────────────────────────────────────────────────────────┤
│ [M+3] child_ptr[0]                                               │
│ ...                                                              │
└──────────────────────────────────────────────────────────────────┘
```

**Note:** Only valid when fixed_len == 0.

### Layout 13: EOS | SKIP | SKIP_EOS | POP

(Similar with bitmap. Only valid when fixed_len == 0.)

---

## SWAR Techniques

### Small List Structure (LIST flag, 1-7 children)

```
┌────────────────────────────────────────────────────────────────┬────────┐
│              Sorted characters (up to 7 bytes)                 │ count  │
└────────────────────────────────────────────────────────────────┴────────┘
  bytes 0-6 (characters in sorted order, big-endian)               byte 7
```

**Key SWAR Operations:**

#### 1. Byte Broadcasting
```cpp
constexpr uint64_t REP = 0x0101010101010101ULL;
uint64_t broadcast = REP * byte_value;
```

#### 2. Zero Byte Detection (Finding a character)
```cpp
constexpr uint64_t rep = 0x01'01'01'01'01'01'01'00ULL;  // exclude count byte
constexpr uint64_t low_bits = 0x7F'7F'7F'7F'7F'7F'7F'7FULL;

uint64_t diff = n_ ^ (rep * search_char);
uint64_t zeros = ~((((diff & low_bits) + low_bits) | diff) | low_bits);

int pos = std::countl_zero(zeros) / 8;
return (pos + 1 <= count) ? pos + 1 : 0;
```

**Credit:** Bit Twiddling Hacks - "Determine if a word has a byte equal to n"

#### 3. SWAR Unsigned Byte Comparison (For insertion)
```cpp
constexpr uint64_t h = 0x80'80'80'80'80'80'80'80ULL;
constexpr uint64_t m = 0x7F'7F'7F'7F'7F'7F'7F'7FULL;

uint64_t diff_high = (chars ^ rep_x) & h;
uint64_t B_high_wins = rep_x & diff_high;
uint64_t same_high = ~diff_high & h;
uint64_t low_cmp = ~((chars | h) - rep_x) & h;

uint64_t lt = B_high_wins | (same_high & low_cmp);
int insert_pos = std::popcount(lt);
```

### PopCount Bitmap Structure (POP flag, 8+ children)

```cpp
class popcount_bitmap {
    uint64_t bits_[4]{};
    
public:
    bool find(unsigned char c, int* idx) const {
        int word = c >> 6, bit = c & 63;
        uint64_t mask = 1ULL << bit;
        if (!(bits_[word] & mask)) return false;
        
        *idx = std::popcount(bits_[word] & (mask - 1));
        for (int w = 0; w < word; ++w) 
            *idx += std::popcount(bits_[w]);
        return true;
    }
    
    int set(unsigned char c) {
        int word = c >> 6, bit = c & 63;
        uint64_t mask = 1ULL << bit;
        
        int idx = std::popcount(bits_[word] & (mask - 1));
        for (int w = 0; w < word; ++w) 
            idx += std::popcount(bits_[w]);
        
        bits_[word] |= mask;
        return idx;
    }
};
```

---

## Concurrency Model (THREADED=true)

### Pointer Bit Flags

```cpp
static constexpr uint64_t WRITE_BIT = 1ULL << 63;
static constexpr uint64_t READ_BIT  = 1ULL << 62;
static constexpr uint64_t PTR_MASK  = (1ULL << 62) - 1;
```

### Reader Protocol

**Path Traversal (cheap, no READ_BIT needed):**
```cpp
uint64_t child = child_slot.load(std::memory_order_acquire);
if (child & WRITE_BIT) {
    return retry_from_root();
}
Node* next = reinterpret_cast<Node*>(child & PTR_MASK);
```

**Data Access (leaf) - CAS to avoid reader-reader gap:**
```cpp
while (true) {
    uint64_t old = data_slot.load(std::memory_order_acquire);
    
    if (old & WRITE_BIT) {
        return retry_from_root();
    }
    
    if (old & READ_BIT) {
        // Another reader has it - spin
        spin();
        continue;
    }
    
    // Try to set READ_BIT exclusively
    if (data_slot.compare_exchange_weak(old, old | READ_BIT, 
                                         std::memory_order_acq_rel,
                                         std::memory_order_acquire)) {
        break;  // We own READ_BIT
    }
    // CAS failed - retry loop
}

Value copy = *reinterpret_cast<Value*>(old & PTR_MASK);
data_slot.fetch_and(~READ_BIT, std::memory_order_release);
return copy;
```

**Why CAS instead of fetch_or:**
- R1 sets READ_BIT via fetch_or
- R2's fetch_or sees READ_BIT already set (no-op)
- R1 clears READ_BIT
- Writer grabs WRITE_BIT immediately
- R2 proceeds to copy with WRITE_BIT now set → use-after-free

With CAS, R2 either gets exclusive READ_BIT or spins until safe.

### Writer Protocol

**Phase 1: Build new path (outside lock, as a reader):**
```cpp
// Create complete new path from modification point down
// Reuse existing valid child pointers where unchanged
std::vector<uint64_t*> new_nodes = build_new_path(key, value);
```

**Phase 2: Collect version path (outside lock):**
```cpp
// For fixed_len > 0: std::array<PathEntry, fixed_len>
// For fixed_len == 0: std::vector<PathEntry>
auto path = collect_path_versions(key);

// If we hit a WRITE_BIT during traversal - retry (we're a reader here)
if (hit_write_bit) {
    cleanup_new_nodes(new_nodes);
    return retry();
}
```

**Phase 3: Acquire lock:**
```cpp
std::lock_guard<std::mutex> lock(write_mutex_);
```

**Phase 4: Verify path, redo if changed:**
```cpp
if (!path_versions_match(path)) {
    // Structure changed - redo work inside lock
    cleanup_new_nodes(new_nodes);
    new_nodes = build_new_path(key, value);
    path = collect_path_versions(key);
}
```

**Phase 5: Swap and cleanup flags, update versions:**
```cpp
// Mark data slot, wait for any reader
data_slot.fetch_or(WRITE_BIT, std::memory_order_acq_rel);
while (data_slot.load(std::memory_order_acquire) & READ_BIT) {
    spin();
}

// Mark path bottom-up
for (auto it = path.rbegin(); it != path.rend(); ++it) {
    it->slot->fetch_or(WRITE_BIT, std::memory_order_release);
}

// Perform swap
parent_slot->store(reinterpret_cast<uint64_t>(new_root), std::memory_order_release);

// Update versions in modified nodes
// Clear WRITE_BITs on unchanged portions
update_versions_and_clear_flags(path);
```

**Phase 6: Unlock and delete old nodes:**
```cpp
// Collect nodes to delete (store in vector/array before unlock)
std::vector<uint64_t*> to_delete = collect_replaced_nodes(path);

// Unlock happens here (end of lock_guard scope)
}

// Delete outside lock - safe because:
// - WRITE_BIT forced all readers to retry
// - Anyone who had old pointer hit WRITE_BIT on data access
for (auto* node : to_delete) {
    deallocate_node(node);
}
```

### Race Analysis

| Scenario | Outcome |
|----------|---------|
| Reader loads child ptr, then Writer marks | Reader enters node, CAS on data fails (WRITE_BIT) → retry |
| Writer marks, then Reader loads | Reader sees WRITE_BIT on path → retry |
| Reader CAS succeeds, then Writer marks | Writer spins on READ_BIT until reader copies |
| Writer marks, then Reader CAS | Reader sees WRITE_BIT → retry |
| R1 has READ_BIT, R2 tries | R2 spins until R1 clears, then CAS succeeds |
| R1 clears, Writer grabs before R2 | R2's CAS fails (WRITE_BIT now set) → retry |

---

## The `dataptr` Class

### Template Selection

```cpp
template <typename T>
static constexpr bool can_embed_v = 
    sizeof(T) <= sizeof(uint64_t) && 
    std::is_trivially_copyable_v<T>;

template <typename T, bool THREADED, typename Allocator>
class dataptr;
```

### Interface

```cpp
template <typename T, bool THREADED, typename Allocator>
class dataptr {
public:
    dataptr();
    ~dataptr();
    
    bool has_data() const;
    bool try_read(T& out);  // returns false if no data or WRITE_BIT (caller retries)
    
    void begin_write();     // THREADED: set WRITE_BIT, spin on READ_BIT
    void set(const T& value);
    void set(T&& value);
    void clear();
    void end_write();       // THREADED: clear WRITE_BIT
    
    uint64_t to_u64() const;
    static dataptr from_u64(uint64_t v);
};
```

### Implementation: THREADED=true

```cpp
template <typename T, typename Allocator>
class dataptr<T, true, Allocator> {
    std::atomic<uint64_t> bits_;
    
public:
    bool try_read(T& out) {
        while (true) {
            uint64_t old = bits_.load(std::memory_order_acquire);
            
            if (old & WRITE_BIT) return false;  // caller retries from root
            
            T* ptr = reinterpret_cast<T*>(old & PTR_MASK);
            if (!ptr) return false;
            
            if (old & READ_BIT) {
                spin();
                continue;
            }
            
            if (bits_.compare_exchange_weak(old, old | READ_BIT,
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
                out = *ptr;
                bits_.fetch_and(~READ_BIT, std::memory_order_release);
                return true;
            }
        }
    }
    
    void begin_write() {
        bits_.fetch_or(WRITE_BIT, std::memory_order_acq_rel);
        while (bits_.load(std::memory_order_acquire) & READ_BIT) {
            spin();
        }
    }
    
    void set(const T& value) {
        T* new_ptr = allocate_and_construct(value);
        T* old_ptr = reinterpret_cast<T*>(bits_.load(std::memory_order_relaxed) & PTR_MASK);
        bits_.store(reinterpret_cast<uint64_t>(new_ptr) | WRITE_BIT, std::memory_order_release);
        if (old_ptr) destroy_and_deallocate(old_ptr);
    }
    
    void set(T&& value) {
        T* new_ptr = allocate_and_construct(std::move(value));
        T* old_ptr = reinterpret_cast<T*>(bits_.load(std::memory_order_relaxed) & PTR_MASK);
        bits_.store(reinterpret_cast<uint64_t>(new_ptr) | WRITE_BIT, std::memory_order_release);
        if (old_ptr) destroy_and_deallocate(old_ptr);
    }
    
    void end_write() {
        bits_.fetch_and(~WRITE_BIT, std::memory_order_release);
    }
};
```

### Implementation: THREADED=false, Embeddable T

```cpp
template <typename T, typename Allocator>
class dataptr<T, false, Allocator> {
    uint64_t bits_;
    bool has_value_;
    
public:
    bool has_data() const { return has_value_; }
    
    bool try_read(T& out) {
        if (!has_value_) return false;
        std::memcpy(&out, &bits_, sizeof(T));
        return true;
    }
    
    void begin_write() { }
    void end_write() { }
    
    void set(const T& value) {
        std::memcpy(&bits_, &value, sizeof(T));
        has_value_ = true;
    }
    
    void set(T&& value) { set(value); }
    
    void clear() { has_value_ = false; }
};
```

### Implementation: THREADED=false, Large T

```cpp
template <typename T, typename Allocator>
class dataptr<T, false, Allocator> {
    T* ptr_ = nullptr;
    
public:
    bool has_data() const { return ptr_ != nullptr; }
    
    bool try_read(T& out) {
        if (!ptr_) return false;
        out = *ptr_;
        return true;
    }
    
    void begin_write() { }
    void end_write() { }
    
    void set(const T& value) {
        if (!ptr_) ptr_ = allocate(1);
        else std::destroy_at(ptr_);
        std::construct_at(ptr_, value);
    }
    
    void set(T&& value) {
        if (!ptr_) ptr_ = allocate(1);
        else std::destroy_at(ptr_);
        std::construct_at(ptr_, std::move(value));
    }
    
    void clear() {
        if (ptr_) {
            std::destroy_at(ptr_);
            deallocate(ptr_, 1);
            ptr_ = nullptr;
        }
    }
};
```

---

## Key Traits

```cpp
template <typename Key>
struct tktrie_traits;

template <>
struct tktrie_traits<std::string> {
    static constexpr size_t fixed_len = 0;
    static std::string_view to_bytes(const std::string& k) { return k; }
    static std::string from_bytes(std::string_view bytes) { return std::string(bytes); }
};

template <typename T>
    requires std::is_integral_v<T>
struct tktrie_traits<T> {
    static constexpr size_t fixed_len = sizeof(T);
    using unsigned_type = std::make_unsigned_t<T>;
    
    static std::string to_bytes(T k) {
        unsigned_type sortable;
        if constexpr (std::is_signed_v<T>) {
            sortable = static_cast<unsigned_type>(k) + (unsigned_type{1} << (sizeof(T) * 8 - 1));
        } else {
            sortable = k;
        }
        unsigned_type be = byteswap_if_little_endian(sortable);
        char buf[sizeof(T)];
        std::memcpy(buf, &be, sizeof(T));
        return std::string(buf, sizeof(T));
    }
    
    static T from_bytes(std::string_view bytes) {
        unsigned_type be;
        std::memcpy(&be, bytes.data(), sizeof(T));
        unsigned_type sortable = byteswap_if_little_endian(be);
        if constexpr (std::is_signed_v<T>) {
            return static_cast<T>(sortable - (unsigned_type{1} << (sizeof(T) * 8 - 1)));
        } else {
            return static_cast<T>(sortable);
        }
    }
};
```

---

## Class Structure

### File Organization

```
tktrie/
├── tktrie.h              // Main header, includes all others
├── tktrie_defines.h      // Constants, macros, utility functions
├── tktrie_traits.h       // Key type traits
├── tktrie_dataptr.h      // dataptr class implementations
├── tktrie_small_list.h   // SWAR small list operations
├── tktrie_popcount.h     // Bitmap operations
├── tktrie_node.h         // Node array layout and parsing (node_view, node_builder)
├── tktrie_iterator.h     // Iterator type
├── tktrie_impl.h         // Main trie implementation
├── tktrie_help_common.h  // Static helper functions (shared)
├── tktrie_help_insert.h  // Static helper functions (insert)
├── tktrie_help_remove.h  // Static helper functions (remove)
├── tktrie_help_nav.h     // Static helper functions (navigation)
├── tktrie_debug.h        // pretty_print, validate
└── tktrie_test.h         // Test utilities (optional)
```

**Guidelines:**
- No file should exceed ~1000 lines
- Use sparse comments
- Split helpers if any file grows too large

### Core Classes

```cpp
// tktrie_defines.h
namespace gteitelbaum {

#define KTRIE_FORCE_INLINE ...
#define KTRIE_DEBUG_ASSERT(cond) ...

// Validation control - set via compiler define: -DKTRIE_VALIDATE=1
#ifndef KTRIE_VALIDATE
#define KTRIE_VALIDATE 0
#endif
static constexpr bool k_validate = (KTRIE_VALIDATE != 0);

template <typename T>
constexpr T byteswap_if_little_endian(T value);

inline std::array<char, 8> to_char_static(uint64_t v);
inline uint64_t from_char_static(std::array<char, 8> arr);

}
```

```cpp
// tktrie_small_list.h
namespace gteitelbaum {

class small_list {
    uint64_t n_;
public:
    static constexpr int max_count = 7;
    
    int count() const;
    uint8_t char_at(int pos) const;
    int offset(char c) const;      // 1-based, 0 if not found
    int insert_pos(char c) const;
    
    uint64_t to_u64() const;
    static small_list from_u64(uint64_t v);
};

}
```

```cpp
// tktrie_popcount.h
namespace gteitelbaum {

class popcount_bitmap {
    uint64_t bits_[4]{};
public:
    bool find(unsigned char c, int* idx) const;
    int set(unsigned char c);
    int count() const;
    
    std::array<uint64_t, 4> to_array() const;
    static popcount_bitmap from_array(std::array<uint64_t, 4> arr);
};

}
```

```cpp
// tktrie_node.h
namespace gteitelbaum {

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
class node_view {
    using slot_type = std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t>;
    slot_type* arr_;
    
public:
    explicit node_view(slot_type* arr);
    
    uint64_t flags() const;
    uint32_t version() const;
    uint32_t size() const;
    
    bool has_eos() const;
    bool has_skip() const;
    bool has_skip_eos() const;
    bool has_list() const;
    bool has_pop() const;
    
    dataptr<T, THREADED, Allocator>* eos_data();
    dataptr<T, THREADED, Allocator>* skip_eos_data();
    
    size_t skip_length() const;
    std::string_view skip_chars() const;
    
    small_list* list();
    popcount_bitmap* bitmap();
    slot_type* child_ptrs();
    int child_count() const;
    
    slot_type* find_child(unsigned char c) const;
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
class node_builder {
    Allocator& alloc_;
    
public:
    using slot_type = std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t>;
    
    slot_type* build_eos(const T& value);
    slot_type* build_eos(T&& value);
    slot_type* build_skip(std::string_view skip, const T* skip_eos_value);
    slot_type* build_list(small_list list, std::span<slot_type*> children);
    slot_type* build_pop(popcount_bitmap bitmap, std::span<slot_type*> children);
    // ... other layouts
    
    slot_type* clone_with_new_child(node_view<T, THREADED, Allocator, FIXED_LEN> src,
                                     unsigned char c, slot_type* child);
    slot_type* clone_with_split_skip(node_view<T, THREADED, Allocator, FIXED_LEN> src,
                                      size_t split_pos, const T* new_value);
    
    void deallocate_node(slot_type* node);
};

}
```

```cpp
// tktrie_help_common.h
namespace gteitelbaum {

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct trie_helpers {
    using slot_type = std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t>;
    using node_view_t = node_view<T, THREADED, Allocator, FIXED_LEN>;
    using node_builder_t = node_builder<T, THREADED, Allocator, FIXED_LEN>;
    
    // Common utilities
    static uint64_t load_slot(slot_type* slot);
    static void store_slot(slot_type* slot, uint64_t value);
    static bool cas_slot(slot_type* slot, uint64_t expected, uint64_t desired);
    
    static void spin();
};

}
```

```cpp
// tktrie_help_nav.h
namespace gteitelbaum {

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct nav_helpers : trie_helpers<T, THREADED, Allocator, FIXED_LEN> {
    using base = trie_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using slot_type = typename base::slot_type;
    
    // Returns nullptr if not found, or if WRITE_BIT encountered (sets hit_write)
    static slot_type* find_node(slot_type* root, std::string_view key, bool& hit_write);
    
    // Collect path with versions for optimistic concurrency
    template <typename PathContainer>
    static bool collect_path(slot_type* root, std::string_view key, 
                             PathContainer& path, bool& hit_write);
};

}
```

```cpp
// tktrie_help_insert.h
namespace gteitelbaum {

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct insert_helpers : trie_helpers<T, THREADED, Allocator, FIXED_LEN> {
    using base = trie_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using slot_type = typename base::slot_type;
    
    // Build new path for insertion
    static std::vector<slot_type*> build_insert_path(
        node_builder_t& builder,
        slot_type* cur,
        std::string_view remaining_key,
        const T& value,
        size_t depth);
    
    static std::vector<slot_type*> build_insert_path(
        node_builder_t& builder,
        slot_type* cur,
        std::string_view remaining_key,
        T&& value,
        size_t depth);
};

}
```

```cpp
// tktrie_help_remove.h
namespace gteitelbaum {

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct remove_helpers : trie_helpers<T, THREADED, Allocator, FIXED_LEN> {
    using base = trie_helpers<T, THREADED, Allocator, FIXED_LEN>;
    using slot_type = typename base::slot_type;
    
    // Build new path for removal (may collapse nodes)
    static std::vector<slot_type*> build_remove_path(
        node_builder_t& builder,
        slot_type* cur,
        std::string_view remaining_key,
        size_t depth);
};

}
```

```cpp
// tktrie_iterator.h
namespace gteitelbaum {

template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie;

template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie_iterator {
    friend class tktrie<Key, T, THREADED, Allocator>;
    
    tktrie<Key, T, THREADED, Allocator>* parent_;
    std::string key_bytes_;  // stored as bytes, converted via traits on access
    T value_;                // copy of value
    bool valid_;
    
public:
    using value_type = std::pair<Key, T>;
    using traits = tktrie_traits<Key>;
    
    tktrie_iterator();  // end iterator
    tktrie_iterator(tktrie<Key, T, THREADED, Allocator>* parent,
                    std::string_view key_bytes, const T& value);
    
    Key key() const { return traits::from_bytes(key_bytes_); }
    const T& value() const { return value_; }
    T& value() { return value_; }
    
    value_type operator*() const { return {key(), value_}; }
    
    bool valid() const { return valid_; }
    bool operator==(const tktrie_iterator& o) const;
    bool operator!=(const tktrie_iterator& o) const;
    
    // Iterator is a reader - follows same rules including retry and READ_BIT tagging
    tktrie_iterator& operator++();
    tktrie_iterator operator++(int);
};

}
```

```cpp
// tktrie_debug.h
namespace gteitelbaum {

template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie;

template <typename Key, typename T, bool THREADED, typename Allocator>
struct trie_debug {
    using trie_type = tktrie<Key, T, THREADED, Allocator>;
    using slot_type = std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t>;
    
    // Print tree structure with all details (flags, versions, etc.)
    static void pretty_print(const trie_type& trie, std::ostream& os);
    static void pretty_print_node(slot_type* node, std::ostream& os, 
                                   int depth, const std::string& prefix);
    
    // Validate all invariants - asserts on failure
    // No-op if k_validate == false
    static void validate(const trie_type& trie);
    static void validate_node(slot_type* node, size_t depth, size_t fixed_len);
};

// Writers call this after modifications
template <typename Key, typename T, bool THREADED, typename Allocator>
inline void validate_trie(const tktrie<Key, T, THREADED, Allocator>& trie) {
    if constexpr (k_validate) {
        trie_debug<Key, T, THREADED, Allocator>::validate(trie);
    }
}

}
```

```cpp
// tktrie_impl.h / tktrie.h
namespace gteitelbaum {

template <typename Key, typename T, bool THREADED = false,
          typename Allocator = std::allocator<uint64_t>>
class tktrie {
public:
    using traits = tktrie_traits<Key>;
    static constexpr size_t fixed_len = traits::fixed_len;
    using size_type = std::size_t;
    using iterator = tktrie_iterator<Key, T, THREADED, Allocator>;
    
private:
    using slot_type = std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t>;
    using node_view_t = node_view<T, THREADED, Allocator, fixed_len>;
    using node_builder_t = node_builder<T, THREADED, Allocator, fixed_len>;
    using dataptr_t = dataptr<T, THREADED, Allocator>;
    
    // Path container type based on fixed_len
    using path_container = std::conditional_t<
        (fixed_len > 0),
        std::array<PathEntry, fixed_len>,
        std::vector<PathEntry>
    >;
    
    slot_type* root_;
    std::conditional_t<THREADED, std::atomic<size_type>, size_type> elem_count_{0};
    std::conditional_t<THREADED, std::mutex, empty_mutex> write_mutex_;
    Allocator alloc_;
    
public:
    // Construction
    tktrie();
    explicit tktrie(const Allocator& alloc);
    
    // Destruction
    ~tktrie();
    
    // Copy (deep copy)
    tktrie(const tktrie& other);
    tktrie& operator=(const tktrie& other);
    
    // Move (shallow)
    tktrie(tktrie&& other) noexcept;
    tktrie& operator=(tktrie&& other) noexcept;
    
    // Capacity
    bool empty() const;
    size_type size() const;
    
    // Lookup
    bool contains(const Key& key) const;
    iterator find(const Key& key) const;
    iterator end() const;
    
    // Modification
    std::pair<iterator, bool> insert(const std::pair<const Key, T>& value);
    std::pair<iterator, bool> insert(std::pair<const Key, T>&& value);
    
    template <typename... Args>
    std::pair<iterator, bool> emplace(const Key& key, Args&&... args);
    
    bool erase(const Key& key);
    void clear();
    
    // Range operations (stubs - implementation deferred)
    std::pair<iterator, iterator> prefix_match(const std::string& prefix) const
        requires (fixed_len == 0);
    std::pair<iterator, iterator> prefix_match(const Key& key, size_t depth) const
        requires (fixed_len > 0);
    
    // Iteration
    iterator begin() const;
    
    // Debug
    void pretty_print(std::ostream& os = std::cout) const;
    
private:
    friend struct trie_debug<Key, T, THREADED, Allocator>;
    
    bool contains_impl(std::string_view key_bytes) const;
    iterator find_impl(std::string_view key_bytes) const;
    
    std::pair<iterator, bool> insert_impl(const Key& key, const T& value);
    std::pair<iterator, bool> insert_impl(const Key& key, T&& value);
    
    bool erase_impl(const Key& key);
    
    void delete_tree(slot_type* node);
    slot_type* deep_copy_node(slot_type* node);
};

struct empty_mutex {
    void lock() { }
    void unlock() { }
};

}
```

---

## Algorithm Sketches

### contains() - Lock-Free Read

```cpp
template <typename Key, typename T, bool THREADED, typename Allocator>
bool tktrie<Key, T, THREADED, Allocator>::contains_impl(std::string_view kv) const {
retry:
    slot_type* cur = root_;
    
    while (cur) {
        node_view_t view(cur);
        
        if (view.has_skip()) {
            std::string_view skip = view.skip_chars();
            if (kv.size() < skip.size()) return false;
            if (kv.substr(0, skip.size()) != skip) return false;
            kv.remove_prefix(skip.size());
            
            if (kv.empty()) {
                if (!view.has_skip_eos()) return false;
                // Read data to verify it exists (follows reader protocol with CAS)
                T dummy;
                if (!view.skip_eos_data()->try_read(dummy)) {
                    if constexpr (THREADED) goto retry;
                    return false;
                }
                return true;
            }
        }
        
        if (kv.empty()) {
            if (!view.has_eos()) return false;
            T dummy;
            if (!view.eos_data()->try_read(dummy)) {
                if constexpr (THREADED) goto retry;
                return false;
            }
            return true;
        }
        
        slot_type* child_slot = view.find_child(static_cast<unsigned char>(kv[0]));
        if (!child_slot) return false;
        
        uint64_t child_ptr;
        if constexpr (THREADED) {
            child_ptr = child_slot->load(std::memory_order_acquire);
            if (child_ptr & WRITE_BIT) goto retry;
            child_ptr &= PTR_MASK;
        } else {
            child_ptr = *child_slot;
        }
        
        cur = reinterpret_cast<slot_type*>(child_ptr);
        kv.remove_prefix(1);
    }
    
    return false;
}
```

### insert() - Optimistic then Locked

```cpp
template <typename Key, typename T, bool THREADED, typename Allocator>
auto tktrie<Key, T, THREADED, Allocator>::insert_impl(const Key& key, T&& value)
    -> std::pair<iterator, bool>
{
    std::string key_bytes = traits::to_bytes(key);
    node_builder_t builder(alloc_);
    
    // Phase 1: Build new path (outside lock)
    bool hit_write = false;
    auto new_nodes = insert_helpers<T, THREADED, Allocator, fixed_len>
        ::build_insert_path(builder, root_, key_bytes, std::move(value), 0);
    
    if (new_nodes.empty()) {
        // Key already exists
        return {find_impl(key_bytes), false};
    }
    
    // Phase 2: Collect version path (outside lock, as reader)
    // If WRITE_BIT encountered, retry (we're a reader at this point)
    path_container path;
    if constexpr (fixed_len == 0) path.reserve(16);
    
    if (!nav_helpers<T, THREADED, Allocator, fixed_len>
            ::collect_path(root_, key_bytes, path, hit_write)) {
        if (hit_write) {
            // Clean up and retry
            for (auto* n : new_nodes) builder.deallocate_node(n);
            return insert_impl(key, std::move(value));  // retry
        }
    }
    
    // Phase 3: Lock
    std::lock_guard lock(write_mutex_);
    
    // Phase 4: Verify path, redo if changed
    bool path_changed = false;
    for (const auto& entry : path) {
        node_view_t view(entry.node);
        if (view.version() != entry.version) {
            path_changed = true;
            break;
        }
    }
    
    if (path_changed) {
        for (auto* n : new_nodes) builder.deallocate_node(n);
        // Redo inside lock
        new_nodes = insert_helpers<T, THREADED, Allocator, fixed_len>
            ::build_insert_path(builder, root_, key_bytes, std::move(value), 0);
        nav_helpers<T, THREADED, Allocator, fixed_len>
            ::collect_path(root_, key_bytes, path, hit_write);
    }
    
    // Collect old nodes for deletion (before swap)
    std::vector<slot_type*> to_delete;
    // ... identify replaced nodes
    
    // Phase 5: Swap and cleanup flags, update versions
    if constexpr (THREADED) {
        // Mark data slot, wait for any reader
        // Mark path bottom-up with WRITE_BIT
        // Perform swap
        // Update versions in modified nodes
        // Clear WRITE_BITs on unchanged portions
    } else {
        // Direct swap
    }
    
    elem_count_.fetch_add(1, std::memory_order_relaxed);
    
    // Validate after modification
    validate_trie(*this);
    
    // Phase 6: Unlock (implicit at end of scope)
    }  // lock released here
    
    // Delete old nodes outside lock
    for (auto* n : to_delete) {
        builder.deallocate_node(n);
    }
    
    return {iterator(this, key_bytes, value), true};
}
```

---

## Copy and Move Semantics

### Move (Shallow)

```cpp
tktrie(tktrie&& other) noexcept
    : root_(other.root_)
    , elem_count_(other.elem_count_.load())
    , alloc_(std::move(other.alloc_))
{
    other.root_ = nullptr;
    other.elem_count_ = 0;
}

tktrie& operator=(tktrie&& other) noexcept {
    if (this != &other) {
        delete_tree(root_);
        root_ = other.root_;
        elem_count_ = other.elem_count_.load();
        alloc_ = std::move(other.alloc_);
        other.root_ = nullptr;
        other.elem_count_ = 0;
    }
    return *this;
}
```

### Copy (Deep)

```cpp
tktrie(const tktrie& other)
    : alloc_(other.alloc_)
{
    root_ = deep_copy_node(other.root_);
    elem_count_ = other.elem_count_.load();
}

tktrie& operator=(const tktrie& other) {
    if (this != &other) {
        tktrie tmp(other);  // copy
        *this = std::move(tmp);  // move
    }
    return *this;
}

slot_type* deep_copy_node(slot_type* src) {
    if (!src) return nullptr;
    
    node_view_t view(src);
    size_t size = view.size();
    
    slot_type* dst = alloc_.allocate(size);
    
    // Copy header and non-pointer data
    // Deep copy dataptr (allocate new Value copies)
    // Recursively deep copy child nodes
    // ...
    
    return dst;
}
```

---

## Debug Facilities

### pretty_print

```cpp
template <typename Key, typename T, bool THREADED, typename Allocator>
void trie_debug<Key, T, THREADED, Allocator>::pretty_print(
    const trie_type& trie, std::ostream& os)
{
    os << "tktrie size=" << trie.size() << "\n";
    if (trie.root_) {
        pretty_print_node(trie.root_, os, 0, "");
    }
}

void pretty_print_node(slot_type* node, std::ostream& os,
                       int depth, const std::string& prefix)
{
    node_view_t view(node);
    
    std::string indent(depth * 2, ' ');
    
    os << indent << prefix << "NODE["
       << " flags=0x" << std::hex << view.flags() << std::dec
       << " ver=" << view.version()
       << " size=" << view.size()
       << " ]\n";
    
    if (view.has_eos()) {
        os << indent << "  EOS: (has data)\n";
    }
    
    if (view.has_skip()) {
        os << indent << "  SKIP[" << view.skip_length() << "]: \"" 
           << view.skip_chars() << "\"\n";
        if (view.has_skip_eos()) {
            os << indent << "  SKIP_EOS: (has data)\n";
        }
    }
    
    if (view.has_list()) {
        small_list* lst = view.list();
        os << indent << "  LIST[" << lst->count() << "]: ";
        for (int i = 0; i < lst->count(); ++i) {
            char c = lst->char_at(i);
            if (std::isprint(c)) os << "'" << c << "' ";
            else os << "0x" << std::hex << (int)(unsigned char)c << std::dec << " ";
        }
        os << "\n";
        
        slot_type* children = view.child_ptrs();
        for (int i = 0; i < lst->count(); ++i) {
            char c = lst->char_at(i);
            std::string child_prefix;
            if (std::isprint(c)) child_prefix = std::string("'") + c + "' -> ";
            else child_prefix = "0x" + to_hex(c) + " -> ";
            
            uint64_t ptr = load_slot(&children[i]) & PTR_MASK;
            if (ptr) {
                pretty_print_node(reinterpret_cast<slot_type*>(ptr), os,
                                  depth + 1, child_prefix);
            }
        }
    }
    
    if (view.has_pop()) {
        popcount_bitmap* bmp = view.bitmap();
        os << indent << "  POP[" << bmp->count() << " children]\n";
        // Similar child iteration with bitmap
    }
}
```

### validate

```cpp
template <typename Key, typename T, bool THREADED, typename Allocator>
void trie_debug<Key, T, THREADED, Allocator>::validate(const trie_type& trie) {
    if constexpr (!k_validate) return;
    
    if (trie.root_) {
        validate_node(trie.root_, 0, trie_type::fixed_len);
    }
}

void validate_node(slot_type* node, size_t depth, size_t fixed_len) {
    node_view_t view(node);
    uint64_t flags = view.flags();
    
    // Invariant 1: LIST and POP mutually exclusive
    KTRIE_DEBUG_ASSERT(!((flags & FLAG_LIST) && (flags & FLAG_POP)));
    
    // Invariant 2: SKIP_EOS requires SKIP
    if (flags & FLAG_SKIP_EOS) {
        KTRIE_DEBUG_ASSERT(flags & FLAG_SKIP);
    }
    
    // Invariant 3: SKIP length > 0
    if (flags & FLAG_SKIP) {
        KTRIE_DEBUG_ASSERT(view.skip_length() > 0);
    }
    
    // Invariant 4: LIST count 1 requires SKIP
    if ((flags & FLAG_LIST) && view.list()->count() == 1) {
        KTRIE_DEBUG_ASSERT(flags & FLAG_SKIP);
    }
    
    // Invariant 6: fixed_len EOS/SKIP_EOS restrictions
    if (fixed_len > 0) {
        if (depth < fixed_len) {
            // Internal node - no EOS or SKIP_EOS
            KTRIE_DEBUG_ASSERT(!(flags & FLAG_EOS));
            KTRIE_DEBUG_ASSERT(!(flags & FLAG_SKIP_EOS));
        } else {
            // Leaf - EOS and SKIP_EOS mutually exclusive
            KTRIE_DEBUG_ASSERT(!((flags & FLAG_EOS) && (flags & FLAG_SKIP_EOS)));
        }
    }
    
    // Recurse to children
    if (flags & FLAG_LIST) {
        slot_type* children = view.child_ptrs();
        int count = view.list()->count();
        for (int i = 0; i < count; ++i) {
            uint64_t ptr = load_slot(&children[i]) & PTR_MASK;
            if (ptr) {
                validate_node(reinterpret_cast<slot_type*>(ptr), depth + 1, fixed_len);
            }
        }
    } else if (flags & FLAG_POP) {
        // Similar for POP
    }
}
```

---

## Exception Safety

**Strong guarantee** for single-element operations:

1. All allocations happen before any modifications (Phase 1: build new path)
2. New path is built completely before swap
3. If allocation fails, no state change occurs
4. Swap is a pointer exchange (cannot fail)
5. Deallocation of old nodes happens after successful swap (Phase 6)

---

## Testing Strategy

### Unit Tests

1. SWAR operations (small_list, popcount_bitmap)
2. Node layouts (all 13 valid configurations)
3. dataptr variants (embedded, pointer, threaded)
4. Key traits conversions

### Integration Tests

1. Basic CRUD operations
2. Edge cases (empty key, very long keys, prefix sharing)
3. fixed_len specialization
4. Concurrency stress tests (THREADED=true)

### Debug Tests

1. pretty_print output verification
2. validate catches all invariant violations
3. Validate called after every write operation (when KTRIE_VALIDATE=1)

---

## Future Enhancements

1. Memory-mapped mode
2. Compression for common patterns
3. Range queries / full iteration
4. Bulk loading optimization
5. Serialization

---

## References

1. Bit Twiddling Hacks - Zero byte detection
   https://graphics.stanford.edu/~seander/bithacks.html

2. SWAR techniques (Daniel Lemire's blog)

3. Optimistic concurrency control (Wikipedia)

4. Epoch-based reclamation (Hart thesis)
