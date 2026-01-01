# Array-Based Trie Design Document

## Overview

A thread-safe trie implementation where nodes are represented as contiguous arrays of `uint64_t`. This design prioritizes cache locality, SWAR (SIMD Within A Register) operations, and clean separation between threaded and non-threaded variants.

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

## Node Array Layout

Each node is a contiguous array of `uint64_t` values. The layout is determined by flags in the header.

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

**Size Capacity:** 27 bits allows up to 134,217,727 elements per node (far exceeding practical needs).

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
4. `LIST` with count 1 is invalid if `SKIP` is also set (extend the skip instead)
5. `nullptr` values are not allowed

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

### Layout 4: LIST (1-7 children, no data here)

```
Flags: LIST
┌──────────────────────────────────────────────────────────────────┐
│ [0] HEADER: LIST | version | size                                │
├──────────────────────────────────────────────────────────────────┤
│ [1] small_list: sorted_chars[0-6] | count (see SWAR section)     │
├──────────────────────────────────────────────────────────────────┤
│ [2] child_ptr[0] (Node*)                                         │
│ [3] child_ptr[1] (Node*)                                         │
│ ... (count child pointers)                                       │
└──────────────────────────────────────────────────────────────────┘
```

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
│ [5] child_ptr[0] (Node*)                                         │
│ [6] child_ptr[1] (Node*)                                         │
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

### Layout 13: EOS | SKIP | SKIP_EOS | POP

(Similar with bitmap)

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
Replicate a single byte to all positions:
```cpp
constexpr uint64_t REP = 0x0101010101010101ULL;
uint64_t broadcast = REP * byte_value;
// Example: byte = 0x41 ('A') → 0x4141414141414141
```

#### 2. Zero Byte Detection (Finding a character)
```cpp
// Find which bytes in a word equal search_char
constexpr uint64_t rep = 0x01'01'01'01'01'01'01'00ULL;  // exclude count byte
constexpr uint64_t low_bits = 0x7F'7F'7F'7F'7F'7F'7F'7FULL;

uint64_t diff = n_ ^ (rep * search_char);  // matching bytes become 0x00
uint64_t zeros = ~((((diff & low_bits) + low_bits) | diff) | low_bits);
// zeros has 0x80 in each position where match occurred

int pos = std::countl_zero(zeros) / 8;  // byte index of first match
return (pos + 1 <= count) ? pos + 1 : 0;  // 1-based offset, 0 if not found
```

**Credit:** Bit Twiddling Hacks - "Determine if a word has a byte equal to n"

#### 3. SWAR Unsigned Byte Comparison (For insertion)
Split into two cases based on high bit:
```cpp
constexpr uint64_t h = 0x80'80'80'80'80'80'80'80ULL;
constexpr uint64_t m = 0x7F'7F'7F'7F'7F'7F'7F'7FULL;

// Case 1: High bits differ - A < B iff B has high bit set
uint64_t diff_high = (chars ^ rep_x) & h;
uint64_t B_high_wins = rep_x & diff_high;

// Case 2: High bits match - compare low 7 bits
uint64_t same_high = ~diff_high & h;
uint64_t low_cmp = ~((chars | h) - rep_x) & h;

uint64_t lt = B_high_wins | (same_high & low_cmp);
int insert_pos = std::popcount(lt);  // count bytes where chars[i] < new_char
```

### PopCount Bitmap Structure (POP flag, 8+ children)

Four `uint64_t` words covering all 256 possible byte values:

```cpp
class PopCount {
    uint64_t bits[4]{};  // bits[c >> 6] & (1ULL << (c & 63))
    
public:
    bool find(unsigned char c, int* idx) const {
        int word = c >> 6, bit = c & 63;
        uint64_t mask = 1ULL << bit;
        if (!(bits[word] & mask)) return false;
        
        *idx = std::popcount(bits[word] & (mask - 1));
        for (int w = 0; w < word; ++w) 
            *idx += std::popcount(bits[w]);
        return true;
    }
    
    int set(unsigned char c) {
        int word = c >> 6, bit = c & 63;
        uint64_t mask = 1ULL << bit;
        
        int idx = std::popcount(bits[word] & (mask - 1));
        for (int w = 0; w < word; ++w) 
            idx += std::popcount(bits[w]);
        
        bits[word] |= mask;
        return idx;
    }
};
```

### LIST to POP Transition

When child count exceeds 7:
- Replace 1-word small_list with 4-word bitmap
- Net cost: +3 words for the transition
- Threshold of 7 keeps most nodes compact (sparse branching is common)

---

## Concurrency Model (THREADED=true)

### Pointer Bit Flags

```cpp
static constexpr uint64_t WRITE_BIT = 1ULL << 63;
static constexpr uint64_t READ_BIT  = 1ULL << 62;
static constexpr uint64_t PTR_MASK  = (1ULL << 62) - 1;
```

### Reader Protocol

**Path Traversal (cheap):**
```cpp
uint64_t child = child_slot.load(std::memory_order_acquire);
if (child & WRITE_BIT) {
    return retry_from_root();
}
Node* next = reinterpret_cast<Node*>(child & PTR_MASK);
```

**Data Access (leaf):**
```cpp
uint64_t old = data_slot.fetch_or(READ_BIT, std::memory_order_acq_rel);
if (old & WRITE_BIT) {
    data_slot.fetch_and(~READ_BIT, std::memory_order_release);
    return retry_from_root();
}

Value copy = *reinterpret_cast<Value*>(old & PTR_MASK);
data_slot.fetch_and(~READ_BIT, std::memory_order_release);
return copy;
```

**Reader-Reader Contention:**
- If another reader has READ_BIT set, the new reader's fetch_or is a no-op
- Both see WRITE_BIT clear, both proceed
- Brief overlap during copy is safe (both reading same data)

### Writer Protocol

**Marking Phase (bottom-up):**
```cpp
// 1. Mark data slot first
uint64_t old_data = data_slot.fetch_or(WRITE_BIT, std::memory_order_acq_rel);
if (old_data & READ_BIT) {
    // Reader is copying - spin until done
    while (data_slot.load(std::memory_order_acquire) & READ_BIT) {
        // spin
    }
}

// 2. Mark path from leaf to modification point (bottom-up)
for (auto it = path.rbegin(); it != path.rend(); ++it) {
    it->slot->fetch_or(WRITE_BIT, std::memory_order_release);
}
```

**Modification Phase:**
```cpp
std::lock_guard<std::mutex> lock(write_mutex_);

// Verify versions unchanged (optimistic check)
if (!path_versions_valid(path)) {
    // Another writer modified structure - retry
    clear_write_bits(path);
    return retry();
}

// Create new node, perform swap
Node* new_node = create_modified_node(...);
parent_slot->store(reinterpret_cast<uint64_t>(new_node), std::memory_order_release);

// Clear write bits on unchanged path portions
clear_unchanged_write_bits(path);
```

**Freeing Old Nodes:**
- Writer holds write lock during swap
- Any reader that got old pointer before marking will hit WRITE_BIT on data slot and retry
- Any reader that got old pointer after marking sees WRITE_BIT on path and retries
- After clearing bits and releasing lock, old node can be freed
- The "hole" is only the copy duration (reader sets READ_BIT → clears READ_BIT)

### Race Analysis

| Scenario | Outcome |
|----------|---------|
| Reader loads child ptr, then Writer marks | Reader enters node, sees WRITE_BIT on data → retry |
| Writer marks, then Reader loads | Reader sees WRITE_BIT on path → retry |
| Reader fetch_or READ_BIT, then Writer marks | Writer spins on READ_BIT until reader finishes copy |
| Writer marks, then Reader fetch_or READ_BIT | Reader sees WRITE_BIT → undo READ_BIT → retry |

**Key Invariant:** Reader never accesses memory that writer might free because:
1. Path traversal is pointer loads only (no dereference of node content until reaching data)
2. Data access sets READ_BIT, writer waits for it to clear before freeing

---

## The `dataptr` Class

Unified interface for data storage with different implementations based on template parameters.

### Template Selection

```cpp
template <typename T>
static constexpr bool can_embed_v = 
    sizeof(T) <= sizeof(uint64_t) && 
    std::is_trivially_copyable_v<T>;

template <typename T, bool THREADED, typename Allocator>
class dataptr {
    // Implementation selected based on THREADED and can_embed_v<T>
};
```

### Interface

```cpp
template <typename T, bool THREADED, typename Allocator>
class dataptr {
public:
    // Construction / Destruction
    dataptr();
    ~dataptr();  // frees owned data if present
    
    // Reading
    bool has_data() const;
    bool try_read(T& out);  // copies value to out, returns false if no data
    
    // Writing (call in sequence for THREADED=true)
    void begin_write();     // marks WRITE_BIT, spins on READ_BIT
    void set(const T& value);  // copy
    void set(T&& value);       // move
    void clear();              // remove data
    void end_write();       // clears WRITE_BIT
    
    // For non-threaded or when lock already held
    void set_direct(const T& value);
    void set_direct(T&& value);
    
    // Raw access (for node array serialization)
    uint64_t to_u64() const;
    static dataptr from_u64(uint64_t v);
};
```

### Implementation: THREADED=true

```cpp
template <typename T, typename Allocator>
class dataptr<T, true, Allocator> {
    std::atomic<uint64_t> bits_;
    
    static constexpr uint64_t WRITE_BIT = 1ULL << 63;
    static constexpr uint64_t READ_BIT  = 1ULL << 62;
    static constexpr uint64_t PTR_MASK  = (1ULL << 62) - 1;
    
public:
    bool has_data() const {
        return (bits_.load(std::memory_order_acquire) & PTR_MASK) != 0;
    }
    
    bool try_read(T& out) {
        while (true) {
            uint64_t old = bits_.fetch_or(READ_BIT, std::memory_order_acq_rel);
            
            if (old & WRITE_BIT) {
                bits_.fetch_and(~READ_BIT, std::memory_order_release);
                return false;  // caller should retry from root
            }
            
            T* ptr = reinterpret_cast<T*>(old & PTR_MASK);
            if (!ptr) {
                bits_.fetch_and(~READ_BIT, std::memory_order_release);
                return false;
            }
            
            out = *ptr;  // copy
            bits_.fetch_and(~READ_BIT, std::memory_order_release);
            return true;
        }
    }
    
    void begin_write() {
        bits_.fetch_or(WRITE_BIT, std::memory_order_acq_rel);
        while (bits_.load(std::memory_order_acquire) & READ_BIT) {
            // spin - reader is copying
        }
    }
    
    void set(const T& value) {
        T* new_ptr = /* allocate and copy construct */;
        T* old_ptr = reinterpret_cast<T*>(bits_.load(std::memory_order_relaxed) & PTR_MASK);
        bits_.store(reinterpret_cast<uint64_t>(new_ptr) | WRITE_BIT, std::memory_order_release);
        if (old_ptr) { /* destroy and deallocate old_ptr */ }
    }
    
    void set(T&& value) {
        T* new_ptr = /* allocate and move construct */;
        T* old_ptr = reinterpret_cast<T*>(bits_.load(std::memory_order_relaxed) & PTR_MASK);
        bits_.store(reinterpret_cast<uint64_t>(new_ptr) | WRITE_BIT, std::memory_order_release);
        if (old_ptr) { /* destroy and deallocate old_ptr */ }
    }
    
    void end_write() {
        bits_.fetch_and(~WRITE_BIT, std::memory_order_release);
    }
};
```

### Implementation: THREADED=false, Embeddable T

```cpp
template <typename T, typename Allocator>
    requires (!THREADED && can_embed_v<T>)
class dataptr<T, false, Allocator> {
    uint64_t bits_;
    bool has_value_;  // cannot use bits_ == 0 as sentinel (T might be 0)
    
public:
    bool has_data() const { return has_value_; }
    
    bool try_read(T& out) {
        if (!has_value_) return false;
        std::memcpy(&out, &bits_, sizeof(T));
        return true;
    }
    
    void begin_write() { }  // no-op
    void end_write() { }    // no-op
    
    void set(const T& value) {
        std::memcpy(&bits_, &value, sizeof(T));
        has_value_ = true;
    }
    
    void set(T&& value) {
        set(value);  // same as copy for trivially copyable
    }
    
    void clear() {
        has_value_ = false;
    }
};
```

### Implementation: THREADED=false, Large T

```cpp
template <typename T, typename Allocator>
    requires (!THREADED && !can_embed_v<T>)
class dataptr<T, false, Allocator> {
    T* ptr_;
    Allocator alloc_;
    
public:
    bool has_data() const { return ptr_ != nullptr; }
    
    bool try_read(T& out) {
        if (!ptr_) return false;
        out = *ptr_;
        return true;
    }
    
    void begin_write() { }  // no-op
    void end_write() { }    // no-op
    
    void set(const T& value) {
        if (ptr_) { std::destroy_at(ptr_); }
        else { ptr_ = alloc_.allocate(1); }
        std::construct_at(ptr_, value);
    }
    
    void set(T&& value) {
        if (ptr_) { std::destroy_at(ptr_); }
        else { ptr_ = alloc_.allocate(1); }
        std::construct_at(ptr_, std::move(value));
    }
    
    void clear() {
        if (ptr_) {
            std::destroy_at(ptr_);
            alloc_.deallocate(ptr_, 1);
            ptr_ = nullptr;
        }
    }
};
```

---

## Key Traits

Support for different key types via traits specialization:

```cpp
template <typename Key>
struct tktrie_traits;

// String keys
template <>
struct tktrie_traits<std::string> {
    static constexpr size_t fixed_len = 0;  // variable length
    static std::string_view to_bytes(const std::string& k) { return k; }
};

// Integral keys (sorted by value, not byte representation)
template <typename T>
    requires std::is_integral_v<T>
struct tktrie_traits<T> {
    static constexpr size_t fixed_len = sizeof(T);
    using unsigned_type = std::make_unsigned_t<T>;
    
    static std::string to_bytes(T k) {
        unsigned_type sortable;
        if constexpr (std::is_signed_v<T>) {
            // Flip sign bit so negative < positive in byte comparison
            sortable = static_cast<unsigned_type>(k) + (unsigned_type{1} << (sizeof(T) * 8 - 1));
        } else {
            sortable = k;
        }
        // Convert to big-endian for correct lexicographic ordering
        unsigned_type be = byteswap_if_little_endian(sortable);
        
        char buf[sizeof(T)];
        std::memcpy(buf, &be, sizeof(T));
        return std::string(buf, sizeof(T));
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
├── tktrie_node.h         // Node array layout and parsing
├── tktrie_iterator.h     // Iterator type
└── tktrie_impl.h         // Main trie implementation
```

### Core Classes

```cpp
// tktrie_defines.h
namespace gteitelbaum {
    // Force inline macro
    #define KTRIE_FORCE_INLINE ...
    
    // Debug assertions
    #define KTRIE_DEBUG_ASSERT(cond) ...
    
    // Byte order utilities
    template <typename T>
    constexpr T byteswap_if_little_endian(T value);
    
    // Char array casting utilities
    inline std::array<char, 8> to_char_static(uint64_t v);
    inline uint64_t from_char_static(std::array<char, 8> arr);
}

// tktrie_small_list.h
namespace gteitelbaum {
    class small_list {
        uint64_t n_;
    public:
        static constexpr int max_count = 7;
        
        int count() const;
        uint8_t char_at(int pos) const;
        int offset(char c) const;      // 1-based, 0 if not found (SWAR)
        int insert_pos(char c) const;  // position to insert (SWAR)
        
        uint64_t to_u64() const;
        static small_list from_u64(uint64_t v);
    };
}

// tktrie_popcount.h
namespace gteitelbaum {
    class popcount_bitmap {
        uint64_t bits_[4]{};
    public:
        bool find(unsigned char c, int* idx) const;
        int set(unsigned char c);  // returns index
        int count() const;
        
        std::array<uint64_t, 4> to_array() const;
        static popcount_bitmap from_array(std::array<uint64_t, 4> arr);
    };
}

// tktrie_node.h
namespace gteitelbaum {
    template <typename T, bool THREADED, typename Allocator>
    class node_view {
        // Non-owning view into a node array
        // Provides accessors based on flags
        uint64_t* arr_;
        
    public:
        explicit node_view(uint64_t* arr);
        
        // Header access
        uint64_t flags() const;
        uint32_t version() const;
        uint32_t size() const;
        
        // Flag queries
        bool has_eos() const;
        bool has_skip() const;
        bool has_skip_eos() const;
        bool has_list() const;
        bool has_pop() const;
        
        // Data access (offsets computed from flags)
        dataptr<T, THREADED, Allocator>* eos_data();
        dataptr<T, THREADED, Allocator>* skip_eos_data();
        
        // Skip access
        size_t skip_length() const;
        std::string_view skip_chars() const;
        
        // Children access
        small_list* list();
        popcount_bitmap* bitmap();
        uint64_t* child_ptrs();  // array of Node*
        int child_count() const;
        
        // Traversal
        uint64_t* find_child(unsigned char c) const;  // returns slot, or nullptr
    };
    
    template <typename T, bool THREADED, typename Allocator>
    class node_builder {
        // Constructs new node arrays
        Allocator& alloc_;
        
    public:
        // Build various node types
        uint64_t* build_eos(const T& value);
        uint64_t* build_skip(std::string_view skip, const T* skip_eos_value);
        uint64_t* build_list(small_list list, std::span<uint64_t*> children);
        // ... etc for all layouts
        
        // Modification helpers
        uint64_t* clone_with_new_child(node_view<T, THREADED, Allocator> src, 
                                        unsigned char c, uint64_t* child);
        uint64_t* clone_with_split_skip(node_view<T, THREADED, Allocator> src,
                                         size_t split_pos, const T* new_value);
    };
}

// tktrie_iterator.h
namespace gteitelbaum {
    template <typename Key, typename T>
    class tktrie_iterator {
        Key key_;
        T value_;
        bool valid_;
        
    public:
        using value_type = std::pair<Key, T>;
        
        tktrie_iterator();  // end iterator
        tktrie_iterator(const Key& k, const T& v);
        
        const Key& key() const;
        T& value();
        value_type operator*() const;
        
        bool valid() const;
        bool operator==(const tktrie_iterator& o) const;
        bool operator!=(const tktrie_iterator& o) const;
    };
}

// tktrie_impl.h / tktrie.h
namespace gteitelbaum {
    template <typename Key, typename T, bool THREADED = false, 
              typename Allocator = std::allocator<uint64_t>>
    class tktrie {
    public:
        using traits = tktrie_traits<Key>;
        using size_type = std::size_t;
        using iterator = tktrie_iterator<Key, T>;
        
    private:
        using node_view_t = node_view<T, THREADED, Allocator>;
        using node_builder_t = node_builder<T, THREADED, Allocator>;
        using dataptr_t = dataptr<T, THREADED, Allocator>;
        
        uint64_t* root_;
        std::conditional_t<THREADED, std::atomic<size_type>, size_type> elem_count_;
        std::conditional_t<THREADED, std::mutex, empty_mutex> write_mutex_;
        Allocator alloc_;
        
    public:
        // Construction / Destruction
        tktrie();
        explicit tktrie(const Allocator& alloc);
        ~tktrie();
        
        tktrie(const tktrie&) = delete;  // or implement deep copy
        tktrie& operator=(const tktrie&) = delete;
        tktrie(tktrie&&) noexcept;
        tktrie& operator=(tktrie&&) noexcept;
        
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
        
    private:
        // Internal implementation
        bool contains_impl(std::string_view key_bytes) const;
        iterator find_impl(const Key& key, std::string_view key_bytes) const;
        std::pair<iterator, bool> insert_impl(const Key& key, const T& value);
        std::pair<iterator, bool> insert_impl(const Key& key, T&& value);
        bool erase_impl(const Key& key);
        
        // Node management
        void delete_tree(uint64_t* node);
        uint64_t* clone_node(uint64_t* node);
    };
    
    // Empty mutex for THREADED=false
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
bool contains_impl(std::string_view kv) const {
    uint64_t* cur = root_;
    
    while (cur) {
        node_view_t view(cur);
        
        // Check skip sequence
        if (view.has_skip()) {
            std::string_view skip = view.skip_chars();
            if (kv.size() < skip.size()) return false;
            if (kv.substr(0, skip.size()) != skip) return false;
            kv.remove_prefix(skip.size());
            
            if (kv.empty()) {
                return view.has_skip_eos();
            }
        }
        
        // Check EOS
        if (kv.empty()) {
            return view.has_eos();
        }
        
        // Find child
        uint64_t* child_slot = view.find_child(static_cast<unsigned char>(kv[0]));
        if (!child_slot) return false;
        
        uint64_t child_ptr;
        if constexpr (THREADED) {
            child_ptr = reinterpret_cast<std::atomic<uint64_t>*>(child_slot)
                        ->load(std::memory_order_acquire);
            if (child_ptr & WRITE_BIT) {
                // Writer active - restart
                return contains_impl(/* original key */);
            }
            child_ptr &= PTR_MASK;
        } else {
            child_ptr = *child_slot;
        }
        
        cur = reinterpret_cast<uint64_t*>(child_ptr);
        kv.remove_prefix(1);
    }
    
    return false;
}
```

### insert() - Optimistic then Locked

```cpp
std::pair<iterator, bool> insert_impl(const Key& key, T&& value) {
    std::string key_bytes = traits::to_bytes(key);
    
    // Phase 1: Optimistic traversal (collect path + versions)
    struct PathEntry {
        uint64_t* node;
        uint64_t* child_slot;  // slot we followed
        uint32_t version;
    };
    std::vector<PathEntry> path;
    path.reserve(16);
    
    std::string_view kv(key_bytes);
    uint64_t* cur = root_;
    
    while (true) {
        node_view_t view(cur);
        path.push_back({cur, nullptr, view.version()});
        
        // Match skip
        if (view.has_skip()) {
            std::string_view skip = view.skip_chars();
            size_t common = 0;
            while (common < skip.size() && common < kv.size() && 
                   skip[common] == kv[common]) {
                ++common;
            }
            
            if (common < skip.size()) {
                // Need to split - break here
                break;
            }
            kv.remove_prefix(common);
            
            if (kv.empty() && view.has_skip_eos()) {
                // Key exists at skip_eos
                break;
            }
        }
        
        if (kv.empty()) {
            // Key ends at this node
            break;
        }
        
        // Find child
        uint64_t* child_slot = view.find_child(static_cast<unsigned char>(kv[0]));
        if (!child_slot) {
            // Need to add child - break here
            break;
        }
        
        path.back().child_slot = child_slot;
        
        uint64_t child_ptr = *child_slot;  // safe in phase 1
        if constexpr (THREADED) {
            child_ptr &= PTR_MASK;
        }
        
        cur = reinterpret_cast<uint64_t*>(child_ptr);
        kv.remove_prefix(1);
    }
    
    // Phase 2: Lock and verify
    std::lock_guard lock(write_mutex_);
    
    // Verify versions
    for (const auto& entry : path) {
        node_view_t view(entry.node);
        if (view.version() != entry.version) {
            // Structure changed - redo under lock
            return insert_locked(key, std::move(value), key_bytes);
        }
    }
    
    // Phase 3: Execute modification
    if constexpr (THREADED) {
        // Mark path with WRITE_BIT (bottom-up)
        // Handle any READ_BIT on data slots
        // Create new nodes, swap pointers
        // Clear WRITE_BIT on unchanged
    } else {
        // Direct modification
    }
    
    return do_insert(key, std::move(value), path, kv);
}
```

---

## Memory Management

### Node Allocation

Nodes are allocated as contiguous `uint64_t` arrays via the provided allocator:

```cpp
uint64_t* allocate_node(size_t num_words) {
    return std::allocator_traits<Allocator>::allocate(alloc_, num_words);
}

void deallocate_node(uint64_t* node, size_t num_words) {
    std::allocator_traits<Allocator>::deallocate(alloc_, node, num_words);
}
```

### Value Storage

- Small, trivially copyable values (≤8 bytes): Embedded in dataptr
- Larger values: Allocated separately via allocator (rebound to T)
- dataptr owns the value and frees on destruction/replacement

### Threaded Cleanup

When THREADED=true, nodes are freed immediately after swap because:
1. Writer marks data slot first, spins on READ_BIT
2. Writer marks path bottom-up with WRITE_BIT
3. Any in-flight reader either:
   - Already set READ_BIT on data → writer waits for copy to complete
   - Tries to set READ_BIT → sees WRITE_BIT → retries from root
4. After swap and clearing bits, no reader can have reference to old node

---

## Open Design Decisions

### 1. Copy Semantics for Trie

Options:
- Delete copy constructor/assignment (move-only)
- Deep copy all nodes
- Copy-on-write with shared ownership

**Recommendation:** Start with delete (move-only), add deep copy if needed.

### 2. Exception Safety

What if allocator throws during insert?
- Strong guarantee (no change on failure) requires careful node construction before swap
- Basic guarantee (valid state) is easier

**Recommendation:** Strong guarantee for single-element operations.

### 3. Iteration

Current design doesn't support iteration over all elements. Would require:
- Maintaining parent pointers, or
- Stack-based traversal
- Thread-safety complications for iteration

**Recommendation:** Defer full iteration support; focus on point queries first.

### 4. Prefix Operations

Natural extensions:
- `prefix_match(key)` - find longest prefix in trie
- `prefix_range(prefix)` - iterate all keys with prefix

**Recommendation:** Add after core functionality is stable.

---

## Testing Strategy

### Unit Tests

1. **Small list SWAR operations**
   - offset() finds correct position
   - insert_pos() computes correct position
   - Boundary cases (empty, full, duplicate)

2. **PopCount bitmap operations**
   - find() and set() correctness
   - Transition from list to bitmap

3. **Node layouts**
   - Each of 13 valid layouts parses correctly
   - Invalid flag combinations rejected

4. **dataptr variants**
   - Embedded small types
   - Pointer for large types
   - THREADED read/write bit protocol

### Integration Tests

1. **Basic operations**
   - Insert, find, erase on various key types
   - String keys, integer keys

2. **Edge cases**
   - Empty key
   - Single character key
   - Very long keys (skip compression)
   - Keys sharing long prefixes

3. **Concurrency (THREADED=true)**
   - Concurrent readers
   - Reader + writer
   - Multiple writers (serialized by mutex)
   - Stress test with random operations

### Benchmarks

1. Insert throughput (sequential keys, random keys)
2. Lookup latency (existing keys, missing keys)
3. Memory usage vs std::map / std::unordered_map
4. Concurrent read scaling

---

## Future Enhancements

1. **Persistent/memory-mapped mode** - Node arrays are allocation-friendly; could be memory-mapped
2. **Compression** - Store common skip patterns in shared dictionary
3. **Range queries** - Leverage sorted structure for range iteration
4. **Bulk loading** - Optimized construction from sorted input
5. **Serialization** - Dump/load trie to/from file

---

## References

1. Bit Twiddling Hacks - Zero byte detection
   https://graphics.stanford.edu/~seander/bithacks.html

2. SWAR byte comparison techniques
   https://lemire.me/blog/ (various posts on SIMD-within-a-register)

3. Optimistic concurrency control
   https://en.wikipedia.org/wiki/Optimistic_concurrency_control

4. Epoch-based reclamation
   https://www.cs.toronto.edu/~tomhart/papers/tomhart_thesis.pdf
