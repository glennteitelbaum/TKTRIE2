#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>

#ifdef NDEBUG
#define KTRIE_DEBUG_ASSERT(cond) ((void)0)
#else
#include <cassert>
#define KTRIE_DEBUG_ASSERT(cond) assert(cond)
#endif

// =============================================================================
// PLACEMENT NEW/DESTROY UTILITIES
// C++20 compatible, uses std::construct_at/destroy_at if available (C++20+)
// =============================================================================

template <typename T, typename... Args>
constexpr T* ktrie_construct_at(T* p, Args&&... args) {
#if __cplusplus >= 202002L && defined(__cpp_lib_constexpr_dynamic_alloc)
    return std::construct_at(p, std::forward<Args>(args)...);
#else
    return ::new (static_cast<void*>(p)) T(std::forward<Args>(args)...);
#endif
}

template <typename T>
constexpr void ktrie_destroy_at(T* p) {
#if __cplusplus >= 202002L && defined(__cpp_lib_constexpr_dynamic_alloc)
    std::destroy_at(p);
#else
    p->~T();
#endif
}

namespace gteitelbaum {

// =============================================================================
// ATOMIC STORAGE HELPERS - eliminates repeated if constexpr (THREADED) patterns
// =============================================================================

template <typename T, bool THREADED>
class atomic_storage {
    std::conditional_t<THREADED, std::atomic<T>, T> value_;
public:
    constexpr atomic_storage() noexcept : value_{} {}
    constexpr explicit atomic_storage(T v) noexcept : value_(v) {}
    
    T load() const noexcept {
        if constexpr (THREADED) return value_.load(std::memory_order_acquire);
        else return value_;
    }
    
    void store(T v) noexcept {
        if constexpr (THREADED) value_.store(v, std::memory_order_release);
        else value_ = v;
    }
    
    T exchange(T v) noexcept {
        if constexpr (THREADED) return value_.exchange(v, std::memory_order_acq_rel);
        else { T old = value_; value_ = v; return old; }
    }
    
    T fetch_add(T v) noexcept {
        if constexpr (THREADED) return value_.fetch_add(v, std::memory_order_acq_rel);
        else { T old = value_; value_ += v; return old; }
    }
    
    T fetch_sub(T v) noexcept {
        if constexpr (THREADED) return value_.fetch_sub(v, std::memory_order_acq_rel);
        else { T old = value_; value_ -= v; return old; }
    }
    
    T fetch_or(T v) noexcept {
        if constexpr (THREADED) return value_.fetch_or(v, std::memory_order_acq_rel);
        else { T old = value_; value_ |= v; return old; }
    }
    
    T fetch_and(T v) noexcept {
        if constexpr (THREADED) return value_.fetch_and(v, std::memory_order_acq_rel);
        else { T old = value_; value_ &= v; return old; }
    }
};

// Convenience alias for size counters
template <bool THREADED>
using atomic_counter = atomic_storage<size_t, THREADED>;

// =============================================================================
// HEADER FLAGS AND CONSTANTS
// =============================================================================

// Header layout (11 bits flags, 53 bits version):
// [LEAF:1][POISON:1][HAS_EOS:1][SKIP_USED:1][IS_FLOOR:1][IS_CEIL:1]
// [IS_SKIP:1][IS_BINARY:1][IS_LIST:1][IS_POP:1][IS_FULL:1][VERSION:53]
//
// Flag meanings:
//   LEAF:      Node is a leaf (stores values, not children)
//   POISON:    Node is being retired (EBR)
//   HAS_EOS:   Interior node has end-of-string value (FIXED_LEN==0 only)
//   SKIP_USED: Skip string is non-empty (optimization: skip skip_str() load if false)
//   IS_FLOOR:  At minimum count for type (erase needs structural change)
//   IS_CEIL:   At maximum count for type (insert needs structural change)
//   IS_SKIP/BINARY/LIST/POP/FULL: Node type (exactly one set)
//
// IS_FLOOR/IS_CEIL thresholds (child/value count, EOS not counted):
//   SKIP:   floor=always, ceil=n/a (single value)
//   BINARY: floor=always (1-2), ceil=count==2
//   LIST:   floor=count==3, ceil=count==7
//   POP:    floor=count==8, ceil=count==32
//   FULL:   floor=count==33, ceil=never

static constexpr uint64_t FLAG_LEAF      = 1ULL << 63;
static constexpr uint64_t FLAG_POISON    = 1ULL << 62;
static constexpr uint64_t FLAG_HAS_EOS   = 1ULL << 61;
static constexpr uint64_t FLAG_SKIP_USED = 1ULL << 60;
static constexpr uint64_t FLAG_IS_FLOOR  = 1ULL << 59;
static constexpr uint64_t FLAG_IS_CEIL   = 1ULL << 58;
static constexpr uint64_t FLAG_SKIP      = 1ULL << 57;
static constexpr uint64_t FLAG_BINARY    = 1ULL << 56;
static constexpr uint64_t FLAG_LIST      = 1ULL << 55;
static constexpr uint64_t FLAG_POP       = 1ULL << 54;
static constexpr uint64_t FLAG_FULL      = 1ULL << 53;

static constexpr uint64_t VERSION_MASK = (1ULL << 53) - 1;
static constexpr uint64_t FLAGS_MASK = ~VERSION_MASK;
static constexpr uint64_t TYPE_FLAGS_MASK = FLAG_SKIP | FLAG_BINARY | FLAG_LIST | FLAG_POP | FLAG_FULL;

static constexpr int BINARY_MIN = 1;
static constexpr int BINARY_MAX = 2;
static constexpr int LIST_MIN = 3;
static constexpr int LIST_MAX = 7;
static constexpr int POP_MIN = 8;
static constexpr int POP_MAX = 32;
static constexpr int FULL_MIN = 33;

// Interior FULL node header with poison flag set - used for retry sentinel
static constexpr uint64_t RETRY_SENTINEL_HEADER = FLAG_POISON | FLAG_FULL;

// Header queries
inline constexpr bool is_poisoned_header(uint64_t h) noexcept {
    return (h & FLAG_POISON) != 0;
}
inline constexpr bool is_leaf(uint64_t h) noexcept { return (h & FLAG_LEAF) != 0; }
inline constexpr bool has_eos_flag(uint64_t h) noexcept { return (h & FLAG_HAS_EOS) != 0; }
inline constexpr bool has_skip_used(uint64_t h) noexcept { return (h & FLAG_SKIP_USED) != 0; }
inline constexpr bool is_at_floor(uint64_t h) noexcept { return (h & FLAG_IS_FLOOR) != 0; }
inline constexpr bool is_at_ceil(uint64_t h) noexcept { return (h & FLAG_IS_CEIL) != 0; }
inline constexpr uint64_t get_version(uint64_t h) noexcept { return h & VERSION_MASK; }

// Make header with all relevant flags
// type_flag should be one of FLAG_SKIP, FLAG_BINARY, FLAG_LIST, FLAG_POP, FLAG_FULL
inline constexpr uint64_t make_header(bool is_leaf, uint64_t type_flag, 
                                       bool skip_used = false, bool at_floor = false, 
                                       bool at_ceil = false, uint64_t version = 0) noexcept {
    return (is_leaf ? FLAG_LEAF : 0) 
         | type_flag 
         | (skip_used ? FLAG_SKIP_USED : 0)
         | (at_floor ? FLAG_IS_FLOOR : 0)
         | (at_ceil ? FLAG_IS_CEIL : 0)
         | (version & VERSION_MASK);
}

// Bump version preserving all flags
inline constexpr uint64_t bump_version(uint64_t h) noexcept {
    uint64_t flags = h & FLAGS_MASK;
    uint64_t ver = (h & VERSION_MASK) + 1;
    return flags | (ver & VERSION_MASK);
}

// Flag manipulation helpers
inline constexpr uint64_t set_flag(uint64_t h, uint64_t flag) noexcept { return h | flag; }
inline constexpr uint64_t clear_flag(uint64_t h, uint64_t flag) noexcept { return h & ~flag; }

template <typename T>
constexpr T ktrie_byteswap(T value) noexcept {
    static_assert(std::is_integral_v<T>);
    if constexpr (sizeof(T) == 1) return value;
    else if constexpr (sizeof(T) == 2)
        return static_cast<T>(((static_cast<uint16_t>(value) & 0x00FFu) << 8) |
                              ((static_cast<uint16_t>(value) & 0xFF00u) >> 8));
    else if constexpr (sizeof(T) == 4) {
        uint32_t v = static_cast<uint32_t>(value);
        return static_cast<T>(((v & 0x000000FFu) << 24) | ((v & 0x0000FF00u) << 8) |
                              ((v & 0x00FF0000u) >> 8)  | ((v & 0xFF000000u) >> 24));
    } else {
        uint64_t v = static_cast<uint64_t>(value);
        return static_cast<T>(
            ((v & 0x00000000000000FFull) << 56) | ((v & 0x000000000000FF00ull) << 40) |
            ((v & 0x0000000000FF0000ull) << 24) | ((v & 0x00000000FF000000ull) << 8)  |
            ((v & 0x000000FF00000000ull) >> 8)  | ((v & 0x0000FF0000000000ull) >> 24) |
            ((v & 0x00FF000000000000ull) >> 40) | ((v & 0xFF00000000000000ull) >> 56));
    }
}

template <typename T>
constexpr T to_big_endian(T value) noexcept {
    if constexpr (std::endian::native == std::endian::big) return value;
    else return ktrie_byteswap(value);
}

template <typename T>
constexpr T from_big_endian(T value) noexcept { return to_big_endian(value); }

// =============================================================================
// SMALL_LIST - packed list of up to 7 chars in single uint64
// Layout: [count:8][char6:8][char5:8][char4:8][char3:8][char2:8][char1:8][char0:8]
// Templated on THREADED to avoid atomic overhead when not needed
// =============================================================================

template <bool THREADED>
class small_list {
    atomic_storage<uint64_t, THREADED> data_{0};
public:
    constexpr small_list() noexcept = default;
    
    small_list(const small_list& o) noexcept : data_(o.data_.load()) {}
    small_list& operator=(const small_list& o) noexcept {
        data_.store(o.data_.load());
        return *this;
    }
    
    int count() const noexcept { 
        return static_cast<int>((data_.load() >> 56) & 0xFF); 
    }
    
    unsigned char char_at(int i) const noexcept {
        return static_cast<unsigned char>((data_.load() >> (i * 8)) & 0xFF);
    }
    
    int find(unsigned char c) const noexcept {
        uint64_t d = data_.load();
        int n = static_cast<int>((d >> 56) & 0xFF);
        [[assume(n >= 0 && n < 8)]];
        for (int i = 0; i < n; ++i) {
            if (static_cast<unsigned char>((d >> (i * 8)) & 0xFF) == c) return i;
        }
        return -1;
    }
    
    int add(unsigned char c) noexcept {
        uint64_t d = data_.load();
        int n = static_cast<int>((d >> 56) & 0xFF);
        [[assume(n >= 0 && n < 7)]];  // Must have room to add
        d = (d & ~(0xFFULL << 56)) | (static_cast<uint64_t>(c) << (n * 8)) |
            (static_cast<uint64_t>(n + 1) << 56);
        data_.store(d);
        return n;
    }
    
    void remove_at(int idx) noexcept {
        uint64_t d = data_.load();
        int n = static_cast<int>((d >> 56) & 0xFF);
        [[assume(n >= 0 && n < 8)]];
        [[assume(idx >= 0 && idx < n)]];
        for (int i = idx; i < n - 1; ++i) {
            unsigned char next = static_cast<unsigned char>((d >> ((i + 1) * 8)) & 0xFF);
            d &= ~(0xFFULL << (i * 8));
            d |= (static_cast<uint64_t>(next) << (i * 8));
        }
        d &= ~(0xFFULL << ((n - 1) * 8));
        d = (d & ~(0xFFULL << 56)) | (static_cast<uint64_t>(n - 1) << 56);
        data_.store(d);
    }
};

// =============================================================================
// BITMAP256 - 256-bit bitmap for FULL nodes
// =============================================================================

class bitmap256 {
    uint64_t bits_[4] = {};
public:
    constexpr bitmap256() noexcept = default;
    bool test(unsigned char c) const noexcept { return (bits_[c >> 6] & (1ULL << (c & 63))) != 0; }
    void set(unsigned char c) noexcept { bits_[c >> 6] |= (1ULL << (c & 63)); }
    void clear(unsigned char c) noexcept { bits_[c >> 6] &= ~(1ULL << (c & 63)); }
    uint64_t word(int w) const noexcept { return bits_[w]; }  // Access individual 64-bit word
    int count() const noexcept {
        return std::popcount(bits_[0]) + std::popcount(bits_[1]) +
               std::popcount(bits_[2]) + std::popcount(bits_[3]);
    }
    unsigned char first() const noexcept {
        for (int w = 0; w < 4; ++w)
            if (bits_[w]) return static_cast<unsigned char>((w << 6) | std::countr_zero(bits_[w]));
        return 0;
    }
    
    // Kernighan's method iteration - O(k) where k = popcount
    template <typename Fn>
    void for_each_set(Fn&& fn) const noexcept {
        for (int w = 0; w < 4; ++w) {
            uint64_t bits = bits_[w];
            while (bits) {
                unsigned char c = static_cast<unsigned char>((w << 6) | std::countr_zero(bits));
                fn(c);
                bits &= bits - 1;
            }
        }
    }
    
    template <bool THREADED>
    bool atomic_test(unsigned char c) const noexcept {
        if constexpr (THREADED) {
            uint64_t val = reinterpret_cast<const std::atomic<uint64_t>*>(&bits_[c >> 6])->load(std::memory_order_acquire);
            return (val & (1ULL << (c & 63))) != 0;
        } else {
            return test(c);
        }
    }
    
    template <bool THREADED>
    void atomic_set(unsigned char c) noexcept {
        if constexpr (THREADED)
            reinterpret_cast<std::atomic<uint64_t>*>(&bits_[c >> 6])->fetch_or(1ULL << (c & 63), std::memory_order_release);
        else set(c);
    }
    template <bool THREADED>
    void atomic_clear(unsigned char c) noexcept {
        if constexpr (THREADED)
            reinterpret_cast<std::atomic<uint64_t>*>(&bits_[c >> 6])->fetch_and(~(1ULL << (c & 63)), std::memory_order_release);
        else clear(c);
    }
};

// =============================================================================
// EMPTY_MUTEX - no-op mutex for non-threaded mode
// =============================================================================

struct empty_mutex {
    void lock() noexcept {}
    void unlock() noexcept {}
};

// =============================================================================
// INLINE_SKIP - compact skip storage for fixed-length keys
// =============================================================================

template <size_t MAX_LEN>
class inline_skip {
    // Layout: bytes 0..(MAX_LEN-1) = data, last byte = length
    // For MAX_LEN=8: 7 bytes data max + 1 byte length (supports 0-7 length)
    static_assert(MAX_LEN > 0 && MAX_LEN <= 16, "MAX_LEN must be 1-16");
    
    char data_[MAX_LEN] = {};
    
public:
    inline_skip() noexcept = default;
    
    inline_skip(std::string_view sv) noexcept { assign(sv); }
    
    void assign(std::string_view sv) noexcept {
        size_t n = sv.size() < MAX_LEN ? sv.size() : MAX_LEN - 1;
        std::memset(data_, 0, MAX_LEN);
        std::memcpy(data_, sv.data(), n);
        data_[MAX_LEN - 1] = static_cast<char>(n);
    }
    
    void assign(const char* s, size_t len) noexcept {
        assign(std::string_view(s, len));
    }
    
    inline_skip& operator=(std::string_view sv) noexcept {
        assign(sv);
        return *this;
    }
    
    size_t size() const noexcept { 
        return static_cast<unsigned char>(data_[MAX_LEN - 1]); 
    }
    
    bool empty() const noexcept { return size() == 0; }
    
    const char* data() const noexcept { return data_; }
    
    char operator[](size_t i) const noexcept { return data_[i]; }
    
    std::string_view view() const noexcept { return std::string_view(data_, size()); }
    
    operator std::string_view() const noexcept { return view(); }
    
    // For substr operations - returns a string_view (doesn't modify this)
    std::string_view substr(size_t pos, size_t len = std::string_view::npos) const noexcept {
        return view().substr(pos, len);
    }
    
    void clear() noexcept {
        std::memset(data_, 0, MAX_LEN);
    }
    
    // Append a character (used in collapse operations)
    void push_back(char c) noexcept {
        size_t n = size();
        if (n < MAX_LEN - 1) {
            data_[n] = c;
            data_[MAX_LEN - 1] = static_cast<char>(n + 1);
        }
    }
    
    // Append another skip's contents
    void append(std::string_view sv) noexcept {
        size_t n = size();
        size_t add = sv.size();
        if (n + add > MAX_LEN - 1) add = MAX_LEN - 1 - n;
        std::memcpy(data_ + n, sv.data(), add);
        data_[MAX_LEN - 1] = static_cast<char>(n + add);
    }
};

// Type selector for skip storage
template <size_t FIXED_KEY_LEN>
using skip_storage_t = std::conditional_t<
    FIXED_KEY_LEN == 0,
    std::string,
    inline_skip<FIXED_KEY_LEN>
>;

// =============================================================================
// SKIP MATCHING
// =============================================================================

// Reader version - check if key starts with skip and consume it if so
inline bool consume_prefix(std::string_view& key, std::string_view skip) noexcept {
    size_t sz = skip.size();
    if (sz > key.size() || std::memcmp(skip.data(), key.data(), sz) != 0) return false;
    key.remove_prefix(sz);
    return true;
}

// Insert version - returns mismatch position (needed for split operations)
inline size_t match_skip_impl(std::string_view skip, std::string_view key) noexcept {
    size_t min_len = skip.size() < key.size() ? skip.size() : key.size();
    if (std::memcmp(skip.data(), key.data(), min_len) == 0) return min_len;
    size_t i = 0;
    while (i < min_len && skip[i] == key[i]) ++i;
    return i;
}

}  // namespace gteitelbaum
