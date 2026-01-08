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

// Header: [LEAF:1][SKIP:1][BINARY:1][LIST:1][POP:1][POISON:1][VERSION:58]
// Type determination (mutually exclusive):
//   SKIP:   FLAG_SKIP set (always leaf, single key-value)
//   BINARY: FLAG_BINARY set (2 entries, leaf or interior)
//   LIST:   FLAG_LIST set (3-7 entries, leaf or interior)
//   POP:    FLAG_POP set (8-32 entries, leaf or interior)
//   FULL:   none of SKIP/BINARY/LIST/POP set (33+ entries, leaf or interior)
// Note: For interior with FIXED_LEN==0, entry count includes EOS if present
static constexpr uint64_t FLAG_LEAF   = 1ULL << 63;
static constexpr uint64_t FLAG_SKIP   = 1ULL << 62;  // always leaf, 1 entry
static constexpr uint64_t FLAG_BINARY = 1ULL << 61;  // leaf or interior, 2 entries
static constexpr uint64_t FLAG_LIST   = 1ULL << 60;  // leaf or interior, 3-7 entries
static constexpr uint64_t FLAG_POP    = 1ULL << 59;  // leaf or interior, 8-32 entries
static constexpr uint64_t FLAG_POISON = 1ULL << 58;
static constexpr uint64_t VERSION_MASK = (1ULL << 58) - 1;
static constexpr uint64_t FLAGS_MASK = FLAG_LEAF | FLAG_SKIP | FLAG_BINARY | FLAG_LIST | FLAG_POP | FLAG_POISON;
static constexpr uint64_t TYPE_FLAGS_MASK = FLAG_SKIP | FLAG_BINARY | FLAG_LIST | FLAG_POP;

static constexpr int BINARY_MAX = 2;
static constexpr int LIST_MAX = 7;
static constexpr int POP_MAX = 32;

// Interior FULL node header with poison flag set - used for retry sentinel
static constexpr uint64_t RETRY_SENTINEL_HEADER = FLAG_POISON;  // FULL (no type flags) + poison

inline constexpr bool is_poisoned_header(uint64_t h) noexcept {
    return (h & FLAG_POISON) != 0;
}

inline constexpr uint64_t make_header(bool is_leaf, uint64_t type_flag, uint64_t version = 0) noexcept {
    return (is_leaf ? FLAG_LEAF : 0) | type_flag | (version & VERSION_MASK);
}
inline constexpr bool is_leaf(uint64_t h) noexcept { return (h & FLAG_LEAF) != 0; }
inline constexpr uint64_t get_version(uint64_t h) noexcept { return h & VERSION_MASK; }

// Bump version preserving flags (including poison)
inline constexpr uint64_t bump_version(uint64_t h) noexcept {
    uint64_t flags = h & FLAGS_MASK;
    uint64_t ver = (h & VERSION_MASK) + 1;
    return flags | (ver & VERSION_MASK);
}

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
