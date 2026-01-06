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
    atomic_storage() noexcept : value_{} {}
    explicit atomic_storage(T v) noexcept : value_(v) {}
    
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

// Header: [LEAF:1][TYPE:2][POISON:1][VERSION:60]
// TYPE: 00=SKIP (always leaf), 01=LIST, 10=FULL
static constexpr uint64_t FLAG_LEAF   = 1ULL << 63;
static constexpr uint64_t TYPE_SKIP   = 0ULL << 61;  // always leaf
static constexpr uint64_t TYPE_LIST   = 1ULL << 61;  // leaf or interior
static constexpr uint64_t TYPE_FULL   = 2ULL << 61;  // leaf or interior
static constexpr uint64_t TYPE_MASK   = 3ULL << 61;
static constexpr uint64_t FLAG_POISON = 1ULL << 60;
static constexpr uint64_t VERSION_MASK = (1ULL << 60) - 1;
static constexpr uint64_t FLAGS_MASK = FLAG_LEAF | TYPE_MASK | FLAG_POISON;

static constexpr int LIST_MAX = 7;

// Interior FULL node header with poison flag set - used for sentinel
static constexpr uint64_t SENTINEL_HEADER = TYPE_FULL | FLAG_POISON;

inline constexpr bool is_poisoned_header(uint64_t h) noexcept {
    return (h & FLAG_POISON) != 0;
}

inline constexpr uint64_t make_header(bool is_leaf, uint64_t type, uint64_t version = 0) noexcept {
    return (is_leaf ? FLAG_LEAF : 0) | type | (version & VERSION_MASK);
}
inline constexpr bool is_leaf(uint64_t h) noexcept { return (h & FLAG_LEAF) != 0; }
inline constexpr uint64_t get_type(uint64_t h) noexcept { return h & TYPE_MASK; }
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
// SMALL_LIST - packed list of up to 7 chars
// =============================================================================

class small_list {
    atomic_storage<uint64_t, true> data_;  // Always atomic for thread-safety
public:
    small_list() noexcept = default;
    
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
        for (int i = 0; i < n; ++i) {
            if (static_cast<unsigned char>((d >> (i * 8)) & 0xFF) == c) return i;
        }
        return -1;
    }
    
    int add(unsigned char c) noexcept {
        uint64_t d = data_.load();
        int n = static_cast<int>((d >> 56) & 0xFF);
        d = (d & ~(0xFFULL << 56)) | (static_cast<uint64_t>(c) << (n * 8)) |
            (static_cast<uint64_t>(n + 1) << 56);
        data_.store(d);
        return n;
    }
    void remove_at(int idx) noexcept {
        uint64_t d = data_.load();
        int n = static_cast<int>((d >> 56) & 0xFF);
        for (int i = idx; i < n - 1; ++i) {
            unsigned char next = static_cast<unsigned char>((d >> ((i + 1) * 8)) & 0xFF);
            d &= ~(0xFFULL << (i * 8));
            d |= (static_cast<uint64_t>(next) << (i * 8));
        }
        d = (d & ~(0xFFULL << ((n-1) * 8))) & ~(0xFFULL << 56);
        d |= (static_cast<uint64_t>(n - 1) << 56);
        data_.store(d);
    }
};

// =============================================================================
// BITMAP256 - 256-bit bitmap for FULL nodes
// =============================================================================

class bitmap256 {
    uint64_t bits_[4] = {};
public:
    bitmap256() noexcept = default;
    bool test(unsigned char c) const noexcept { return (bits_[c >> 6] & (1ULL << (c & 63))) != 0; }
    void set(unsigned char c) noexcept { bits_[c >> 6] |= (1ULL << (c & 63)); }
    void clear(unsigned char c) noexcept { bits_[c >> 6] &= ~(1ULL << (c & 63)); }
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
// SKIP MATCHING
// =============================================================================

inline size_t match_skip_impl(std::string_view skip, std::string_view key) noexcept {
    size_t min_len = skip.size() < key.size() ? skip.size() : key.size();
    
    if (min_len <= 8) {
        size_t i = 0;
        while (i < min_len && skip[i] == key[i]) ++i;
        return i;
    }
    
    if (std::memcmp(skip.data(), key.data(), min_len) == 0) {
        return min_len;
    }
    
    size_t i = 0;
    while (i < min_len && skip[i] == key[i]) ++i;
    return i;
}

}  // namespace gteitelbaum
