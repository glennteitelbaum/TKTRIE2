#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>
#include <utility>

// Validation control - set via compiler define: -DKTRIE_VALIDATE=1
#ifndef KTRIE_VALIDATE
#define KTRIE_VALIDATE 0
#endif

// Force inline
#if defined(_MSC_VER)
#define KTRIE_FORCE_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define KTRIE_FORCE_INLINE __attribute__((always_inline)) inline
#else
#define KTRIE_FORCE_INLINE inline
#endif

// Debug assertions
#ifdef NDEBUG
#define KTRIE_DEBUG_ASSERT(cond) ((void)0)
#else
#include <cassert>
#define KTRIE_DEBUG_ASSERT(cond) assert(cond)
#endif

namespace gteitelbaum {

static constexpr bool k_validate = (KTRIE_VALIDATE != 0);

// Flag constants
static constexpr uint64_t FLAG_EOS      = 1ULL << 63;
static constexpr uint64_t FLAG_SKIP     = 1ULL << 62;
static constexpr uint64_t FLAG_SKIP_EOS = 1ULL << 61;
static constexpr uint64_t FLAG_LIST     = 1ULL << 60;
static constexpr uint64_t FLAG_POP      = 1ULL << 59;

static constexpr uint64_t FLAGS_MASK    = 0xF800000000000000ULL;
static constexpr uint64_t SIZE_MASK     = 0x07FFFFFFFFFFFFFFULL;  // 59 bits for size

// Pointer bit flags for concurrency
static constexpr uint64_t WRITE_BIT = 1ULL << 63;
static constexpr uint64_t READ_BIT  = 1ULL << 62;
static constexpr uint64_t PTR_MASK  = ~(WRITE_BIT | READ_BIT);  // masks off both control bits

// Switch helper for pure bool combinations
// Use: switch (mk_switch(a, b, c)) { case mk_switch(true, false, true): ... }
template <typename... Bools>
KTRIE_FORCE_INLINE constexpr uint8_t mk_switch(Bools... bs) noexcept {
    uint8_t result = 0;
    ((result = (result << 1) | uint8_t(bool(bs))), ...);
    return result;
}

// Switch helper for flag + bool combinations
// Uses Kernighan's bit iteration (low-to-high) for efficiency
// Use: switch (mk_flag_switch(flags, FLAG_EOS|FLAG_SKIP, is_list)) { 
//        case mk_flag_switch(FLAG_EOS, FLAG_EOS|FLAG_SKIP, true): ... }
template <typename... Bools>
KTRIE_FORCE_INLINE constexpr uint8_t mk_flag_switch(uint64_t flags, uint64_t mask, Bools... extra) noexcept {
    uint8_t result = 0;
    
    // Process flags using Kernighan's method (low-to-high)
    for (uint64_t m = mask; m; m &= m - 1) {
        uint64_t bit = m ^ (m & (m - 1));  // isolate lowest set bit (unsigned only)
        result = (result << 1) | uint8_t(bool(flags & bit));
    }
    
    // Append extra bools
    ((result = (result << 1) | uint8_t(bool(extra))), ...);
    return result;
}

// Byteswap - C++23 has std::byteswap, provide fallback for C++20
template <typename T>
constexpr T byteswap_impl(T value) noexcept {
    static_assert(std::is_integral_v<T>, "byteswap requires integral type");
    if constexpr (sizeof(T) == 1) {
        return value;
    } else if constexpr (sizeof(T) == 2) {
        return static_cast<T>(
            ((static_cast<uint16_t>(value) & 0x00FFu) << 8) |
            ((static_cast<uint16_t>(value) & 0xFF00u) >> 8)
        );
    } else if constexpr (sizeof(T) == 4) {
        uint32_t v = static_cast<uint32_t>(value);
        return static_cast<T>(
            ((v & 0x000000FFu) << 24) |
            ((v & 0x0000FF00u) << 8)  |
            ((v & 0x00FF0000u) >> 8)  |
            ((v & 0xFF000000u) >> 24)
        );
    } else if constexpr (sizeof(T) == 8) {
        uint64_t v = static_cast<uint64_t>(value);
        return static_cast<T>(
            ((v & 0x00000000000000FFull) << 56) |
            ((v & 0x000000000000FF00ull) << 40) |
            ((v & 0x0000000000FF0000ull) << 24) |
            ((v & 0x00000000FF000000ull) << 8)  |
            ((v & 0x000000FF00000000ull) >> 8)  |
            ((v & 0x0000FF0000000000ull) >> 24) |
            ((v & 0x00FF000000000000ull) >> 40) |
            ((v & 0xFF00000000000000ull) >> 56)
        );
    }
}

template <typename T>
constexpr T ktrie_byteswap(T value) noexcept {
#if __cpp_lib_byteswap >= 202110L
    return std::byteswap(value);
#else
    return byteswap_impl(value);
#endif
}

template <typename T>
constexpr T to_big_endian(T value) noexcept {
    if constexpr (std::endian::native == std::endian::big) {
        return value;
    } else {
        return ktrie_byteswap(value);
    }
}

template <typename T>
constexpr T from_big_endian(T value) noexcept {
    return to_big_endian(value);  // symmetric operation
}

// Char array utilities for SWAR operations
KTRIE_FORCE_INLINE std::array<char, 8> to_char_array(uint64_t v) noexcept {
    std::array<char, 8> arr;
    uint64_t be = to_big_endian(v);
    std::memcpy(arr.data(), &be, 8);
    return arr;
}

KTRIE_FORCE_INLINE uint64_t from_char_array(const std::array<char, 8>& arr) noexcept {
    uint64_t be;
    std::memcpy(&be, arr.data(), 8);
    return from_big_endian(be);
}

// Header manipulation (version removed - COW uses pointer identity)
KTRIE_FORCE_INLINE uint64_t make_header(uint64_t flags, uint32_t size) noexcept {
    return (flags & FLAGS_MASK) | (static_cast<uint64_t>(size) & SIZE_MASK);
}

KTRIE_FORCE_INLINE uint64_t get_flags(uint64_t header) noexcept {
    return header & FLAGS_MASK;
}

KTRIE_FORCE_INLINE uint32_t get_size(uint64_t header) noexcept {
    return static_cast<uint32_t>(header & SIZE_MASK);
}

// Empty mutex for non-threaded mode
struct empty_mutex {
    void lock() noexcept { }
    void unlock() noexcept { }
    bool try_lock() noexcept { return true; }
};

// Spin helper
KTRIE_FORCE_INLINE void cpu_pause() noexcept {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    #if defined(_MSC_VER)
        _mm_pause();
    #else
        __builtin_ia32_pause();
    #endif
#elif defined(__aarch64__) || defined(_M_ARM64)
    #if defined(_MSC_VER)
        __yield();
    #else
        asm volatile("yield");
    #endif
#else
    // Generic fallback
    std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
}

// Slot type selection
template <bool THREADED>
using slot_type_t = std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t>;

// Slot load/store helpers
template <bool THREADED>
KTRIE_FORCE_INLINE uint64_t load_slot(const slot_type_t<THREADED>* slot) noexcept {
    if constexpr (THREADED) {
        return slot->load(std::memory_order_acquire);
    } else {
        return *slot;
    }
}

template <bool THREADED>
KTRIE_FORCE_INLINE void store_slot(slot_type_t<THREADED>* slot, uint64_t value) noexcept {
    if constexpr (THREADED) {
        slot->store(value, std::memory_order_release);
    } else {
        *slot = value;
    }
}

template <bool THREADED>
KTRIE_FORCE_INLINE bool cas_slot(slot_type_t<THREADED>* slot, uint64_t& expected, uint64_t desired) noexcept {
    if constexpr (THREADED) {
        return slot->compare_exchange_weak(expected, desired, 
                                           std::memory_order_acq_rel,
                                           std::memory_order_acquire);
    } else {
        if (*slot == expected) {
            *slot = desired;
            return true;
        }
        expected = *slot;
        return false;
    }
}

template <bool THREADED>
KTRIE_FORCE_INLINE uint64_t fetch_or_slot(slot_type_t<THREADED>* slot, uint64_t bits) noexcept {
    if constexpr (THREADED) {
        return slot->fetch_or(bits, std::memory_order_acq_rel);
    } else {
        uint64_t old = *slot;
        *slot |= bits;
        return old;
    }
}

template <bool THREADED>
KTRIE_FORCE_INLINE uint64_t fetch_and_slot(slot_type_t<THREADED>* slot, uint64_t bits) noexcept {
    if constexpr (THREADED) {
        return slot->fetch_and(bits, std::memory_order_acq_rel);
    } else {
        uint64_t old = *slot;
        *slot &= bits;
        return old;
    }
}

// Calculate number of uint64_t words needed to store n bytes
KTRIE_FORCE_INLINE constexpr size_t bytes_to_words(size_t bytes) noexcept {
    return (bytes + 7) / 8;
}

// =============================================================================
// small_list - Compact sorted character list for small branch points (1-7 children)
// =============================================================================

/**
 * Compact sorted character list for small branch points (1-7 children)
 * 
 * Memory Layout (64 bits, big-endian):
 * ┌────────────────────────────────────────────────────────────┬────────┐
 * │              Sorted characters (up to 7 bytes)             │ count  │
 * └────────────────────────────────────────────────────────────┴────────┘
 *   bytes 0-6 (characters in sorted order)                       byte 7
 */
class small_list {
    uint64_t n_;

public:
    static constexpr int max_count = 7;

    small_list() noexcept : n_{0} {}
    
    explicit small_list(uint64_t x) noexcept : n_{x} {}
    
    small_list(unsigned char c1, unsigned char c2) noexcept : n_{0} {
        if (c1 > c2) std::swap(c1, c2);
        auto arr = to_char_array(0ULL);
        arr[0] = static_cast<char>(c1);
        arr[1] = static_cast<char>(c2);
        arr[7] = 2;
        n_ = from_char_array(arr);
    }

    KTRIE_FORCE_INLINE int count() const noexcept {
        return static_cast<int>(n_ & 0xFF);
    }

    KTRIE_FORCE_INLINE uint8_t char_at(int pos) const noexcept {
        KTRIE_DEBUG_ASSERT(pos >= 0 && pos < max_count);
        auto arr = to_char_array(n_);
        return static_cast<uint8_t>(arr[pos]);
    }

    KTRIE_FORCE_INLINE void set_char_at(int pos, unsigned char c) noexcept {
        KTRIE_DEBUG_ASSERT(pos >= 0 && pos < max_count);
        auto arr = to_char_array(n_);
        arr[pos] = static_cast<char>(c);
        n_ = from_char_array(arr);
    }

    KTRIE_FORCE_INLINE void set_count(int cnt) noexcept {
        KTRIE_DEBUG_ASSERT(cnt >= 0 && cnt <= max_count);
        n_ = (n_ & ~0xFFULL) | static_cast<uint64_t>(cnt);
    }

    /**
     * Find 1-based offset of a character (0 if not found)
     * Uses SWAR zero-byte detection
     */
    KTRIE_FORCE_INLINE int offset(unsigned char c) const noexcept {
        constexpr uint64_t rep = 0x01'01'01'01'01'01'01'00ULL;  // exclude count byte
        constexpr uint64_t low_bits = 0x7F'7F'7F'7F'7F'7F'7F'7FULL;
        
        uint64_t x = static_cast<uint64_t>(c);
        uint64_t diff = n_ ^ (rep * x);
        
        // Zero-byte detection: produces 0x80 where byte was 0x00
        uint64_t zeros = ~((((diff & low_bits) + low_bits) | diff) | low_bits);
        
        int pos = std::countl_zero(zeros) / 8;
        return (pos + 1 <= count()) ? pos + 1 : 0;
    }

    /**
     * Find insertion position for a character (0-based index where it should go)
     * Uses SWAR unsigned byte comparison
     */
    KTRIE_FORCE_INLINE int insert_pos(unsigned char c) const noexcept {
        int len = count();
        if (len == 0) return 0;
        
        constexpr uint64_t h = 0x80'80'80'80'80'80'80'80ULL;
        constexpr uint64_t m = 0x7F'7F'7F'7F'7F'7F'7F'7FULL;
        constexpr uint64_t REP = 0x01'01'01'01'01'01'01'01ULL;
        
        // Create mask for valid positions (top 'len' bytes in big-endian)
        uint64_t valid_mask = ~0ULL << (8 * (8 - len));
        
        uint64_t chars = n_ & valid_mask;
        uint64_t rep_x = (REP * static_cast<uint64_t>(c)) & valid_mask;
        
        // SWAR unsigned comparison: chars[i] < c for each byte
        uint64_t diff_high = (chars ^ rep_x) & h;
        uint64_t B_high_wins = rep_x & diff_high;
        uint64_t same_high = ~diff_high & h;
        uint64_t low_chars = chars & m;
        uint64_t low_x = rep_x & m;
        uint64_t low_cmp = ~((low_chars | h) - low_x) & h;
        
        uint64_t lt = (B_high_wins | (same_high & low_cmp)) & valid_mask;
        
        return std::popcount(lt);
    }

    /**
     * Insert character in sorted order, returns insertion position
     * Caller must ensure len < max_count
     */
    int insert(int len, unsigned char c) noexcept {
        KTRIE_DEBUG_ASSERT(len >= 0 && len < max_count);
        
        if (len == 0) {
            auto arr = to_char_array(0ULL);
            arr[0] = static_cast<char>(c);
            arr[7] = 1;
            n_ = from_char_array(arr);
            return 0;
        }
        
        int pos = insert_pos(c);
        
        // Shift bytes to make room using array manipulation
        auto arr = to_char_array(n_);
        for (int i = len; i > pos; --i) {
            arr[i] = arr[i - 1];
        }
        arr[pos] = static_cast<char>(c);
        arr[7] = static_cast<char>(len + 1);
        n_ = from_char_array(arr);
        
        return pos;
    }

    /**
     * Remove character at position, shifts remaining down
     */
    void remove_at(int pos) noexcept {
        int len = count();
        KTRIE_DEBUG_ASSERT(pos >= 0 && pos < len);
        
        auto arr = to_char_array(n_);
        for (int i = pos; i < len - 1; ++i) {
            arr[i] = arr[i + 1];
        }
        arr[len - 1] = 0;
        arr[7] = static_cast<char>(len - 1);
        n_ = from_char_array(arr);
    }

    std::string to_string() const {
        auto arr = to_char_array(n_);
        std::string r;
        r.reserve(count());
        for (int i = 0; i < count(); ++i) {
            r += arr[i];
        }
        return r;
    }

    KTRIE_FORCE_INLINE uint64_t to_u64() const noexcept { return n_; }
    
    KTRIE_FORCE_INLINE static small_list from_u64(uint64_t v) noexcept { 
        return small_list(v); 
    }
};

// =============================================================================
// popcount_bitmap - 256-bit bitmap for large branch points (8+ children)
// =============================================================================

/**
 * 256-bit bitmap for large branch points (8+ children)
 * Uses popcount to find child index
 */
class popcount_bitmap {
    uint64_t bits_[4]{};

public:
    popcount_bitmap() noexcept = default;
    
    explicit popcount_bitmap(const std::array<uint64_t, 4>& arr) noexcept {
        bits_[0] = arr[0];
        bits_[1] = arr[1];
        bits_[2] = arr[2];
        bits_[3] = arr[3];
    }

    /**
     * Check if character exists and return its index
     * @param c Character to find
     * @param idx Output: index in child array
     * @return true if found
     */
    KTRIE_FORCE_INLINE bool find(unsigned char c, int* idx) const noexcept {
        int word = c >> 6;
        int bit = c & 63;
        uint64_t mask = 1ULL << bit;
        
        if (!(bits_[word] & mask)) return false;
        
        *idx = std::popcount(bits_[word] & (mask - 1));
        for (int w = 0; w < word; ++w) {
            *idx += std::popcount(bits_[w]);
        }
        return true;
    }

    /**
     * Check if character exists
     */
    KTRIE_FORCE_INLINE bool contains(unsigned char c) const noexcept {
        int word = c >> 6;
        int bit = c & 63;
        return (bits_[word] & (1ULL << bit)) != 0;
    }

    /**
     * Set bit for character and return its index
     * @param c Character to add
     * @return Index where child should be inserted
     */
    KTRIE_FORCE_INLINE int set(unsigned char c) noexcept {
        int word = c >> 6;
        int bit = c & 63;
        uint64_t mask = 1ULL << bit;
        
        int idx = std::popcount(bits_[word] & (mask - 1));
        for (int w = 0; w < word; ++w) {
            idx += std::popcount(bits_[w]);
        }
        
        bits_[word] |= mask;
        return idx;
    }

    /**
     * Clear bit for character and return its former index
     * @param c Character to remove
     * @return Index where child was (or -1 if not found)
     */
    KTRIE_FORCE_INLINE int clear(unsigned char c) noexcept {
        int word = c >> 6;
        int bit = c & 63;
        uint64_t mask = 1ULL << bit;
        
        if (!(bits_[word] & mask)) return -1;
        
        int idx = std::popcount(bits_[word] & (mask - 1));
        for (int w = 0; w < word; ++w) {
            idx += std::popcount(bits_[w]);
        }
        
        bits_[word] &= ~mask;
        return idx;
    }

    /**
     * Get index for character (assumes it exists)
     */
    KTRIE_FORCE_INLINE int index_of(unsigned char c) const noexcept {
        int word = c >> 6;
        int bit = c & 63;
        uint64_t mask = 1ULL << bit;
        
        int idx = std::popcount(bits_[word] & (mask - 1));
        for (int w = 0; w < word; ++w) {
            idx += std::popcount(bits_[w]);
        }
        return idx;
    }

    /**
     * Total number of set bits (number of children)
     */
    KTRIE_FORCE_INLINE int count() const noexcept {
        return std::popcount(bits_[0]) + std::popcount(bits_[1]) + 
               std::popcount(bits_[2]) + std::popcount(bits_[3]);
    }

    /**
     * Check if bitmap is empty
     */
    KTRIE_FORCE_INLINE bool empty() const noexcept {
        return (bits_[0] | bits_[1] | bits_[2] | bits_[3]) == 0;
    }

    /**
     * Get the nth set character (0-based)
     */
    unsigned char nth_char(int n) const noexcept {
        KTRIE_DEBUG_ASSERT(n >= 0 && n < count());
        
        int remaining = n;
        for (int word = 0; word < 4; ++word) {
            int word_count = std::popcount(bits_[word]);
            if (remaining < word_count) {
                // Find the nth set bit in this word
                uint64_t b = bits_[word];
                for (int bit = 0; bit < 64; ++bit) {
                    if (b & (1ULL << bit)) {
                        if (remaining == 0) {
                            return static_cast<unsigned char>((word << 6) | bit);
                        }
                        --remaining;
                    }
                }
            }
            remaining -= word_count;
        }
        return 0;  // Should not reach here
    }

    std::array<uint64_t, 4> to_array() const noexcept {
        return {bits_[0], bits_[1], bits_[2], bits_[3]};
    }
    
    static popcount_bitmap from_array(const std::array<uint64_t, 4>& arr) noexcept {
        return popcount_bitmap(arr);
    }

    uint64_t word(int i) const noexcept {
        KTRIE_DEBUG_ASSERT(i >= 0 && i < 4);
        return bits_[i];
    }

    void set_word(int i, uint64_t v) noexcept {
        KTRIE_DEBUG_ASSERT(i >= 0 && i < 4);
        bits_[i] = v;
    }
};

}  // namespace gteitelbaum
