#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <type_traits>

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
static constexpr uint64_t VERSION_MASK  = 0x07FFFFFF'F8000000ULL;
static constexpr uint64_t SIZE_MASK     = 0x00000000'07FFFFFFULL;
static constexpr int VERSION_SHIFT      = 27;

// Pointer bit flags for concurrency
static constexpr uint64_t WRITE_BIT = 1ULL << 63;
static constexpr uint64_t READ_BIT  = 1ULL << 62;
static constexpr uint64_t PTR_MASK  = ~(WRITE_BIT | READ_BIT);  // masks off both control bits

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

// Header manipulation
KTRIE_FORCE_INLINE uint64_t make_header(uint64_t flags, uint32_t version, uint32_t size) noexcept {
    return (flags & FLAGS_MASK) | 
           ((static_cast<uint64_t>(version) << VERSION_SHIFT) & VERSION_MASK) |
           (static_cast<uint64_t>(size) & SIZE_MASK);
}

KTRIE_FORCE_INLINE uint64_t get_flags(uint64_t header) noexcept {
    return header & FLAGS_MASK;
}

KTRIE_FORCE_INLINE uint32_t get_version(uint64_t header) noexcept {
    return static_cast<uint32_t>((header & VERSION_MASK) >> VERSION_SHIFT);
}

KTRIE_FORCE_INLINE uint32_t get_size(uint64_t header) noexcept {
    return static_cast<uint32_t>(header & SIZE_MASK);
}

KTRIE_FORCE_INLINE uint64_t set_version(uint64_t header, uint32_t version) noexcept {
    return (header & ~VERSION_MASK) | ((static_cast<uint64_t>(version) << VERSION_SHIFT) & VERSION_MASK);
}

KTRIE_FORCE_INLINE uint64_t increment_version(uint64_t header) noexcept {
    uint32_t ver = get_version(header);
    return set_version(header, ver + 1);
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

}  // namespace gteitelbaum
