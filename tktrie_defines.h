#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>
#include <utility>

#ifndef KTRIE_VALIDATE
#define KTRIE_VALIDATE 0
#endif

#if defined(_MSC_VER)
#define KTRIE_FORCE_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
#define KTRIE_FORCE_INLINE __attribute__((always_inline)) inline
#else
#define KTRIE_FORCE_INLINE inline
#endif

#ifdef NDEBUG
#define KTRIE_DEBUG_ASSERT(cond) ((void)0)
#else
#include <cassert>
#define KTRIE_DEBUG_ASSERT(cond) assert(cond)
#endif

namespace gteitelbaum {

static constexpr bool k_validate = (KTRIE_VALIDATE != 0);

// Node type constants (2 bits)
static constexpr uint64_t NODE_TYPE_MASK = 0x03ULL;
static constexpr uint64_t NODE_EOS  = 0x00ULL;  // Just EOS pointer
static constexpr uint64_t NODE_SKIP = 0x01ULL;  // EOS + skip + skip_eos
static constexpr uint64_t NODE_LIST = 0x02ULL;  // EOS + skip + skip_eos + ≤7 children
static constexpr uint64_t NODE_FULL = 0x03ULL;  // EOS + skip + skip_eos + 256 children

// Version starts at bit 2
static constexpr uint64_t VERSION_SHIFT = 2;
static constexpr uint64_t VERSION_MASK = ~NODE_TYPE_MASK;

static constexpr int LIST_MAX = 7;

KTRIE_FORCE_INLINE constexpr uint64_t get_node_type(uint64_t header) noexcept {
    return header & NODE_TYPE_MASK;
}

KTRIE_FORCE_INLINE constexpr uint64_t get_version(uint64_t header) noexcept {
    return header >> VERSION_SHIFT;
}

KTRIE_FORCE_INLINE constexpr uint64_t make_header(uint64_t type, uint64_t version = 0) noexcept {
    return (version << VERSION_SHIFT) | (type & NODE_TYPE_MASK);
}

KTRIE_FORCE_INLINE constexpr bool is_eos_node(uint64_t header) noexcept {
    return get_node_type(header) == NODE_EOS;
}

KTRIE_FORCE_INLINE constexpr bool is_skip_node(uint64_t header) noexcept {
    return get_node_type(header) == NODE_SKIP;
}

KTRIE_FORCE_INLINE constexpr bool is_list_node(uint64_t header) noexcept {
    return get_node_type(header) == NODE_LIST;
}

KTRIE_FORCE_INLINE constexpr bool is_full_node(uint64_t header) noexcept {
    return get_node_type(header) == NODE_FULL;
}

KTRIE_FORCE_INLINE constexpr bool has_skip(uint64_t header) noexcept {
    return get_node_type(header) != NODE_EOS;
}

KTRIE_FORCE_INLINE constexpr bool has_children(uint64_t header) noexcept {
    uint64_t t = get_node_type(header);
    return t == NODE_LIST || t == NODE_FULL;
}

struct empty_mutex {
    void lock() noexcept {}
    void unlock() noexcept {}
    bool try_lock() noexcept { return true; }
};

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
    std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
}

// Byteswap utilities for endian-independent key encoding
template <typename T>
constexpr T byteswap_impl(T value) noexcept {
    static_assert(std::is_integral_v<T>);
    if constexpr (sizeof(T) == 1) return value;
    else if constexpr (sizeof(T) == 2)
        return static_cast<T>(((static_cast<uint16_t>(value) & 0x00FFu) << 8) |
                              ((static_cast<uint16_t>(value) & 0xFF00u) >> 8));
    else if constexpr (sizeof(T) == 4) {
        uint32_t v = static_cast<uint32_t>(value);
        return static_cast<T>(((v & 0x000000FFu) << 24) | ((v & 0x0000FF00u) << 8) |
                              ((v & 0x00FF0000u) >> 8)  | ((v & 0xFF000000u) >> 24));
    } else if constexpr (sizeof(T) == 8) {
        uint64_t v = static_cast<uint64_t>(value);
        return static_cast<T>(
            ((v & 0x00000000000000FFull) << 56) | ((v & 0x000000000000FF00ull) << 40) |
            ((v & 0x0000000000FF0000ull) << 24) | ((v & 0x00000000FF000000ull) << 8)  |
            ((v & 0x000000FF00000000ull) >> 8)  | ((v & 0x0000FF0000000000ull) >> 24) |
            ((v & 0x00FF000000000000ull) >> 40) | ((v & 0xFF00000000000000ull) >> 56));
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
    if constexpr (std::endian::native == std::endian::big) return value;
    else return ktrie_byteswap(value);
}

template <typename T>
constexpr T from_big_endian(T value) noexcept { return to_big_endian(value); }

// 256-bit bitmap for FULL node validity
class bitmap256 {
    uint64_t bits_[4]{};
public:
    bitmap256() noexcept = default;

    KTRIE_FORCE_INLINE bool test(unsigned char c) const noexcept {
        return (bits_[c >> 6] & (1ULL << (c & 63))) != 0;
    }

    KTRIE_FORCE_INLINE void set(unsigned char c) noexcept {
        bits_[c >> 6] |= (1ULL << (c & 63));
    }

    KTRIE_FORCE_INLINE void clear(unsigned char c) noexcept {
        bits_[c >> 6] &= ~(1ULL << (c & 63));
    }

    KTRIE_FORCE_INLINE int count() const noexcept {
        return std::popcount(bits_[0]) + std::popcount(bits_[1]) + 
               std::popcount(bits_[2]) + std::popcount(bits_[3]);
    }

    KTRIE_FORCE_INLINE bool empty() const noexcept {
        return (bits_[0] | bits_[1] | bits_[2] | bits_[3]) == 0;
    }

    // Atomic set for THREADED mode
    template <bool THREADED>
    void atomic_set(unsigned char c) noexcept {
        if constexpr (THREADED) {
            auto* atomic_word = reinterpret_cast<std::atomic<uint64_t>*>(&bits_[c >> 6]);
            atomic_word->fetch_or(1ULL << (c & 63), std::memory_order_release);
        } else {
            set(c);
        }
    }

    // Atomic clear for THREADED mode
    template <bool THREADED>
    void atomic_clear(unsigned char c) noexcept {
        if constexpr (THREADED) {
            auto* atomic_word = reinterpret_cast<std::atomic<uint64_t>*>(&bits_[c >> 6]);
            atomic_word->fetch_and(~(1ULL << (c & 63)), std::memory_order_release);
        } else {
            clear(c);
        }
    }

    // Find first set bit, returns 256 if none
    unsigned char first_set() const noexcept {
        for (int w = 0; w < 4; ++w) {
            if (bits_[w] != 0) {
                return static_cast<unsigned char>((w << 6) | std::countr_zero(bits_[w]));
            }
        }
        return 255;  // None found (use 255 as sentinel)
    }

    // Find next set bit after c, returns 256 if none
    unsigned char next_set(unsigned char c) const noexcept {
        int start_word = (c + 1) >> 6;
        int start_bit = (c + 1) & 63;
        
        // Check rest of current word
        if (start_word < 4) {
            uint64_t mask = bits_[start_word] & (~0ULL << start_bit);
            if (mask != 0) {
                return static_cast<unsigned char>((start_word << 6) | std::countr_zero(mask));
            }
        }
        
        // Check subsequent words
        for (int w = start_word + 1; w < 4; ++w) {
            if (bits_[w] != 0) {
                return static_cast<unsigned char>((w << 6) | std::countr_zero(bits_[w]));
            }
        }
        return 255;
    }
};

// Small list: up to 7 chars + count packed in 64 bits
class small_list {
    uint64_t data_{0};  // [char0..char6][count] - big-endian layout
public:
    small_list() noexcept = default;

    KTRIE_FORCE_INLINE int count() const noexcept {
        return static_cast<int>(data_ & 0xFF);
    }

    KTRIE_FORCE_INLINE unsigned char char_at(int idx) const noexcept {
        KTRIE_DEBUG_ASSERT(idx < count());
        return static_cast<unsigned char>((data_ >> (56 - idx * 8)) & 0xFF);
    }

    KTRIE_FORCE_INLINE int find(unsigned char c) const noexcept {
        int n = count();
        for (int i = 0; i < n; ++i) {
            if (char_at(i) == c) return i;
        }
        return -1;
    }

    KTRIE_FORCE_INLINE bool contains(unsigned char c) const noexcept {
        return find(c) >= 0;
    }

    // Add char, returns index. Caller must ensure count < 7
    int add(unsigned char c) noexcept {
        int n = count();
        KTRIE_DEBUG_ASSERT(n < LIST_MAX);
        data_ &= ~0xFFULL;  // Clear count
        data_ |= (static_cast<uint64_t>(c) << (56 - n * 8));
        data_ |= static_cast<uint64_t>(n + 1);
        return n;
    }

    // Remove char at index, shifts remaining. Returns new count
    int remove_at(int idx) noexcept {
        int n = count();
        KTRIE_DEBUG_ASSERT(idx < n);
        // Shift chars after idx
        for (int i = idx; i < n - 1; ++i) {
            unsigned char next = char_at(i + 1);
            data_ &= ~(0xFFULL << (56 - i * 8));
            data_ |= (static_cast<uint64_t>(next) << (56 - i * 8));
        }
        // Clear last slot and decrement count
        data_ &= ~(0xFFULL << (56 - (n - 1) * 8));
        data_ &= ~0xFFULL;
        data_ |= static_cast<uint64_t>(n - 1);
        return n - 1;
    }

    // Get first char (for iteration)
    unsigned char first() const noexcept {
        if (count() == 0) return 255;
        return char_at(0);
    }

    // Get smallest char (sorted first)
    unsigned char smallest() const noexcept {
        int n = count();
        if (n == 0) return 255;
        unsigned char min_c = char_at(0);
        for (int i = 1; i < n; ++i) {
            unsigned char c = char_at(i);
            if (c < min_c) min_c = c;
        }
        return min_c;
    }
};

}  // namespace gteitelbaum
