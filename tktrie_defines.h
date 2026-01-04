#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

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

// Header: IS_LEAF (1 bit) | TYPE (2 bits)
// TYPE: 00=EOS, 01=SKIP, 10=LIST, 11=FULL
static constexpr uint64_t FLAG_LEAF = 1ULL << 63;
static constexpr uint64_t TYPE_MASK = 0x3ULL << 61;
static constexpr uint64_t TYPE_EOS  = 0x0ULL << 61;
static constexpr uint64_t TYPE_SKIP = 0x1ULL << 61;
static constexpr uint64_t TYPE_LIST = 0x2ULL << 61;
static constexpr uint64_t TYPE_FULL = 0x3ULL << 61;

static constexpr int LIST_MAX = 7;

KTRIE_FORCE_INLINE constexpr uint64_t make_header(bool is_leaf, uint64_t type) noexcept {
    return (is_leaf ? FLAG_LEAF : 0) | type;
}

KTRIE_FORCE_INLINE constexpr bool is_leaf_node(uint64_t h) noexcept { return (h & FLAG_LEAF) != 0; }
KTRIE_FORCE_INLINE constexpr uint64_t get_type(uint64_t h) noexcept { return h & TYPE_MASK; }
KTRIE_FORCE_INLINE constexpr bool is_eos_type(uint64_t h) noexcept { return get_type(h) == TYPE_EOS; }
KTRIE_FORCE_INLINE constexpr bool is_skip_type(uint64_t h) noexcept { return get_type(h) == TYPE_SKIP; }
KTRIE_FORCE_INLINE constexpr bool is_list_type(uint64_t h) noexcept { return get_type(h) == TYPE_LIST; }
KTRIE_FORCE_INLINE constexpr bool is_full_type(uint64_t h) noexcept { return get_type(h) == TYPE_FULL; }

// Byteswap utilities
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

// small_list: 7 chars + count packed in 64 bits, SWAR search
class small_list {
    uint64_t data_{0};
public:
    small_list() noexcept = default;
    explicit small_list(uint64_t v) noexcept : data_(v) {}

    KTRIE_FORCE_INLINE int count() const noexcept {
        return static_cast<int>((data_ >> 56) & 0xFF);
    }

    KTRIE_FORCE_INLINE unsigned char char_at(int i) const noexcept {
        return static_cast<unsigned char>((data_ >> (i * 8)) & 0xFF);
    }

    // SWAR parallel search - returns index or -1
    KTRIE_FORCE_INLINE int find(unsigned char c) const noexcept {
        constexpr uint64_t rep = 0x0101010101010100ULL;
        constexpr uint64_t low_bits = 0x7F7F7F7F7F7F7F7FULL;
        uint64_t target = rep * static_cast<uint64_t>(c);
        uint64_t diff = data_ ^ target;
        uint64_t zeros = ~((((diff & low_bits) + low_bits) | diff) | low_bits);
        if (zeros == 0) return -1;
        int pos = std::countl_zero(zeros) / 8;
        return (pos < count()) ? pos : -1;
    }

    int add(unsigned char c) noexcept {
        int n = count();
        KTRIE_DEBUG_ASSERT(n < LIST_MAX);
        data_ &= ~(0xFFULL << 56);
        data_ |= (static_cast<uint64_t>(c) << (n * 8));
        data_ |= (static_cast<uint64_t>(n + 1) << 56);
        return n;
    }

    void remove_at(int idx) noexcept {
        int n = count();
        KTRIE_DEBUG_ASSERT(idx < n);
        uint64_t mask_low = (1ULL << (idx * 8)) - 1;
        uint64_t mask_high = ~((1ULL << ((idx + 1) * 8)) - 1) & 0x00FFFFFFFFFFFFFFULL;
        uint64_t low_part = data_ & mask_low;
        uint64_t high_part = (data_ & mask_high) >> 8;
        data_ = low_part | high_part | (static_cast<uint64_t>(n - 1) << 56);
    }

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

    uint64_t raw() const noexcept { return data_; }
};

// bitmap256: 256-bit bitmap for FULL nodes
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

    template <bool THREADED>
    KTRIE_FORCE_INLINE void atomic_set(unsigned char c) noexcept {
        if constexpr (THREADED) {
            reinterpret_cast<std::atomic<uint64_t>*>(&bits_[c >> 6])
                ->fetch_or(1ULL << (c & 63), std::memory_order_release);
        } else {
            set(c);
        }
    }

    template <bool THREADED>
    KTRIE_FORCE_INLINE void atomic_clear(unsigned char c) noexcept {
        if constexpr (THREADED) {
            reinterpret_cast<std::atomic<uint64_t>*>(&bits_[c >> 6])
                ->fetch_and(~(1ULL << (c & 63)), std::memory_order_release);
        } else {
            clear(c);
        }
    }

    KTRIE_FORCE_INLINE int count() const noexcept {
        return std::popcount(bits_[0]) + std::popcount(bits_[1]) +
               std::popcount(bits_[2]) + std::popcount(bits_[3]);
    }

    unsigned char first_set() const noexcept {
        for (int w = 0; w < 4; ++w) {
            if (bits_[w]) {
                return static_cast<unsigned char>((w << 6) | std::countr_zero(bits_[w]));
            }
        }
        return 255;
    }

    unsigned char next_set(unsigned char after) const noexcept {
        int start_word = (after + 1) >> 6;
        int start_bit = (after + 1) & 63;

        if (start_word < 4) {
            uint64_t mask = ~((1ULL << start_bit) - 1);
            uint64_t masked = bits_[start_word] & mask;
            if (masked) {
                return static_cast<unsigned char>((start_word << 6) | std::countr_zero(masked));
            }
            for (int w = start_word + 1; w < 4; ++w) {
                if (bits_[w]) {
                    return static_cast<unsigned char>((w << 6) | std::countr_zero(bits_[w]));
                }
            }
        }
        return 255;
    }

    int index_of(unsigned char c) const noexcept {
        int word = c >> 6;
        int bit = c & 63;
        int idx = std::popcount(bits_[word] & ((1ULL << bit) - 1));
        for (int w = 0; w < word; ++w) idx += std::popcount(bits_[w]);
        return idx;
    }
};

// Empty mutex for non-threaded
struct empty_mutex {
    void lock() noexcept {}
    void unlock() noexcept {}
    bool try_lock() noexcept { return true; }
};

}  // namespace gteitelbaum
