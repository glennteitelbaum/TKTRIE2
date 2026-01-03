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
#include <stdlib.h>  // For _byteswap_* on MSVC
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

// Flag constants
// LIST|POP together (without FULL) = HAS_EOS for LEAF nodes
static constexpr uint64_t FLAG_SKIP = 1ULL << 63;
static constexpr uint64_t FLAG_LIST = 1ULL << 62;
static constexpr uint64_t FLAG_POP  = 1ULL << 61;
static constexpr uint64_t FLAG_FULL = 1ULL << 60;
static constexpr uint64_t FLAG_LEAF = 1ULL << 59;

static constexpr uint64_t FLAGS_MASK = 0xF800000000000000ULL;
static constexpr uint64_t SIZE_MASK  = 0x07FFFFFFFFFFFFFFULL;

static constexpr int FULL_THRESHOLD = 176;
static constexpr int LIST_MAX = 7;

template <typename T>
static constexpr bool can_embed_leaf_v = 
    sizeof(T) <= sizeof(uint64_t) && std::is_trivially_copyable_v<T>;

// Derived flag checks
KTRIE_FORCE_INLINE constexpr bool flags_has_list(uint64_t f) noexcept {
    return (f & FLAG_LIST) && !(f & FLAG_POP);
}

KTRIE_FORCE_INLINE constexpr bool flags_has_pop(uint64_t f) noexcept {
    return (f & FLAG_POP) && !(f & FLAG_LIST);
}

KTRIE_FORCE_INLINE constexpr bool flags_has_full(uint64_t f) noexcept {
    return (f & FLAG_FULL) != 0;
}

KTRIE_FORCE_INLINE constexpr bool flags_has_skip(uint64_t f) noexcept {
    return (f & FLAG_SKIP) != 0;
}

KTRIE_FORCE_INLINE constexpr bool flags_has_leaf(uint64_t f) noexcept {
    return (f & FLAG_LEAF) != 0;
}

// For LEAF: LIST|POP without FULL = terminal (EOS)
KTRIE_FORCE_INLINE constexpr bool flags_leaf_has_eos(uint64_t f) noexcept {
    return (f & (FLAG_LIST | FLAG_POP)) == (FLAG_LIST | FLAG_POP) && !(f & FLAG_FULL);
}

// For LEAF: has children = LIST xor POP xor FULL
KTRIE_FORCE_INLINE constexpr bool flags_leaf_has_children(uint64_t f) noexcept {
    return flags_has_list(f) || flags_has_pop(f) || flags_has_full(f);
}

template <typename... Bools>
KTRIE_FORCE_INLINE constexpr uint8_t mk_switch(Bools... bs) noexcept {
    uint8_t result = 0;
    ((result = (result << 1) | uint8_t(bool(bs))), ...);
    return result;
}

template <typename T>
constexpr T ktrie_byteswap(T value) noexcept {
    static_assert(std::is_integral_v<T>);
    if constexpr (sizeof(T) == 1) {
        return value;
    }
#if __cpp_lib_byteswap >= 202110L
    else {
        return std::byteswap(value);
    }
#elif defined(_MSC_VER)
    // MSVC intrinsics (not constexpr, but fast at runtime)
    else if constexpr (sizeof(T) == 2) {
        return static_cast<T>(_byteswap_ushort(static_cast<uint16_t>(value)));
    } else if constexpr (sizeof(T) == 4) {
        return static_cast<T>(_byteswap_ulong(static_cast<uint32_t>(value)));
    } else if constexpr (sizeof(T) == 8) {
        return static_cast<T>(_byteswap_uint64(static_cast<uint64_t>(value)));
    }
#elif defined(__GNUC__) || defined(__clang__)
    // GCC/Clang builtins (constexpr-friendly)
    else if constexpr (sizeof(T) == 2) {
        return static_cast<T>(__builtin_bswap16(static_cast<uint16_t>(value)));
    } else if constexpr (sizeof(T) == 4) {
        return static_cast<T>(__builtin_bswap32(static_cast<uint32_t>(value)));
    } else if constexpr (sizeof(T) == 8) {
        return static_cast<T>(__builtin_bswap64(static_cast<uint64_t>(value)));
    }
#else
    // Fallback manual implementation
    else if constexpr (sizeof(T) == 2) {
        return static_cast<T>(((static_cast<uint16_t>(value) & 0x00FFu) << 8) |
                              ((static_cast<uint16_t>(value) & 0xFF00u) >> 8));
    } else if constexpr (sizeof(T) == 4) {
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
#endif
}

template <typename T>
constexpr T to_big_endian(T value) noexcept {
    if constexpr (std::endian::native == std::endian::big) return value;
    else return ktrie_byteswap(value);
}

template <typename T>
constexpr T from_big_endian(T value) noexcept { return to_big_endian(value); }

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

KTRIE_FORCE_INLINE uint64_t make_header(uint64_t flags, uint32_t size) noexcept {
    return (flags & FLAGS_MASK) | (static_cast<uint64_t>(size) & SIZE_MASK);
}

KTRIE_FORCE_INLINE uint64_t get_flags(uint64_t header) noexcept { return header & FLAGS_MASK; }
KTRIE_FORCE_INLINE uint32_t get_size(uint64_t header) noexcept { return static_cast<uint32_t>(header & SIZE_MASK); }

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

template <bool THREADED>
using slot_type_t = std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t>;

template <bool THREADED>
KTRIE_FORCE_INLINE uint64_t load_slot(const slot_type_t<THREADED>* slot) noexcept {
    if constexpr (THREADED) return slot->load(std::memory_order_acquire);
    else return *slot;
}

template <bool THREADED>
KTRIE_FORCE_INLINE void store_slot(slot_type_t<THREADED>* slot, uint64_t value) noexcept {
    if constexpr (THREADED) slot->store(value, std::memory_order_release);
    else *slot = value;
}

template <bool THREADED>
KTRIE_FORCE_INLINE bool cas_slot(slot_type_t<THREADED>* slot, uint64_t& expected, uint64_t desired) noexcept {
    if constexpr (THREADED)
        return slot->compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire);
    else {
        if (*slot == expected) { *slot = desired; return true; }
        expected = *slot;
        return false;
    }
}

KTRIE_FORCE_INLINE constexpr size_t bytes_to_words(size_t bytes) noexcept { return (bytes + 7) / 8; }

class small_list {
    uint64_t n_;
public:
    static constexpr int max_count = 7;
    small_list() noexcept : n_{0} {}
    explicit small_list(uint64_t x) noexcept : n_{x} {}
    small_list(unsigned char c1, unsigned char c2) noexcept : n_{0} {
        auto arr = to_char_array(0ULL);
        arr[0] = static_cast<char>(c1);
        arr[1] = static_cast<char>(c2);
        arr[7] = 2;
        n_ = from_char_array(arr);
    }

    KTRIE_FORCE_INLINE int count() const noexcept { return static_cast<int>(n_ & 0xFF); }
    KTRIE_FORCE_INLINE uint8_t char_at(int pos) const noexcept {
        auto arr = to_char_array(n_);
        return static_cast<uint8_t>(arr[pos]);
    }

    KTRIE_FORCE_INLINE int offset(unsigned char c) const noexcept {
        constexpr uint64_t rep = 0x01'01'01'01'01'01'01'00ULL;
        constexpr uint64_t low_bits = 0x7F'7F'7F'7F'7F'7F'7F'7FULL;
        uint64_t diff = n_ ^ (rep * static_cast<uint64_t>(c));
        uint64_t zeros = ~((((diff & low_bits) + low_bits) | diff) | low_bits);
        int pos = std::countl_zero(zeros) / 8;
        return (pos + 1 <= count()) ? pos + 1 : 0;
    }

    int add(unsigned char c) noexcept {
        int len = count();
        auto arr = to_char_array(n_);
        arr[len] = static_cast<char>(c);
        arr[7] = static_cast<char>(len + 1);
        n_ = from_char_array(arr);
        return len;
    }

    std::pair<std::array<uint8_t, max_count>, int> sorted_chars() const noexcept {
        std::array<uint8_t, max_count> chars{};
        int len = count();
        auto arr = to_char_array(n_);
        for (int i = 0; i < len; ++i) chars[i] = static_cast<uint8_t>(arr[i]);
        for (int i = 1; i < len; ++i) {
            uint8_t key = chars[i];
            int j = i - 1;
            while (j >= 0 && chars[j] > key) { chars[j + 1] = chars[j]; --j; }
            chars[j + 1] = key;
        }
        return {chars, len};
    }

    KTRIE_FORCE_INLINE uint64_t to_u64() const noexcept { return n_; }
    KTRIE_FORCE_INLINE static small_list from_u64(uint64_t v) noexcept { return small_list(v); }
};

class popcount_bitmap {
    uint64_t bits_[4]{};
public:
    popcount_bitmap() noexcept = default;
    explicit popcount_bitmap(const std::array<uint64_t, 4>& arr) noexcept {
        bits_[0] = arr[0]; bits_[1] = arr[1]; bits_[2] = arr[2]; bits_[3] = arr[3];
    }

    KTRIE_FORCE_INLINE bool find(unsigned char c, int* idx) const noexcept {
        int word = c >> 6, bit = c & 63;
        uint64_t mask = 1ULL << bit;
        if (!(bits_[word] & mask)) return false;
        *idx = std::popcount(bits_[word] & (mask - 1));
        for (int w = 0; w < word; ++w) *idx += std::popcount(bits_[w]);
        return true;
    }

    KTRIE_FORCE_INLINE bool contains(unsigned char c) const noexcept {
        return (bits_[c >> 6] & (1ULL << (c & 63))) != 0;
    }

    KTRIE_FORCE_INLINE int set(unsigned char c) noexcept {
        int word = c >> 6, bit = c & 63;
        uint64_t mask = 1ULL << bit;
        int idx = std::popcount(bits_[word] & (mask - 1));
        for (int w = 0; w < word; ++w) idx += std::popcount(bits_[w]);
        bits_[word] |= mask;
        return idx;
    }

    KTRIE_FORCE_INLINE int clear(unsigned char c) noexcept {
        int word = c >> 6, bit = c & 63;
        uint64_t mask = 1ULL << bit;
        if (!(bits_[word] & mask)) return -1;
        int idx = std::popcount(bits_[word] & (mask - 1));
        for (int w = 0; w < word; ++w) idx += std::popcount(bits_[w]);
        bits_[word] &= ~mask;
        return idx;
    }

    KTRIE_FORCE_INLINE int index_of(unsigned char c) const noexcept {
        int word = c >> 6, bit = c & 63;
        int idx = std::popcount(bits_[word] & ((1ULL << bit) - 1));
        for (int w = 0; w < word; ++w) idx += std::popcount(bits_[w]);
        return idx;
    }

    KTRIE_FORCE_INLINE int count() const noexcept {
        return std::popcount(bits_[0]) + std::popcount(bits_[1]) + 
               std::popcount(bits_[2]) + std::popcount(bits_[3]);
    }

    KTRIE_FORCE_INLINE bool empty() const noexcept {
        return (bits_[0] | bits_[1] | bits_[2] | bits_[3]) == 0;
    }

    unsigned char nth_char(int n) const noexcept {
        int remaining = n;
        for (int word = 0; word < 4; ++word) {
            int wc = std::popcount(bits_[word]);
            if (remaining < wc) {
                uint64_t b = bits_[word];
                for (int bit = 0; bit < 64; ++bit) {
                    if (b & (1ULL << bit)) {
                        if (remaining == 0) return static_cast<unsigned char>((word << 6) | bit);
                        --remaining;
                    }
                }
            }
            remaining -= wc;
        }
        return 0;
    }

    std::array<uint64_t, 4> to_array() const noexcept { return {bits_[0], bits_[1], bits_[2], bits_[3]}; }
    static popcount_bitmap from_array(const std::array<uint64_t, 4>& arr) noexcept { return popcount_bitmap(arr); }
    uint64_t word(int i) const noexcept { return bits_[i]; }
    void set_word(int i, uint64_t v) noexcept { bits_[i] = v; }
};

}  // namespace gteitelbaum
