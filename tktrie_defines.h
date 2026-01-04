#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>

#ifdef NDEBUG
#define KTRIE_DEBUG_ASSERT(cond) ((void)0)
#else
#include <cassert>
#define KTRIE_DEBUG_ASSERT(cond) assert(cond)
#endif

namespace gteitelbaum {

// Header: [LEAF:1][TYPE:2][VERSION:61]
// TYPE: 00=EOS, 01=SKIP, 10=LIST, 11=FULL
static constexpr uint64_t FLAG_LEAF = 1ULL << 63;
static constexpr uint64_t TYPE_EOS  = 0ULL << 61;
static constexpr uint64_t TYPE_SKIP = 1ULL << 61;
static constexpr uint64_t TYPE_LIST = 2ULL << 61;
static constexpr uint64_t TYPE_FULL = 3ULL << 61;
static constexpr uint64_t TYPE_MASK = 3ULL << 61;
static constexpr uint64_t FLAGS_MASK = FLAG_LEAF | TYPE_MASK;
static constexpr uint64_t VERSION_MASK = (1ULL << 61) - 1;

static constexpr int LIST_MAX = 7;

inline constexpr uint64_t make_header(bool is_leaf, uint64_t type, uint64_t version = 0) noexcept {
    return (is_leaf ? FLAG_LEAF : 0) | type | (version & VERSION_MASK);
}
inline constexpr bool is_leaf(uint64_t h) noexcept { return (h & FLAG_LEAF) != 0; }
inline constexpr uint64_t get_type(uint64_t h) noexcept { return h & TYPE_MASK; }
inline constexpr uint64_t get_version(uint64_t h) noexcept { return h & VERSION_MASK; }

// Bump version preserving flags
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

class small_list {
    uint64_t data_ = 0;
public:
    small_list() noexcept = default;
    int count() const noexcept { return static_cast<int>((data_ >> 56) & 0xFF); }
    unsigned char char_at(int i) const noexcept {
        return static_cast<unsigned char>((data_ >> (i * 8)) & 0xFF);
    }
    
    // SWAR find - O(1) regardless of list size
    int find(unsigned char c) const noexcept {
        int n = count();
        if (n == 0) return -1;
        
        // Broadcast c to all 7 data bytes
        uint64_t broadcast = 0x0101010101010101ULL * c;
        // Mask to only compare the bytes that contain data (first n bytes)
        uint64_t mask = (1ULL << (n * 8)) - 1;
        uint64_t data_masked = data_ & mask;
        uint64_t broadcast_masked = broadcast & mask;
        
        // XOR - matching bytes become 0
        uint64_t xored = data_masked ^ broadcast_masked;
        
        // SWAR zero-byte detection: ((v - 0x01...) & ~v & 0x80...)
        // A byte is zero iff its high bit is set in the result
        uint64_t zero_detect = (xored - 0x0101010101010101ULL) & ~xored & 0x8080808080808080ULL;
        
        if (zero_detect == 0) return -1;
        
        // Find position of first zero byte (first match)
        int bit_pos = std::countr_zero(zero_detect);
        int byte_pos = bit_pos >> 3;
        
        return byte_pos < n ? byte_pos : -1;
    }
    
    int add(unsigned char c) noexcept {
        int n = count();
        data_ = (data_ & ~(0xFFULL << 56)) | (static_cast<uint64_t>(c) << (n * 8)) |
                (static_cast<uint64_t>(n + 1) << 56);
        return n;
    }
    void remove_at(int idx) noexcept {
        int n = count();
        for (int i = idx; i < n - 1; ++i) {
            unsigned char next = char_at(i + 1);
            data_ &= ~(0xFFULL << (i * 8));
            data_ |= (static_cast<uint64_t>(next) << (i * 8));
        }
        data_ = (data_ & ~(0xFFULL << ((n-1) * 8))) & ~(0xFFULL << 56);
        data_ |= (static_cast<uint64_t>(n - 1) << 56);
    }
};

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
                bits &= bits - 1;  // Clear LSB - safe for uint64_t when bits != 0
            }
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

struct empty_mutex {
    void lock() noexcept {}
    void unlock() noexcept {}
};

// Fast skip matching - uses memcmp for longer strings
inline size_t match_skip_impl(std::string_view skip, std::string_view key) noexcept {
    size_t min_len = skip.size() < key.size() ? skip.size() : key.size();
    
    // For short strings, byte-by-byte is fine (cache-friendly)
    if (min_len <= 8) {
        size_t i = 0;
        while (i < min_len && skip[i] == key[i]) ++i;
        return i;
    }
    
    // For longer strings, use memcmp to find if they match fully
    if (std::memcmp(skip.data(), key.data(), min_len) == 0) {
        return min_len;
    }
    
    // Find first mismatch
    size_t i = 0;
    while (i < min_len && skip[i] == key[i]) ++i;
    return i;
}

}  // namespace gteitelbaum
