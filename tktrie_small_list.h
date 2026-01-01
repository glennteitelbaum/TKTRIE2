#pragma once

#include <bit>
#include <cstdint>
#include <string>
#include <utility>

#include "tktrie_defines.h"

namespace gteitelbaum {

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
     * 
     * Credit: Bit Twiddling Hacks
     *         https://graphics.stanford.edu/~seander/bithacks.html
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

}  // namespace gteitelbaum
