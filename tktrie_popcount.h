#pragma once

#include <array>
#include <bit>
#include <cstdint>

#include "tktrie_defines.h"

namespace gteitelbaum {

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
