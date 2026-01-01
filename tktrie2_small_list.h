/**
 * @file ktrie_small_list.h
 * @brief Compact sorted list for small branch points (≤7 children)
 * 
 * When a trie node has few children (≤7), they're stored in a small sorted
 * list rather than a full 256-bit bitmap. This saves memory for the common
 * case of sparse branching.
 * 
 * Memory Layout (64 bits, big-endian):
 * ┌────────────────────────────────────────────────────────────┬────────┐
 * │              Sorted characters (up to 7 bytes)             │ count  │
 * └────────────────────────────────────────────────────────────┴────────┘
 *   bytes 0-6 (characters in sorted order)                       byte 7
 * 
 * ============================================================================
 * SWAR (SIMD Within A Register) TECHNIQUES EXPLAINED
 * ============================================================================
 * 
 * SWAR is a technique for performing parallel operations on multiple data
 * elements packed into a single machine word. We operate on up to 8 bytes
 * simultaneously within a 64-bit register.
 * 
 * KEY SWAR PATTERNS USED:
 * 
 * 1. BYTE BROADCASTING:
 *    Replicate a single byte to all positions in a 64-bit word.
 *    
 *    uint64_t rep = 0x0101010101010101ULL;
 *    uint64_t broadcast = rep * byte_value;
 *    
 *    Example: byte = 0x41 ('A')
 *    rep * 0x41 = 0x4141414141414141
 * 
 * 2. ZERO BYTE DETECTION:
 *  Credit: Bit Twiddling Hacks
 *          https://graphics.stanford.edu/~seander/bithacks.html
 *          'Determine if a word has a byte equal to n'
 *
 *    Find which bytes in a word are zero (used after XOR for equality).
 *    
 *    Formula: ~((((x & 0x7F...7F) + 0x7F...7F) | x) | 0x7F...7F)
 *    Result: 0x80 in each byte position where x was 0x00
 * 
 * 3. PARALLEL COMPARISON:
 *    Compare all bytes simultaneously using bit manipulation.
 * 
 * @note This is an internal header. Users should include ktrie.h instead.
 */

#pragma once

#include <bit>
#include <cstdint>
#include <string>
#include <utility>

#include "ktrie_defines.h"

namespace gteitelbaum {

/**
 * @class t_small_list
 * @brief Compact sorted character list for small branch points
 */
class t_small_list {
  uint64_t n_;  ///< Packed sorted characters and count

 public:
  static constexpr int max_list = 7;

  inline t_small_list();
  inline explicit t_small_list(uint64_t x);
  inline t_small_list(char c1, char c2);

  KTRIE_FORCE_INLINE int get_list_sz() const;
  KTRIE_FORCE_INLINE uint8_t get_list_at(int pos) const;
  KTRIE_FORCE_INLINE void set_list_at(int pos, char c);
  KTRIE_FORCE_INLINE void set_list_sz(int len);
  inline std::string to_string() const;

  /**
   * @brief Find 1-based offset of a character (0 if not found)
   * 
   * ============================================================================
   * SWAR ZERO-BYTE DETECTION ALGORITHM
   * ============================================================================
   * 
   * ALGORITHM STEPS:
   * 
   * 1. BROADCAST: Replicate search character to all byte positions
   *    rep = 0x0101010101010100 (lowest byte excluded for count)
   *    broadcast = rep * search_char
   * 
   * 2. XOR: Matching bytes become 0x00
   *    diff = n_ ^ broadcast
   * 
   * 3. DETECT ZEROS using formula:
   *    zeros = ~((((diff & 0x7F..7F) + 0x7F..7F) | diff) | 0x7F..7F)
   *    
   *    For each byte:
   *    - If byte is 0x00: formula produces 0x80
   *    - Otherwise: formula produces 0x00
   * 
   * 4. FIND POSITION using countl_zero
   *    Position = countl_zero(zeros) / 8
   * 
   * PERFORMANCE: O(1), ~6-8 instructions, no branches
   */
  KTRIE_FORCE_INLINE int offset(char c) const;

  /**
   * @brief Insert a character in sorted order using SWAR
   * 
   * ============================================================================
   * SWAR UNSIGNED BYTE COMPARISON FOR INSERTION
   * ============================================================================
   * 
   * CHALLENGE: Need unsigned comparison across full 0x00-0xFF range
   * 
   * SOLUTION: Split into two cases based on high bit:
   * 
   * Case 1: High bits DIFFER
   *   A < B iff B has high bit set (B is 128-255, A is 0-127)
   * 
   * Case 2: High bits MATCH
   *   Compare low 7 bits: ~((A|0x80) - B) & 0x80 gives 0x80 where A < B
   * 
   * ALGORITHM:
   * 1. Create valid position mask
   * 2. Broadcast character, mask both operands
   * 3. Compare using split high/low bit technique
   * 4. popcount gives insertion position
   * 5. SWAR shift to make room
   * 6. Insert and update count
   * 
   * PERFORMANCE: O(1), ~15-20 instructions, no branches
   */
  inline int insert(int len, char c);

  KTRIE_FORCE_INLINE uint64_t to_u64() const;
  KTRIE_FORCE_INLINE static t_small_list from_u64(uint64_t v);
};

// =============================================================================
// Inline Definitions
// =============================================================================

t_small_list::t_small_list() : n_{0} {}

t_small_list::t_small_list(uint64_t x) : n_{x} {}

t_small_list::t_small_list(char c1, char c2) : n_{0} {
  if (static_cast<uint8_t>(c1) > static_cast<uint8_t>(c2)) std::swap(c1, c2);
  auto arr = to_char_static(0ULL);
  arr[0] = c1;
  arr[1] = c2;
  arr[7] = 2;
  n_ = from_char_static(arr);
}

int t_small_list::get_list_sz() const {
  return static_cast<int>(n_ & 0xFF);
}

uint8_t t_small_list::get_list_at(int pos) const {
  return to_char_static(n_)[pos];
}

void t_small_list::set_list_at(int pos, char c) {
  KTRIE_DEBUG_ASSERT(pos >= 0 && pos < max_list);
  auto arr = to_char_static(n_);
  arr[pos] = c;
  n_ = from_char_static(arr);
}

void t_small_list::set_list_sz(int len) {
  n_ = (n_ & ~0xFFULL) | static_cast<uint64_t>(len);
}

std::string t_small_list::to_string() const {
  auto arr = to_char_static(n_);
  std::string r;
  for (int i = 0; i < get_list_sz(); ++i) r += arr[i];
  return r;
}

int t_small_list::offset(char c) const {
  //----------------------------------------------------------------------------
  // STEP 1: Replication constant (excludes count byte at position 7)
  //----------------------------------------------------------------------------
  constexpr uint64_t rep = 0x01'01'01'01'01'01'01'00ULL;
  constexpr uint64_t low_bits = 0x7F'7F'7F'7F'7F'7F'7F'7FULL;
  
  //----------------------------------------------------------------------------
  // STEP 2: Broadcast search character and XOR to find matches
  //----------------------------------------------------------------------------
  uint64_t x = static_cast<uint8_t>(c);
  uint64_t diff = n_ ^ (rep * x);  // Matching bytes become 0x00
  
  //----------------------------------------------------------------------------
  // STEP 3: Zero-byte detection using SWAR trick
  // Credit: Bit Twiddling Hacks
  //         https://graphics.stanford.edu/~seander/bithacks.html
  //         'Determine if a word has a byte equal to n'
  //
  // For each byte in diff:
  //   - If byte == 0x00: produces 0x80 (match indicator)
  //   - Otherwise: produces 0x00
  //
  // Formula breakdown for a single byte b:
  //   (b & 0x7F) + 0x7F: if b=0x00, gives 0x7F; else gives 0x80+
  //   | b: preserves high bit of original
  //   | 0x7F: sets all low bits
  //   ~: inverts - only 0x00 bytes become 0x80
  //----------------------------------------------------------------------------
  uint64_t zeros = ~((((diff & low_bits) + low_bits) | diff) | low_bits);
  
  //----------------------------------------------------------------------------
  // STEP 4: Find position using leading zero count
  //
  // zeros has 0x80 in matching positions. countl_zero / 8 gives byte index.
  //----------------------------------------------------------------------------
  int pos = std::countl_zero(zeros) / 8;
  
  return (pos + 1 <= get_list_sz()) ? pos + 1 : 0;
}

int t_small_list::insert(int len, char c) {
  KTRIE_DEBUG_ASSERT(len >= 2 && len < max_list);
  
  //----------------------------------------------------------------------------
  // Bit pattern constants
  //----------------------------------------------------------------------------
  constexpr uint64_t h = 0x80'80'80'80'80'80'80'80ULL;  // high bits
  constexpr uint64_t m = 0x7F'7F'7F'7F'7F'7F'7F'7FULL;  // low 7 bits
  constexpr uint64_t REP = 0x01'01'01'01'01'01'01'01ULL;
  
  //----------------------------------------------------------------------------
  // STEP 1: Create mask for valid positions (top 'len' bytes in big-endian)
  //----------------------------------------------------------------------------
  uint64_t valid_mask = ~0ULL << (8 * (8 - len));
  
  //----------------------------------------------------------------------------
  // STEP 2: Extract characters and broadcast search value
  //----------------------------------------------------------------------------
  uint64_t chars = n_ & valid_mask;
  uint64_t rep_x = (REP * static_cast<uint8_t>(c)) & valid_mask;
  
  //----------------------------------------------------------------------------
  // STEP 3: SWAR unsigned comparison (chars[i] < c for each byte)
  //
  // Split into two cases:
  // - High bits differ: A < B iff B has high bit set
  // - High bits same: compare low 7 bits using ~((A|h) - B) & h
  //----------------------------------------------------------------------------
  uint64_t diff_high = (chars ^ rep_x) & h;
  uint64_t B_high_wins = rep_x & diff_high;
  
  uint64_t same_high = ~diff_high & h;
  uint64_t low_chars = chars & m;
  uint64_t low_x = rep_x & m;
  uint64_t low_cmp = ~((low_chars | h) - low_x) & h;
  
  uint64_t lt = (B_high_wins | (same_high & low_cmp)) & valid_mask;
  
  //----------------------------------------------------------------------------
  // STEP 4: Count bytes where existing < new → insertion position
  //----------------------------------------------------------------------------
  int pos = std::popcount(lt);
  
  //----------------------------------------------------------------------------
  // STEP 5: SWAR shift bytes to make room
  //----------------------------------------------------------------------------
  uint64_t stay_mask = ~(~0ULL >> (8 * pos));
  uint64_t left = n_ & stay_mask;
  uint64_t shift_mask = ~stay_mask & ~0xFFULL;
  uint64_t shifted = (n_ & shift_mask) >> 8;
  n_ = left | shifted;
  
  //----------------------------------------------------------------------------
  // STEP 6: Insert character and update count
  //----------------------------------------------------------------------------
  set_list_at(pos, c);
  set_list_sz(len + 1);
  
  return pos;
}

uint64_t t_small_list::to_u64() const { return n_; }

t_small_list t_small_list::from_u64(uint64_t v) { return t_small_list(v); }

}  // namespace gteitelbaum
