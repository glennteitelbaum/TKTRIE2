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
// ATOMIC STORAGE HELPERS
// =============================================================================

template <typename T, bool THREADED>
class atomic_storage {
    std::conditional_t<THREADED, std::atomic<T>, T> value_;
public:
    constexpr atomic_storage() noexcept : value_{} {}
    constexpr explicit atomic_storage(T v) noexcept : value_(v) {}
    
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

template <bool THREADED>
using atomic_counter = atomic_storage<size_t, THREADED>;

// =============================================================================
// HEADER FLAGS AND CONSTANTS
// =============================================================================

static constexpr uint64_t FLAG_LEAF      = 1ULL << 63;
static constexpr uint64_t FLAG_POISON    = 1ULL << 62;
static constexpr uint64_t FLAG_HAS_EOS   = 1ULL << 61;
static constexpr uint64_t FLAG_SKIP_USED = 1ULL << 60;
static constexpr uint64_t FLAG_IS_FLOOR  = 1ULL << 59;
static constexpr uint64_t FLAG_IS_CEIL   = 1ULL << 58;
static constexpr uint64_t FLAG_SKIP      = 1ULL << 57;
static constexpr uint64_t FLAG_BINARY    = 1ULL << 56;
static constexpr uint64_t FLAG_LIST      = 1ULL << 55;
static constexpr uint64_t FLAG_POP       = 1ULL << 54;
static constexpr uint64_t FLAG_FULL      = 1ULL << 53;

static constexpr uint64_t VERSION_MASK = (1ULL << 53) - 1;
static constexpr uint64_t FLAGS_MASK = ~VERSION_MASK;
static constexpr uint64_t TYPE_FLAGS_MASK = FLAG_SKIP | FLAG_BINARY | FLAG_LIST | FLAG_POP | FLAG_FULL;

static constexpr int BINARY_MIN = 1;
static constexpr int BINARY_MAX = 2;
static constexpr int LIST_MIN = 3;
static constexpr int LIST_MAX = 7;
static constexpr int POP_MIN = 8;
static constexpr int POP_MAX = 32;
static constexpr int FULL_MIN = 33;

static constexpr uint64_t RETRY_SENTINEL_HEADER = FLAG_POISON | FLAG_FULL;

inline constexpr bool is_poisoned_header(uint64_t h) noexcept {
    return (h & FLAG_POISON) != 0;
}
inline constexpr bool is_leaf(uint64_t h) noexcept { return (h & FLAG_LEAF) != 0; }
inline constexpr bool has_eos_flag(uint64_t h) noexcept { return (h & FLAG_HAS_EOS) != 0; }
inline constexpr bool has_skip_used(uint64_t h) noexcept { return (h & FLAG_SKIP_USED) != 0; }
inline constexpr bool is_at_floor(uint64_t h) noexcept { return (h & FLAG_IS_FLOOR) != 0; }
inline constexpr bool is_at_ceil(uint64_t h) noexcept { return (h & FLAG_IS_CEIL) != 0; }
inline constexpr uint64_t get_version(uint64_t h) noexcept { return h & VERSION_MASK; }

inline constexpr uint64_t make_header(bool is_leaf, uint64_t type_flag, 
                                       bool skip_used = false, bool at_floor = false, 
                                       bool at_ceil = false, uint64_t version = 0) noexcept {
    return (is_leaf ? FLAG_LEAF : 0) 
         | type_flag 
         | (skip_used ? FLAG_SKIP_USED : 0)
         | (at_floor ? FLAG_IS_FLOOR : 0)
         | (at_ceil ? FLAG_IS_CEIL : 0)
         | (version & VERSION_MASK);
}

inline constexpr uint64_t bump_version(uint64_t h) noexcept {
    uint64_t flags = h & FLAGS_MASK;
    uint64_t ver = (h & VERSION_MASK) + 1;
    return flags | (ver & VERSION_MASK);
}

inline constexpr uint64_t set_flag(uint64_t h, uint64_t flag) noexcept { return h | flag; }
inline constexpr uint64_t clear_flag(uint64_t h, uint64_t flag) noexcept { return h & ~flag; }

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
// SMALL_LIST - packed list of up to 7 chars in single uint64
// =============================================================================

template <bool THREADED>
class small_list {
    atomic_storage<uint64_t, THREADED> data_{0};
public:
    constexpr small_list() noexcept = default;
    
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
        
        //----------------------------------------------------------------------
        // SWAR constants - exclude count byte (byte 7)
        //----------------------------------------------------------------------
        constexpr uint64_t rep = 0x00'01'01'01'01'01'01'01ULL;
        constexpr uint64_t low_bits = 0x7F'7F'7F'7F'7F'7F'7F'7FULL;
        
        //----------------------------------------------------------------------
        // Broadcast search character and XOR to find matches
        //----------------------------------------------------------------------
        uint64_t diff = d ^ (rep * static_cast<uint64_t>(static_cast<uint8_t>(c)));
        
        // Force count byte (byte 7) to non-zero to prevent false matches
        diff |= 0xFF'00'00'00'00'00'00'00ULL;
        
        //----------------------------------------------------------------------
        // Zero-byte detection using SWAR trick
        // Credit: Bit Twiddling Hacks
        //         https://graphics.stanford.edu/~seander/bithacks.html
        //
        // For each byte in diff:
        //   - If byte == 0x00: produces 0x80 (match indicator)
        //   - Otherwise: produces 0x00
        //----------------------------------------------------------------------
        uint64_t zeros = ~((((diff & low_bits) + low_bits) | diff) | low_bits);
        
        if (zeros == 0) return -1;
        
        //----------------------------------------------------------------------
        // Find position using trailing zero count (chars at bytes 0-6)
        //----------------------------------------------------------------------
        int pos = std::countr_zero(zeros) / 8;
        int n = static_cast<int>((d >> 56) & 0xFF);
        
        return (pos < n) ? pos : -1;
    }
    
    int add(unsigned char c) noexcept {
        uint64_t d = data_.load();
        int n = static_cast<int>((d >> 56) & 0xFF);
        [[assume(n >= 0 && n < 7)]];
        d = (d & ~(0xFFULL << 56)) | (static_cast<uint64_t>(c) << (n * 8)) |
            (static_cast<uint64_t>(n + 1) << 56);
        data_.store(d);
        return n;
    }
    
    // Sorted insert - returns insertion position
    int add_sorted(unsigned char c) noexcept {
        uint64_t d = data_.load();
        int n = static_cast<int>((d >> 56) & 0xFF);
        [[assume(n >= 0 && n < 7)]];
        
        // Find insertion point (linear search, n <= 6)
        int pos = 0;
        while (pos < n && static_cast<unsigned char>((d >> (pos * 8)) & 0xFF) < c) ++pos;
        
        // Shift elements from pos..n-1 up to pos+1..n
        for (int i = n; i > pos; --i) {
            unsigned char ch = static_cast<unsigned char>((d >> ((i - 1) * 8)) & 0xFF);
            d &= ~(0xFFULL << (i * 8));
            d |= (static_cast<uint64_t>(ch) << (i * 8));
        }
        
        // Insert new char at pos
        d &= ~(0xFFULL << (pos * 8));
        d |= (static_cast<uint64_t>(c) << (pos * 8));
        
        // Update count
        d = (d & ~(0xFFULL << 56)) | (static_cast<uint64_t>(n + 1) << 56);
        data_.store(d);
        return pos;
    }
    
    void remove_at(int idx) noexcept {
        uint64_t d = data_.load();
        int n = static_cast<int>((d >> 56) & 0xFF);
        [[assume(n >= 0 && n < 8)]];
        [[assume(idx >= 0 && idx < n)]];
        for (int i = idx; i < n - 1; ++i) {
            unsigned char next = static_cast<unsigned char>((d >> ((i + 1) * 8)) & 0xFF);
            d &= ~(0xFFULL << (i * 8));
            d |= (static_cast<uint64_t>(next) << (i * 8));
        }
        d &= ~(0xFFULL << ((n - 1) * 8));
        d = (d & ~(0xFFULL << 56)) | (static_cast<uint64_t>(n - 1) << 56);
        data_.store(d);
    }
};

// =============================================================================
// BITMAP256 - 256-bit bitmap for FULL/POP nodes
// =============================================================================

template <bool THREADED>
class bitmap256 {
    std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> bits_[4] = {};
    
    uint64_t load_word(int w) const noexcept {
        if constexpr (THREADED) {
            return bits_[w].load(std::memory_order_acquire);
        } else {
            return bits_[w];
        }
    }
    
    void store_word(int w, uint64_t val) noexcept {
        if constexpr (THREADED) {
            bits_[w].store(val, std::memory_order_release);
        } else {
            bits_[w] = val;
        }
    }
    
public:
    constexpr bitmap256() noexcept = default;
    
    bitmap256(const bitmap256& other) noexcept {
        for (int w = 0; w < 4; ++w) {
            store_word(w, other.load_word(w));
        }
    }
    
    bitmap256& operator=(const bitmap256& other) noexcept {
        if (this != &other) {
            for (int w = 0; w < 4; ++w) {
                store_word(w, other.load_word(w));
            }
        }
        return *this;
    }
    
    bool test(unsigned char c) const noexcept {
        return (load_word(c >> 6) & (1ULL << (c & 63))) != 0;
    }
    
    void set(unsigned char c) noexcept {
        if constexpr (THREADED) {
            bits_[c >> 6].fetch_or(1ULL << (c & 63), std::memory_order_release);
        } else {
            bits_[c >> 6] |= (1ULL << (c & 63));
        }
    }
    
    void clear(unsigned char c) noexcept {
        if constexpr (THREADED) {
            bits_[c >> 6].fetch_and(~(1ULL << (c & 63)), std::memory_order_release);
        } else {
            bits_[c >> 6] &= ~(1ULL << (c & 63));
        }
    }
    
    uint64_t word(int w) const noexcept { return load_word(w); }
    
    int count() const noexcept {
        return std::popcount(load_word(0)) + std::popcount(load_word(1)) +
               std::popcount(load_word(2)) + std::popcount(load_word(3));
    }
    
    int slot_for(unsigned char c) const noexcept {
        int w = c >> 6;
        uint64_t mask = (1ULL << (c & 63)) - 1;
        
        int s0 = std::popcount(load_word(w) & mask);
        int s1 = s0 + std::popcount(load_word(0));
        int s2 = s1 + std::popcount(load_word(1));
        int s3 = s2 + std::popcount(load_word(2));
        
        std::array<int, 4> cumulative = {s0, s1, s2, s3};
        return cumulative[w];
    }
    
    // Combined test + slot_for: returns slot index or -1 if not set
    int test_slot(unsigned char c) const noexcept {
        if ((load_word(c >> 6) & (1ULL << (c & 63))) == 0) return -1;
        return slot_for(c);
    }
    
    unsigned char first() const noexcept {
        for (int w = 0; w < 4; ++w) {
            uint64_t bits = load_word(w);
            if (bits) return static_cast<unsigned char>((w << 6) | std::countr_zero(bits));
        }
        return 0;
    }
    
    template <typename Fn>
    void for_each_set(Fn&& fn) const noexcept {
        for (int w = 0; w < 4; ++w) {
            uint64_t bits = load_word(w);
            while (bits) {
                unsigned char c = static_cast<unsigned char>((w << 6) | std::countr_zero(bits));
                fn(c);
                bits &= bits - 1;
            }
        }
    }
    
    template <typename Array>
    int shift_up_for_insert(unsigned char c, Array& arr, int current_count) noexcept {
        int slot = slot_for(c);
        for (int i = current_count; i > slot; --i) {
            arr[i] = std::move(arr[i - 1]);
        }
        return slot;
    }
    
    template <typename Array, typename ClearFn>
    int shift_down_for_remove(unsigned char c, Array& arr, ClearFn&& clear_fn) noexcept {
        int slot = slot_for(c);
        clear(c);
        int new_count = count();
        for (int i = slot; i < new_count; ++i) {
            arr[i] = std::move(arr[i + 1]);
        }
        clear_fn(arr[new_count]);
        return new_count;
    }
    
    template <bool>
    bool atomic_test(unsigned char c) const noexcept { return test(c); }
    
    template <bool>
    void atomic_set(unsigned char c) noexcept { set(c); }
    
    template <bool>
    void atomic_clear(unsigned char c) noexcept { clear(c); }
};

// =============================================================================
// EMPTY_MUTEX
// =============================================================================

struct empty_mutex {
    void lock() noexcept {}
    void unlock() noexcept {}
};

// =============================================================================
// INLINE_SKIP - compact skip storage for fixed-length keys
// =============================================================================

template <size_t MAX_LEN>
class inline_skip {
    static_assert(MAX_LEN > 0 && MAX_LEN <= 16, "MAX_LEN must be 1-16");
    
    char data_[MAX_LEN] = {};
    
public:
    inline_skip() noexcept = default;
    
    inline_skip(std::string_view sv) noexcept { assign(sv); }
    
    void assign(std::string_view sv) noexcept {
        size_t n = sv.size() < MAX_LEN ? sv.size() : MAX_LEN - 1;
        std::memset(data_, 0, MAX_LEN);
        std::memcpy(data_, sv.data(), n);
        data_[MAX_LEN - 1] = static_cast<char>(n);
    }
    
    void assign(const char* s, size_t len) noexcept {
        assign(std::string_view(s, len));
    }
    
    inline_skip& operator=(std::string_view sv) noexcept {
        assign(sv);
        return *this;
    }
    
    size_t size() const noexcept { 
        return static_cast<unsigned char>(data_[MAX_LEN - 1]); 
    }
    
    bool empty() const noexcept { return size() == 0; }
    
    const char* data() const noexcept { return data_; }
    
    char operator[](size_t i) const noexcept { return data_[i]; }
    
    std::string_view view() const noexcept { return std::string_view(data_, size()); }
    
    operator std::string_view() const noexcept { return view(); }
    
    std::string_view substr(size_t pos, size_t len = std::string_view::npos) const noexcept {
        return view().substr(pos, len);
    }
    
    void clear() noexcept {
        std::memset(data_, 0, MAX_LEN);
    }
    
    void push_back(char c) noexcept {
        size_t n = size();
        if (n < MAX_LEN - 1) {
            data_[n] = c;
            data_[MAX_LEN - 1] = static_cast<char>(n + 1);
        }
    }
    
    void append(std::string_view sv) noexcept {
        size_t n = size();
        size_t add = sv.size();
        if (n + add > MAX_LEN - 1) add = MAX_LEN - 1 - n;
        std::memcpy(data_ + n, sv.data(), add);
        data_[MAX_LEN - 1] = static_cast<char>(n + add);
    }
};

template <size_t FIXED_KEY_LEN>
using skip_storage_t = std::conditional_t<
    FIXED_KEY_LEN == 0,
    std::string,
    inline_skip<FIXED_KEY_LEN>
>;

// =============================================================================
// SKIP MATCHING
// =============================================================================

inline bool consume_prefix(std::string_view& key, std::string_view skip) noexcept {
    size_t sz = skip.size();
    if (sz > key.size() || std::memcmp(skip.data(), key.data(), sz) != 0) return false;
    key.remove_prefix(sz);
    return true;
}

inline size_t match_skip_impl(std::string_view skip, std::string_view key) noexcept {
    size_t min_len = skip.size() < key.size() ? skip.size() : key.size();
    if (std::memcmp(skip.data(), key.data(), min_len) == 0) return min_len;
    size_t i = 0;
    while (i < min_len && skip[i] == key[i]) ++i;
    return i;
}

}  // namespace gteitelbaum
