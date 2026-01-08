// TKTRIE - Combined single-header version
// Auto-generated - do not edit
#pragma once

// === tktrie_defines.h ===

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

// =============================================================================
// PLACEMENT NEW/DESTROY UTILITIES
// C++20 compatible, uses std::construct_at/destroy_at if available (C++20+)
// =============================================================================

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
// ATOMIC STORAGE HELPERS - eliminates repeated if constexpr (THREADED) patterns
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

// Convenience alias for size counters
template <bool THREADED>
using atomic_counter = atomic_storage<size_t, THREADED>;

// =============================================================================
// HEADER FLAGS AND CONSTANTS
// =============================================================================

// Header: [LEAF:1][SKIP:1][LIST:1][POISON:1][VERSION:60]
// SKIP: FLAG_SKIP set (always leaf)
// LIST: FLAG_LIST set
// FULL: neither SKIP nor LIST set
static constexpr uint64_t FLAG_LEAF   = 1ULL << 63;
static constexpr uint64_t FLAG_SKIP   = 1ULL << 62;  // always leaf
static constexpr uint64_t FLAG_LIST   = 1ULL << 61;  // leaf or interior
static constexpr uint64_t FLAG_POISON = 1ULL << 60;
static constexpr uint64_t VERSION_MASK = (1ULL << 60) - 1;
static constexpr uint64_t FLAGS_MASK = FLAG_LEAF | FLAG_SKIP | FLAG_LIST | FLAG_POISON;

static constexpr int LIST_MAX = 7;

// Interior FULL node header with poison flag set - used for retry sentinel
static constexpr uint64_t RETRY_SENTINEL_HEADER = FLAG_POISON;  // FULL (no SKIP/LIST) + poison

inline constexpr bool is_poisoned_header(uint64_t h) noexcept {
    return (h & FLAG_POISON) != 0;
}

inline constexpr uint64_t make_header(bool is_leaf, uint64_t type_flag, uint64_t version = 0) noexcept {
    return (is_leaf ? FLAG_LEAF : 0) | type_flag | (version & VERSION_MASK);
}
inline constexpr bool is_leaf(uint64_t h) noexcept { return (h & FLAG_LEAF) != 0; }
inline constexpr uint64_t get_version(uint64_t h) noexcept { return h & VERSION_MASK; }

// Bump version preserving flags (including poison)
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

// =============================================================================
// SMALL_LIST - packed list of up to 7 chars in single uint64
// Layout: [count:8][char6:8][char5:8][char4:8][char3:8][char2:8][char1:8][char0:8]
// Templated on THREADED to avoid atomic overhead when not needed
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
        int n = static_cast<int>((d >> 56) & 0xFF);
        [[assume(n >= 0 && n < 8)]];
        for (int i = 0; i < n; ++i) {
            if (static_cast<unsigned char>((d >> (i * 8)) & 0xFF) == c) return i;
        }
        return -1;
    }
    
    int add(unsigned char c) noexcept {
        uint64_t d = data_.load();
        int n = static_cast<int>((d >> 56) & 0xFF);
        [[assume(n >= 0 && n < 7)]];  // Must have room to add
        d = (d & ~(0xFFULL << 56)) | (static_cast<uint64_t>(c) << (n * 8)) |
            (static_cast<uint64_t>(n + 1) << 56);
        data_.store(d);
        return n;
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
// BITMAP256 - 256-bit bitmap for FULL nodes
// =============================================================================

class bitmap256 {
    uint64_t bits_[4] = {};
public:
    constexpr bitmap256() noexcept = default;
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
                bits &= bits - 1;
            }
        }
    }
    
    template <bool THREADED>
    bool atomic_test(unsigned char c) const noexcept {
        if constexpr (THREADED) {
            uint64_t val = reinterpret_cast<const std::atomic<uint64_t>*>(&bits_[c >> 6])->load(std::memory_order_acquire);
            return (val & (1ULL << (c & 63))) != 0;
        } else {
            return test(c);
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

// =============================================================================
// EMPTY_MUTEX - no-op mutex for non-threaded mode
// =============================================================================

struct empty_mutex {
    void lock() noexcept {}
    void unlock() noexcept {}
};

// =============================================================================
// SKIP MATCHING
// =============================================================================

// Reader version - check if key starts with skip and consume it if so
inline bool consume_prefix(std::string_view& key, std::string_view skip) noexcept {
    size_t sz = skip.size();
    if (sz > key.size() || std::memcmp(skip.data(), key.data(), sz) != 0) return false;
    key.remove_prefix(sz);
    return true;
}

// Insert version - returns mismatch position (needed for split operations)
inline size_t match_skip_impl(std::string_view skip, std::string_view key) noexcept {
    size_t min_len = skip.size() < key.size() ? skip.size() : key.size();
    if (std::memcmp(skip.data(), key.data(), min_len) == 0) return min_len;
    size_t i = 0;
    while (i < min_len && skip[i] == key[i]) ++i;
    return i;
}

}  // namespace gteitelbaum

// === tktrie_dataptr.h ===

#include <atomic>
#include <cstring>
#include <memory>
#include <type_traits>


namespace gteitelbaum {

// =============================================================================
// DATAPTR - value storage with inline optimization
// sizeof(T) <= sizeof(T*): store T inline (atomic if THREADED)
// sizeof(T) > sizeof(T*): store T* pointer (atomic swap for updates)
// OPTIONAL=true forces pointer mode so has_data() can distinguish empty from zero
// =============================================================================

template <typename T, bool THREADED, typename Allocator, bool OPTIONAL = false>
class dataptr {
    // For optional values, always use pointer mode so nullptr means "not set"
    static constexpr bool INLINE = !OPTIONAL && sizeof(T) <= sizeof(T*) && std::is_trivially_copyable_v<T>;
    
    using alloc_traits = std::allocator_traits<Allocator>;
    using value_alloc_t = typename alloc_traits::template rebind_alloc<T>;
    using value_alloc_traits = std::allocator_traits<value_alloc_t>;

    // Inline: store T directly (atomic<T> if threaded)
    // Pointer: store T* (atomic<T*> if threaded)
    std::conditional_t<INLINE,
        std::conditional_t<THREADED, std::atomic<T>, T>,
        std::conditional_t<THREADED, std::atomic<T*>, T*>
    > storage_{};

public:
    dataptr() noexcept = default;
    
    ~dataptr() {
        if constexpr (!INLINE) {
            T* p = load_ptr();
            if (p) {
                value_alloc_t alloc;
                std::destroy_at(p);
                value_alloc_traits::deallocate(alloc, p, 1);
            }
        }
    }
    
    dataptr(const dataptr&) = delete;
    dataptr& operator=(const dataptr&) = delete;
    
    dataptr(dataptr&& other) noexcept {
        if constexpr (INLINE) {
            store_inline(other.load_inline());
        } else {
            store_ptr(other.exchange_ptr(nullptr));
        }
    }
    
    dataptr& operator=(dataptr&& other) noexcept {
        if (this != &other) {
            if constexpr (INLINE) {
                store_inline(other.load_inline());
            } else {
                T* old = exchange_ptr(other.exchange_ptr(nullptr));
                if (old) {
                    value_alloc_t alloc;
                    std::destroy_at(old);
                    value_alloc_traits::deallocate(alloc, old, 1);
                }
            }
        }
        return *this;
    }

    bool has_data() const noexcept {
        if constexpr (INLINE) return true;  // Inline always "has" data
        else return load_ptr() != nullptr;
    }

    bool try_read(T& out) const noexcept {
        if constexpr (INLINE) {
            out = load_inline();
            return true;
        } else {
            T* p = load_ptr();
            if (!p) return false;
            out = *p;
            return true;
        }
    }
    
    T read() const noexcept {
        if constexpr (INLINE) {
            return load_inline();
        } else {
            T* p = load_ptr();
            return p ? *p : T{};
        }
    }

    void set(const T& value) {
        if constexpr (INLINE) {
            store_inline(value);
        } else {
            value_alloc_t alloc;
            T* new_ptr = value_alloc_traits::allocate(alloc, 1);
            try { std::construct_at(new_ptr, value); }
            catch (...) { value_alloc_traits::deallocate(alloc, new_ptr, 1); throw; }
            T* old = exchange_ptr(new_ptr);
            if (old) { std::destroy_at(old); value_alloc_traits::deallocate(alloc, old, 1); }
        }
    }

    void set(T&& value) {
        if constexpr (INLINE) {
            store_inline(value);
        } else {
            value_alloc_t alloc;
            T* new_ptr = value_alloc_traits::allocate(alloc, 1);
            try { std::construct_at(new_ptr, std::move(value)); }
            catch (...) { value_alloc_traits::deallocate(alloc, new_ptr, 1); throw; }
            T* old = exchange_ptr(new_ptr);
            if (old) { std::destroy_at(old); value_alloc_traits::deallocate(alloc, old, 1); }
        }
    }

    void clear() noexcept {
        if constexpr (INLINE) {
            store_inline(T{});
        } else {
            T* old = exchange_ptr(nullptr);
            if (old) {
                value_alloc_t alloc;
                std::destroy_at(old);
                value_alloc_traits::deallocate(alloc, old, 1);
            }
        }
    }

    void deep_copy_from(const dataptr& other) {
        if constexpr (INLINE) {
            store_inline(other.load_inline());
        } else {
            T* src = other.load_ptr();
            if (src) set(*src);
            else clear();
        }
    }

private:
    // Inline accessors
    T load_inline() const noexcept {
        if constexpr (THREADED) return storage_.load(std::memory_order_acquire);
        else return storage_;
    }
    void store_inline(const T& v) noexcept {
        if constexpr (THREADED) storage_.store(v, std::memory_order_release);
        else storage_ = v;
    }
    
    // Pointer accessors (only used when !INLINE)
    T* load_ptr() const noexcept {
        if constexpr (THREADED) return storage_.load(std::memory_order_acquire);
        else return storage_;
    }
    void store_ptr(T* p) noexcept {
        if constexpr (THREADED) storage_.store(p, std::memory_order_release);
        else storage_ = p;
    }
    T* exchange_ptr(T* p) noexcept {
        if constexpr (THREADED) return storage_.exchange(p, std::memory_order_acq_rel);
        else { T* old = storage_; storage_ = p; return old; }
    }
};

}  // namespace gteitelbaum

// === tktrie_node.h ===

#include <atomic>
#include <array>
#include <memory>
#include <string>

namespace gteitelbaum {

// =============================================================================
// FORWARD DECLARATIONS
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN> struct node_base;
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN> struct node_with_skip;
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN> struct skip_node;
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF> struct list_node;
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN, bool IS_LEAF> struct full_node;
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN> class node_builder;

// Forward declare retry sentinel getter
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
node_base<T, THREADED, Allocator, FIXED_LEN>* get_retry_sentinel() noexcept;

// =============================================================================
// SKIP_STRING - fixed or variable length skip storage
// =============================================================================

template <size_t FIXED_LEN>
struct skip_string {
    std::array<char, FIXED_LEN> data{};
    uint8_t len = 0;
    
    skip_string() = default;
    skip_string(std::string_view sv) : len(static_cast<uint8_t>(sv.size())) {
        std::memcpy(data.data(), sv.data(), sv.size());
    }
    
    std::string_view view() const noexcept { return {data.data(), len}; }
    size_t size() const noexcept { return len; }
    bool empty() const noexcept { return len == 0; }
    char operator[](size_t i) const noexcept { return data[i]; }
    
    void assign(std::string_view sv) {
        len = static_cast<uint8_t>(sv.size());
        std::memcpy(data.data(), sv.data(), sv.size());
    }
    void clear() noexcept { len = 0; }
};

template <>
struct skip_string<0> {
    std::string data;
    
    skip_string() = default;
    skip_string(std::string_view sv) : data(sv) {}
    
    std::string_view view() const noexcept { return data; }
    size_t size() const noexcept { return data.size(); }
    bool empty() const noexcept { return data.empty(); }
    char operator[](size_t i) const noexcept { return data[i]; }
    
    void assign(std::string_view sv) { data = std::string(sv); }
    void clear() { data.clear(); }
};

// =============================================================================
// ATOMIC_NODE_PTR - defaults to nullptr
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct atomic_node_ptr {
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = base_t*;
    
    std::atomic<ptr_t> ptr_;
    
    atomic_node_ptr() noexcept : ptr_(nullptr) {}
    explicit atomic_node_ptr(ptr_t p) noexcept : ptr_(p) {}
    
    ptr_t load() const noexcept { return ptr_.load(std::memory_order_acquire); }
    void store(ptr_t p) noexcept { ptr_.store(p, std::memory_order_release); }
    ptr_t exchange(ptr_t p) noexcept { return ptr_.exchange(p, std::memory_order_acq_rel); }
};

// =============================================================================
// NODE_BASE - header only, type queries and dispatchers
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct node_base {
    using self_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = self_t*;
    using atomic_ptr = atomic_node_ptr<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = dataptr<T, THREADED, Allocator, false>;      // Required values
    using eos_data_t = dataptr<T, THREADED, Allocator, true>;   // Optional EOS values
    using skip_t = skip_string<FIXED_LEN>;
    
    atomic_storage<uint64_t, THREADED> header_;
    
    // EBR retire fields - only used when node is poisoned/retired
    uint64_t retire_epoch_ = 0;
    self_t* retire_next_ = nullptr;
    
    constexpr node_base() noexcept = default;
    constexpr explicit node_base(uint64_t initial_header) noexcept : header_(initial_header) {}
    
    // Header access
    uint64_t header() const noexcept { return header_.load(); }
    void set_header(uint64_t h) noexcept { header_.store(h); }
    
    // Version and poison
    uint64_t version() const noexcept { return get_version(header()); }
    void bump_version() noexcept { header_.store(gteitelbaum::bump_version(header_.load())); }
    void poison() noexcept {
        // Bump version AND set poison - ensures version check catches poisoned nodes
        uint64_t h = header_.load();
        header_.store(gteitelbaum::bump_version(h) | FLAG_POISON);
    }
    void unpoison() noexcept { header_.store(header_.load() & ~FLAG_POISON); }
    bool is_poisoned() const noexcept { return is_poisoned_header(header()); }
    
    // Type queries
    bool is_leaf() const noexcept { return gteitelbaum::is_leaf(header()); }
    bool is_skip() const noexcept { return header() & FLAG_SKIP; }
    bool is_list() const noexcept { return header() & FLAG_LIST; }
    bool is_full() const noexcept { return !(header() & (FLAG_SKIP | FLAG_LIST)); }
    
    // Downcasts
    skip_node<T, THREADED, Allocator, FIXED_LEN>* as_skip() noexcept {
        return static_cast<skip_node<T, THREADED, Allocator, FIXED_LEN>*>(this);
    }
    template <bool IS_LEAF>
    list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_list() noexcept {
        return static_cast<list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    template <bool IS_LEAF>
    full_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_full() noexcept {
        return static_cast<full_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    
    const skip_node<T, THREADED, Allocator, FIXED_LEN>* as_skip() const noexcept {
        return static_cast<const skip_node<T, THREADED, Allocator, FIXED_LEN>*>(this);
    }
    template <bool IS_LEAF>
    const list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_list() const noexcept {
        return static_cast<const list_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    template <bool IS_LEAF>
    const full_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>* as_full() const noexcept {
        return static_cast<const full_node<T, THREADED, Allocator, FIXED_LEN, IS_LEAF>*>(this);
    }
    
    // Skip access - all node types have skip via node_with_skip
    std::string_view skip_str() const noexcept {
        return static_cast<const node_with_skip<T, THREADED, Allocator, FIXED_LEN>*>(this)->skip.view();
    }
    
    // =========================================================================
    // DISPATCHERS - is_leaf param allows compiler to eliminate dead branches
    // =========================================================================
    
    // Child access (interior nodes only)
    ptr_t get_child(unsigned char c) const noexcept {
        if (is_list()) [[likely]] {
            return as_list<false>()->get_child(c);
        }
        return as_full<false>()->get_child(c);
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        if (is_list()) [[likely]] {
            return as_list<false>()->get_child_slot(c);
        }
        return as_full<false>()->get_child_slot(c);
    }
    
    int child_count() const noexcept {
        if (is_list()) [[likely]] return as_list<false>()->count();
        return as_full<false>()->count();
    }
    
    // EOS access (interior nodes only, FIXED_LEN==0 only)
    bool has_eos() const noexcept {
        if constexpr (FIXED_LEN > 0) {
            return false;
        } else {
            if (is_list()) [[likely]] return as_list<false>()->eos.has_data();
            return as_full<false>()->eos.has_data();
        }
    }
    
    bool try_read_eos(T& out) const noexcept {
        if constexpr (FIXED_LEN > 0) {
            (void)out;
            return false;
        } else {
            if (is_list()) [[likely]] return as_list<false>()->eos.try_read(out);
            return as_full<false>()->eos.try_read(out);
        }
    }
    
    void set_eos(const T& value) {
        if constexpr (FIXED_LEN > 0) {
            (void)value;
        } else {
            if (is_list()) [[likely]] as_list<false>()->eos.set(value);
            else as_full<false>()->eos.set(value);
        }
    }
    
    void clear_eos() {
        if constexpr (FIXED_LEN > 0) {
            // No EOS for fixed-length keys
        } else {
            if (is_list()) [[likely]] as_list<false>()->eos.clear();
            else as_full<false>()->eos.clear();
        }
    }
};

// =============================================================================
// NODE_WITH_SKIP - intermediate base with skip field first
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct node_with_skip : node_base<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using skip_t = typename base_t::skip_t;
    
    skip_t skip;
    
    constexpr node_with_skip() noexcept = default;
    constexpr explicit node_with_skip(uint64_t h) noexcept : base_t(h) {}
};

// =============================================================================
// SKIP_NODE - skip string + single value (always leaf)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct skip_node : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    
    data_t value;
    
    skip_node() = default;
    ~skip_node() = default;
};

// =============================================================================
// LIST_NODE - LEAF specialization (stores values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct list_node<T, THREADED, Allocator, FIXED_LEN, true> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    
    static constexpr int MAX_CHILDREN = 7;
    
    small_list<THREADED> chars;
    std::array<data_t, MAX_CHILDREN> values;
    
    list_node() = default;
    ~list_node() = default;
    
    // Unified interface
    int count() const noexcept { return chars.count(); }
    int find(unsigned char c) const noexcept { return chars.find(c); }
    bool has(unsigned char c) const noexcept { return chars.find(c) >= 0; }
    
    // Caller must verify find(c) >= 0 first
    bool read_value(int idx, T& out) const noexcept {
        [[assume(idx >= 0 && idx < 7)]];
        return values[idx].try_read(out);
    }
    
    void set_value(unsigned char c, const T& val) {
        int idx = chars.find(c);
        if (idx >= 0) {
            values[idx].set(val);
        } else {
            idx = chars.add(c);
            values[idx].set(val);
        }
    }
    
    int add_value(unsigned char c, const T& val) {
        int idx = chars.add(c);
        values[idx].set(val);
        return idx;
    }
    
    void remove_value(unsigned char c) {
        int idx = chars.find(c);
        if (idx < 0) return;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = idx; i < cnt - 1; ++i) {
            values[i] = std::move(values[i + 1]);
        }
        values[cnt - 1].clear();
        chars.remove_at(idx);
    }
    
    void copy_values_to(list_node* dest) const {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->values[i].deep_copy_from(values[i]);
        }
    }
};

// =============================================================================
// LIST_NODE - INTERIOR specialization, FIXED_LEN > 0 (no eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct list_node<T, THREADED, Allocator, FIXED_LEN, false> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    static constexpr int MAX_CHILDREN = 7;
    
    small_list<THREADED> chars;
    std::array<atomic_ptr, MAX_CHILDREN> children;
    
    list_node() = default;
    ~list_node() = default;
    
    // Unified interface
    int count() const noexcept { return chars.count(); }
    bool has(unsigned char c) const noexcept { return chars.find(c) >= 0; }
    
    ptr_t get_child(unsigned char c) const noexcept {
        int idx = chars.find(c);
        return idx >= 0 ? children[idx].load() : nullptr;
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        int idx = chars.find(c);
        return idx >= 0 ? &children[idx] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        int idx = chars.add(c);
        children[idx].store(child);
    }
    
    void add_two_children(unsigned char c1, ptr_t child1, unsigned char c2, ptr_t child2) {
        chars.add(c1);
        chars.add(c2);
        children[0].store(child1);
        children[1].store(child2);
    }
    
    void remove_child(unsigned char c) {
        int idx = chars.find(c);
        if (idx < 0) return;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = idx; i < cnt - 1; ++i) {
            children[i].store(children[i + 1].load());
        }
        children[cnt - 1].store(nullptr);
        chars.remove_at(idx);
    }
    
    void move_children_to(list_node* dest) {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void copy_children_to(list_node* dest) const {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
        }
    }
    
    void move_interior_to(list_node* dest) {
        move_children_to(dest);
    }
    
    void copy_interior_to(list_node* dest) const {
        copy_children_to(dest);
    }
    
    void move_interior_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest);
    void copy_interior_to_full(full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) const;
};

// =============================================================================
// LIST_NODE - INTERIOR specialization, FIXED_LEN == 0 (has eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct list_node<T, THREADED, Allocator, 0, false> 
    : node_with_skip<T, THREADED, Allocator, 0> {
    using base_t = node_with_skip<T, THREADED, Allocator, 0>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using data_t = typename base_t::data_t;
    using eos_data_t = typename base_t::eos_data_t;
    
    static constexpr int MAX_CHILDREN = 7;
    
    eos_data_t eos;
    small_list<THREADED> chars;
    std::array<atomic_ptr, MAX_CHILDREN> children;
    
    list_node() = default;
    ~list_node() = default;
    
    // Unified interface
    int count() const noexcept { return chars.count(); }
    bool has(unsigned char c) const noexcept { return chars.find(c) >= 0; }
    
    ptr_t get_child(unsigned char c) const noexcept {
        int idx = chars.find(c);
        return idx >= 0 ? children[idx].load() : nullptr;
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        int idx = chars.find(c);
        return idx >= 0 ? &children[idx] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        int idx = chars.add(c);
        children[idx].store(child);
    }
    
    void add_two_children(unsigned char c1, ptr_t child1, unsigned char c2, ptr_t child2) {
        chars.add(c1);
        chars.add(c2);
        children[0].store(child1);
        children[1].store(child2);
    }
    
    void remove_child(unsigned char c) {
        int idx = chars.find(c);
        if (idx < 0) return;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = idx; i < cnt - 1; ++i) {
            children[i].store(children[i + 1].load());
        }
        children[cnt - 1].store(nullptr);
        chars.remove_at(idx);
    }
    
    void move_children_to(list_node* dest) {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
            children[i].store(nullptr);
        }
    }
    
    void copy_children_to(list_node* dest) const {
        dest->chars = chars;
        int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
        for (int i = 0; i < cnt; ++i) {
            dest->children[i].store(children[i].load());
        }
    }
    
    void move_interior_to(list_node* dest) {
        dest->eos = std::move(eos);
        move_children_to(dest);
    }
    
    void copy_interior_to(list_node* dest) const {
        dest->eos.deep_copy_from(eos);
        copy_children_to(dest);
    }
    
    void move_interior_to_full(full_node<T, THREADED, Allocator, 0, false>* dest);
    void copy_interior_to_full(full_node<T, THREADED, Allocator, 0, false>* dest) const;
};

// =============================================================================
// FULL_NODE - LEAF specialization (stores values)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct full_node<T, THREADED, Allocator, FIXED_LEN, true> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = typename base_t::data_t;
    
    bitmap256 valid;
    std::array<data_t, 256> values;
    
    full_node() = default;
    ~full_node() = default;
    
    // Unified interface
    int count() const noexcept { return valid.count(); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }  // Non-atomic read
    
    // Caller must verify has(c) first
    bool read_value(unsigned char c, T& out) const noexcept {
        return values[c].try_read(out);
    }
    
    void set_value(unsigned char c, const T& val) {
        values[c].set(val);
        valid.template atomic_set<THREADED>(c);
    }
    
    void add_value(unsigned char c, const T& val) {
        values[c].set(val);
        valid.set(c);
    }
    
    void add_value_atomic(unsigned char c, const T& val) {
        values[c].set(val);
        valid.template atomic_set<THREADED>(c);
    }
    
    void remove_value(unsigned char c) {
        values[c].clear();
        valid.template atomic_clear<THREADED>(c);
    }
    
    void copy_values_to(full_node* dest) const {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->values[c].deep_copy_from(values[c]);
        });
    }
};

// =============================================================================
// FULL_NODE - INTERIOR specialization, FIXED_LEN > 0 (no eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct full_node<T, THREADED, Allocator, FIXED_LEN, false> 
    : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    using base_t = node_with_skip<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    
    bitmap256 valid;
    std::array<atomic_ptr, 256> children;
    
    full_node() = default;
    ~full_node() = default;
    
    // Unified interface
    int count() const noexcept { return valid.count(); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }
    
    ptr_t get_child(unsigned char c) const noexcept {
        return children[c].load();
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        return valid.test(c) ? &children[c] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        children[c].store(child);
        valid.set(c);
    }
    
    void add_child_atomic(unsigned char c, ptr_t child) {
        children[c].store(child);
        valid.template atomic_set<THREADED>(c);
    }
    
    void remove_child(unsigned char c) {
        valid.template atomic_clear<THREADED>(c);
        children[c].store(nullptr);
    }
    
    void move_interior_to(full_node* dest) {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
            children[c].store(nullptr);
        });
    }
    
    void copy_interior_to(full_node* dest) const {
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
        });
    }
};

// =============================================================================
// FULL_NODE - INTERIOR specialization, FIXED_LEN == 0 (has eos)
// =============================================================================

template <typename T, bool THREADED, typename Allocator>
struct full_node<T, THREADED, Allocator, 0, false> 
    : node_with_skip<T, THREADED, Allocator, 0> {
    using base_t = node_with_skip<T, THREADED, Allocator, 0>;
    using ptr_t = typename base_t::ptr_t;
    using atomic_ptr = typename base_t::atomic_ptr;
    using data_t = typename base_t::data_t;
    using eos_data_t = typename base_t::eos_data_t;
    
    eos_data_t eos;
    bitmap256 valid;
    std::array<atomic_ptr, 256> children;
    
    full_node() = default;
    ~full_node() = default;
    
    // Unified interface
    int count() const noexcept { return valid.count(); }
    bool has(unsigned char c) const noexcept { return valid.test(c); }
    
    ptr_t get_child(unsigned char c) const noexcept {
        return children[c].load();
    }
    
    atomic_ptr* get_child_slot(unsigned char c) noexcept {
        return valid.test(c) ? &children[c] : nullptr;
    }
    
    void add_child(unsigned char c, ptr_t child) {
        children[c].store(child);
        valid.set(c);
    }
    
    void add_child_atomic(unsigned char c, ptr_t child) {
        children[c].store(child);
        valid.template atomic_set<THREADED>(c);
    }
    
    void remove_child(unsigned char c) {
        valid.template atomic_clear<THREADED>(c);
        children[c].store(nullptr);
    }
    
    void move_interior_to(full_node* dest) {
        dest->eos = std::move(eos);
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
            children[c].store(nullptr);
        });
    }
    
    void copy_interior_to(full_node* dest) const {
        dest->eos.deep_copy_from(eos);
        dest->valid = valid;
        valid.for_each_set([this, dest](unsigned char c) {
            dest->children[c].store(children[c].load());
        });
    }
};

// Out-of-line definitions for move_interior_to_full
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
void list_node<T, THREADED, Allocator, FIXED_LEN, false>::move_interior_to_full(
    full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) {
    int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars.char_at(i);
        dest->valid.set(ch);
        dest->children[ch].store(children[i].load());
        children[i].store(nullptr);
    }
}

template <typename T, bool THREADED, typename Allocator>
void list_node<T, THREADED, Allocator, 0, false>::move_interior_to_full(
    full_node<T, THREADED, Allocator, 0, false>* dest) {
    dest->eos = std::move(eos);
    int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars.char_at(i);
        dest->valid.set(ch);
        dest->children[ch].store(children[i].load());
        children[i].store(nullptr);
    }
}

// Out-of-line definitions for copy_interior_to_full
template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
void list_node<T, THREADED, Allocator, FIXED_LEN, false>::copy_interior_to_full(
    full_node<T, THREADED, Allocator, FIXED_LEN, false>* dest) const {
    int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars.char_at(i);
        dest->valid.set(ch);
        dest->children[ch].store(children[i].load());
    }
}

template <typename T, bool THREADED, typename Allocator>
void list_node<T, THREADED, Allocator, 0, false>::copy_interior_to_full(
    full_node<T, THREADED, Allocator, 0, false>* dest) const {
    dest->eos.deep_copy_from(eos);
    int cnt = chars.count();
        [[assume(cnt >= 0 && cnt < 8)]];
    for (int i = 0; i < cnt; ++i) {
        unsigned char ch = chars.char_at(i);
        dest->valid.set(ch);
        dest->children[ch].store(children[i].load());
    }
}

// =============================================================================
// RETRY SENTINEL STORAGE - constinit compatible
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct retry_storage : node_with_skip<T, THREADED, Allocator, FIXED_LEN> {
    bitmap256 valid{};
    std::array<void*, 256> dummy_children{};
    
    constexpr retry_storage() noexcept 
        : node_with_skip<T, THREADED, Allocator, FIXED_LEN>(RETRY_SENTINEL_HEADER) {}
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
struct sentinel_holder {
    static constinit retry_storage<T, THREADED, Allocator, FIXED_LEN> retry;
};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
constinit retry_storage<T, THREADED, Allocator, FIXED_LEN> 
    sentinel_holder<T, THREADED, Allocator, FIXED_LEN>::retry{};

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
node_base<T, THREADED, Allocator, FIXED_LEN>* get_retry_sentinel() noexcept {
    return reinterpret_cast<node_base<T, THREADED, Allocator, FIXED_LEN>*>(
        &sentinel_holder<T, THREADED, Allocator, FIXED_LEN>::retry);
}

// =============================================================================
// NODE_BUILDER - allocation and type-safe construction
// =============================================================================

template <typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
class node_builder {
public:
    using base_t = node_base<T, THREADED, Allocator, FIXED_LEN>;
    using ptr_t = base_t*;
    using skip_t = skip_node<T, THREADED, Allocator, FIXED_LEN>;
    using leaf_list_t = list_node<T, THREADED, Allocator, FIXED_LEN, true>;
    using interior_list_t = list_node<T, THREADED, Allocator, FIXED_LEN, false>;
    using leaf_full_t = full_node<T, THREADED, Allocator, FIXED_LEN, true>;
    using interior_full_t = full_node<T, THREADED, Allocator, FIXED_LEN, false>;
    
    static constexpr bool is_retry_sentinel(ptr_t n) noexcept {
        if constexpr (!THREADED) {
            (void)n;
            return false;
        } else {
            return n == get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>();
        }
    }
    
    // is_sentinel is now just is_retry_sentinel (no more not_found sentinel)
    static constexpr bool is_sentinel(ptr_t n) noexcept {
        return is_retry_sentinel(n);
    }
    
    static void delete_node(ptr_t n) {
        if (!n || is_sentinel(n)) return;
        if (n->is_skip()) {
            delete n->as_skip();
        } else if (n->is_list()) [[likely]] {
            if (n->is_leaf()) delete n->template as_list<true>();
            else delete n->template as_list<false>();
        } else {
            if (n->is_leaf()) delete n->template as_full<true>();
            else delete n->template as_full<false>();
        }
    }
    
    ptr_t make_leaf_skip(std::string_view sk, const T& value) {
        auto* n = new skip_t();
        n->set_header(make_header(true, FLAG_SKIP));
        n->skip.assign(sk);
        n->value.set(value);
        return n;
    }
    
    ptr_t make_leaf_list(std::string_view sk) {
        auto* n = new leaf_list_t();
        n->set_header(make_header(true, FLAG_LIST));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_leaf_full(std::string_view sk) {
        auto* n = new leaf_full_t();
        n->set_header(make_header(true, 0));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_list(std::string_view sk) {
        auto* n = new interior_list_t();
        n->set_header(make_header(false, FLAG_LIST));
        n->skip.assign(sk);
        return n;
    }
    
    ptr_t make_interior_full(std::string_view sk) {
        auto* n = new interior_full_t();
        n->set_header(make_header(false, 0));
        n->skip.assign(sk);
        return n;
    }
    
    void dealloc_node(ptr_t n) {
        if (!n || is_sentinel(n)) return;
        
        // If poisoned, this is a speculative node with borrowed children - don't recurse
        if (n->is_poisoned()) {
            delete_node(n);
            return;
        }
        
        if (!n->is_leaf()) {
            if (n->is_list()) [[likely]] {
                auto* ln = n->template as_list<false>();
                int cnt = ln->count();
                for (int i = 0; i < cnt; ++i) {
                    dealloc_node(ln->children[i].load());
                }
            } else {
                auto* fn = n->template as_full<false>();
                fn->valid.for_each_set([this, fn](unsigned char c) {
                    dealloc_node(fn->children[c].load());
                });
            }
        }
        delete_node(n);
    }
    
    ptr_t deep_copy(ptr_t src) {
        if (!src || is_sentinel(src)) return nullptr;
        
        if (src->is_leaf()) {
            if (src->is_skip()) {
                auto* s = src->as_skip();
                auto* d = new skip_t();
                d->set_header(s->header());
                d->skip = s->skip;
                d->value.deep_copy_from(s->value);
                return d;
            }
            if (src->is_list()) [[likely]] {
                auto* s = src->template as_list<true>();
                auto* d = new leaf_list_t();
                d->set_header(s->header());
                d->skip = s->skip;
                s->copy_values_to(d);
                return d;
            }
            auto* s = src->template as_full<true>();
            auto* d = new leaf_full_t();
            d->set_header(s->header());
            d->skip = s->skip;
            s->copy_values_to(d);
            return d;
        }
        
        // Interior
        if (src->is_list()) [[likely]] {
            auto* s = src->template as_list<false>();
            auto* d = new interior_list_t();
            d->set_header(s->header());
            d->skip = s->skip;
            d->chars = s->chars;
            if constexpr (FIXED_LEN == 0) {
                d->eos.deep_copy_from(s->eos);
            }
            int cnt = s->count();
            for (int i = 0; i < cnt; ++i) {
                d->children[i].store(deep_copy(s->children[i].load()));
            }
            return d;
        }
        auto* s = src->template as_full<false>();
        auto* d = new interior_full_t();
        d->set_header(s->header());
        d->skip = s->skip;
        d->valid = s->valid;
        if constexpr (FIXED_LEN == 0) {
            d->eos.deep_copy_from(s->eos);
        }
        s->valid.for_each_set([this, s, d](unsigned char c) {
            d->children[c].store(deep_copy(s->children[c].load()));
        });
        return d;
    }
};

}  // namespace gteitelbaum

// === tktrie_ebr.h ===

// =============================================================================
// TKTRIE EBR - Epoch-Based Reclamation (fully per-trie, no globals)
// =============================================================================
//
// Each tktrie instance has its own:
//   - epoch_ : incremented on writes
//   - reader_epochs_[MAX_SLOTS] : tracks active reader epochs (0 = inactive)
//   - retired list : nodes waiting to be freed
//
// Reader protocol:
//   enter() - hash thread to slot, store current epoch
//   exit()  - clear slot (store 0)
//
// Reclamation:
//   Compute min_reader_epoch across slots, free nodes retired before that
//
// Slot collisions (two threads hash to same slot):
//   Conservative - we see older epoch, delay reclamation. Safe.
//
// =============================================================================

#include <atomic>
#include <thread>

namespace gteitelbaum {

// Hash thread ID to slot index
inline size_t thread_slot_hash(size_t max_slots) noexcept {
    std::hash<std::thread::id> hasher;
    return hasher(std::this_thread::get_id()) % max_slots;
}

}  // namespace gteitelbaum

// === tktrie.h ===

#include <cstring>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>


namespace gteitelbaum {

// =============================================================================
// KEY TRAITS - provides to_bytes, from_bytes, and FIXED_LEN
// =============================================================================

template <typename Key> struct tktrie_traits;

template <>
struct tktrie_traits<std::string> {
    static constexpr size_t FIXED_LEN = 0;  // Variable length
    static std::string_view to_bytes(const std::string& k) noexcept { return k; }
    static std::string from_bytes(std::string_view b) { return std::string(b); }
};

template <typename T> requires std::is_integral_v<T>
struct tktrie_traits<T> {
    static constexpr size_t FIXED_LEN = sizeof(T);  // Fixed length
    using unsigned_t = std::make_unsigned_t<T>;
    using bytes_t = std::array<char, sizeof(T)>;
    
    static bytes_t to_bytes(T k) noexcept {
        unsigned_t sortable;
        if constexpr (std::is_signed_v<T>)
            sortable = static_cast<unsigned_t>(k) ^ (unsigned_t{1} << (sizeof(T) * 8 - 1));
        else
            sortable = k;
        unsigned_t be = to_big_endian(sortable);
        bytes_t result;
        std::memcpy(result.data(), &be, sizeof(T));
        return result;
    }
    static T from_bytes(std::string_view b) {
        unsigned_t be;
        std::memcpy(&be, b.data(), sizeof(T));
        unsigned_t sortable = from_big_endian(be);
        if constexpr (std::is_signed_v<T>)
            return static_cast<T>(sortable ^ (unsigned_t{1} << (sizeof(T) * 8 - 1)));
        else
            return static_cast<T>(sortable);
    }
};

// Forward declarations
template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie_iterator;

// =============================================================================
// TKTRIE CLASS DECLARATION
// =============================================================================

template <typename Key, typename T, bool THREADED = false, typename Allocator = std::allocator<uint64_t>>
class tktrie {
public:
    using traits = tktrie_traits<Key>;
    static constexpr size_t FIXED_LEN = traits::FIXED_LEN;
    
    using ptr_t = node_base<T, THREADED, Allocator, FIXED_LEN>*;
    using atomic_ptr = atomic_node_ptr<T, THREADED, Allocator, FIXED_LEN>;
    using builder_t = node_builder<T, THREADED, Allocator, FIXED_LEN>;
    using skip_t = skip_node<T, THREADED, Allocator, FIXED_LEN>;
    using data_t = dataptr<T, THREADED, Allocator>;
    using iterator = tktrie_iterator<Key, T, THREADED, Allocator>;
    using mutex_t = std::conditional_t<THREADED, std::mutex, empty_mutex>;

    // -------------------------------------------------------------------------
    // Result types
    // -------------------------------------------------------------------------
    
    // Small fixed-capacity list for retired nodes (avoids heap allocation)
    // Max 4: typical is 1-2, worst case split/collapse is 3
    struct retired_list {
        ptr_t nodes[4];  // Not initialized - only count elements are valid
        uint8_t count = 0;
        
        void push_back(ptr_t n) noexcept { nodes[count++] = n; }
        bool empty() const noexcept { return count == 0; }
        ptr_t* begin() noexcept { return nodes; }
        ptr_t* end() noexcept { return nodes + count; }
    };
    
    struct insert_result {
        ptr_t new_node = nullptr;
        retired_list old_nodes;
        bool inserted = false;
        bool in_place = false;
    };

    struct erase_result {
        ptr_t new_node = nullptr;
        retired_list old_nodes;
        bool erased = false;
        bool deleted_subtree = false;
    };

    struct path_entry {
        ptr_t node;
        uint64_t version;
        unsigned char edge;
    };

    // -------------------------------------------------------------------------
    // Optimistic read types (for lock-free reads)
    // -------------------------------------------------------------------------
    struct read_path {
        static constexpr int MAX_DEPTH = 64;
        std::array<ptr_t, MAX_DEPTH> nodes{};
        std::array<uint64_t, MAX_DEPTH> versions{};
        int len = 0;
        
        void clear() noexcept { len = 0; }
        
        // Original push (for non-THREADED or when poison already checked)
        bool push(ptr_t n) noexcept {
            if (len >= MAX_DEPTH) return false;
            nodes[len] = n;
            versions[len] = n->version();
            ++len;
            return true;
        }
        
        // Combined push + poison check (single header load)
        bool push_checked(ptr_t n) noexcept {
            if (len >= MAX_DEPTH) return false;
            uint64_t h = n->header();  // Single atomic load
            if (is_poisoned_header(h)) return false;
            nodes[len] = n;
            versions[len] = get_version(h);
            ++len;
            return true;
        }
    };

    // -------------------------------------------------------------------------
    // Speculative insert types
    // -------------------------------------------------------------------------
    enum class spec_op {
        EXISTS, RETRY,  // RETRY = need to re-probe (concurrent write detected)
        IN_PLACE_LEAF, IN_PLACE_INTERIOR, EMPTY_TREE,
        SPLIT_LEAF_SKIP, PREFIX_LEAF_SKIP, EXTEND_LEAF_SKIP,
        SPLIT_LEAF_LIST, PREFIX_LEAF_LIST, ADD_EOS_LEAF_LIST, LIST_TO_FULL_LEAF,
        DEMOTE_LEAF_LIST, SPLIT_INTERIOR, PREFIX_INTERIOR, ADD_CHILD_CONVERT,
    };

    struct speculative_info {
        static constexpr int MAX_PATH = 64;
        std::array<path_entry, MAX_PATH> path{};
        int path_len = 0;
        spec_op op = spec_op::EXISTS;
        ptr_t target = nullptr;
        uint64_t target_version = 0;
        unsigned char c = 0;
        bool is_eos = false;
        size_t match_pos = 0;
        std::string target_skip;
        std::string remaining_key;
    };

    struct pre_alloc {
        ptr_t nodes[8];  // Not initialized
        int count = 0;
        ptr_t root_replacement = nullptr;
        void add(ptr_t n) { nodes[count++] = n; }
    };
    
#ifdef TKTRIE_INSTRUMENT_RETRIES
    struct retry_stats {
        std::atomic<uint64_t> speculative_attempts{0};
        std::atomic<uint64_t> speculative_successes{0};
        std::atomic<uint64_t> retries[8]{};  // retries[i] = count that needed i retries
        std::atomic<uint64_t> fallbacks{0};  // exceeded max retries
    };
    static retry_stats& get_retry_stats() {
        static retry_stats stats;
        return stats;
    }
    static void stat_attempt() { get_retry_stats().speculative_attempts.fetch_add(1, std::memory_order_relaxed); }
    static void stat_success(int r) { 
        get_retry_stats().speculative_successes.fetch_add(1, std::memory_order_relaxed);
        if (r < 8) get_retry_stats().retries[r].fetch_add(1, std::memory_order_relaxed);
    }
    static void stat_fallback() { get_retry_stats().fallbacks.fetch_add(1, std::memory_order_relaxed); }
#else
    static void stat_attempt() {}
    static void stat_success(int) {}
    static void stat_fallback() {}
#endif

    // -------------------------------------------------------------------------
    // Speculative erase types
    // -------------------------------------------------------------------------
    enum class erase_op {
        NOT_FOUND,
        // In-place operations (no structural change)
        IN_PLACE_LEAF_LIST, IN_PLACE_LEAF_FULL,
        // Structural operations
        DELETE_SKIP_LEAF,           // Delete entire SKIP leaf
        DELETE_LAST_LEAF_ENTRY,     // Delete last entry from LIST/FULL leaf  
        DELETE_EOS_INTERIOR,        // Remove EOS from interior (may collapse)
        DELETE_CHILD_COLLAPSE,      // Remove child and collapse to merged node
        DELETE_CHILD_NO_COLLAPSE,   // Remove child, no collapse needed
    };

    struct erase_spec_info {
        static constexpr int MAX_PATH = 64;
        std::array<path_entry, MAX_PATH> path{};
        int path_len = 0;
        erase_op op = erase_op::NOT_FOUND;
        ptr_t target = nullptr;
        uint64_t target_version = 0;
        unsigned char c = 0;
        bool is_eos = false;
        // For collapse operations
        ptr_t collapse_child = nullptr;
        unsigned char collapse_char = 0;
        std::string target_skip;
        std::string child_skip;
    };

    struct erase_pre_alloc {
        ptr_t nodes[4];
        int count = 0;
        ptr_t replacement = nullptr;
        void add(ptr_t n) { nodes[count++] = n; }
    };

    // -------------------------------------------------------------------------
    // Per-trie EBR - epoch-based reclamation with per-trie reader tracking
    // No global state - each trie manages its own readers and retired nodes
    // -------------------------------------------------------------------------
    static constexpr size_t EBR_MIN_RETIRED = 64;      // Writers cleanup at this threshold

private:
    atomic_ptr root_;
    atomic_counter<THREADED> size_;
    mutable mutex_t mutex_;
    builder_t builder_;
    
    // Epoch counter: bumped on writes, used for read validation AND EBR
    alignas(64) std::conditional_t<THREADED, std::atomic<uint64_t>, uint64_t> epoch_{1};
    
    // Per-trie reader tracking with cache-line padding to prevent false sharing
    // Each slot is 64 bytes to ensure no two threads share a cache line
    // Using 16 slots = 1KB per trie (reasonable memory, good coverage)
    static constexpr size_t EBR_PADDED_SLOTS = 16;
    
    struct alignas(64) PaddedReaderSlot {
        std::atomic<uint64_t> epoch{0};  // 0 = inactive
    };
    
    std::conditional_t<THREADED, 
        std::array<PaddedReaderSlot, EBR_PADDED_SLOTS>,
        std::array<uint64_t, 1>> reader_epochs_{};
    
    // Lock-free retired list using embedded fields in nodes (MPSC)
    std::conditional_t<THREADED, std::atomic<ptr_t>, ptr_t> retired_head_{nullptr};
    std::conditional_t<THREADED, std::atomic<size_t>, size_t> retired_count_{0};
    mutable std::conditional_t<THREADED, std::mutex, empty_mutex> ebr_mutex_;  // Only for cleanup
    
    // EBR helpers
    void ebr_retire(ptr_t n, uint64_t epoch);      // Lock-free push
    void ebr_cleanup();                             // Free reclaimable nodes (grabs ebr_mutex_)
    uint64_t min_reader_epoch() const noexcept;    // Scan slots for oldest active reader
    void reader_enter() const noexcept;            // Store epoch in thread's slot
    void reader_exit() const noexcept;             // Clear thread's slot

    // -------------------------------------------------------------------------
    // Static helpers
    // -------------------------------------------------------------------------
    static void node_deleter(void* ptr);

    // -------------------------------------------------------------------------
    // Instance helpers
    // -------------------------------------------------------------------------
    void retire_node(ptr_t n);

    // -------------------------------------------------------------------------
    // Read operations
    // -------------------------------------------------------------------------
    template <bool NEED_VALUE>
    bool read_impl(ptr_t n, std::string_view key, T& out) const noexcept
        requires NEED_VALUE;
    
    template <bool NEED_VALUE>
    bool read_impl(ptr_t n, std::string_view key) const noexcept
        requires (!NEED_VALUE);
    
    template <bool NEED_VALUE>
    bool read_impl_optimistic(ptr_t n, std::string_view key, T& out, read_path& path) const noexcept
        requires NEED_VALUE;
    
    template <bool NEED_VALUE>
    bool read_impl_optimistic(ptr_t n, std::string_view key, read_path& path) const noexcept
        requires (!NEED_VALUE);
    
    bool validate_read_path(const read_path& path) const noexcept;

    // -------------------------------------------------------------------------
    // Insert operations
    // -------------------------------------------------------------------------
    insert_result insert_impl(atomic_ptr* slot, ptr_t n, std::string_view key, const T& value);
    insert_result insert_into_leaf(atomic_ptr* slot, ptr_t leaf, std::string_view key, const T& value);
    insert_result insert_into_interior(atomic_ptr* slot, ptr_t n, std::string_view key, const T& value);
    ptr_t create_leaf_for_key(std::string_view key, const T& value);
    insert_result split_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m);
    insert_result prefix_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m);
    insert_result extend_leaf_skip(ptr_t leaf, std::string_view key, const T& value, size_t m);
    insert_result split_leaf_list(ptr_t leaf, std::string_view key, const T& value, size_t m);
    insert_result prefix_leaf_list(ptr_t leaf, std::string_view key, const T& value, size_t m);
    ptr_t clone_leaf_with_skip(ptr_t leaf, std::string_view new_skip);
    insert_result add_eos_to_leaf_list(ptr_t leaf, const T& value);
    insert_result add_char_to_leaf(ptr_t leaf, unsigned char c, const T& value);
    insert_result demote_leaf_list(ptr_t leaf, std::string_view key, const T& value);
    insert_result split_interior(ptr_t n, std::string_view key, const T& value, size_t m);
    ptr_t clone_interior_with_skip(ptr_t n, std::string_view new_skip);
    insert_result prefix_interior(ptr_t n, std::string_view key, const T& value, size_t m);
    insert_result set_interior_eos(ptr_t n, const T& value);
    insert_result add_child_to_interior(ptr_t n, unsigned char c, std::string_view remaining, const T& value);

    // -------------------------------------------------------------------------
    // Speculative insert operations
    // -------------------------------------------------------------------------
    speculative_info probe_speculative(ptr_t n, std::string_view key) const noexcept;
    speculative_info probe_leaf_speculative(ptr_t n, std::string_view key, speculative_info& info) const noexcept;
    pre_alloc allocate_speculative(const speculative_info& info, const T& value);
    bool validate_path(const speculative_info& info) const noexcept;
    atomic_ptr* find_slot_for_commit(const speculative_info& info) noexcept;
    atomic_ptr* get_verified_slot(const speculative_info& info) noexcept;
    void commit_to_slot(atomic_ptr* slot, ptr_t new_node, const speculative_info& info) noexcept;
    bool commit_speculative(speculative_info& info, pre_alloc& alloc, const T& value);
    void dealloc_speculation(pre_alloc& alloc);
    std::pair<iterator, bool> insert_locked(const Key& key, std::string_view kb, const T& value, bool* retired_any);

    // -------------------------------------------------------------------------
    // Erase operations
    // -------------------------------------------------------------------------
    erase_spec_info probe_erase(ptr_t n, std::string_view key) const noexcept;
    erase_spec_info probe_leaf_erase(ptr_t n, std::string_view key, erase_spec_info& info) const noexcept;
    erase_spec_info probe_interior_erase(ptr_t n, std::string_view key, erase_spec_info& info) const noexcept;
    bool do_inplace_leaf_list_erase(ptr_t leaf, unsigned char c, uint64_t expected_version);
    bool do_inplace_leaf_full_erase(ptr_t leaf, unsigned char c, uint64_t expected_version);
    erase_pre_alloc allocate_erase_speculative(const erase_spec_info& info);
    bool validate_erase_path(const erase_spec_info& info) const noexcept;
    bool commit_erase_speculative(erase_spec_info& info, erase_pre_alloc& alloc);
    void dealloc_erase_speculation(erase_pre_alloc& alloc);
    std::pair<bool, bool> erase_locked(std::string_view kb);  // Returns (erased, retired_any)
    erase_result erase_impl(atomic_ptr* slot, ptr_t n, std::string_view key);
    erase_result erase_from_leaf(ptr_t leaf, std::string_view key);
    erase_result erase_from_interior(ptr_t n, std::string_view key);
    erase_result try_collapse_interior(ptr_t n);
    erase_result try_collapse_after_child_removal(ptr_t n, unsigned char removed_c, erase_result& child_res);
    erase_result collapse_single_child(ptr_t n, unsigned char c, ptr_t child, erase_result& res);

public:
    // -------------------------------------------------------------------------
    // Constructors / Destructor
    // -------------------------------------------------------------------------
    tktrie();
    ~tktrie();
    tktrie(const tktrie& other);
    tktrie& operator=(const tktrie& other);
    tktrie(tktrie&& other) noexcept;
    tktrie& operator=(tktrie&& other) noexcept;

    // -------------------------------------------------------------------------
    // Public interface
    // -------------------------------------------------------------------------
    void clear();
    size_t size() const noexcept { return size_.load(); }
    bool empty() const noexcept { return size() == 0; }
    bool contains(const Key& key) const;
    std::pair<iterator, bool> insert(const std::pair<const Key, T>& kv);
    bool erase(const Key& key);
    iterator find(const Key& key) const;
    iterator end() const noexcept { return iterator(); }
    void reclaim_retired() noexcept;
    
    // For diagnostics/testing only
    ptr_t test_root() const noexcept { return root_.load(); }
};

// =============================================================================
// TKTRIE_ITERATOR CLASS
// =============================================================================

template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie_iterator {
public:
    using trie_t = tktrie<Key, T, THREADED, Allocator>;
    using traits = tktrie_traits<Key>;
    static constexpr size_t FIXED_LEN = traits::FIXED_LEN;
    
    // Store bytes: array for fixed-length, string for variable-length
    // We already converted for lookup - just keep them
    using key_storage_t = std::conditional_t<(FIXED_LEN > 0),
        std::array<char, FIXED_LEN>,
        std::string>;

private:
    const trie_t* trie_ = nullptr;
    key_storage_t key_bytes_{};
    T value_{};
    bool valid_ = false;

public:
    tktrie_iterator() = default;
    
    // Constructor from string_view - stores the already-converted bytes
    tktrie_iterator(const trie_t* t, std::string_view kb, const T& v)
        : trie_(t), value_(v), valid_(true) {
        if constexpr (FIXED_LEN > 0) {
            std::memcpy(key_bytes_.data(), kb.data(), FIXED_LEN);
        } else {
            key_bytes_ = std::string(kb);
        }
    }

    // Convert bytes back to Key only when requested
    Key key() const { 
        if constexpr (FIXED_LEN > 0) {
            return traits::from_bytes(std::string_view(key_bytes_.data(), FIXED_LEN));
        } else {
            return traits::from_bytes(key_bytes_);
        }
    }
    const T& value() const { return value_; }
    bool valid() const { return valid_; }
    explicit operator bool() const { return valid_; }

    bool operator==(const tktrie_iterator& o) const {
        if ((!valid_) & (!o.valid_)) return true;
        return (valid_ == o.valid_) & (key_bytes_ == o.key_bytes_);
    }
    bool operator!=(const tktrie_iterator& o) const { return !(*this == o); }
};

// =============================================================================
// TYPE ALIASES
// =============================================================================

template <typename T, typename Allocator = std::allocator<uint64_t>>
using string_trie = tktrie<std::string, T, false, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_string_trie = tktrie<std::string, T, true, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using int32_trie = tktrie<int32_t, T, false, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_int32_trie = tktrie<int32_t, T, true, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using int64_trie = tktrie<int64_t, T, false, Allocator>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_int64_trie = tktrie<int64_t, T, true, Allocator>;

}  // namespace gteitelbaum


// === tktrie_core.h ===

// This file contains implementation details for tktrie
// It should only be included from tktrie.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

// -----------------------------------------------------------------------------
// Static helpers
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
void TKTRIE_CLASS::node_deleter(void* ptr) {
    if (!ptr) return;
    auto* n = static_cast<ptr_t>(ptr);
    if (builder_t::is_sentinel(n)) return;
    builder_t::delete_node(n);
}

// -----------------------------------------------------------------------------
// Instance helpers
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
void TKTRIE_CLASS::retire_node(ptr_t n) {
    if (!n || builder_t::is_sentinel(n)) return;
    if constexpr (THREADED) {
        n->poison();
        uint64_t epoch = epoch_.load(std::memory_order_acquire);
        ebr_retire(n, epoch);  // Lock-free
    } else {
        node_deleter(n);
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::ebr_retire(ptr_t n, uint64_t epoch) {
    // Lock-free push using embedded fields in node
    if constexpr (THREADED) {
        n->retire_epoch_ = epoch;
        ptr_t old_head = retired_head_.load(std::memory_order_relaxed);
        do {
            n->retire_next_ = old_head;
        } while (!retired_head_.compare_exchange_weak(old_head, n,
                    std::memory_order_release, std::memory_order_relaxed));
        retired_count_.fetch_add(1, std::memory_order_relaxed);
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::reader_enter() const noexcept {
    if constexpr (THREADED) {
        size_t slot = thread_slot_hash(EBR_PADDED_SLOTS);
        uint64_t epoch = epoch_.load(std::memory_order_acquire);
        const_cast<PaddedReaderSlot&>(reader_epochs_[slot])
            .epoch.store(epoch, std::memory_order_release);
    }
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::reader_exit() const noexcept {
    if constexpr (THREADED) {
        size_t slot = thread_slot_hash(EBR_PADDED_SLOTS);
        const_cast<PaddedReaderSlot&>(reader_epochs_[slot])
            .epoch.store(0, std::memory_order_release);
    }
}

TKTRIE_TEMPLATE
uint64_t TKTRIE_CLASS::min_reader_epoch() const noexcept {
    if constexpr (THREADED) {
        uint64_t current = epoch_.load(std::memory_order_acquire);
        uint64_t min_e = current;
        for (size_t i = 0; i < EBR_PADDED_SLOTS; ++i) {
            uint64_t e = reader_epochs_[i].epoch.load(std::memory_order_acquire);
            if (e != 0 && e < min_e) {
                min_e = e;
            }
        }
        return min_e;
    }
    return 0;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::ebr_cleanup() {
    // Grab mutex, atomically take list, process, push back unreclaimable
    if constexpr (THREADED) {
        std::lock_guard<std::mutex> lock(ebr_mutex_);
        
        // Atomically take ownership of entire retired list
        ptr_t list = retired_head_.exchange(nullptr, std::memory_order_acquire);
        retired_count_.store(0, std::memory_order_relaxed);
        if (!list) return;
        
        uint64_t min_epoch = min_reader_epoch();
        
        // Partition into freeable and still-retired
        ptr_t still_head = nullptr;
        size_t still_count = 0;
        
        while (list) {
            ptr_t curr = list;
            list = list->retire_next_;
            
            // Safe if retired at least 2 epochs before min reader epoch
            if (curr->retire_epoch_ + 2 <= min_epoch) {
                node_deleter(curr);
            } else {
                // Still needs protection - prepend to still list
                curr->retire_next_ = still_head;
                still_head = curr;
                ++still_count;
            }
        }
        
        // Push still-retired back (other threads may have added more)
        if (still_head) {
            // Find tail of still list
            ptr_t still_tail = still_head;
            while (still_tail->retire_next_) still_tail = still_tail->retire_next_;
            
            ptr_t old_head = retired_head_.load(std::memory_order_relaxed);
            do {
                still_tail->retire_next_ = old_head;
            } while (!retired_head_.compare_exchange_weak(old_head, still_head,
                        std::memory_order_release, std::memory_order_relaxed));
            retired_count_.fetch_add(still_count, std::memory_order_relaxed);
        }
    }
}

// -----------------------------------------------------------------------------
// Read operations
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl(ptr_t n, std::string_view key, T& out) const noexcept
    requires NEED_VALUE {
    if constexpr (THREADED) {
        if (!n || n->is_poisoned()) return false;
    } else {
        if (!n) return false;
    }
    
    // Unified loop: consume skip for every node (interior or leaf)
    while (true) {
        if (!consume_prefix(key, n->skip_str())) return false;
        
        if (n->is_leaf()) break;
        
        // Interior node: check EOS or descend
        if (key.empty()) {
            return n->try_read_eos(out);
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        n = n->get_child(c);
        
        if constexpr (THREADED) {
            if (!n || n->is_poisoned()) return false;
        } else {
            if (!n) return false;
        }
    }
    
    // Leaf node: skip already consumed, poison already checked
    if (n->is_skip()) {
        if (!key.empty()) return false;
        return n->as_skip()->value.try_read(out);
    }
    
    // LIST or FULL leaf - need exactly 1 char remaining
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        int idx = ln->find(c);
        if (idx < 0) return false;
        return ln->read_value(idx, out);
    }
    auto* fn = n->template as_full<true>();
    if (!fn->has(c)) return false;
    return fn->read_value(c, out);
}

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl(ptr_t n, std::string_view key) const noexcept
    requires (!NEED_VALUE) {
    if constexpr (THREADED) {
        if (!n || n->is_poisoned()) return false;
    } else {
        if (!n) return false;
    }
    
    // Unified loop: consume skip for every node (interior or leaf)
    while (true) {
        if (!consume_prefix(key, n->skip_str())) return false;
        
        if (n->is_leaf()) break;
        
        // Interior node: check EOS or descend
        if (key.empty()) {
            return n->has_eos();
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        n = n->get_child(c);
        
        if constexpr (THREADED) {
            if (!n || n->is_poisoned()) return false;
        } else {
            if (!n) return false;
        }
    }
    
    // Leaf node: skip already consumed, poison already checked
    if (n->is_skip()) {
        return key.empty();
    }
    
    // LIST or FULL leaf - need exactly 1 char remaining
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_list()) [[likely]] {
        return n->template as_list<true>()->has(c);
    }
    return n->template as_full<true>()->has(c);
}

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl_optimistic(ptr_t n, std::string_view key, T& out, read_path& path) const noexcept
    requires NEED_VALUE {
    if (!n) return false;
    if (!path.push_checked(n)) return false;  // Single header load for poison + version
    
    // Unified loop: consume skip for every node
    while (true) {
        if (!consume_prefix(key, n->skip_str())) return false;
        
        if (n->is_leaf()) break;
        
        // Interior node: check EOS or descend
        if (key.empty()) {
            return n->try_read_eos(out);
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        n = n->get_child(c);
        
        if (!n) return false;
        if (!path.push_checked(n)) return false;  // Single header load
    }
    
    // Leaf node: skip already consumed, poison already checked in push_checked
    if (n->is_skip()) {
        if (!key.empty()) return false;
        return n->as_skip()->value.try_read(out);
    }
    
    // LIST or FULL leaf
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        int idx = ln->find(c);
        if (idx < 0) return false;
        return ln->read_value(idx, out);
    }
    auto* fn = n->template as_full<true>();
    if (!fn->has(c)) return false;
    return fn->read_value(c, out);
}

TKTRIE_TEMPLATE
template <bool NEED_VALUE>
inline bool TKTRIE_CLASS::read_impl_optimistic(ptr_t n, std::string_view key, read_path& path) const noexcept
    requires (!NEED_VALUE) {
    if (!n) return false;
    if (!path.push_checked(n)) return false;  // Single header load for poison + version
    
    // Unified loop: consume skip for every node
    while (true) {
        if (!consume_prefix(key, n->skip_str())) return false;
        
        if (n->is_leaf()) break;
        
        // Interior node: check EOS or descend
        if (key.empty()) {
            return n->has_eos();
        }
        
        unsigned char c = static_cast<unsigned char>(key[0]);
        key.remove_prefix(1);
        n = n->get_child(c);
        
        if (!n) return false;
        if (!path.push_checked(n)) return false;  // Single header load
    }
    
    // Leaf node: skip already consumed, poison already checked in push_checked
    if (n->is_skip()) {
        return key.empty();
    }
    
    // LIST or FULL leaf
    if (key.size() != 1) return false;
    unsigned char c = static_cast<unsigned char>(key[0]);
    
    if (n->is_list()) [[likely]] {
        return n->template as_list<true>()->has(c);
    }
    return n->template as_full<true>()->has(c);
}

TKTRIE_TEMPLATE
inline bool TKTRIE_CLASS::validate_read_path(const read_path& path) const noexcept {
    [[assume(path.len >= 0 && path.len <= 64)]];
    // Version check is sufficient - poison() bumps version
    for (int i = 0; i < path.len; ++i) {
        if (get_version(path.nodes[i]->header()) != path.versions[i]) {
            return false;
        }
    }
    return true;
}

// -----------------------------------------------------------------------------
// Public interface
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
TKTRIE_CLASS::tktrie() : root_(nullptr) {}

TKTRIE_TEMPLATE
TKTRIE_CLASS::~tktrie() { clear(); }

TKTRIE_TEMPLATE
TKTRIE_CLASS::tktrie(const tktrie& other) : root_(nullptr) {
    ptr_t other_root = other.root_.load();
    if (other_root && !builder_t::is_sentinel(other_root)) {
        root_.store(builder_.deep_copy(other_root));
    }
    size_.store(other.size_.load());
}

TKTRIE_TEMPLATE
TKTRIE_CLASS& TKTRIE_CLASS::operator=(const tktrie& other) {
    if (this != &other) {
        clear();
        ptr_t other_root = other.root_.load();
        if (other_root && !builder_t::is_sentinel(other_root)) {
            root_.store(builder_.deep_copy(other_root));
        }
        size_.store(other.size_.load());
    }
    return *this;
}

TKTRIE_TEMPLATE
TKTRIE_CLASS::tktrie(tktrie&& other) noexcept : root_(nullptr) {
    root_.store(other.root_.load());
    other.root_.store(nullptr);
    size_.store(other.size_.exchange(0));
}

TKTRIE_TEMPLATE
TKTRIE_CLASS& TKTRIE_CLASS::operator=(tktrie&& other) noexcept {
    if (this != &other) {
        clear();
        root_.store(other.root_.load());
        other.root_.store(nullptr);
        size_.store(other.size_.exchange(0));
    }
    return *this;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::clear() {
    ptr_t r = root_.load();
    root_.store(nullptr);
    if (r && !builder_t::is_sentinel(r)) {
        builder_.dealloc_node(r);
    }
    size_.store(0);
    if constexpr (THREADED) {
        // Drain and delete entire retired list
        std::lock_guard<std::mutex> lock(ebr_mutex_);
        ptr_t list = retired_head_.exchange(nullptr, std::memory_order_acquire);
        retired_count_.store(0, std::memory_order_relaxed);
        while (list) {
            ptr_t curr = list;
            list = list->retire_next_;
            node_deleter(curr);
        }
    }
}

TKTRIE_TEMPLATE
inline bool TKTRIE_CLASS::contains(const Key& key) const {
    auto kb = traits::to_bytes(key);
    std::string_view kbv(kb.data(), kb.size());
    if constexpr (THREADED) {
        // Readers cleanup at 2x threshold (backstop only)
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED * 2) {
            const_cast<tktrie*>(this)->ebr_cleanup();
        }
        
        reader_enter();
        
        for (int attempts = 0; attempts < 10; ++attempts) {
            // Fast path: check epoch before and after traversal
            uint64_t epoch_before = epoch_.load(std::memory_order_acquire);
            
            ptr_t root = root_.load();
            if (!root) {
                reader_exit();
                return false;
            }
            
            // Must check poison on root to catch sentinel (infinite loop otherwise)
            if (root->is_poisoned()) continue;
            
            // Simple traversal without path recording
            bool found = read_impl<false>(root, kbv);
            
            // Validate: if epoch unchanged, result is valid
            uint64_t epoch_after = epoch_.load(std::memory_order_acquire);
            if (epoch_before == epoch_after) {
                reader_exit();
                return found;
            }
            // Write happened during traversal, retry
        }
        // Fallback: too many retries, use locked read
        bool result = read_impl<false>(root_.load(), kbv);
        reader_exit();
        return result;
    } else {
        return read_impl<false>(root_.load(), kbv);
    }
}

TKTRIE_TEMPLATE
std::pair<typename TKTRIE_CLASS::iterator, bool> TKTRIE_CLASS::insert(const std::pair<const Key, T>& kv) {
    auto kb = traits::to_bytes(kv.first);
    std::string_view kbv(kb.data(), kb.size());
    bool retired_any = false;
    auto result = insert_locked(kv.first, kbv, kv.second, &retired_any);
    if constexpr (THREADED) {
        if (retired_any) {
            epoch_.fetch_add(1, std::memory_order_acq_rel);
        }
        // Writers cleanup at 1x threshold
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED) {
            ebr_cleanup();
        }
    }
    return result;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::erase(const Key& key) {
    auto kb = traits::to_bytes(key);
    std::string_view kbv(kb.data(), kb.size());
    auto [erased, retired_any] = erase_locked(kbv);
    if constexpr (THREADED) {
        if (retired_any) {
            epoch_.fetch_add(1, std::memory_order_acq_rel);
        }
        // Writers cleanup at 1x threshold
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED) {
            ebr_cleanup();
        }
    }
    return erased;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::iterator TKTRIE_CLASS::find(const Key& key) const {
    auto kb = traits::to_bytes(key);
    std::string_view kbv(kb.data(), kb.size());
    T value;
    if constexpr (THREADED) {
        // Readers cleanup at 2x threshold (backstop only)
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED * 2) {
            const_cast<tktrie*>(this)->ebr_cleanup();
        }
        
        reader_enter();
        
        for (int attempts = 0; attempts < 10; ++attempts) {
            read_path path;
            
            ptr_t root = root_.load();
            if (!root) {
                reader_exit();
                return end();
            }
            if (root->is_poisoned()) continue;
            
            bool found = read_impl_optimistic<true>(root, kbv, value, path);
            if (validate_read_path(path)) {
                reader_exit();
                if (found) return iterator(this, kbv, value);
                return end();
            }
        }
        bool found = read_impl<true>(root_.load(), kbv, value);
        reader_exit();
        if (found) return iterator(this, kbv, value);
    } else {
        if (read_impl<true>(root_.load(), kbv, value)) {
            return iterator(this, kbv, value);
        }
    }
    return end();
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::reclaim_retired() noexcept {
    if constexpr (THREADED) {
        std::lock_guard<std::mutex> lock(ebr_mutex_);
        // Take ownership of entire list
        ptr_t list = retired_head_.exchange(nullptr, std::memory_order_acquire);
        retired_count_.store(0, std::memory_order_relaxed);
        // Free everything
        while (list) {
            ptr_t curr = list;
            list = list->retire_next_;
            node_deleter(curr);
        }
    }
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum


// === tktrie_insert.h ===

// This file contains implementation details for tktrie (insert operations)
// It should only be included from tktrie_core.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

// -----------------------------------------------------------------------------
// Insert operations
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::insert_impl(
    atomic_ptr* slot, ptr_t n, std::string_view key, const T& value) {
    insert_result res;

    if (!n || n->is_poisoned() || builder_t::is_sentinel(n)) {
        res.new_node = create_leaf_for_key(key, value);
        res.inserted = true;
        return res;
    }

    if (n->is_leaf()) return insert_into_leaf(slot, n, key, value);
    return insert_into_interior(slot, n, key, value);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::insert_into_leaf(
    atomic_ptr*, ptr_t leaf, std::string_view key, const T& value) {
    insert_result res;
    std::string_view leaf_skip = leaf->skip_str();

    if (leaf->is_skip()) {
        size_t m = match_skip_impl(leaf_skip, key);
        if ((m == leaf_skip.size()) & (m == key.size())) return res;
        if ((m < leaf_skip.size()) & (m < key.size())) return split_leaf_skip(leaf, key, value, m);
        if (m == key.size()) return prefix_leaf_skip(leaf, key, value, m);
        return extend_leaf_skip(leaf, key, value, m);
    }

    size_t m = match_skip_impl(leaf_skip, key);
    if ((m < leaf_skip.size()) & (m < key.size())) return split_leaf_list(leaf, key, value, m);
    if (m < leaf_skip.size()) return prefix_leaf_list(leaf, key, value, m);
    key.remove_prefix(m);

    if (key.empty()) return add_eos_to_leaf_list(leaf, value);
    if (key.size() == 1) {
        unsigned char c = static_cast<unsigned char>(key[0]);
        return add_char_to_leaf(leaf, c, value);
    }
    return demote_leaf_list(leaf, key, value);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::insert_into_interior(
    atomic_ptr*, ptr_t n, std::string_view key, const T& value) {
    insert_result res;
    std::string_view skip = n->skip_str();

    size_t m = match_skip_impl(skip, key);
    if ((m < skip.size()) & (m < key.size())) return split_interior(n, key, value, m);
    if (m < skip.size()) return prefix_interior(n, key, value, m);
    key.remove_prefix(m);

    if (key.empty()) return set_interior_eos(n, value);

    unsigned char c = static_cast<unsigned char>(key[0]);
    key.remove_prefix(1);

    ptr_t child = n->get_child(c);
    if (child && !builder_t::is_sentinel(child)) {
        atomic_ptr* child_slot = n->get_child_slot(c);
        auto child_res = insert_impl(child_slot, child, key, value);
        if (child_res.new_node && child_res.new_node != child) {
            if constexpr (THREADED) {
                child_slot->store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
            }
            child_slot->store(child_res.new_node);
        }
        res.inserted = child_res.inserted;
        res.in_place = child_res.in_place;
        res.old_nodes = std::move(child_res.old_nodes);
        return res;
    }

    return add_child_to_interior(n, c, key, value);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::create_leaf_for_key(std::string_view key, const T& value) {
    return builder_.make_leaf_skip(key, value);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::split_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->skip_str();

    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);

    ptr_t interior = builder_.make_interior_list(common);
    T old_value;
    leaf->as_skip()->value.try_read(old_value);
    ptr_t old_child = builder_.make_leaf_skip(old_skip.substr(m + 1), old_value);
    ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
    interior->template as_list<false>()->add_two_children(old_c, old_child, new_c, new_child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->skip_str();

    ptr_t interior = builder_.make_interior_list(std::string(key));
    if constexpr (FIXED_LEN == 0) {
        interior->set_eos(value);
    }

    T old_value;
    leaf->as_skip()->value.try_read(old_value);
    ptr_t child = builder_.make_leaf_skip(old_skip.substr(m + 1), old_value);
    interior->template as_list<false>()->add_child(static_cast<unsigned char>(old_skip[m]), child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::extend_leaf_skip(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->skip_str();

    ptr_t interior = builder_.make_interior_list(std::string(old_skip));
    if constexpr (FIXED_LEN == 0) {
        T old_value;
        leaf->as_skip()->value.try_read(old_value);
        interior->set_eos(old_value);
    }

    ptr_t child = create_leaf_for_key(key.substr(m + 1), value);
    interior->template as_list<false>()->add_child(static_cast<unsigned char>(key[m]), child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::split_leaf_list(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->skip_str();

    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);

    ptr_t interior = builder_.make_interior_list(common);
    ptr_t old_child = clone_leaf_with_skip(leaf, old_skip.substr(m + 1));
    ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
    interior->template as_list<false>()->add_two_children(old_c, old_child, new_c, new_child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_leaf_list(
    ptr_t leaf, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = leaf->skip_str();

    ptr_t interior = builder_.make_interior_list(std::string(key));
    if constexpr (FIXED_LEN == 0) {
        interior->set_eos(value);
    }

    ptr_t old_child = clone_leaf_with_skip(leaf, old_skip.substr(m + 1));
    interior->template as_list<false>()->add_child(static_cast<unsigned char>(old_skip[m]), old_child);

    res.new_node = interior;
    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::clone_leaf_with_skip(ptr_t leaf, std::string_view new_skip) {
    if (leaf->is_list()) {
        ptr_t n = builder_.make_leaf_list(new_skip);
        leaf->template as_list<true>()->copy_values_to(n->template as_list<true>());
        return n;
    }
    ptr_t n = builder_.make_leaf_full(new_skip);
    leaf->template as_full<true>()->copy_values_to(n->template as_full<true>());
    return n;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_eos_to_leaf_list(ptr_t leaf, const T& value) {
    insert_result res;
    
    if constexpr (FIXED_LEN > 0) {
        return res;
    } else {
        std::string_view leaf_skip = leaf->skip_str();

        if (leaf->is_list()) [[likely]] {
            auto* src = leaf->template as_list<true>();
            ptr_t interior = builder_.make_interior_list(leaf_skip);
            interior->set_eos(value);
            int cnt = src->count();
            for (int i = 0; i < cnt; ++i) {
                unsigned char c = src->chars.char_at(i);
                T val;
                src->values[i].try_read(val);
                ptr_t child = builder_.make_leaf_skip("", val);
                interior->template as_list<false>()->add_child(c, child);
            }
            res.new_node = interior;
        } else {
            auto* src = leaf->template as_full<true>();
            ptr_t interior = builder_.make_interior_full(leaf_skip);
            interior->set_eos(value);
            src->valid.for_each_set([this, src, interior](unsigned char c) {
                T val;
                src->values[c].try_read(val);
                ptr_t child = builder_.make_leaf_skip("", val);
                interior->template as_full<false>()->add_child(c, child);
            });
            res.new_node = interior;
        }

        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_char_to_leaf(
    ptr_t leaf, unsigned char c, const T& value) {
    insert_result res;

    if (leaf->is_list()) {
        auto* ln = leaf->template as_list<true>();
        if (ln->has(c)) return res;

        if (ln->count() < LIST_MAX) {
            ln->add_value(c, value);
            res.in_place = true;
            res.inserted = true;
            return res;
        }

        ptr_t full = builder_.make_leaf_full(leaf->skip_str());
        auto* fn = full->template as_full<true>();
        [[assume(ln->count() == 7)]];  // Must be LIST_MAX to reach here
        for (int i = 0; i < ln->count(); ++i) {
            unsigned char ch = ln->chars.char_at(i);
            T val;
            ln->values[i].try_read(val);
            fn->add_value(ch, val);
        }
        fn->add_value(c, value);

        res.new_node = full;
        res.old_nodes.push_back(leaf);
        res.inserted = true;
        return res;
    }

    auto* fn = leaf->template as_full<true>();
    if (fn->has(c)) return res;
    fn->add_value_atomic(c, value);
    res.in_place = true;
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::demote_leaf_list(
    ptr_t leaf, std::string_view key, const T& value) {
    insert_result res;
    std::string_view leaf_skip = leaf->skip_str();
    unsigned char first_c = static_cast<unsigned char>(key[0]);

    if (leaf->is_list()) [[likely]] {
        auto* src = leaf->template as_list<true>();
        int leaf_count = src->count();
        [[assume(leaf_count >= 0 && leaf_count <= 7)]];
        int existing_idx = src->chars.find(first_c);
        bool need_full = (existing_idx < 0) && (leaf_count >= LIST_MAX);
        
        if (need_full) {
            ptr_t interior = builder_.make_interior_full(leaf_skip);
            auto* dst = interior->template as_full<false>();
            for (int i = 0; i < leaf_count; ++i) {
                unsigned char c = src->chars.char_at(i);
                T val;
                src->values[i].try_read(val);
                ptr_t child = builder_.make_leaf_skip("", val);
                dst->add_child(c, child);
            }
            ptr_t child = create_leaf_for_key(key.substr(1), value);
            dst->add_child(first_c, child);
            res.new_node = interior;
        } else {
            ptr_t interior = builder_.make_interior_list(leaf_skip);
            auto* dst = interior->template as_list<false>();
            for (int i = 0; i < leaf_count; ++i) {
                unsigned char c = src->chars.char_at(i);
                T val;
                src->values[i].try_read(val);
                ptr_t child = builder_.make_leaf_skip("", val);
                dst->add_child(c, child);
            }

            if (existing_idx >= 0) {
                ptr_t child = dst->children[existing_idx].load();
                auto child_res = insert_impl(&dst->children[existing_idx], child, key.substr(1), value);
                if (child_res.new_node) {
                    dst->children[existing_idx].store(child_res.new_node);
                }
                for (auto* old : child_res.old_nodes) res.old_nodes.push_back(old);
            } else {
                ptr_t child = create_leaf_for_key(key.substr(1), value);
                dst->add_child(first_c, child);
            }
            res.new_node = interior;
        }
    } else {
        auto* src = leaf->template as_full<true>();
        ptr_t interior = builder_.make_interior_full(leaf_skip);
        auto* dst = interior->template as_full<false>();
        src->valid.for_each_set([this, src, dst](unsigned char c) {
            T val;
            src->values[c].try_read(val);
            ptr_t child = builder_.make_leaf_skip("", val);
            dst->add_child(c, child);
        });

        if (dst->has(first_c)) {
            ptr_t child = dst->get_child(first_c);
            auto child_res = insert_impl(dst->get_child_slot(first_c), child, key.substr(1), value);
            if (child_res.new_node) {
                dst->children[first_c].store(child_res.new_node);
            }
            for (auto* old : child_res.old_nodes) res.old_nodes.push_back(old);
        } else {
            ptr_t child = create_leaf_for_key(key.substr(1), value);
            dst->add_child(first_c, child);
        }
        res.new_node = interior;
    }

    res.old_nodes.push_back(leaf);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::split_interior(
    ptr_t n, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = n->skip_str();

    std::string common(old_skip.substr(0, m));
    unsigned char old_c = static_cast<unsigned char>(old_skip[m]);
    unsigned char new_c = static_cast<unsigned char>(key[m]);

    ptr_t new_int = builder_.make_interior_list(common);
    ptr_t old_child = clone_interior_with_skip(n, old_skip.substr(m + 1));
    ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
    new_int->template as_list<false>()->add_two_children(old_c, old_child, new_c, new_child);

    res.new_node = new_int;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::ptr_t TKTRIE_CLASS::clone_interior_with_skip(ptr_t n, std::string_view new_skip) {
    if (n->is_list()) [[likely]] {
        ptr_t clone = builder_.make_interior_list(new_skip);
        n->template as_list<false>()->move_interior_to(clone->template as_list<false>());
        return clone;
    }
    ptr_t clone = builder_.make_interior_full(new_skip);
    n->template as_full<false>()->move_interior_to(clone->template as_full<false>());
    return clone;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::prefix_interior(
    ptr_t n, std::string_view key, const T& value, size_t m) {
    insert_result res;
    std::string_view old_skip = n->skip_str();

    ptr_t new_int = builder_.make_interior_list(std::string(key));
    if constexpr (FIXED_LEN == 0) {
        new_int->set_eos(value);
    }

    ptr_t old_child = clone_interior_with_skip(n, old_skip.substr(m + 1));
    new_int->template as_list<false>()->add_child(static_cast<unsigned char>(old_skip[m]), old_child);

    res.new_node = new_int;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::set_interior_eos(ptr_t n, const T& value) {
    insert_result res;
    
    if constexpr (FIXED_LEN > 0) {
        return res;
    } else {
        if (n->has_eos()) return res;
        n->set_eos(value);
        res.in_place = true;
        res.inserted = true;
        return res;
    }
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::insert_result TKTRIE_CLASS::add_child_to_interior(
    ptr_t n, unsigned char c, std::string_view remaining, const T& value) {
    insert_result res;
    ptr_t child = create_leaf_for_key(remaining, value);

    if (n->is_list()) {
        auto* ln = n->template as_list<false>();
        if (ln->count() < LIST_MAX) {
            ln->add_child(c, child);
            res.in_place = true;
            res.inserted = true;
            return res;
        }
        ptr_t full = builder_.make_interior_full(n->skip_str());
        ln->move_interior_to_full(full->template as_full<false>());
        full->template as_full<false>()->add_child(c, child);

        res.new_node = full;
        res.old_nodes.push_back(n);
        res.inserted = true;
        return res;
    }

    if (n->is_full()) {
        n->template as_full<false>()->add_child_atomic(c, child);
        res.in_place = true;
        res.inserted = true;
        return res;
    }

    ptr_t list = builder_.make_interior_list(n->skip_str());
    list->template as_list<false>()->add_child(c, child);

    res.new_node = list;
    res.old_nodes.push_back(n);
    res.inserted = true;
    return res;
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum


// === tktrie_insert_probe.h ===

// This file contains speculative insert probing for concurrent operations
// It should only be included from tktrie_insert.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::speculative_info TKTRIE_CLASS::probe_leaf_speculative(
    ptr_t n, std::string_view key, speculative_info& info) const noexcept {
    if (n->is_poisoned()) {
        info.op = spec_op::RETRY;  // Signal retry, not EXISTS
        return info;
    }
    
    std::string_view skip = n->skip_str();
    size_t m = match_skip_impl(skip, key);

    if (n->is_skip()) {
        if ((m == skip.size()) & (m == key.size())) { info.op = spec_op::EXISTS; return info; }
        info.target = n;
        info.target_version = n->version();
        info.target_skip = std::string(skip);
        info.match_pos = m;

        if ((m < skip.size()) & (m < key.size())) { info.op = spec_op::SPLIT_LEAF_SKIP; }
        else if (m == key.size()) { info.op = spec_op::PREFIX_LEAF_SKIP; }
        else { info.op = spec_op::EXTEND_LEAF_SKIP; }
        info.remaining_key = std::string(key);
        return info;
    }

    info.target = n;
    info.target_version = n->version();
    info.target_skip = std::string(skip);

    if ((m < skip.size()) & (m < key.size())) {
        info.op = spec_op::SPLIT_LEAF_LIST;
        info.match_pos = m;
        info.remaining_key = std::string(key);
        return info;
    }
    if (m < skip.size()) {
        info.op = spec_op::PREFIX_LEAF_LIST;
        info.match_pos = m;
        info.remaining_key = std::string(key);
        return info;
    }
    key.remove_prefix(m);
    info.remaining_key = std::string(key);

    if (key.empty()) { info.op = spec_op::ADD_EOS_LEAF_LIST; return info; }
    if (key.size() != 1) { info.op = spec_op::DEMOTE_LEAF_LIST; return info; }

    unsigned char c = static_cast<unsigned char>(key[0]);
    info.c = c;

    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        if (ln->has(c)) { info.op = spec_op::EXISTS; return info; }
        info.op = (ln->count() < LIST_MAX) ? spec_op::IN_PLACE_LEAF : spec_op::LIST_TO_FULL_LEAF;
        return info;
    }
    auto* fn = n->template as_full<true>();
    info.op = fn->has(c) ? spec_op::EXISTS : spec_op::IN_PLACE_LEAF;
    return info;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::speculative_info TKTRIE_CLASS::probe_speculative(
    ptr_t n, std::string_view key) const noexcept {
    speculative_info info;
    info.remaining_key = std::string(key);

    if (!n || builder_t::is_sentinel(n)) {
        info.op = spec_op::EMPTY_TREE;
        return info;
    }

    if (n->is_poisoned()) {
        info.op = spec_op::RETRY;  // Signal retry, not EXISTS
        return info;
    }

    info.path[info.path_len++] = {n, n->version(), 0};

    while (!n->is_leaf()) {
        std::string_view skip = n->skip_str();
        size_t m = match_skip_impl(skip, key);

        if ((m < skip.size()) & (m < key.size())) {
            info.op = spec_op::SPLIT_INTERIOR;
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            info.match_pos = m;
            info.remaining_key = std::string(key);
            return info;
        }
        if (m < skip.size()) {
            info.op = spec_op::PREFIX_INTERIOR;
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            info.match_pos = m;
            info.remaining_key = std::string(key);
            return info;
        }
        key.remove_prefix(m);

        if (key.empty()) {
            if (n->has_eos()) { info.op = spec_op::EXISTS; return info; }
            info.op = spec_op::IN_PLACE_INTERIOR;
            info.target = n;
            info.target_version = n->version();
            info.is_eos = true;
            return info;
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = n->get_child(c);

        if (!child || builder_t::is_sentinel(child)) {
            info.target = n;
            info.target_version = n->version();
            info.target_skip = std::string(skip);
            info.c = c;
            info.remaining_key = std::string(key.substr(1));

            if (n->is_list()) {
                info.op = (n->template as_list<false>()->count() < LIST_MAX) 
                    ? spec_op::IN_PLACE_INTERIOR : spec_op::ADD_CHILD_CONVERT;
            } else {
                info.op = spec_op::IN_PLACE_INTERIOR;
            }
            return info;
        }

        key.remove_prefix(1);
        n = child;
        
        if (n->is_poisoned()) {
            info.op = spec_op::RETRY;  // Signal retry, not EXISTS
            return info;
        }
        
        if (info.path_len < speculative_info::MAX_PATH) {
            info.path[info.path_len++] = {n, n->version(), c};
        }
    }

    return probe_leaf_speculative(n, key, info);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::pre_alloc TKTRIE_CLASS::allocate_speculative(
    const speculative_info& info, const T& value) {
    pre_alloc alloc;
    std::string_view key = info.remaining_key;
    std::string_view skip = info.target_skip;
    size_t m = info.match_pos;

    // Allocate AND fill data outside lock - speculative reads are validated later
    // All new nodes are poisoned so dealloc_node won't recurse into borrowed children
    switch (info.op) {
    case spec_op::EMPTY_TREE: {
        alloc.root_replacement = create_leaf_for_key(key, value);
        alloc.root_replacement->poison();
        alloc.add(alloc.root_replacement);
        break;
    }
    case spec_op::SPLIT_LEAF_SKIP: {
        T old_value{};
        info.target->as_skip()->value.try_read(old_value);
        
        std::string common(skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);

        ptr_t interior = builder_.make_interior_list(common);
        ptr_t old_child = builder_.make_leaf_skip(skip.substr(m + 1), old_value);
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        interior->template as_list<false>()->add_two_children(old_c, old_child, new_c, new_child);

        interior->poison();
        old_child->poison();
        new_child->poison();
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(old_child);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_LEAF_SKIP: {
        T old_value{};
        info.target->as_skip()->value.try_read(old_value);
        
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        
        ptr_t interior = builder_.make_interior_list(std::string(key));
        if constexpr (FIXED_LEN == 0) {
            interior->set_eos(value);
        }
        ptr_t child = builder_.make_leaf_skip(skip.substr(m + 1), old_value);
        interior->template as_list<false>()->add_child(old_c, child);

        interior->poison();
        child->poison();
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::EXTEND_LEAF_SKIP: {
        T old_value{};
        info.target->as_skip()->value.try_read(old_value);
        
        unsigned char new_c = static_cast<unsigned char>(key[m]);
        
        ptr_t interior = builder_.make_interior_list(std::string(skip));
        if constexpr (FIXED_LEN == 0) {
            interior->set_eos(old_value);
        }
        ptr_t child = create_leaf_for_key(key.substr(m + 1), value);
        interior->template as_list<false>()->add_child(new_c, child);

        interior->poison();
        child->poison();
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(child);
        break;
    }
    case spec_op::SPLIT_LEAF_LIST: {
        std::string common(skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);

        ptr_t interior = builder_.make_interior_list(common);
        ptr_t old_child;
        if (info.target->is_list()) {
            old_child = builder_.make_leaf_list(skip.substr(m + 1));
            info.target->template as_list<true>()->copy_values_to(old_child->template as_list<true>());
        } else {
            old_child = builder_.make_leaf_full(skip.substr(m + 1));
            info.target->template as_full<true>()->copy_values_to(old_child->template as_full<true>());
        }
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        interior->template as_list<false>()->add_two_children(old_c, old_child, new_c, new_child);

        interior->poison();
        old_child->poison();
        new_child->poison();
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(old_child);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_LEAF_LIST: {
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        
        ptr_t interior = builder_.make_interior_list(std::string(key));
        if constexpr (FIXED_LEN == 0) {
            interior->set_eos(value);
        }
        ptr_t old_child;
        if (info.target->is_list()) {
            old_child = builder_.make_leaf_list(skip.substr(m + 1));
            info.target->template as_list<true>()->copy_values_to(old_child->template as_list<true>());
        } else {
            old_child = builder_.make_leaf_full(skip.substr(m + 1));
            info.target->template as_full<true>()->copy_values_to(old_child->template as_full<true>());
        }
        interior->template as_list<false>()->add_child(old_c, old_child);

        interior->poison();
        old_child->poison();
        
        alloc.root_replacement = interior;
        alloc.add(interior);
        alloc.add(old_child);
        break;
    }
    case spec_op::LIST_TO_FULL_LEAF: {
        ptr_t full = builder_.make_leaf_full(std::string(skip));
        auto* src = info.target->template as_list<true>();
        auto* dst = full->template as_full<true>();
        [[assume(src->count() == 7)]];  // Must be LIST_MAX to reach here
        for (int i = 0; i < src->count(); ++i) {
            unsigned char ch = src->chars.char_at(i);
            T val{};
            src->values[i].try_read(val);
            dst->add_value(ch, val);
        }
        dst->add_value(info.c, value);
        
        full->poison();
        
        alloc.root_replacement = full;
        alloc.add(full);
        break;
    }
    case spec_op::SPLIT_INTERIOR: {
        std::string common(skip.substr(0, m));
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        unsigned char new_c = static_cast<unsigned char>(key[m]);

        ptr_t new_int = builder_.make_interior_list(common);
        ptr_t old_child;
        // Copy interior with borrowed children (poison prevents recursive delete)
        if (info.target->is_list()) {
            old_child = builder_.make_interior_list(skip.substr(m + 1));
            info.target->template as_list<false>()->copy_interior_to(old_child->template as_list<false>());
        } else {
            old_child = builder_.make_interior_full(skip.substr(m + 1));
            info.target->template as_full<false>()->copy_interior_to(old_child->template as_full<false>());
        }
        ptr_t new_child = create_leaf_for_key(key.substr(m + 1), value);
        new_int->template as_list<false>()->add_two_children(old_c, old_child, new_c, new_child);

        new_int->poison();
        old_child->poison();
        new_child->poison();
        
        alloc.root_replacement = new_int;
        alloc.add(new_int);
        alloc.add(old_child);
        alloc.add(new_child);
        break;
    }
    case spec_op::PREFIX_INTERIOR: {
        unsigned char old_c = static_cast<unsigned char>(skip[m]);
        
        ptr_t new_int = builder_.make_interior_list(std::string(key));
        if constexpr (FIXED_LEN == 0) {
            new_int->set_eos(value);
        }
        ptr_t old_child;
        // Copy interior with borrowed children (poison prevents recursive delete)
        if (info.target->is_list()) {
            old_child = builder_.make_interior_list(skip.substr(m + 1));
            info.target->template as_list<false>()->copy_interior_to(old_child->template as_list<false>());
        } else {
            old_child = builder_.make_interior_full(skip.substr(m + 1));
            info.target->template as_full<false>()->copy_interior_to(old_child->template as_full<false>());
        }
        new_int->template as_list<false>()->add_child(old_c, old_child);

        new_int->poison();
        old_child->poison();
        
        alloc.root_replacement = new_int;
        alloc.add(new_int);
        alloc.add(old_child);
        break;
    }
    case spec_op::ADD_CHILD_CONVERT: {
        // LIST interior full, convert to FULL - copy children (poison prevents recursive delete)
        ptr_t full = builder_.make_interior_full(std::string(skip));
        info.target->template as_list<false>()->copy_interior_to_full(full->template as_full<false>());
        ptr_t child = create_leaf_for_key(info.remaining_key, value);
        full->template as_full<false>()->add_child(info.c, child);
        
        full->poison();
        child->poison();
        
        alloc.root_replacement = full;
        alloc.add(full);
        alloc.add(child);
        break;
    }
    // These are handled differently (in-place or complex)
    case spec_op::EXISTS:
    case spec_op::IN_PLACE_LEAF:
    case spec_op::IN_PLACE_INTERIOR:
    case spec_op::ADD_EOS_LEAF_LIST:
    case spec_op::DEMOTE_LEAF_LIST:
        break;
    }

    return alloc;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::validate_path(const speculative_info& info) const noexcept {
    [[assume(info.path_len >= 0 && info.path_len <= 64)]];
    // Version check is sufficient - poison() bumps version
    for (int i = 0; i < info.path_len; ++i) {
        if (info.path[i].node->version() != info.path[i].version) return false;
    }
    if (info.target && (info.path_len == 0 || info.path[info.path_len-1].node != info.target)) {
        if (info.target->version() != info.target_version) return false;
    }
    return true;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::atomic_ptr* TKTRIE_CLASS::find_slot_for_commit(
    const speculative_info& info) noexcept {
    if (info.path_len <= 1) return &root_;
    ptr_t parent = info.path[info.path_len - 2].node;
    unsigned char edge = info.path[info.path_len - 1].edge;
    return parent->get_child_slot(edge);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::atomic_ptr* TKTRIE_CLASS::get_verified_slot(
    const speculative_info& info) noexcept {
    atomic_ptr* slot = (info.path_len <= 1) ? &root_ : find_slot_for_commit(info);
    return (slot->load() == info.target) ? slot : nullptr;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::commit_to_slot(atomic_ptr* slot, ptr_t new_node, 
                                   const speculative_info& info) noexcept {
    if (info.path_len > 1) info.path[info.path_len - 2].node->bump_version();
    if constexpr (THREADED) {
        slot->store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
    }
    slot->store(new_node);
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::commit_speculative(
    speculative_info& info, pre_alloc& alloc, [[maybe_unused]] const T& value) {
    
    // All data already filled in allocate_speculative - just validate and swap
    // On success, unpoison all nodes so they become live
    switch (info.op) {
    case spec_op::EMPTY_TREE:
        if (root_.load() != nullptr) return false;
        // Unpoison before making visible
        [[assume(alloc.count >= 0 && alloc.count <= 8)]];
        for (int i = 0; i < alloc.count; ++i) {
            if (alloc.nodes[i]) alloc.nodes[i]->unpoison();
        }
        root_.store(alloc.root_replacement);
        return true;

    case spec_op::SPLIT_LEAF_SKIP:
    case spec_op::PREFIX_LEAF_SKIP:
    case spec_op::EXTEND_LEAF_SKIP:
    case spec_op::SPLIT_LEAF_LIST:
    case spec_op::PREFIX_LEAF_LIST:
    case spec_op::LIST_TO_FULL_LEAF:
    case spec_op::SPLIT_INTERIOR:
    case spec_op::PREFIX_INTERIOR:
    case spec_op::ADD_CHILD_CONVERT: {
        atomic_ptr* slot = get_verified_slot(info);
        if (!slot) return false;
        // Unpoison before making visible
        [[assume(alloc.count >= 0 && alloc.count <= 8)]];
        for (int i = 0; i < alloc.count; ++i) {
            if (alloc.nodes[i]) alloc.nodes[i]->unpoison();
        }
        commit_to_slot(slot, alloc.root_replacement, info);
        return true;
    }

    // These should not reach commit_speculative
    case spec_op::EXISTS:
    case spec_op::IN_PLACE_LEAF:
    case spec_op::IN_PLACE_INTERIOR:
    case spec_op::ADD_EOS_LEAF_LIST:
    case spec_op::DEMOTE_LEAF_LIST:
        return false;
    }
    return false;
}

TKTRIE_TEMPLATE
void TKTRIE_CLASS::dealloc_speculation(pre_alloc& alloc) {
    [[assume(alloc.count >= 0 && alloc.count <= 8)]];
    // Iterate all allocated nodes - dealloc_node handles poison (won't recurse into borrowed children)
    for (int i = 0; i < alloc.count; ++i) {
        if (alloc.nodes[i]) {
            builder_.dealloc_node(alloc.nodes[i]);
            alloc.nodes[i] = nullptr;
        }
    }
    alloc.count = 0;
    alloc.root_replacement = nullptr;
}

TKTRIE_TEMPLATE
std::pair<typename TKTRIE_CLASS::iterator, bool> TKTRIE_CLASS::insert_locked(
    const Key& key, std::string_view kb, const T& value, bool* retired_any) {
    if (retired_any) *retired_any = false;
    
    if constexpr (!THREADED) {
        std::lock_guard<mutex_t> lock(mutex_);

        ptr_t root = root_.load();
        auto res = insert_impl(&root_, root, kb, value);

        if (!res.inserted) {
            if (retired_any && !res.old_nodes.empty()) *retired_any = true;
            for (auto* old : res.old_nodes) retire_node(old);
            return {find(key), false};
        }

        if (res.new_node) root_.store(res.new_node);
        if (retired_any && !res.old_nodes.empty()) *retired_any = true;
        for (auto* old : res.old_nodes) retire_node(old);
        size_.fetch_add(1);

        return {iterator(this, kb, value), true};
    } else {
        // Writers cleanup at 1x threshold
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED) {
            ebr_cleanup();
        }
        
        reader_enter();
        
        constexpr int MAX_RETRIES = 7;
        
        for (int retry = 0; retry <= MAX_RETRIES; ++retry) {
            speculative_info spec = probe_speculative(root_.load(), kb);
            
            stat_attempt();

            // RETRY means concurrent write detected - try again
            if (spec.op == spec_op::RETRY) {
                continue;
            }

            if (spec.op == spec_op::EXISTS) {
                stat_success(retry);
                reader_exit();
                return {iterator(this, kb, value), false};
            }

            // In-place leaf - no allocation, brief lock
            if (spec.op == spec_op::IN_PLACE_LEAF) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_path(spec)) continue;
                
                ptr_t n = spec.target;
                unsigned char c = spec.c;
                
                if (n->is_list()) {
                    auto* ln = n->template as_list<true>();
                    if (ln->has(c)) continue;
                    if (ln->count() >= LIST_MAX) {
                        // Need LIST_TO_FULL - re-probe will get it
                        continue;
                    }
                    epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
                    n->bump_version();
                    ln->add_value(c, value);
                } else {
                    auto* fn = n->template as_full<true>();
                    if (fn->has(c)) continue;
                    epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
                    n->bump_version();
                    fn->add_value_atomic(c, value);
                }
                size_.fetch_add(1);
                stat_success(retry);
                reader_exit();
                return {iterator(this, kb, value), true};
            }

            // In-place interior - brief lock
            if (spec.op == spec_op::IN_PLACE_INTERIOR) {
                if (spec.is_eos) {
                    if constexpr (FIXED_LEN > 0) {
                        // Can't happen for fixed-length keys
                        continue;
                    } else {
                        std::lock_guard<mutex_t> lock(mutex_);
                        if (!validate_path(spec)) continue;
                        
                        ptr_t n = spec.target;
                        if (n->has_eos()) continue;
                        
                        epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
                        n->bump_version();
                        n->set_eos(value);
                        size_.fetch_add(1);
                        stat_success(retry);
                        reader_exit();
                        return {iterator(this, kb, value), true};
                    }
                } else {
                    // Adding child to interior - allocate child outside lock
                    ptr_t child = create_leaf_for_key(spec.remaining_key, value);
                    std::lock_guard<mutex_t> lock(mutex_);
                    if (!validate_path(spec)) { builder_.dealloc_node(child); continue; }
                    
                    ptr_t n = spec.target;
                    unsigned char c = spec.c;
                    
                    if (n->is_list()) {
                        auto* ln = n->template as_list<false>();
                        if (ln->has(c)) { builder_.dealloc_node(child); continue; }
                        if (ln->count() >= LIST_MAX) {
                            builder_.dealloc_node(child);
                            continue;  // Re-probe will get ADD_CHILD_CONVERT
                        }
                        epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
                        n->bump_version();
                        ln->add_child(c, child);
                    } else if (n->is_full()) {
                        auto* fn = n->template as_full<false>();
                        if (fn->has(c)) { builder_.dealloc_node(child); continue; }
                        epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
                        n->bump_version();
                        fn->add_child_atomic(c, child);
                    } else {
                        builder_.dealloc_node(child);
                        continue;
                    }
                    size_.fetch_add(1);
                    stat_success(retry);
                    reader_exit();
                    return {iterator(this, kb, value), true};
                }
            }

            // Complex ops that need full speculative path (ADD_EOS_LEAF_LIST, DEMOTE_LEAF_LIST)
            // Fall through to allocation-based speculative or fallback
            if (spec.op == spec_op::ADD_EOS_LEAF_LIST || spec.op == spec_op::DEMOTE_LEAF_LIST) {
                // These are complex - use fallback
                if (retry == MAX_RETRIES) break;
                continue;
            }

            // Speculative path: allocate outside lock, then brief lock for commit
            pre_alloc alloc = allocate_speculative(spec, value);
            
            if (alloc.root_replacement) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_path(spec)) {
                    dealloc_speculation(alloc);
                    continue;
                }
                
                if (commit_speculative(spec, alloc, value)) {
                    epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
                    // Retire old node
                    if (spec.target) {
                        retire_node(spec.target);
                        if (retired_any) *retired_any = true;
                    }
                    size_.fetch_add(1);
                    stat_success(retry);
                    reader_exit();
                    return {iterator(this, kb, value), true};
                }
                dealloc_speculation(alloc);
                continue;
            }
        }
        
        // Fallback after MAX_RETRIES
        stat_fallback();
        {
            std::lock_guard<mutex_t> lock(mutex_);
            
            ptr_t root = root_.load();
            auto res = insert_impl(&root_, root, kb, value);
            
            if (!res.inserted) {
                if (retired_any && !res.old_nodes.empty()) *retired_any = true;
                for (auto* old : res.old_nodes) retire_node(old);
                reader_exit();
                return {iterator(this, kb, value), false};
            }
            
            epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
            if (res.new_node) {
                root_.store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
                root_.store(res.new_node);
            }
            if (retired_any && !res.old_nodes.empty()) *retired_any = true;
            for (auto* old : res.old_nodes) retire_node(old);
            size_.fetch_add(1);
            reader_exit();
            return {iterator(this, kb, value), true};
        }
    }
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum


// === tktrie_erase_probe.h ===

// This file contains speculative erase probing and allocation
// It should only be included from tktrie_insert_probe.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

// -----------------------------------------------------------------------------
// Probe leaf for erase operation
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_spec_info TKTRIE_CLASS::probe_leaf_erase(
    ptr_t n, std::string_view key, erase_spec_info& info) const noexcept {
    if (n->is_poisoned()) {
        info.op = erase_op::NOT_FOUND;
        return info;
    }
    
    std::string_view skip = n->skip_str();
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) { info.op = erase_op::NOT_FOUND; return info; }
    key.remove_prefix(m);

    info.target = n;
    info.target_version = n->version();
    info.target_skip = std::string(skip);

    // SKIP leaf - delete entire node
    if (n->is_skip()) {
        if (!key.empty()) { info.op = erase_op::NOT_FOUND; return info; }
        info.op = erase_op::DELETE_SKIP_LEAF;
        return info;
    }

    // LIST or FULL leaf
    if (key.size() != 1) { info.op = erase_op::NOT_FOUND; return info; }

    unsigned char c = static_cast<unsigned char>(key[0]);
    info.c = c;

    if (n->is_list()) [[likely]] {
        auto* ln = n->template as_list<true>();
        if (!ln->has(c)) { info.op = erase_op::NOT_FOUND; return info; }
        if (ln->count() == 1) {
            info.op = erase_op::DELETE_LAST_LEAF_ENTRY;
        } else {
            info.op = erase_op::IN_PLACE_LEAF_LIST;
        }
        return info;
    }

    auto* fn = n->template as_full<true>();
    if (!fn->has(c)) { info.op = erase_op::NOT_FOUND; return info; }
    // FULL leaf always in-place (never becomes empty from one removal)
    info.op = erase_op::IN_PLACE_LEAF_FULL;
    return info;
}

// -----------------------------------------------------------------------------
// Probe interior for erase - handles EOS deletion
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_spec_info TKTRIE_CLASS::probe_interior_erase(
    ptr_t n, std::string_view key, erase_spec_info& info) const noexcept {
    info.target = n;
    info.target_version = n->version();
    info.target_skip = std::string(n->skip_str());
    
    if (key.empty()) {
        // Deleting EOS from interior
        if constexpr (FIXED_LEN > 0) {
            info.op = erase_op::NOT_FOUND;
            return info;
        }
        if (!n->has_eos()) {
            info.op = erase_op::NOT_FOUND;
            return info;
        }
        
        int child_cnt = n->child_count();
        if (child_cnt == 0) {
            info.op = erase_op::NOT_FOUND;  // Use slow path
            return info;
        }
        if (child_cnt == 1) {
            // Will collapse with single child
            unsigned char c = 0;
            ptr_t child = nullptr;
            if (n->is_list()) {
                auto* ln = n->template as_list<false>();
                c = ln->chars.char_at(0);
                child = ln->children[0].load();
            } else {
                auto* fn = n->template as_full<false>();
                c = fn->valid.first();
                child = fn->children[c].load();
            }
            if (child && !builder_t::is_sentinel(child) && !child->is_poisoned()) {
                info.collapse_child = child;
                info.collapse_char = c;
                info.child_skip = std::string(child->skip_str());
            }
        }
        info.op = erase_op::DELETE_EOS_INTERIOR;
        return info;
    }
    
    info.op = erase_op::NOT_FOUND;
    return info;
}

// -----------------------------------------------------------------------------
// Main probe dispatcher
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_spec_info TKTRIE_CLASS::probe_erase(
    ptr_t n, std::string_view key) const noexcept {
    erase_spec_info info;

    if (!n || n->is_poisoned() || builder_t::is_sentinel(n)) {
        info.op = erase_op::NOT_FOUND;
        return info;
    }

    info.path[info.path_len++] = {n, n->version(), 0};

    while (!n->is_leaf()) {
        std::string_view skip = n->skip_str();
        size_t m = match_skip_impl(skip, key);
        if (m < skip.size()) { info.op = erase_op::NOT_FOUND; return info; }
        key.remove_prefix(m);

        if (key.empty()) {
            return probe_interior_erase(n, key, info);
        }

        unsigned char c = static_cast<unsigned char>(key[0]);
        ptr_t child = n->get_child(c);
        
        if (!child || builder_t::is_sentinel(child)) { 
            info.op = erase_op::NOT_FOUND; 
            return info; 
        }

        key.remove_prefix(1);
        n = child;
        
        if (n->is_poisoned()) {
            info.op = erase_op::NOT_FOUND;
            return info;
        }
        
        if (info.path_len < erase_spec_info::MAX_PATH) {
            info.path[info.path_len++] = {n, n->version(), c};
        }
    }

    return probe_leaf_erase(n, key, info);
}

// -----------------------------------------------------------------------------
// Allocate replacement nodes for erase
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_pre_alloc TKTRIE_CLASS::allocate_erase_speculative(
    const erase_spec_info& info) {
    erase_pre_alloc alloc;
    
    switch (info.op) {
    case erase_op::DELETE_SKIP_LEAF:
    case erase_op::DELETE_LAST_LEAF_ENTRY:
    case erase_op::DELETE_CHILD_NO_COLLAPSE:
        break;
        
    case erase_op::DELETE_EOS_INTERIOR:
    case erase_op::DELETE_CHILD_COLLAPSE: {
        if (!info.collapse_child) break;
        
        std::string new_skip(info.target_skip);
        new_skip.push_back(static_cast<char>(info.collapse_char));
        new_skip.append(info.child_skip);
        
        ptr_t child = info.collapse_child;
        ptr_t merged = nullptr;
        
        if (child->is_leaf()) {
            if (child->is_skip()) {
                T val{};
                child->as_skip()->value.try_read(val);
                merged = builder_.make_leaf_skip(new_skip, val);
            } else if (child->is_list()) {
                merged = builder_.make_leaf_list(new_skip);
                child->template as_list<true>()->copy_values_to(merged->template as_list<true>());
            } else {
                merged = builder_.make_leaf_full(new_skip);
                child->template as_full<true>()->copy_values_to(merged->template as_full<true>());
            }
        } else {
            if (child->is_list()) {
                merged = builder_.make_interior_list(new_skip);
                child->template as_list<false>()->copy_interior_to(merged->template as_list<false>());
            } else {
                merged = builder_.make_interior_full(new_skip);
                child->template as_full<false>()->copy_interior_to(merged->template as_full<false>());
            }
        }
        
        if (merged) {
            merged->poison();
            alloc.replacement = merged;
            alloc.add(merged);
        }
        break;
    }
    
    case erase_op::NOT_FOUND:
    case erase_op::IN_PLACE_LEAF_LIST:
    case erase_op::IN_PLACE_LEAF_FULL:
        break;
    }
    
    return alloc;
}

// -----------------------------------------------------------------------------
// Validate erase path
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::validate_erase_path(const erase_spec_info& info) const noexcept {
    [[assume(info.path_len >= 0 && info.path_len <= 64)]];
    for (int i = 0; i < info.path_len; ++i) {
        if (info.path[i].node->is_poisoned()) return false;
        if (info.path[i].node->version() != info.path[i].version) return false;
    }
    if (info.target && (info.path_len == 0 || info.path[info.path_len-1].node != info.target)) {
        if (info.target->is_poisoned()) return false;
        if (info.target->version() != info.target_version) return false;
    }
    if (info.collapse_child && info.collapse_child->is_poisoned()) {
        return false;
    }
    return true;
}

// -----------------------------------------------------------------------------
// Commit erase speculation
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::commit_erase_speculative(
    erase_spec_info& info, erase_pre_alloc& alloc) {
    
    atomic_ptr* slot = nullptr;
    if (info.path_len <= 1) {
        slot = &root_;
    } else {
        ptr_t parent = info.path[info.path_len - 2].node;
        unsigned char edge = info.path[info.path_len - 1].edge;
        slot = parent->get_child_slot(edge);
    }
    
    switch (info.op) {
    case erase_op::DELETE_SKIP_LEAF:
    case erase_op::DELETE_LAST_LEAF_ENTRY: {
        if (!slot || slot->load() != info.target) return false;
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        if constexpr (THREADED) {
            slot->store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
        }
        slot->store(nullptr);
        return true;
    }
    
    case erase_op::DELETE_CHILD_NO_COLLAPSE: {
        ptr_t parent = info.target;
        if (parent->version() != info.target_version) return false;
        
        parent->bump_version();
        if (parent->is_list()) {
            parent->template as_list<false>()->remove_child(info.c);
        } else {
            parent->template as_full<false>()->remove_child(info.c);
        }
        return true;
    }
    
    case erase_op::DELETE_EOS_INTERIOR: {
        ptr_t target = info.target;
        if (target->version() != info.target_version) return false;
        
        if (alloc.replacement) {
            if (!slot || slot->load() != target) return false;
            [[assume(alloc.count >= 0 && alloc.count <= 4)]];
            for (int i = 0; i < alloc.count; ++i) {
                if (alloc.nodes[i]) alloc.nodes[i]->unpoison();
            }
            if (info.path_len > 1) {
                info.path[info.path_len - 2].node->bump_version();
            }
            if constexpr (THREADED) {
                slot->store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
            }
            slot->store(alloc.replacement);
        } else {
            target->bump_version();
            target->clear_eos();
        }
        return true;
    }
    
    case erase_op::DELETE_CHILD_COLLAPSE: {
        if (!alloc.replacement) return false;
        if (!slot || slot->load() != info.target) return false;
        
        [[assume(alloc.count >= 0 && alloc.count <= 4)]];
        for (int i = 0; i < alloc.count; ++i) {
            if (alloc.nodes[i]) alloc.nodes[i]->unpoison();
        }
        if (info.path_len > 1) {
            info.path[info.path_len - 2].node->bump_version();
        }
        if constexpr (THREADED) {
            slot->store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
        }
        slot->store(alloc.replacement);
        return true;
    }
    
    case erase_op::NOT_FOUND:
    case erase_op::IN_PLACE_LEAF_LIST:
    case erase_op::IN_PLACE_LEAF_FULL:
        return false;
    }
    return false;
}

// -----------------------------------------------------------------------------
// Dealloc erase speculation
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
void TKTRIE_CLASS::dealloc_erase_speculation(erase_pre_alloc& alloc) {
    [[assume(alloc.count >= 0 && alloc.count <= 4)]];
    for (int i = 0; i < alloc.count; ++i) {
        if (alloc.nodes[i]) {
            builder_.dealloc_node(alloc.nodes[i]);
            alloc.nodes[i] = nullptr;
        }
    }
    alloc.count = 0;
    alloc.replacement = nullptr;
}

// -----------------------------------------------------------------------------
// In-place erase handlers
// -----------------------------------------------------------------------------

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_leaf_list_erase(ptr_t leaf, unsigned char c, uint64_t expected_version) {
    if (leaf->version() != expected_version) return false;
    auto* ln = leaf->template as_list<true>();
    if (!ln->has(c)) return false;
    if (ln->count() <= 1) return false;

    leaf->bump_version();
    ln->remove_value(c);
    return true;
}

TKTRIE_TEMPLATE
bool TKTRIE_CLASS::do_inplace_leaf_full_erase(ptr_t leaf, unsigned char c, uint64_t expected_version) {
    if (leaf->version() != expected_version) return false;
    auto* fn = leaf->template as_full<true>();
    if (!fn->has(c)) return false;
    leaf->bump_version();
    fn->remove_value(c);
    return true;
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum


// === tktrie_erase.h ===

// This file contains erase implementation
// It should only be included from tktrie_erase_probe.h

namespace gteitelbaum {

#define TKTRIE_TEMPLATE template <typename Key, typename T, bool THREADED, typename Allocator>
#define TKTRIE_CLASS tktrie<Key, T, THREADED, Allocator>

TKTRIE_TEMPLATE
std::pair<bool, bool> TKTRIE_CLASS::erase_locked(std::string_view kb) {
    // Returns (erased, retired_any)
    auto apply_erase_result = [this](erase_result& res) -> std::pair<bool, bool> {
        if (!res.erased) return {false, false};
        if constexpr (THREADED) {
            epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
        }
        if (res.deleted_subtree) {
            if constexpr (THREADED) root_.store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
            root_.store(nullptr);
        } else if (res.new_node) {
            if constexpr (THREADED) root_.store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
            root_.store(res.new_node);
        }
        bool retired_any = !res.old_nodes.empty();
        for (auto* old : res.old_nodes) retire_node(old);
        size_.fetch_sub(1);
        return {true, retired_any};
    };
    
    if constexpr (!THREADED) {
        std::lock_guard<mutex_t> lock(mutex_);
        auto res = erase_impl(&root_, root_.load(), kb);
        return apply_erase_result(res);
    } else {
        // Writers cleanup at 1x threshold
        if (retired_count_.load(std::memory_order_relaxed) >= EBR_MIN_RETIRED) {
            ebr_cleanup();
        }
        
        reader_enter();
        
        static constexpr int MAX_RETRIES = 7;
        
        for (int retry = 0; retry <= MAX_RETRIES; ++retry) {
            erase_spec_info info = probe_erase(root_.load(), kb);

            if (info.op == erase_op::NOT_FOUND) {
                reader_exit();
                return {false, false};
            }

            // In-place operations - no allocation needed
            if (info.op == erase_op::IN_PLACE_LEAF_LIST) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_erase_path(info)) continue;
                if (do_inplace_leaf_list_erase(info.target, info.c, info.target_version)) {
                    epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
                    size_.fetch_sub(1);
                    reader_exit();
                    return {true, false};
                }
                continue;
            }
            
            if (info.op == erase_op::IN_PLACE_LEAF_FULL) {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_erase_path(info)) continue;
                if (do_inplace_leaf_full_erase(info.target, info.c, info.target_version)) {
                    epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
                    size_.fetch_sub(1);
                    reader_exit();
                    return {true, false};
                }
                continue;
            }

            // Speculative path: allocate outside lock, brief lock for commit
            erase_pre_alloc alloc = allocate_erase_speculative(info);
            
            {
                std::lock_guard<mutex_t> lock(mutex_);
                if (!validate_erase_path(info)) {
                    dealloc_erase_speculation(alloc);
                    continue;
                }
                
                if (commit_erase_speculative(info, alloc)) {
                    epoch_.fetch_add(1, std::memory_order_release);  // Signal readers
                    // Retire old nodes
                    bool retired_any = false;
                    if (info.target) {
                        retire_node(info.target);
                        retired_any = true;
                    }
                    if (info.collapse_child) {
                        retire_node(info.collapse_child);
                        retired_any = true;
                    }
                    size_.fetch_sub(1);
                    reader_exit();
                    return {true, retired_any};
                }
                dealloc_erase_speculation(alloc);
                continue;
            }
        }
        
        // Fallback after MAX_RETRIES
        {
            std::lock_guard<mutex_t> lock(mutex_);
            auto res = erase_impl(&root_, root_.load(), kb);
            reader_exit();
            return apply_erase_result(res);
        }
    }
}


TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::erase_impl(
    atomic_ptr*, ptr_t n, std::string_view key) {
    erase_result res;
    if (!n || n->is_poisoned() || builder_t::is_sentinel(n)) return res;
    if (n->is_leaf()) return erase_from_leaf(n, key);
    return erase_from_interior(n, key);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::erase_from_leaf(
    ptr_t leaf, std::string_view key) {
    erase_result res;
    std::string_view skip = leaf->skip_str();
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) return res;
    key.remove_prefix(m);

    if (leaf->is_skip()) {
        if (!key.empty()) return res;
        res.erased = true;
        res.deleted_subtree = true;
        res.old_nodes.push_back(leaf);
        return res;
    }

    if (key.size() != 1) return res;
    unsigned char c = static_cast<unsigned char>(key[0]);

    if (leaf->is_list()) [[likely]] {
        auto* ln = leaf->template as_list<true>();
        if (!ln->has(c)) return res;

        if (ln->count() == 1) {
            res.erased = true;
            res.deleted_subtree = true;
            res.old_nodes.push_back(leaf);
            return res;
        }

        leaf->bump_version();
        ln->remove_value(c);
        res.erased = true;
        return res;
    }

    auto* fn = leaf->template as_full<true>();
    if (!fn->has(c)) return res;
    leaf->bump_version();
    fn->remove_value(c);
    res.erased = true;
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::erase_from_interior(
    ptr_t n, std::string_view key) {
    erase_result res;
    std::string_view skip = n->skip_str();
    size_t m = match_skip_impl(skip, key);
    if (m < skip.size()) return res;
    key.remove_prefix(m);

    if (key.empty()) {
        if constexpr (FIXED_LEN > 0) {
            return res;
        } else {
            if (!n->has_eos()) return res;
            n->bump_version();
            n->clear_eos();
            res.erased = true;
            return try_collapse_interior(n);
        }
    }

    unsigned char c = static_cast<unsigned char>(key[0]);
    ptr_t child = n->get_child(c);
    
    if (!child || builder_t::is_sentinel(child)) return res;

    auto child_res = erase_impl(n->get_child_slot(c), child, key.substr(1));
    if (!child_res.erased) return res;

    if (child_res.deleted_subtree) {
        return try_collapse_after_child_removal(n, c, child_res);
    }

    if (child_res.new_node) {
        n->bump_version();
        if constexpr (THREADED) {
            n->get_child_slot(c)->store(get_retry_sentinel<T, THREADED, Allocator, FIXED_LEN>());
        }
        n->get_child_slot(c)->store(child_res.new_node);
    }
    res.erased = true;
    res.old_nodes = std::move(child_res.old_nodes);
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::try_collapse_interior(ptr_t n) {
    erase_result res;
    res.erased = true;

    bool eos_exists = n->has_eos();
    if (eos_exists) return res;

    int child_cnt = n->child_count();
    if (child_cnt == 0) {
        res.deleted_subtree = true;
        res.old_nodes.push_back(n);
        return res;
    }
    if (child_cnt != 1) return res;

    unsigned char c = 0;
    ptr_t child = nullptr;
    if (n->is_list()) {
        auto* ln = n->template as_list<false>();
        c = ln->chars.char_at(0);
        child = ln->children[0].load();
    } else if (n->is_full()) {
        auto* fn = n->template as_full<false>();
        c = fn->valid.first();
        child = fn->children[c].load();
    }
    if (!child || builder_t::is_sentinel(child)) return res;

    return collapse_single_child(n, c, child, res);
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::try_collapse_after_child_removal(
    ptr_t n, unsigned char removed_c, erase_result& child_res) {
    erase_result res;
    res.old_nodes = std::move(child_res.old_nodes);
    res.erased = true;

    bool eos_exists = n->has_eos();
    int remaining = n->child_count();

    if (n->is_list()) {
        auto* ln = n->template as_list<false>();
        if (ln->has(removed_c)) remaining--;
    } else if (n->is_full()) {
        auto* fn = n->template as_full<false>();
        if (fn->has(removed_c)) remaining--;
    }

    if ((!eos_exists) & (remaining == 0)) {
        res.deleted_subtree = true;
        res.old_nodes.push_back(n);
        return res;
    }

    if (n->is_list()) {
        auto* ln = n->template as_list<false>();
        if (ln->has(removed_c)) {
            n->bump_version();
            ln->remove_child(removed_c);
        }
    } else if (n->is_full()) {
        auto* fn = n->template as_full<false>();
        if (fn->has(removed_c)) {
            n->bump_version();
            fn->remove_child(removed_c);
        }
    }

    bool can_collapse = false;
    unsigned char c = 0;
    ptr_t child = nullptr;

    if (n->is_list()) {
        auto* ln = n->template as_list<false>();
        if (ln->count() == 1 && !eos_exists) {
            c = ln->chars.char_at(0);
            child = ln->children[0].load();
            can_collapse = (child != nullptr && !builder_t::is_sentinel(child));
        }
    } else if (n->is_full() && !eos_exists) {
        auto* fn = n->template as_full<false>();
        if (fn->count() == 1) {
            c = fn->valid.first();
            child = fn->children[c].load();
            can_collapse = (child != nullptr && !builder_t::is_sentinel(child));
        }
    }

    if (can_collapse) {
        return collapse_single_child(n, c, child, res);
    }
    return res;
}

TKTRIE_TEMPLATE
typename TKTRIE_CLASS::erase_result TKTRIE_CLASS::collapse_single_child(
    ptr_t n, unsigned char c, ptr_t child, erase_result& res) {
    std::string new_skip(n->skip_str());
    new_skip.push_back(static_cast<char>(c));
    new_skip.append(child->skip_str());

    ptr_t merged;
    if (child->is_leaf()) {
        if (child->is_skip()) {
            T val;
            child->as_skip()->value.try_read(val);
            merged = builder_.make_leaf_skip(new_skip, val);
        } else if (child->is_list()) [[likely]] {
            merged = builder_.make_leaf_list(new_skip);
            child->template as_list<true>()->copy_values_to(merged->template as_list<true>());
        } else {
            merged = builder_.make_leaf_full(new_skip);
            child->template as_full<true>()->copy_values_to(merged->template as_full<true>());
        }
    } else {
        if (child->is_list()) [[likely]] {
            merged = builder_.make_interior_list(new_skip);
            child->template as_list<false>()->move_interior_to(merged->template as_list<false>());
        } else {
            merged = builder_.make_interior_full(new_skip);
            child->template as_full<false>()->move_interior_to(merged->template as_full<false>());
        }
    }

    res.new_node = merged;
    res.old_nodes.push_back(n);
    res.old_nodes.push_back(child);
    return res;
}

#undef TKTRIE_TEMPLATE
#undef TKTRIE_CLASS

}  // namespace gteitelbaum

