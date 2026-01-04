#pragma once

#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "tktrie_defines.h"
#include "tktrie_node.h"
#include "tktrie_help_nav.h"
#include "tktrie_help_insert.h"
#include "tktrie_help_remove.h"
#include "tktrie_impl.h"

namespace gteitelbaum {

// =============================================================================
// Key type traits
// =============================================================================

template <typename Key>
struct tktrie_traits;

template <>
struct tktrie_traits<std::string> {
    static constexpr size_t fixed_len = 0;
    static std::string to_bytes(const std::string& k) { return k; }
    static std::string from_bytes(std::string_view bytes) { return std::string(bytes); }
};

template <>
struct tktrie_traits<std::string_view> {
    static constexpr size_t fixed_len = 0;
    static std::string to_bytes(std::string_view k) { return std::string(k); }
    static std::string from_bytes(std::string_view bytes) { return std::string(bytes); }
};

template <typename T>
    requires std::is_integral_v<T>
struct tktrie_traits<T> {
    static constexpr size_t fixed_len = sizeof(T);
    using unsigned_type = std::make_unsigned_t<T>;

    static std::string to_bytes(T k) {
        unsigned_type sortable;
        if constexpr (std::is_signed_v<T>) {
            sortable = static_cast<unsigned_type>(k) ^ (unsigned_type{1} << (sizeof(T) * 8 - 1));
        } else {
            sortable = k;
        }
        unsigned_type be = to_big_endian(sortable);
        char buf[sizeof(T)];
        std::memcpy(buf, &be, sizeof(T));
        return std::string(buf, sizeof(T));
    }

    static T from_bytes(std::string_view bytes) {
        KTRIE_DEBUG_ASSERT(bytes.size() == sizeof(T));
        unsigned_type be;
        std::memcpy(&be, bytes.data(), sizeof(T));
        unsigned_type sortable = from_big_endian(be);
        if constexpr (std::is_signed_v<T>) {
            return static_cast<T>(sortable ^ (unsigned_type{1} << (sizeof(T) * 8 - 1)));
        } else {
            return static_cast<T>(sortable);
        }
    }
};

// =============================================================================
// Iterator
// =============================================================================

template <typename Key, typename T, bool THREADED, typename Allocator, size_t FIXED_LEN>
class tktrie_iterator {
public:
    using trie_type = tktrie<Key, T, THREADED, Allocator, FIXED_LEN>;
    using traits = tktrie_traits<Key>;
    using value_type = std::pair<Key, T>;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::forward_iterator_tag;

private:
    friend class tktrie<Key, T, THREADED, Allocator, FIXED_LEN>;
    const trie_type* parent_;
    std::string key_bytes_;
    T value_;
    bool valid_;

public:
    tktrie_iterator() noexcept : parent_(nullptr), valid_(false) {}

    tktrie_iterator(const trie_type* parent, std::string_view key_bytes, const T& value)
        : parent_(parent), key_bytes_(key_bytes), value_(value), valid_(true) {}

    tktrie_iterator(const tktrie_iterator& other)
        : parent_(other.parent_), key_bytes_(other.key_bytes_),
          value_(other.value_), valid_(other.valid_) {}

    tktrie_iterator& operator=(const tktrie_iterator& other) {
        if (this != &other) {
            parent_ = other.parent_;
            key_bytes_ = other.key_bytes_;
            value_ = other.value_;
            valid_ = other.valid_;
        }
        return *this;
    }

    tktrie_iterator(tktrie_iterator&& other) noexcept
        : parent_(other.parent_), key_bytes_(std::move(other.key_bytes_)),
          value_(std::move(other.value_)), valid_(other.valid_) {
        other.valid_ = false;
    }

    tktrie_iterator& operator=(tktrie_iterator&& other) noexcept {
        if (this != &other) {
            parent_ = other.parent_;
            key_bytes_ = std::move(other.key_bytes_);
            value_ = std::move(other.value_);
            valid_ = other.valid_;
            other.valid_ = false;
        }
        return *this;
    }

    Key key() const { return traits::from_bytes(key_bytes_); }
    const std::string& key_bytes() const noexcept { return key_bytes_; }
    const T& value() const noexcept { return value_; }
    T& value() noexcept { return value_; }
    value_type operator*() const { return {key(), value_}; }
    bool valid() const noexcept { return valid_; }
    explicit operator bool() const noexcept { return valid_; }

    bool operator==(const tktrie_iterator& other) const noexcept {
        if (!valid_ && !other.valid_) return true;
        if (valid_ != other.valid_) return false;
        return key_bytes_ == other.key_bytes_;
    }

    bool operator!=(const tktrie_iterator& other) const noexcept {
        return !(*this == other);
    }

    tktrie_iterator& operator++() {
        if (!valid_ || !parent_) {
            valid_ = false;
            return *this;
        }
        *this = parent_->next_after(key_bytes_);
        return *this;
    }

    tktrie_iterator operator++(int) {
        tktrie_iterator tmp(*this);
        ++(*this);
        return tmp;
    }

    static tktrie_iterator end_iterator() noexcept { return tktrie_iterator(); }
};

// =============================================================================
// Convenience type aliases
// =============================================================================

// Variable-length string keys
template <typename T, typename Allocator = std::allocator<uint64_t>>
using string_trie = tktrie<std::string, T, false, Allocator, 0>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_string_trie = tktrie<std::string, T, true, Allocator, 0>;

// Fixed-length 32-bit integer keys
template <typename T, typename Allocator = std::allocator<uint64_t>>
using int32_trie = tktrie<int32_t, T, false, Allocator, 4>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_int32_trie = tktrie<int32_t, T, true, Allocator, 4>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using uint32_trie = tktrie<uint32_t, T, false, Allocator, 4>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_uint32_trie = tktrie<uint32_t, T, true, Allocator, 4>;

// Fixed-length 64-bit integer keys
template <typename T, typename Allocator = std::allocator<uint64_t>>
using int64_trie = tktrie<int64_t, T, false, Allocator, 8>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_int64_trie = tktrie<int64_t, T, true, Allocator, 8>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using uint64_trie = tktrie<uint64_t, T, false, Allocator, 8>;

template <typename T, typename Allocator = std::allocator<uint64_t>>
using concurrent_uint64_trie = tktrie<uint64_t, T, true, Allocator, 8>;

}  // namespace gteitelbaum
