#pragma once

#include <string>
#include <utility>

#include "tktrie_defines.h"
#include "tktrie_traits.h"

namespace gteitelbaum {

template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie;

template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie_iterator {
public:
    using trie_type = tktrie<Key, T, THREADED, Allocator>;
    using traits = tktrie_traits<Key>;
    using value_type = std::pair<Key, T>;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::forward_iterator_tag;

private:
    friend class tktrie<Key, T, THREADED, Allocator>;
    const trie_type* parent_;
    std::string key_bytes_;
    T value_;
    bool valid_;

public:
    tktrie_iterator() noexcept : parent_(nullptr), valid_(false) {}
    tktrie_iterator(const trie_type* parent, std::string_view key_bytes, const T& value)
        : parent_(parent), key_bytes_(key_bytes), value_(value), valid_(true) {}
    tktrie_iterator(const tktrie_iterator& other)
        : parent_(other.parent_), key_bytes_(other.key_bytes_), value_(other.value_), valid_(other.valid_) {}
    tktrie_iterator& operator=(const tktrie_iterator& other) {
        if (this != &other) { parent_ = other.parent_; key_bytes_ = other.key_bytes_; value_ = other.value_; valid_ = other.valid_; }
        return *this;
    }
    tktrie_iterator(tktrie_iterator&& other) noexcept
        : parent_(other.parent_), key_bytes_(std::move(other.key_bytes_)), value_(std::move(other.value_)), valid_(other.valid_) { other.valid_ = false; }
    tktrie_iterator& operator=(tktrie_iterator&& other) noexcept {
        if (this != &other) { parent_ = other.parent_; key_bytes_ = std::move(other.key_bytes_); value_ = std::move(other.value_); valid_ = other.valid_; other.valid_ = false; }
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
    bool operator!=(const tktrie_iterator& other) const noexcept { return !(*this == other); }
    tktrie_iterator& operator++() {
        if (!valid_ || !parent_) { valid_ = false; return *this; }
        *this = parent_->next_after(key_bytes_);
        return *this;
    }
    tktrie_iterator operator++(int) { tktrie_iterator tmp(*this); ++(*this); return tmp; }
    static tktrie_iterator end_iterator() noexcept { return tktrie_iterator(); }
};

}  // namespace gteitelbaum
