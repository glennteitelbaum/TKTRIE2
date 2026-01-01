#pragma once

#include <string>
#include <utility>

#include "tktrie_defines.h"
#include "tktrie_traits.h"

namespace gteitelbaum {

// Forward declaration
template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie;

/**
 * Iterator for tktrie
 * Holds a copy of key (as bytes) and value
 * Follows reader protocol for thread safety
 */
template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie_iterator {
public:
    using trie_type = tktrie<Key, T, THREADED, Allocator>;
    using traits = tktrie_traits<Key>;
    using value_type = std::pair<Key, T>;
    using pointer = value_type*;
    using reference = value_type&;
    using difference_type = std::ptrdiff_t;
    using iterator_category = std::forward_iterator_tag;

private:
    friend class tktrie<Key, T, THREADED, Allocator>;
    
    const trie_type* parent_;
    std::string key_bytes_;   // Key stored as bytes
    T value_;                 // Copy of value
    bool valid_;

public:
    // Default constructor - end iterator
    tktrie_iterator() noexcept 
        : parent_(nullptr), valid_(false) {}

    // Construct from found key/value
    tktrie_iterator(const trie_type* parent, std::string_view key_bytes, const T& value)
        : parent_(parent)
        , key_bytes_(key_bytes)
        , value_(value)
        , valid_(true) {}

    // Copy constructor
    tktrie_iterator(const tktrie_iterator& other)
        : parent_(other.parent_)
        , key_bytes_(other.key_bytes_)
        , value_(other.value_)
        , valid_(other.valid_) {}

    // Copy assignment
    tktrie_iterator& operator=(const tktrie_iterator& other) {
        if (this != &other) {
            parent_ = other.parent_;
            key_bytes_ = other.key_bytes_;
            value_ = other.value_;
            valid_ = other.valid_;
        }
        return *this;
    }

    // Move constructor
    tktrie_iterator(tktrie_iterator&& other) noexcept
        : parent_(other.parent_)
        , key_bytes_(std::move(other.key_bytes_))
        , value_(std::move(other.value_))
        , valid_(other.valid_) {
        other.valid_ = false;
    }

    // Move assignment
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

    /**
     * Get key (converted from bytes via traits)
     */
    Key key() const {
        return traits::from_bytes(key_bytes_);
    }

    /**
     * Get key as raw bytes
     */
    const std::string& key_bytes() const noexcept {
        return key_bytes_;
    }

    /**
     * Get value (const reference to copy)
     */
    const T& value() const noexcept {
        return value_;
    }

    /**
     * Get value (mutable reference - note: modifying does not update trie)
     */
    T& value() noexcept {
        return value_;
    }

    /**
     * Dereference - returns pair
     */
    value_type operator*() const {
        return {key(), value_};
    }

    /**
     * Check if iterator is valid
     */
    bool valid() const noexcept {
        return valid_;
    }

    /**
     * Explicit bool conversion
     */
    explicit operator bool() const noexcept {
        return valid_;
    }

    /**
     * Equality comparison
     */
    bool operator==(const tktrie_iterator& other) const noexcept {
        if (!valid_ && !other.valid_) return true;
        if (valid_ != other.valid_) return false;
        return key_bytes_ == other.key_bytes_;
    }

    bool operator!=(const tktrie_iterator& other) const noexcept {
        return !(*this == other);
    }

    /**
     * Prefix increment - move to next key
     * For THREADED, follows reader protocol with retry
     */
    tktrie_iterator& operator++() {
        if (!valid_ || !parent_) {
            valid_ = false;
            return *this;
        }
        
        // Find next key after current key_bytes_
        *this = parent_->next_after(key_bytes_);
        return *this;
    }

    /**
     * Postfix increment
     */
    tktrie_iterator operator++(int) {
        tktrie_iterator tmp(*this);
        ++(*this);
        return tmp;
    }

    /**
     * Static end iterator factory
     */
    static tktrie_iterator end_iterator() noexcept {
        return tktrie_iterator();
    }
};

}  // namespace gteitelbaum
