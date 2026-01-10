#pragma once

// =============================================================================
// TKTRIE ITERATOR - Snapshot iterator with cached key/value
// =============================================================================
// NOTE: This file is included inside namespace gteitelbaum in tktrie.h
//
// Iterator is a snapshot containing:
//   - parent_: pointer to trie (const or non-const based on CONST)
//   - key_bytes_: cached key in byte form
//   - value_: cached value
//
// Iterators remain valid even if their key is deleted from the trie.
// ++ and -- find next/prev key relative to cached key.
//
// Template parameters:
//   CONST = false: can call erase()/insert() via parent
//   CONST = true:  read-only
//   REVERSE = false: ++ moves toward larger keys
//   REVERSE = true:  ++ moves toward smaller keys

// Forward declaration
template <typename Key, typename T, bool THREADED, typename Allocator>
class tktrie;

template <typename Key, typename T, bool THREADED, typename Allocator, bool CONST, bool REVERSE>
class tktrie_iterator_impl {
public:
    using trie_t = tktrie<Key, T, THREADED, Allocator>;
    using traits = tktrie_traits<Key>;
    static constexpr size_t FIXED_LEN = traits::FIXED_LEN;
    
    // Parent pointer type depends on CONST
    using trie_ptr_t = std::conditional_t<CONST, const trie_t*, trie_t*>;
    
    // Standard iterator type aliases
    using iterator_category = std::bidirectional_iterator_tag;
    using value_type = std::pair<const Key, T>;
    using difference_type = std::ptrdiff_t;
    using pointer = const value_type*;
    using reference = const value_type&;

private:
    // Core data: parent trie, cached key, cached value
    trie_ptr_t parent_ = nullptr;
    std::string key_bytes_;  // Cached key in byte form
    T value_{};              // Cached value
    bool valid_ = false;     // false = end() iterator

public:
    tktrie_iterator_impl() = default;
    
    // Constructor with parent only (for end())
    explicit tktrie_iterator_impl(trie_ptr_t t) : parent_(t), valid_(false) {}
    
    // Constructor with cached key/value (for find(), begin(), etc.)
    tktrie_iterator_impl(trie_ptr_t t, std::string_view kb, const T& v)
        : parent_(t), key_bytes_(kb), value_(v), valid_(true) {}
    
    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------
    static tktrie_iterator_impl make_begin(trie_ptr_t t) {
        std::string kb;
        T v{};
        bool found;
        if constexpr (REVERSE) {
            found = t->find_last_bytes(kb, v);
        } else {
            found = t->find_first_bytes(kb, v);
        }
        if (found) return tktrie_iterator_impl(t, kb, v);
        return tktrie_iterator_impl(t);
    }
    
    static tktrie_iterator_impl make_end(trie_ptr_t t) {
        return tktrie_iterator_impl(t);
    }

    // -------------------------------------------------------------------------
    // Access cached data
    // -------------------------------------------------------------------------
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
    
    // Access parent trie
    trie_ptr_t parent() const { return parent_; }
    
    // Access raw key bytes (for internal use by trie)
    const std::string& key_bytes() const { return key_bytes_; }

    // -------------------------------------------------------------------------
    // Comparison
    // -------------------------------------------------------------------------
    bool operator==(const tktrie_iterator_impl& o) const {
        if (!valid_ && !o.valid_) return true;
        if (valid_ != o.valid_) return false;
        return key_bytes_ == o.key_bytes_;
    }
    bool operator!=(const tktrie_iterator_impl& o) const { return !(*this == o); }

    // -------------------------------------------------------------------------
    // Increment/Decrement
    // Both forward and reverse support ++ and --
    // Forward:  ++ finds next larger key, -- finds next smaller key
    // Reverse:  ++ finds next smaller key, -- finds next larger key
    // -------------------------------------------------------------------------
    tktrie_iterator_impl& operator++() {
        if (!parent_) { valid_ = false; return *this; }
        
        std::string kb;
        T v{};
        bool found;
        if constexpr (REVERSE) {
            found = parent_->find_less_bytes(key_bytes_, kb, v);
        } else {
            found = parent_->find_greater_bytes(key_bytes_, kb, v);
        }
        if (found) {
            key_bytes_ = std::move(kb);
            value_ = std::move(v);
            valid_ = true;
        } else {
            valid_ = false;
        }
        return *this;
    }
    
    tktrie_iterator_impl operator++(int) {
        tktrie_iterator_impl tmp = *this;
        ++(*this);
        return tmp;
    }
    
    tktrie_iterator_impl& operator--() {
        if (!parent_) { valid_ = false; return *this; }
        
        std::string kb;
        T v{};
        bool found;
        if constexpr (REVERSE) {
            found = parent_->find_greater_bytes(key_bytes_, kb, v);
        } else {
            found = parent_->find_less_bytes(key_bytes_, kb, v);
        }
        if (found) {
            key_bytes_ = std::move(kb);
            value_ = std::move(v);
            valid_ = true;
        } else {
            valid_ = false;
        }
        return *this;
    }
    
    tktrie_iterator_impl operator--(int) {
        tktrie_iterator_impl tmp = *this;
        --(*this);
        return tmp;
    }

    // -------------------------------------------------------------------------
    // Modification via parent (non-const only)
    // These don't change iterator state - iterator is a snapshot
    // -------------------------------------------------------------------------
    
    // Erase current key via parent->erase(key())
    // Iterator keeps its cached key/value (now a stale snapshot)
    // Returns true if element was erased
    bool erase() requires (!CONST) {
        if (!valid_ || !parent_) return false;
        return parent_->erase(key());
    }
    
    // Insert via parent->insert()
    auto insert(const std::pair<const Key, T>& kv) requires (!CONST) {
        if (!parent_) {
            return std::make_pair(tktrie_iterator_impl{}, false);
        }
        return parent_->insert(kv);
    }
    
    // -------------------------------------------------------------------------
    // Conversion: non-const -> const
    // -------------------------------------------------------------------------
    operator tktrie_iterator_impl<Key, T, THREADED, Allocator, true, REVERSE>() const 
        requires (!CONST) 
    {
        if (valid_) {
            return tktrie_iterator_impl<Key, T, THREADED, Allocator, true, REVERSE>(
                parent_, key_bytes_, value_);
        }
        return tktrie_iterator_impl<Key, T, THREADED, Allocator, true, REVERSE>(parent_);
    }
};
