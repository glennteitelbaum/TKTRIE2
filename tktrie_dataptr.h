#pragma once

#include <atomic>
#include <cstring>
#include <memory>
#include <type_traits>

#include "tktrie_defines.h"

namespace gteitelbaum {

template <typename T>
static constexpr bool can_embed_v = 
    sizeof(T) <= sizeof(uint64_t) && 
    std::is_trivially_copyable_v<T>;

// Forward declaration
template <typename T, bool THREADED, typename Allocator, bool CAN_EMBED = can_embed_v<T>>
class dataptr;

// =============================================================================
// THREADED=true implementation (always uses pointer, never embeds)
// =============================================================================
template <typename T, typename Allocator>
class dataptr<T, true, Allocator, true> {
    std::atomic<uint64_t> bits_{0};
    
    using alloc_traits = std::allocator_traits<Allocator>;
    using value_alloc_t = typename alloc_traits::template rebind_alloc<T>;
    using value_alloc_traits = std::allocator_traits<value_alloc_t>;
    
    static T* get_ptr(uint64_t v) noexcept {
        return reinterpret_cast<T*>(v & PTR_MASK);
    }

public:
    dataptr() noexcept = default;
    
    ~dataptr() {
        T* ptr = get_ptr(bits_.load(std::memory_order_relaxed));
        if (ptr) {
            value_alloc_t alloc;
            std::destroy_at(ptr);
            value_alloc_traits::deallocate(alloc, ptr, 1);
        }
    }
    
    dataptr(const dataptr&) = delete;
    dataptr& operator=(const dataptr&) = delete;
    
    dataptr(dataptr&& other) noexcept {
        bits_.store(other.bits_.exchange(0, std::memory_order_acq_rel), 
                    std::memory_order_relaxed);
    }
    
    dataptr& operator=(dataptr&& other) noexcept {
        if (this != &other) {
            T* old = get_ptr(bits_.load(std::memory_order_relaxed));
            bits_.store(other.bits_.exchange(0, std::memory_order_acq_rel),
                       std::memory_order_relaxed);
            if (old) {
                value_alloc_t alloc;
                std::destroy_at(old);
                value_alloc_traits::deallocate(alloc, old, 1);
            }
        }
        return *this;
    }

    bool has_data() const noexcept {
        return get_ptr(bits_.load(std::memory_order_acquire)) != nullptr;
    }

    /**
     * Try to read value using CAS protocol
     * Returns false if WRITE_BIT set (caller should retry from root)
     * or if no data present
     */
    bool try_read(T& out) const noexcept {
        auto* self = const_cast<dataptr*>(this);
        
        while (true) {
            uint64_t old = self->bits_.load(std::memory_order_acquire);
            
            if (old & WRITE_BIT) return false;
            
            T* ptr = get_ptr(old);
            if (!ptr) return false;
            
            if (old & READ_BIT) {
                cpu_pause();
                continue;
            }
            
            if (self->bits_.compare_exchange_weak(old, old | READ_BIT,
                                                   std::memory_order_acq_rel,
                                                   std::memory_order_acquire)) {
                out = *ptr;
                self->bits_.fetch_and(~READ_BIT, std::memory_order_release);
                return true;
            }
        }
    }

    void begin_write() noexcept {
        bits_.fetch_or(WRITE_BIT, std::memory_order_acq_rel);
        while (bits_.load(std::memory_order_acquire) & READ_BIT) {
            cpu_pause();
        }
    }

    void set(const T& value) {
        value_alloc_t alloc;
        T* new_ptr = value_alloc_traits::allocate(alloc, 1);
        try {
            std::construct_at(new_ptr, value);
        } catch (...) {
            value_alloc_traits::deallocate(alloc, new_ptr, 1);
            throw;
        }
        
        T* old_ptr = get_ptr(bits_.load(std::memory_order_relaxed));
        bits_.store(reinterpret_cast<uint64_t>(new_ptr) | WRITE_BIT, 
                   std::memory_order_release);
        
        if (old_ptr) {
            std::destroy_at(old_ptr);
            value_alloc_traits::deallocate(alloc, old_ptr, 1);
        }
    }

    void set(T&& value) {
        value_alloc_t alloc;
        T* new_ptr = value_alloc_traits::allocate(alloc, 1);
        try {
            std::construct_at(new_ptr, std::move(value));
        } catch (...) {
            value_alloc_traits::deallocate(alloc, new_ptr, 1);
            throw;
        }
        
        T* old_ptr = get_ptr(bits_.load(std::memory_order_relaxed));
        bits_.store(reinterpret_cast<uint64_t>(new_ptr) | WRITE_BIT,
                   std::memory_order_release);
        
        if (old_ptr) {
            std::destroy_at(old_ptr);
            value_alloc_traits::deallocate(alloc, old_ptr, 1);
        }
    }

    void clear() noexcept {
        T* old_ptr = get_ptr(bits_.load(std::memory_order_relaxed));
        bits_.store(WRITE_BIT, std::memory_order_release);
        
        if (old_ptr) {
            value_alloc_t alloc;
            std::destroy_at(old_ptr);
            value_alloc_traits::deallocate(alloc, old_ptr, 1);
        }
    }

    void end_write() noexcept {
        bits_.fetch_and(~WRITE_BIT, std::memory_order_release);
    }

    uint64_t to_u64() const noexcept {
        return bits_.load(std::memory_order_relaxed);
    }

    void from_u64(uint64_t v) noexcept {
        bits_.store(v, std::memory_order_relaxed);
    }

    // Deep copy for trie copy constructor
    void deep_copy_from(const dataptr& other) {
        T* other_ptr = get_ptr(other.bits_.load(std::memory_order_relaxed));
        if (other_ptr) {
            set(*other_ptr);
            end_write();
        }
    }
};

// THREADED=true, large T (same implementation)
template <typename T, typename Allocator>
class dataptr<T, true, Allocator, false> : public dataptr<T, true, Allocator, true> {
    using dataptr<T, true, Allocator, true>::dataptr;
};

// =============================================================================
// THREADED=false, embeddable T
// =============================================================================
template <typename T, typename Allocator>
class dataptr<T, false, Allocator, true> {
    uint64_t bits_{0};
    bool has_value_{false};

public:
    dataptr() noexcept = default;
    ~dataptr() = default;
    
    dataptr(const dataptr& other) noexcept 
        : bits_(other.bits_), has_value_(other.has_value_) {}
    
    dataptr& operator=(const dataptr& other) noexcept {
        bits_ = other.bits_;
        has_value_ = other.has_value_;
        return *this;
    }
    
    dataptr(dataptr&& other) noexcept 
        : bits_(other.bits_), has_value_(other.has_value_) {
        other.has_value_ = false;
    }
    
    dataptr& operator=(dataptr&& other) noexcept {
        bits_ = other.bits_;
        has_value_ = other.has_value_;
        other.has_value_ = false;
        return *this;
    }

    bool has_data() const noexcept { return has_value_; }

    bool try_read(T& out) const noexcept {
        if (!has_value_) return false;
        std::memcpy(&out, &bits_, sizeof(T));
        return true;
    }

    void begin_write() noexcept { }
    
    void set(const T& value) noexcept {
        std::memcpy(&bits_, &value, sizeof(T));
        has_value_ = true;
    }
    
    void set(T&& value) noexcept {
        set(value);
    }
    
    void clear() noexcept {
        has_value_ = false;
        bits_ = 0;
    }
    
    void end_write() noexcept { }

    uint64_t to_u64() const noexcept { return bits_; }
    
    void from_u64(uint64_t v) noexcept { bits_ = v; }
    
    bool get_has_value() const noexcept { return has_value_; }
    void set_has_value(bool v) noexcept { has_value_ = v; }

    void deep_copy_from(const dataptr& other) noexcept {
        bits_ = other.bits_;
        has_value_ = other.has_value_;
    }
};

// =============================================================================
// THREADED=false, large T (uses pointer)
// =============================================================================
template <typename T, typename Allocator>
class dataptr<T, false, Allocator, false> {
    T* ptr_{nullptr};
    
    using alloc_traits = std::allocator_traits<Allocator>;
    using value_alloc_t = typename alloc_traits::template rebind_alloc<T>;
    using value_alloc_traits = std::allocator_traits<value_alloc_t>;

public:
    dataptr() noexcept = default;
    
    ~dataptr() {
        if (ptr_) {
            value_alloc_t alloc;
            std::destroy_at(ptr_);
            value_alloc_traits::deallocate(alloc, ptr_, 1);
        }
    }
    
    dataptr(const dataptr& other) : ptr_(nullptr) {
        if (other.ptr_) {
            value_alloc_t alloc;
            ptr_ = value_alloc_traits::allocate(alloc, 1);
            try {
                std::construct_at(ptr_, *other.ptr_);
            } catch (...) {
                value_alloc_traits::deallocate(alloc, ptr_, 1);
                ptr_ = nullptr;
                throw;
            }
        }
    }
    
    dataptr& operator=(const dataptr& other) {
        if (this != &other) {
            dataptr tmp(other);
            std::swap(ptr_, tmp.ptr_);
        }
        return *this;
    }
    
    dataptr(dataptr&& other) noexcept : ptr_(other.ptr_) {
        other.ptr_ = nullptr;
    }
    
    dataptr& operator=(dataptr&& other) noexcept {
        if (this != &other) {
            if (ptr_) {
                value_alloc_t alloc;
                std::destroy_at(ptr_);
                value_alloc_traits::deallocate(alloc, ptr_, 1);
            }
            ptr_ = other.ptr_;
            other.ptr_ = nullptr;
        }
        return *this;
    }

    bool has_data() const noexcept { return ptr_ != nullptr; }

    bool try_read(T& out) const noexcept {
        if (!ptr_) return false;
        out = *ptr_;
        return true;
    }

    void begin_write() noexcept { }
    
    void set(const T& value) {
        value_alloc_t alloc;
        if (!ptr_) {
            ptr_ = value_alloc_traits::allocate(alloc, 1);
            try {
                std::construct_at(ptr_, value);
            } catch (...) {
                value_alloc_traits::deallocate(alloc, ptr_, 1);
                ptr_ = nullptr;
                throw;
            }
        } else {
            *ptr_ = value;
        }
    }
    
    void set(T&& value) {
        value_alloc_t alloc;
        if (!ptr_) {
            ptr_ = value_alloc_traits::allocate(alloc, 1);
            try {
                std::construct_at(ptr_, std::move(value));
            } catch (...) {
                value_alloc_traits::deallocate(alloc, ptr_, 1);
                ptr_ = nullptr;
                throw;
            }
        } else {
            *ptr_ = std::move(value);
        }
    }
    
    void clear() {
        if (ptr_) {
            value_alloc_t alloc;
            std::destroy_at(ptr_);
            value_alloc_traits::deallocate(alloc, ptr_, 1);
            ptr_ = nullptr;
        }
    }
    
    void end_write() noexcept { }

    uint64_t to_u64() const noexcept { 
        return reinterpret_cast<uint64_t>(ptr_); 
    }
    
    void from_u64(uint64_t v) noexcept { 
        ptr_ = reinterpret_cast<T*>(v); 
    }

    void deep_copy_from(const dataptr& other) {
        if (other.ptr_) {
            set(*other.ptr_);
        }
    }
};

}  // namespace gteitelbaum
