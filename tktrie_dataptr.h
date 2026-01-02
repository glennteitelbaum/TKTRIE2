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
// THREADED=true implementation - simplified for COW+EBR
// With COW, each modification creates new nodes with new dataptrs.
// Readers see immutable data protected by EBR. No READ_BIT/WRITE_BIT needed.
// =============================================================================
template <typename T, typename Allocator>
class dataptr<T, true, Allocator, true> {
    std::atomic<T*> ptr_{nullptr};
    
    using alloc_traits = std::allocator_traits<Allocator>;
    using value_alloc_t = typename alloc_traits::template rebind_alloc<T>;
    using value_alloc_traits = std::allocator_traits<value_alloc_t>;

public:
    dataptr() noexcept = default;
    
    ~dataptr() {
        T* ptr = ptr_.load(std::memory_order_relaxed);
        if (ptr) {
            value_alloc_t alloc;
            std::destroy_at(ptr);
            value_alloc_traits::deallocate(alloc, ptr, 1);
        }
    }
    
    dataptr(const dataptr&) = delete;
    dataptr& operator=(const dataptr&) = delete;
    
    dataptr(dataptr&& other) noexcept {
        ptr_.store(other.ptr_.exchange(nullptr, std::memory_order_acq_rel), 
                   std::memory_order_relaxed);
    }
    
    dataptr& operator=(dataptr&& other) noexcept {
        if (this != &other) {
            T* old = ptr_.load(std::memory_order_relaxed);
            ptr_.store(other.ptr_.exchange(nullptr, std::memory_order_acq_rel),
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
        return ptr_.load(std::memory_order_acquire) != nullptr;
    }

    /**
     * Read value - with COW+EBR, data is immutable once visible
     */
    bool try_read(T& out) const noexcept {
        T* ptr = ptr_.load(std::memory_order_acquire);
        if (!ptr) return false;
        out = *ptr;
        return true;
    }

    // begin_write/end_write are no-ops with COW - kept for API compatibility
    void begin_write() noexcept { }
    void end_write() noexcept { }

    void set(const T& value) {
        value_alloc_t alloc;
        T* new_ptr = value_alloc_traits::allocate(alloc, 1);
        try {
            std::construct_at(new_ptr, value);
        } catch (...) {
            value_alloc_traits::deallocate(alloc, new_ptr, 1);
            throw;
        }
        
        T* old_ptr = ptr_.load(std::memory_order_relaxed);
        ptr_.store(new_ptr, std::memory_order_release);
        
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
        
        T* old_ptr = ptr_.load(std::memory_order_relaxed);
        ptr_.store(new_ptr, std::memory_order_release);
        
        if (old_ptr) {
            std::destroy_at(old_ptr);
            value_alloc_traits::deallocate(alloc, old_ptr, 1);
        }
    }

    void clear() noexcept {
        T* old_ptr = ptr_.load(std::memory_order_relaxed);
        ptr_.store(nullptr, std::memory_order_release);
        
        if (old_ptr) {
            value_alloc_t alloc;
            std::destroy_at(old_ptr);
            value_alloc_traits::deallocate(alloc, old_ptr, 1);
        }
    }

    uint64_t to_u64() const noexcept {
        return reinterpret_cast<uint64_t>(ptr_.load(std::memory_order_relaxed));
    }

    void from_u64(uint64_t v) noexcept {
        ptr_.store(reinterpret_cast<T*>(v), std::memory_order_relaxed);
    }

    // Deep copy - with COW, source is immutable
    void deep_copy_from(const dataptr& other) {
        T* src = other.ptr_.load(std::memory_order_acquire);
        if (src) {
            set(*src);
        }
    }
};

// THREADED=true, large T (same implementation)
template <typename T, typename Allocator>
class dataptr<T, true, Allocator, false> : public dataptr<T, true, Allocator, true> {
    using dataptr<T, true, Allocator, true>::dataptr;
};

// =============================================================================
// THREADED=false, embeddable T - use pointer to avoid size issues
// =============================================================================
template <typename T, typename Allocator>
class dataptr<T, false, Allocator, true> {
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
