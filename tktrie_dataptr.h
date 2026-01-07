#pragma once

#include <atomic>
#include <cstring>
#include <memory>
#include <type_traits>

#include "tktrie_defines.h"

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
