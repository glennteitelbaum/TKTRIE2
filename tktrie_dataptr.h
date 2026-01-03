#pragma once

#include <atomic>
#include <cstring>
#include <memory>
#include <type_traits>

#include "tktrie_defines.h"

namespace gteitelbaum {

template <typename T, bool THREADED, typename Allocator>
class dataptr {
    using alloc_traits = std::allocator_traits<Allocator>;
    using value_alloc_t = typename alloc_traits::template rebind_alloc<T>;
    using value_alloc_traits = std::allocator_traits<value_alloc_t>;

    std::conditional_t<THREADED, std::atomic<T*>, T*> ptr_{nullptr};

public:
    dataptr() noexcept = default;
    
    ~dataptr() {
        T* p = load_ptr();
        if (p) {
            value_alloc_t alloc;
            std::destroy_at(p);
            value_alloc_traits::deallocate(alloc, p, 1);
        }
    }
    
    dataptr(const dataptr&) = delete;
    dataptr& operator=(const dataptr&) = delete;
    
    dataptr(dataptr&& other) noexcept { store_ptr(other.exchange_ptr(nullptr)); }
    
    dataptr& operator=(dataptr&& other) noexcept {
        if (this != &other) {
            T* old = exchange_ptr(other.exchange_ptr(nullptr));
            if (old) {
                value_alloc_t alloc;
                std::destroy_at(old);
                value_alloc_traits::deallocate(alloc, old, 1);
            }
        }
        return *this;
    }

    bool has_data() const noexcept { return load_ptr() != nullptr; }

    bool try_read(T& out) const noexcept {
        T* p = load_ptr();
        if (!p) return false;
        out = *p;
        return true;
    }

    void set(const T& value) {
        value_alloc_t alloc;
        T* new_ptr = value_alloc_traits::allocate(alloc, 1);
        try { std::construct_at(new_ptr, value); }
        catch (...) { value_alloc_traits::deallocate(alloc, new_ptr, 1); throw; }
        T* old = exchange_ptr(new_ptr);
        if (old) { std::destroy_at(old); value_alloc_traits::deallocate(alloc, old, 1); }
    }

    void set(T&& value) {
        value_alloc_t alloc;
        T* new_ptr = value_alloc_traits::allocate(alloc, 1);
        try { std::construct_at(new_ptr, std::move(value)); }
        catch (...) { value_alloc_traits::deallocate(alloc, new_ptr, 1); throw; }
        T* old = exchange_ptr(new_ptr);
        if (old) { std::destroy_at(old); value_alloc_traits::deallocate(alloc, old, 1); }
    }

    void clear() noexcept {
        T* old = exchange_ptr(nullptr);
        if (old) {
            value_alloc_t alloc;
            std::destroy_at(old);
            value_alloc_traits::deallocate(alloc, old, 1);
        }
    }

    void deep_copy_from(const dataptr& other) {
        T* src = other.load_ptr();
        if (src) set(*src);
        else clear();
    }

private:
    T* load_ptr() const noexcept {
        if constexpr (THREADED) return ptr_.load(std::memory_order_acquire);
        else return ptr_;
    }
    void store_ptr(T* p) noexcept {
        if constexpr (THREADED) ptr_.store(p, std::memory_order_release);
        else ptr_ = p;
    }
    T* exchange_ptr(T* p) noexcept {
        if constexpr (THREADED) return ptr_.exchange(p, std::memory_order_acq_rel);
        else { T* old = ptr_; ptr_ = p; return old; }
    }
};

}  // namespace gteitelbaum
