#pragma once

#include <cstring>
#include <string>
#include <string_view>
#include <type_traits>

#include "tktrie_defines.h"

namespace gteitelbaum {

template <typename Key>
struct tktrie_traits;

// String keys - variable length
template <>
struct tktrie_traits<std::string> {
    static constexpr size_t fixed_len = 0;
    
    static std::string_view to_bytes(const std::string& k) noexcept {
        return k;
    }
    
    static std::string from_bytes(std::string_view bytes) {
        return std::string(bytes);
    }
};

// std::string_view keys - variable length
template <>
struct tktrie_traits<std::string_view> {
    static constexpr size_t fixed_len = 0;
    
    static std::string_view to_bytes(std::string_view k) noexcept {
        return k;
    }
    
    static std::string from_bytes(std::string_view bytes) {
        return std::string(bytes);
    }
};

// Integral keys - fixed length, sorted by value
template <typename T>
    requires std::is_integral_v<T>
struct tktrie_traits<T> {
    static constexpr size_t fixed_len = sizeof(T);
    using unsigned_type = std::make_unsigned_t<T>;
    
    static std::string to_bytes(T k) {
        unsigned_type sortable;
        if constexpr (std::is_signed_v<T>) {
            // Flip sign bit so negative < positive in byte comparison
            // -128 -> 0, -1 -> 127, 0 -> 128, 127 -> 255 (for int8_t)
            sortable = static_cast<unsigned_type>(k) ^ (unsigned_type{1} << (sizeof(T) * 8 - 1));
        } else {
            sortable = k;
        }
        
        // Convert to big-endian for correct lexicographic ordering
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
            // Reverse the sign bit flip
            return static_cast<T>(sortable ^ (unsigned_type{1} << (sizeof(T) * 8 - 1)));
        } else {
            return static_cast<T>(sortable);
        }
    }
};

// Convenience aliases for common types
using string_traits = tktrie_traits<std::string>;
using int32_traits = tktrie_traits<int32_t>;
using uint32_traits = tktrie_traits<uint32_t>;
using int64_traits = tktrie_traits<int64_t>;
using uint64_traits = tktrie_traits<uint64_t>;

}  // namespace gteitelbaum
