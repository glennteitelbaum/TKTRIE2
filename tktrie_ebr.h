#pragma once

#include <atomic>
#include <thread>

namespace gteitelbaum {

inline size_t thread_slot_hash(size_t max_slots) noexcept {
    std::hash<std::thread::id> hasher;
    return hasher(std::this_thread::get_id()) % max_slots;
}

}  // namespace gteitelbaum
