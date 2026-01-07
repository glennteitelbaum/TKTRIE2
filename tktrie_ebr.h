#pragma once

// =============================================================================
// TKTRIE EBR - Epoch-Based Reclamation (fully per-trie, no globals)
// =============================================================================
//
// Each tktrie instance has its own:
//   - epoch_ : incremented on writes
//   - reader_epochs_[MAX_SLOTS] : tracks active reader epochs (0 = inactive)
//   - retired list : nodes waiting to be freed
//
// Reader protocol:
//   enter() - hash thread to slot, store current epoch
//   exit()  - clear slot (store 0)
//
// Reclamation:
//   Compute min_reader_epoch across slots, free nodes retired before that
//
// Slot collisions (two threads hash to same slot):
//   Conservative - we see older epoch, delay reclamation. Safe.
//
// =============================================================================

#include <atomic>
#include <thread>

namespace gteitelbaum {

// Hash thread ID to slot index
inline size_t thread_slot_hash(size_t max_slots) noexcept {
    std::hash<std::thread::id> hasher;
    return hasher(std::this_thread::get_id()) % max_slots;
}

}  // namespace gteitelbaum
