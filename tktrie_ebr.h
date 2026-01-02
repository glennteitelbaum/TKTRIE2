#pragma once

#include <atomic>
#include <array>
#include <vector>
#include <thread>
#include <functional>
#include <memory>
#include <mutex>

#include "tktrie_defines.h"

namespace gteitelbaum {

/**
 * Epoch-Based Reclamation (EBR)
 * 
 * Protocol:
 * - Global epoch counter (0, 1, 2, ...)
 * - Readers: enter_epoch() on start, exit_epoch() on end
 * - Writers: retire() nodes with deleter, periodically try_reclaim()
 * - Nodes retired in epoch N can be freed when all threads have exited epoch N
 * 
 * This is a GLOBAL singleton shared by all tries to avoid thread_local issues.
 */

// Maximum concurrent threads (can be increased)
static constexpr size_t EBR_MAX_THREADS = 128;

// Number of epochs to track (we use 3: current, current-1, current-2)
static constexpr size_t EBR_NUM_EPOCHS = 3;

/**
 * Per-thread epoch state
 */
struct alignas(64) ebr_thread_slot {
    std::atomic<uint64_t> epoch{0};      // Current epoch when active, 0 when inactive
    std::atomic<bool> active{false};      // Is thread currently in a read operation?
    std::atomic<bool> in_use{false};      // Is this slot claimed by a thread?
    char padding[64 - sizeof(std::atomic<uint64_t>) - 2*sizeof(std::atomic<bool>)];
};

/**
 * Retired node entry - stores pointer + deleter
 */
struct retired_node {
    void* ptr;
    void (*deleter)(void*);
};

/**
 * Global EBR manager (singleton)
 */
class ebr_global {
private:
    std::atomic<uint64_t> global_epoch_{1};  // Start at 1 so 0 means "not active"
    std::array<ebr_thread_slot, EBR_MAX_THREADS> slots_{};
    
    // Retire lists per epoch (circular buffer of 3)
    std::array<std::vector<retired_node>, EBR_NUM_EPOCHS> retire_lists_;
    std::array<std::mutex, EBR_NUM_EPOCHS> retire_mutexes_;
    
    // Find minimum epoch across all active threads
    uint64_t min_active_epoch() const noexcept {
        uint64_t min_epoch = global_epoch_.load(std::memory_order_acquire);
        
        for (const auto& slot : slots_) {
            if (slot.active.load(std::memory_order_acquire)) {
                uint64_t thread_epoch = slot.epoch.load(std::memory_order_acquire);
                if (thread_epoch > 0 && thread_epoch < min_epoch) {
                    min_epoch = thread_epoch;
                }
            }
        }
        return min_epoch;
    }

    ebr_global() = default;

public:
    static ebr_global& instance() {
        static ebr_global inst;
        return inst;
    }
    
    ~ebr_global() {
        // Free all remaining retired nodes
        for (size_t i = 0; i < EBR_NUM_EPOCHS; ++i) {
            for (auto& rn : retire_lists_[i]) {
                if (rn.deleter && rn.ptr) {
                    rn.deleter(rn.ptr);
                }
            }
            retire_lists_[i].clear();
        }
    }
    
    ebr_global(const ebr_global&) = delete;
    ebr_global& operator=(const ebr_global&) = delete;
    
    /**
     * Acquire a thread slot (call once per thread, typically via thread_local)
     * Returns slot index, or -1 if no slots available
     */
    int acquire_slot() noexcept {
        for (size_t i = 0; i < EBR_MAX_THREADS; ++i) {
            bool expected = false;
            if (slots_[i].in_use.compare_exchange_strong(expected, true,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                return static_cast<int>(i);
            }
        }
        return -1;  // No slots available
    }
    
    /**
     * Release a thread slot (call when thread exits)
     */
    void release_slot(int slot_idx) noexcept {
        if (slot_idx >= 0 && slot_idx < static_cast<int>(EBR_MAX_THREADS)) {
            slots_[slot_idx].active.store(false, std::memory_order_release);
            slots_[slot_idx].epoch.store(0, std::memory_order_release);
            slots_[slot_idx].in_use.store(false, std::memory_order_release);
        }
    }
    
    /**
     * Enter epoch (start of read operation)
     */
    void enter_epoch(int slot_idx) noexcept {
        if (slot_idx < 0) return;
        
        uint64_t epoch = global_epoch_.load(std::memory_order_acquire);
        slots_[slot_idx].epoch.store(epoch, std::memory_order_release);
        slots_[slot_idx].active.store(true, std::memory_order_release);
        
        // Re-read epoch in case it advanced
        std::atomic_thread_fence(std::memory_order_seq_cst);
        epoch = global_epoch_.load(std::memory_order_acquire);
        slots_[slot_idx].epoch.store(epoch, std::memory_order_release);
    }
    
    /**
     * Exit epoch (end of read operation)
     */
    void exit_epoch(int slot_idx) noexcept {
        if (slot_idx < 0) return;
        slots_[slot_idx].active.store(false, std::memory_order_release);
    }
    
    /**
     * Retire a pointer with its deleter (defer deletion until safe)
     */
    void retire(void* ptr, void (*deleter)(void*)) {
        if (!ptr || !deleter) return;
        
        uint64_t epoch = global_epoch_.load(std::memory_order_acquire);
        size_t list_idx = epoch % EBR_NUM_EPOCHS;
        
        std::lock_guard<std::mutex> lock(retire_mutexes_[list_idx]);
        retire_lists_[list_idx].push_back({ptr, deleter});
    }
    
    /**
     * Try to reclaim nodes from old epochs
     * Call periodically from writers
     */
    void try_reclaim() {
        uint64_t current = global_epoch_.load(std::memory_order_acquire);
        uint64_t min_active = min_active_epoch();
        
        // Can reclaim epochs older than min_active
        // Check epochs current-2 and current-1
        for (uint64_t old_epoch = current > 2 ? current - 2 : 1; old_epoch < min_active; ++old_epoch) {
            size_t list_idx = old_epoch % EBR_NUM_EPOCHS;
            
            std::vector<retired_node> to_delete;
            {
                std::lock_guard<std::mutex> lock(retire_mutexes_[list_idx]);
                to_delete = std::move(retire_lists_[list_idx]);
                retire_lists_[list_idx].clear();
            }
            
            for (auto& rn : to_delete) {
                if (rn.deleter && rn.ptr) {
                    rn.deleter(rn.ptr);
                }
            }
        }
        
        // Advance epoch periodically
        // This is a simple heuristic - advance if we've accumulated enough retires
        size_t current_list_idx = current % EBR_NUM_EPOCHS;
        size_t retire_count = 0;
        {
            std::lock_guard<std::mutex> lock(retire_mutexes_[current_list_idx]);
            retire_count = retire_lists_[current_list_idx].size();
        }
        
        if (retire_count >= 64) {
            // Try to advance epoch
            global_epoch_.compare_exchange_strong(current, current + 1,
                std::memory_order_acq_rel, std::memory_order_relaxed);
        }
    }
    
    /**
     * Force advance epoch (for testing)
     */
    void advance_epoch() {
        global_epoch_.fetch_add(1, std::memory_order_acq_rel);
    }
    
    /**
     * Get current epoch (for debugging)
     */
    uint64_t current_epoch() const noexcept {
        return global_epoch_.load(std::memory_order_acquire);
    }
};

/**
 * RAII guard for epoch entry/exit
 */
class ebr_guard {
    int slot_idx_;
    
public:
    explicit ebr_guard(int slot_idx) noexcept
        : slot_idx_(slot_idx) {
        ebr_global::instance().enter_epoch(slot_idx_);
    }
    
    ~ebr_guard() {
        ebr_global::instance().exit_epoch(slot_idx_);
    }
    
    ebr_guard(const ebr_guard&) = delete;
    ebr_guard& operator=(const ebr_guard&) = delete;
    
    ebr_guard(ebr_guard&& other) noexcept 
        : slot_idx_(other.slot_idx_) {
        other.slot_idx_ = -1;
    }
    
    ebr_guard& operator=(ebr_guard&& other) noexcept {
        if (this != &other) {
            ebr_global::instance().exit_epoch(slot_idx_);
            slot_idx_ = other.slot_idx_;
            other.slot_idx_ = -1;
        }
        return *this;
    }
};

/**
 * Thread-local slot - acquires slot on first use, releases on thread exit
 */
class ebr_thread_slot_holder {
    int slot_idx_;
    
public:
    ebr_thread_slot_holder() 
        : slot_idx_(ebr_global::instance().acquire_slot()) {}
    
    ~ebr_thread_slot_holder() {
        if (slot_idx_ >= 0) {
            ebr_global::instance().release_slot(slot_idx_);
        }
    }
    
    ebr_thread_slot_holder(const ebr_thread_slot_holder&) = delete;
    ebr_thread_slot_holder& operator=(const ebr_thread_slot_holder&) = delete;
    
    int slot_idx() const noexcept { return slot_idx_; }
    
    ebr_guard guard() noexcept {
        return ebr_guard(slot_idx_);
    }
};

/**
 * Get thread-local EBR slot holder
 */
inline ebr_thread_slot_holder& get_ebr_slot() {
    static thread_local ebr_thread_slot_holder holder;
    return holder;
}

}  // namespace gteitelbaum
