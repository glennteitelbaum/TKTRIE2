#pragma once

#include <atomic>
#include <array>
#include <vector>
#include <thread>
#include <functional>
#include <memory>

#include "tktrie_defines.h"

namespace gteitelbaum {

/**
 * Epoch-Based Reclamation (EBR)
 * 
 * Protocol:
 * - Global epoch counter (0, 1, 2, ...)
 * - Readers: enter_epoch() on start, exit_epoch() on end
 * - Writers: retire() nodes, periodically try_reclaim()
 * - Nodes retired in epoch N can be freed when all threads have exited epoch N
 * 
 * Thread tracking uses a fixed-size array of thread slots.
 * Each slot tracks: active flag, current epoch when active.
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
 * Global EBR state
 */
template <typename Deleter>
class ebr_manager {
public:
    using deleter_t = Deleter;
    
private:
    std::atomic<uint64_t> global_epoch_{1};  // Start at 1 so 0 means "not active"
    std::array<ebr_thread_slot, EBR_MAX_THREADS> slots_{};
    
    // Retire lists per epoch (circular buffer of 3)
    std::array<std::vector<void*>, EBR_NUM_EPOCHS> retire_lists_;
    std::array<std::mutex, EBR_NUM_EPOCHS> retire_mutexes_;
    
    deleter_t deleter_;
    
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

public:
    explicit ebr_manager(deleter_t deleter = deleter_t{}) 
        : deleter_(std::move(deleter)) {}
    
    ~ebr_manager() {
        // Free all remaining retired nodes
        for (size_t i = 0; i < EBR_NUM_EPOCHS; ++i) {
            for (void* ptr : retire_lists_[i]) {
                deleter_(ptr);
            }
            retire_lists_[i].clear();
        }
    }
    
    ebr_manager(const ebr_manager&) = delete;
    ebr_manager& operator=(const ebr_manager&) = delete;
    
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
     * Retire a pointer (defer deletion until safe)
     */
    void retire(void* ptr) {
        if (!ptr) return;
        
        uint64_t epoch = global_epoch_.load(std::memory_order_acquire);
        size_t list_idx = epoch % EBR_NUM_EPOCHS;
        
        std::lock_guard<std::mutex> lock(retire_mutexes_[list_idx]);
        retire_lists_[list_idx].push_back(ptr);
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
            
            std::vector<void*> to_delete;
            {
                std::lock_guard<std::mutex> lock(retire_mutexes_[list_idx]);
                to_delete = std::move(retire_lists_[list_idx]);
                retire_lists_[list_idx].clear();
            }
            
            for (void* ptr : to_delete) {
                deleter_(ptr);
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
template <typename Deleter>
class ebr_guard {
    ebr_manager<Deleter>* mgr_;
    int slot_idx_;
    
public:
    ebr_guard(ebr_manager<Deleter>* mgr, int slot_idx) noexcept
        : mgr_(mgr), slot_idx_(slot_idx) {
        if (mgr_) mgr_->enter_epoch(slot_idx_);
    }
    
    ~ebr_guard() {
        if (mgr_) mgr_->exit_epoch(slot_idx_);
    }
    
    ebr_guard(const ebr_guard&) = delete;
    ebr_guard& operator=(const ebr_guard&) = delete;
    
    ebr_guard(ebr_guard&& other) noexcept 
        : mgr_(other.mgr_), slot_idx_(other.slot_idx_) {
        other.mgr_ = nullptr;
    }
    
    ebr_guard& operator=(ebr_guard&& other) noexcept {
        if (this != &other) {
            if (mgr_) mgr_->exit_epoch(slot_idx_);
            mgr_ = other.mgr_;
            slot_idx_ = other.slot_idx_;
            other.mgr_ = nullptr;
        }
        return *this;
    }
};

/**
 * Thread-local slot manager
 * Automatically acquires/releases slots per thread
 */
template <typename Deleter>
class ebr_thread_context {
    ebr_manager<Deleter>* mgr_;
    int slot_idx_;
    
public:
    explicit ebr_thread_context(ebr_manager<Deleter>* mgr) 
        : mgr_(mgr), slot_idx_(mgr ? mgr->acquire_slot() : -1) {}
    
    ~ebr_thread_context() {
        if (mgr_ && slot_idx_ >= 0) {
            mgr_->release_slot(slot_idx_);
        }
    }
    
    ebr_thread_context(const ebr_thread_context&) = delete;
    ebr_thread_context& operator=(const ebr_thread_context&) = delete;
    
    int slot_idx() const noexcept { return slot_idx_; }
    
    ebr_guard<Deleter> guard() noexcept {
        return ebr_guard<Deleter>(mgr_, slot_idx_);
    }
};

}  // namespace gteitelbaum
