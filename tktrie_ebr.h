#pragma once

#include <atomic>
#include <mutex>
#include <vector>
#include <algorithm>

namespace gteitelbaum {

// =============================================================================
// EBR_SLOT - per-thread epoch tracking (single-field design)
// 0 = inactive, non-zero = epoch when reader entered
// =============================================================================

class ebr_slot {
    std::atomic<uint64_t> active_epoch_{0};
    std::atomic<bool> valid_{true};
    
public:
    ebr_slot();
    ~ebr_slot();
    
    void enter(uint64_t epoch) noexcept {
        active_epoch_.store(epoch, std::memory_order_release);
    }
    
    // No-op exit - stale epochs handled by cleanup threshold
    void exit() noexcept {}
    
    // Returns 0 if inactive, epoch otherwise
    uint64_t epoch() const noexcept { 
        return active_epoch_.load(std::memory_order_acquire); 
    }
    
    bool is_valid() const noexcept { 
        return valid_.load(std::memory_order_acquire); 
    }
    
    void invalidate() noexcept {
        valid_.store(false, std::memory_order_release);
    }
};

// =============================================================================
// EBR_GLOBAL - global epoch and slot management
// =============================================================================

class ebr_global {
private:
    std::mutex slots_mutex_;
    std::vector<ebr_slot*> slots_;
    std::atomic<uint64_t> global_epoch_{1};  // Start at 1, 0 = inactive sentinel
    
    ebr_global() = default;
    
public:
    ~ebr_global() = default;
    
    static ebr_global& instance() {
        static ebr_global inst;
        return inst;
    }
    
    void register_slot(ebr_slot* slot) {
        std::lock_guard<std::mutex> lock(slots_mutex_);
        slots_.push_back(slot);
    }
    
    void unregister_slot(ebr_slot* slot) {
        std::lock_guard<std::mutex> lock(slots_mutex_);
        auto it = std::find(slots_.begin(), slots_.end(), slot);
        if (it != slots_.end()) slots_.erase(it);
    }
    
    ebr_slot& get_slot() {
        thread_local ebr_slot slot;
        return slot;
    }
    
    uint64_t current_epoch() const noexcept {
        return global_epoch_.load(std::memory_order_acquire);
    }
    
    uint64_t advance_epoch() noexcept {
        return global_epoch_.fetch_add(1, std::memory_order_acq_rel) + 1;
    }
    
    // Compute min epoch across all slots (0s ignored as inactive)
    // Returns current global epoch if no active readers
    uint64_t compute_min_epoch() {
        uint64_t ge = current_epoch();
        uint64_t min_e = ge;
        std::lock_guard<std::mutex> lock(slots_mutex_);
        for (auto* slot : slots_) {
            if (slot->is_valid()) {
                uint64_t e = slot->epoch();
                if (e != 0 && e < min_e) {
                    min_e = e;
                }
            }
        }
        return min_e;
    }
};

inline ebr_slot& get_ebr_slot() {
    return ebr_global::instance().get_slot();
}

inline ebr_slot::ebr_slot() { 
    ebr_global::instance().register_slot(this); 
}

inline ebr_slot::~ebr_slot() {
    invalidate();
    ebr_global::instance().unregister_slot(this);
}

}  // namespace gteitelbaum
