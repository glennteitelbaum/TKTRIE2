#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <vector>
#include <thread>
#include <algorithm>

namespace gteitelbaum {

// Forward declaration
class ebr_global;

// =============================================================================
// EBR_SLOT - per-thread epoch tracking
// =============================================================================

class ebr_slot {
    std::atomic<uint64_t> epoch_{0};
    std::atomic<bool> active_{false};
    std::atomic<bool> valid_{true};
    
public:
    class guard {
        ebr_slot* slot_;
    public:
        explicit guard(ebr_slot* s) : slot_(s) { slot_->enter(); }
        ~guard() { if (slot_) slot_->exit(); }
        guard(const guard&) = delete;
        guard& operator=(const guard&) = delete;
        guard(guard&& o) noexcept : slot_(o.slot_) { o.slot_ = nullptr; }
        guard& operator=(guard&&) = delete;
    };
    
    ebr_slot();
    ~ebr_slot();
    
    void enter() noexcept {
        epoch_.store(global_epoch().load(std::memory_order_acquire), std::memory_order_release);
        active_.store(true, std::memory_order_release);
    }
    
    void exit() noexcept { active_.store(false, std::memory_order_release); }
    bool is_active() const noexcept { return active_.load(std::memory_order_acquire); }
    bool is_valid() const noexcept { return valid_.load(std::memory_order_acquire); }
    uint64_t epoch() const noexcept { return epoch_.load(std::memory_order_acquire); }
    guard get_guard() { return guard(this); }
    
    static std::atomic<uint64_t>& global_epoch() {
        static std::atomic<uint64_t> e{0};
        return e;
    }
};

// =============================================================================
// EBR_GLOBAL - global slot management only (retired lists are per-trie)
// =============================================================================

class ebr_global {
private:
    std::mutex slots_mutex_;
    std::vector<ebr_slot*> slots_;
    
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
    
    void advance_epoch() {
        ebr_slot::global_epoch().fetch_add(1, std::memory_order_acq_rel);
    }
    
    // Compute safe epoch - oldest epoch any active reader holds
    uint64_t compute_safe_epoch() {
        uint64_t ge = ebr_slot::global_epoch().load(std::memory_order_acquire);
        uint64_t safe = ge;
        std::lock_guard<std::mutex> lock(slots_mutex_);
        for (auto* slot : slots_) {
            if (slot->is_valid() && slot->is_active()) {
                uint64_t se = slot->epoch();
                if (se < safe) safe = se;
            }
        }
        return safe;
    }
};

inline ebr_slot& get_ebr_slot() {
    return ebr_global::instance().get_slot();
}

// Out-of-line definitions for ebr_slot
inline ebr_slot::ebr_slot() { ebr_global::instance().register_slot(this); }
inline ebr_slot::~ebr_slot() {
    valid_.store(false, std::memory_order_release);
    ebr_global::instance().unregister_slot(this);
}

}  // namespace gteitelbaum
