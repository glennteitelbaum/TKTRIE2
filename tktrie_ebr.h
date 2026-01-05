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
    std::atomic<bool> valid_{true};  // Set to false when slot is destroyed
    
public:
    // RAII guard for reader critical sections
    class guard {
        ebr_slot* slot_;
    public:
        explicit guard(ebr_slot* s);
        ~guard();
        guard(const guard&) = delete;
        guard& operator=(const guard&) = delete;
        guard(guard&& o) noexcept;
        guard& operator=(guard&&) = delete;
    };
    
    ebr_slot();
    ~ebr_slot();
    
    void enter() noexcept;
    void exit() noexcept;
    bool is_active() const noexcept;
    bool is_valid() const noexcept;
    uint64_t epoch() const noexcept;
    guard get_guard();
    
    static std::atomic<uint64_t>& global_epoch();
};

// =============================================================================
// EBR_GLOBAL - global epoch management and reclamation
// =============================================================================

class ebr_global {
public:
    using deleter_t = void(*)(void*);
    
private:
    struct retired_node {
        void* ptr;
        deleter_t deleter;
        uint64_t epoch;
    };
    
    std::mutex mutex_;
    std::vector<retired_node> retired_;
    std::vector<ebr_slot*> slots_;
    
    ebr_global() = default;
    
public:
    ~ebr_global();
    
    static ebr_global& instance();
    void register_slot(ebr_slot* slot);
    void unregister_slot(ebr_slot* slot);
    ebr_slot& get_slot();
    void retire(void* ptr, deleter_t deleter);
    void advance_epoch();
    void try_reclaim();
    void force_reclaim_all();
};

// Free function for convenience
inline ebr_slot& get_ebr_slot();

// =============================================================================
// EBR_SLOT::GUARD DEFINITIONS
// =============================================================================

inline ebr_slot::guard::guard(ebr_slot* s) : slot_(s) { 
    slot_->enter(); 
}

inline ebr_slot::guard::~guard() { 
    if (slot_) slot_->exit(); 
}

inline ebr_slot::guard::guard(guard&& o) noexcept : slot_(o.slot_) { 
    o.slot_ = nullptr; 
}

// =============================================================================
// EBR_SLOT DEFINITIONS
// =============================================================================

inline ebr_slot::ebr_slot() {
    ebr_global::instance().register_slot(this);
}

inline ebr_slot::~ebr_slot() {
    valid_.store(false, std::memory_order_release);
    ebr_global::instance().unregister_slot(this);
}

inline void ebr_slot::enter() noexcept {
    // CRITICAL: Store epoch FIRST, then set active
    // This ensures that when safe_epoch() sees active=true, it will also see the updated epoch
    epoch_.store(global_epoch().load(std::memory_order_acquire), std::memory_order_release);
    active_.store(true, std::memory_order_release);
}

inline void ebr_slot::exit() noexcept {
    active_.store(false, std::memory_order_release);
}

inline bool ebr_slot::is_active() const noexcept {
    return active_.load(std::memory_order_acquire);
}

inline bool ebr_slot::is_valid() const noexcept {
    return valid_.load(std::memory_order_acquire);
}

inline uint64_t ebr_slot::epoch() const noexcept {
    return epoch_.load(std::memory_order_acquire);
}

inline ebr_slot::guard ebr_slot::get_guard() { 
    return guard(this); 
}

inline std::atomic<uint64_t>& ebr_slot::global_epoch() {
    static std::atomic<uint64_t> e{0};
    return e;
}

// =============================================================================
// EBR_GLOBAL DEFINITIONS
// =============================================================================

inline ebr_global& ebr_global::instance() {
    static ebr_global inst;
    return inst;
}

inline ebr_global::~ebr_global() {
    // Clean up any remaining retired nodes
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& r : retired_) {
        r.deleter(r.ptr);
    }
    retired_.clear();
}

inline void ebr_global::register_slot(ebr_slot* slot) {
    std::lock_guard<std::mutex> lock(mutex_);
    slots_.push_back(slot);
}

inline void ebr_global::unregister_slot(ebr_slot* slot) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::find(slots_.begin(), slots_.end(), slot);
    if (it != slots_.end()) {
        slots_.erase(it);
    }
}

inline ebr_slot& ebr_global::get_slot() {
    thread_local ebr_slot slot;
    return slot;
}

inline void ebr_global::retire(void* ptr, deleter_t deleter) {
    uint64_t e = ebr_slot::global_epoch().load(std::memory_order_acquire);
    std::lock_guard<std::mutex> lock(mutex_);
    // Check for duplicate retirement (indicates bug)
    for (const auto& r : retired_) {
        if (r.ptr == ptr) {
            // Already retired - skip
            return;
        }
    }
    retired_.push_back({ptr, deleter, e});
}

inline void ebr_global::advance_epoch() {
    ebr_slot::global_epoch().fetch_add(1, std::memory_order_acq_rel);
}

inline void ebr_global::try_reclaim() {
    std::vector<retired_node> still_retired;
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Calculate safe epoch - minimum of global epoch and all active readers
    uint64_t ge = ebr_slot::global_epoch().load(std::memory_order_acquire);
    uint64_t safe = ge;
    for (auto* slot : slots_) {
        // Only check valid slots (not being destroyed)
        if (slot->is_valid() && slot->is_active()) {
            uint64_t se = slot->epoch();
            if (se < safe) safe = se;
        }
    }
    
    // Reclaim nodes retired before the safe epoch
    for (auto& r : retired_) {
        if (r.epoch < safe) {
            r.deleter(r.ptr);
        } else {
            still_retired.push_back(r);
        }
    }
    retired_ = std::move(still_retired);
}

inline void ebr_global::force_reclaim_all() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& r : retired_) {
        r.deleter(r.ptr);
    }
    retired_.clear();
}

// =============================================================================
// FREE FUNCTION
// =============================================================================

inline ebr_slot& get_ebr_slot() {
    return ebr_global::instance().get_slot();
}

}  // namespace gteitelbaum
