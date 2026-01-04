#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <vector>
#include <thread>

namespace gteitelbaum {

// Epoch-based reclamation for lock-free reads
// Writers retire nodes; readers pin epochs; reclamation happens when safe

class ebr_slot {
    std::atomic<uint64_t> epoch_{0};
    std::atomic<bool> active_{false};
    
public:
    class guard {
        ebr_slot* slot_;
    public:
        explicit guard(ebr_slot* s) : slot_(s) { slot_->enter(); }
        ~guard() { slot_->exit(); }
        guard(const guard&) = delete;
        guard& operator=(const guard&) = delete;
        guard(guard&& o) noexcept : slot_(o.slot_) { o.slot_ = nullptr; }
        guard& operator=(guard&&) = delete;
    };
    
    void enter() noexcept {
        active_.store(true, std::memory_order_relaxed);
        epoch_.store(global_epoch().load(std::memory_order_acquire), std::memory_order_release);
    }
    
    void exit() noexcept {
        active_.store(false, std::memory_order_release);
    }
    
    bool is_active() const noexcept {
        return active_.load(std::memory_order_acquire);
    }
    
    uint64_t epoch() const noexcept {
        return epoch_.load(std::memory_order_acquire);
    }
    
    guard get_guard() { return guard(this); }
    
    static std::atomic<uint64_t>& global_epoch() {
        static std::atomic<uint64_t> e{0};
        return e;
    }
};

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
    static ebr_global& instance() {
        static ebr_global inst;
        return inst;
    }
    
    ebr_slot& get_slot() {
        thread_local ebr_slot slot;
        thread_local bool registered = false;
        if (!registered) {
            std::lock_guard<std::mutex> lock(mutex_);
            slots_.push_back(&slot);
            registered = true;
        }
        return slot;
    }
    
    void retire(void* ptr, deleter_t deleter) {
        uint64_t e = ebr_slot::global_epoch().load(std::memory_order_acquire);
        std::lock_guard<std::mutex> lock(mutex_);
        retired_.push_back({ptr, deleter, e});
    }
    
    void advance_epoch() {
        ebr_slot::global_epoch().fetch_add(1, std::memory_order_acq_rel);
    }
    
    void try_reclaim() {
        uint64_t safe = safe_epoch();
        std::vector<retired_node> still_retired;
        
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& r : retired_) {
            if (r.epoch < safe) {
                r.deleter(r.ptr);
            } else {
                still_retired.push_back(r);
            }
        }
        retired_ = std::move(still_retired);
    }
    
private:
    uint64_t safe_epoch() {
        uint64_t ge = ebr_slot::global_epoch().load(std::memory_order_acquire);
        uint64_t safe = ge;
        
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto* slot : slots_) {
            if (slot->is_active()) {
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

}  // namespace gteitelbaum
