#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace gteitelbaum {

// Per-thread EBR slot
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
    };
    
    void enter() noexcept {
        active_.store(true, std::memory_order_release);
        std::atomic_thread_fence(std::memory_order_seq_cst);
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
    
    void set_epoch(uint64_t e) noexcept {
        epoch_.store(e, std::memory_order_release);
    }
    
    guard get_guard() { return guard(this); }
};

// Global EBR manager
class ebr_global {
public:
    using deleter_fn = void(*)(void*);
    
private:
    struct retired_node {
        void* ptr;
        deleter_fn deleter;
        uint64_t retire_epoch;
    };
    
    std::atomic<uint64_t> global_epoch_{0};
    std::mutex slots_mutex_;
    std::vector<ebr_slot*> slots_;
    std::mutex retired_mutex_;
    std::vector<retired_node> retired_;
    
    ebr_global() = default;
    
public:
    static ebr_global& instance() {
        static ebr_global g;
        return g;
    }
    
    ebr_slot* register_thread() {
        auto* slot = new ebr_slot();
        slot->set_epoch(global_epoch_.load(std::memory_order_relaxed));
        std::lock_guard<std::mutex> lock(slots_mutex_);
        slots_.push_back(slot);
        return slot;
    }
    
    void unregister_thread(ebr_slot* slot) {
        if (!slot) return;
        {
            std::lock_guard<std::mutex> lock(slots_mutex_);
            slots_.erase(std::remove(slots_.begin(), slots_.end(), slot), slots_.end());
        }
        delete slot;
    }
    
    uint64_t current_epoch() const noexcept {
        return global_epoch_.load(std::memory_order_acquire);
    }
    
    void advance_epoch() noexcept {
        global_epoch_.fetch_add(1, std::memory_order_acq_rel);
    }
    
    void retire(void* ptr, deleter_fn deleter) {
        if (!ptr) return;
        uint64_t epoch = global_epoch_.load(std::memory_order_acquire);
        std::lock_guard<std::mutex> lock(retired_mutex_);
        retired_.push_back({ptr, deleter, epoch});
    }
    
    void try_reclaim() {
        uint64_t safe_epoch = compute_safe_epoch();
        
        std::vector<retired_node> to_delete;
        {
            std::lock_guard<std::mutex> lock(retired_mutex_);
            auto it = std::remove_if(retired_.begin(), retired_.end(),
                [safe_epoch, &to_delete](const retired_node& n) {
                    if (n.retire_epoch < safe_epoch) {
                        to_delete.push_back(n);
                        return true;
                    }
                    return false;
                });
            retired_.erase(it, retired_.end());
        }
        
        for (auto& n : to_delete) {
            n.deleter(n.ptr);
        }
    }
    
private:
    uint64_t compute_safe_epoch() {
        uint64_t global = global_epoch_.load(std::memory_order_acquire);
        uint64_t safe = global;
        
        std::lock_guard<std::mutex> lock(slots_mutex_);
        for (auto* slot : slots_) {
            if (slot->is_active()) {
                uint64_t e = slot->epoch();
                if (e < safe) safe = e;
            }
        }
        return safe;
    }
};

// Thread-local EBR slot accessor
inline ebr_slot& get_ebr_slot() {
    thread_local ebr_slot* slot = ebr_global::instance().register_thread();
    slot->set_epoch(ebr_global::instance().current_epoch());
    return *slot;
}

}  // namespace gteitelbaum
