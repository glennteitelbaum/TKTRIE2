#pragma once

#include <atomic>
#include <vector>
#include <functional>
#include <thread>

namespace gteitelbaum {

// Simple Epoch-Based Reclamation for thread-safe memory management

class ebr_guard {
    std::atomic<uint64_t>* epoch_ptr_;
    uint64_t saved_epoch_;
public:
    ebr_guard(std::atomic<uint64_t>* epoch_ptr, uint64_t epoch) 
        : epoch_ptr_(epoch_ptr), saved_epoch_(epoch) {
        epoch_ptr_->store(epoch, std::memory_order_release);
    }
    ~ebr_guard() {
        epoch_ptr_->store(UINT64_MAX, std::memory_order_release);
    }
    ebr_guard(const ebr_guard&) = delete;
    ebr_guard& operator=(const ebr_guard&) = delete;
};

class ebr_slot {
    std::atomic<uint64_t> local_epoch_{UINT64_MAX};
    friend class ebr_global;
public:
    ebr_guard get_guard();
};

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
    std::vector<ebr_slot*> slots_;
    std::vector<retired_node> retired_;
    std::mutex slots_mutex_;
    std::mutex retired_mutex_;
    
    ebr_global() = default;
    
public:
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
        slots_.erase(std::remove(slots_.begin(), slots_.end(), slot), slots_.end());
    }
    
    uint64_t current_epoch() const {
        return global_epoch_.load(std::memory_order_acquire);
    }
    
    void advance_epoch() {
        global_epoch_.fetch_add(1, std::memory_order_acq_rel);
    }
    
    void retire(void* ptr, deleter_fn deleter) {
        std::lock_guard<std::mutex> lock(retired_mutex_);
        retired_.push_back({ptr, deleter, current_epoch()});
    }
    
    void try_reclaim() {
        uint64_t safe_epoch = current_epoch();
        
        {
            std::lock_guard<std::mutex> lock(slots_mutex_);
            for (auto* slot : slots_) {
                uint64_t slot_epoch = slot->local_epoch_.load(std::memory_order_acquire);
                if (slot_epoch != UINT64_MAX && slot_epoch < safe_epoch) {
                    safe_epoch = slot_epoch;
                }
            }
        }
        
        if (safe_epoch > 0) safe_epoch -= 1;
        
        std::vector<retired_node> still_retired;
        {
            std::lock_guard<std::mutex> lock(retired_mutex_);
            for (auto& node : retired_) {
                if (node.retire_epoch < safe_epoch) {
                    node.deleter(node.ptr);
                } else {
                    still_retired.push_back(node);
                }
            }
            retired_ = std::move(still_retired);
        }
    }
};

inline ebr_guard ebr_slot::get_guard() {
    return ebr_guard(&local_epoch_, ebr_global::instance().current_epoch());
}

inline ebr_slot& get_ebr_slot() {
    thread_local ebr_slot slot;
    thread_local bool registered = false;
    if (!registered) {
        ebr_global::instance().register_slot(&slot);
        registered = true;
    }
    return slot;
}

}  // namespace gteitelbaum
