#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <vector>
#include <algorithm>

#include "tktrie_defines.h"

namespace gteitelbaum {

class ebr_global;

class ebr_slot {
    friend class ebr_global;
    std::atomic<uint64_t> epoch_{0};
    std::atomic<bool> active_{false};

public:
    ebr_slot() = default;

    class guard {
        ebr_slot& slot_;
    public:
        explicit guard(ebr_slot& slot) : slot_(slot) { slot_.enter(); }
        ~guard() { slot_.exit(); }
        guard(const guard&) = delete;
        guard& operator=(const guard&) = delete;
    };

    guard get_guard() { return guard(*this); }

private:
    void enter();
    void exit() { active_.store(false, std::memory_order_release); }
    static uint64_t global_epoch();
};

class ebr_global {
    std::atomic<uint64_t> epoch_{0};

    struct retired_node {
        void* ptr;
        void (*deleter)(void*);
        uint64_t retire_epoch;
    };

    std::mutex retire_mutex_;
    std::vector<retired_node> retired_;
    std::mutex slots_mutex_;
    std::vector<ebr_slot*> slots_;

    ebr_global() { retired_.reserve(1024); }

public:
    static ebr_global& instance() {
        static ebr_global inst;
        return inst;
    }

    uint64_t current_epoch() const { return epoch_.load(std::memory_order_acquire); }
    void advance_epoch() { epoch_.fetch_add(1, std::memory_order_acq_rel); }

    void register_slot(ebr_slot* slot) {
        std::lock_guard<std::mutex> lock(slots_mutex_);
        slots_.push_back(slot);
    }

    void unregister_slot(ebr_slot* slot) {
        std::lock_guard<std::mutex> lock(slots_mutex_);
        slots_.erase(std::remove(slots_.begin(), slots_.end(), slot), slots_.end());
    }

    void retire(void* ptr, void (*deleter)(void*)) {
        std::lock_guard<std::mutex> lock(retire_mutex_);
        retired_.push_back({ptr, deleter, current_epoch()});
    }

    void try_reclaim() {
        uint64_t min_epoch = current_epoch();
        {
            std::lock_guard<std::mutex> lock(slots_mutex_);
            for (auto* slot : slots_) {
                if (slot->active_.load(std::memory_order_acquire)) {
                    uint64_t e = slot->epoch_.load(std::memory_order_acquire);
                    if (e < min_epoch) min_epoch = e;
                }
            }
        }

        std::vector<retired_node> still_retired;
        {
            std::lock_guard<std::mutex> lock(retire_mutex_);
            for (auto& node : retired_) {
                if (node.retire_epoch < min_epoch) node.deleter(node.ptr);
                else still_retired.push_back(node);
            }
            retired_ = std::move(still_retired);
        }
    }
};

inline uint64_t ebr_slot::global_epoch() { return ebr_global::instance().current_epoch(); }

inline void ebr_slot::enter() {
    active_.store(true, std::memory_order_relaxed);
    epoch_.store(global_epoch(), std::memory_order_release);
    std::atomic_thread_fence(std::memory_order_seq_cst);
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
