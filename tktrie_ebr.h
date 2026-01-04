#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <vector>

namespace gteitelbaum {

// Epoch-based reclamation for THREADED mode
class ebr_slot {
    std::atomic<uint64_t> local_epoch_{0};
    std::atomic<bool> active_{false};

public:
    class guard {
        ebr_slot& slot_;
    public:
        explicit guard(ebr_slot& slot) : slot_(slot) { slot_.enter(); }
        ~guard() { slot_.leave(); }
        guard(const guard&) = delete;
        guard& operator=(const guard&) = delete;
    };

    void enter() noexcept {
        active_.store(true, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

    void leave() noexcept {
        active_.store(false, std::memory_order_release);
    }

    bool is_active() const noexcept {
        return active_.load(std::memory_order_acquire);
    }

    uint64_t epoch() const noexcept {
        return local_epoch_.load(std::memory_order_acquire);
    }

    void set_epoch(uint64_t e) noexcept {
        local_epoch_.store(e, std::memory_order_release);
    }

    guard get_guard() { return guard(*this); }
};

class ebr_global {
    std::atomic<uint64_t> global_epoch_{0};
    std::mutex retire_mutex_;

    struct retired_item {
        void* ptr;
        uint64_t epoch;
        void (*deleter)(void*);
    };

    std::vector<retired_item> retired_;

    ebr_global() { retired_.reserve(1024); }

public:
    static ebr_global& instance() {
        static ebr_global inst;
        return inst;
    }

    uint64_t current_epoch() const noexcept {
        return global_epoch_.load(std::memory_order_acquire);
    }

    void advance_epoch() noexcept {
        global_epoch_.fetch_add(1, std::memory_order_acq_rel);
    }

    void retire(void* ptr, void (*deleter)(void*)) {
        std::lock_guard<std::mutex> lock(retire_mutex_);
        retired_.push_back({ptr, current_epoch(), deleter});
    }

    void try_reclaim() {
        std::lock_guard<std::mutex> lock(retire_mutex_);
        if (retired_.empty()) return;

        uint64_t safe_epoch = current_epoch();
        if (safe_epoch >= 2) safe_epoch -= 2;
        else safe_epoch = 0;

        auto it = retired_.begin();
        while (it != retired_.end()) {
            if (it->epoch <= safe_epoch) {
                it->deleter(it->ptr);
                it = retired_.erase(it);
            } else {
                ++it;
            }
        }
    }
};

inline thread_local ebr_slot tl_ebr_slot;

inline ebr_slot& get_ebr_slot() {
    return tl_ebr_slot;
}

}  // namespace gteitelbaum
