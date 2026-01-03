#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace gteitelbaum {

// Simple epoch-based reclamation for safe memory reclamation
class ebr_manager {
    static constexpr int NUM_EPOCHS = 3;

    struct retired_item {
        void* ptr;
        void (*deleter)(void*);
    };

    std::atomic<uint64_t> global_epoch_{0};
    std::vector<retired_item> retired_[NUM_EPOCHS];
    std::mutex retire_mutex_;
    std::atomic<int> active_readers_{0};

public:
    static ebr_manager& instance() {
        static ebr_manager mgr;
        return mgr;
    }

    uint64_t enter_read() {
        active_readers_.fetch_add(1, std::memory_order_acquire);
        return global_epoch_.load(std::memory_order_acquire);
    }

    void leave_read() {
        active_readers_.fetch_sub(1, std::memory_order_release);
    }

    void retire(void* ptr, void (*deleter)(void*)) {
        std::lock_guard<std::mutex> lock(retire_mutex_);
        uint64_t epoch = global_epoch_.load(std::memory_order_relaxed);
        retired_[epoch % NUM_EPOCHS].push_back({ptr, deleter});
    }

    void try_reclaim() {
        if (active_readers_.load(std::memory_order_acquire) > 0) return;
        
        std::lock_guard<std::mutex> lock(retire_mutex_);
        if (active_readers_.load(std::memory_order_acquire) > 0) return;

        uint64_t old_epoch = global_epoch_.fetch_add(1, std::memory_order_acq_rel);
        int reclaim_idx = (old_epoch + 1) % NUM_EPOCHS;
        
        for (auto& item : retired_[reclaim_idx]) {
            item.deleter(item.ptr);
        }
        retired_[reclaim_idx].clear();
    }

    void force_reclaim() {
        // Wait for readers and reclaim all
        while (active_readers_.load(std::memory_order_acquire) > 0) {
            std::this_thread::yield();
        }
        
        std::lock_guard<std::mutex> lock(retire_mutex_);
        for (int i = 0; i < NUM_EPOCHS; ++i) {
            for (auto& item : retired_[i]) {
                item.deleter(item.ptr);
            }
            retired_[i].clear();
        }
    }
};

class ebr_guard {
public:
    ebr_guard() { ebr_manager::instance().enter_read(); }
    ~ebr_guard() { ebr_manager::instance().leave_read(); }
    ebr_guard(const ebr_guard&) = delete;
    ebr_guard& operator=(const ebr_guard&) = delete;
};

}  // namespace gteitelbaum
