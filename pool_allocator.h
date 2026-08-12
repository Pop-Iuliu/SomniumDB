#pragma once

#include <iostream>
#include <vector>
#include <mutex>
#include <utility>
#include <cstddef>
#include <memory>
#include <atomic>
#include <deque>

// compliant cu standardul c++
// https://en.wikipedia.org/wiki/Allocator_(C%2B%2B)

template <typename T, size_t BlockSize = 1024>
class PoolAllocator {
public:
    using value_type = T;

private:
    union Node {
        alignas(T) char data[sizeof(T)];
        Node* next;
    };

    struct RetiredNode {
        T* ptr;
        uint64_t epoch;
    };

    struct PoolState {
        Node* head = nullptr;
        std::vector<Node*> blocks;
        std::mutex pool_mutex;

        std::atomic<uint64_t> global_epoch{0};
        std::deque<RetiredNode> retired_queue;
        std::atomic<size_t> retired_count{0};
        std::atomic<size_t> active_allocations{0};

        ~PoolState() {
            for (Node* block : blocks) {
                delete[] block;
            }
        }
    };

    std::shared_ptr<PoolState> state;

    void allocate_block() {
        Node* new_block = new Node[BlockSize];
        state->blocks.push_back(new_block);

        for (size_t i = 0; i < BlockSize - 1; ++i) {
            new_block[i].next = &new_block[i + 1];
        }
        new_block[BlockSize - 1].next = nullptr;
        state->head = new_block;
    }

public:
    PoolAllocator() : state(std::make_shared<PoolState>()) {}

    ~PoolAllocator() = default;

    PoolAllocator(const PoolAllocator& other) noexcept = default;
    PoolAllocator& operator=(const PoolAllocator& other) noexcept = default;

    template <typename U>
    PoolAllocator(const PoolAllocator<U, BlockSize>& other) noexcept
        : state(std::make_shared<PoolState>()) {}

    T* allocate(std::size_t n) {
        if (n != 1) {
            return static_cast<T*>(::operator new(n * sizeof(T)));
        }

        std::lock_guard<std::mutex> lock(state->pool_mutex);

        if (!state->head) {
            collect_internal();
            if (!state->head) {
                allocate_block();
            }
        }

        Node* node = state->head;
        state->head = state->head->next;
        state->active_allocations.fetch_add(1, std::memory_order_relaxed);
        return reinterpret_cast<T*>(node);
    }

    void deallocate(T* ptr, std::size_t n) noexcept {
        if (!ptr) return;

        if (n != 1) {
            ::operator delete(ptr);
            return;
        }
        uint64_t current_e = state->global_epoch.load(std::memory_order_relaxed);

        {
            std::lock_guard<std::mutex> lock(state->pool_mutex);
            state->retired_queue.push_back({ptr, current_e});
            state->retired_count.fetch_add(1, std::memory_order_relaxed);
        }
        if (state->retired_count.load(std::memory_order_relaxed) >= 256) {
            collect();
        }
    }

    void advance_epoch() noexcept {
        state->global_epoch.fetch_add(1, std::memory_order_relaxed);
    }

    size_t collect() noexcept {
        std::lock_guard<std::mutex> lock(state->pool_mutex);
        return collect_internal();
    }

    float get_memory_stress() const noexcept {
        size_t active = state->active_allocations.load(std::memory_order_relaxed);
        size_t retired = state->retired_count.load(std::memory_order_relaxed);
        if (active == 0) return 0.0f;
        return static_cast<float>(retired) / static_cast<float>(active + retired);
    }

    template <typename... Args>
    T* construct(Args&&... args) {
        T* ptr = allocate(1);
        return new (ptr) T(std::forward<Args>(args)...);
    }

    void destroy(T* ptr) {
        if (!ptr) return;
        ptr->~T();
        deallocate(ptr, 1);
    }

    template <typename U>
    bool operator==(const PoolAllocator<U, BlockSize>& other) const noexcept {
        return state == other.state;
    }

    template <typename U>
    bool operator!=(const PoolAllocator<U, BlockSize>& other) const noexcept {
        return !(*this == other);
    }

private:
    size_t collect_internal() noexcept {
        if (state->retired_queue.empty()) return 0;

        uint64_t current_e = state->global_epoch.load(std::memory_order_relaxed);
        uint64_t safe_epoch = (current_e > 1) ? (current_e - 1) : 0;

        size_t reclaimed = 0;

        while (!state->retired_queue.empty()) {
            auto& item = state->retired_queue.front();
            if (item.epoch <= safe_epoch) {
                Node* node = reinterpret_cast<Node*>(item.ptr);
                node->next = state->head;
                state->head = node;

                state->retired_queue.pop_front();
                reclaimed++;
            } else {
                break;
            }
        }

        state->retired_count.fetch_sub(reclaimed, std::memory_order_relaxed);
        if (state->active_allocations.load(std::memory_order_relaxed) >= reclaimed) {
            state->active_allocations.fetch_sub(reclaimed, std::memory_order_relaxed);
        }

        return reclaimed;
    }
};