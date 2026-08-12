#pragma once

#include <iostream>
#include <vector>
#include <mutex>
#include <utility>
#include <cstddef>
#include <memory>

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

    struct PoolState {
        Node* head = nullptr;
        std::vector<Node*> blocks;
        std::mutex pool_mutex;

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
    explicit PoolAllocator(const PoolAllocator<U, BlockSize>& other) noexcept
        : state(std::make_shared<PoolState>()) {}

    T* allocate(std::size_t n) {
        if (n != 1) {
            return static_cast<T*>(::operator new(n * sizeof(T)));
        }

        std::lock_guard<std::mutex> lock(state->pool_mutex);

        if (!state->head) {
            allocate_block();
        }

        Node* node = state->head;
        state->head = state->head->next;
        return reinterpret_cast<T*>(node);
    }

    void deallocate(T* ptr, std::size_t n) noexcept {
        if (!ptr) return;

        if (n != 1) {
            ::operator delete(ptr);
            return;
        }

        std::lock_guard<std::mutex> lock(state->pool_mutex);

        Node* node = reinterpret_cast<Node*>(ptr);
        node->next = state->head;
        state->head = node;
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
};