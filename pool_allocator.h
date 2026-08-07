#pragma once

#include <iostream>
#include <vector>
#include <mutex>
#include <utility>
#include <cstddef>

template <typename T, size_t BlockSize = 1024>
class PoolAllocator {
private:
    union Node {
        alignas(T) char data[sizeof(T)];
        Node* next;
    };

    Node* head = nullptr;
    std::vector<Node*> blocks;
    std::mutex pool_mutex;

    void allocate_block() {
        Node* new_block = new Node[BlockSize];
        blocks.push_back(new_block);

        // construim free-list-ul legand nodurile între ele
        for (size_t i = 0; i < BlockSize - 1; ++i) {
            new_block[i].next = &new_block[i + 1];
        }
        new_block[BlockSize - 1].next = nullptr;
        head = new_block;
    }

public:
    PoolAllocator() = default;

    ~PoolAllocator() {
        // la distrugerea allocatorului, eliberam chunk urile mari de mem
        for (Node* block : blocks) {
            delete[] block;
        }
    }

    PoolAllocator(const PoolAllocator&) = delete;
    PoolAllocator& operator=(const PoolAllocator&) = delete;

    template<typename... Args>
    T* allocate(Args&&... args) {
        std::lock_guard<std::mutex> lock(pool_mutex);

        if (!head) {
            allocate_block();
        }

        Node* node = head;
        head = head->next;
        return new (&node->data) T(std::forward<Args>(args)...);
    }

    void deallocate(T* ptr) {
        if (!ptr) return;

        ptr->~T();

        std::lock_guard<std::mutex> lock(pool_mutex);

        Node* node = reinterpret_cast<Node*>(ptr);
        node->next = head;
        head = node;
    }
};