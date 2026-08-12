#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <vector>
#include <algorithm>
#include <tuple>
#include <chrono>

struct PieceCRDTRecord {
    std::string value;
    uint64_t timestamp_ms;
    uint32_t node_id;
    bool is_deleted;

    bool merge(const PieceCRDTRecord& incoming) {
        auto current_state = std::tie(timestamp_ms, node_id);
        auto incoming_state = std::tie(incoming.timestamp_ms, incoming.node_id);

        if (incoming_state > current_state) {
            value = incoming.value;
            timestamp_ms = incoming.timestamp_ms;
            node_id = incoming.node_id;
            is_deleted = incoming.is_deleted;
            return true;
        }
        return false;
    }
};

class PiecePartition {
private:
    uint32_t piece_id;
    uint32_t local_node_id;
    std::unordered_map<std::string, PieceCRDTRecord> storage;
    mutable std::mutex partition_mutex;

    uint64_t get_now_ms() const {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

public:
    PiecePartition(uint32_t p_id, uint32_t node_id) 
        : piece_id(p_id), local_node_id(node_id) {}

    void set(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(partition_mutex);
        uint64_t now = get_now_ms();

        PieceCRDTRecord record{
            .value = value,
            .timestamp_ms = now,
            .node_id = local_node_id,
            .is_deleted = false
        };

        storage[key].merge(record);
    }
    bool get(const std::string& key, std::string& out_value) const {
        std::lock_guard<std::mutex> lock(partition_mutex);
        auto it = storage.find(key);
        if (it != storage.end() && !it->second.is_deleted) {
            out_value = it->second.value;
            return true;
        }
        return false;
    }
    void remove(const std::string& key) {
        std::lock_guard<std::mutex> lock(partition_mutex);
        uint64_t now = get_now_ms();

        PieceCRDTRecord tombstone{
            .value = "",
            .timestamp_ms = now,
            .node_id = local_node_id,
            .is_deleted = true
        };

        storage[key].merge(tombstone);
    }

    size_t merge_incoming_piece(const std::unordered_map<std::string, PieceCRDTRecord>& incoming_data) {
        std::lock_guard<std::mutex> lock(partition_mutex);
        size_t updated_keys = 0;

        for (const auto& [key, incoming_record] : incoming_data) {
            if (storage[key].merge(incoming_record)) {
                updated_keys++;
            }
        }
        return updated_keys;
    }

    std::unordered_map<std::string, PieceCRDTRecord> export_state() const {
        std::lock_guard<std::mutex> lock(partition_mutex);
        return storage;
    }
};