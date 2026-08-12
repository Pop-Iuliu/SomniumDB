#pragma once

#include <string>

struct Record {
    std::string value;
    long long expire_at;
    Record() : expire_at(0) {}
    Record(std::string v, const long long e) : value(std::move(v)), expire_at(e) {}
    uint64_t timestamp_ms{0};
    uint32_t node_id{1};
};

