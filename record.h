#ifndef RECORD_H
#define RECORD_H

#include <string>

struct Record {
    std::string value;
    long long expire_at;
    Record() : expire_at(0) {}
    Record(std::string v, const long long e) : value(std::move(v)), expire_at(e) {}
};

#endif
