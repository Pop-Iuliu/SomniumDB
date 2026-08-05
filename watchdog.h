//
// Created by tiwerlol on 02.08.2026.
//

#ifndef REDIS_WATCHDOG_H
#define REDIS_WATCHDOG_H
#include "database.h"
#include <thread>
#include <atomic>
#include <mutex>

class Database;

class Watchdog {
private:
    Database& db;
    std::thread worker;
    std::atomic<bool> running;

public:
    explicit Watchdog(Database& database);
    ~Watchdog();
    void start();
    void stop();
};

#endif
