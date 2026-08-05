#include "watchdog.h"

Watchdog::Watchdog(Database& database) : db(database), running(false) {}

void Watchdog::start() {
    running = true;
    // Lansam thread-ul in fundal
    worker = std::thread([this]() {
        while (running) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            
            if (running) {
                db.clean_expired_keys();
                db.hibernate_inactive_rooms();
            }
        }
    });
}

void Watchdog::stop() {
    running = false;
    if (worker.joinable()) {
        worker.join();
    }
}

Watchdog::~Watchdog() {
    stop();
}