#include "watchdog.h"
#include <chrono>

Watchdog::Watchdog(Database& database) : db(database), running(false) {}

void Watchdog::start() {
    running = true;
    // Intretinerea (expirare + hibernare) ramane aici, la ~5s. Sincronizarea
    // everysec nu mai sta pe acest thread: un snapshot lent nu ii amana fdatasync-ul.
    worker = std::thread([this]() {
        int tick = 0;
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            if (running && ++tick % 10 == 0) {
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