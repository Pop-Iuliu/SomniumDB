#include "watchdog.h"
#include <chrono>

Watchdog::Watchdog(Database& database) : db(database), running(false) {}

void Watchdog::start() {
    running = true;
    // Lansam thread-ul in fundal; tick la 500ms ca politica everysec a AOF-ului
    // sa fie respectata cu adevarat (cel mult o secunda de pierderi posibile),
    // iar intretinerea grea (expirare + hibernare) ramane la ~5s
    worker = std::thread([this]() {
        int tick = 0;
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            if (running) {
                db.sync_aof_if_due();
                if (++tick % 10 == 0) {
                    db.clean_expired_keys();
                    db.hibernate_inactive_rooms();
                }
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