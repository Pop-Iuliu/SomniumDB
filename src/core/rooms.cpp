// Ciclul de viata al camerelor: registrul, trezirea, hibernarea si comenzile ROOM.*.
// Lock-uri (room.h): rooms_mutex inaintea room_mutex, niciodata invers; activate()
// si hibernate() presupun room_mutex prins.
#include "database.h"
#include "../../metrics.h"

std::shared_ptr<Room> Database::find_room(const std::string& name, const bool create) {
    {
        std::shared_lock lock(rooms_mutex);
        if (const auto it = rooms.find(name); it != rooms.end()) {
            return it->second;
        }
    }
    if (!create) return nullptr;

    std::unique_lock lock(rooms_mutex);
    auto& room = rooms[name];
    if (!room) room = std::make_shared<Room>(name);
    return room;
}

bool Database::activate(Room& room, const bool over_budget) {
    room.last_access_time = get_current_time_ms();
    if (room.state == RoomState::Active) return true;

    // rezervam locul inainte de I/O: bugetul nu poate fi depasit nici de
    // activari concurente, fara sa parcurgem registrul sub lock
    size_t active = active_rooms.load();
    do {
        if (active >= MAX_ACTIVE_ROOMS && !over_budget) return false;
    } while (!active_rooms.compare_exchange_weak(active, active + 1));

    room.state = RoomState::Loading;
    SnapshotManager::wakeup_room(room, record_pool);
    const size_t cost = room.keys_cost();
    if (!over_budget && !eviction.fits(cost)) {
        // camera nu incape in SOMNIUM_MAXMEMORY: renuntam la ce am incarcat;
        // snapshot-ul a ramas neatins, deci nu se schimba nimic
        for (const auto& [key, r] : room.keys) record_pool.destroy(r);
        room.keys.clear();
        room.state = RoomState::Sleeping;
        active_rooms.fetch_sub(1);
        return false;
    }
    global_metrics.keys_in_ram.fetch_add(room.keys.size(), std::memory_order_relaxed);
    room.charge(static_cast<long long>(cost));
    room.state = RoomState::Active;
    return true;
}

bool Database::hibernate(Room& room) {
    if (room.state == RoomState::Sleeping) return true;

    room.state = RoomState::Hibernating;
    const size_t resident = room.keys.size();
    const size_t cost = room.keys_cost();
    if (!SnapshotManager::hibernate_room(room, record_pool, snapshot_delay_ms_)) {
        room.state = RoomState::Active;
        return false;
    }
    global_metrics.keys_in_ram.fetch_sub(resident, std::memory_order_relaxed);
    room.charge(-static_cast<long long>(cost));
    room.state = RoomState::Sleeping;
    active_rooms.fetch_sub(1);
    return true;
}

const char* Database::wake_room(const std::string& name) {
    // SEC-5: numele devine nume de fisier ("room_" + name + ".bin")
    if (name.empty() || name.size() > 200 || name.find_first_of(std::string_view("/\0", 2)) != std::string::npos) {
        return "-ERR invalid room name\r\n";
    }
    std::shared_ptr<Room> room = find_room(name, false);
    if (!room) {
        // SEC-7: un nume nou ramane in registru pentru totdeauna (redarea AOF nu trece pe aici)
        if (std::shared_lock lock(rooms_mutex); rooms.size() >= max_rooms_) return "-ERR too many rooms\r\n";
        room = find_room(name, true);
    }
    std::lock_guard lock(room->room_mutex);
    return activate(*room) ? nullptr : ROOMS_FULL;
}

std::string Database::handle_room_admin(const std::string& command, const std::vector<std::string>& args) {
    if (command == "ROOMS") {
        std::shared_lock lock(rooms_mutex);
        std::string out = "*" + std::to_string(rooms.size()) + "\r\n";
        for (const auto& [name, room] : rooms) out += bulk_string(name);
        return out;
    }

    if (args.size() != 2) return "-ERR Wrong number of arguments for " + command + "\r\n";
    const std::string& name = args[1];

    if (command == "ROOM.WAKE") {
        const char* err = wake_room(name);
        return err ? err : "+OK\r\n";
    }

    const std::shared_ptr<Room> room = find_room(name, false);
    if (!room) return "-ERR no such room\r\n";

    if (command == "ROOM.HIBERNATE") {
        if (name == "default") return "-ERR the default room is pinned\r\n";
        std::lock_guard lock(room->room_mutex);
        return hibernate(*room) ? "+OK\r\n" : "-ERR snapshot failed, room stays active\r\n";
    }

    if (command == "ROOM.INFO") {
        // starea se citeste fara lock (nu asteptam dupa I/O-ul de snapshot);
        // cheile rezidente doar pentru camerele active, sub lock-ul lor
        RoomState state = room->state;
        size_t keys = 0;
        size_t bytes = 0;
        if (state == RoomState::Active) {
            std::lock_guard lock(room->room_mutex);
            state = room->state;
            keys = room->keys.size();
            bytes = room->bytes;
        }
        static constexpr const char* STATE_NAMES[] = {"sleeping", "loading", "active", "hibernating"};
        return map_header(4) + bulk_string("state") + bulk_string(STATE_NAMES[static_cast<int>(state)]) +
               bulk_string("last_access_ms") + ":" + std::to_string(room->last_access_time) + "\r\n" +
               bulk_string("keys") + ":" + std::to_string(keys) + "\r\n" +
               bulk_string("bytes") + ":" + std::to_string(bytes) + "\r\n";
    }

    return "-ERR unknown command\r\n";
}

void Database::hibernate_inactive_rooms() {
    std::vector<std::shared_ptr<Room>> candidates;
    const long long now = get_current_time_ms();

    {
        std::shared_lock shared_rooms_lock(rooms_mutex);
        for (const auto& [room_name, room] : rooms) {
            if (room->state == RoomState::Active && room_name != "default" &&
                now - room->last_access_time > ROOM_IDLE_MS) {
                candidates.push_back(room);
            }
        }
    }
    // lock-ul global e liber de aici: I/O-ul de snapshot blocheaza doar camera respectiva

    for (const std::shared_ptr<Room>& room : candidates) {
        std::lock_guard room_lock(room->room_mutex);

        // re-verificam sub lock: o comanda poate a ajuns la camera intre timp
        if (room->state != RoomState::Active || get_current_time_ms() - room->last_access_time <= ROOM_IDLE_MS) {
            continue;
        }
        if (!hibernate(*room)) {
            printf("Hibernare esuata pentru camera '%s': persistenta nereusita, camera ramane activa.\n",
                   room->name.c_str());
        }
    }
}
