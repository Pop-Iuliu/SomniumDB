#include "database.h"
#include <random>
#include <ranges>
#include "metrics.h"
/*
    functii utilitare
*/

static long long get_current_time_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}
/*
static void to_json(nlohmann::json& j, const Record& r) {
    j = nlohmann::json{
        {"value", r.value},
        {"expire_at", r.expire_at}
    };
}

static void from_json(const nlohmann::json& j, Record& r) {
    j.at("value").get_to(r.value);
    j.at("expire_at").get_to(r.expire_at);
}
*/

static std::string encode_resp(const std::vector<std::string>& args) {
    std::string resp = "*" + std::to_string(args.size()) + "\r\n";
    for (const auto& arg : args) {
        resp += "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n";
    }
    return resp;
}

std::string Database::read_from_cold_storage(const std::string& room_name, const std::string& key) {
    const int fd = open("despised_keys.bin", O_RDONLY);
    if (fd < 0) return "";

    struct stat sb{};
    if (fstat(fd, &sb) == -1 || sb.st_size == 0) {
        close(fd);
        return "";
    }

    // 3. MAGIA: Kernel-ul Linux mapează fișierul direct în spațiul nostru virtual (RAM)
    const auto mapped = static_cast<char*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (mapped == MAP_FAILED) {
        close(fd);
        return "";
    }

    std::string found_value;
    size_t offset = 0;

    while (offset < static_cast<size_t>(sb.st_size)) {
        if (offset + sizeof(size_t) > sb.st_size) break;
        size_t rlen = *reinterpret_cast<size_t*>(mapped + offset);
        offset += sizeof(size_t);

        if (offset + rlen > sb.st_size) break;
        std::string_view current_room(mapped + offset, rlen);
        offset += rlen;

        if (offset + sizeof(size_t) > sb.st_size) break;
        size_t klen = *reinterpret_cast<size_t*>(mapped + offset);
        offset += sizeof(size_t);

        if (offset + klen > sb.st_size) break;
        std::string_view current_key(mapped + offset, klen);
        offset += klen;

        if (offset + sizeof(size_t) > sb.st_size) break;
        size_t vlen = *reinterpret_cast<size_t*>(mapped + offset);
        offset += sizeof(size_t);

        if (offset + vlen > sb.st_size) break;

        if (current_room == room_name && current_key == key) {
            found_value = std::string(mapped + offset, vlen);
        }
        offset += vlen;
    }

    munmap(mapped, sb.st_size);
    close(fd);

    return found_value;
}

/*
    main stuff
*/

Database::Database() : is_recovering(true) {
    start_time = std::chrono::steady_clock::now();
    total_commands = 0;

    aof_file.open("appendonly.aof", std::ios::app | std::ios::binary);

    aof_recover();

    is_recovering = false;
}

Database::~Database() {
    if (aof_file.is_open()) {
        aof_file.close();
    }
}

// Logica AOF
void Database::aof_append(const std::vector<std::string>& args) {
    if (is_recovering || !aof_file.is_open()) return;

    const std::string resp_cmd = encode_resp(args);
    aof_file.write(resp_cmd.c_str(), resp_cmd.size());
    // aof_file.flush();
}

void Database::aof_recover() {
    std::ifstream file("appendonly.aof", std::ios::binary);
    if (!file.is_open()) return;

    std::string buffer;
    char temp[4096];

    while (file.read(temp, sizeof(temp))) {
        buffer.append(temp, file.gcount());
    }
    buffer.append(temp, file.gcount());
    file.close();

    size_t pos = 0;
    while (pos < buffer.length()) {
        if (buffer[pos] != '*') break;

        size_t crlf = buffer.find("\r\n", pos);
        if (crlf == std::string::npos) break;

        int num_args = std::stoi(buffer.substr(pos + 1, crlf - pos - 1));
        pos = crlf + 2;

        std::vector<std::string> args;
        for (int i = 0; i < num_args; i++) {
            crlf = buffer.find("\r\n", pos);
            int len = std::stoi(buffer.substr(pos + 1, crlf - pos - 1));
            pos = crlf + 2;
            args.push_back(buffer.substr(pos, len));
            pos += len + 2;
        }

        execute(-1, args);
    }
    printf("AOF Recovery finalizat. Baza de date este pregatita!\n");
}

void Database::set_client_room(const int client_fd, const std::string& room_name) {
    client_rooms[client_fd] = room_name;
}

std::string Database::get_client_room(const int client_fd) {
    // Daca clientul nu e in nicio camera, il punem in "default"
    if (!client_rooms.contains(client_fd)) {
        client_rooms[client_fd] = "default";
    }
    return client_rooms[client_fd];
}

// !!! Count-Sketch min alg

void Database::evict_despised_keys(const std::string& room_name) {
    auto& room = rooms[room_name];

    while (room.size() > MAX_KEYS_PER_ROOM) {
        constexpr int SAMPLE_SIZE = 5;
        std::string despised_key = "";
        uint16_t lowest_freq = 0xFFFF;

        size_t bucket_count = room.bucket_count();
        if (bucket_count == 0) break;

        static std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<size_t> dist(0, bucket_count - 1);

        for (int i = 0; i < SAMPLE_SIZE; ++i) {
            size_t random_bucket = dist(rng);
            auto it = room.begin(random_bucket);

            while (it == room.end(random_bucket)) {
                random_bucket = (random_bucket + 1) % bucket_count;
                it = room.begin(random_bucket);
            }

            const std::string& candidate_key = it->first;
            uint16_t freq = cms.estimate_frequency(candidate_key);

            if (freq < lowest_freq) {
                lowest_freq = freq;
                despised_key = candidate_key;
            }
        }

        if (lowest_freq > 10) {
            for (const auto &k: room | std::views::keys) {
                if (cms.estimate_frequency(k) <= 2) {
                    despised_key = k;
                    break;
                }
            }
        }

        if (!despised_key.empty() && room.contains(despised_key)) {
            ++global_metrics.keys_evicted;
            --global_metrics.keys_in_ram;

            disk_shield.add(despised_key);
            if (std::ofstream cold_file("despised_keys.bin", std::ios::app | std::ios::binary); cold_file.is_open()) {
                size_t rlen = room_name.size();
                size_t klen = despised_key.size();
                size_t vlen = room[despised_key]->value.size();

                cold_file.write(reinterpret_cast<const char*>(&rlen), sizeof(rlen));
                cold_file.write(room_name.data(), rlen);
                cold_file.write(reinterpret_cast<const char*>(&klen), sizeof(klen));
                cold_file.write(despised_key.data(), klen);
                cold_file.write(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
                cold_file.write(room[despised_key]->value.data(), vlen);
                cold_file.close();
            }

            record_pool.deallocate(room[despised_key]);
            room.erase(despised_key);
        } else {
            break;
        }
    }
}
std::string Database::execute(const int client_fd, const std::vector<std::string>& args) {
    if (args.empty()) return "";

    const std::string current_room = get_client_room(client_fd);
    std::string command = args[0];
    std::ranges::transform(command, command.begin(), ::toupper);

    if (command == "SUBSCRIBE") {
        if (args.size() >= 2) {
            return pubsub.subscribe(client_fd, args[1]);
        }
        return "-ERR Wrong number of arguments for SUBSCRIBE\r\n";
    }

    if (command == "PUBLISH") {
        if (args.size() >= 3) {
            return pubsub.publish(args[1], args[2]);
        }
        return "-ERR Wrong number of arguments for PUBLISH\r\n";
    }

    if (pubsub.is_subscribed(client_fd)) {
        return "-ERR Clientul este in mod SUBSCRIBE. Nu poti trimite comenzi de Database!\r\n";
    }

    std::lock_guard lock(db_mutex);

    total_commands++;

    std::string target_room = current_room;
    if (command == "ROOM" && args.size() >= 2) {
        target_room = args[1];
    }

    if (!rooms.contains(target_room)) {
        if (rooms.size() >= MAX_ACTIVE_ROOMS) {
            return "-ERR RAM FULL. Te rog asteapta ca o alta camera sa hiberneze!\r\n";
        }
        wakeup_room(target_room);
    }

    room_access_time[target_room] = get_current_time_ms();

    if (command == "COMMAND" || command == "HELLO") {
        return "*0\r\n";
    }

    if (command == "ROOM" && args.size() >= 2) {
        set_client_room(client_fd, args[1]);
        return "+OK\r\n";
    }

    if (command == "GET") {
        if (args.size() == 2) {
            const std::string& key = args[1];
            // some sort of L1
            ++global_metrics.total_gets;
            if (rooms[current_room].contains(key)) {
                cms.record_access(key);
                ++global_metrics.cache_hits;
                Record* r = rooms[current_room][key];

                if (r->expire_at > 0 && get_current_time_ms() > r->expire_at) {
                    record_pool.deallocate(r);
                    rooms[current_room].erase(key);
                    return "$-1\r\n";
                }
                return "$" + std::to_string(r->value.length()) + "\r\n" + r->value + "\r\n";
            }

            ++global_metrics.cache_misses;

            // ram miss - L2
            if (!disk_shield.possibly_exists(key)) {
                ++global_metrics.bloom_prevented_disk_reads;
                return "$-1\r\n";
            }

            std::string cold_val = read_from_cold_storage(current_room, key);

            if (!cold_val.empty()) {
                rooms[current_room][key] = record_pool.allocate(cold_val, 0);
                cms.record_access(key);

                global_metrics.keys_in_ram++;

                return "$" + std::to_string(cold_val.length()) + "\r\n" + cold_val + "\r\n";
            }
            return "$-1\r\n";
        }
        return "-ERR Wrong number of arguments for GET\r\n";
    }

    if (command == "SET") {
        if (args.size() >= 3) {
            const std::string& key = args[1];
            const std::string& value = args[2];
            global_metrics.total_sets++;

            if (!rooms[current_room].contains(key)) {
                global_metrics.keys_in_ram++;
            }

            long long expire_at = 0;

            if (rooms[current_room].contains(key)) {
                rooms[current_room][key]->value = value;
                rooms[current_room][key]->expire_at = expire_at;
            } else {
                rooms[current_room][key] = record_pool.allocate(value, expire_at);
            }

            cms.record_access(key);
            aof_append(args);

            evict_despised_keys(current_room);

            return "+OK\r\n";
        }
        return "-ERR Wrong number of arguments for SET\r\n";
    }

    if (command == "INFO") {
        const auto now = std::chrono::steady_clock::now();
        const auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
        size_t total_keys = 0;
        for (const auto &val: rooms | std::views::values) { // asta mi-a recomandat-o clang tidy, idk ce face si cum (TO LOOK UP AND LEARN !!!!!!!!!!!!)
            total_keys += val.size();
        }

        const std::string info_text =
            "Uptime: " + std::to_string(uptime) + "s\n" +
            "Camere active: " + std::to_string(rooms.size()) + "\n" +
            "Chei totale: " + std::to_string(total_keys) + "\n" +
            "Comenzi procesate: " + std::to_string(total_commands) + "\n";
        // il ducem inapoi in format RESP
        return "$" + std::to_string(info_text.length()) + "\r\n" + info_text + "\r\n";
    }

    if (command == "SAVE") {
        return "+OK AOF is active and up to date\r\n";
    }

    return "-ERR unknown command\r\n";
}

void Database::cleanup_client(const int client_fd) {
    pubsub.remove_client(client_fd);

    std::lock_guard lock(db_mutex);
    client_rooms.erase(client_fd);
}

/*
    functii pentru Snapshotting (de acum fara json!)
*/

void Database::wakeup_room(const std::string& room_name) {
    std::string filename = "room_" + room_name + ".bin"; // Schimbam extensia in .bin

    std::ifstream file(filename, std::ios::binary);
    if (file.is_open()) {
        try {
            rooms[room_name].clear();

            size_t num_records = 0;
            if (file.read(reinterpret_cast<char*>(&num_records), sizeof(num_records))) {
                for (size_t i = 0; i < num_records; ++i) {
                    size_t klen = 0;
                    file.read(reinterpret_cast<char*>(&klen), sizeof(klen));
                    std::string key(klen, '\0');
                    file.read(key.data(), klen);

                    size_t vlen = 0;
                    file.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));
                    std::string val(vlen, '\0');
                    file.read(val.data(), vlen);

                    long long expire_at = 0;
                    file.read(reinterpret_cast<char*>(&expire_at), sizeof(expire_at));

                    rooms[room_name][key] = record_pool.allocate(std::move(val), expire_at);
                }
            }
        } catch (const std::exception& e) {
            printf("-ERR eroare citire binara %s: %s\n", filename.c_str(), e.what());
            rooms[room_name] = {};
        }

        file.close();
        std::remove(filename.c_str());
    } else {
        rooms[room_name] = {};
    }
}

void Database::hibernate_inactive_rooms() {
    std::lock_guard lock(db_mutex);
    long long now = get_current_time_ms();
    std::vector<std::string> rooms_to_sleep;

    for (const auto& [room_name, last_time] : room_access_time) {
        if (now - last_time > 10000 && room_name != "default") {
            rooms_to_sleep.push_back(room_name);
        }
    }

    for (const std::string& r : rooms_to_sleep) {
        std::string filename = "room_" + r + ".bin";
        std::ofstream file(filename, std::ios::binary);

        if (file.is_open()) {
            size_t num_records = rooms[r].size();
            file.write(reinterpret_cast<const char*>(&num_records), sizeof(num_records));

            for (const auto& [key, record_ptr] : rooms[r]) {
                size_t klen = key.size();
                file.write(reinterpret_cast<const char*>(&klen), sizeof(klen));
                file.write(key.data(), klen);

                size_t vlen = record_ptr->value.size();
                file.write(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
                file.write(record_ptr->value.data(), vlen);

                file.write(reinterpret_cast<const char*>(&record_ptr->expire_at), sizeof(record_ptr->expire_at));

                record_pool.deallocate(record_ptr);
            }
            file.close();
        }

        rooms.erase(r);
        room_access_time.erase(r);
    }
}

// TTL Watchdog

void Database::clean_expired_keys() {
    std::lock_guard lock(db_mutex);

    const long long now = get_current_time_ms();
    int deleted_count = 0;

    for (auto &room_keys: rooms | std::views::values) {
        for (auto it = room_keys.begin(); it != room_keys.end(); ) {
            if (it->second->expire_at > 0 && it->second->expire_at < now) {
                record_pool.deallocate(it->second);
                it = room_keys.erase(it);
                deleted_count++;
            } else {
                ++it;
            }
        }
    }
    /*
    if (deleted_count > 0) {
        printf("🐶👀 (cainele cu gitul lunge) a sters %d chei expirate din RAM.\n", deleted_count);
    } */
}
