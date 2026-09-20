#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <functional>
#include <mutex>

// Format AOF: cate o linie RESP per comanda, de forma [room, args...].
// Prima intrare este camera-tinta, ca replay-ul sa nu depinda de contextul clientului.
// Fisierul vechi (fara prefix de camera) e recunoscut si redat in "default".
class AOFManager {
private:
    std::ofstream aof_file;
    bool is_recovering;
    std::mutex aof_mutex;

    static std::string encode_resp(const std::vector<std::string>& args);

public:
    AOFManager();
    ~AOFManager();

    void append(const std::string& room_name, const std::vector<std::string>& args);
    void recover(const std::function<void(const std::string&, const std::vector<std::string>&)>& execute_callback);

    bool recovering() const { return is_recovering; }
};
