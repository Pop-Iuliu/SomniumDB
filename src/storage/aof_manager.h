#ifndef REDIS_AOF_MANAGER_H
#define REDIS_AOF_MANAGER_H

#include <string>
#include <vector>
#include <fstream>
#include <functional>
#include <mutex>

class AOFManager {
private:
    std::ofstream aof_file;
    bool is_recovering;
    std::mutex aof_mutex;

    static std::string encode_resp(const std::vector<std::string>& args);

public:
    AOFManager();
    ~AOFManager();

    void append(const std::vector<std::string>& args);
    void recover(const std::function<void(const std::vector<std::string>&)>& execute_callback);

    bool recovering() const { return is_recovering; }
};

#endif //REDIS_AOF_MANAGER_H
