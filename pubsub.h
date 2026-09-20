//
// Created by tiwerlol on 05.08.2026.
//

#ifndef REDIS_PUBSUB_H
#define REDIS_PUBSUB_H

#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <string>
#include <functional>

class PubSubManager {
private:
    std::unordered_map<std::string, std::unordered_set<int>> channel_subscribers;

    std::unordered_map<int, std::unordered_set<std::string>> client_subscriptions;

    std::mutex ps_mutex;

    // livrarea mesajelor e delegata serverului (coada de output a clientului);
    // nu scriem niciodata direct in socket din pubsub
    std::function<void(int, const std::string&)> message_sink;

public:
    void set_message_sink(std::function<void(int, const std::string&)> fn) {
        message_sink = std::move(fn);
    }

    std::string subscribe(int client_fd, const std::string& channel);

    std::string publish(const std::string& channel, const std::string& message);

    void remove_client(int client_fd);

    bool is_subscribed(int client_fd);
};

#endif //REDIS_PUBSUB_H
