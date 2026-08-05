//
// Created by tiwerlol on 05.08.2026.
//

#ifndef REDIS_PUBSUB_H
#define REDIS_PUBSUB_H

#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <string>

class PubSubManager {
private:
    std::unordered_map<std::string, std::unordered_set<int>> channel_subscribers;

    std::unordered_map<int, std::unordered_set<std::string>> client_subscriptions;

    std::mutex ps_mutex;

public:
    std::string subscribe(int client_fd, const std::string& channel);

    std::string publish(const std::string& channel, const std::string& message);

    void remove_client(int client_fd);

    bool is_subscribed(int client_fd);
};

#endif //REDIS_PUBSUB_H
