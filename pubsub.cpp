//
// Created by tiwerlol on 05.08.2026.
//
#include "pubsub.h"
#include <cstdio>
#include <iostream>
#include <vector>

std::string PubSubManager::subscribe(const int client_fd, const std::string& channel) {
    std::lock_guard lock(ps_mutex);

    channel_subscribers[channel].insert(client_fd);
    client_subscriptions[client_fd].insert(channel);

    int sub_count = client_subscriptions[client_fd].size();

    // facem raspunsul in RESP
    std::string resp = "*3\r\n";
    resp += "$9\r\nsubscribe\r\n";
    resp += "$" + std::to_string(channel.length()) + "\r\n" + channel + "\r\n";
    resp += ":" + std::to_string(sub_count) + "\r\n";

    printf("[PubSub] Clientul (FD: %d) s-a abonat la '%s'.\n", client_fd, channel.c_str());
    return resp;
}

std::string PubSubManager::publish(const std::string& channel, const std::string& message) {
    // livram DUPA eliberarea lock-ului: sink-ul poate deconecta un abonat lent
    // (capul de output), iar remove_client() ia acelasi mutex pe acelasi thread
    std::vector<int> targets;
    {
        std::lock_guard lock(ps_mutex);
        if (const auto it = channel_subscribers.find(channel); it != channel_subscribers.end()) {
            targets.assign(it->second.begin(), it->second.end());
        }
    }

    if (!targets.empty() && message_sink) {
        // RESP: [ "message", "nume_canal", "mesajul_efectiv" ]
        std::string msg_resp = "*3\r\n";
        msg_resp += "$7\r\nmessage\r\n";
        msg_resp += "$" + std::to_string(channel.length()) + "\r\n" + channel + "\r\n";
        msg_resp += "$" + std::to_string(message.length()) + "\r\n" + message + "\r\n";
        for (const int fd : targets) message_sink(fd, msg_resp);
    }

    // returnam cat au primit
    return ":" + std::to_string(targets.size()) + "\r\n";
}

void PubSubManager::remove_client(const int client_fd) {
    std::lock_guard lock(ps_mutex);

    if (!client_subscriptions.contains(client_fd)) {
        return;
    }
    for (const std::string& channel : client_subscriptions[client_fd]) {
        channel_subscribers[channel].erase(client_fd);
        if (channel_subscribers[channel].empty()) {
            channel_subscribers.erase(channel);
        }
    }

    client_subscriptions.erase(client_fd);
    printf("[PubSub] Clientul (FD: %d) deconectat. Rute curatate.\n", client_fd);
}

bool PubSubManager::is_subscribed(const int client_fd) {
    std::lock_guard lock(ps_mutex);
    return client_subscriptions.contains(client_fd);
}