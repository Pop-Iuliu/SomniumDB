//
// Created by tiwerlol on 05.08.2026.
//
#include "pubsub.h"
#include <sys/socket.h>
#include <iostream>

std::string PubSubManager::subscribe(int client_fd, const std::string& channel) {
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
    std::lock_guard lock(ps_mutex);

    int receivers = 0;
    if (channel_subscribers.contains(channel)) {
        // RESP: [ "message", "nume_canal", "mesajul_efectiv" ]
        std::string msg_resp = "*3\r\n";
        msg_resp += "$7\r\nmessage\r\n";
        msg_resp += "$" + std::to_string(channel.length()) + "\r\n" + channel + "\r\n";
        msg_resp += "$" + std::to_string(message.length()) + "\r\n" + message + "\r\n";

        // trm mesajul
        for (const int fd : channel_subscribers[channel]) {
            send(fd, msg_resp.c_str(), msg_resp.size(), 0);
            receivers++;
        }
    }

    // returnam cat au primit
    return ":" + std::to_string(receivers) + "\r\n";
}

void PubSubManager::remove_client(int client_fd) {
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