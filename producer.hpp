//
// Created by Michael Pearce on 18/09/2017.
//

#ifndef producer_h
#define producer_h

#include <proton/messaging_handler.hpp>
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/work_queue.hpp>

#include <chrono>
#include <mutex>
#include <iostream>
#include <queue>
#include <proton/message.hpp>
#include <proton/delivery.hpp>
#include <map>
#include <unordered_map>
#include <thread>

#include "metrics/metrics.hpp"
#include "message_delivery.hpp"

namespace ig {
    class producer {

        proton::sender sender_;
        proton::work_queue *work_queue_;

    public:
        producer(const proton::sender &sender) : sender_(sender), work_queue_(&sender.work_queue()) {}

        // Thread safe
        void send(proton::message &msg) {
            struct timespec send_time;
            clock_gettime(CLOCK_REALTIME, &send_time);
            msg.properties().put("tv_sec", send_time.tv_sec);
            msg.properties().put("tv_nsec", send_time.tv_nsec);
            // Use [=] to copy the message, we cannot pass it by reference since it
            // will be used in another thread.
            work_queue_->add([=]() { sender_.send(msg); });
        }

        proton::sender sender() {
            return sender_;
        }


        // Thread safe
        void close() {
            work_queue_->add([=]() { sender_.close(); });
        }

    private:
        proton::work_queue *work_queue() {
            return work_queue_;
        }
    };
}
#endif