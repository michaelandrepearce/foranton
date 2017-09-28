/*
  Copyright (c) 2017 IG Group

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

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
            msg.properties().put("MetricsEpochSecond", send_time.tv_sec);
            msg.properties().put("MetricsNano", send_time.tv_nsec);
            // Use [=] to copy the message, we cannot pass it by reference since it
            // will be used in another thread.
            work_queue_->add([=]() { sender_.send(msg); });
        }

        proton::sender sender() {
            return sender_;
        }


        void close() {
            sender_.close();
        }

    private:
        proton::work_queue *work_queue() {
            return work_queue_;
        }
    };
}
#endif