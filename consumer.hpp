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

#ifndef consumer_h
#define consumer_h

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
#include <utility>

#include "metrics/metrics.hpp"
#include "message_delivery.hpp"


namespace ig {

    class consumer {

        // Shared by proton and user threads, protected by lock_
        std::mutex lock_;
        proton::receiver receiver_;
        proton::work_queue *work_queue_;
        std::queue<proton::message> messages_;
        std::condition_variable messages_ready_;
        ig::Metrics *metrics_;


    public:
        consumer(proton::receiver receiver, ig::Metrics *metrics) : receiver_(std::move(receiver)),
                                                                           work_queue_(&receiver_.work_queue()),
                                                                           metrics_(metrics) {}

        // Thread safe
        proton::message receive() {
            std::unique_lock<std::mutex> l(lock_);
            while (messages_.empty()) messages_ready_.wait(l);
            proton::message msg = std::move(messages_.front());
            messages_.pop();
            if (metrics_) {
                struct timespec recieve_time;
                clock_gettime(CLOCK_REALTIME, &recieve_time);

                struct timespec send_time;
                send_time.tv_sec = proton::get<int64_t>(msg.properties().get("MetricsEpochSecond"));
                send_time.tv_nsec = proton::get<int64_t>(msg.properties().get("MetricsNano"));

                long latency = diff_nanoseconds(recieve_time, send_time);
                metrics_->record_request(latency);
            }
            return msg;
        }

        void on_message(proton::delivery &dlv, proton::message &msg) {
            std::lock_guard<std::mutex> l(lock_);
            messages_.push(msg);
            messages_ready_.notify_all();
            dlv.accept();
        }

        proton::receiver receiver() {
            return receiver_;
        }


        void close() {
            receiver_.close();
        }

    private:
        proton::work_queue *work_queue() {
            return work_queue_;
        }

        long diff_nanoseconds(timespec t1, timespec t2) {
            long diff_seconds = (t1.tv_sec - t2.tv_sec) * 1000000000;
            long diff_nanos = t1.tv_nsec - t2.tv_nsec;
            return diff_seconds + diff_nanos;
        }


    };
}
#endif