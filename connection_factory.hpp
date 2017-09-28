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

#ifndef connection_factory_h
#define connection_factory_h

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
#include "connection.hpp"


namespace ig {

    class connection_factory : public proton::messaging_handler {

        ig::Metrics *metrics_;

        // Invariant
        const std::string url_;
        const proton::connection_options connection_options_;

    public:
        connection_factory(const std::string &url, const proton::connection_options connection_options, ig::Metrics *metrics)
                : url_(url), connection_options_(connection_options), metrics_(metrics) {
        }

        ig::connection *createConnection(){
            return new ig::connection(url_, connection_options_, metrics_);
        }

    };

} // namespace ig
#endif