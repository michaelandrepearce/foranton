//
// Created by Michael Pearce on 18/09/2017.
//

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
