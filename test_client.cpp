//
// Created by Michael Pearce on 18/09/2017.
//

#include <proton/messaging_handler.hpp>
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/reconnect_options.hpp>

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
#include <unistd.h>
#include <sstream>


#include "metrics/metrics.hpp"
#include "message_delivery.hpp"
#include "connection.hpp"
#include "connection_factory.hpp"
#include "properties/propertyutil.h"



#define OUT(x) do { std::lock_guard<std::mutex> l(out_lock); x; } while (false)

namespace ig {

    class test_client {


    };
}

void splitCSV(std::string &in, std::vector<std::string> &out){
    std::stringstream ss(in);

    while( ss.good() )
    {
        std::string substr;
        getline( ss, substr, ',' );
        out.push_back( substr );
    }
}

void reconnect_options(PropertyUtil::PropertyMapT &properties, proton::reconnect_options &ro){

    std::string ro_failover_urls_csv = properties["reconnect.failover.urls"];
    if (!ro_failover_urls_csv.empty()) {
        std::vector<std::string> failover_urls;
        splitCSV(ro_failover_urls_csv, failover_urls);
        ro.failover_urls(failover_urls);
    }

    std::string ro_max_delay_ms = properties["reconnect.max.delay.ms"];
    if (!ro_max_delay_ms.empty()) {
        ro.max_delay(proton::duration(atol(ro_max_delay_ms.c_str())));
    }

    std::string ro_delay_ms = properties["reconnect.delay.ms"];
    if (!ro_delay_ms.empty()) {
        ro.delay(proton::duration(atol(ro_delay_ms.c_str())));
    }

    std::string ro_delay_multiplier = properties["reconnect.delay.multiplier"];
    if (!ro_delay_multiplier.empty()) {
        ro.delay_multiplier(atof(ro_delay_multiplier.c_str()));
    }

    std::string ro_max_attempts = properties["reconnect.max.attempts"];
    if (!ro_max_attempts.empty()) {
        ro.max_attempts(atoi(ro_max_attempts.c_str()));
    }
}

void connect_options(PropertyUtil::PropertyMapT &properties, proton::connection_options &co){

    std::string co_user = properties["connect.user"];
    if (!co_user.empty()) {
        co.user(co_user);
    }

    std::string co_password = properties["connect.password"];
    if (!co_password.empty()) {
        co.password(co_password);
    }

    std::string co_idle_timeout = properties["connect.idle.timeout.ms"];
    if (!co_idle_timeout.empty()) {
        co.idle_timeout(proton::duration(atol(co_idle_timeout.c_str())));
    }

    proton::reconnect_options ro;
    reconnect_options(properties, ro);
    co.reconnect(ro);
}






int main(int argc, const char **argv) {
    const char * filename;
    if (argc > 1) {
        filename = argv[1];
    } else {
        throw std::invalid_argument("properties must be passed in");
    }

    PropertyUtil::PropertyMapT properties;
    PropertyUtil::read(filename, properties);

    std::string url = properties["connect.url"];
    proton::connection_options co;
    connect_options(properties, co);



    ig::Metrics *metrics = new ig::Metrics(5);

    ig::connection connection(url, co, metrics);


    proton::container container(connection);
    std::thread container_thread([&]() { container.run(); });

    std::string address = "example";
    std::string queue = "example";

    auto producer = connection.create_producer(address);
    auto producer2 = connection.create_producer("example");

    auto consumer = connection.create_consumer(address);
    auto consumer2 = connection.create_consumer(address);

    int n_messages = 100;

    bool run = true;

    std::thread receiver([&]() {
        while (run) {
            auto msg = consumer->receive();
            //OUT(std::cout << "received \"" << msg.body() << '"' << std::endl);
        }
    });

    std::thread receiver2([&]() {
        while (run) {
            auto msg = consumer2->receive();
            //OUT(std::cout << "received \"" << msg.body() << '"' << std::endl);
        }
    });

    std::thread sender([&]() {
        while (run) {
            proton::message msg("hello");
            producer->send(msg);
            //OUT(std::cout << "sent \"" << msg.body() << '"' << std::endl);
        }
    });

    std::thread sender2([&]() {
        while (run) {
            proton::message msg("hello");
            producer2->send(msg);
            //OUT(std::cout << "sent \"" << msg.body() << '"' << std::endl);
        }
    });

    usleep(60000);

    run = false;

    sender.join();
    sender2.join();

    receiver.join();
    receiver2.join();

    struct ig::Metrics::Histogram::Snapshot snapshot;
    metrics -> request_latencies.get_snapshot(&snapshot);
    double rate = metrics -> request_rates.mean_rate();
    OUT(std::cerr << "rate= " << rate << std::endl);
    OUT(std::cerr << "mean= " << snapshot.mean << std::endl);
    OUT(std::cerr << "percentile_50th= " << snapshot.median << std::endl);
    OUT(std::cerr << "percentile_75th= " << snapshot.percentile_75th << std::endl);
    OUT(std::cerr << "percentile_90th= " << snapshot.percentile_90th << std::endl);
    OUT(std::cerr << "percentile_95th= " << snapshot.percentile_95th << std::endl);
    OUT(std::cerr << "percentile_98th= " << snapshot.percentile_98th << std::endl);
    OUT(std::cerr << "percentile_99th= " << snapshot.percentile_99th << std::endl);
    OUT(std::cerr << "percentile_999th= " << snapshot.percentile_999th << std::endl);


    connection.close();
    container_thread.join();
    free(metrics);
    free(consumer);
    free(consumer2);
    free(producer);
    free(producer2);


}
