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
        uint64_t previous_total_count = 0;
        bool metrics_report_console = true;
        bool metrics_report_influx = true;
        int test_no_consumers = 0;
        int test_no_producers = 0;
        long test_timeout_ms = 6000000;
        long metrics_report_ms = 1000000;
        std::string message_text = "hello";
        std::string address = "example";
        std::string queue = "example";
        std::string url = "amqp://localhost";
        ig::Metrics *metrics_;
        proton::connection_options co;

        PropertyUtil::PropertyMapT properties_;
    public:
        test_client(const PropertyUtil::PropertyMapT properties)
                : properties_(properties) {
            std::string test_no_consumers_str = properties_["test.consumer.count"];
            if (!test_no_consumers_str.empty()) {
                test_no_consumers = atoi(test_no_consumers_str.c_str());
            }

            std::string test_no_producers_str = properties_["test.producer.count"];
            if (!test_no_producers_str.empty()) {
                test_no_producers = atoi(test_no_producers_str.c_str());
            }

            std::string test_timeout_ms_str = properties_["test.timeout.ms"];
            if (!test_timeout_ms_str.empty()) {
                test_timeout_ms = atol(test_timeout_ms_str.c_str());
            }

            std::string message_text_str = properties_["test.message.text"];
            if (!message_text_str.empty()) {
                message_text = message_text_str;
            }
            std::string address_str = properties_["test.producer.address"];
            if (!address_str.empty()) {
                address = address_str;
            }
            std::string queue_str = properties_["test.consumer.queue"];
            if (!queue_str.empty()) {
                queue = queue_str;
            }

            std::string metrics_report_ms_str = properties_["metrics.report.ms"];
            if (!metrics_report_ms_str.empty()) {
                test_timeout_ms = atol(metrics_report_ms_str.c_str());
            }

            std::string metrics_report_console_str = properties_["metrics.report.console"];
            if (!metrics_report_console_str.empty()) {
                metrics_report_console = to_bool(metrics_report_console_str);
            }

            std::string metrics_report_kafka_str = properties_["metrics.report.influx"];
            if (!metrics_report_kafka_str.empty()) {
                metrics_report_influx = to_bool(metrics_report_kafka_str);
            }

            metrics_ = new ig::Metrics(test_no_consumers + 1);

            std::string url_str = properties_["connect.url"];
            if (!url_str.empty()) {
                url = url_str;
            }
            connect_options(properties_, co);

        }

        void run() {
            ig::connection connection(url, co, metrics_);

            proton::container container(connection);
            std::thread container_thread([&]() { container.run(); });

            bool run = true;

            std::vector<std::thread*> consumers;
            for (int i = 0; i < test_no_consumers; i++) {
                std::thread *receiver = new std::thread([&]() {
                    auto consumer = connection.create_consumer(queue);
                    while (run) {
                        auto msg = consumer->receive();
                        //OUT(std::cout << "received \"" << msg.body() << '"' << std::endl);
                    }
                    consumer->close();
                });
                consumers.push_back(receiver);
            }

            std::vector<std::thread*> producers;
            for (int i = 0; i < test_no_producers; i++) {
                std::thread *sender = new std::thread([&]() {
                    auto producer = connection.create_producer(address);
                    while (run) {
                        proton::message msg(message_text);
                        producer->send(msg);
                        //OUT(std::cout << "sent \"" << msg.body() << '"' << std::endl);
                    }
                    producer -> close();
                });
                producers.push_back(sender);
            }


            std::thread metrics_reporter([&]() {
                while (run) {
                    usleep(metrics_report_ms);
                    report_metrics(metrics_);
                }
            });


            usleep(test_timeout_ms);

            run = false;
            metrics_reporter.join();

            for (auto &producer : producers) {
                producer -> join();
            }
            for (auto &consumer : consumers) {
                consumer -> join();
            }

            report_metrics(metrics_);



            connection.close();
            container_thread.join();
        }

        void report_metrics(ig::Metrics *metrics) {
            struct ig::Metrics::Histogram::Snapshot snapshot;
            metrics -> request_latencies.get_snapshot(&snapshot);
            uint64_t total_count = metrics -> request_rates.count();
            uint64_t count = total_count - previous_total_count;
            previous_total_count = total_count;
            double mean_rate = metrics -> request_rates.mean_rate();
            if (metrics_report_console)
                report_metrics_console(count, mean_rate, snapshot);
            if (metrics_report_influx)
                report_metrics_influxdb(count, mean_rate, snapshot);
        }

        void report_metrics_console(uint64_t count, double mean_rate, ig::Metrics::Histogram::Snapshot &snapshot) {
            OUT(std::cerr << "count= " << count << std::endl);
            OUT(std::cerr << "rate= " <<  mean_rate << std::endl);
            OUT(std::cerr << "mean= " << snapshot.mean << std::endl);
            OUT(std::cerr << "min= " << snapshot.min << std::endl);
            OUT(std::cerr << "max= " << snapshot.max << std::endl);
            OUT(std::cerr << "percentile_50th= " << snapshot.median << std::endl);
            OUT(std::cerr << "percentile_75th= " << snapshot.percentile_75th << std::endl);
            OUT(std::cerr << "percentile_90th= " << snapshot.percentile_90th << std::endl);
            OUT(std::cerr << "percentile_95th= " << snapshot.percentile_95th << std::endl);
            OUT(std::cerr << "percentile_98th= " << snapshot.percentile_98th << std::endl);
            OUT(std::cerr << "percentile_99th= " << snapshot.percentile_99th << std::endl);
            OUT(std::cerr << "percentile_999th= " << snapshot.percentile_999th << std::endl);
        }

        void report_metrics_influxdb(uint64_t count, double mean_rate, ig::Metrics::Histogram::Snapshot &snapshot) {
            //TODO
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

        bool to_bool(std::string str) {
            std::transform(str.begin(), str.end(), str.begin(), ::tolower);
            std::istringstream is(str);
            bool b;
            is >> std::boolalpha >> b;
            return b;
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
    };
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

    ig::test_client test_client(properties);
    test_client.run();
}
