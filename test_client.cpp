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

#include <proton/messaging_handler.hpp>
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/reconnect_options.hpp>
#include <proton/version.h>

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
#include "influx_serializer.h"
#include <librdkafka/rdkafkacpp.h>
#include <ifaddrs.h>


#define OUT(x) do { std::lock_guard<std::mutex> l(out_lock); x; } while (false)

namespace ig {


    class test_client {
        uint64_t previous_run_count = 0;
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
        std::string environment = "dev";
        std::string application_name = "cpp_client";
        std::string application_version = "1.0.0";
        std::string jms_provider = "proton-cpp";
        std::string jms_provider_version = std::to_string(PN_VERSION_MAJOR) + "." + std::to_string(PN_VERSION_MINOR) + "." + std::to_string(PN_VERSION_POINT);
        std::string host_address;
        std::string host_name;

        ig::Metrics *metrics_;
        RdKafka::Producer *kafka_producer;
        RdKafka::Topic *kafka_topic;

        proton::connection_options co;

        ig::connection_factory *connection_factory_;

        properties_util::properties properties_;
    public:
        test_client(const properties_util::properties properties)
                : properties_(properties) {


            host_name = gethostname_str();

            host_address = gethostaddress_str();

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
            std::string environment_str = properties_["test.environment"];
            if (!environment_str.empty()) {
                environment = environment_str;
            }
            std::string application_name_str = properties_["test.application.name"];
            if (!application_name_str.empty()) {
                application_name = application_name_str;
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

            if (metrics_report_influx) {
                RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::ConfType::CONF_GLOBAL);

                OUT(std::cout << "kafka version \"" << RdKafka::version_str() << '"' << std::endl);


                std::string brokers = "godzilla-kafka-1.test.iggroup.local:9092,godzilla-kafka-2.test.iggroup.local:9092,godzilla-kafka-3.test.iggroup.local:9092";
                std::string errstr;
                std::string topic_str = "com_ig_influx_v1_jms_metrics";


                conf->set("metadata.broker.list", brokers, errstr);


                /*
                 * Create producer using accumulated global configuration.
                 */
                kafka_producer = RdKafka::Producer::create(conf, errstr);
                if (!kafka_producer) {
                    std::cerr << "Failed to create producer: " << errstr << std::endl;
                    exit(1);
                }


                /*
                 * Create topic handle.
                 */
                kafka_topic = RdKafka::Topic::create(kafka_producer, topic_str,
                                                               NULL, errstr);
                if (!kafka_topic) {
                    std::cerr << "Failed to create topic: " << errstr << std::endl;
                    exit(1);
                }
            }

            metrics_ = new ig::Metrics(test_no_consumers + 1);

            std::string url_str = properties_["connect.url"];
            if (!url_str.empty()) {
                url = url_str;
            }
            connect_options(properties_, co);

            connection_factory_ = new ig::connection_factory(url, co, metrics_);
        }

        void run() {
            ig::connection *connection = connection_factory_ -> createConnection();

            proton::container container(*connection);
            std::thread container_thread([&]() { container.run(); });

            bool run = true;

            std::vector<std::thread*> consumers;
            for (int i = 0; i < test_no_consumers; i++) {
                std::thread *receiver = new std::thread([&]() {
                    auto consumer = connection -> create_consumer(queue);
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
                    auto producer = connection -> create_producer(address);
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



            connection -> close();
            container_thread.join();
            free(connection);
        }

        void destroy() {
            delete connection_factory_;
            delete metrics_;
            delete kafka_producer;
        }

        void report_metrics(ig::Metrics *metrics) {
            struct ig::Metrics::Histogram::Snapshot snapshot;
            metrics -> request_latencies.get_snapshot(&snapshot, true);
            std::chrono::nanoseconds timestamp = std::chrono::duration_cast< std::chrono::nanoseconds >(
                    std::chrono::system_clock::now().time_since_epoch()
            );


            uint64_t run_count = metrics -> request_rates.count();
            uint64_t count = run_count - previous_run_count;
            previous_run_count = run_count;
            double mean_rate = metrics -> request_rates.mean_rate();
            if (metrics_report_console)
                report_metrics_console(timestamp, count, run_count, mean_rate, snapshot);
            if (metrics_report_influx)
                report_metrics_influxdb(timestamp, count, run_count, mean_rate, snapshot);
        }

        void report_metrics_console(std::chrono::nanoseconds timestamp, uint64_t count, uint64_t run_count, double mean_rate, ig::Metrics::Histogram::Snapshot &snapshot) {
            OUT(std::cerr << "count= " << count << std::endl);
            OUT(std::cerr << "run-count= " << run_count << std::endl);
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

        void report_metrics_influxdb(std::chrono::nanoseconds timestamp, uint64_t count, uint64_t run_count, double mean_rate, ig::Metrics::Histogram::Snapshot &snapshot) {


            ig::metrics::InfluxSerializer influxSerializer("jms_metric_latency", "application_name", application_name.c_str(), "application_version", application_version.c_str(), "type", "CONSUMER", "destination", address.c_str(), "environment", environment.c_str(), "host", host_name.c_str(), "host_address", host_address.c_str(), "jms_provider", jms_provider.c_str(), "jms_provider_version", jms_provider_version.c_str());

            influxSerializer.addValue("count", count, "i");
            influxSerializer.addValue("run-count", run_count, "i");

            influxSerializer.addValue("mean", snapshot.mean);
            influxSerializer.addValue("min", snapshot.min);
            influxSerializer.addValue("max", snapshot.max);

            influxSerializer.addValue("50-percentile", snapshot.median);
            influxSerializer.addValue("75-percentile", snapshot.percentile_75th);
            influxSerializer.addValue("90-percentile", snapshot.percentile_90th);
            influxSerializer.addValue("95-percentile", snapshot.percentile_95th);
            influxSerializer.addValue("98-percentile", snapshot.percentile_98th);
            influxSerializer.addValue("99-percentile", snapshot.percentile_99th);
            influxSerializer.addValue("999-percentile", snapshot.percentile_999th);

            std::string influx_str = influxSerializer.finish(timestamp);

            int32_t partition = RdKafka::Topic::PARTITION_UA;

            /*
             * Produce message
             */
            RdKafka::ErrorCode resp =
                    kafka_producer->produce(kafka_topic, partition,
                                      RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                                      const_cast<char *>(influx_str.c_str()), influx_str.size(),
                                      NULL, NULL);

            if (resp != RdKafka::ERR_NO_ERROR)
                std::cerr << "% Produce failed: " <<
                          RdKafka::err2str(resp) << std::endl;
            else
                std::cerr << "% Produced message (" << influx_str.size() << " bytes)" <<
                          std::endl;

            std::cout << influx_str;
            kafka_producer->poll(1000);
        }

        std::string gethostname_str(){
            int HOST_NAME_MAX = 255;
            char hostname[HOST_NAME_MAX];
            gethostname(hostname, HOST_NAME_MAX);
            return std::string(hostname);
        }

        std::string gethostaddress_str(){
            std::string hostaddress = "";
            struct ifaddrs * ifAddrStruct=NULL;
            struct ifaddrs * ifa=NULL;
            void * tmpAddrPtr=NULL;

            getifaddrs(&ifAddrStruct);

            for (ifa = ifAddrStruct; ifa != NULL; ifa = ifa->ifa_next) {
                if (!ifa->ifa_addr) {
                    continue;
                }
                if (ifa->ifa_addr->sa_family == AF_INET) { // check it is IP4
                    // is a valid IP4 Address
                    tmpAddrPtr=&((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
                    char addressBuffer[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
                    printf("%s IP Address %s\n", ifa->ifa_name, addressBuffer);
                    hostaddress = std::string(addressBuffer, INET_ADDRSTRLEN);
                } else if (ifa->ifa_addr->sa_family == AF_INET6) { // check it is IP6
                    // is a valid IP6 Address
                    tmpAddrPtr=&((struct sockaddr_in6 *)ifa->ifa_addr)->sin6_addr;
                    char addressBuffer[INET6_ADDRSTRLEN];
                    inet_ntop(AF_INET6, tmpAddrPtr, addressBuffer, INET6_ADDRSTRLEN);
                    printf("%s IP Address %s\n", ifa->ifa_name, addressBuffer);
                    hostaddress = std::string(addressBuffer, INET6_ADDRSTRLEN);
                }
            }
            if (ifAddrStruct!=NULL)
                freeifaddrs(ifAddrStruct);
            return hostaddress;
        }

        void reconnect_options(properties_util::properties &properties, proton::reconnect_options &ro){

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

        void connect_options(properties_util::properties &properties, proton::connection_options &co){

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

    properties_util::properties properties;
    properties_util::read(filename, properties);

    ig::test_client test_client(properties);
    test_client.run();
    test_client.destroy();
}