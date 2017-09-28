//
// Created by Michael Pearce on 25/09/2017.
//

#ifndef UNTITLED3_INFLUX_H
#define UNTITLED3_INFLUX_H

#include <string>
#include <array>

#include <chrono>
#include <sstream>

namespace ig {
    namespace metrics {

        class InfluxSerializer {
        public:
            template<typename... Tags>
            InfluxSerializer(const std::string &metricName, Tags... tagArgs) {
                static_assert(sizeof...(Tags) % 2 == 0, "Expecters pairs of tagKey tagValue");
                std::array<const char *, sizeof...(Tags)> tags{tagArgs...};

                ss_ << metricName;

                for (size_t i = 0; i < tags.size(); i += 2) {
                    ss_ << ',' << tags[i] << '=' << tags[i + 1];
                }
            }

            template<typename Number>
            void addValue(const char *name, Number number) {
                if (hasAlreadyAddedAnyValue_) {
                    ss_ << ',';
                } else {
                    ss_ << ' ';
                }
                ss_ << name << '=' << number;

                hasAlreadyAddedAnyValue_ = true;
            }

            template<typename Number>
            void addValue(const char *name, Number number, std::string number_suffix) {
                if (hasAlreadyAddedAnyValue_) {
                    ss_ << ',';
                } else {
                    ss_ << ' ';
                }
                ss_ << name << '=' << number << number_suffix;

                hasAlreadyAddedAnyValue_ = true;
            }

            std::string finish(std::chrono::nanoseconds timestamp) {
                ss_ << ' ' << "" << timestamp.count();

                return ss_.str();
            }

        private:
            std::stringstream ss_;

            bool hasAlreadyAddedAnyValue_{false};
        };

    }
}

#endif //UNTITLED3_INFLUX_H
