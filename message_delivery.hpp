//
// Created by Michael Pearce on 21/09/2017.
//
#ifndef message_delivery_h
#define message_delivery_h

#include <proton/message.hpp>
#include <proton/delivery.hpp>

namespace ig {

    class message_delivery {
        proton::message message_;
        proton::delivery delivery_;

    public:
        message_delivery(const proton::message &message, const proton::delivery &delivery) : message_(message), delivery_(delivery) {}

        proton::message message(){
            return message_;
        }

        proton::delivery delivery(){
            return delivery_;
        }
    };

}
#endif