// Java-Style Properties in C++
//
// (c) Paul D. Senzee
// Senzee 5
// http://senzee.blogspot.com

#ifndef _PROPERTYUTIL_H
#define _PROPERTYUTIL_H

#include <map>
#include <string>
#include <iostream>
#include <unordered_map>

class properties_util
{

public:

    typedef std::unordered_map<std::string, std::string> properties;
    typedef properties::value_type           value_type;
    typedef properties::iterator             iterator;

    static void read(const char *filename, properties &map);
    static void read(std::istream &is, properties &map);
    static void write(const char *filename, properties &map, const char *header = NULL);
    static void write(std::ostream &os, properties &map, const char *header = NULL);
    static void print(std::ostream &os, properties &map);

private:

    static inline char m_hex(int nibble)
    {
        static const char *digits = "0123456789ABCDEF";
        return digits[nibble & 0xf];
    }
};

#endif  // _PROPERTYUTIL_H