#pragma once

#include <iostream>
#include <sstream>

using namespace std;
class Terr {
    private:
        stringstream os_;
    public:
        Terr() { }
        ~Terr() { close(); }

        // Rule of 5
        Terr(const Terr& other) = delete;
        Terr(Terr&& other) noexcept = delete;
        Terr& operator=(const Terr& other) = delete;
        Terr& operator=(Terr&& other) noexcept = delete;

        template<class T>
        Terr &operator << (const T &msg) {
            os_ << msg;
            return *this;
        }

        void close() {
            cerr << os_.str();
            stringstream().swap(os_);
        }
};
