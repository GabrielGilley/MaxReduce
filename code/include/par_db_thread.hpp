#pragma once

#include <thread>

using namespace std;
using namespace elga;

namespace pando {

class ParDB;

/** @brief Contains a launcher that creates a ParDB */
template <class T=ParDB>
class ParDBThread {
    private:
        /** @brief The internal ParDB */
        T db_;

        /** @brief The thread to run the server on */
        thread t_;

        /** @brief The shutdown signaling variable */
        volatile sig_atomic_t shutdown_;
    public:
        /** @brief Initialize a ParDB and run its server on a new thread */
        ParDBThread(ZMQAddress addr, size_t sz) : db_(addr, sz), shutdown_(false) {
            t_ = thread(&ParDBThread::launch, this);
        }
        ParDBThread(ZMQAddress addr) : ParDBThread(addr, 2ull*(1ull<<29)) { }
        /** @brief Cleanup the ParDB, including first shutting down gracefully */
        ~ParDBThread() { shutdown(); }
        /** @brief Shutdown the ParDB gracefully */
        void shutdown() {
            shutdown_ = true;
            join();
        }

        /** @brief Wait for the server to stop */
        void join() {
            if (t_.joinable())
                t_.join();
        }

        /** @brief Launch the ParDB server */
        void launch() {
            while (!shutdown_) {
                // Process messages, and wait if there are none (due to the
                // polling wait)
                db_.recv_msg();
            }
        }

        volatile sig_atomic_t* get_shutdown_flag() {
            return &shutdown_;
        }
};

}
