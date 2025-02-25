#pragma once

#include <chrono>
#include "dbentry.hpp"

using namespace std;
using namespace std::chrono;

class RandomKeyGen {
    private:
        //Some values for random key generation
        mt19937_64 mt_;
        uniform_int_distribution<chain_info_t> rnd_c_;
        uniform_int_distribution<vtx_t> rnd_v_;

    public:
        RandomKeyGen() {
            //There were issues seeding this way, so ideally this would never
            //be used in favor of seeding with an agent_id. However, this is a
            //"best effort" that is sufficient for test cases, etc.
            random_device rd;
            mt_.seed(rd());
        }
        RandomKeyGen(uint64_t agent_id) {
            initialize_seed(agent_id);
        }

        void initialize_seed(uint64_t agent_id) {
            uint64_t seed;
            auto now_time = high_resolution_clock::now();
            uint64_t now = duration_cast<nanoseconds>(now_time.time_since_epoch()).count();

            uint64_t first_of_id = agent_id >> 32;
            first_of_id = first_of_id << 32;

            uint64_t last_of_time = now << 32;
            last_of_time = last_of_time >> 32;

            seed = first_of_id | last_of_time;

            mt_.seed(seed);

        }

        dbkey_t get() {
            dbkey_t key;

            key.a = (1llu << 63) | rnd_c_(mt_);
            key.b = rnd_v_(mt_);
            key.c = rnd_v_(mt_);

            return key;
        }

        /** @brief Check if the entry is still using the INITIAL_KEY, and
         * randomly initialize it if so */
        template<typename Alloc>
        void verify_entry_key(DBEntry<Alloc>* e) {
            if ((*e).get_key() == INITIAL_KEY) {
                (*e).set_key(get());
            }
        }

};
