/**
 * ElGA Consistent Hasher Class
 *
 * Author: Kaan Sancak, Kasimir Gabert
 *
 * Copyright 2021 National Technology & Engineering Solutions of Sandia, LLC
 * (NTESS). Under the terms of Contract DE-NA0003525 with NTESS, the U.S.
 * Government retains certain rights in this software.
 *
 * Please see the LICENSE.md file for license information.
 */

#ifndef CONSISTENT_HASHER_HPP
#define CONSISTENT_HASHER_HPP

#define TABLE_WIDTH 262144
#define TABLE_DEPTH 8

#include <algorithm>
#include <cmath>
#include <iostream>

#include <vector>
#include "absl/container/flat_hash_map.h"

#include "replicationmap.hpp"
#include "integer_hash.hpp"
#include "pack.hpp"

#include "types.hpp"
#include "dbkey.h"

class ConsistentHasher {
    private:
        std::vector<uint64_t> agents_;
        const ReplicationMap &rm_;
        std::vector<uint64_t> ring_;
        absl::flat_hash_map<uint64_t, uint64_t> agent_map_;

    public:
        ConsistentHasher(std::vector<uint64_t> &agents, ReplicationMap &rm);

        /** Return the number of replicas for a given key */
        int32_t count_reps(uint64_t key) { return rm_.query(key); }

        /** Retrieve all of the containers for a given key */
        std::vector<uint64_t> find(uint64_t key);

        /** Retrieve a single u.r. container in the consistent hash ring */
        uint64_t find_one(uint64_t key, uint64_t owner_check, bool &have_ownership);

        /** Retrieve the agent address for a given db key */
        template <class T>
        addr_t lookup_agent(T k) {
            k.c = 0;
            uint64_t h = (std::hash<T>{}(k));

            std::vector<uint64_t> containers = find(h);
            if (containers.size() != 1) throw std::runtime_error("Need to handle partitioning");

            // Remove the virtual component
            addr_t res; aid_t a;
            elga::unpack_agent(containers[0], res, a);

            return res;
        }

        /** Support replacing the agents */
        void update_agents(std::vector<uint64_t> &agents);

        /** Return the size of the CH ring */
        size_t ring_size() const { return ring_.size(); }
};

#endif
