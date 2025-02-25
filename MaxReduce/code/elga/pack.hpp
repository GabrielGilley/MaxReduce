/**
 * ElGA packing helpers
 *
 * Author: Kasimir Gabert
 *
 * Copyright 2021 National Technology & Engineering Solutions of Sandia, LLC
 * (NTESS). Under the terms of Contract DE-NA0003525 with NTESS, the U.S.
 * Government retains certain rights in this software.
 *
 * Please see the LICENSE.md file for license information.
 */

#include "types.hpp"
#include <cstring>

#ifndef PACK_HPP
#define PACK_HPP

namespace elga {

    const size_t pack_msg_uint64_size = sizeof(msg_type_t)+sizeof(uint64_t);
    const size_t pack_msg_size_size = sizeof(msg_type_t)+sizeof(size_t);
    const size_t pack_msg_agent_size = sizeof(msg_type_t)+sizeof(uint64_t);

    template <typename T>
    inline __attribute__((always_inline))
    void pack_single(char *&msg, T t) {
        *(T*)msg = t;
        msg += sizeof(T);
    }

    template <typename T>
    inline __attribute__((always_inline))
    void unpack_single(const char *&msg, T &t) {
        t = *(T*)msg;
        msg += sizeof(T);
    }

    template <typename T>
    inline __attribute__((always_inline))
    T unpack_single(const char *&msg) {
        T t;
        unpack_single<T>(msg, t);
        return t;
    }

    inline __attribute__((always_inline))
    void pack_msg(char *&msg, msg_type_t t) {
        pack_single(msg, t);
    }

    inline __attribute__((always_inline))
    void pack_string(char *&msg, const std::string& s) {
        strncpy(msg, s.c_str(), s.size());
        msg += s.size();
    }

    inline __attribute__((always_inline))
    void pack_string_null_term(char *&msg, const std::string& s) {
        strcpy(msg, s.c_str());
        msg += s.size() + 1;
    }

    inline __attribute__((always_inline))
    msg_type_t unpack_msg(const char *&msg) {
        msg_type_t t;
        unpack_single(msg, t);
        return t;
    }

    inline __attribute__((always_inline))
    uint64_t pack_agent(uint64_t agent_ser, aid_t aid) {
        return agent_ser | (uint64_t)aid << 49;
    }

    inline __attribute__((always_inline))
    void unpack_agent(uint64_t in, uint64_t &agent_ser, aid_t &aid) {
        agent_ser = in & ((1llu << 49)-1);
        aid = in >> 49;
    }

    inline __attribute__((always_inline))
    void pack_msg_agent(char *&msg, msg_type_t t, uint64_t agent_ser, aid_t aid) {
        *(msg_type_t*)msg = t;
        msg += sizeof(msg_type_t);
        uint64_t i = pack_agent(agent_ser, aid);
        *(uint64_t*)msg = i;
        msg += sizeof(uint64_t);
    }

    inline __attribute__((always_inline))
    void unpack_msg_agent(char *&msg, msg_type_t &t, uint64_t &agent_ser, aid_t &aid) {
        t = *(msg_type_t*)msg;
        msg += sizeof(msg_type_t);

        uint64_t i = *(uint64_t*)msg;
        unpack_agent(i, agent_ser, aid);

        msg += sizeof(uint64_t);
    }
}

#endif
