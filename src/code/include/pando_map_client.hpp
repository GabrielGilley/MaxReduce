#pragma once

#include "util.hpp"
#include "chatterbox.hpp"

using namespace std;
using namespace elga;

namespace pando {

class PandoMapClient : public ZMQRequester {
    private:
    public:
        using ZMQRequester::ZMQRequester;

        size_t size() {
            send(MAP_SIZE);
            ZMQMessage resp = read();
            const char *resp_data = resp.data();
            size_t ret = unpack_single<size_t>(resp_data);
            return ret;
        }

        void insert(DBEntry<> e) {
            // Serialize the DB entry
            size_t msg_size = sizeof(msg_type_t)+e.serialize_size();
            char* msg = new char[msg_size];
            char* msg_ptr = msg;

            pack_msg(msg_ptr, MAP_INSERT);

            // Add the actual entry
            e.serialize(msg_ptr);


            // Send it to the PandoMap
            send(msg, msg_size);

            delete [] msg;

            wait_ack();
        }

        void insert_multiple(absl::flat_hash_map<dbkey_t, DBEntry<>>* entries) {
            size_t msg_size = sizeof(msg_type_t);
            for (auto & [key, entry] : *entries) {
                msg_size += entry.serialize_size();
            }
            char* msg = new char[msg_size];
            char* msg_ptr = msg;

            pack_msg(msg_ptr, MAP_INSERT_MULTIPLE);

            // Add the actual entry
            for (auto & [key, entry] : *entries) {
                entry.serialize(msg_ptr);
            }

            // Send it to the PandoMap
            send(msg, msg_size);

            delete [] msg;

            wait_ack();
        }

        tuple<DBEntry<>, bool> retrieve_if_exists(dbkey_t key) {
            // Serialize the key and tag
            size_t msg_size = sizeof(msg_type_t)+sizeof(dbkey_t);
            char msg[msg_size];
            char* msg_ptr = msg;

            pack_msg(msg_ptr, MAP_RETRIEVE_IF_EXISTS);

            // Add the actual entry
            pack_single(msg_ptr, key);

            // Send it to the PandoMap
            send(msg, msg_size);

            ZMQMessage resp = read();
            const char *resp_data = resp.data();
            bool exists;
            unpack_single(resp_data, exists);

            if (!exists) {
                DBEntry e;
                return {move(e), exists};
            } else {
                DBEntry e {resp_data};
                return {move(e), exists};
            }
        }


        DBEntry<> retrieve(dbkey_t key) {
            // Serialize the key and tag
            size_t msg_size = sizeof(msg_type_t)+sizeof(dbkey_t);
            char msg[msg_size];
            char* msg_ptr = msg;

            pack_msg(msg_ptr, MAP_RETRIEVE);

            // Add the actual entry
            pack_single(msg_ptr, key);

            // Send it to the PandoMap
            send(msg, msg_size);

            ZMQMessage resp = read();
            const char *resp_data = resp.data();
            DBEntry e {resp_data};
            return e;
        }

        vector<DBEntry<>> retrieve_all_entries() {
            // Serialize the key and tag
            size_t msg_size = sizeof(msg_type_t);
            char msg[msg_size];
            char* msg_ptr = msg;

            pack_msg(msg_ptr, MAP_GET_ENTRIES);

            // Send it to the PandoMap
            send(msg, msg_size);

            ZMQMessage resp = read();
            const char* resp_data = resp.data();

            size_t recvd_msg_size;
            unpack_single(resp_data, recvd_msg_size);

            const char *resp_end = resp_data + recvd_msg_size - sizeof(size_t);;

            vector<DBEntry<>> entries;
            entries.reserve(1ull<<16);
            while (resp_data < resp_end) {
                DBEntry e {resp_data};
                entries.push_back(move(e));
            }

            return entries;
        }

        bool key_exist(dbkey_t key) {
            // Serialize the key and tag
            size_t msg_size = sizeof(msg_type_t)+sizeof(dbkey_t);
            char msg[msg_size];
            char* msg_ptr = msg;

            pack_msg(msg_ptr, MAP_DOES_KEY_EXIST);

            // Add the actual entry
            pack_single(msg_ptr, key);

            // Send it to the PandoMap
            send(msg, msg_size);

            ZMQMessage resp = read();

            const char *resp_data = resp.data();
            bool exist;
            unpack_single(resp_data, exist);
            return exist;
        }

        vector<dbkey_t> keys() {
            // Serialize the key and tag
            size_t msg_size = sizeof(msg_type_t);
            char msg[msg_size];
            char* msg_ptr = msg;

            pack_msg(msg_ptr, MAP_GET_KEYS);

            // Send it to the PandoMap
            send(msg, msg_size);

            ZMQMessage resp = read();

            const char *resp_data = resp.data();
            size_t num_keys;
            unpack_single(resp_data, num_keys);

            vector <dbkey_t> keys;

            for (size_t i = 0; i < num_keys; i++) {
                dbkey_t key;
                unpack_single(resp_data, key);
                keys.push_back(key);
            }

            return keys;
        }

};

}
