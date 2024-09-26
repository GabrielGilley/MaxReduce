#pragma once

#include "absl/container/flat_hash_map.h"
#include "dbentry.hpp"
#include "address.hpp"
#include "par_db_participant.hpp"
#include "pack.hpp"
#include <thread>
#include <arpa/inet.h>
#include <sstream>
#include "big_space.hpp"


using namespace std;
using namespace elga;

namespace pando {

class PandoMap : public PandoParticipant {
    public:
        using Space = BigSpace;
        using Alloc = BigSpaceAllocator<char>;
    private:
        Alloc alloc_;
        absl::flat_hash_map<dbkey_t, DBEntry<Alloc>> map_;
        ZMQAddress addr_;

        /** Socket for our pando map requests*/
        zmq_socket_t sock_map_;

    public:

        PandoMap(const ZMQAddress &addr, size_t space_size) : PandoParticipant(addr, false), alloc_(make_shared<Space>(space_size)) {
            map_.reserve(1<<22);
        }
        PandoMap(const ZMQAddress &addr) : PandoMap(addr, (size_t)(2ull*(1ull<<29)) ) { }

        bool does_key_exist(dbkey_t key) {
            auto it = map_.find(key);
            return !(it == map_.end());
        }

        DBEntry<Alloc>* retrieve(dbkey_t key) {
            auto it = map_.find(key);
            if (it == map_.end()) {
                throw runtime_error("Unable to find entry to retrieve");
            }
            DBEntry<Alloc>* entry = &(it->second);
            entry->set_key(key);

            return entry;
        }

        void insert(DBEntry<Alloc> entry) {
            map_.insert_or_assign(entry.get_key(), move(entry));
        }

        void recv_map_insert(zmq_socket_t sock, [[maybe_unused]] const char* data, [[maybe_unused]] const char* end) {
            insert({alloc_, data});

            // If necessary, respond with an acknowledgement
            if (ZMQRequester::is_reqrep_sock(sock))
                ZMQChatterbox::ack(sock);

        }

        void recv_map_insert_multiple(zmq_socket_t sock, [[maybe_unused]] const char* data, [[maybe_unused]] const char* end) {
            while (data < end) {
                insert({alloc_, data});
            }

            // If necessary, respond with an acknowledgement
            if (ZMQRequester::is_reqrep_sock(sock))
                ZMQChatterbox::ack(sock);

        }

        void recv_map_retrieve(zmq_socket_t sock, const char* data, [[maybe_unused]] const char* end) {
            dbkey_t key;
            unpack_single(data, key);

            DBEntry<Alloc>* entry = retrieve(key);

            size_t ser_size = entry->serialize_size();
            size_t msg_size = ser_size;
            char* msg = new char[msg_size];
            char *msg_ptr = msg;

            entry->serialize(msg_ptr);

            ZMQChatterbox::send(sock, msg, msg_size);

            delete [] msg;
        }

        void recv_map_retrieve_if_exists(zmq_socket_t sock, const char* data, [[maybe_unused]] const char* end) {
            dbkey_t key;
            unpack_single(data, key);

            DBEntry<Alloc>* entry = nullptr;
            bool exists = true;
            size_t ser_size = 0;

            try {
                entry = retrieve(key);
                ser_size = entry->serialize_size();
            } catch (...) {
                exists = false;
            }

            size_t msg_size = sizeof(bool)+ser_size;
            char* msg = new char[msg_size];
            char *msg_ptr = msg;

            pack_single(msg_ptr, exists);
            if (exists)
                entry->serialize(msg_ptr);

            ZMQChatterbox::send(sock, msg, msg_size);

            delete [] msg;
        }

        void recv_map_size(zmq_socket_t sock, [[maybe_unused]] const char* data, [[maybe_unused]] const char* end) {
            size_t res = map_.size();

            size_t msg_size = sizeof(size_t);
            char msg[msg_size];
            char *msg_ptr = msg;
            pack_single(msg_ptr, res);

            ZMQChatterbox::send(sock, msg, msg_size);
        }

        void recv_map_get_keys(zmq_socket_t sock, [[maybe_unused]] const char* data, [[maybe_unused]] const char* end) {
            size_t msg_size = sizeof(size_t) + (sizeof(dbkey_t) * map_.size());
            char* msg = new char[msg_size];
            char *msg_ptr = msg;

            pack_single(msg_ptr, map_.size());

            for (auto & [key, entry] : map_) {
                pack_single(msg_ptr, key);
            }

            ZMQChatterbox::send(sock, msg, msg_size);

            delete [] msg;
        }

        void recv_map_get_entries(zmq_socket_t sock, [[maybe_unused]] const char* data, [[maybe_unused]] const char* end) {
            size_t msg_size = 0;
            // Iterate through the DB, finding the serialize size everywhere
            for (auto & [key, entry] : map_) {
                msg_size += entry.serialize_size();
            }

            // Allocate the msg
            char* msg = new char[msg_size];
            char *msg_ptr = msg;

            // Serialize all entries into it
            for (auto & [key, entry] : map_) {
                entry.serialize(msg_ptr);
            }

            ZMQChatterbox::send(sock, msg, msg_size);

            delete [] msg;
        }

        void recv_map_does_key_exist(zmq_socket_t sock, const char* data, [[maybe_unused]] const char* end) {
            dbkey_t key;
            unpack_single(data, key);
            bool exist = does_key_exist(key);

            size_t msg_size = sizeof(bool);
            char msg[msg_size];
            char *msg_ptr = msg;
            pack_single(msg_ptr, exist);

            ZMQChatterbox::send(sock, msg, msg_size);
        }

        void process_msg(msg_type_t type, zmq_socket_t sock, const char *data, const char* end) {
            if (type == MAP_INSERT) 
                recv_map_insert(sock, data, end);
            else if (type == MAP_INSERT_MULTIPLE)
                recv_map_insert_multiple(sock, data, end);
            else if (type == MAP_RETRIEVE)
                recv_map_retrieve(sock, data, end);
            else if (type == MAP_RETRIEVE_IF_EXISTS)
                recv_map_retrieve_if_exists(sock, data, end);
            else if (type == MAP_SIZE)
                recv_map_size(sock, data, end);
            else if (type == MAP_GET_KEYS)
                recv_map_get_keys(sock, data, end);
            else if (type == MAP_GET_ENTRIES)
                recv_map_get_entries(sock, data, end);
            else if (type == MAP_DOES_KEY_EXIST)
                recv_map_does_key_exist(sock, data, end);
            else
                throw runtime_error("Unknown message type");
        }

        /** @brief Return the SeqDB allocator used for large data */
        Alloc& get_allocator() {
            return alloc_;
        }

        DBEntry<Alloc> realloc_entry(DBEntry<> entry) {
            DBEntry<Alloc> new_entry {alloc_};
            new_entry.value().assign(entry.value());
            new_entry.set_key(entry.get_key());
            for (auto& tag : entry.tags()) {
                new_entry.add_tag(tag);
            }

            return new_entry;
        }

        DBEntry<> realloc_entry(DBEntry<Alloc> entry) {
            DBEntry<> new_entry;
            new_entry.value().assign(entry.value());
            new_entry.set_key(entry.get_key());
            for (auto& tag : entry.tags()) {
                new_entry.add_tag(tag);
            }

            return new_entry;
        }

        //DEBUGGING
        void print_entries() {
            for (auto & [key, entry] : map_) {
                cout << entry << "\n---------" << endl;
            }
        }
};

}
